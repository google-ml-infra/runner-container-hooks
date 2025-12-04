import * as core from '@actions/core'
import * as io from '@actions/io'
import * as k8s from '@kubernetes/client-node'
import {
  JobContainerInfo,
  ContextPorts,
  PrepareJobArgs,
  ServiceContainerInfo,
  writeToResponseFile
} from 'hooklib'
import path from 'path'
import {
  containerPorts,
  createPod,
  isPodContainerAlpine,
  prunePods,
  waitForPodPhases,
  getPrepareJobTimeoutSeconds,
  extractErrorMessageFromK8sError,
  createJobSet,
  createPodSpec,
  getPodsFromJobSet,
  BackOffManager,
  createHeadlessServiceWithRetry
} from '../k8s'
import {
  containerVolumes,
  DEFAULT_CONTAINER_ENTRY_POINT,
  DEFAULT_CONTAINER_ENTRY_POINT_ARGS,
  generateContainerName,
  mergeContainerWithOptions,
  readExtensionFromFile,
  PodPhase,
  fixArgs,
  useScriptExecutor,
  SCRIPT_EXECUTOR_ENTRY_POINT,
  SCRIPT_EXECUTOR_ENTRY_POINT_ARGS,
  getNumberOfHost,
  sleep,
  generateServicesName,
  getEntryPointAndArgs,
  getTpuRequest
} from '../k8s/utils'
import {
  CONTAINER_EXTENSION_PREFIX,
  getJobSetName,
  getServiceName,
  JOB_CONTAINER_NAME
} from './constants'

export async function prepareJob(
  args: PrepareJobArgs,
  responseFile
): Promise<void> {
  if (!args.container) {
    throw new Error('Job Container is required.')
  }

  await prunePods()

  const extension = readExtensionFromFile()
  await copyExternalsToRoot()

  let container: k8s.V1Container | undefined = undefined
  if (args.container?.image) {
    core.debug(`Using image '${args.container.image}' for job image`)
    container = createContainerSpec(
      args.container,
      JOB_CONTAINER_NAME,
      true,
      extension
    )
  }

  const services: k8s.V1Container[] = processServiceContainers(
    args.services,
    container,
    extension
  )

  if (!container && !services?.length) {
    throw new Error('No containers exist, skipping hook invocation')
  }

  core.debug(`number of hosts requested are ${getNumberOfHost()}`)
  if (getNumberOfHost() > 1) {
    return await prepareJobSet(args, responseFile, container, extension)
  }

  let createdPod: k8s.V1Pod | undefined = undefined
  try {
    createdPod = await createPod(
      container,
      services,
      args.container.registry,
      extension
    )
  } catch (err) {
    await prunePods()
    core.debug(`createPod failed: ${JSON.stringify(err)}`)
    const message = extractErrorMessageFromK8sError(err)
    throw new Error(`failed to create job pod: ${message}`)
  }

  if (!createdPod?.metadata?.name) {
    throw new Error('created pod should have metadata.name')
  }
  core.debug(
    `Job pod created, waiting for it to come online ${createdPod?.metadata?.name}`
  )

  try {
    await waitForPodPhases(
      createdPod.metadata.name,
      new Set([PodPhase.RUNNING]),
      new Set([PodPhase.PENDING]),
      getPrepareJobTimeoutSeconds()
    )
  } catch (err) {
    await prunePods()
    throw new Error(`pod failed to come online with error: ${err}`)
  }

  core.debug('Job pod is ready for traffic')

  let isAlpine = false
  try {
    isAlpine = await isPodContainerAlpine(
      createdPod.metadata.name,
      JOB_CONTAINER_NAME
    )
  } catch (err) {
    core.debug(
      `Failed to determine if the pod is alpine: ${JSON.stringify(err)}`
    )
    const message = extractErrorMessageFromK8sError(err)
    throw new Error(`failed to determine if the pod is alpine: ${message}`)
  }
  core.debug(`Setting isAlpine to ${isAlpine}`)

  if (useScriptExecutor() && getNumberOfHost() === 1) {
    core.debug(`Creating headless service`)
    await createHeadlessServiceWithRetry()
  }

  generateResponseFile(responseFile, args, createdPod, isAlpine)
}

export function processServiceContainers(
  services?: ServiceContainerInfo[],
  container?: k8s.V1Container,
  extension?: k8s.V1PodTemplateSpec
): k8s.V1Container[] {
  if (!services?.length) {
    return []
  }
  generateServicesName(services)
  const serviceContainers = services.map(service => {
    core.debug(`Adding service '${service.image}' to pod definition`)
    return createContainerSpec(
      service,
      service.name,
      false,
      extension,
      service.createOptions
    )
  })

  const tpuRequestingContainers = serviceContainers.filter(
    service =>
      service.resources?.limits && service.resources.limits['google.com/tpu']
  )
  core.debug(
    `There are ${tpuRequestingContainers.length} service container requesting for TPU's.`
  )

  if (tpuRequestingContainers.length > 1) {
    throw new Error(
      `${tpuRequestingContainers.length} containers request for TPU's. Only 1 container per pod can request for TPU's.`
    )
  }

  if (tpuRequestingContainers.length === 1) {
    if (
      container?.resources?.requests &&
      container.resources.requests['google.com/tpu']
    ) {
      core.debug(
        'removing tpu from main container resources request and limits as they are requested by the service container and only 1 container in a pod can request TPU.'
      )
      delete container.resources.requests['google.com/tpu']
      if (
        container.resources.limits &&
        container.resources.limits['google.com/tpu']
      ) {
        core.debug('removing tpu from main container resource limits')
        delete container.resources.limits['google.com/tpu']
      }
    }
  }
  return serviceContainers
}

// Create JobSet and waits for it to come online
async function prepareJobSet(
  args: PrepareJobArgs,
  responseFile,
  jobContainer?: k8s.V1Container,
  extension?: k8s.V1PodTemplateSpec
): Promise<void> {
  const jobSetName = getJobSetName()
  const noOfHosts = getNumberOfHost()

  const podSpec = await createPodSpec(jobContainer, [], null, extension)
  core.info(`creating JobSet ${jobSetName} for ${noOfHosts} hosts.`)
  await createJobSet(jobSetName, podSpec, noOfHosts)

  core.info('waiting for pods from JobSet to come online')
  await sleep(5_000)
  const pods = await getPodsFromJobSet(jobSetName)

  let createdPod: k8s.V1Pod | undefined = undefined
  await Promise.all(
    pods.items.map(async pod => {
      try {
        if (!createdPod) {
          createdPod = pod
        }
        core.debug(`waiting for pod ${pod.metadata?.name} to come online`)
        await waitForPodPhases(
          pod.metadata!!.name!!,
          new Set([PodPhase.RUNNING]),
          new Set([PodPhase.PENDING]),
          getPrepareJobTimeoutSeconds()
        )
      } catch (err) {
        throw new Error(
          `pod from job set failed to come online with error: ${err}`
        )
      }
    })
  )

  if (!createdPod) {
    throw new Error(
      `failed to retrieve a pod from JobSet ${jobSetName} for ${noOfHosts} hosts.`
    )
  }

  let isAlpine = false
  try {
    isAlpine = await isPodContainerAlpine(
      (createdPod as k8s.V1Pod).metadata!!.name!!,
      JOB_CONTAINER_NAME
    )
  } catch (err) {
    const message = extractErrorMessageFromK8sError(err)
    core.debug(`Failed to determine if the pod is alpine: ${message}`)
    throw new Error(`failed to determine if the pod is alpine: ${message}`)
  }

  core.debug('pods from jobset are now online ')
  generateResponseFile(responseFile, args, createdPod, isAlpine)
}

function generateResponseFile(
  responseFile: string,
  args: PrepareJobArgs,
  appPod: k8s.V1Pod,
  isAlpine
): void {
  if (!appPod.metadata?.name) {
    throw new Error('app pod must have metadata.name specified')
  }
  const response = {
    state: {
      jobPod: appPod.metadata.name
    },
    context: {},
    isAlpine
  }

  const mainContainer = appPod.spec?.containers?.find(
    c => c.name === JOB_CONTAINER_NAME
  )
  if (mainContainer) {
    const mainContainerContextPorts: ContextPorts = {}
    if (mainContainer?.ports) {
      for (const port of mainContainer.ports) {
        mainContainerContextPorts[port.containerPort] =
          mainContainerContextPorts.hostPort
      }
    }

    response.context['container'] = {
      image: mainContainer.image,
      ports: mainContainerContextPorts
    }
  }

  if (args.services?.length) {
    const serviceContainerNames =
      args.services?.map(s => generateContainerName(s.image)) || []

    response.context['services'] = appPod?.spec?.containers
      ?.filter(c => serviceContainerNames.includes(c.name))
      .map(c => {
        const ctxPorts: ContextPorts = {}
        if (c.ports?.length) {
          for (const port of c.ports) {
            ctxPorts[port.containerPort] = port.hostPort
          }
        }

        return {
          image: c.image,
          ports: ctxPorts
        }
      })
  }

  writeToResponseFile(responseFile, JSON.stringify(response))
}

async function copyExternalsToRoot(): Promise<void> {
  const workspace = process.env['RUNNER_WORKSPACE']
  if (workspace) {
    await io.cp(
      path.join(workspace, '../../externals'),
      path.join(workspace, '../externals'),
      { force: true, recursive: true, copySourceDirectory: false }
    )
  }
}

export function createContainerSpec(
  container: JobContainerInfo,
  name: string,
  jobContainer = false,
  extension?: k8s.V1PodTemplateSpec,
  createOptions?: string
): k8s.V1Container {
  if (!container.entryPoint && jobContainer) {
    container.entryPoint = DEFAULT_CONTAINER_ENTRY_POINT
    container.entryPointArgs = DEFAULT_CONTAINER_ENTRY_POINT_ARGS

    if (useScriptExecutor()) {
      core.debug('starting script executor server')
      // Starting the server.
      container.entryPoint =
        process.env['ACTIONS_RUNNER_SCRIPT_EXECUTOR_ENTRY_POINT'] ||
        SCRIPT_EXECUTOR_ENTRY_POINT
      container.entryPointArgs = process.env[
        'ACTIONS_RUNNER_SCRIPT_EXECUTOR_ARGS'
      ]
        ? process.env['ACTIONS_RUNNER_SCRIPT_EXECUTOR_ARGS'].split(' ')
        : SCRIPT_EXECUTOR_ENTRY_POINT_ARGS
    }
  }

  let tpuRequest = 0
  if (!jobContainer && createOptions && createOptions?.length > 0) {
    core.debug(
      `overriding service container ${JSON.stringify(
        container
      )} with createOptions ${createOptions}`
    )
    const entryPointAndArgs = getEntryPointAndArgs(createOptions)
    if (entryPointAndArgs.length > 1) {
      core.debug(`overriding container entry points with ${entryPointAndArgs}`)
      container.entryPoint = entryPointAndArgs[0]
      container.entryPointArgs = entryPointAndArgs.slice(1)
    }
    tpuRequest = getTpuRequest(createOptions)
    core.debug(`TPU request from service container is ${tpuRequest}`)
  }

  const podContainer = {
    name,
    image: container.image,
    ports: containerPorts(container)
  } as k8s.V1Container
  if (container.workingDirectory) {
    podContainer.workingDir = container.workingDirectory
  }

  if (container.entryPoint) {
    podContainer.command = [container.entryPoint]
  }

  if (container.entryPointArgs?.length > 0) {
    podContainer.args = fixArgs(container.entryPointArgs)
  }

  if (tpuRequest > 0) {
    core.debug(`assigning ${tpuRequest} to podContainer`)
    podContainer.resources = {
      limits: {
        'google.com/tpu': String(tpuRequest)
      },
      requests: {
        'google.com/tpu': String(tpuRequest)
      }
    }
  }

  podContainer.env = []
  if (container['environmentVariables']) {
    for (const [key, value] of Object.entries(
      container['environmentVariables']
    )) {
      if (value && key !== 'HOME') {
        podContainer.env.push({ name: key, value: value as string })
      }
    }

    if (!('CI' in container['environmentVariables'])) {
      podContainer.env.push({
        name: 'CI',
        value: 'true'
      })
    }
  }

  podContainer.env.push({
    name: 'GITHUB_ACTIONS',
    value: 'true'
  })

  podContainer.volumeMounts = containerVolumes(
    container.userMountVolumes,
    jobContainer
  )

  if (!extension) {
    return podContainer
  }

  const from = extension.spec?.containers?.find(
    c => c.name === CONTAINER_EXTENSION_PREFIX + name
  )

  if (from) {
    mergeContainerWithOptions(podContainer, from)
  }

  return podContainer
}
