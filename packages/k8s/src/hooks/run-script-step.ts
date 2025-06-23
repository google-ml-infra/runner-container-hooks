/* eslint-disable @typescript-eslint/no-unused-vars */
import * as fs from 'fs'
import * as core from '@actions/core'

import { RunScriptStepArgs } from 'hooklib'
import { clonePersistentVolume, createK8sPod, createPod, execPodStep, getPod, getPodStatus, getPrepareJobTimeoutSeconds, getRootCertClientCertAndKey, waitForPodPhases } from '../k8s'
import {
  fixArgs,
  PodPhase,
  getEntryPointScriptContent,
  runScriptByGrpc,
  sleep,
  useScriptExecutor,
  writeEntryPointScript
} from '../k8s/utils'
import { GRPC_SCRIPT_EXECUTOR_PORT, JOB_CONTAINER_NAME } from './constants'

async function runScriptStepWithGRPC(
  args: RunScriptStepArgs,
  state
): Promise<void> {
  const { entryPoint, entryPointArgs, environmentVariables } = args
  const scriptContent = getEntryPointScriptContent(
    args.workingDirectory,
    entryPoint,
    entryPointArgs,
    args.prependPath,
    environmentVariables
  )

  let createdQuoctPod;
  try {
   createdQuoctPod = await getPod("quoct-post-test-workflow")
  } catch (error) {
    core.debug("Error getting quoct pod " + error)
  }
  if (!createdQuoctPod) {
    core.debug("Could not find quoct pod")
    core.debug("clone persistent volume for test")
    await clonePersistentVolume("quoct-post-test-workflow")
    core.debug("creating pod helper")
  
    createdQuoctPod = await getPod(state.jobPod)
    if (!createPod) {
      core.debug("Cannot find " + state.podName)
      throw new Error("cannot find created pod")
    }
    const previousPodMetadata = createdQuoctPod!!.metadata
    createdQuoctPod!!.metadata = {
      name: "quoct-post-test-workflow",
      namespace: previousPodMetadata!!.namespace,
      annotations: previousPodMetadata!!.annotations,
      labels: previousPodMetadata!!.labels,
    }

    createdQuoctPod!!.spec!!.nodeName = ""
    core.debug(`volume are ${JSON.stringify(createdQuoctPod!!.spec!!.volumes!!)}`)
  
    const volume = createdQuoctPod!!.spec!!.volumes!!.find(vol => vol.name === 'work')
    core.debug(`volume is ${JSON.stringify(volume)}`)
    volume!!.persistentVolumeClaim = {
      claimName: "quoct-post-test-workflow"
    }
    core.debug(`volumes are now ${JSON.stringify(createdQuoctPod!!.spec!!.volumes!!)}`)
    const newPod = await createK8sPod(createdQuoctPod!!)
    core.debug(`Created new pod ${JSON.stringify(newPod)}`)

    await waitForPodPhases(
      newPod!!.metadata!!.name!!,
      new Set([PodPhase.RUNNING]),
      new Set([PodPhase.PENDING]),
      getPrepareJobTimeoutSeconds()
    )
    core.debug(`Pod quoct-post-test-workflow is now ready`)

  } else {
    core.debug("Found quoct pod " + JSON.stringify(createdQuoctPod))
  }

  try {
    const podName = state.jobPod
    core.info('using script executor')

    const status = await getPodStatus(podName)
    if (status?.phase === 'Succeeded') {
      throw new Error(`Failed to get pod ${podName} status`)
    }
    if (status?.podIP === undefined) {
      throw new Error(`Failed to get pod ${podName} IP`)
    }

    const rootCertClientAndKey = await getRootCertClientCertAndKey()
    core.debug('successfully retrieved root cert, client and key')
    await runScriptByGrpc(
      scriptContent,
      rootCertClientAndKey.caCertAndkey.cert,
      rootCertClientAndKey.clientCertAndKey.cert,
      rootCertClientAndKey.clientCertAndKey.privateKey,
      status.podIP,
      GRPC_SCRIPT_EXECUTOR_PORT
    )

    core.debug(`created pod status ${JSON.stringify(createdQuoctPod.status)}`)
    await runScriptByGrpc(
      scriptContent,
      rootCertClientAndKey.caCertAndkey.cert,
      rootCertClientAndKey.clientCertAndKey.cert,
      rootCertClientAndKey.clientCertAndKey.privateKey,
      createdQuoctPod.status.podIP,
      GRPC_SCRIPT_EXECUTOR_PORT
    )
  } catch (err) {
    core.debug(
      `Run script Executor through GRPC failed: ${JSON.stringify(err)}`
    )
    const message = (err as any)?.response?.body?.message || err
    throw new Error(`failed to run script step: ${message}`)
  }
}

export async function runScriptStep(
  args: RunScriptStepArgs,
  state,
  responseFile
): Promise<void> {
  if (useScriptExecutor()) {
    return runScriptStepWithGRPC(args, state)
  }

  const { entryPoint, entryPointArgs, environmentVariables } = args
  const { containerPath, runnerPath } = writeEntryPointScript(
    args.workingDirectory,
    entryPoint,
    entryPointArgs,
    args.prependPath,
    environmentVariables
  )

  args.entryPoint = 'sh'
  args.entryPointArgs = ['-e', containerPath]
  const podName = state.jobPod
  try {
    core.info('using exec pod step')
    await execPodStep(
      [args.entryPoint, ...args.entryPointArgs],
      podName,
      JOB_CONTAINER_NAME
    )
  } catch (err) {
    core.debug(`execPodStep failed: ${JSON.stringify(err)}`)
    const message = (err as any)?.response?.body?.message || err
    throw new Error(`failed to run script step: ${message}`)
  } finally {
    fs.rmSync(runnerPath)
  }
}
