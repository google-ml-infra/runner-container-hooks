/* eslint-disable @typescript-eslint/no-unused-vars */
import * as fs from 'fs'
import * as core from '@actions/core'

import { RunScriptStepArgs } from 'hooklib'
import {
  BackOffManager,
  copyFromPod,
  cpToPod,
  execPodStep,
  extractErrorMessageFromK8sError,
  getPodsFromJobSet,
  getRootCertClientCertAndKey,
  jobSetExists
} from '../k8s'
import {
  getEntryPointScriptContent,
  getNumberOfHost,
  runScriptByGrpc,
  sleep,
  useScriptExecutor,
  writeEntryPointScript
} from '../k8s/utils'
import {
  getJobSetName,
  getServiceName,
  GRPC_SCRIPT_EXECUTOR_PORT,
  JOB_CONTAINER_NAME
} from './constants'
import { MTLSCertAndPrivateKey } from '../k8s/certs'

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

  core.info('using script executor')
  const rootCertClientAndKey = await getRootCertClientCertAndKey()
  core.debug('successfully retrieved root cert, client and key')
  core.info('script content is ' + scriptContent)

  if (getNumberOfHost() > 1) {
    return runScriptStepInJobSet(scriptContent, rootCertClientAndKey)
  }

  // This will throw after retrying with back off for up to 60s.
  const backOffmanager = new BackOffManager(60)
  while (true) {
    try {
      await runScriptByGrpc(
        scriptContent,
        rootCertClientAndKey.caCertAndkey.cert,
        rootCertClientAndKey.clientCertAndKey.cert,
        rootCertClientAndKey.clientCertAndKey.privateKey,
        getServiceName(),
        GRPC_SCRIPT_EXECUTOR_PORT
      )
      break
    } catch (err) {
      const message = extractErrorMessageFromK8sError(err)
      core.debug(`ScriptExecutorError when trying to execute: ${message}`)
      if (message.includes('ECONNREFUSED')) {
        // Retry for 60s since the service may not be established.
        core.debug(`Retrying execution for ECONNREFUSED.`)
        await backOffmanager.backOff()
      } else {
        throw new Error(
          `ScriptExecutorError when trying to execute: ${message}`
        )
      }
    }
  }
}

async function runScriptStepInJobSet(
  scriptContent: string,
  rootCertClientAndKey: MTLSCertAndPrivateKey
): Promise<void> {
  const jobSetName = getJobSetName()
  if (!(await jobSetExists(jobSetName))) {
    throw new Error(`JobSet ${jobSetName} does not exist.`)
  }

  try {
    core.debug(`retrieving pods from JobSet ${jobSetName}`)
    const pods = await getPodsFromJobSet(jobSetName)

    core.debug(`syncing runner folders to workflow pods for ${jobSetName}`)
    await Promise.all(
      pods.items.map(async pod => {
        try {
          await syncRunnerFolderToWorkflowPod(
            pod.metadata?.name!!,
            pod.status!!.podIP!!,
            rootCertClientAndKey
          )
        } catch (error) {
          const message = extractErrorMessageFromK8sError(error)
          throw new Error(
            `MultiHostError when execing the pod ${pod.metadata?.name} in JobSet ${jobSetName}: ${message}`
          )
        }
      })
    )

    core.debug(`executing script for ${jobSetName}`)
    await Promise.all(
      pods.items.map(async pod => {
        try {
          const jobCompletionIndex =
            pod.metadata?.annotations!![
              'batch.kubernetes.io/job-completion-index'
            ]
          core.debug(
            `Running script by grpc in pod ${pod.metadata?.name} with prefix ${jobCompletionIndex}`
          )
          // For the 0th index, don't put a prefix.
          const jobPrefix = jobCompletionIndex && Number(jobCompletionIndex) > 0 ? `job-${jobCompletionIndex}: ` : ''
          core.info('job prefix is ' + jobPrefix)
          // TODO(quoct): Add a prefix to the log output
          return runScriptByGrpc(
            scriptContent,
            rootCertClientAndKey.caCertAndkey.cert,
            rootCertClientAndKey.clientCertAndKey.cert,
            rootCertClientAndKey.clientCertAndKey.privateKey,
            pod.status!!.podIP!!,
            GRPC_SCRIPT_EXECUTOR_PORT,
            true,
            jobPrefix
          )
        } catch (error) {
          const message = extractErrorMessageFromK8sError(error)
          throw new Error(
            `MultiHostError when execing the pod ${pod.metadata?.name} in JobSet ${jobSetName}: ${message}`
          )
        }
      })
    )
    core.debug(`syncing workflow pod to runner pod with copyFromPod`)
    // We can just copy from one of the pod.
    await copyFromPod('/__w/_temp/_runner_file_commands', '/home/runner/_work/_temp/_runner_file_commands', pods.items[0].metadata!!.name!!, JOB_CONTAINER_NAME)
    core.debug(`done copying`)
    await sleep(50000)
  } catch (error) {
    const message = extractErrorMessageFromK8sError(error)
    throw new Error(
      `MultiHostError when execing pods in JobSet ${jobSetName}: ${message}`
    )
  }
}

// TODO(quoct): Check if we can optimize and not delete the _actions folder every time.
async function syncRunnerFolderToWorkflowPod(
  podName: string,
  podIp: string,
  rootCertClientAndKey: MTLSCertAndPrivateKey
): Promise<void> {
  // TODO(quoct): Check if we can optimize and not delete the _actions folder every time.
  core.debug(
    'deleting _temp folder, /github/workflow/, _actions and /github/home/ folders'
  )
  const command = `
mkdir -p /github/home;
mkdir -p /github/workflow;
mkdir -p /__w/_temp;
mkdir -p /__w/_actions;
mkdir -p /__w/_tool`

  await runScriptByGrpc(
    command,
    rootCertClientAndKey.caCertAndkey.cert,
    rootCertClientAndKey.clientCertAndKey.cert,
    rootCertClientAndKey.clientCertAndKey.privateKey,
    podIp,
    GRPC_SCRIPT_EXECUTOR_PORT,
    false
  )

  core.info(
    `copying temp folder for ${podName} in ${JOB_CONTAINER_NAME} container`
  )
  await cpToPod(
    podName,
    JOB_CONTAINER_NAME,
    '/home/runner/_work/_temp',
    '/__w/_temp'
  )

  if (fs.existsSync('/home/runner/_work/_actions')) {
    core.info('copying /home/runner/_work/_actions')
    await cpToPod(
      podName,
      JOB_CONTAINER_NAME,
      '/home/runner/_work/_actions',
      '/__w/_actions'
    )
  }

  if (fs.existsSync('/home/runner/_work/_tool')) {
    core.info('copying /home/runner/_work/_tool')
    await cpToPod(
      podName,
      JOB_CONTAINER_NAME,
      '/home/runner/_work/_tool',
      '/__w/_tool'
    )
  }

  core.info('copying github_home and github_workflow folder')
  await runScriptByGrpc(
    'cp -a /__w/_temp/_github_home/. /github/home/; cp -a /__w/_temp/_github_workflow/. /github/workflow',
    rootCertClientAndKey.caCertAndkey.cert,
    rootCertClientAndKey.clientCertAndKey.cert,
    rootCertClientAndKey.clientCertAndKey.privateKey,
    podIp,
    GRPC_SCRIPT_EXECUTOR_PORT,
    false
  )
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
    const message = extractErrorMessageFromK8sError(err)
    core.debug(`execPodStep failed: ${message}`)
    throw new Error(`failed to run script step: ${message}`)
  } finally {
    fs.rmSync(runnerPath)
  }
}
