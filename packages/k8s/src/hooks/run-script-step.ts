/* eslint-disable @typescript-eslint/no-unused-vars */
import * as fs from 'fs'
import * as core from '@actions/core'

import { RunScriptStepArgs } from 'hooklib'
import {
  BackOffManager,
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

    await Promise.all(
      pods.items.map(async pod => {
        try {
          // TODO(quoct): Check if we can optimize and not delete the _actions folder every time.
          core.debug(
            'deleting _temp folder, /github/workflow/, _actions and /github/home/ folders'
          )
          await runScriptByGrpc(
            'rm -rf /__w/_actions; rm -rf /__w/_temp/*; rm -rf /github/home/*; rm -rf /github/workflow/*; mkdir -p /github/home; mkdir -p /github/workflow; mkdir -p /__w/_temp; mkdir -p /__w/_actions',
            rootCertClientAndKey.caCertAndkey.cert,
            rootCertClientAndKey.clientCertAndKey.cert,
            rootCertClientAndKey.clientCertAndKey.privateKey,
            pod.status!!.podIP!!,
            GRPC_SCRIPT_EXECUTOR_PORT,
            false
          )

          core.debug(
            `copying temp folder for ${pod.metadata!!
              .name!!} in ${JOB_CONTAINER_NAME} container`
          )
          await cpToPod(
            pod.metadata!!.name!!,
            JOB_CONTAINER_NAME,
            '/home/runner/_work/_temp',
            '/__w/_temp'
          )

          if (fs.existsSync('/home/runner/_work/_actions')) {
            core.debug('copying /home/runner/_work/_actions')
            await cpToPod(
              pod.metadata!!.name!!,
              JOB_CONTAINER_NAME,
              '/home/runner/_work/_actions',
              '/__w/_actions'
            )
          }

          core.debug('copying github_home and github_workflow folder')
          await runScriptByGrpc(
            'cp -a /__w/_temp/_github_home/. /github/home/; cp -a /__w/_temp/_github_workflow/. /github/workflow',
            rootCertClientAndKey.caCertAndkey.cert,
            rootCertClientAndKey.clientCertAndKey.cert,
            rootCertClientAndKey.clientCertAndKey.privateKey,
            pod.status!!.podIP!!,
            GRPC_SCRIPT_EXECUTOR_PORT,
            false
          )

          const jobCompletionIndex =
            pod.metadata?.annotations!![
              'batch.kubernetes.io/job-completion-index'
            ]
          core.debug(
            `Running script by grpc in pod ${pod.metadata?.name} with prefix ${jobCompletionIndex}`
          )
          // TODO(quoct): Add a prefix to the log output
          return runScriptByGrpc(
            scriptContent,
            rootCertClientAndKey.caCertAndkey.cert,
            rootCertClientAndKey.clientCertAndKey.cert,
            rootCertClientAndKey.clientCertAndKey.privateKey,
            pod.status!!.podIP!!,
            GRPC_SCRIPT_EXECUTOR_PORT,
            true,
            `job-${jobCompletionIndex}: `
          )
        } catch (error) {
          const message = extractErrorMessageFromK8sError(error)
          core.info(`quoct sleeping ${message}`)
          await sleep(500000)
          throw new Error(
            `MultiHostError when execing the pod ${pod.metadata?.name} in JobSet ${jobSetName}: ${message}`
          )
        }
      })
    )
  } catch (error) {
    const message = extractErrorMessageFromK8sError(error)
    core.info(`quoct sleeping ${message}`)
    await sleep(500000)
    throw new Error(
      `MultiHostError when execing pods in JobSet ${jobSetName}: ${message}`
    )
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
    const message = extractErrorMessageFromK8sError(err)
    core.debug(`execPodStep failed: ${message}`)
    throw new Error(`failed to run script step: ${message}`)
  } finally {
    fs.rmSync(runnerPath)
  }
}
