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
import { tmpdir } from 'os'
import { join } from 'path'
import { Writable } from 'stream'

async function runScriptStepWithGRPC(
  args: RunScriptStepArgs,
  state
): Promise<void> {
  const runnerDir = `/home/runner/_work/_temp/_runner_file_commands`
  const files = fs.readdirSync('/home/runner/_work/_temp/_runner_file_commands')

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
    try {
      return runScriptStepInJobSet(scriptContent, rootCertClientAndKey)
    } catch (error) {
      const message = extractErrorMessageFromK8sError(error)
      throw new Error(
        `MultiHostError when execing pods in JobSet ${getJobSetName()}: ${message}`
      )
    }
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
        getServiceName()
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

  core.debug(`retrieving pods from JobSet ${jobSetName}`)
  const pods = await getPodsFromJobSet(jobSetName)

  core.debug(`syncing runner folders to workflow pods for ${jobSetName}`)
  await Promise.all(
    pods.items.map(async pod => {
      core.debug(`sync runner folder to workflow pod ${pod.metadata?.name}`)
      await syncRunnerFolderToWorkflowPod(
        pod.metadata?.name!!,
        pod.status!!.podIP!!,
        rootCertClientAndKey
      )
    })
  )

  const tempTestDir = tmpdir()
  const indexToStreamMap: Map<number, [output: Writable, errStream: Writable]> =
    new Map()
  for (let i = 0; i < pods.items.length; i += 1) {
    indexToStreamMap[i] = [
      i === 0
        ? process.stdout
        : fs.createWriteStream(join(tempTestDir, `${jobSetName}-${i}.out`)),
      i === 0
        ? process.stderr
        : fs.createWriteStream(join(tempTestDir, `${jobSetName}-${i}.err`))
    ]
  }

  core.debug(`executing script for ${jobSetName}`)
  await Promise.all(
    pods.items.map(async pod => {
      const jobCompletionIndex = Number(
        pod.metadata?.annotations!!['batch.kubernetes.io/job-completion-index']
      )
      core.debug(
        `Running script by grpc in pod ${pod.metadata?.name} with prefix ${jobCompletionIndex}`
      )

      return runScriptByGrpc(
        scriptContent,
        rootCertClientAndKey.caCertAndkey.cert,
        rootCertClientAndKey.clientCertAndKey.cert,
        rootCertClientAndKey.clientCertAndKey.privateKey,
        pod.status!!.podIP!!,
        GRPC_SCRIPT_EXECUTOR_PORT,
        indexToStreamMap[jobCompletionIndex][0],
        indexToStreamMap[jobCompletionIndex][1]
      )
    })
  )

  // Output the output and error for the rest of the jobs.
  for (let i = 1; i < pods.items.length; i += 1) {
    const jobOutput = fs.readFileSync(
      join(tempTestDir, `${jobSetName}-${i}.out`)
    )
    if (jobOutput.length) {
      core.notice(`Job ${i} output`)
      process.stdout.write(jobOutput)
    }
    const jobError = fs.readFileSync(
      join(tempTestDir, `${jobSetName}-${i}.err`)
    )
    if (jobError.length) {
      core.warning(`Job ${i} error`)
      process.stderr.write(jobError)
    }
    indexToStreamMap[i][0].close()
    indexToStreamMap[i][1].close()
  }

  core.debug(`syncing workflow pod to runner pod with copyFromPod`)
  await copyFromPod(
    '/__w/_temp/_runner_file_commands',
    '/home/runner/_work/_temp/_runner_file_commands',
    pods.items[0].metadata!!.name!!,
    JOB_CONTAINER_NAME
  )
}

async function syncRunnerFolderToWorkflowPod(
  podName: string,
  podIp: string,
  rootCertClientAndKey: MTLSCertAndPrivateKey
): Promise<void> {
  core.debug('create folders used by GitHub Actions.')
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
    undefined,
    undefined
  )

  core.debug(
    `copying temp folder for ${podName} in ${JOB_CONTAINER_NAME} container`
  )
  await cpToPod(
    podName,
    JOB_CONTAINER_NAME,
    '/home/runner/_work/_temp',
    '/__w/_temp'
  )

  if (fs.existsSync('/home/runner/_work/_actions')) {
    core.debug('copying /home/runner/_work/_actions')
    await cpToPod(
      podName,
      JOB_CONTAINER_NAME,
      '/home/runner/_work/_actions',
      '/__w/_actions'
    )
  }

  if (fs.existsSync('/home/runner/_work/_tool')) {
    core.debug('copying /home/runner/_work/_tool')
    await cpToPod(
      podName,
      JOB_CONTAINER_NAME,
      '/home/runner/_work/_tool',
      '/__w/_tool'
    )
  }

  core.debug('copying github_home and github_workflow folder')
  await runScriptByGrpc(
    'cp -a /__w/_temp/_github_home/. /github/home/; cp -a /__w/_temp/_github_workflow/. /github/workflow',
    rootCertClientAndKey.caCertAndkey.cert,
    rootCertClientAndKey.clientCertAndKey.cert,
    rootCertClientAndKey.clientCertAndKey.privateKey,
    podIp,
    GRPC_SCRIPT_EXECUTOR_PORT,
    undefined,
    undefined
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
