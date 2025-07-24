/* eslint-disable @typescript-eslint/no-unused-vars */
import * as fs from 'fs'
import * as core from '@actions/core'

import { RunScriptStepArgs } from 'hooklib'
import { BackOffManager, execPodStep, getRootCertClientCertAndKey } from '../k8s'
import {
  getEntryPointScriptContent,
  runScriptByGrpc,
  useScriptExecutor,
  writeEntryPointScript
} from '../k8s/utils'
import {
  getServiceName,
  GRPC_SCRIPT_EXECUTOR_PORT,
  JOB_CONTAINER_NAME
} from './constants'
import { MTLSCertAndPrivateKey } from 'src/k8s/certs'

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

  let rootCertClientAndKey: MTLSCertAndPrivateKey;
  let serviceName: string;
  try {
    core.info('using script executor')

    serviceName = getServiceName()
    core.debug(`using service name ${serviceName}`)

    rootCertClientAndKey = await getRootCertClientCertAndKey()
    core.debug('successfully retrieved root cert, client and key')
  } catch (err) {
    core.error(`ScriptExecutorError when trying to get cert: ${JSON.stringify(err)}`)
    const message = (err as any)?.response?.body?.message || err
    throw new Error(`failed to get script cert: ${message}`)
  }

  const backOffmanager = new BackOffManager(60)
  while (true) {
    try {
      await runScriptByGrpc(
        scriptContent,
        rootCertClientAndKey.caCertAndkey.cert,
        rootCertClientAndKey.clientCertAndKey.cert,
        rootCertClientAndKey.clientCertAndKey.privateKey,
        serviceName,
        GRPC_SCRIPT_EXECUTOR_PORT
      )
      break;
    } catch (err) {
      core.error(`ScriptExecutorError when trying to get cert: ${JSON.stringify(err)}`)
      const message = (err as any)?.response?.body?.message || err
      if (String(message).includes("ECONNREFUSED")) {
        core.debug('quoct ECONNREFUSED')
        await backOffmanager.backOff()
      } else {
        break;
      }
    }
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
