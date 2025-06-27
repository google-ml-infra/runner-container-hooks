/* eslint-disable @typescript-eslint/no-unused-vars */
import * as fs from 'fs'
import * as core from '@actions/core'

import { RunScriptStepArgs } from 'hooklib'
import { checkIfPvcExist, clonePVCReadOnlyManyFromExistingPVC, createJobSet, createK8sPod, createPod, execPodStep, getJobSet, getPod, getPodsFromJobSet, getPodStatus, getPrepareJobTimeoutSeconds, getRootCertClientCertAndKey, waitForPodPhases } from '../k8s'
import {
  fixArgs,
  PodPhase,
  getEntryPointScriptContent,
  runScriptByGrpc,
  sleep,
  useScriptExecutor,
  writeEntryPointScript
} from '../k8s/utils'
import { getJobSetName, getReadOnlyManyVolumeClaimName, getVolumeClaimName, GRPC_SCRIPT_EXECUTOR_PORT, JOB_CONTAINER_NAME } from './constants'

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
  const podName = state.jobPod
  const pod = await getPod(state.jobPod)
  const status = pod?.status
  if (status?.phase === 'Succeeded') {
    throw new Error(`Failed to get pod ${podName} status`)
  }
  if (status?.podIP === undefined) {
    throw new Error(`Failed to get pod ${podName} IP`)
  }

  const romPVC = getReadOnlyManyVolumeClaimName()
  const jobSetName = getJobSetName()
  core.info('checking if pvc exists ' + romPVC)
  if (await checkIfPvcExist(romPVC)) {
    core.info("Found pvc" + romPVC)
  } else {
    core.info("clone persistent volume " + romPVC + " for test")
    // await clonePersistentVolume("quoct-post-test-workflow")
    await clonePVCReadOnlyManyFromExistingPVC(getVolumeClaimName(), romPVC)

    core.info('creating job set ' + jobSetName)
    await createJobSet(jobSetName, pod!!.spec!!, romPVC)
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

  core.debug('Retrieving job set pods')
  const pods = await getPodsFromJobSet(jobSetName)
  core.debug(`Retrieved ${pods.items.length}`)
  await Promise.all(pods.items.map(async (pod) => {
    core.debug(`Running script by grpc in pod ${pod.metadata?.name}`)
    return await runScriptByGrpc(
      scriptContent,
      rootCertClientAndKey.caCertAndkey.cert,
      rootCertClientAndKey.clientCertAndKey.cert,
      rootCertClientAndKey.clientCertAndKey.privateKey,
      pod.status!!.podIP!!,
      GRPC_SCRIPT_EXECUTOR_PORT
    )
  }))
}

export async function runScriptStep(
  args: RunScriptStepArgs,
  state,
  responseFile
): Promise<void> {
  if (useScriptExecutor()) {
    try {
      return runScriptStepWithGRPC(args, state)    
    } catch (err) {
      core.debug(
        `Run script Executor through GRPC failed: ${JSON.stringify(err)}`
      )
      const message = (err as any)?.response?.body?.message || err
      throw new Error(`failed to run script step: ${message}`)
    }
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
