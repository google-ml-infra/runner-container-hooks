/* eslint-disable @typescript-eslint/no-unused-vars */
import * as fs from 'fs'
import * as core from '@actions/core'

import { RunScriptStepArgs } from 'hooklib'
import { checkIfPvcExist, clonePVCReadOnlyManyFromExistingPVC, cpToPod, createJobSet, createK8sPod, createPod, execPodStep, getJobSet, getPod, getPodPhase, getPodsFromJobSet, getPodStatus, getPrepareJobTimeoutSeconds, getRootCertClientCertAndKey, waitForPodPhases } from '../k8s'
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
  core.info('read dir files')
  const files = fs.readdirSync('/home/runner/_work/_temp/');
  core.info('Files in current directory (synchronous):' + files);
  const more_files = fs.readdirSync('/home/runner/_work/_temp/_runner_file_commands');
  core.info('Files in current directory (synchronous):' + more_files);

  core.info("script content is " + scriptContent)

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

    core.info('waiting for jobset pod to come online')
    // TODO(quoct): Make a wait for up to 60 seconds here with a loop?
    core.info('sleep for 5 seconds to wait for pods creation')
    await sleep(5000)
    const pods = await getPodsFromJobSet(jobSetName)
    core.info(`pods items are ${pods.items}`)

    await Promise.all((pods.items.map(async (pod) => {
      try {
        core.info(`waiting for pod ${pod.metadata?.name} to come online`)
        await waitForPodPhases(
          pod.metadata!!.name!!,
          new Set([PodPhase.RUNNING]),
          new Set([PodPhase.PENDING]),
          getPrepareJobTimeoutSeconds()
        )
        core.info(`pod phase is now ${await getPodPhase(pod.metadata!!.name!!)}`)
      } catch (err) {
        throw new Error(`pod from job set failed to come online with error: ${err}`)
      }
    })))
    core.info('pods from jobset are now online')
  }

  const rootCertClientAndKey = await getRootCertClientCertAndKey()
  /*
  core.debug('successfully retrieved root cert, client and key')
  await runScriptByGrpc(
    scriptContent,
    rootCertClientAndKey.caCertAndkey.cert,
    rootCertClientAndKey.clientCertAndKey.cert,
    rootCertClientAndKey.clientCertAndKey.privateKey,
    status.podIP,
    GRPC_SCRIPT_EXECUTOR_PORT
  )
  */

  core.debug('Retrieving job set pods')
  const pods = await getPodsFromJobSet(jobSetName)
  core.debug(`Retrieved ${pods.items.length}`)
  try {
    await Promise.all(pods.items.map(async (pod) => {
      try {
        core.debug(`Running script by grpc in pod ${pod.metadata?.name}`)
        core.info('deleting _temp folder')
        await runScriptByGrpc(
          "rm -rf /__w/_temp/*",
          rootCertClientAndKey.caCertAndkey.cert,
          rootCertClientAndKey.clientCertAndKey.cert,
          rootCertClientAndKey.clientCertAndKey.privateKey,
          pod.status!!.podIP!!,
          GRPC_SCRIPT_EXECUTOR_PORT
        )
        core.info('copying temp folder')
        cpToPod(pod.metadata!!.name!!, JOB_CONTAINER_NAME, "/home/runner/_work/_temp", "/__w/_temp")
        core.info('done copying temp folder')
        return await runScriptByGrpc(
          scriptContent,
          rootCertClientAndKey.caCertAndkey.cert,
          rootCertClientAndKey.clientCertAndKey.cert,
          rootCertClientAndKey.clientCertAndKey.privateKey,
          pod.status!!.podIP!!,
          GRPC_SCRIPT_EXECUTOR_PORT
        )  
      } catch(error) {
        core.info(`error while execing the pod in the jobset ${error}`)
        await sleep(600000)
      }
    }))  
  } catch (error) {
    core.info("error execing waiting for debug " + error)
    await sleep(600000)
  }
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
