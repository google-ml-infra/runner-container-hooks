/* eslint-disable @typescript-eslint/no-unused-vars */
import * as fs from 'fs'
import * as core from '@actions/core'

import { RunScriptStepArgs } from 'hooklib'
import { clonePersistentVolume, createK8sPod, createPod, execPodStep, getPod, getPodStatus, getRootCertClientCertAndKey } from '../k8s'
import {
  fixArgs,
  runScriptByGrpc,
  useScriptExecutor,
  writeEntryPointScript
} from '../k8s/utils'
import { GRPC_SCRIPT_EXECUTOR_PORT, JOB_CONTAINER_NAME } from './constants'

export async function runScriptStep(
  args: RunScriptStepArgs,
  state,
  responseFile
): Promise<void> {
  const { entryPoint, entryPointArgs, environmentVariables } = args
  const { containerPath, runnerPath } = writeEntryPointScript(
    args.workingDirectory,
    entryPoint,
    entryPointArgs,
    args.prependPath,
    environmentVariables
  )

  let createdQuoctPod;
  try {
   createdQuoctPod = await getPod("quoct-post-test")
  } catch (error) {
    core.debug("Error getting quoct pod " + error)
  }
  if (!createdQuoctPod) {
    core.debug("Could not find quoct pod")
    core.debug("clone persistent volume for test")
    await clonePersistentVolume("quoct-post-test")
    core.debug("creating pod helper")
  
    const createdPod = await getPod(state.jobPod)
    if (!createPod) {
      core.debug("Cannot find " + state.podName)
      throw new Error("cannot find created pod")
    }
    const previousPodMetadata = createdPod!!.metadata
    createdPod!!.metadata = {
      name: "quoct-post-test",
      namespace: previousPodMetadata!!.namespace,
      annotations: previousPodMetadata!!.annotations,
      labels: previousPodMetadata!!.labels,
    }

    createdPod!!.spec!!.nodeName = ""
    core.debug(`volume are ${JSON.stringify(createdPod!!.spec!!.volumes!!)}`)
  
    const volume = createdPod!!.spec!!.volumes!!.find(vol => vol.name === 'work')
    core.debug(`volume is ${JSON.stringify(volume)}`)
    volume!!.persistentVolumeClaim = {
      claimName: "quoct-post-test"
    }
    core.debug(`volumes are now ${JSON.stringify(createdPod!!.spec!!.volumes!!)}`)
    const newPod = await createK8sPod(createdPod!!)
    core.debug(`Created new pod ${JSON.stringify(newPod)}`)
  } else {
    core.debug("Found quoct pod " + JSON.stringify(createdQuoctPod))
  }

  args.entryPoint = 'sh'
  args.entryPointArgs = ['-e', containerPath]
  const podName = state.jobPod
  try {
    if (useScriptExecutor()) {
      core.info('using script executor')
      const command = fixArgs([args.entryPoint, ...args.entryPointArgs]).join(
        ' '
      )
      core.debug(`exec command ${command}`)

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
        command,
        rootCertClientAndKey.caCertAndkey.cert,
        rootCertClientAndKey.clientCertAndKey.cert,
        rootCertClientAndKey.clientCertAndKey.privateKey,
        status.podIP,
        GRPC_SCRIPT_EXECUTOR_PORT
      )
    } else {
      core.info('using exec pod step')
      await execPodStep(
        [args.entryPoint, ...args.entryPointArgs],
        podName,
        JOB_CONTAINER_NAME
      )

      core.debug('execing into quoct pod')
      try {
        await execPodStep(
          [args.entryPoint, ...args.entryPointArgs],
          "quoct-post-test",
          JOB_CONTAINER_NAME
        )  
      } catch (err) {
        core.debug("Failed to exec pod step for quoct pod ")
        core.debug(`${err}`)
      }
    }
  } catch (err) {
    core.debug(`execPodStep failed: ${JSON.stringify(err)}`)
    const message = (err as any)?.response?.body?.message || err
    throw new Error(`failed to run script step: ${message}`)
  } finally {
    fs.rmSync(runnerPath)
  }
}
