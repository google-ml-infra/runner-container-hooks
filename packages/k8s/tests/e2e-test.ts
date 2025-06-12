import * as fs from 'fs'
import {
  cleanupJob,
  prepareJob,
  runContainerStep,
  runScriptStep
} from '../src/hooks'
import { TestHelper } from './test-setup'
import * as k8s from '@kubernetes/client-node'
import { ReadableStreamBuffer, WritableStreamBuffer } from 'stream-buffers'
import { getPodByName } from '../src/k8s'

const kc = new k8s.KubeConfig()
kc.loadFromDefault()

const forward = new k8s.PortForward(kc)

jest.useRealTimers()

let testHelper: TestHelper

let prepareJobData: any

let prepareJobOutputFilePath: string
describe.only('e2e', () => {
  beforeEach(async () => {
    testHelper = new TestHelper()
    await testHelper.initialize()

    prepareJobData = testHelper.getPrepareJobDefinition()
    prepareJobOutputFilePath = testHelper.createFile('prepare-job-output.json')
  })
  afterEach(async () => {
    await testHelper.cleanup()
  })
  it('should prepare job, run script step, run container step then cleanup without errors', async () => {
    await expect(
      prepareJob(prepareJobData.args, prepareJobOutputFilePath)
    ).resolves.not.toThrow()

    const scriptStepData = testHelper.getRunScriptStepDefinition()

    const prepareJobOutputJson = fs.readFileSync(prepareJobOutputFilePath)
    const prepareJobOutputData = JSON.parse(prepareJobOutputJson.toString())

    await expect(
      runScriptStep(scriptStepData.args, prepareJobOutputData.state, null)
    ).resolves.not.toThrow()

    const runContainerStepData = testHelper.getRunContainerStepDefinition()

    await expect(
      runContainerStep(runContainerStepData.args)
    ).resolves.not.toThrow()

    await expect(cleanupJob()).resolves.not.toThrow()
  })

  it.only('should prepare job, run script step, run container step then cleanup without errors for script executor', async () => {
    process.env['ACTIONS_RUNNER_USE_SCRIPT_EXECUTOR'] = 'true'
    process.env['ACTIONS_RUNNER_SCRIPT_EXECUTOR_ENTRY_POINT'] = 'node'
    process.env['ACTIONS_RUNNER_SCRIPT_EXECUTOR_ARGS'] =
      '/script_executor/dist/index.js'
    try {
      await expect(
        prepareJob(prepareJobData.args, prepareJobOutputFilePath)
      ).resolves.not.toThrow()

      const content = JSON.parse(
        fs.readFileSync(prepareJobOutputFilePath).toString()
      )

      const pod = await getPodByName(content.state.jobPod)

      const isStream = new ReadableStreamBuffer()
      forward.portForward(
        pod.metadata!!.namespace!!,
        pod.metadata?.name!!,
        [50051],
        process.stdout,
        process.stderr,
        isStream
      )

      const scriptStepData = testHelper.getRunScriptStepDefinition()

      const prepareJobOutputJson = fs.readFileSync(prepareJobOutputFilePath)
      const prepareJobOutputData = JSON.parse(prepareJobOutputJson.toString())

      await expect(
        runScriptStep(scriptStepData.args, prepareJobOutputData.state, null)
      ).resolves.not.toThrow()

      const runContainerStepData = testHelper.getRunContainerStepDefinition()

      await expect(
        runContainerStep(runContainerStepData.args)
      ).resolves.not.toThrow()

      await expect(cleanupJob()).resolves.not.toThrow()
    } finally {
      process.env['ACTIONS_RUNNER_USE_SCRIPT_EXECUTOR'] = ''
    }
  })
})
