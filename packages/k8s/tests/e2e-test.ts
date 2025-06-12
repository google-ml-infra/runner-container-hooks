import * as fs from 'fs'
import {
  cleanupJob,
  prepareJob,
  runContainerStep,
  runScriptStep
} from '../src/hooks'
import { TestHelper } from './test-setup'

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
