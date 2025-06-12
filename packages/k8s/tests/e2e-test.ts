import * as fs from 'fs'
import {
  cleanupJob,
  prepareJob,
  runContainerStep,
  runScriptStep
} from '../src/hooks'
import { TestHelper } from './test-setup'
import * as k8s from '@kubernetes/client-node'
import { generateCerts } from '../src/k8s/certs'
import { exec, execSync } from 'child_process'
import { runScriptByGrpc } from '../dist/k8s/utils'

const kc = new k8s.KubeConfig()
kc.loadFromDefault()

const forward = new k8s.PortForward(kc)

jest.useRealTimers()

let testHelper: TestHelper

let prepareJobData: any

let prepareJobOutputFilePath: string
describe.skip('e2e', () => {
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
})

describe('script-executor', () => {
  it('should execute script successfully', async () => {
    const certs = generateCerts()
    if (!fs.existsSync('/certs')) {
      fs.mkdirSync('/certs')
    }

    fs.writeFileSync('/certs/ca.crt', certs.caCertAndkey.cert)
    fs.writeFileSync('/certs/server.crt', certs.serverCertAndKey.cert)
    fs.writeFileSync('/certs/server.key', certs.serverCertAndKey.privateKey)

    const result = execSync('npm install ml-velocity-script-executor', {
      cwd: '/tmp'
    })
    console.log(result.toString())

    const process = exec(
      'node /tmp/node_modules/ml-velocity-script-executor/dist/index.js'
    )
    process.stdout?.on('data', data => {
      console.log(`stdout: ${data}`)
    })

    process.stderr?.on('data', data => {
      console.log(`stderr: ${data}`)
    })

    process.on('close', code => {
      console.log(`child process exited with code ${code}`)
    })

    await runScriptByGrpc(
      'ls',
      certs.caCertAndkey.cert,
      certs.clientCertAndKey.cert,
      certs.clientCertAndKey.privateKey,
      'localhost'
    )
  })
})
