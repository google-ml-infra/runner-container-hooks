// eslint-disable-next-line import/no-commonjs
module.exports = {
  clearMocks: true,
  moduleFileExtensions: ['js', 'ts'],
  testEnvironment: 'node',
  testMatch: ['**/*-test.ts'],
  testRunner: 'jest-circus/runner',
  transformIgnorePatterns: ['/node_modules/(?!(@kubernetes/client-node|openid-client|oauth4webapi))'],
  transform: {
    "^.+\\.[tj]s$": "babel-jest",
  },
  setupFilesAfterEnv: ['./jest.setup.js'],
  verbose: true
}
