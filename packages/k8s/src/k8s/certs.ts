import * as forge from 'node-forge'
import * as crypto from 'crypto'

interface CertAndKeyPairs {
  cert: forge.pki.Certificate
  keyPairs: forge.pki.rsa.KeyPair
}

interface CertAndKeys {
  cert: string
  privateKey: string
  publicKey: string
}

export interface MTLSCertAndPrivateKey {
  caCertAndkey: CertAndKeys
  serverCertAndKey: CertAndKeys
  clientCertAndKey: CertAndKeys
}

export enum CertCommonName {
  ROOT = 'root',
  SERVER = 'server',
  CLIENT = 'client'
}

export function generateCerts(): MTLSCertAndPrivateKey {
  const caKeyAndCert = generateCert(7, CertCommonName.ROOT, CertCommonName.ROOT)

  // Self-sign CA certificate.
  caKeyAndCert.cert.sign(
    caKeyAndCert.keyPairs.privateKey,
    forge.md.sha256.create()
  )

  // Server certificate.
  const serverKeyAndCert = generateCert(
    7,
    CertCommonName.ROOT,
    CertCommonName.SERVER
  )

  serverKeyAndCert.cert.sign(
    caKeyAndCert.keyPairs.privateKey,
    forge.md.sha256.create()
  )

  // Client certificate.
  const clientKeyAndCert = generateCert(
    7,
    CertCommonName.ROOT,
    CertCommonName.CLIENT
  )
  clientKeyAndCert.cert.sign(
    caKeyAndCert.keyPairs.privateKey,
    forge.md.sha256.create()
  )

  const caPem = forge.pki.certificateToPem(caKeyAndCert.cert)
  const caPublicKeyPem = forge.pki.publicKeyToPem(
    caKeyAndCert.keyPairs.publicKey
  )
  const serverPem = forge.pki.certificateToPem(serverKeyAndCert.cert)
  const serverKeyPem = forge.pki.privateKeyToPem(
    serverKeyAndCert.keyPairs.privateKey
  )
  const serverPublicKeyPem = forge.pki.publicKeyToPem(
    serverKeyAndCert.keyPairs.publicKey
  )
  const clientPem = forge.pki.certificateToPem(clientKeyAndCert.cert)
  const clientKeyPem = forge.pki.privateKeyToPem(
    clientKeyAndCert.keyPairs.privateKey
  )
  const clientPublicKeyPem = forge.pki.publicKeyToPem(
    clientKeyAndCert.keyPairs.publicKey
  )

  // We do not need to store to private key for the self-signed CA certificate.
  return {
    caCertAndkey: { cert: caPem, privateKey: '', publicKey: caPublicKeyPem },
    serverCertAndKey: {
      cert: serverPem,
      privateKey: serverKeyPem,
      publicKey: serverPublicKeyPem
    },
    clientCertAndKey: {
      cert: clientPem,
      privateKey: clientKeyPem,
      publicKey: clientPublicKeyPem
    }
  }
}

export function generateCert(
  validityDays: number,
  issuerName: CertCommonName,
  subjectName: CertCommonName
): CertAndKeyPairs {
  const keyPairs = forge.pki.rsa.generateKeyPair(2048)
  const cert = forge.pki.createCertificate()
  cert.publicKey = keyPairs.publicKey

  cert.serialNumber = crypto.randomBytes(19).toString('hex')
  cert.validity.notBefore = new Date()
  cert.validity.notAfter = new Date(
    new Date().getTime() + 1000 * 60 * 60 * 24 * (validityDays ?? 1)
  )
  const attributes: forge.pki.CertificateField[] = [
    {
      name: 'countryName',
      value: 'US'
    },
    {
      shortName: 'ST',
      value: 'California'
    }
  ]
  cert.setSubject(
    attributes.concat([{ name: 'commonName', value: subjectName }])
  )
  cert.setIssuer(attributes.concat([{ name: 'commonName', value: issuerName }]))

  // Needed because the GRPC client and server will use localhost address.
  cert.setExtensions([
    {
      name: 'basicConstraints',
      cA: subjectName === CertCommonName.ROOT ? true : false
    },
    {
      name: 'keyUsage',
      keyCertSign: true,
      digitalSignature: true,
      nonRepudiation: true,
      keyEncipherment: true,
      dataEncipherment: true
    },
    // Needed because the GRPC client and server will use localhost address.
    {
      name: 'subjectAltName',
      altNames: [
        { type: 2, value: 'localhost' }, // DNS name
        { type: 7, ip: '127.0.0.1' }, // IP address
        { type: 7, ip: '0.0.0.0' } // IP address
      ]
    }
  ])

  if (subjectName === CertCommonName.SERVER) {
    cert.extensions.push({
      name: 'extKeyUsage',
      serverAuth: true
    })
  }

  if (subjectName === CertCommonName.CLIENT) {
    cert.extensions.push({
      name: 'extKeyUsage',
      clientAuth: true
    })
  }

  return {
    keyPairs,
    cert
  }
}
