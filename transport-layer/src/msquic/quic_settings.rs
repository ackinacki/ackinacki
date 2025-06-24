use msquic::BufferRef;
use msquic::CertificatePkcs12;
use msquic::Configuration;
use msquic::Credential;
use msquic::CredentialConfig;
use msquic::CredentialFlags;
use msquic::Registration;
use msquic::ServerResumptionLevel;
use msquic::Settings;

use crate::msquic::pkcs12;
use crate::NetCredential;

pub enum ConfigFactory {
    Client,
    Server,
}

impl ConfigFactory {
    pub fn build(
        &self,
        registration: &Registration,
        alpn: &[&str],
        credential: &NetCredential,
    ) -> anyhow::Result<Configuration> {
        let alpn: Vec<BufferRef> = alpn.iter().map(|s| BufferRef::from(*s)).collect();
        let settings = self.build_settings();
        let credential = self.build_credential(credential)?;
        let config = Configuration::open(registration, &alpn, Some(&settings))?;
        config.load_credential(&credential)?;
        Ok(config)
    }

    pub(crate) fn build_settings(&self) -> Settings {
        Settings::new()
            .set_ServerResumptionLevel(ServerResumptionLevel::ResumeAndZerortt)
            .set_PeerBidiStreamCount(0)
            .set_PeerUnidiStreamCount(1300)
            .set_InitialRttMs(2)
            .set_IdleTimeoutMs(0)
            .set_KeepAliveIntervalMs(500)
            .set_MaxAckDelayMs(1)

        // .set_DatagramReceiveEnabled()
        // .set_StreamMultiReceiveEnabled()
        // .set_StreamRecvWindowDefault(500 * 1024)
        // .set_PacingEnabled()
        // .set_MaxWorkerQueueDelayUs(1000)
        // .set_StreamRecvBufferDefault(330 * 1024)
        // .set_MaxStatelessOperations(1000)
        // .set_InitialWindowPackets(10000)
        // set_StreamRecvWindowUnidiDefault(500 * 1024)
    }

    pub(crate) fn build_credential(
        &self,
        credential: &NetCredential,
    ) -> anyhow::Result<CredentialConfig> {
        let pkcs12 = build_pkcs12(credential)?;
        let cred = Credential::CertificatePkcs12(CertificatePkcs12::new(pkcs12, None));

        let cred_flags = match self {
            Self::Client => {
                CredentialFlags::NO_CERTIFICATE_VALIDATION
                    | CredentialFlags::CLIENT
                    | CredentialFlags::INDICATE_CERTIFICATE_RECEIVED
                    | CredentialFlags::USE_PORTABLE_CERTIFICATES
            }
            Self::Server => {
                CredentialFlags::REQUIRE_CLIENT_AUTHENTICATION
                    | CredentialFlags::NO_CERTIFICATE_VALIDATION
                    | CredentialFlags::INDICATE_CERTIFICATE_RECEIVED
                    | CredentialFlags::USE_PORTABLE_CERTIFICATES
            }
        };
        Ok(CredentialConfig::new().set_credential_flags(cred_flags).set_credential(cred))
    }
}

fn build_pkcs12(credential: &NetCredential) -> anyhow::Result<Vec<u8>> {
    let p12_der = pkcs12::Pfx::new(
        credential.my_certs[0].as_ref(),
        credential.my_key.secret_der(),
        None,
        "",
        "",
    )
    .ok_or_else(|| anyhow::anyhow!("Failed to build PFX"))?
    .to_der();
    Ok(p12_der)
}
