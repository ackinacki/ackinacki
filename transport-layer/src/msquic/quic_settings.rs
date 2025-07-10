use msquic::BufferRef;
use msquic::CertificatePkcs12;
use msquic::Configuration;
use msquic::Credential;
use msquic::CredentialConfig;
use msquic::CredentialFlags;
use msquic::Registration;
use msquic::ServerResumptionLevel;
use msquic::Settings;

use crate::tls::build_pkcs12;
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
            .set_PeerUnidiStreamCount(1024)
            .set_InitialRttMs(2)
            .set_IdleTimeoutMs(0)
            .set_KeepAliveIntervalMs(500)
            .set_MaxAckDelayMs(1)
            .set_SendIdleTimeoutMs(0)
            .set_InitialWindowPackets(100)
            .set_StreamRecvWindowDefault(268_435_456)
            .set_ConnFlowControlWindow(2_147_483_648)
            .set_SendBufferingEnabled()
    }

    pub(crate) fn build_credential(
        &self,
        credential: &NetCredential,
    ) -> anyhow::Result<CredentialConfig> {
        let pkcs12 = build_pkcs12(credential)?;
        let cred = Credential::CertificatePkcs12(CertificatePkcs12::new(pkcs12, None));
        let mut cred_flags = CredentialFlags::INDICATE_CERTIFICATE_RECEIVED
            | CredentialFlags::USE_PORTABLE_CERTIFICATES;
        cred_flags |= CredentialFlags::NO_CERTIFICATE_VALIDATION;
        match self {
            Self::Client => cred_flags |= CredentialFlags::CLIENT,
            Self::Server => cred_flags |= CredentialFlags::REQUIRE_CLIENT_AUTHENTICATION,
        }
        Ok(CredentialConfig::new().set_credential_flags(cred_flags).set_credential(cred))
    }
}
