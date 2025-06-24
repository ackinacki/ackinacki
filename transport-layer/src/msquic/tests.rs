use msquic::CertificateFile;
use msquic::Credential;
use tracing_subscriber::EnvFilter;

static LOG_INIT: once_cell::sync::OnceCell<()> = once_cell::sync::OnceCell::new();

fn init_logs() {
    LOG_INIT.get_or_init(|| {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_test_writer()
            .init();
    });
}

#[cfg(test)]
pub(crate) fn get_test_cred() -> Credential {
    #[allow(clippy::disallowed_methods)]
    let cert_dir = std::env::temp_dir().join("msquic_test_rs");
    let key = "key.pem";
    let cert = "cert.pem";
    let key_path = cert_dir.join(key);
    let cert_path = cert_dir.join(cert);
    if !key_path.exists() || !cert_path.exists() {
        // remove the dir
        let _ = std::fs::remove_dir_all(&cert_dir);
        std::fs::create_dir_all(&cert_dir).expect("cannot create cert dir");
        // generate test cert using openssl cli
        let output = std::process::Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:4096",
                "-keyout",
                "key.pem",
                "-out",
                "cert.pem",
                "-sha256",
                "-days",
                "3650",
                "-nodes",
                "-subj",
                "/CN=localhost",
            ])
            .current_dir(cert_dir)
            .stderr(std::process::Stdio::inherit())
            .stdout(std::process::Stdio::inherit())
            .output()
            .expect("cannot generate cert");
        if !output.status.success() {
            panic!("generate cert failed");
        }
    }
    Credential::CertificateFile(CertificateFile::new(
        key_path.display().to_string(),
        cert_path.display().to_string(),
    ))
}

#[cfg(test)]
mod unit_tests {
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;
    use std::vec;

    use bytes::Bytes;
    use futures::AsyncWriteExt;
    use msquic::BufferRef;
    use msquic::Configuration;
    use msquic::Credential;
    use msquic::CredentialConfig;
    use msquic::CredentialFlags;
    use msquic::Registration;
    use msquic::RegistrationConfig;
    use rustls_pki_types::PrivateKeyDer;
    use rustls_pki_types::PrivatePkcs8KeyDer;
    use tokio::io::AsyncRead;
    use tokio::io::AsyncReadExt;
    use tokio::io::ReadBuf;
    use tokio::task::JoinSet;

    use super::*;
    use crate::msquic::msquic_async;
    use crate::msquic::quic_settings::ConfigFactory;
    use crate::msquic::read_message_from_stream;
    use crate::msquic::MsQuicTransport;
    use crate::NetConnection;
    use crate::NetCredential;
    use crate::NetIncomingRequest;
    use crate::NetListener;
    use crate::NetRecvRequest;
    use crate::NetTransport;

    #[tokio::test]
    #[ignore]
    async fn test_msquic_transport() {
        init_logs();
        let server_cred = NetCredential::generate_self_signed();
        let client_cred = NetCredential::generate_self_signed();
        let server_cred_clone = server_cred.clone();
        let client_cred_clone = client_cred.clone();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let transport = MsQuicTransport::new();
            let server_addr = SocketAddr::from(([127, 0, 0, 1], 5555));
            let listener = transport
                .create_listener(server_addr, &["yy1", "zzz", "bbb", "ccc"], server_cred_clone)
                .await
                .unwrap();

            let incoming_request = listener.accept().await.unwrap();
            let connection = incoming_request.accept().await.unwrap();
            let message = connection.accept_recv().await.unwrap().recv().await.unwrap();
            assert_eq!(client_cred_clone.identity(), connection.remote_identity());
            assert_eq!(message.len(), 5);
            println!("{:?}", message);
        });

        let server_cred_clone = server_cred.clone();
        let client_cred_clone = client_cred.clone();
        tasks.spawn(async move {
            let server_addr = SocketAddr::from(([127, 0, 0, 1], 5555));
            let transport = MsQuicTransport::new();
            let connection = transport
                .connect(server_addr, &["xxx", "yyy", "bbb", "ccc"], client_cred_clone)
                .await
                .unwrap();

            // Check negotiated alpn
            assert_eq!(connection.alpn_negotiated(), Some("bbb".to_string()));
            assert_eq!(connection.remote_identity(), server_cred_clone.identity());
            connection.send("hello".as_bytes()).await.unwrap();
        });
        tasks.join_all().await;
    }

    #[tokio::test]
    async fn test_example_raw_msquick_async() {
        let cred: Credential = get_test_cred();
        let reg = Registration::new(&RegistrationConfig::default()).unwrap();
        let alpn: [BufferRef; 1] = [BufferRef::from("qtest")];
        let settings = ConfigFactory::Server.build_settings();

        let config = Configuration::open(&reg, &alpn, Some(&settings)).unwrap();
        let cred_config = CredentialConfig::new()
            .set_credential_flags(CredentialFlags::NO_CERTIFICATE_VALIDATION)
            .set_credential(cred);

        config.load_credential(&cred_config).unwrap();

        let address = "127.0.0.1";
        let port = 4444;
        let l = msquic_async::Listener::new(&reg, config).unwrap();
        let local_address = SocketAddr::new(address.parse().unwrap(), port);

        l.start(&alpn, Some(local_address)).unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let _ = std::thread::Builder::new().spawn(|| {
            let reg = Registration::new(&RegistrationConfig::default()).unwrap();
            let alpn = [BufferRef::from("qtest")];
            let settings = ConfigFactory::Client.build_settings();
            let configuration = Configuration::open(&reg, &alpn, Some(&settings)).unwrap();

            let cred_config = CredentialConfig::new_client()
                .set_credential_flags(CredentialFlags::NO_CERTIFICATE_VALIDATION);

            configuration.load_credential(&cred_config).unwrap();

            let host = "127.0.0.1";
            let port = 4444;
            let conn = msquic_async::Connection::new(&reg).unwrap();
            let rt = tokio::runtime::Runtime::new().unwrap();

            rt.block_on(async {
                conn.start(&configuration, host, port).await.unwrap();
                let mut stream = conn
                    .open_outbound_stream(msquic_async::StreamType::Unidirectional, false)
                    .await
                    .unwrap();
                stream.write_all("hello".as_bytes()).await.unwrap();
                stream.flush().await.unwrap();
                stream.close().await.unwrap();
            });
        });

        let conn = l.accept().await.unwrap();
        let mut stream = conn.accept_inbound_uni_stream().await.unwrap();
        let mut buf = [0u8; 5];
        let len = stream.read_exact(&mut buf).await.unwrap();
        let received_str = String::from_utf8_lossy(&buf[0..len]);
        println!("Received: {}", received_str);
        assert_eq!(received_str, "hello");
    }

    // Tests of read_message_from_stream function
    //
    // Test structure that implements AsyncRead
    struct MockAsyncReader {
        chunks: Vec<Bytes>,
        current: usize,
        offset: usize,
    }

    impl MockAsyncReader {
        fn new(chunks: Vec<Bytes>) -> Self {
            Self { chunks, current: 0, offset: 0 }
        }
    }

    impl AsyncRead for MockAsyncReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.current >= self.chunks.len() {
                return Poll::Ready(Ok(()));
            }

            let chunk = &self.chunks[self.current];
            let len = chunk.len();
            let to_copy = std::cmp::min(len - self.offset, buf.remaining());
            buf.put_slice(&chunk[self.offset..self.offset + to_copy]);
            self.offset += to_copy;
            if self.offset >= len {
                self.current += 1;
                self.offset = 0;
            }
            Poll::Ready(Ok(()))
        }
    }

    // Helper function to create a message with given payload
    fn create_transport_message(payload: &[u8]) -> Vec<u8> {
        let mut message = Vec::new();
        message.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        message.extend_from_slice(payload);
        message
    }

    #[tokio::test]
    async fn test_read_complete_message_in_one_chunk() -> anyhow::Result<()> {
        let payload = b"hello";
        let message = create_transport_message(payload);
        let mut reader = MockAsyncReader::new(vec![Bytes::from(message.clone())]);

        let result = read_message_from_stream(&mut reader).await?;

        assert_eq!(result, payload);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_message_in_multiple_chunks() -> anyhow::Result<()> {
        let payload = b"this is a longer message that will be split into multiple chunks";
        let message = create_transport_message(payload);

        let chunk1 = Bytes::from(message[0..2].to_vec());
        let chunk2 = Bytes::from(message[2..6].to_vec());
        let chunk3 = Bytes::from(message[6..].to_vec());

        let mut reader = MockAsyncReader::new(vec![chunk1, chunk2, chunk3]);

        let result = read_message_from_stream(&mut reader).await?;

        assert_eq!(result, payload);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_empty_message() -> anyhow::Result<()> {
        // Create a message with an empty payload
        let payload = b"";
        let message = create_transport_message(payload);

        let mut reader = MockAsyncReader::new(vec![Bytes::from(message.clone())]);
        let result = read_message_from_stream(&mut reader).await?;

        assert_eq!(result, payload);
        Ok(())
    }

    #[tokio::test]
    async fn test_zero_reads() -> anyhow::Result<()> {
        let payload = b"test zero reads";
        let message = create_transport_message(payload);

        // Create a mock reader that sometimes returns 0 bytes read
        struct ZeroReader {
            data: Vec<u8>,
            position: usize,
            zero_read_counter: usize,
        }

        impl AsyncRead for ZeroReader {
            fn poll_read(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                if self.zero_read_counter > 0 {
                    self.zero_read_counter -= 1;
                    return Poll::Ready(Ok(()));
                }

                if self.position >= self.data.len() {
                    return Poll::Ready(Ok(()));
                }

                let remaining = &self.data[self.position..];
                let to_copy = std::cmp::min(remaining.len(), buf.remaining());
                buf.put_slice(&remaining[..to_copy]);
                self.position += to_copy;

                // Set up to return 0 bytes on next read
                self.zero_read_counter = 1;

                Poll::Ready(Ok(()))
            }
        }

        let mut reader = ZeroReader {
            data: message.clone(),
            position: 0,
            zero_read_counter: 1, // Start with a zero read
        };

        let result = read_message_from_stream(&mut reader).await?;

        assert_eq!(result, payload);
        Ok(())
    }

    #[test]
    fn test_cert() {
        let key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert = key.cert.der().clone();
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.key_pair.serialize_der()));
        let creds = NetCredential { my_key: key, my_certs: vec![cert], root_certs: vec![] };
        let reg = Registration::new(&RegistrationConfig::default()).unwrap();
        let alpn = ["qtest"];
        let _cred_config = ConfigFactory::Server.build(&reg, &alpn, &creds).unwrap();

        println!("ok");
    }
}
