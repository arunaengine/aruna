//! End-to-end tests of the listener: the RFC 9289 probe and TLS 1.3 upgrade, `sunrpc` ALPN,
//! SPKI pinning with proof of possession, revocation of live connections and the drain handle.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    date_time_ymd,
};
use rustls::client::ResolvesClientCert;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::sign::CertifiedKey;
use rustls::{ClientConfig, RootCertStore, SignatureScheme};
use tokio::io::AsyncWriteExt;
use tokio_rustls::TlsConnector;
use ulid::Ulid;

use super::*;
use crate::backend::Revocation;
use crate::mock::MockBackend;
use crate::rpc::{AuthSys, encode_call, parse_reply};

/// Bound for every network step, so a broken server fails the test instead of hanging it.
const STEP: Duration = Duration::from_secs(10);
const SERVER_NAME: &str = "nfs.test";

/// A test authority and a server certificate for `SERVER_NAME` signed by it.
fn server_tls(loopback_cleartext: bool) -> (NfsTlsConfig, String) {
    let ca_key = KeyPair::generate().expect("CA key");
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.not_before = date_time_ymd(2020, 1, 1);
    ca_params.not_after = date_time_ymd(2100, 1, 1);
    let ca_pem = ca_params
        .self_signed(&ca_key)
        .expect("CA certificate")
        .pem();
    let issuer = Issuer::new(ca_params, ca_key);
    let leaf_key = KeyPair::generate().expect("leaf key");
    let mut leaf = CertificateParams::new(vec![SERVER_NAME.to_string()]).expect("leaf params");
    leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    leaf.not_before = date_time_ymd(2020, 1, 1);
    leaf.not_after = date_time_ymd(2100, 1, 1);
    let cert_pem = leaf.signed_by(&leaf_key, &issuer).expect("leaf").pem();
    let tls = NfsTlsConfig {
        cert_pem: cert_pem.into_bytes(),
        key_pem: leaf_key.serialize_pem().into_bytes(),
        loopback_cleartext,
    };
    (tls, ca_pem)
}

/// A self-signed client certificate and its key; enrollment pins the key, not a chain.
fn client_identity() -> (CertificateDer<'static>, KeyPair) {
    let key = KeyPair::generate().expect("client key");
    let params = CertificateParams::new(vec!["client.test".to_string()]).expect("params");
    let certificate = params.self_signed(&key).expect("client certificate");
    (certificate.der().clone(), key)
}

fn private_key(key: &KeyPair) -> PrivateKeyDer<'static> {
    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der()))
}

/// Presents an enrolled certificate but signs with another key.
#[derive(Debug)]
struct ForeignKey(Arc<CertifiedKey>);

impl ResolvesClientCert for ForeignKey {
    fn resolve(&self, _hints: &[&[u8]], _schemes: &[SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        Some(self.0.clone())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

enum Identity {
    Key(CertificateDer<'static>, PrivateKeyDer<'static>),
    Foreign(CertificateDer<'static>, Box<KeyPair>),
}

fn client_config(ca_pem: &str, identity: Identity, alpn: bool) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
        roots.add(certificate.expect("CA PEM")).expect("CA root");
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("TLS 1.3")
        .with_root_certificates(roots);
    let mut config = match identity {
        Identity::Key(certificate, key) => builder
            .with_client_auth_cert(vec![certificate], key)
            .expect("client certificate"),
        Identity::Foreign(certificate, other) => {
            let signer = provider
                .key_provider
                .load_private_key(private_key(&other))
                .expect("signing key");
            let certified = CertifiedKey::new(vec![certificate], signer);
            builder.with_client_cert_resolver(Arc::new(ForeignKey(Arc::new(certified))))
        }
    };
    if alpn {
        config.alpn_protocols = vec![SUNRPC_ALPN.to_vec()];
    }
    Arc::new(config)
}

fn sys() -> RpcCred {
    RpcCred::Sys(AuthSys {
        stamp: 1,
        machine: "client".to_string(),
        uid: 1000,
        gid: 1000,
        groups: Vec::new(),
    })
}

struct Running {
    backend: Arc<MockBackend>,
    handle: NfsServerHandle,
    address: SocketAddr,
    ca_pem: String,
    certificate: CertificateDer<'static>,
    key: KeyPair,
}

/// A server whose mock backend enrolls one client key, listening on a free loopback port.
async fn start(loopback_cleartext: bool) -> Running {
    let (tls, ca_pem) = server_tls(loopback_cleartext);
    let (certificate, key) = client_identity();
    let pin = fingerprint(&certificate).expect("pin");
    let (backend, _) = MockBackend::with_host(pin);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let config = NfsServerConfig::new(address, tls, [6; 32]);
    let server = NfsServer::new(config, backend.clone(), Arc::new(RateLimits::default()));
    let handle = server
        .run_with_listener(listener, CancellationToken::new())
        .expect("server");
    Running {
        backend,
        handle,
        address,
        ca_pem,
        certificate,
        key,
    }
}

/// Opens TCP, sends the AUTH_TLS NULL probe and checks the STARTTLS answer.
async fn probe(address: SocketAddr) -> TcpStream {
    let mut tcp = TcpStream::connect(address).await.expect("connect");
    let call = encode_call(1, NULL_PROCEDURE, &RpcCred::Tls, &[]);
    write_record(&mut tcp, &call).await.expect("probe");
    let record = tokio::time::timeout(STEP, read_record(&mut tcp, 4096))
        .await
        .expect("probe answer in time")
        .expect("probe answer")
        .expect("a record");
    let (xid, accepted, verifier, stat, _) = parse_reply(record);
    assert_eq!((xid, accepted, stat), (1, true, 0));
    assert_eq!(&verifier[..], b"STARTTLS");
    tcp
}

async fn upgrade(
    tcp: TcpStream,
    config: Arc<ClientConfig>,
) -> std::io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let name = ServerName::try_from(SERVER_NAME).expect("server name");
    tokio::time::timeout(STEP, TlsConnector::from(config).connect(name, tcp))
        .await
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))?
}

/// Sends one call and returns its reply record, or `None` once the server closed the stream.
async fn call<S>(stream: &mut S, xid: u32, procedure: u32, body: &[u8]) -> Option<Bytes>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_record(stream, &encode_call(xid, procedure, &sys(), body))
        .await
        .ok()?;
    let read = tokio::time::timeout(STEP, read_record(stream, 1 << 20))
        .await
        .expect("an answer or a close in time");
    read.ok().flatten()
}

/// An empty minor version 1 COMPOUND.
fn empty_compound() -> Vec<u8> {
    vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]
}

#[tokio::test]
async fn starttls_serves_rpc() {
    let running = start(false).await;
    let tcp = probe(running.address).await;
    let config = client_config(
        &running.ca_pem,
        Identity::Key(running.certificate.clone(), private_key(&running.key)),
        true,
    );
    let mut stream = upgrade(tcp, config).await.expect("TLS");
    let (_, connection) = stream.get_ref();
    assert_eq!(connection.alpn_protocol(), Some(SUNRPC_ALPN));
    let reply = call(&mut stream, 2, NULL_PROCEDURE, &[])
        .await
        .expect("NULL reply");
    let (xid, accepted, _, stat, _) = parse_reply(reply);
    assert_eq!((xid, accepted, stat), (2, true, 0));
    let reply = call(&mut stream, 3, COMPOUND_PROCEDURE, &empty_compound())
        .await
        .expect("COMPOUND reply");
    let (_, accepted, _, stat, mut body) = parse_reply(reply);
    assert_eq!((accepted, stat), (true, 0));
    assert_eq!(body.u32(), Ok(0), "NFS4_OK");
}

// rustls accepts a client without ALPN, so the server checks `sunrpc` itself and closes.
#[tokio::test]
async fn missing_alpn_closed() {
    let running = start(false).await;
    let tcp = probe(running.address).await;
    let config = client_config(
        &running.ca_pem,
        Identity::Key(running.certificate.clone(), private_key(&running.key)),
        false,
    );
    if let Ok(mut stream) = upgrade(tcp, config).await {
        assert!(call(&mut stream, 2, NULL_PROCEDURE, &[]).await.is_none());
    }
}

#[tokio::test]
async fn unenrolled_key_closed() {
    let running = start(false).await;
    let tcp = probe(running.address).await;
    let (certificate, key) = client_identity();
    let config = client_config(
        &running.ca_pem,
        Identity::Key(certificate, private_key(&key)),
        true,
    );
    if let Ok(mut stream) = upgrade(tcp, config).await {
        assert!(call(&mut stream, 2, NULL_PROCEDURE, &[]).await.is_none());
    }
}

// The enrolled certificate alone is not enough: the handshake signature must come from its key.
#[tokio::test]
async fn foreign_key_refused() {
    let running = start(false).await;
    let tcp = probe(running.address).await;
    let other = KeyPair::generate().expect("other key");
    let config = client_config(
        &running.ca_pem,
        Identity::Foreign(running.certificate.clone(), Box::new(other)),
        true,
    );
    if let Ok(mut stream) = upgrade(tcp, config).await {
        assert!(call(&mut stream, 2, NULL_PROCEDURE, &[]).await.is_none());
    }
}

// Without the probe a peer gets AUTH_TOOWEAK and the connection closes.
#[tokio::test]
async fn cleartext_refused() {
    let running = start(false).await;
    let mut tcp = TcpStream::connect(running.address).await.expect("connect");
    let reply = call(&mut tcp, 7, NULL_PROCEDURE, &[])
        .await
        .expect("denial");
    let (xid, accepted, _, stat, _) = parse_reply(reply);
    assert_eq!((xid, accepted), (7, false));
    assert_eq!(stat, (1 << 16) | AuthStat::TooWeak as u32);
    assert!(call(&mut tcp, 8, NULL_PROCEDURE, &[]).await.is_none());
}

#[tokio::test]
async fn loopback_cleartext_allowed() {
    let running = start(true).await;
    let host = running.backend.state.lock().hosts.values().next().copied();
    // A cleartext loopback connection proves no key.
    let host = host.map(|host| HostIdentity {
        fingerprint: None,
        ..host
    });
    running.backend.state.lock().loopback = host;
    let mut tcp = TcpStream::connect(running.address).await.expect("connect");
    let reply = call(&mut tcp, 4, NULL_PROCEDURE, &[]).await.expect("reply");
    let (xid, accepted, _, stat, _) = parse_reply(reply);
    assert_eq!((xid, accepted, stat), (4, true, 0));
}

// Revoking the host closes its established connection at once, without another request.
#[tokio::test]
async fn host_revocation_disconnects() {
    let running = start(false).await;
    let tcp = probe(running.address).await;
    let config = client_config(
        &running.ca_pem,
        Identity::Key(running.certificate.clone(), private_key(&running.key)),
        true,
    );
    let mut stream = upgrade(tcp, config).await.expect("TLS");
    assert!(call(&mut stream, 2, NULL_PROCEDURE, &[]).await.is_some());
    let host_id = running
        .backend
        .state
        .lock()
        .hosts
        .values()
        .next()
        .map(|host| host.host_id)
        .expect("host");
    running
        .backend
        .events
        .send(Revocation::Host(host_id))
        .expect("subscriber");
    let read = tokio::time::timeout(STEP, read_record(&mut stream, 1 << 20))
        .await
        .expect("closed in time");
    assert!(
        !matches!(read, Ok(Some(_))),
        "the revoked connection closed"
    );
}

// Revocations missed by a lagging listener close every connection, so a lost host revocation
// or key rotation never leaves one admitted.
#[tokio::test]
async fn lagged_revocations_disconnect() {
    let running = start(false).await;
    let tcp = probe(running.address).await;
    let config = client_config(
        &running.ca_pem,
        Identity::Key(running.certificate.clone(), private_key(&running.key)),
        true,
    );
    let mut stream = upgrade(tcp, config).await.expect("TLS");
    assert!(call(&mut stream, 2, NULL_PROCEDURE, &[]).await.is_some());
    // The listener runs only when this test awaits, so it misses all but the newest events.
    for mount in 0..64 {
        let revocation = Revocation::Mount(Ulid::from_parts(9, mount));
        running.backend.events.send(revocation).expect("subscriber");
    }
    let read = tokio::time::timeout(STEP, read_record(&mut stream, 1 << 20))
        .await
        .expect("closed in time");
    assert!(!matches!(read, Ok(Some(_))), "the connection closed");
}

// A declared record beyond the limit closes the connection before any of it is buffered.
#[tokio::test]
async fn oversized_record_closes() {
    let running = start(false).await;
    let tcp = probe(running.address).await;
    let config = client_config(
        &running.ca_pem,
        Identity::Key(running.certificate.clone(), private_key(&running.key)),
        true,
    );
    let mut stream = upgrade(tcp, config).await.expect("TLS");
    stream
        .write_all(&0xffff_fff0u32.to_be_bytes())
        .await
        .expect("header");
    stream.flush().await.expect("flush");
    let read = tokio::time::timeout(STEP, read_record(&mut stream, 1 << 20))
        .await
        .expect("closed in time");
    assert!(!matches!(read, Ok(Some(_))));
}

#[tokio::test]
async fn abort_releases_connections() {
    let mut running = start(false).await;
    let tcp = probe(running.address).await;
    let config = client_config(
        &running.ca_pem,
        Identity::Key(running.certificate.clone(), private_key(&running.key)),
        true,
    );
    let mut stream = upgrade(tcp, config).await.expect("TLS");
    assert!(call(&mut stream, 2, NULL_PROCEDURE, &[]).await.is_some());
    assert_eq!(running.handle.connection_count(), 1);
    running.handle.abort();
    tokio::time::timeout(STEP, running.handle.wait_until_released())
        .await
        .expect("released in time");
    assert!(running.handle.is_finished());
    assert_eq!(running.handle.connection_count(), 0);
}
