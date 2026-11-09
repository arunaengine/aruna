//! Tests native HTTPS: certificate checks and reload, routing, handshake limits and shutdown.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::middleware::from_fn_with_state;
use axum::routing::get;
use http::StatusCode;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    date_time_ymd,
};
use rustls::client::Resumption;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream as ClientStream;
use tokio_util::sync::CancellationToken;

use super::certificate::{CertificateError, CertificateStore, ReloadOutcome, check_pair};
use super::listener::{CombinedListener, HttpsHandle, RoutedListener, serve_routers};
use super::{HostCheck, HostRoute, HostRoutes, check_host};

/// 2026-01-01T00:00:00Z, inside every default validity window below.
const NOW: u64 = 1_767_225_600;
const DAY: u64 = 86_400;

pub(super) fn at(seconds: u64) -> UnixTime {
    UnixTime::since_unix_epoch(Duration::from_secs(seconds))
}

pub(super) fn routes() -> HostRoutes {
    let url = |value: &str| url::Url::parse(value).expect("test origin");
    HostRoutes::new(
        &url("https://api.test"),
        Some(&url("https://s3.test")),
        Some(&url("https://portal.test")),
    )
}

/// A test certificate authority that signs intermediates and leaves.
pub(super) struct Authority {
    pub(super) pem: String,
    issuer: Issuer<'static, KeyPair>,
}

/// A validity window in years, from the first January of `from` to that of `until`.
#[derive(Clone, Copy)]
pub(super) struct Window(pub(super) i32, pub(super) i32);

const VALID: Window = Window(2020, 2040);

fn ca_params(window: Window) -> CertificateParams {
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.not_before = date_time_ymd(window.0, 1, 1);
    params.not_after = date_time_ymd(window.1, 1, 1);
    params
}

impl Authority {
    pub(super) fn root() -> Self {
        let key = KeyPair::generate().expect("CA key");
        let params = ca_params(VALID);
        let pem = params.self_signed(&key).expect("CA certificate").pem();
        Self {
            pem,
            issuer: Issuer::new(params, key),
        }
    }

    fn intermediate(&self, window: Window) -> Self {
        let key = KeyPair::generate().expect("intermediate key");
        let params = ca_params(window);
        let pem = params
            .signed_by(&key, &self.issuer)
            .expect("intermediate certificate")
            .pem();
        Self {
            pem,
            issuer: Issuer::new(params, key),
        }
    }

    pub(super) fn leaf(&self, names: &[&str], window: Window, key: &KeyPair) -> String {
        leaf_params(names, window)
            .signed_by(key, &self.issuer)
            .expect("leaf certificate")
            .pem()
    }
}

fn leaf_params(names: &[&str], window: Window) -> CertificateParams {
    let names: Vec<String> = names.iter().map(ToString::to_string).collect();
    let mut params = CertificateParams::new(names).expect("leaf params");
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    params.not_before = date_time_ymd(window.0, 1, 1);
    params.not_after = date_time_ymd(window.1, 1, 1);
    params
}

pub(super) const NAMES: [&str; 4] = ["api.test", "s3.test", "*.s3.test", "portal.test"];

/// A leaf for every route host with its key, signed by `authority`.
pub(super) fn pair(authority: &Authority) -> (String, String) {
    let key = KeyPair::generate().expect("leaf key");
    (authority.leaf(&NAMES, VALID, &key), key.serialize_pem())
}

fn check(certificate: &str, key: &str, ca: &str) -> Result<(), CertificateError> {
    check_pair(
        certificate.as_bytes(),
        key.as_bytes(),
        Some(ca.as_bytes()),
        &routes(),
        at(NOW),
    )
    .map(|_| ())
}

/// The three PEM files of a store in a unique folder.
pub(super) struct Files {
    _dir: tempfile::TempDir,
    pub(super) certificate: PathBuf,
    pub(super) key: PathBuf,
    pub(super) ca: PathBuf,
}

impl Files {
    pub(super) fn new(certificate: &str, key: &str, ca: &str) -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let files = Self {
            certificate: dir.path().join("fullchain.pem"),
            key: dir.path().join("privkey.pem"),
            ca: dir.path().join("ca.pem"),
            _dir: dir,
        };
        files.write(&files.certificate, certificate);
        files.write(&files.key, key);
        files.write(&files.ca, ca);
        files
    }

    /// Replaces a file atomically: a temporary name, then a rename.
    pub(super) fn write(&self, path: &Path, content: &str) {
        let temporary = path.with_extension("new");
        std::fs::write(&temporary, content).expect("write PEM");
        std::fs::rename(&temporary, path).expect("rename PEM");
    }

    pub(super) fn store(&self, now: UnixTime) -> Result<CertificateStore, CertificateError> {
        CertificateStore::load(
            self.certificate.clone(),
            self.key.clone(),
            Some(self.ca.clone()),
            Arc::new(routes()),
            now,
        )
    }
}

fn leaf_der(store: &CertificateStore) -> Vec<u8> {
    store
        .active()
        .end_entity_cert()
        .expect("active leaf")
        .to_vec()
}

#[test]
fn good_pair_loads() {
    let root = Authority::root();
    let (certificate, key) = pair(&root);
    check(&certificate, &key, &root.pem).expect("a complete pair is accepted");
    let files = Files::new(&certificate, &key, &root.pem);
    assert!(files.store(at(NOW)).is_ok());
}

#[test]
fn refuses_bad_pem() {
    let root = Authority::root();
    let (certificate, key) = pair(&root);
    assert!(matches!(
        check("", &key, &root.pem),
        Err(CertificateError::NoCertificate {
            file: "certificate"
        })
    ));
    assert!(matches!(
        check(&certificate, "", &root.pem),
        Err(CertificateError::KeyCount { count: 0 })
    ));
    let broken = "-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n";
    assert!(matches!(
        check(broken, &key, &root.pem),
        Err(CertificateError::Parse { .. })
    ));
    let two_keys = format!("{key}{key}");
    assert!(matches!(
        check(&certificate, &two_keys, &root.pem),
        Err(CertificateError::KeyCount { count: 2 })
    ));
}

#[test]
fn refuses_wrong_key() {
    let root = Authority::root();
    let (certificate, _) = pair(&root);
    let other = KeyPair::generate().expect("other key").serialize_pem();
    assert!(matches!(
        check(&certificate, &other, &root.pem),
        Err(CertificateError::Key(_))
    ));
}

#[test]
fn refuses_uncovered_host() {
    let root = Authority::root();
    let key = KeyPair::generate().expect("leaf key");
    let certificate = root.leaf(&["api.test", "s3.test"], VALID, &key);
    match check(&certificate, &key.serialize_pem(), &root.pem) {
        Err(CertificateError::Name { host }) => assert_eq!(host, "portal.test"),
        other => panic!("expected a name error, got {other:?}"),
    }
}

#[test]
fn refuses_dated_certificates() {
    let root = Authority::root();
    let key = KeyPair::generate().expect("leaf key");
    let pem = key.serialize_pem();
    let expired = root.leaf(&NAMES, Window(2020, 2021), &key);
    let future = root.leaf(&NAMES, Window(2030, 2031), &key);
    for certificate in [expired, future] {
        assert!(matches!(
            check(&certificate, &pem, &root.pem),
            Err(CertificateError::Chain(_))
        ));
    }
    let old = root.intermediate(Window(2020, 2021));
    let leaf = old.leaf(&NAMES, VALID, &key);
    let chain = format!("{leaf}{}", old.pem);
    assert!(matches!(
        check(&chain, &pem, &root.pem),
        Err(CertificateError::Chain(_))
    ));
}

#[test]
fn refuses_incomplete_chain() {
    let root = Authority::root();
    let middle = root.intermediate(VALID);
    let key = KeyPair::generate().expect("leaf key");
    let pem = key.serialize_pem();
    let leaf = middle.leaf(&NAMES, VALID, &key);
    assert!(matches!(
        check(&leaf, &pem, &root.pem),
        Err(CertificateError::Chain(_))
    ));
    let chain = format!("{leaf}{}", middle.pem);
    check(&chain, &pem, &root.pem).expect("leaf and intermediate complete the chain");
}

#[test]
fn self_signed_listed() {
    let key = KeyPair::generate().expect("key");
    let certificate = leaf_params(&NAMES, VALID)
        .self_signed(&key)
        .expect("self-signed")
        .pem();
    let pem = key.serialize_pem();
    let unrelated = Authority::root();
    assert!(matches!(
        check(&certificate, &pem, &unrelated.pem),
        Err(CertificateError::Chain(_))
    ));
    check(&certificate, &pem, &certificate).expect("a listed self-signed certificate passes");
}

#[test]
fn reload_same_key() {
    let root = Authority::root();
    let key = KeyPair::generate().expect("leaf key");
    let first = root.leaf(&NAMES, VALID, &key);
    let files = Files::new(&first, &key.serialize_pem(), &root.pem);
    let store = files.store(at(NOW)).expect("store loads");
    let before = leaf_der(&store);

    files.write(&files.certificate, &root.leaf(&NAMES, VALID, &key));
    assert!(matches!(store.reload(at(NOW)), ReloadOutcome::Replaced));
    assert_ne!(leaf_der(&store), before, "the renewed leaf is served");
}

#[test]
fn reload_new_key() {
    let root = Authority::root();
    let (certificate, key) = pair(&root);
    let files = Files::new(&certificate, &key, &root.pem);
    let store = files.store(at(NOW)).expect("store loads");
    let before = leaf_der(&store);

    let (certificate, key) = pair(&root);
    files.write(&files.key, &key);
    files.write(&files.certificate, &certificate);
    assert!(matches!(store.reload(at(NOW)), ReloadOutcome::Replaced));
    assert_ne!(leaf_der(&store), before);
    assert!(matches!(store.reload(at(NOW)), ReloadOutcome::Unchanged));
}

#[test]
fn reload_keeps_active() {
    let root = Authority::root();
    let key = KeyPair::generate().expect("leaf key");
    let pem = key.serialize_pem();
    let files = Files::new(&root.leaf(&NAMES, VALID, &key), &pem, &root.pem);
    let store = files.store(at(NOW)).expect("store loads");
    let before = leaf_der(&store);

    let middle = root.intermediate(VALID);
    let refused = [
        root.leaf(&NAMES, Window(2020, 2021), &key),
        root.leaf(&NAMES, Window(2030, 2031), &key),
        middle.leaf(&NAMES, VALID, &key),
    ];
    for certificate in refused {
        files.write(&files.certificate, &certificate);
        assert!(matches!(
            store.reload(at(NOW)),
            ReloadOutcome::Rejected {
                error: CertificateError::Chain(_),
                logged: true
            }
        ));
        assert_eq!(leaf_der(&store), before, "a refused pair never replaces");
    }
}

#[test]
fn waits_for_validity() {
    // A renewal installed before its start date: two refusals at different
    // times log once, and the same bytes load once the window has started.
    let root = Authority::root();
    let key = KeyPair::generate().expect("leaf key");
    let pem = key.serialize_pem();
    let files = Files::new(&root.leaf(&NAMES, VALID, &key), &pem, &root.pem);
    let store = files.store(at(NOW)).expect("store loads");

    files.write(
        &files.certificate,
        &root.leaf(&NAMES, Window(2027, 2040), &key),
    );
    match store.reload(at(NOW)) {
        ReloadOutcome::Rejected {
            error: CertificateError::Chain(cause),
            logged: true,
        } => assert!(cause.contains("not valid before"), "{cause}"),
        other => panic!("expected a logged refusal, got {other:?}"),
    }
    let second = store.reload(at(NOW + 30 * DAY));
    assert!(
        matches!(second, ReloadOutcome::Rejected { logged: false, .. }),
        "the same files and cause must not log again: {second:?}"
    );
    assert!(matches!(
        store.reload(at(NOW + 400 * DAY)),
        ReloadOutcome::Replaced
    ));
}

#[test]
fn ca_change_applies() {
    let root = Authority::root();
    let (certificate, key) = pair(&root);
    let files = Files::new(&certificate, &key, &root.pem);
    let store = files.store(at(NOW)).expect("store loads");

    let private = Authority::root();
    let (certificate, key) = pair(&private);
    files.write(&files.key, &key);
    files.write(&files.certificate, &certificate);
    assert!(matches!(
        store.reload(at(NOW)),
        ReloadOutcome::Rejected { .. }
    ));
    files.write(&files.ca, &private.pem);
    assert!(matches!(store.reload(at(NOW)), ReloadOutcome::Replaced));
}

#[test]
fn rename_order_recovers() {
    let root = Authority::root();
    let (certificate, key) = pair(&root);
    let files = Files::new(&certificate, &key, &root.pem);
    let store = files.store(at(NOW)).expect("store loads");

    let (certificate, key) = pair(&root);
    files.write(&files.certificate, &certificate);
    assert!(matches!(
        store.reload(at(NOW)),
        ReloadOutcome::Rejected {
            error: CertificateError::Key(_),
            ..
        }
    ));
    files.write(&files.key, &key);
    assert!(matches!(store.reload(at(NOW)), ReloadOutcome::Replaced));
}

#[test]
fn startup_refuses_expired() {
    let root = Authority::root();
    let key = KeyPair::generate().expect("leaf key");
    let expired = root.leaf(&NAMES, Window(2020, 2021), &key);
    let files = Files::new(&expired, &key.serialize_pem(), &root.pem);
    assert!(matches!(
        files.store(at(NOW)),
        Err(CertificateError::Chain(_))
    ));
    let missing = CertificateStore::load(
        files.certificate.with_extension("missing"),
        files.key.clone(),
        None,
        Arc::new(routes()),
        at(NOW),
    );
    assert!(matches!(missing, Err(CertificateError::Read { .. })));
}

/// A front on a loopback port with one small router per route behind it.
struct Stack {
    address: SocketAddr,
    ca: String,
    root: Authority,
    files: Files,
    store: Arc<CertificateStore>,
    handle: HttpsHandle,
    shutdown: CancellationToken,
    /// The REST route's listener when the test keeps it from accepting.
    parked: Option<RoutedListener>,
}

/// Answers its route name on `/route` and the client address on `/peer`.
fn route_router(name: &'static str) -> Router {
    Router::new()
        .route("/route", get(move || async move { name }))
        .route(
            "/peer",
            get(|ConnectInfo(peer): ConnectInfo<SocketAddr>| async move { peer.to_string() }),
        )
}

async fn stack(permits: usize, deadline: Duration, queue: usize, serve_rest: bool) -> Stack {
    let root = Authority::root();
    let (certificate, key) = pair(&root);
    let files = Files::new(&certificate, &key, &root.pem);
    let store = Arc::new(files.store(UnixTime::now()).expect("store loads"));
    let shutdown = CancellationToken::new();
    let mut senders = HashMap::new();
    let mut parked = None;
    for (route, name) in [
        (HostRoute::Rest, "rest"),
        (HostRoute::S3, "s3"),
        (HostRoute::Portal, "portal"),
    ] {
        let check = HostCheck::new(store.routes().clone(), route);
        let (sender, routed) = RoutedListener::bounded(check.clone(), queue);
        senders.insert(route, sender);
        if route == HostRoute::Rest && !serve_rest {
            parked = Some(routed);
            continue;
        }
        let plain = TcpListener::bind("127.0.0.1:0").await.expect("plain port");
        let router = route_router(name);
        let tls = router.clone().layer(from_fn_with_state(check, check_host));
        tokio::spawn(serve_routers(
            CombinedListener::new(plain, Some(routed)),
            router,
            Some(tls),
            shutdown.clone(),
        ));
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("HTTPS port");
    let address = listener.local_addr().expect("HTTPS address");
    let handle = HttpsHandle::spawn_with(
        listener,
        store.clone(),
        senders,
        shutdown.clone(),
        permits,
        deadline,
    )
    .expect("front starts");
    Stack {
        address,
        ca: root.pem.clone(),
        root,
        files,
        store,
        handle,
        shutdown,
        parked,
    }
}

async fn default_stack() -> Stack {
    stack(16, Duration::from_secs(10), 8, true).await
}

/// A client that trusts `ca`, offers `alpn` and never resumes a session, so
/// every connection shows the certificate the server serves now.
fn client_config(ca: &str, alpn: &[&[u8]]) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(ca.as_bytes()) {
        roots.add(certificate.expect("CA PEM")).expect("CA root");
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("TLS versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
    config.resumption = Resumption::disabled();
    Arc::new(config)
}

async fn connect(
    address: SocketAddr,
    name: ServerName<'static>,
    config: Arc<ClientConfig>,
) -> std::io::Result<ClientStream<TcpStream>> {
    let tcp = TcpStream::connect(address).await?;
    TlsConnector::from(config).connect(name, tcp).await
}

fn dns(name: &str) -> ServerName<'static> {
    ServerName::try_from(name.to_string()).expect("DNS name")
}

async fn connect_h1(stack: &Stack, name: &str) -> ClientStream<TcpStream> {
    connect(
        stack.address,
        dns(name),
        client_config(&stack.ca, &[b"http/1.1"]),
    )
    .await
    .expect("TLS connection")
}

/// One HTTP/1.1 GET with an explicit `Host`, answering the status and body.
async fn get_h1(stream: ClientStream<TcpStream>, host: &str, path: &str) -> (StatusCode, String) {
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .expect("HTTP/1.1 handshake");
    tokio::spawn(connection);
    let request = http::Request::get(path)
        .header(http::header::HOST, host)
        .body(Body::empty())
        .expect("request");
    let response = sender.send_request(request).await.expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(Body::new(response.into_body()), 1 << 20)
        .await
        .expect("body");
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// Waits until the server closes `stream`, so a test observes the close itself.
async fn closed(mut stream: impl AsyncRead + Unpin) {
    let mut buffer = [0u8; 64];
    let read = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buffer))
        .await
        .expect("the server must close the connection");
    assert!(matches!(read, Ok(0) | Err(_)), "unexpected data: {read:?}");
}

#[tokio::test]
async fn routes_by_sni() {
    let stack = default_stack().await;
    for (name, expected) in [
        ("api.test", "rest"),
        ("portal.test", "portal"),
        ("s3.test", "s3"),
        ("bucket.s3.test", "s3"),
    ] {
        let stream = connect_h1(&stack, name).await;
        assert_eq!(
            get_h1(stream, name, "/route").await,
            (StatusCode::OK, expected.to_string()),
            "{name}"
        );
    }
}

#[tokio::test]
async fn refuses_unknown_names() {
    // HTTPS serves domain names only: no SNI (an IP address) and unknown names fail.
    let stack = default_stack().await;
    let config = client_config(&stack.ca, &[b"http/1.1"]);
    let ip = ServerName::IpAddress(std::net::Ipv4Addr::LOCALHOST.into());
    assert!(connect(stack.address, ip, config.clone()).await.is_err());
    assert!(
        connect(stack.address, dns("other.test"), config)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn negotiates_alpn() {
    let stack = default_stack().await;
    let config = client_config(&stack.ca, &[b"h2", b"http/1.1"]);
    let rest = connect(stack.address, dns("api.test"), config.clone())
        .await
        .expect("REST connection");
    assert_eq!(rest.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
    let s3 = connect(stack.address, dns("s3.test"), config)
        .await
        .expect("S3 connection");
    assert_eq!(s3.get_ref().1.alpn_protocol(), Some(&b"http/1.1"[..]));
}

#[tokio::test]
async fn http2_misdirected() {
    // One HTTP/2 connection reused for another route's name gets 421, so a
    // browser that coalesced connections retries on a new one.
    let stack = default_stack().await;
    let stream = connect(
        stack.address,
        dns("api.test"),
        client_config(&stack.ca, &[b"h2"]),
    )
    .await
    .expect("HTTP/2 connection");
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .expect("HTTP/2 handshake");
    tokio::spawn(connection);
    for (uri, expected) in [
        ("https://api.test/route", StatusCode::OK),
        ("https://portal.test/route", StatusCode::MISDIRECTED_REQUEST),
    ] {
        let request = http::Request::get(uri)
            .body(Body::empty())
            .expect("request");
        let response = sender.send_request(request).await.expect("response");
        assert_eq!(response.status(), expected, "{uri}");
    }
}

#[tokio::test]
async fn host_mismatch_misdirected() {
    let stack = default_stack().await;
    let stream = connect_h1(&stack, "api.test").await;
    let (status, _) = get_h1(stream, "portal.test", "/route").await;
    assert_eq!(status, StatusCode::MISDIRECTED_REQUEST);
}

#[tokio::test]
async fn handler_sees_peer() {
    let stack = default_stack().await;
    let stream = connect_h1(&stack, "api.test").await;
    let local = stream.get_ref().0.local_addr().expect("client address");
    let (status, body) = get_h1(stream, "api.test", "/peer").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, local.to_string());
}

#[tokio::test]
async fn reload_serves_new() {
    let stack = default_stack().await;
    let leaf = |stream: &ClientStream<TcpStream>| {
        stream.get_ref().1.peer_certificates().expect("peer chain")[0].to_vec()
    };
    let before = leaf(&connect_h1(&stack, "api.test").await);

    let key = KeyPair::generate().expect("leaf key");
    let renewed = stack.root.leaf(&NAMES, VALID, &key);
    stack.files.write(&stack.files.key, &key.serialize_pem());
    stack.files.write(&stack.files.certificate, &renewed);
    assert!(matches!(
        stack.store.reload(UnixTime::now()),
        ReloadOutcome::Replaced
    ));
    let after = leaf(&connect_h1(&stack, "api.test").await);
    assert_ne!(before, after, "a new connection sees the reloaded leaf");
}

#[tokio::test]
async fn handshake_deadline_frees() {
    let stack = stack(1, Duration::from_millis(300), 8, true).await;
    let idle = TcpStream::connect(stack.address)
        .await
        .expect("idle client");
    let config = client_config(&stack.ca, &[b"http/1.1"]);
    assert!(
        connect(stack.address, dns("api.test"), config.clone())
            .await
            .is_err(),
        "the only permit is held by the idle client"
    );
    closed(idle).await;
    // The permit is released right after the idle socket closes.
    let mut connected = false;
    for _ in 0..50 {
        if connect(stack.address, dns("api.test"), config.clone())
            .await
            .is_ok()
        {
            connected = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(connected, "the freed permit admits a new handshake");
}

#[tokio::test]
async fn full_queue_drops() {
    // REST never accepts: one stream fills its queue of one, and the next
    // handshake gives up at the deadline instead of waiting without bound.
    let stack = stack(16, Duration::from_millis(300), 1, false).await;
    let _queued = connect_h1(&stack, "api.test").await;
    let waiting = connect_h1(&stack, "api.test").await;
    closed(waiting).await;
    assert!(stack.parked.is_some());
}

#[tokio::test]
async fn shutdown_ends_handshakes() {
    let mut stack = stack(16, Duration::from_secs(60), 8, true).await;
    let idle = TcpStream::connect(stack.address)
        .await
        .expect("idle client");
    stack.shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), stack.handle.wait_until_released())
        .await
        .expect("shutdown releases the front and its handshakes");
    closed(idle).await;
}

/// A front whose REST route reaches the real REST server.
struct RestStack {
    address: SocketAddr,
    ca: String,
    _files: Files,
    _storage: tempfile::TempDir,
    _handle: HttpsHandle,
    _shutdown: tokio_util::sync::DropGuard,
}

async fn rest_stack() -> RestStack {
    use crate::server::state::ServerState;
    use crate::server::{Server, ServerConfig};
    use aruna_core::structs::identity::auth::NodeCapabilities;
    use aruna_core::structs::identity::realm::RealmId;

    let storage = tempfile::tempdir().expect("temp dir");
    let store_path = storage.path().to_str().expect("utf8 path");
    let driver_ctx = Arc::new(aruna_operations::driver::DriverContext {
        storage_handle: aruna_storage::FjallStorage::open(store_path).expect("storage"),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let realm_id = RealmId::from_bytes(
        ed25519_dalek::SigningKey::from_bytes(&[5u8; 32])
            .verifying_key()
            .to_bytes(),
    );
    let state = Arc::new(
        ServerState::new(
            driver_ctx,
            realm_id,
            iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            NodeCapabilities::user_node(realm_id).expect("capabilities"),
            false,
            None,
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await,
    );
    let root = Authority::root();
    let (certificate, key) = pair(&root);
    let files = Files::new(&certificate, &key, &root.pem);
    let store = Arc::new(files.store(UnixTime::now()).expect("store loads"));
    let shutdown = CancellationToken::new();
    let (sender, routed) =
        RoutedListener::channel(HostCheck::new(store.routes().clone(), HostRoute::Rest));
    let plain = TcpListener::bind("127.0.0.1:0").await.expect("plain port");
    let server = Server::new(
        state,
        ServerConfig {
            http_addr: plain.local_addr().expect("plain address"),
            max_body_size: crate::server::MAX_BODY_SIZE,
            cors: crate::cors::CorsConfig::default(),
        },
    );
    tokio::spawn(
        server.run_with_listener(CombinedListener::new(plain, Some(routed)), shutdown.clone()),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("HTTPS port");
    let address = listener.local_addr().expect("HTTPS address");
    let handle = HttpsHandle::spawn(
        listener,
        store,
        HashMap::from([(HostRoute::Rest, sender)]),
        shutdown.clone(),
    )
    .expect("front starts");
    RestStack {
        address,
        ca: root.pem,
        _files: files,
        _storage: storage,
        _handle: handle,
        _shutdown: shutdown.drop_guard(),
    }
}

fn organization_url(body: &[u8]) -> String {
    let info: serde_json::Value = serde_json::from_slice(body).expect("service info JSON");
    info["organization"]["url"]
        .as_str()
        .expect("organization URL")
        .to_string()
}

#[tokio::test]
async fn tls_urls_use_origin() {
    // DRS builds absolute URLs from the request; over TLS they carry the
    // configured HTTPS origin on HTTP/1.1 and on HTTP/2, which sends no `Host`.
    let stack = rest_stack().await;
    let path = "/api/v1/ga4gh/drs/v1/service-info";

    let stream = connect(
        stack.address,
        dns("api.test"),
        client_config(&stack.ca, &[b"http/1.1"]),
    )
    .await
    .expect("HTTP/1.1 connection");
    let (status, body) = get_h1(stream, "api.test", path).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(organization_url(body.as_bytes()), "https://api.test");

    let stream = connect(
        stack.address,
        dns("api.test"),
        client_config(&stack.ca, &[b"h2"]),
    )
    .await
    .expect("HTTP/2 connection");
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .expect("HTTP/2 handshake");
    tokio::spawn(connection);
    let request = http::Request::get(format!("https://api.test{path}"))
        .body(Body::empty())
        .expect("request");
    let response = sender.send_request(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(Body::new(response.into_body()), 1 << 20)
        .await
        .expect("body");
    assert_eq!(organization_url(&body), "https://api.test");
}
