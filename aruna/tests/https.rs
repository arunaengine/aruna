//! Tests native HTTPS on a real node: S3 streaming over TLS, virtual-hosted buckets, REST and drain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use aruna::shutdown::NodeShutdown;
use aruna_api::cors::CorsConfig;
use aruna_api::https::certificate::CertificateStore;
use aruna_api::https::listener::{CombinedListener, HttpsHandle, RoutedListener};
use aruna_api::https::{HostCheck, HostRoute, HostRoutes};
use aruna_api::s3::server::{S3Server, S3ServerHandle};
use aruna_core::shutdown::Shutdown;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_http_client::tls::{Provider, TlsContext, TrustStore, rustls_provider::CryptoMode};
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    date_time_ymd,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::shared::{
    self, AWS_REGION, S3Credentials, SeedNode, TestResult, create_bearer_token, create_group_http,
    create_s3_credentials, spawn_complete_seed,
};

/// A CA and a leaf for every route host, written to a unique folder.
struct TlsFiles {
    dir: tempfile::TempDir,
    ca: String,
}

impl TlsFiles {
    fn new() -> Self {
        let ca_key = KeyPair::generate().expect("CA key");
        let mut ca = CertificateParams::new(Vec::<String>::new()).expect("CA params");
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.not_before = date_time_ymd(2020, 1, 1);
        ca.not_after = date_time_ymd(2100, 1, 1);
        let ca_pem = ca.self_signed(&ca_key).expect("CA").pem();
        let issuer = Issuer::new(ca, ca_key);
        let names = ["api.test", "s3.test", "*.s3.test", "portal.test"];
        let mut leaf =
            CertificateParams::new(names.map(String::from).to_vec()).expect("leaf params");
        leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        leaf.not_before = date_time_ymd(2020, 1, 1);
        leaf.not_after = date_time_ymd(2100, 1, 1);
        let key = KeyPair::generate().expect("leaf key");
        let leaf_pem = leaf.signed_by(&key, &issuer).expect("leaf").pem();
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(dir.path().join("fullchain.pem"), leaf_pem).expect("certificate");
        std::fs::write(dir.path().join("privkey.pem"), key.serialize_pem()).expect("key");
        std::fs::write(dir.path().join("ca.pem"), &ca_pem).expect("CA file");
        Self { dir, ca: ca_pem }
    }

    fn store(&self, routes: HostRoutes) -> Arc<CertificateStore> {
        Arc::new(
            CertificateStore::load(
                self.dir.path().join("fullchain.pem"),
                self.dir.path().join("privkey.pem"),
                Some(self.dir.path().join("ca.pem")),
                Arc::new(routes),
                UnixTime::now(),
            )
            .expect("the test pair loads"),
        )
    }
}

/// Native settings parsed like a real node, so `S3_HOST` gets the settings normalization.
fn native_settings(s3_host: &str) -> aruna::settings::Settings {
    let map: BTreeMap<String, String> = [
        ("DATABASE_DIR", "/tmp/aruna-https-settings"),
        ("SOCKET_ADDRESS", "127.0.0.1:0"),
        ("PORTAL_MODE", "disabled"),
        ("ARUNA_FJALL_PERSIST_MODE", "buffer"),
        ("API_PUBLIC_URL", "https://api.test"),
        ("S3_HOST", s3_host),
        ("S3_ADDRESS", "127.0.0.1:0"),
        ("S3_PUBLIC_URL", "https://s3.test"),
        ("HTTPS_SOCKET_ADDRESS", "127.0.0.1:0"),
        ("HTTPS_CERTIFICATE_PATH", "/tls/fullchain.pem"),
        ("HTTPS_KEY_PATH", "/tls/privkey.pem"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value.to_string()))
    .collect();
    aruna::settings::read_settings_from(&map).expect("native settings parse")
}

/// One S3 server on the seed's storage, serving plain and TLS from one loop.
struct TlsS3 {
    https: SocketAddr,
    plain: SocketAddr,
    files: TlsFiles,
    routes: HostRoutes,
    /// Owned here until a drain test hands them to `NodeShutdown`.
    s3: Option<S3ServerHandle>,
    front: Option<HttpsHandle>,
}

async fn tls_s3(
    context: Arc<aruna_operations::driver::DriverContext>,
    realm_id: aruna_core::structs::identity::realm::RealmId,
    node_id: iroh::PublicKey,
    s3_host: &str,
    shutdown: CancellationToken,
    configure: impl FnOnce(S3Server) -> S3Server,
) -> TestResult<TlsS3> {
    let settings = native_settings(s3_host);
    let domain = settings.s3_host.clone().expect("S3 host");
    let routes = settings.https.expect("native HTTPS").routes;
    let files = TlsFiles::new();
    let store = files.store(routes.clone());
    let encryption_key = context
        .net_handle
        .as_ref()
        .map(|net| net.credential_encryption_key())
        .unwrap_or_else(aruna_core::credential_encryption::CredentialEncryptionKey::random);
    let plain = TcpListener::bind("127.0.0.1:0").await?;
    let plain_address = plain.local_addr()?;
    let (sender, routed) =
        RoutedListener::channel(HostCheck::new(store.routes().clone(), HostRoute::S3));
    let server = S3Server::new(
        plain_address.to_string().as_str(),
        domain,
        context,
        realm_id,
        node_id,
        encryption_key,
        Default::default(),
        CorsConfig::default(),
        Arc::new(aruna_core::metrics::NodeMetrics::new()),
    )
    .await?;
    let (_, s3) = configure(server)
        .run_with_listener(CombinedListener::new(plain, Some(routed)), shutdown.clone())?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let https = listener.local_addr()?;
    let front = HttpsHandle::spawn(
        listener,
        store,
        HashMap::from([(HostRoute::S3, sender)]),
        shutdown.clone(),
    )?;
    Ok(TlsS3 {
        https,
        plain: plain_address,
        files,
        routes,
        s3: Some(s3),
        front: Some(front),
    })
}

/// Every name resolves to loopback, so SNI and `Host` keep the test hostnames.
#[derive(Clone, Debug)]
struct Loopback;

impl ResolveDns for Loopback {
    fn resolve_dns<'a>(&'a self, _name: &'a str) -> DnsFuture<'a> {
        DnsFuture::ready(Ok(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]))
    }
}

fn tls_client(stack: &TlsS3, credentials: &S3Credentials, path_style: bool) -> S3Client {
    let trust = TrustStore::empty().with_pem_certificate(stack.files.ca.as_bytes());
    let context = TlsContext::builder()
        .with_trust_store(trust)
        .build()
        .expect("TLS context");
    let http = aws_smithy_http_client::Builder::new()
        .tls_provider(Provider::Rustls(CryptoMode::AwsLc))
        .tls_context(context)
        .build_with_resolver(Loopback);
    let config = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(AWS_REGION))
        .credentials_provider(Credentials::new(
            credentials.access_key_id.clone(),
            credentials.access_secret.clone(),
            None,
            None,
            "aruna-https-test",
        ))
        .endpoint_url(format!("https://s3.test:{}", stack.https.port()))
        .force_path_style(path_style)
        .http_client(http)
        .build();
    S3Client::from_conf(config)
}

fn plain_client(stack: &TlsS3, credentials: &S3Credentials) -> S3Client {
    let endpoint = shared::S3Endpoint {
        endpoint_url: format!("http://{}", stack.plain),
        host: stack.plain.to_string(),
    };
    shared::s3_client(&endpoint, credentials)
}

/// Deterministic bytes, so a corrupted or reordered stream changes the digest.
fn pattern(length: usize, seed: u8) -> Vec<u8> {
    (0..length)
        .map(|index| (index as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

async fn read_all(client: &S3Client, bucket: &str, key: &str) -> TestResult<Vec<u8>> {
    let object = client.get_object().bucket(bucket).key(key).send().await?;
    Ok(object.body.collect().await?.into_bytes().to_vec())
}

async fn seed_credentials(seed: &SeedNode, group: &str) -> TestResult<(String, S3Credentials)> {
    let token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_http(&seed.base_url, &token, group).await?;
    let credentials = create_s3_credentials(&seed.base_url, &token, &group.group_id).await?;
    Ok((token, credentials))
}

#[tokio::test]
async fn s3_streams_tls() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let (_, credentials) = seed_credentials(&seed, "https-stream").await?;
        let stack = tls_s3(
            seed.context.clone(),
            seed.realm_id,
            seed.net.node_id(),
            "s3.test",
            CancellationToken::new(),
            |server| server,
        )
        .await?;
        let client = tls_client(&stack, &credentials, true);
        let bucket = format!("https-{}", Ulid::generate().to_string().to_lowercase());
        client.create_bucket().bucket(&bucket).send().await?;

        let whole = pattern(6 << 20, 7);
        client
            .put_object()
            .bucket(&bucket)
            .key("whole.bin")
            .body(ByteStream::from(whole.clone()))
            .send()
            .await?;
        let read = read_all(&client, &bucket, "whole.bin").await?;
        assert_eq!(blake3::hash(&read), blake3::hash(&whole));

        let parts = [pattern(5 << 20, 1), pattern(1 << 20, 2)];
        let upload = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("parts.bin")
            .send()
            .await?;
        let upload_id = upload.upload_id().expect("upload id").to_string();
        let mut completed = Vec::new();
        for (index, part) in parts.iter().enumerate() {
            let number = index as i32 + 1;
            let uploaded = client
                .upload_part()
                .bucket(&bucket)
                .key("parts.bin")
                .upload_id(&upload_id)
                .part_number(number)
                .body(ByteStream::from(part.clone()))
                .send()
                .await?;
            completed.push(
                CompletedPart::builder()
                    .part_number(number)
                    .e_tag(uploaded.e_tag().expect("part ETag"))
                    .build(),
            );
        }
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("parts.bin")
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed))
                    .build(),
            )
            .send()
            .await?;
        let expected = parts.concat();
        let read = read_all(&client, &bucket, "parts.bin").await?;
        assert_eq!(blake3::hash(&read), blake3::hash(&expected));

        // The same server answers plain S3 for the same bucket.
        let plain = read_all(&plain_client(&stack, &credentials), &bucket, "whole.bin").await?;
        assert_eq!(blake3::hash(&plain), blake3::hash(&whole));
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

#[tokio::test]
async fn virtual_hosted_tls() -> TestResult<()> {
    // `S3.Test` reaches the S3 parser in lower case through the settings, so
    // `bucket.s3.test` is read as a virtual-hosted bucket either way.
    let seed = spawn_complete_seed().await?;
    let result = async {
        let (_, credentials) = seed_credentials(&seed, "https-virtual").await?;
        for s3_host in ["s3.test", "S3.Test"] {
            let stack = tls_s3(
                seed.context.clone(),
                seed.realm_id,
                seed.net.node_id(),
                s3_host,
                CancellationToken::new(),
                |server| server,
            )
            .await?;
            let client = tls_client(&stack, &credentials, false);
            let bucket = format!("virtual-{}", Ulid::generate().to_string().to_lowercase());
            client.create_bucket().bucket(&bucket).send().await?;
            client
                .put_object()
                .bucket(&bucket)
                .key("note.txt")
                .body(ByteStream::from_static(b"virtual"))
                .send()
                .await?;
            assert_eq!(
                read_all(&client, &bucket, "note.txt").await?,
                b"virtual",
                "{s3_host}"
            );
            let path_style = tls_client(&stack, &credentials, true);
            assert_eq!(
                read_all(&path_style, &bucket, "note.txt").await?,
                b"virtual"
            );
            assert!(stack.routes.contains(HostRoute::S3));
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

#[tokio::test]
async fn rest_bearer_tls() -> TestResult<()> {
    let seed = spawn_complete_seed().await?;
    let result = async {
        let (token, _) = seed_credentials(&seed, "https-rest").await?;
        let routes = native_settings("s3.test").https.expect("native").routes;
        let files = TlsFiles::new();
        let store = files.store(routes);
        let state = Arc::new(
            aruna_api::server::state::ServerState::new(
                seed.context.clone(),
                seed.realm_id,
                seed.net.node_id(),
                seed.capabilities.clone(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );
        let shutdown = CancellationToken::new();
        let _stop = shutdown.clone().drop_guard();
        let (sender, routed) =
            RoutedListener::channel(HostCheck::new(store.routes().clone(), HostRoute::Rest));
        let plain = TcpListener::bind("127.0.0.1:0").await?;
        let server = aruna_api::server::Server::new(
            state,
            aruna_api::server::ServerConfig {
                http_addr: plain.local_addr()?,
                max_body_size: aruna_api::server::MAX_BODY_SIZE,
                cors: CorsConfig::default(),
            },
        );
        tokio::spawn(
            server.run_with_listener(CombinedListener::new(plain, Some(routed)), shutdown.clone()),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let _front = HttpsHandle::spawn(
            listener,
            store,
            HashMap::from([(HostRoute::Rest, sender)]),
            shutdown.clone(),
        )?;
        let client = reqwest::Client::builder()
            .resolve("api.test", address)
            .tls_certs_only([reqwest::Certificate::from_pem(files.ca.as_bytes())?])
            .build()?;
        let response = client
            .get(format!(
                "https://api.test:{}/api/v1/access/groups",
                address.port()
            ))
            .bearer_auth(&token)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let anonymous = client
            .get(format!(
                "https://api.test:{}/api/v1/access/groups",
                address.port()
            ))
            .send()
            .await?;
        assert_eq!(anonymous.status(), reqwest::StatusCode::UNAUTHORIZED);
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    seed.shutdown().await;
    result
}

/// A TLS client for `s3.test` that trusts the test CA.
async fn connect_s3(stack: &TlsS3) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(stack.files.ca.as_bytes()) {
        roots.add(certificate.expect("CA PEM")).expect("CA root");
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("TLS versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    let tcp = TcpStream::connect(stack.https).await.expect("TCP connect");
    let name = ServerName::try_from("s3.test").expect("DNS name");
    tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(name, tcp)
        .await
        .expect("TLS connection")
}

/// Sends one request and reads its answer, which proves the server accepted
/// the connection; the connection stays open for the drain under test.
async fn answered(stream: &mut tokio_rustls::client::TlsStream<TcpStream>) {
    stream
        .write_all(b"GET /bucket/key HTTP/1.1\r\nHost: s3.test\r\nContent-Length: 0\r\n\r\n")
        .await
        .expect("request");
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).await.expect("response head");
        head.push(byte[0]);
    }
    let text = String::from_utf8_lossy(&head);
    let length = text
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse::<usize>().expect("length"))
        })
        .unwrap_or(0);
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).await.expect("response body");
}

async fn released(mut stream: tokio_rustls::client::TlsStream<TcpStream>) {
    let mut buffer = [0u8; 16];
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buffer))
        .await
        .expect("the drain must release the TLS connection");
    assert!(matches!(read, Ok(0) | Err(_)), "unexpected data: {read:?}");
}

fn shutdown_sequence(
    shutdown: Shutdown,
    storage_handle: aruna_storage::StorageHandle,
    grace: Duration,
) -> NodeShutdown {
    NodeShutdown {
        shutdown,
        readiness: aruna_api::monitoring::Readiness::new(),
        rest: None,
        s3: None,
        portal: None,
        session_s3: None,
        https: None,
        monitoring: None,
        task_handle: aruna_tasks::TaskHandle::new(),
        jobs_runtime: aruna_operations::jobs::runtime::JobsRuntime::new(),
        net_handle: None,
        metadata_handle: None,
        blob_handle: None,
        storage_handle,
        ops: None,
        grace,
    }
}

/// A TLS S3 stack on fresh storage with one connection permit, stopped by `shutdown`.
async fn drained_stack(
    shutdown: &Shutdown,
) -> (TlsS3, aruna_storage::StorageHandle, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let storage = aruna_storage::FjallStorage::open(dir.path().to_str().expect("utf8 path"))
        .expect("storage opens");
    let context = Arc::new(aruna_operations::driver::DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let secret = iroh::SecretKey::from_bytes(&[0x61; 32]);
    let stack = tls_s3(
        context,
        aruna_core::structs::identity::realm::RealmId::from_bytes([0x61; 32]),
        secret.public(),
        "s3.test",
        shutdown.token(),
        |server| server.with_concurrency_limits(1, 64),
    )
    .await
    .expect("TLS S3 stack");
    (stack, storage, dir)
}

#[tokio::test]
async fn tls_drain_graceful() {
    // An established, answered TLS connection drains within the ingress slice.
    let shutdown = Shutdown::new();
    let (mut stack, storage, _dir) = drained_stack(&shutdown).await;
    let mut client = connect_s3(&stack).await;
    answered(&mut client).await;
    let mut sequence = shutdown_sequence(shutdown.clone(), storage, Duration::from_secs(20));
    sequence.s3 = stack.s3.take();
    sequence.https = stack.front.take();
    let outcome = sequence.run().await;
    assert!(
        !outcome.ingress_forced,
        "an idle TLS connection drains gracefully"
    );
    released(client).await;
}

/// A TLS connection stuck in its first request head that the S3 loop has
/// accepted. With one permit the server drops whichever connection came
/// second, so a dropped plain probe proves the TLS connection holds it.
async fn stuck_tls(stack: &TlsS3) -> tokio_rustls::client::TlsStream<TcpStream> {
    for _ in 0..20 {
        let mut client = connect_s3(stack).await;
        client
            .write_all(b"PUT /bucket/key HTTP/1.1\r\nHost: s3.test\r\n")
            .await
            .expect("partial head");
        client.flush().await.expect("flush");
        let mut probe = TcpStream::connect(stack.plain).await.expect("plain probe");
        let (mut tls_byte, mut probe_byte) = ([0u8; 1], [0u8; 1]);
        let tls_dropped = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                _ = probe.read(&mut probe_byte) => false,
                _ = client.read(&mut tls_byte) => true,
            }
        })
        .await
        .expect("the server drops one of the two connections");
        if !tls_dropped {
            return client;
        }
    }
    panic!("the TLS connection never reached the S3 loop first");
}

#[tokio::test]
async fn tls_drain_forced() {
    // A TLS connection stuck in a request head outlives the ingress slice; the
    // forced cleanup still releases it before the sequence continues.
    let shutdown = Shutdown::new();
    let (mut stack, storage, _dir) = drained_stack(&shutdown).await;
    let client = stuck_tls(&stack).await;
    let mut sequence = shutdown_sequence(shutdown.clone(), storage, Duration::from_millis(200));
    sequence.s3 = stack.s3.take();
    sequence.https = stack.front.take();
    let outcome = sequence.run().await;
    assert!(
        outcome.ingress_forced,
        "the stuck connection outlives the slice"
    );
    released(client).await;
}
