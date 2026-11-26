//! Full-stack tests of the embedded SMB and NFS mounts on a real seed: Fjall storage, file
//! blobs, the file server state, the S3 and REST listeners and loopback wire clients.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod nfs_client;
mod nfs_mounts;
mod shared_mounts;
mod smb_client;
mod smb_mounts;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use aruna_admission::RateLimits;
use aruna_api::routes::storage::nfs::{HostPage, HostView, MountPage, MountView};
use aruna_core::keys::generate_signing_key;
use aruna_nfs::{ArunaNfsBackend, ArunaNfsConfig, NfsServer, NfsServerConfig, NfsServerHandle};
use aruna_nfs::{NfsTlsConfig, Revocation};
use aruna_smb::{ArunaSmbBackend, ArunaSmbConfig, SmbConfig, SmbServer, SmbServerHandle};
use aruna_vfs::GrantRevocation;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use base64::Engine;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::{Signer, SigningKey};
use rcgen::{BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose};
use rcgen::{IsCa, Issuer, KeyPair};
use reqwest::{RequestBuilder, StatusCode};
use rustls::pki_types::CertificateDer;
use tokio::net::TcpListener;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::shared::spawn_vfs_seed;
use crate::shared::{S3Credentials, SeedNode, TestResult, create_bearer_token};
use crate::shared::{create_group_http, create_s3_credentials, revoke_s3_credentials, s3_client};

/// The bucket every test shares over S3, SMB and NFS.
pub(crate) const BUCKET: &str = "mounts";
const LOOPBACK: &str = "127.0.0.1/32";
/// Deadlock cap of one shutdown phase; an honest shutdown takes far less.
const SHUTDOWN_CAP: Duration = Duration::from_secs(120);

/// NFS without TLS for loopback peers only, as `ARUNA_VFS_NFS_LOOPBACK_CLEARTEXT` configures.
pub(crate) fn loopback_tls() -> NfsTlsConfig {
    NfsTlsConfig {
        cert_pem: Vec::new(),
        key_pem: Vec::new(),
        loopback_cleartext: true,
    }
}

/// A test authority and a server certificate for `nfs.test`, with the authority certificate.
pub(crate) fn server_tls() -> TestResult<(NfsTlsConfig, CertificateDer<'static>)> {
    let authority_key = KeyPair::generate()?;
    let mut authority = CertificateParams::new(Vec::<String>::new())?;
    authority.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let root = authority.self_signed(&authority_key)?;
    let issuer = Issuer::new(authority, authority_key);
    let server_key = KeyPair::generate()?;
    let mut leaf = CertificateParams::new(vec![nfs_client::SERVER_NAME.to_string()])?;
    leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let certificate = leaf.signed_by(&server_key, &issuer)?;
    let tls = NfsTlsConfig {
        cert_pem: certificate.pem().into_bytes(),
        key_pem: server_key.serialize_pem().into_bytes(),
        loopback_cleartext: false,
    };
    Ok((tls, root.der().clone()))
}

/// An NFS client host key: Ed25519, so it signs the enrollment proof, with its own certificate.
pub(crate) struct HostKey {
    signer: SigningKey,
    key: KeyPair,
    certificate: Certificate,
}

impl HostKey {
    pub(crate) fn generate() -> TestResult<Self> {
        let signer = generate_signing_key();
        let key = KeyPair::from_pem(&signer.to_pkcs8_pem(LineEnding::default())?)?;
        let params = CertificateParams::new(vec!["client.test".to_string()])?;
        let certificate = params.self_signed(&key)?;
        Ok(Self {
            signer,
            key,
            certificate,
        })
    }

    /// The hex SHA-256 SPKI digest the server pins, computed as the server does.
    fn fingerprint(&self) -> TestResult<String> {
        let digest = aruna_nfs::tls::fingerprint(self.certificate.der());
        Ok(hex::encode(
            digest.ok_or("the host certificate has no SPKI")?,
        ))
    }
}

/// Sends `request` and fails unless the answer has the `expected` status.
async fn answered(request: RequestBuilder, expected: StatusCode) -> TestResult<reqwest::Response> {
    let response = request.send().await?;
    let status = response.status();
    if status == expected {
        return Ok(response);
    }
    let body = response.text().await.unwrap_or_default();
    Err(format!("expected {expected}, got {status}: {body}").into())
}

/// Bounds one shutdown phase and names it on elapse.
async fn capped<F: Future>(phase: &str, work: F) {
    if tokio::time::timeout(SHUTDOWN_CAP, work).await.is_err() {
        panic!("{phase} did not stop within {SHUTDOWN_CAP:?}");
    }
}

/// Hands every revoked host, binding or key to the running servers, as the node's bridge does.
fn forward(
    revocation: GrantRevocation,
    smb: Option<&ArunaSmbBackend>,
    nfs: Option<&ArunaNfsBackend>,
) {
    if let (Some(smb), Some(key)) = (smb, revocation.access_key.as_ref()) {
        smb.publish_revocation(key.clone());
    }
    let events = [
        revocation.host_id.map(Revocation::Host),
        revocation.mount_id.map(Revocation::Mount),
        revocation.access_key.map(Revocation::AccessKey),
    ];
    for event in events.into_iter().flatten() {
        if let Some(nfs) = nfs {
            nfs.publish_revocation(event);
        }
    }
}

/// The SMB and NFS listeners a test asks for, their backends and their handles.
#[derive(Default)]
struct Servers {
    smb: Option<(Arc<ArunaSmbBackend>, SocketAddr, SmbServerHandle)>,
    nfs: Option<(Arc<ArunaNfsBackend>, SocketAddr, NfsServerHandle)>,
}

async fn start_smb(
    seed: &SeedNode,
    limits: Arc<RateLimits>,
    stop: &CancellationToken,
) -> TestResult<(Arc<ArunaSmbBackend>, SocketAddr, SmbServerHandle)> {
    let config = ArunaSmbConfig {
        realm_id: seed.realm_id,
        node_id: seed.net.node_id(),
        credential_key: seed.net.credential_encryption_key(),
    };
    let backend = Arc::new(ArunaSmbBackend::new(seed.context.clone(), config));
    let server = SmbServer::new(backend.clone(), limits, SmbConfig::default())?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let (address, handle) = server.run_with_listener(listener, stop.clone())?;
    Ok((backend, address, handle))
}

async fn start_nfs(
    seed: &SeedNode,
    tls: NfsTlsConfig,
    limits: Arc<RateLimits>,
    stop: &CancellationToken,
) -> TestResult<(Arc<ArunaNfsBackend>, SocketAddr, NfsServerHandle)> {
    let backend = Arc::new(ArunaNfsBackend::new(ArunaNfsConfig {
        context: seed.context.clone(),
        realm_id: seed.realm_id,
        node_id: seed.net.node_id(),
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let config = NfsServerConfig::new(address, tls, [7; 32]);
    let server = NfsServer::new(config, backend.clone(), limits);
    let handle = server.run_with_listener(listener, stop.clone())?;
    Ok((backend, address, handle))
}

/// One vfs seed with a group, an S3 key pair of it, the bucket `BUCKET`, the requested file
/// servers and the revocation bridge between the file server state and them.
pub(crate) struct Mounts {
    pub(crate) seed: SeedNode,
    pub(crate) s3: S3Client,
    pub(crate) keys: S3Credentials,
    pub(crate) smb: Option<SocketAddr>,
    pub(crate) nfs: Option<SocketAddr>,
    token: String,
    group_id: String,
    handles: (Option<SmbServerHandle>, Option<NfsServerHandle>),
    stop: CancellationToken,
    bridge: JoinHandle<()>,
}

impl Mounts {
    pub(crate) async fn start(smb: bool, nfs: Option<NfsTlsConfig>) -> TestResult<Self> {
        let seed = spawn_vfs_seed().await?;
        let capabilities = seed.capabilities.clone();
        let context = seed.context.as_ref();
        let token = create_bearer_token(context, seed.user_id, seed.realm_id, capabilities).await?;
        let group = create_group_http(&seed.base_url, &token, "file-mounts").await?;
        let keys = create_s3_credentials(&seed.base_url, &token, &group.group_id).await?;
        let s3 = s3_client(seed.s3.as_ref().ok_or("the seed runs no S3")?, &keys);
        s3.create_bucket().bucket(BUCKET).send().await?;
        let (stop, limits) = (CancellationToken::new(), Arc::new(RateLimits::default()));
        let mut servers = Servers::default();
        if smb {
            servers.smb = Some(start_smb(&seed, limits.clone(), &stop).await?);
        }
        if let Some(tls) = nfs {
            servers.nfs = Some(start_nfs(&seed, tls, limits, &stop).await?);
        }
        let vfs = seed
            .context
            .vfs_handle
            .as_ref()
            .ok_or("the seed has no file servers")?;
        let mut revocations = vfs.subscribe_revocations();
        let smb_backend = servers.smb.as_ref().map(|(backend, ..)| backend.clone());
        let nfs_backend = servers.nfs.as_ref().map(|(backend, ..)| backend.clone());
        let cancelled = stop.clone();
        let bridge = tokio::spawn(async move {
            loop {
                let received = tokio::select! {
                    _ = cancelled.cancelled() => return,
                    received = revocations.recv() => received,
                };
                match received {
                    Ok(revocation) => {
                        forward(revocation, smb_backend.as_deref(), nfs_backend.as_deref());
                    }
                    Err(RecvError::Lagged(_)) => {}
                    Err(RecvError::Closed) => return,
                }
            }
        });
        let (smb, smb_handle) = servers.smb.map(|(_, at, handle)| (at, handle)).unzip();
        let (nfs, nfs_handle) = servers.nfs.map(|(_, at, handle)| (at, handle)).unzip();
        Ok(Self {
            seed,
            s3,
            keys,
            smb,
            nfs,
            token,
            group_id: group.group_id,
            handles: (smb_handle, nfs_handle),
            stop,
            bridge,
        })
    }

    /// Stops the file servers, then the bridge, then the seed.
    pub(crate) async fn shutdown(self) {
        self.stop.cancel();
        let (smb, nfs) = self.handles;
        if let Some(handle) = smb {
            handle.abort();
            capped("the SMB server", handle.wait()).await;
        }
        if let Some(handle) = nfs {
            handle.abort();
            capped("the NFS server", handle.wait()).await;
        }
        capped("the revocation bridge", self.bridge).await;
        self.seed.shutdown().await;
    }

    pub(crate) async fn put(&self, key: &str, data: &[u8]) -> TestResult<()> {
        let body = ByteStream::from(data.to_vec());
        let put = self.s3.put_object().bucket(BUCKET).key(key).body(body);
        put.send().await?;
        Ok(())
    }

    pub(crate) async fn get(&self, key: &str) -> TestResult<Vec<u8>> {
        let object = self.s3.get_object().bucket(BUCKET).key(key).send().await?;
        Ok(object.body.collect().await?.into_bytes().to_vec())
    }

    pub(crate) async fn delete(&self, key: &str) -> TestResult<()> {
        self.s3
            .delete_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await?;
        Ok(())
    }

    /// Whether S3 answers `NoSuchKey` for `key`.
    pub(crate) async fn missing(&self, key: &str) -> bool {
        let read = self.s3.get_object().bucket(BUCKET).key(key).send().await;
        read.is_err_and(|error| {
            error
                .as_service_error()
                .is_some_and(|found| found.is_no_such_key())
        })
    }

    pub(crate) async fn exists(&self, key: &str) -> bool {
        let head = self.s3.head_object().bucket(BUCKET).key(key).send().await;
        head.is_ok()
    }

    /// Revokes the S3 key pair through `DELETE /access/credentials/{key}`.
    pub(crate) async fn revoke_key(&self) -> TestResult<()> {
        let key = &self.keys.access_key_id;
        revoke_s3_credentials(&self.seed.base_url, &self.token, key).await
    }

    fn nfs_route(&self, path: &str) -> String {
        format!("{}/api/v1/data/nfs/{path}", self.seed.base_url)
    }

    /// Enrolls `host` for the loopback network and binds the whole bucket to it writable.
    pub(crate) async fn bind_host(&self, host: &HostKey) -> TestResult<(HostView, MountView)> {
        self.bind_host_mode(host, false).await
    }

    /// Enrolls `host` for the loopback network and binds the whole bucket to it read-only.
    pub(crate) async fn bind_ro_host(&self, host: &HostKey) -> TestResult<(HostView, MountView)> {
        self.bind_host_mode(host, true).await
    }

    /// Enrolls `host` and binds the whole bucket with `read_only`, checking that both list
    /// routes show the new rows.
    async fn bind_host_mode(
        &self,
        host: &HostKey,
        read_only: bool,
    ) -> TestResult<(HostView, MountView)> {
        let http = reqwest::Client::new();
        let fingerprint = host.fingerprint()?;
        let challenge = format!(
            "aruna-nfs-host-enroll-v1:{}:{fingerprint}",
            self.seed.user_id
        );
        let signature = host.signer.sign(challenge.as_bytes()).to_bytes();
        let enroll = serde_json::json!({
            "fingerprints": [fingerprint],
            "public_key_pem": host.key.public_key_pem(),
            "proof": base64::engine::general_purpose::STANDARD.encode(signature),
            "allowed_networks": [LOOPBACK],
        });
        let request = http.post(self.nfs_route("hosts")).bearer_auth(&self.token);
        let created = answered(request.json(&enroll), StatusCode::CREATED).await?;
        let created: HostView = created.json().await?;
        let request = http.get(self.nfs_route("hosts")).bearer_auth(&self.token);
        let hosts: HostPage = answered(request, StatusCode::OK).await?.json().await?;
        assert!(
            hosts.items.contains(&created),
            "GET /data/nfs/hosts lists the new host"
        );
        let binding = serde_json::json!({
            "host_id": created.host_id,
            "access_key": self.keys.access_key_id,
            "group_id": self.group_id,
            "bucket": BUCKET,
            "read_only": read_only,
            "allowed_networks": [LOOPBACK],
            "display_uid": 1000,
            "display_gid": 1000,
            "mode": 0o755,
        });
        let request = http.post(self.nfs_route("mounts")).bearer_auth(&self.token);
        let mount = answered(request.json(&binding), StatusCode::CREATED).await?;
        let mount: MountView = mount.json().await?;
        let request = http.get(self.nfs_route("mounts")).bearer_auth(&self.token);
        let mounts: MountPage = answered(request, StatusCode::OK).await?.json().await?;
        assert!(
            mounts.items.contains(&mount),
            "GET /data/nfs/mounts lists the binding"
        );
        assert_eq!(
            (mount.kind.as_str(), mount.read_only),
            ("bucket", read_only)
        );
        Ok((created, mount))
    }

    /// Revokes the host through `DELETE /data/nfs/hosts/{host_id}`.
    pub(crate) async fn revoke_host(&self, host_id: &str) -> TestResult<()> {
        let route = self.nfs_route(&format!("hosts/{host_id}"));
        let request = reqwest::Client::new()
            .delete(route)
            .bearer_auth(&self.token);
        answered(request, StatusCode::NO_CONTENT).await?;
        Ok(())
    }
}
