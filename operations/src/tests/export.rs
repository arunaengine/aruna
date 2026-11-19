//! Builds the specs, documents, job contexts, probes and nodes the export tests share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use crate::driver::drive;
use crate::jobs::context::JobContext;
use crate::jobs::executor::ProgressReporter;
use crate::jobs::export::ExportFailure;
use crate::jobs::export::archive::recognize_entities;
use crate::jobs::export::open::OpenCandidateOperation;
use crate::jobs::export::probe::{CandidateFailures, ProbeSourcesOperation};
use crate::jobs::export::resolve::ExportCandidate;
use crate::jobs::export::sources::AliasState;
use crate::jobs::export::*;
use crate::jobs::repository::test_support::owner_node;
use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::Event;
use aruna_core::events::StorageEvent;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::keyspaces::{BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::stream::BackendStream;
use aruna_core::stream::StreamError;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::execution::job::{ExportRoCrateSpec, JobId};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmNodeKind,
};
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::blob::{Backend, BackendConfig, BackendRef, BlobLocationKey};
use aruna_core::structs::storage::blob::{BlobVersion, BucketInfo, VersionKey};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::format::Compression;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_storage::FjallStorage;
use bytes::Bytes;
use serde_json::Value as JsonValue;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use ulid::Ulid;

pub fn remote_spec(realm_id: RealmId, user_id: UserId) -> ExportRoCrateSpec {
    ExportRoCrateSpec {
        selection: None,
        destination: None,
        auth_context: AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        document_id: Ulid::from_bytes([91; 16]),
        limits: RoCrateLimits::default(),
    }
}

pub fn recognized_entities(
    document: &JsonValue,
    realm_id: RealmId,
) -> Result<Vec<ExportEntity>, ExportFailure> {
    let jsonld = serde_json::to_string(document).unwrap();
    let canonical = craqle::canonicalize_jsonld(&jsonld).unwrap();
    recognize_entities(document, &canonical.nquads, realm_id)
}

pub fn job_context(driver: Arc<DriverContext>, owner_node_id: NodeId) -> JobContext {
    JobContext {
        driver,
        job_id: JobId::from_bytes([84; 16]),
        owner_node_id,
        claim_token: Ulid::from_bytes([85; 16]),
        final_attempt: false,
        cancel: tokio_util::sync::CancellationToken::new(),
        shutdown: tokio_util::sync::CancellationToken::new(),
        progress: ProgressReporter::from_progress(
            &aruna_core::structs::execution::job::JobProgress {
                current: 0,
                total: None,
                unit: "entries".to_string(),
                paused_until: None,
            },
        ),
        range_turns: Default::default(),
        handlers: Default::default(),
    }
}

pub fn crate_document(parts: &[JsonValue]) -> JsonValue {
    let mut graph = vec![
        json!({
            "@id": "./",
            "@type": "Dataset",
            "name": "test",
            "description": "test crate",
            "datePublished": "2026-07-23",
            "hasPart": parts
                .iter()
                .map(|part| json!({"@id": part["@id"]}))
                .collect::<Vec<_>>()
        }),
        json!({
            "@id": METADATA_PATH,
            "@type": "CreativeWork",
            "about": {"@id": "./"},
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}
        }),
    ];
    graph.extend(parts.iter().cloned());
    json!({"@context": "https://w3id.org/ro/crate/1.2/context", "@graph": graph})
}

pub fn scope_of(ctx: &JobContext, spec: &ExportRoCrateSpec) -> SourceScope {
    SourceScope {
        spec: Arc::new(spec.clone()),
        owner_node: ctx.owner_node_id,
    }
}

pub async fn probe_sources(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    checkpoint: &mut ExportCheckpoint,
    failures: &CandidateFailures,
) -> Result<Vec<ProbedEntry>, ExportFailure> {
    let probe = ProbeSourcesOperation::new(
        scope_of(ctx, spec),
        checkpoint.entities.clone(),
        failures.clone(),
        AliasState::default(),
    );
    let probed = drive(probe, ctx.driver.as_ref()).await?;
    checkpoint.entities = probed.entities;
    Ok(probed.entries)
}

pub fn destination() -> aruna_core::repository::RepositoryDestination {
    aruna_core::repository::RepositoryDestination {
        group_id: Ulid::from_parts(2, 2),
        connector_id: Ulid::from_parts(4, 4),
        draft_id: None,
        published_id: None,
        metadata_json: "{}".to_string(),
        publish: false,
        public_files: false,
        credential: None,
        link: None,
    }
}

pub const FIXTURE_BYTES: &[u8] = b"duplicate fixture payload";

/// A test node with its own storage, network and blob store.
pub struct BaoNode {
    _tempdir: TempDir,
    pub net: NetHandle,
    pub driver: Arc<DriverContext>,
}

/// A node with storage, net and blob handles, and a metadata handle when `metadata` holds.
pub async fn export_node(realm_id: RealmId, metadata: bool) -> BaoNode {
    let tempdir = tempfile::tempdir().unwrap();
    let root = tempdir.path().to_str().unwrap();
    let blob_root = tempdir.path().join("blobstore");
    std::fs::create_dir_all(&blob_root).unwrap();
    let storage = FjallStorage::open(root).unwrap();
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .unwrap();
    let blob = BlobHandler::new(
        BackendConfig {
            backend_type: Backend::FileSystem,
            root: blob_root.to_str().unwrap().to_string(),
            service_config: HashMap::new(),
            bucket_prefix: Some("aruna-export-".to_string()),
            max_bucket_size: Some(100),
            multipart_bucket: Some("multipart".to_string()),
            timeouts: Default::default(),
        },
        storage.clone(),
        net.clone(),
    )
    .await
    .unwrap();
    let path = tempdir.path().join("metadata");
    if metadata {
        crate::metadata::install_indexes(&storage, &path);
    }
    let graph_handle = metadata.then(|| {
        aruna_graph::GraphHandle::open(&path, net.node_id(), None, None, Default::default())
            .unwrap()
    });
    let driver = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: Some(blob),
        graph_handle,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    });
    BaoNode {
        _tempdir: tempdir,
        net,
        driver,
    }
}

pub async fn seed_bao(
    source: &BaoNode,
    peer: NodeId,
    owner: UserId,
    group_id: Ulid,
    version_id: Ulid,
    location: &BackendLocation,
) {
    let realm_id = owner.realm_id;
    let actor = Actor {
        node_id: source.net.node_id(),
        user_id: owner,
        realm_id,
    };
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(source.net.node_id(), RealmNodeKind::Server);
    config.ensure_node(peer, RealmNodeKind::Server);
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
    let group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    // Policy loading resolves the group record before group policies apply.
    let group = aruna_core::structs::identity::group::Group {
        display_name: "export".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    let bucket = BucketInfo {
        group_id,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: owner,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    let hash: [u8; 32] = location.get_blake3().unwrap().try_into().unwrap();
    let version = BlobVersion::materialized(
        hash,
        BackendRef::node_default(),
        location.format.encoding(),
        std::time::SystemTime::UNIX_EPOCH,
        owner,
        None,
    );
    let version_key = VersionKey::new("remote", "payload", version_id);
    let writes = vec![
        (
            REALM_CONFIG_KEYSPACE.to_string(),
            realm_id.as_bytes().to_vec().into(),
            config.to_bytes(&actor).unwrap().into(),
        ),
        (
            AUTH_KEYSPACE.to_string(),
            realm_id.as_bytes().to_vec().into(),
            realm_auth.to_bytes(&actor).unwrap().into(),
        ),
        (
            AUTH_KEYSPACE.to_string(),
            group_id.to_bytes().to_vec().into(),
            group_auth.to_bytes(&actor).unwrap().into(),
        ),
        (
            GROUP_KEYSPACE.to_string(),
            group_id.to_bytes().to_vec().into(),
            group.to_bytes(&actor).unwrap().into(),
        ),
        (
            S3_BUCKET_KEYSPACE.to_string(),
            b"remote".to_vec().into(),
            bucket.to_bytes().unwrap().into(),
        ),
        (
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version_key.to_bytes().unwrap().into(),
            version.to_bytes().unwrap().into(),
        ),
        (
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            BlobLocationKey::new(hash, location.format.encoding(), location.backend.clone())
                .to_bytes()
                .into(),
            location.to_bytes().unwrap().into(),
        ),
    ];
    assert!(matches!(
        source
            .driver
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

pub fn byte_stream(bytes: &'static [u8]) -> BackendStream<Result<Bytes, StreamError>> {
    BackendStream::new(futures_util::stream::iter([Ok::<Bytes, std::io::Error>(
        Bytes::from_static(bytes),
    )]))
}

/// Writes a bucket's encryption settings row on `node`.
pub async fn encrypting_bucket(node: &BaoNode, bucket: &str, bucket_id: Ulid) {
    use aruna_core::structs::storage::encryption::EncryptionMode;
    let settings = BucketEncryption {
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(bucket_id),
        key_generation: 1,
        ..Default::default()
    };
    let event = node
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            key: bucket.as_bytes().to_vec().into(),
            value: settings.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

pub async fn open_candidate(
    driver: &DriverContext,
    spec: &ExportRoCrateSpec,
    candidate: &ExportCandidate,
    metadata_only: bool,
) -> Result<CandidateOpen, ExportFailure> {
    let scope = SourceScope {
        spec: Arc::new(spec.clone()),
        owner_node: owner_node(),
    };
    let open = OpenCandidateOperation::new(scope, candidate.clone(), metadata_only);
    drive(open, driver).await
}
