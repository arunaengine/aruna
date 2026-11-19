//! Tests RO-Crate exports that read their sources from other nodes through the peer handler.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::BlobEffect;
use aruna_core::effects::StorageEffect;
use aruna_core::events::BlobEvent;
use aruna_core::events::Event;
use aruna_core::events::StorageEvent;
use aruna_core::id::NodeId;
use aruna_core::jobs::WaitTarget;
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::keyspaces::{BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::stream::BackendStream;
use aruna_core::stream::StreamError;
use aruna_core::structs::execution::job::ExportReportSource;
use aruna_core::structs::execution::job::ExportRoCrateSpec;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::blob::HashIndex;
use aruna_core::structs::storage::blob::{
    BlobVersion, BucketInfo, VersionKey, object_permission_path,
};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_operations::auth::enforcement::PolicyBulkOperation;
use aruna_operations::auth::permission_rules::PermissionRulesConfig;
use aruna_operations::auth::permission_rules::PermissionRulesOperation;
use aruna_operations::driver::DriverContext;
use aruna_operations::driver::drive;
use aruna_operations::jobs::context::JobContext;
use aruna_operations::jobs::executor::JobRunOutcome;
use aruna_operations::jobs::export::ExportCheckpoint;
use aruna_operations::jobs::export::ExportEntity;
use aruna_operations::jobs::export::ExportFailure;
use aruna_operations::jobs::export::METADATA_PATH;
use aruna_operations::jobs::export::ProbedEntry;
use aruna_operations::jobs::export::archive::*;
use aruna_operations::jobs::export::consent::UnheldBucketOperation;
use aruna_operations::jobs::export::open::OpenCandidateOperation;
use aruna_operations::jobs::export::probe::{CandidateFailures, ProbeSourcesOperation};
use aruna_operations::jobs::export::resolve::CandidateSource;
use aruna_operations::jobs::export::resolve::ExportCandidate;
use aruna_operations::jobs::export::sources::{
    AliasState, HashCandidatesOperation, HashFound, ResolveEntriesOperation, alias_allowed,
};
use aruna_operations::jobs::export::*;
use aruna_operations::jobs::key_locks::AskWaitsOperation;
use aruna_operations::jobs::repository::test_support::{job_id, owner_node};
use aruna_operations::replication::bao_read::BaoReadOutput;
use aruna_operations::replication::plaintext::HolderCheckOperation;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::io::AsyncWriteExt;
use serde_json::Value as JsonValue;
use serde_json::json;
use std::collections::BTreeMap;
use ulid::Ulid;

use aruna::inbound::initialize_incoming_fixture;
use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, GROUP_KEYSPACE, PATHS_INDEX_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmNodeKind,
};
use aruna_core::structs::storage::blob::{Backend, BackendConfig, BackendRef, BlobLocationKey};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::jobs::executor::ProgressReporter;
use aruna_storage::FjallStorage;
use std::collections::HashMap;
use std::io::{Read, Seek, Write};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::AsyncReadExt;

struct BaoNode {
    _tempdir: TempDir,
    net: NetHandle,
    driver: Arc<DriverContext>,
}

async fn bao_node(realm_id: RealmId) -> BaoNode {
    node_with(realm_id, false).await
}

/// A node as `bao_node`, with a metadata handle when `metadata` holds, so it answers job control.
async fn node_with(realm_id: RealmId, metadata: bool) -> BaoNode {
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
    let metadata_handle = metadata.then(|| {
        let net = Some(net.clone());
        aruna_operations::metadata::MetadataHandle::new(
            &path,
            storage.clone(),
            net,
            Default::default(),
        )
    });
    let graph_handle = metadata.then(|| {
        aruna_graph::GraphHandle::open(&path, net.node_id(), None, None, Default::default())
            .unwrap()
    });
    let driver = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: Some(blob),
        metadata_handle,
        graph_handle,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    });
    initialize_incoming_fixture(driver.clone());
    BaoNode {
        _tempdir: tempdir,
        net,
        driver,
    }
}

fn remote_spec(realm_id: RealmId, user_id: UserId) -> ExportRoCrateSpec {
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

async fn seed_bao(
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

async fn keyspace_len(driver: &DriverContext, key_space: &str) -> usize {
    let Event::Storage(StorageEvent::IterResult { values, .. }) = driver
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start: None,
            limit: 10,
            txn_id: None,
        })
        .await
    else {
        panic!("keyspace iteration failed")
    };
    values.len()
}

fn byte_stream(bytes: &'static [u8]) -> BackendStream<Result<Bytes, StreamError>> {
    BackendStream::new(futures_util::stream::iter([Ok::<Bytes, std::io::Error>(
        Bytes::from_static(bytes),
    )]))
}

fn recognized_entities(
    document: &JsonValue,
    realm_id: RealmId,
) -> Result<Vec<ExportEntity>, ExportFailure> {
    let jsonld = serde_json::to_string(document).unwrap();
    let canonical = craqle::canonicalize_jsonld(&jsonld).unwrap();
    recognize_entities(document, &canonical.nquads, realm_id)
}

#[tokio::test]
async fn stale_holder_offline() {
    let realm_id = RealmId::from_bytes([61; 32]);
    let node = bao_node(realm_id).await;
    node.net.shutdown().await;
    let hash = [62; 32];
    let candidate = ExportCandidate {
        source: CandidateSource::RemoteHash {
            node_id: iroh::SecretKey::from_bytes(&[63; 32]).public(),
            hash,
        },
        report_source: ExportReportSource::Hash,
        resolved_version: None,
        expected_blake3: Some(hash),
    };

    assert!(matches!(
        open_candidate(
            node.driver.as_ref(),
            &remote_spec(realm_id, UserId::nil(realm_id)),
            &candidate,
            true,
        )
        .await
        .unwrap(),
        CandidateOpen::Status(OpenStatus::Offline)
    ));
}

#[tokio::test]
async fn remote_read_ephemeral() {
    let realm_id = RealmId::from_bytes([71; 32]);
    let owner = UserId::local(Ulid::from_bytes([72; 16]), realm_id);
    let group_id = Ulid::from_bytes([73; 16]);
    let version_id = Ulid::from_bytes([74; 16]);
    let client = bao_node(realm_id).await;
    let source = bao_node(realm_id).await;
    client.net.add_peer_addr(source.net.endpoint_addr()).await;
    source.net.add_peer_addr(client.net.endpoint_addr()).await;
    let Event::Blob(BlobEvent::WriteFinished { location }) = source
        .driver
        .blob_handle
        .as_ref()
        .unwrap()
        .send_blob_effect(BlobEffect::Write {
            resolved: aruna_core::structs::storage::blob::ResolvedBackend::node_default(),
            bucket: "remote".to_string(),
            key: "payload".to_string(),
            created_by: owner,
            blob: byte_stream(FIXTURE_BYTES),
            size: None,
        })
        .await
    else {
        panic!("source blob write failed")
    };
    let hash: [u8; 32] = location.get_blake3().unwrap().try_into().unwrap();
    seed_bao(
        &source,
        client.net.node_id(),
        owner,
        group_id,
        version_id,
        &location,
    )
    .await;
    let key_spaces = [
        BLOB_HEAD_KEYSPACE,
        BLOB_LOCATIONS_KEYSPACE,
        BLOB_VERSIONS_KEYSPACE,
        PATHS_INDEX_KEYSPACE,
    ];
    for key_space in key_spaces {
        assert_eq!(keyspace_len(client.driver.as_ref(), key_space).await, 0);
    }
    let exact = VersionedObjectArn::new(
        realm_id,
        source.net.node_id(),
        "remote",
        "payload",
        version_id,
    )
    .unwrap();
    let candidate = ExportCandidate {
        source: CandidateSource::RemoteExact {
            node_id: source.net.node_id(),
            target: exact,
        },
        report_source: ExportReportSource::Remote,
        resolved_version: Some(version_id),
        expected_blake3: Some(hash),
    };

    let CandidateOpen::Opened(BaoReadOutput::Stream {
        mut blob,
        size,
        blake3,
        ..
    }) = open_candidate(
        client.driver.as_ref(),
        &remote_spec(realm_id, owner),
        &candidate,
        false,
    )
    .await
    .unwrap()
    else {
        panic!("remote Bao read did not open")
    };
    let mut received = Vec::new();
    while let Some(chunk) = blob.next().await {
        received.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(received, FIXTURE_BYTES);
    assert_eq!(size, FIXTURE_BYTES.len() as u64);
    assert_eq!(blake3, hash);
    for key_space in key_spaces {
        assert_eq!(keyspace_len(client.driver.as_ref(), key_space).await, 0);
    }
    client.net.shutdown().await;
    source.net.shutdown().await;
}

fn job_context(driver: Arc<DriverContext>, owner_node_id: NodeId) -> JobContext {
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

/// Seeds one node with a locally stored object and the checkpoint candidate
/// that names it.
async fn local_candidate() -> (BaoNode, UserId, ExportCandidate) {
    let realm_id = RealmId::from_bytes([101; 32]);
    let owner = UserId::local(Ulid::from_bytes([102; 16]), realm_id);
    let group_id = Ulid::from_bytes([103; 16]);
    let version_id = Ulid::from_bytes([104; 16]);
    let node = bao_node(realm_id).await;
    let node_id = node.net.node_id();
    let Event::Blob(BlobEvent::WriteFinished { location }) = node
        .driver
        .blob_handle
        .as_ref()
        .unwrap()
        .send_blob_effect(BlobEffect::Write {
            resolved: aruna_core::structs::storage::blob::ResolvedBackend::node_default(),
            bucket: "remote".to_string(),
            key: "payload".to_string(),
            created_by: owner,
            blob: byte_stream(FIXTURE_BYTES),
            size: None,
        })
        .await
    else {
        panic!("local blob write failed")
    };
    let hash: [u8; 32] = location.get_blake3().unwrap().try_into().unwrap();
    seed_bao(&node, node_id, owner, group_id, version_id, &location).await;
    let candidate = ExportCandidate {
        source: CandidateSource::Local {
            location,
            group_id,
            permission_path: object_permission_path(
                realm_id, group_id, node_id, "remote", "payload",
            ),
            node_id,
            bucket: "remote".to_string(),
            key: "payload".to_string(),
        },
        report_source: ExportReportSource::Local,
        resolved_version: Some(version_id),
        expected_blake3: Some(hash),
    };
    (node, owner, candidate)
}

#[tokio::test]
async fn revalidates_local_candidate() {
    // A checkpoint outlives the state it was planned from, so a candidate
    // naming a stale group, path, or hash must be refused, not opened.
    let (node, owner, candidate) = local_candidate().await;
    let spec = remote_spec(owner.realm_id, owner);
    let driver = node.driver.as_ref();

    assert!(matches!(
        open_candidate(driver, &spec, &candidate, true).await.unwrap(),
        CandidateOpen::Opened(BaoReadOutput::Metadata { blake3, .. })
            if Some(blake3) == candidate.expected_blake3
    ));

    let mut foreign_group = candidate.clone();
    let CandidateSource::Local { group_id, .. } = &mut foreign_group.source else {
        panic!("expected a local candidate")
    };
    *group_id = Ulid::from_bytes([105; 16]);
    let mut foreign_path = candidate.clone();
    let CandidateSource::Local {
        permission_path, ..
    } = &mut foreign_path.source
    else {
        panic!("expected a local candidate")
    };
    *permission_path = format!("{permission_path}-sibling");
    let mut stale_hash = candidate.clone();
    stale_hash.expected_blake3 = Some([106; 32]);

    for stale in [foreign_group, foreign_path, stale_hash] {
        assert!(matches!(
            open_candidate(driver, &spec, &stale, true).await.unwrap(),
            CandidateOpen::Status(OpenStatus::Missing)
        ));
    }
    node.net.shutdown().await;
}

#[tokio::test]
async fn denies_local_read() {
    // Each refusal toggles one input away from an open that succeeded: first
    // the requesting identity, then a realm policy denying the export read.
    let (node, owner, candidate) = local_candidate().await;
    let realm_id = owner.realm_id;
    let driver = node.driver.as_ref();

    assert!(matches!(
        open_candidate(driver, &remote_spec(realm_id, owner), &candidate, true)
            .await
            .unwrap(),
        CandidateOpen::Opened(_)
    ));

    let stranger = UserId::local(Ulid::from_bytes([107; 16]), realm_id);
    assert!(matches!(
        open_candidate(driver, &remote_spec(realm_id, stranger), &candidate, true)
            .await
            .unwrap(),
        CandidateOpen::Status(OpenStatus::Denied)
    ));

    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(node.net.node_id(), RealmNodeKind::Server);
    config
        .request_policies
        .push(aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::from_bytes([108; 16]),
            name: "no export reads".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: "operation == 's3.GetObject'".to_string(),
            enabled: true,
        });
    let actor = Actor {
        node_id: node.net.node_id(),
        user_id: owner,
        realm_id,
    };
    assert!(matches!(
        driver
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: realm_id.as_bytes().to_vec().into(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    assert!(matches!(
        open_candidate(driver, &remote_spec(realm_id, owner), &candidate, true)
            .await
            .unwrap(),
        CandidateOpen::Status(OpenStatus::Denied)
    ));
    node.net.shutdown().await;
}

#[tokio::test]
async fn alias_needs_scopes() {
    // A hash alias is authorized only from rules and policy loaded for its
    // own group; an unloaded scope must fail closed instead of allowing.
    let (node, owner, candidate) = local_candidate().await;
    let CandidateSource::Local {
        group_id,
        permission_path,
        ..
    } = &candidate.source
    else {
        panic!("expected a local candidate")
    };
    let spec = remote_spec(owner.realm_id, owner);
    let ctx = job_context(node.driver.clone(), node.net.node_id());
    let auth = &spec.auth_context;
    let config = PermissionRulesConfig {
        auth_context: auth.clone(),
        path: format!("/{}/g/{group_id}", auth.realm_id),
    };
    let loaded = drive(PermissionRulesOperation::new(config), ctx.driver.as_ref());
    let rules = BTreeMap::from([(*group_id, loaded.await.unwrap())]);
    let scope = (auth.realm_id, *group_id);
    let loaded = drive(PolicyBulkOperation::new([scope]), ctx.driver.as_ref());
    let evaluator = loaded.await.unwrap().remove(&scope).unwrap();
    let policies = BTreeMap::from([(*group_id, evaluator)]);

    assert!(alias_allowed(&spec, *group_id, permission_path, &rules, &policies).unwrap());
    assert!(!alias_allowed(&spec, *group_id, "/other/object", &rules, &policies).unwrap());
    assert!(matches!(
        alias_allowed(&spec, *group_id, permission_path, &BTreeMap::new(), &policies),
        Err(ExportFailure::Retryable(message)) if message == "authorization rules unavailable"
    ));
    assert!(matches!(
        alias_allowed(&spec, *group_id, permission_path, &rules, &BTreeMap::new()),
        Err(ExportFailure::Retryable(message)) if message == "object policy unavailable"
    ));
    node.net.shutdown().await;
}

#[tokio::test]
async fn denies_foreign_alias() {
    // A hash alias in a group whose authorization document this node cannot
    // read must deny that alias, not fail the export with a retryable error.
    let (node, owner, candidate) = local_candidate().await;
    let realm_id = owner.realm_id;
    let hash = candidate.expected_blake3.unwrap();
    let foreign = Ulid::from_bytes([200; 16]);
    let alias = HashIndex::new(
        hash,
        Ulid::from_bytes([201; 16]),
        realm_id,
        foreign,
        node.net.node_id(),
        "restricted",
        "secret.csv",
    );
    assert!(matches!(
        node.driver
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: PATHS_INDEX_KEYSPACE.to_string(),
                key: alias.to_bytes().unwrap().into(),
                value: Vec::new().into(),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    let spec = remote_spec(realm_id, owner);
    let ctx = job_context(node.driver.clone(), node.net.node_id());
    let found = hash_lookup(&ctx, &spec, hash)
        .await
        .expect("a foreign group must deny instead of failing the export");

    assert!(found.denied);
    assert!(!found.candidates.iter().any(|candidate| matches!(
        &candidate.source,
        CandidateSource::Local { group_id, .. } if *group_id == foreign
    )));
    node.net.shutdown().await;
}

#[tokio::test]
async fn foreign_arn_located() {
    // A versioned ARN of another realm with an `s3://` location of this node resolves to the
    // current version there, like a File entity without an ARN.
    let (node, owner, candidate) = local_candidate().await;
    let realm_id = owner.realm_id;
    let version = candidate.resolved_version.unwrap();
    let head = aruna_core::structs::storage::blob::CurrentVersionPointer::new(version);
    let head_key = aruna_core::structs::storage::blob::BlobHeadKey::new("remote", "payload");
    let write = StorageEffect::Write {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        key: head_key.to_bytes().unwrap().into(),
        value: head.to_bytes().unwrap().into(),
        txn_id: None,
    };
    node.driver.storage_handle.send_storage_effect(write).await;
    let other = RealmId::from_bytes([107; 32]);
    let foreign = VersionedObjectArn::new(
        other,
        node.net.node_id(),
        "other",
        "file",
        Ulid::from_bytes([108; 16]),
    )
    .unwrap();
    let document = json!({"@graph": [{"@id": foreign.to_string(), "@type": "File",
        "contentUrl": "s3://remote/payload"}]});
    let mut checkpoint = ExportCheckpoint {
        entities: recognized_entities(&document, realm_id).unwrap(),
        ..Default::default()
    };
    let ctx = job_context(node.driver.clone(), node.net.node_id());
    let resolve = ResolveEntriesOperation::new(
        scope_of(&ctx, &remote_spec(realm_id, owner)),
        std::mem::take(&mut checkpoint.entities),
        AliasState::default(),
    );
    checkpoint.entities = drive(resolve, ctx.driver.as_ref()).await.unwrap().entities;
    let entity = &checkpoint.entities[0];
    assert_eq!(entity.omission, None);
    assert!(entity.candidates.iter().any(|found| matches!(
        &found.source,
        CandidateSource::Local { bucket, key, .. } if bucket == "remote" && key == "payload"
    ) && found.resolved_version == Some(version)));
    node.net.shutdown().await;
}

fn crate_document(parts: &[JsonValue]) -> JsonValue {
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

/// Writes a bucket's encryption settings row on `node`.
async fn encrypting_bucket(node: &BaoNode, bucket: &str, bucket_id: Ulid) {
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

#[tokio::test]
async fn sealed_local_export() {
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::structs::storage::encryption::{SealPlan, public_key_of};
    use aruna_core::structs::storage::format::StoredLayout;

    let realm_id = RealmId::from_bytes([111; 32]);
    let owner = UserId::local(Ulid::from_bytes([112; 16]), realm_id);
    let group_id = Ulid::from_bytes([113; 16]);
    let version_id = Ulid::from_bytes([114; 16]);
    let node = bao_node(realm_id).await;
    let node_id = node.net.node_id();
    let blob = node.driver.blob_handle.as_ref().unwrap();
    let private = || SecretBytes::new(vec![9; 32]);
    let key = BucketKeyRef::new(Ulid::from_bytes([115; 16]), 1);
    let plan = SealPlan {
        key,
        public_key: public_key_of(&private()).unwrap(),
        cipher: Default::default(),
        block_keys: Default::default(),
        storage_generation: 1,
    };
    let resolved = aruna_core::structs::storage::blob::ResolvedBackend::node_default()
        .with_encryption(Some(plan));
    let Event::Blob(BlobEvent::WriteFinished { location }) = blob
        .send_blob_effect(BlobEffect::Write {
            resolved,
            bucket: "remote".to_string(),
            key: "payload".to_string(),
            created_by: owner,
            blob: byte_stream(FIXTURE_BYTES),
            size: None,
        })
        .await
    else {
        panic!("sealed blob write failed")
    };
    assert!(matches!(location.format.layout, StoredLayout::Pithos(_)));
    let hash: [u8; 32] = location.get_blake3().unwrap().try_into().unwrap();
    seed_bao(&node, node_id, owner, group_id, version_id, &location).await;
    let candidate = ExportCandidate {
        source: CandidateSource::Local {
            location,
            group_id,
            permission_path: object_permission_path(
                realm_id, group_id, node_id, "remote", "payload",
            ),
            node_id,
            bucket: "remote".to_string(),
            key: "payload".to_string(),
        },
        report_source: ExportReportSource::Local,
        resolved_version: Some(version_id),
        expected_blake3: Some(hash),
    };
    let spec = remote_spec(realm_id, owner);
    let driver = node.driver.as_ref();

    // Locked, the input parks the export on its key instead of counting as offline.
    let opened = open_candidate(driver, &spec, &candidate, false).await;
    let wait = match opened {
        Err(ExportFailure::KeyLocked(wait)) => wait,
        Err(error) => panic!("a locked input must wait for its key, got {error:?}"),
        Ok(CandidateOpen::Status(status)) => panic!("a locked input got status {status:?}"),
        Ok(CandidateOpen::Opened(_)) => panic!("a locked input must not open"),
    };
    assert_eq!((wait.key, wait.node_id), (key, node_id));
    assert!(matches!(
        failure_outcome(ExportFailure::KeyLocked(wait)),
        JobRunOutcome::AwaitingKey(waits) if waits.len() == 1
    ));

    // Unlocked, the sealed copy is read under a lease and yields the original bytes.
    let prepare = BlobEffect::PrepareKey {
        key,
        public_key: plan.public_key,
        private_key: SharedSecret::new(private()),
        duration: None,
        max: None,
    };
    let Event::Blob(BlobEvent::KeyPrepared { ticket }) = blob.send_blob_effect(prepare).await
    else {
        panic!("prepare failed")
    };
    blob.send_blob_effect(BlobEffect::ActivateKey { ticket })
        .await;
    let CandidateOpen::Opened(BaoReadOutput::Stream { mut blob, .. }) =
        open_candidate(driver, &spec, &candidate, false)
            .await
            .unwrap()
    else {
        panic!("an unlocked sealed input must open")
    };
    let mut received = Vec::new();
    while let Some(chunk) = blob.next().await {
        received.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(received, FIXTURE_BYTES);

    // An export into another realm refuses this copy for a reader without the key.
    let reader = UserId::local(Ulid::from_bytes([116; 16]), realm_id);
    let mut group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    let viewer = group_auth
        .roles
        .values_mut()
        .find(|role| role.name == "viewer");
    viewer.unwrap().assigned_users.insert(reader);
    let group = aruna_core::structs::identity::group::Group {
        display_name: "export".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let key = group_id.to_bytes().to_vec();
    let writes = vec![
        (
            AUTH_KEYSPACE.to_string(),
            key.clone().into(),
            group_auth.to_bytes(&actor).unwrap().into(),
        ),
        (
            GROUP_KEYSPACE.to_string(),
            key.into(),
            group.to_bytes(&actor).unwrap().into(),
        ),
    ];
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    driver.storage_handle.send_storage_effect(write).await;
    let mut spec = remote_spec(realm_id, reader);
    spec.selection = Some(aruna_core::structs::execution::job::ExportSelection {
        files: Vec::new(),
        audience: RealmId::from_bytes([117; 32]),
        intent_digest: String::new(),
    });
    assert!(matches!(
        open_candidate(driver, &spec, &candidate, true).await,
        Ok(CandidateOpen::Status(OpenStatus::NotHolder))
    ));
    node.net.shutdown().await;
}

#[tokio::test]
async fn locked_remote_export() {
    // The source bucket encrypts but its copy is not converted yet; it is still gated.
    let realm_id = RealmId::from_bytes([121; 32]);
    let owner = UserId::local(Ulid::from_bytes([122; 16]), realm_id);
    let group_id = Ulid::from_bytes([123; 16]);
    let version_id = Ulid::from_bytes([124; 16]);
    let client = bao_node(realm_id).await;
    let source = bao_node(realm_id).await;
    client.net.add_peer_addr(source.net.endpoint_addr()).await;
    source.net.add_peer_addr(client.net.endpoint_addr()).await;
    let Event::Blob(BlobEvent::WriteFinished { location }) = source
        .driver
        .blob_handle
        .as_ref()
        .unwrap()
        .send_blob_effect(BlobEffect::Write {
            resolved: aruna_core::structs::storage::blob::ResolvedBackend::node_default(),
            bucket: "remote".to_string(),
            key: "payload".to_string(),
            created_by: owner,
            blob: byte_stream(FIXTURE_BYTES),
            size: None,
        })
        .await
    else {
        panic!("source blob write failed")
    };
    let hash: [u8; 32] = location.get_blake3().unwrap().try_into().unwrap();
    seed_bao(
        &source,
        client.net.node_id(),
        owner,
        group_id,
        version_id,
        &location,
    )
    .await;
    encrypting_bucket(&source, "remote", Ulid::from_bytes([125; 16])).await;
    let exact = VersionedObjectArn::new(
        realm_id,
        source.net.node_id(),
        "remote",
        "payload",
        version_id,
    )
    .unwrap();
    let candidate = ExportCandidate {
        source: CandidateSource::RemoteExact {
            node_id: source.net.node_id(),
            target: exact,
        },
        report_source: ExportReportSource::Remote,
        resolved_version: Some(version_id),
        expected_blake3: Some(hash),
    };

    let opened = open_candidate(
        client.driver.as_ref(),
        &remote_spec(realm_id, owner),
        &candidate,
        false,
    )
    .await;
    let Err(ExportFailure::RemoteLocked {
        node_id, target, ..
    }) = opened
    else {
        panic!("a locked remote input must be typed, got another outcome")
    };
    // The refusal keeps the exact version the read named, not only its content.
    let exact = aruna_core::jobs::WaitTarget::Object {
        bucket: "remote".to_string(),
        key: "payload".to_string(),
        version_id,
    };
    assert_eq!((node_id, target), (source.net.node_id(), exact));
    client.net.shutdown().await;
    source.net.shutdown().await;
}

#[tokio::test]
async fn plaintext_export_parks() {
    // The remote copy is not converted yet; its encrypting bucket's locked key parks the export.
    let realm_id = RealmId::from_bytes([131; 32]);
    let owner = UserId::local(Ulid::from_bytes([132; 16]), realm_id);
    let group_id = Ulid::from_bytes([133; 16]);
    let version_id = Ulid::from_bytes([134; 16]);
    let bucket_id = Ulid::from_bytes([135; 16]);
    let client = bao_node(realm_id).await;
    let source = node_with(realm_id, true).await;
    client.net.add_peer_addr(source.net.endpoint_addr()).await;
    source.net.add_peer_addr(client.net.endpoint_addr()).await;
    let Event::Blob(BlobEvent::WriteFinished { location }) = source
        .driver
        .blob_handle
        .as_ref()
        .unwrap()
        .send_blob_effect(BlobEffect::Write {
            resolved: aruna_core::structs::storage::blob::ResolvedBackend::node_default(),
            bucket: "remote".to_string(),
            key: "payload".to_string(),
            created_by: owner,
            blob: byte_stream(FIXTURE_BYTES),
            size: None,
        })
        .await
    else {
        panic!("source blob write failed")
    };
    let hash: [u8; 32] = location.get_blake3().unwrap().try_into().unwrap();
    seed_bao(
        &source,
        client.net.node_id(),
        owner,
        group_id,
        version_id,
        &location,
    )
    .await;
    encrypting_bucket(&source, "remote", bucket_id).await;
    let alias = aruna_core::structs::storage::blob::HashIndex::new(
        hash,
        version_id,
        realm_id,
        group_id,
        source.net.node_id(),
        "remote",
        "payload",
    );
    let event = source
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: aruna_core::keyspaces::PATHS_INDEX_KEYSPACE.to_string(),
            key: alias.to_bytes().unwrap().into(),
            value: Vec::new().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    let exact = VersionedObjectArn::new(
        realm_id,
        source.net.node_id(),
        "remote",
        "payload",
        version_id,
    )
    .unwrap();
    let candidate = ExportCandidate {
        source: CandidateSource::RemoteExact {
            node_id: source.net.node_id(),
            target: exact,
        },
        report_source: ExportReportSource::Remote,
        resolved_version: Some(version_id),
        expected_blake3: Some(hash),
    };
    let opened = open_candidate(
        client.driver.as_ref(),
        &remote_spec(realm_id, owner),
        &candidate,
        false,
    )
    .await;
    let Err(ExportFailure::RemoteLocked {
        node_id,
        target,
        auth,
    }) = opened
    else {
        panic!("a locked remote input must be typed, got another outcome")
    };

    // The source registers the wait and names the dependency, so the export parks.
    let ask = AskWaitsOperation::new(
        &auth,
        JobId::from_bytes([84; 16]),
        node_id,
        std::slice::from_ref(&target),
        client.driver.node_and_realm(),
    );
    let waits = drive(ask, client.driver.as_ref()).await.unwrap();
    assert_eq!(waits.len(), 1);
    assert_eq!(waits[0].node_id, source.net.node_id());
    assert_eq!(waits[0].bucket, "remote");
    assert_eq!(
        waits[0].key,
        aruna_core::structs::storage::encryption::BucketKeyRef::new(bucket_id, 1)
    );
    client.net.shutdown().await;
    source.net.shutdown().await;
}

#[tokio::test]
async fn selection_reads_locally() {
    // An export into another realm never reads a selected file through another node.
    let realm_id = RealmId::from_bytes([64; 32]);
    let node = bao_node(realm_id).await;
    let hash = [65; 32];
    let candidate = ExportCandidate {
        source: CandidateSource::RemoteHash {
            node_id: iroh::SecretKey::from_bytes(&[66; 32]).public(),
            hash,
        },
        report_source: ExportReportSource::Hash,
        resolved_version: None,
        expected_blake3: Some(hash),
    };
    let mut spec = remote_spec(realm_id, UserId::nil(realm_id));
    spec.selection = Some(aruna_core::structs::execution::job::ExportSelection {
        files: vec!["data.csv".to_string()],
        audience: RealmId::from_bytes([67; 32]),
        intent_digest: String::new(),
    });
    let opened = open_candidate(node.driver.as_ref(), &spec, &candidate, true).await;
    assert!(matches!(
        opened.unwrap(),
        CandidateOpen::Status(OpenStatus::Denied)
    ));
    node.net.shutdown().await;
}

/// One payload stored twice on a node: a plain copy in encrypting bucket `remote` and one in
/// plain bucket `plain`, both with a hash alias. A viewer reads both but holds no bucket key.
async fn two_copies() -> (BaoNode, UserId, UserId, [ExportCandidate; 2]) {
    let realm_id = RealmId::from_bytes([131; 32]);
    let owner = UserId::local(Ulid::from_bytes([132; 16]), realm_id);
    let viewer = UserId::local(Ulid::from_bytes([133; 16]), realm_id);
    let group_id = Ulid::from_bytes([134; 16]);
    let versions = [Ulid::from_bytes([135; 16]), Ulid::from_bytes([136; 16])];
    let node = bao_node(realm_id).await;
    let node_id = node.net.node_id();
    let blob = node.driver.blob_handle.as_ref().unwrap();
    let Event::Blob(BlobEvent::WriteFinished { location }) = blob
        .send_blob_effect(BlobEffect::Write {
            resolved: aruna_core::structs::storage::blob::ResolvedBackend::node_default(),
            bucket: "remote".to_string(),
            key: "payload".to_string(),
            created_by: owner,
            blob: byte_stream(FIXTURE_BYTES),
            size: None,
        })
        .await
    else {
        panic!("blob write failed")
    };
    let hash: [u8; 32] = location.get_blake3().unwrap().try_into().unwrap();
    seed_bao(&node, node_id, owner, group_id, versions[0], &location).await;
    encrypting_bucket(&node, "remote", Ulid::from_bytes([137; 16])).await;
    let mut group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    let role = group_auth
        .roles
        .values_mut()
        .find(|role| role.name == "viewer");
    role.unwrap().assigned_users.insert(viewer);
    let group = aruna_core::structs::identity::group::Group {
        display_name: "export".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
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
    let version = BlobVersion::materialized(
        hash,
        BackendRef::node_default(),
        location.format.encoding(),
        std::time::SystemTime::UNIX_EPOCH,
        owner,
        None,
    );
    let group_key = group_id.to_bytes().to_vec();
    let plain_key = VersionKey::new("plain", "payload", versions[1]);
    let mut writes = vec![
        (
            AUTH_KEYSPACE.to_string(),
            group_key.clone().into(),
            group_auth.to_bytes(&actor).unwrap().into(),
        ),
        (
            GROUP_KEYSPACE.to_string(),
            group_key.into(),
            group.to_bytes(&actor).unwrap().into(),
        ),
        (
            S3_BUCKET_KEYSPACE.to_string(),
            b"plain".to_vec().into(),
            bucket.to_bytes().unwrap().into(),
        ),
        (
            BLOB_VERSIONS_KEYSPACE.to_string(),
            plain_key.to_bytes().unwrap().into(),
            version.to_bytes().unwrap().into(),
        ),
    ];
    let buckets = ["remote", "plain"];
    for (bucket, version_id) in buckets.into_iter().zip(versions) {
        let alias = HashIndex::new(
            hash, version_id, realm_id, group_id, node_id, bucket, "payload",
        );
        let key = alias.to_bytes().unwrap().into();
        writes.push((PATHS_INDEX_KEYSPACE.to_string(), key, Vec::new().into()));
    }
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    assert!(matches!(
        node.driver.storage_handle.send_storage_effect(write).await,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    let candidates = [0, 1].map(|index| ExportCandidate {
        source: CandidateSource::Local {
            location: location.clone(),
            group_id,
            permission_path: object_permission_path(
                realm_id,
                group_id,
                node_id,
                buckets[index],
                "payload",
            ),
            node_id,
            bucket: buckets[index].to_string(),
            key: "payload".to_string(),
        },
        report_source: ExportReportSource::Hash,
        resolved_version: Some(versions[index]),
        expected_blake3: Some(hash),
    });
    (node, owner, viewer, candidates)
}

#[tokio::test]
async fn unheld_copy_skipped() {
    // A copy whose bucket key the caller lacks does not stop an export into another realm from
    // reading another copy; only when none opens does the job fail with the holder message.
    let (node, _, viewer, candidates) = two_copies().await;
    let ctx = job_context(node.driver.clone(), node.net.node_id());
    let mut spec = remote_spec(viewer.realm_id, viewer);
    spec.selection = Some(aruna_core::structs::execution::job::ExportSelection {
        files: vec!["data/payload".to_string()],
        audience: RealmId::from_bytes([138; 32]),
        intent_digest: String::new(),
    });
    let entity = |candidates: &[ExportCandidate]| ExportEntity {
        entity_id: "data/payload".to_string(),
        local_path: None,
        storage_key: None,
        exact: None,
        hash: candidates[0].expected_blake3,
        hash_realm: None,
        candidates: candidates.to_vec(),
        omission: None,
        message: None,
        zip_path: None,
        report_source: None,
        resolved_version: None,
        path_synthesized: false,
        content: None,
    };
    let mut checkpoint = ExportCheckpoint {
        entities: vec![entity(&candidates)],
        ..Default::default()
    };
    let probed = probe_sources(&ctx, &spec, &mut checkpoint, &BTreeMap::new()).await;
    assert_eq!(probed.unwrap()[0].candidate_index, 1);
    let mut checkpoint = ExportCheckpoint {
        entities: vec![entity(&candidates[..1])],
        ..Default::default()
    };
    let refused = aruna_core::transfer::GrantError::NotHolder("remote".to_string());
    assert!(matches!(
        probe_sources(&ctx, &spec, &mut checkpoint, &BTreeMap::new()).await,
        Err(ExportFailure::Permanent(message)) if message == refused.to_string()
    ));
    node.net.shutdown().await;
}

#[tokio::test]
async fn consent_resolves_hashes() {
    // Consent resolves a file named only by its hash as the job does: one copy the caller may
    // read is enough, and a file whose every copy needs a key the caller lacks is refused.
    let (node, owner, viewer, candidates) = two_copies().await;
    let node_id = node.net.node_id();
    let hash = hex::encode(candidates[0].expected_blake3.unwrap());
    let file = format!(
        "{}{hash}",
        aruna_core::structs::storage::replication::ARUNA_DATA_PREFIX
    );
    let jsonld = crate_document(&[json!({"@id": file, "@type": "File", "name": "payload"})]);
    let jsonld = jsonld.to_string();
    let spec = |user_id| {
        let mut spec = remote_spec(viewer.realm_id, user_id);
        spec.selection = Some(aruna_core::structs::execution::job::ExportSelection {
            files: vec![file.clone()],
            audience: RealmId::from_bytes([138; 32]),
            intent_digest: String::new(),
        });
        spec
    };
    let (viewing, owning) = (spec(viewer), spec(owner));
    let unheld = unheld_bucket(&node.driver, &viewing, node_id, &jsonld).await;
    assert_eq!(unheld, Ok(None));
    encrypting_bucket(&node, "plain", Ulid::from_bytes([139; 16])).await;
    let unheld = unheld_bucket(&node.driver, &viewing, node_id, &jsonld).await;
    assert!(matches!(unheld, Ok(Some(bucket)) if bucket == "plain" || bucket == "remote"));
    let unheld = unheld_bucket(&node.driver, &owning, node_id, &jsonld).await;
    assert_eq!(unheld, Ok(None));
    // A foreign user is refused like any non-holder, not with a storage failure.
    let foreign = UserId::new(viewer.user_ulid, RealmId::from_bytes([140; 32]));
    let mut foreign_spec = spec(viewer);
    foreign_spec.auth_context.user_id = foreign;
    let CandidateSource::Local { bucket, .. } = &candidates[0].source else {
        panic!("a local copy")
    };
    let auth = &foreign_spec.auth_context;
    let holder = HolderCheckOperation::new(auth.realm_id, bucket, auth.user_id);
    let allowed = drive(holder, node.driver.as_ref()).await;
    assert!(matches!(allowed, Ok(false)));
    node.net.shutdown().await;
}

fn scope_of(ctx: &JobContext, spec: &ExportRoCrateSpec) -> SourceScope {
    SourceScope {
        spec: Arc::new(spec.clone()),
        owner_node: ctx.owner_node_id,
    }
}

async fn open_candidate(
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

async fn hash_lookup(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    hash: [u8; 32],
) -> Result<HashFound, ExportFailure> {
    let lookup = HashCandidatesOperation::new(
        scope_of(ctx, spec),
        hash,
        None,
        Vec::new(),
        AliasState::default(),
    );
    drive(lookup, ctx.driver.as_ref()).await
}

async fn probe_sources(
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

async fn unheld_bucket(
    driver: &DriverContext,
    spec: &ExportRoCrateSpec,
    node_id: NodeId,
    jsonld: &str,
) -> Result<Option<String>, String> {
    let check = UnheldBucketOperation::new(spec.clone(), node_id, jsonld.to_string());
    drive(check, driver).await.map_err(|error| error.0)
}

pub(super) fn destination() -> aruna_core::repository::RepositoryDestination {
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
