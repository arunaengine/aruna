//! Driven two-node remote (Bao read) RO-Crate export integration.
//!
//! The exporter node holds only the metadata document; the payload version lives
//! solely on a remote holder. `run_export_job` must resolve the File entity to
//! the holder, fetch the bytes over the real net stack, and stream them into the
//! artifact without ever registering the fetched blob locally.

use std::io::{Cursor, Read};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use aruna_blob::blob::BlobHandler;
use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, HASH_PATHS_INDEX_KEYSPACE,
    REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    ARUNA_DATA_PREFIX, Actor, ArtifactRef, AuthContext, Backend, BackendConfig, BackendLocation,
    BlobVersion, BucketInfo, ExportReportRow, ExportReportSource, ExportRoCrateSpec,
    GroupAuthorizationDocument, JobId, JobPayload, JobRecord, JobResultPayload,
    MetadataRegistryRecord, PathRestriction, Permission, RealmAuthorizationDocument,
    RealmConfigDocument, RealmId, RealmNodeKind, ReasonCode, RoCrateLimits, VersionKey,
    VersionedObjectArn,
};
use aruna_core::types::{GroupId, UserId};
use aruna_core::util::unix_timestamp_millis;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::jobs::executor::{JobContext, JobRunOutcome, ProgressReporter};
use aruna_operations::jobs::export::run_export_job;
use aruna_operations::jobs::service::read_artifact_range;
use aruna_operations::jobs::store::{
    ClaimOutcome, claim_job, insert_job, list_job_entries, read_job_record, transition_to_running,
};
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::metadata::materialization_queue::process_metadata_materialization_batch;
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

const BUCKET: &str = "remote";
const KEY: &str = "payload";
const DOC_PATH: &str = "datasets/remote";
const PAYLOAD: &[u8] = b"remote bao export payload";

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct Node {
    _root: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

#[tokio::test]
async fn remote_export_streams() -> TestResult {
    // A full export resolves the File entity to the holder and streams its bytes.
    let realm_id = RealmId::from_bytes([53; 32]);
    let owner = UserId::local(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    let version_id = Ulid::generate();
    let document_id = Ulid::generate();

    let (holder, exporter) =
        setup_remote(realm_id, owner, group_id, version_id, document_id).await?;

    let spec = ExportRoCrateSpec {
        auth_context: AuthContext {
            user_id: owner,
            realm_id,
            path_restrictions: None,
        },
        document_id,
        limits: RoCrateLimits::default(),
    };
    let job_id = JobId::new();
    let ctx = claim_context(
        &exporter,
        owner,
        job_id,
        JobPayload::ExportRoCrate(spec.clone()),
    )
    .await?;
    let JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(result)) =
        run_export_job(&ctx, &spec).await
    else {
        return Err("remote RO-Crate export did not succeed".into());
    };
    assert_eq!(result.included, 1);
    assert_eq!(result.omitted, Default::default());

    let rows = export_rows(&exporter.context, job_id).await?;
    let included = rows
        .iter()
        .find(|row| row.code == ReasonCode::Included)
        .ok_or("included report row is missing")?;
    assert_eq!(included.detail.source, Some(ExportReportSource::Remote));
    assert_eq!(included.detail.resolved_version, Some(version_id));

    let artifact = result.artifact.ok_or("export artifact is missing")?;
    let bytes = artifact_bytes(&exporter.context, &artifact).await?;
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))?;
    let payload_name = archive_names(&mut archive)?
        .into_iter()
        .find(|name| name.starts_with("data/"))
        .ok_or("payload entry is missing from the artifact")?;
    let mut payload = Vec::new();
    archive.by_name(&payload_name)?.read_to_end(&mut payload)?;
    assert_eq!(payload, PAYLOAD);
    assert!(archive.by_name("ro-crate-metadata.json").is_ok());

    // The fetched bytes streamed straight into the artifact; the exporter never
    // registered a blob location or hash-path alias for them.
    assert_eq!(
        keyspace_len(&exporter.context, BLOB_LOCATIONS_KEYSPACE).await?,
        0
    );
    assert_eq!(
        keyspace_len(&exporter.context, HASH_PATHS_INDEX_KEYSPACE).await?,
        0
    );

    holder.net.shutdown().await;
    exporter.net.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn remote_denial_omits() -> TestResult {
    // The holder refuses the wire read when the exporter's auth lacks payload READ.
    let realm_id = RealmId::from_bytes([57; 32]);
    let owner = UserId::local(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    let version_id = Ulid::generate();
    let document_id = Ulid::generate();

    let (holder, exporter) =
        setup_remote(realm_id, owner, group_id, version_id, document_id).await?;

    let metadata_path =
        MetadataRegistryRecord::permission_path_for(&realm_id, group_id, DOC_PATH, document_id);
    let spec = ExportRoCrateSpec {
        auth_context: AuthContext {
            user_id: owner,
            realm_id,
            path_restrictions: Some(vec![PathRestriction {
                pattern: metadata_path,
                permission: Permission::READ,
            }]),
        },
        document_id,
        limits: RoCrateLimits::default(),
    };
    let job_id = JobId::new();
    let ctx = claim_context(
        &exporter,
        owner,
        job_id,
        JobPayload::ExportRoCrate(spec.clone()),
    )
    .await?;
    let JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(result)) =
        run_export_job(&ctx, &spec).await
    else {
        return Err("denied remote RO-Crate export did not succeed".into());
    };
    assert_eq!(result.included, 0);
    assert_eq!(result.omitted.denied, 1);

    let artifact = result.artifact.ok_or("export artifact is missing")?;
    let bytes = artifact_bytes(&exporter.context, &artifact).await?;
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))?;
    assert!(
        !archive_names(&mut archive)?
            .iter()
            .any(|name| name.starts_with("data/")),
        "denied payload must be absent from the artifact"
    );
    assert!(archive.by_name("ro-crate-metadata.json").is_ok());

    holder.net.shutdown().await;
    exporter.net.shutdown().await;
    Ok(())
}

async fn setup_remote(
    realm_id: RealmId,
    owner: UserId,
    group_id: GroupId,
    version_id: Ulid,
    document_id: Ulid,
) -> Result<(Node, Node), Box<dyn std::error::Error>> {
    let holder = build_node(realm_id, false).await?;
    let exporter = build_node(realm_id, true).await?;
    connect(&exporter, &holder).await;

    let location = write_payload(&holder, owner).await?;
    let hash = seed_holder(
        &holder,
        exporter.net.node_id(),
        owner,
        group_id,
        realm_id,
        version_id,
        &location,
    )
    .await?;
    initialize_net_incoming(holder.context.clone());

    seed_exporter(&exporter, holder.net.node_id(), owner, group_id, realm_id).await?;
    let arn = VersionedObjectArn::new(realm_id, holder.net.node_id(), BUCKET, KEY, version_id)?;
    let jsonld = remote_crate_json(&arn.to_w3id(), &content_hash_w3id(hash));
    create_document(&exporter, owner, group_id, realm_id, document_id, jsonld).await?;
    Ok((holder, exporter))
}

async fn build_node(
    realm_id: RealmId,
    with_metadata: bool,
) -> Result<Node, Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let storage_path = root.path().join("storage");
    let blob_path = root.path().join("blob");
    std::fs::create_dir_all(&storage_path)?;
    std::fs::create_dir_all(&blob_path)?;
    let storage = FjallStorage::open(storage_path.to_str().ok_or("invalid storage path")?)?;
    let net = NetHandle::new(
        NetConfig {
            bind_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await?;
    let blob = BlobHandler::new(
        BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100_000),
            multipart_bucket: Some("multipart".to_string()),
            root: blob_path.to_str().ok_or("invalid blob path")?.to_string(),
            service_config: Default::default(),
            timeouts: Default::default(),
        },
        storage.clone(),
        net.clone(),
    )
    .await?;
    let metadata = if with_metadata {
        let metadata_path = root.path().join("metadata");
        std::fs::create_dir_all(&metadata_path)?;
        Some(MetadataHandle::new(
            &metadata_path,
            net.node_id(),
            storage.clone(),
            None,
            None,
            None,
        )?)
    } else {
        None
    };
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: Some(blob),
        metadata_handle: metadata,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });
    Ok(Node {
        _root: root,
        net,
        context,
    })
}

async fn connect(left: &Node, right: &Node) {
    left.net.add_peer_addr(right.net.endpoint_addr()).await;
    right.net.add_peer_addr(left.net.endpoint_addr()).await;
}

async fn write_payload(
    node: &Node,
    owner: UserId,
) -> Result<BackendLocation, Box<dyn std::error::Error>> {
    let blob = BackendStream::<Result<Bytes, StreamError>>::new(stream::once(async {
        Ok::<Bytes, std::io::Error>(Bytes::from_static(PAYLOAD))
    }));
    match node
        .context
        .blob_handle
        .as_ref()
        .ok_or("holder blob handle is missing")?
        .send_blob_effect(BlobEffect::Write {
            bucket: BUCKET.to_string(),
            key: KEY.to_string(),
            created_by: owner,
            blob,
        })
        .await
    {
        Event::Blob(BlobEvent::WriteFinished { location }) => Ok(location),
        event => Err(format!("holder payload write failed: {event:?}").into()),
    }
}

async fn seed_holder(
    node: &Node,
    peer: NodeId,
    owner: UserId,
    group_id: GroupId,
    realm_id: RealmId,
    version_id: Ulid,
    location: &BackendLocation,
) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let actor = Actor {
        node_id: node.net.node_id(),
        user_id: owner,
        realm_id,
    };
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(node.net.node_id(), RealmNodeKind::Server);
    config.ensure_node(peer, RealmNodeKind::Server);
    let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
    let group_auth = GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
    let bucket = BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: owner,
        cors_configuration: None,
    };
    let hash: [u8; 32] = location
        .get_blake3()
        .ok_or("holder payload has no blake3")?
        .try_into()
        .map_err(|_| "holder payload hash is not 32 bytes")?;
    let version = BlobVersion::materialized(hash, SystemTime::UNIX_EPOCH, owner, None);
    let version_key = VersionKey::new(BUCKET, KEY, version_id);
    let writes = vec![
        (
            REALM_CONFIG_KEYSPACE.to_string(),
            realm_id.as_bytes().to_vec().into(),
            config.to_bytes(&actor)?.into(),
        ),
        (
            AUTH_KEYSPACE.to_string(),
            realm_id.as_bytes().to_vec().into(),
            realm_auth.to_bytes(&actor)?.into(),
        ),
        (
            AUTH_KEYSPACE.to_string(),
            group_id.to_bytes().to_vec().into(),
            group_auth.to_bytes(&actor)?.into(),
        ),
        (
            S3_BUCKET_KEYSPACE.to_string(),
            BUCKET.as_bytes().to_vec().into(),
            bucket.to_bytes()?.into(),
        ),
        (
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version_key.to_bytes()?.into(),
            version.to_bytes()?.into(),
        ),
        (
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            hash.to_vec().into(),
            location.to_bytes()?.into(),
        ),
    ];
    match node
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(hash),
        event => Err(format!("holder seed failed: {event:?}").into()),
    }
}

async fn seed_exporter(
    node: &Node,
    peer: NodeId,
    owner: UserId,
    group_id: GroupId,
    realm_id: RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    let actor = Actor {
        node_id: node.net.node_id(),
        user_id: owner,
        realm_id,
    };
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.seed_default_placement();
    config.ensure_node(node.net.node_id(), RealmNodeKind::Server);
    config.ensure_node(peer, RealmNodeKind::Server);
    let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
    let group_auth = GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
    let writes = vec![
        (
            REALM_CONFIG_KEYSPACE.to_string(),
            realm_id.as_bytes().to_vec().into(),
            config.to_bytes(&actor)?.into(),
        ),
        (
            AUTH_KEYSPACE.to_string(),
            realm_id.as_bytes().to_vec().into(),
            realm_auth.to_bytes(&actor)?.into(),
        ),
        (
            AUTH_KEYSPACE.to_string(),
            group_id.to_bytes().to_vec().into(),
            group_auth.to_bytes(&actor)?.into(),
        ),
    ];
    match node
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        event => Err(format!("exporter seed failed: {event:?}").into()),
    }
}

async fn create_document(
    node: &Node,
    owner: UserId,
    group_id: GroupId,
    realm_id: RealmId,
    document_id: Ulid,
    jsonld: String,
) -> Result<(), Box<dyn std::error::Error>> {
    drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: node.net.node_id(),
                user_id: owner,
                realm_id,
            },
            group_id,
            document_id,
            document_path: DOC_PATH.to_string(),
            public: false,
            payload: CreateMetadataDocumentPayload::RoCrate { jsonld },
        }),
        node.context.as_ref(),
    )
    .await?;
    replay_metadata_event_log(node.context.as_ref()).await?;
    process_metadata_materialization_batch(node.context.as_ref()).await?;
    Ok(())
}

async fn claim_context(
    node: &Node,
    owner: UserId,
    job_id: JobId,
    payload: JobPayload,
) -> Result<JobContext, Box<dyn std::error::Error>> {
    let node_id = node.net.node_id();
    if read_job_record(&node.context.storage_handle, job_id, None)
        .await?
        .is_none()
    {
        let now = unix_timestamp_millis();
        insert_job(
            &node.context.storage_handle,
            &JobRecord::new(job_id, payload, owner, node_id, now, now, None),
        )
        .await?;
    }
    let ClaimOutcome::Claimed(claimed) = claim_job(
        &node.context.storage_handle,
        job_id,
        node_id,
        unix_timestamp_millis(),
    )
    .await?
    else {
        return Err("export job was not claimable".into());
    };
    let token = claimed
        .claim
        .as_ref()
        .ok_or("claimed job has no claim")?
        .claim_token;
    let running = transition_to_running(
        &node.context.storage_handle,
        job_id,
        token,
        unix_timestamp_millis(),
    )
    .await?;
    Ok(JobContext {
        driver: node.context.clone(),
        job_id,
        owner_node_id: node_id,
        claim_token: token,
        final_attempt: false,
        cancel: CancellationToken::new(),
        shutdown: CancellationToken::new(),
        progress: ProgressReporter::from_progress(&running.progress),
    })
}

fn remote_crate_json(arn_w3id: &str, hash_w3id: &str) -> String {
    serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "about": {"@id": "./"}
            },
            {
                "@id": "./",
                "@type": "Dataset",
                "name": "Remote fixture",
                "description": "Payload held on a remote node",
                "datePublished": "2026-07-24",
                "hasPart": {"@id": arn_w3id}
            },
            {
                "@id": arn_w3id,
                "@type": "File",
                "name": "Remote payload",
                "contentUrl": hash_w3id
            }
        ]
    })
    .to_string()
}

fn content_hash_w3id(hash: [u8; 32]) -> String {
    format!("{ARUNA_DATA_PREFIX}{}", hex::encode(hash))
}

fn archive_names<R: Read + std::io::Seek>(
    archive: &mut zip::ZipArchive<R>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut names = Vec::with_capacity(archive.len());
    for index in 0..archive.len() {
        names.push(archive.by_index(index)?.name().to_string());
    }
    Ok(names)
}

async fn artifact_bytes(
    context: &DriverContext,
    artifact: &ArtifactRef,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut read = read_artifact_range(context, artifact, 0..artifact.size).await?;
    let mut bytes = Vec::new();
    while let Some(chunk) = read.blob.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}

async fn keyspace_len(
    context: &DriverContext,
    key_space: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start: None,
            limit: 16,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values.len()),
        event => Err(format!("keyspace iteration failed: {event:?}").into()),
    }
}

async fn export_rows(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Vec<ExportReportRow>, Box<dyn std::error::Error>> {
    let (entries, _next) = list_job_entries(&context.storage_handle, job_id, None, 100).await?;
    entries
        .into_iter()
        .map(|(_, value)| postcard::from_bytes(value.as_ref()).map_err(Into::into))
        .collect()
}
