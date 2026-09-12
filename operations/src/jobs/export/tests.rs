use super::*;

use crate::jobs::executor::ProgressReporter;
use crate::sync::incoming::initialize_net_incoming;
use crate::tests::fixtures::import::{
    RewriteTarget, file_id_candidates, inspect_archive, open_archive, payload_entries,
    read_metadata, rewrite_document, signature_entry, validate_document,
};
use crate::tests::fixtures::staging::setup_driver_context;
use aruna_blob::blob::{BlobHandle, BlobHandler};
use aruna_core::UserId;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, GROUP_KEYSPACE, HASH_PATHS_INDEX_KEYSPACE,
    REALM_CONFIG_KEYSPACE,
};
use aruna_core::structs::{
    Actor, AuthContext, Backend, BackendConfig, BackendRef, BlobLocationKey,
    GroupAuthorizationDocument, RealmAuthorizationDocument, RealmConfigDocument, RealmNodeKind,
    RoCrateLimits,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_storage::FjallStorage;
use std::collections::HashMap;
use std::io::{Read, Seek, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use tempfile::TempDir;
use tokio::io::AsyncReadExt;

const FIXTURE_BYTES: &[u8] = b"duplicate fixture payload";
const ROCRATE_12: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/data/rocrate/roundtrip-1.2.json"
));
const ROCRATE_13: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/data/rocrate/roundtrip-1.3.json"
));

struct SparseWriter {
    file: std::fs::File,
    enabled: Arc<AtomicBool>,
}

struct BaoNode {
    _tempdir: TempDir,
    net: NetHandle,
    driver: Arc<DriverContext>,
}

impl futures_util::io::AsyncWrite for SparseWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = if self.enabled.load(Ordering::Relaxed) {
            std::io::Seek::seek(
                &mut self.file,
                std::io::SeekFrom::Current(bytes.len() as i64),
            )
            .map(|_| bytes.len())
        } else {
            std::io::Write::write(&mut self.file, bytes)
        };
        Poll::Ready(result)
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(std::io::Write::flush(&mut self.file))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(std::io::Write::flush(&mut self.file))
    }
}

async fn bao_node(realm_id: RealmId) -> BaoNode {
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
    let driver = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: Some(blob),
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    initialize_net_incoming(driver.clone());
    BaoNode {
        _tempdir: tempdir,
        net,
        driver,
    }
}

fn remote_spec(realm_id: RealmId, user_id: UserId) -> ExportRoCrateSpec {
    ExportRoCrateSpec {
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
    let group = aruna_core::structs::Group {
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
    };
    let hash: [u8; 32] = location.get_blake3().unwrap().try_into().unwrap();
    let version = BlobVersion::materialized(
        hash,
        BackendRef::node_default(),
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
            BlobLocationKey::new(hash, location.backend.clone())
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

fn file_entity(id: &str, local_path: Option<&str>) -> JsonValue {
    let mut entity = json!({
        "@id": id,
        "@type": "File",
        "contentUrl": format!(
            "{}{}",
            aruna_core::structs::ARUNA_DATA_PREFIX,
            "11".repeat(32)
        ),
    });
    if let Some(local_path) = local_path {
        entity["localPath"] = json!(local_path);
    }
    entity
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

#[test]
fn versions_roundtrip() {
    let realm_id = RealmId::from_bytes([96; 32]);
    for (version, seed, jsonld) in [("1.2", 12, ROCRATE_12), ("1.3", 13, ROCRATE_13)] {
        let validated = validate_document(jsonld).expect("fixture validates structurally");
        assert!(validated.file_ids.is_empty());
        let rewritten =
            rewrite_document(validated.value, &HashMap::new()).expect("import rewrite succeeds");
        assert!(rewritten.warnings.is_empty());
        let input = serde_json::from_str::<JsonValue>(jsonld).expect("fixture is JSON");
        let imported =
            serde_json::from_str::<JsonValue>(&rewritten.jsonld).expect("import output is JSON");
        assert_eq!(imported, input, "RO-Crate {version} import changed JSON-LD");

        let entities = recognized_entities(&imported, realm_id).expect("entities resolve");
        assert!(entities.is_empty());
        let spec = ExportRoCrateSpec {
            auth_context: AuthContext {
                user_id: UserId::nil(realm_id),
                realm_id,
                path_restrictions: None,
                session: None,
            },
            document_id: Ulid::from_bytes([seed; 16]),
            limits: RoCrateLimits::default(),
        };
        let mut checkpoint = ExportCheckpoint {
            raw_jsonld: Some(rewritten.jsonld),
            entities,
            ..ExportCheckpoint::default()
        };
        plan_export(&spec, &mut checkpoint, &[]).expect("export plans");
        let exported = checkpoint.rewritten_jsonld.expect("export metadata exists");
        let output = serde_json::from_slice::<JsonValue>(&exported).expect("export is JSON");
        assert_eq!(output, input, "RO-Crate {version} export changed JSON-LD");
        assert_eq!(
            craqle::validate_rocrate_jsonld(std::str::from_utf8(&exported).unwrap())
                .expect("export validates")
                .nquads,
            craqle::validate_rocrate_jsonld(jsonld)
                .expect("fixture validates")
                .nquads,
            "RO-Crate {version} RDF changed during import/export"
        );
    }
}

fn fixture_stream(bytes: Vec<u8>) -> BackendStream<Result<Bytes, StreamError>> {
    BackendStream::new(tokio_util::io::ReaderStream::new(std::io::Cursor::new(
        bytes,
    )))
}

fn fixture_json(version: &str) -> String {
    json!({
        "@context": [
            format!("https://w3id.org/ro/crate/{version}/context"),
            {"custom": "https://example.test/custom"}
        ],
        "@graph": [
            {
                "@id": METADATA_PATH,
                "@type": "CreativeWork",
                "about": {"@id": "./"},
                "conformsTo": {"@id": format!("https://w3id.org/ro/crate/{version}")}
            },
            {
                "@id": "./",
                "@type": "Dataset",
                "name": "Round-trip fixture",
                "description": "Generated attached crate",
                "datePublished": "2026-07-23",
                "hasPart": [
                    {"@id": "data/a%20file.txt"},
                    {"@id": "data/copy.txt"}
                ],
                "custom": ["root-one", "root-two"]
            },
            {
                "@id": "data/a%20file.txt",
                "@type": "File",
                "name": "Original",
                "alternateName": ["A file", "Alpha file"],
                "custom": ["file-one", "file-two"]
            },
            {
                "@id": "data/copy.txt",
                "@type": "File",
                "name": "Copy",
                "alternateName": "Copied file",
                "custom": {"@id": "#note"}
            },
            {
                "@id": "#note",
                "@type": "CreativeWork",
                "name": "Contextual note"
            }
        ]
    })
    .to_string()
}

async fn fixture_archive(jsonld: &str, eln: bool) -> Vec<u8> {
    let prefix = if eln { "experiment/" } else { "" };
    let mut writer = async_zip::base::write::ZipFileWriter::new(Vec::<u8>::new());
    for (path, bytes) in [
        (METADATA_PATH, jsonld.as_bytes()),
        ("data/a file.txt", FIXTURE_BYTES),
        ("data/copy.txt", FIXTURE_BYTES),
        ("notes/unlisted.txt", b"unlisted payload".as_slice()),
    ] {
        writer
            .write_entry_whole(
                zip_entry(&format!("{prefix}{path}"), FIXTURE_MOMENT_MS),
                bytes,
            )
            .await
            .unwrap();
    }
    if eln {
        writer
            .write_entry_whole(
                zip_entry(
                    &format!("{prefix}ro-crate-metadata.json.minisig"),
                    FIXTURE_MOMENT_MS,
                ),
                b"untrusted fixture signature",
            )
            .await
            .unwrap();
    }
    writer.close().await.unwrap()
}

async fn spool_fixture(handle: &BlobHandle, bytes: Vec<u8>, seed: u8) -> (BackendLocation, u64) {
    let expected_size = bytes.len() as u64;
    let expected_hash = *blake3::hash(&bytes).as_bytes();
    let Event::Blob(BlobEvent::HiddenSpooled {
        location,
        blake3,
        size,
    }) = handle
        .send_blob_effect(BlobEffect::SpoolHidden {
            namespace: Ulid::from_bytes([seed; 16]),
            name: "fixture".to_string(),
            created_by: UserId::nil(RealmId::from_bytes([seed; 32])),
            max_bytes: Some(1024 * 1024),
            deadline: None,
            blob: fixture_stream(bytes),
        })
        .await
    else {
        panic!("fixture spool failed")
    };
    assert_eq!(size, expected_size);
    assert_eq!(blake3, expected_hash);
    (location, size)
}

fn read_entry<R: Read + Seek>(archive: &mut zip::ZipArchive<R>, path: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    archive
        .by_name(path)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    bytes
}

async fn assert_roundtrip(handle: &BlobHandle, eln: bool, version: &str, seed: u8) {
    let limits = RoCrateLimits::default();
    let source_json = fixture_json(version);
    let source_bytes = fixture_archive(&source_json, eln).await;
    let (source_location, source_size) =
        spool_fixture(handle, source_bytes, seed.saturating_add(1)).await;
    let inspection = inspect_archive(
        handle.clone(),
        source_location.clone(),
        source_size,
        eln,
        &limits,
    )
    .await
    .unwrap();
    assert_eq!(inspection.wrapper.as_deref(), eln.then_some("experiment"));
    assert_eq!(signature_entry(&inspection).is_some(), eln);

    let mut reader = open_archive(handle.clone(), source_location, source_size)
        .await
        .unwrap();
    let metadata = read_metadata(
        &mut reader,
        inspection.metadata_index,
        limits.metadata_bytes,
    )
    .await
    .unwrap();
    let validated = validate_document(&metadata).unwrap();
    let payload = payload_entries(&inspection);
    let mut described = BTreeMap::new();
    for file_id in &validated.file_ids {
        let candidates = file_id_candidates(file_id).unwrap().unwrap();
        let matches = candidates
            .into_iter()
            .filter(|path| payload.contains_key(path))
            .collect::<Vec<_>>();
        assert_eq!(matches.len(), 1, "{file_id}");
        described.insert(file_id.clone(), matches[0].clone());
    }
    assert_eq!(
        payload
            .keys()
            .filter(|path| !described.values().any(|value| value == *path))
            .cloned()
            .collect::<Vec<_>>(),
        vec!["notes/unlisted.txt".to_string()]
    );

    let realm_id = RealmId::from_bytes([seed; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[seed.saturating_add(1); 32]).public();
    let payload_hash = *blake3::hash(FIXTURE_BYTES).as_bytes();
    let targets = described
        .iter()
        .enumerate()
        .map(|(index, (file_id, path))| {
            let version = Ulid::from_bytes(
                [seed.saturating_add(u8::try_from(index).unwrap())
                    .saturating_add(2); 16],
            );
            let arn = VersionedObjectArn::new(realm_id, node_id, "fixture", path, version).unwrap();
            (
                file_id.clone(),
                RewriteTarget {
                    w3id: arn.to_w3id(),
                    hash_w3id: format!(
                        "{}{}",
                        aruna_core::structs::ARUNA_DATA_PREFIX,
                        hex::encode(payload_hash)
                    ),
                    local_path: path.clone(),
                },
            )
        })
        .collect::<HashMap<_, _>>();
    let rewritten = rewrite_document(validated.value, &targets).unwrap();
    assert!(rewritten.warnings.is_empty());
    let imported: JsonValue = serde_json::from_str(&rewritten.jsonld).unwrap();
    let source: JsonValue = serde_json::from_str(&source_json).unwrap();
    assert!(imported["@graph"][1].get("license").is_none());
    assert_eq!(
        imported["@graph"][1]["custom"],
        source["@graph"][1]["custom"]
    );
    assert_eq!(
        imported["@graph"][2]["alternateName"],
        source["@graph"][2]["alternateName"]
    );

    let entities = recognized_entities(&imported, realm_id).unwrap();
    assert_eq!(entities.len(), 2);
    assert!(entities.iter().all(|entity| {
        entity.exact.is_some()
            && entity.hash == Some(payload_hash)
            && entity
                .local_path
                .as_ref()
                .is_some_and(|path| described.values().any(|value| value == path))
    }));
    let spec = ExportRoCrateSpec {
        auth_context: AuthContext {
            user_id: UserId::nil(realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        },
        document_id: Ulid::from_bytes([seed.saturating_add(8); 16]),
        limits: limits.clone(),
    };
    let opened = entities
        .iter()
        .enumerate()
        .map(|(entity_index, entity)| ProbedEntry {
            entity_index,
            candidate_index: 0,
            size: FIXTURE_BYTES.len() as u64,
            hash: payload_hash,
            report_source: ExportReportSource::Local,
            resolved_version: entity.exact.as_ref().map(|exact| exact.version),
        })
        .collect::<Vec<_>>();
    let mut checkpoint = ExportCheckpoint {
        raw_jsonld: Some(rewritten.jsonld.clone()),
        entities,
        ..ExportCheckpoint::default()
    };
    plan_export(&spec, &mut checkpoint, &opened).unwrap();
    assert!(checkpoint.report_json.is_none());
    assert!(
        checkpoint
            .entities
            .iter()
            .all(|entity| !entity.path_synthesized)
    );

    let entries = checkpoint
        .entities
        .iter()
        .enumerate()
        .map(|(entity_index, entity)| PlannedEntry {
            entity_index,
            candidate_index: 0,
            path: entity.zip_path.clone().unwrap(),
            source: PlannedSource::Ready(byte_stream(FIXTURE_BYTES)),
            expected_blake3: payload_hash,
            modified_ms: FIXTURE_MOMENT_MS,
        })
        .collect();
    let (writer, mut reader) = tokio::io::duplex(64 * 1024);
    let task = tokio::spawn(write_archive(
        writer,
        checkpoint.rewritten_jsonld.clone().unwrap(),
        entries,
        None,
        tokio_util::sync::CancellationToken::new(),
        tokio_util::sync::CancellationToken::new(),
    ));
    let mut exported = Vec::new();
    reader.read_to_end(&mut exported).await.unwrap();
    task.await.unwrap().unwrap();

    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(&exported)).unwrap();
    let names = (0..archive.len())
        .map(|index| archive.by_index(index).unwrap().name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            METADATA_PATH.to_string(),
            "data/a file.txt".to_string(),
            "data/copy.txt".to_string(),
        ]
    );
    assert_eq!(read_entry(&mut archive, "data/a file.txt"), FIXTURE_BYTES);
    assert_eq!(read_entry(&mut archive, "data/copy.txt"), FIXTURE_BYTES);
    drop(archive);

    let (export_location, export_size) =
        spool_fixture(handle, exported, seed.saturating_add(9)).await;
    let exported_inspection = inspect_archive(
        handle.clone(),
        export_location.clone(),
        export_size,
        false,
        &limits,
    )
    .await
    .unwrap();
    assert!(exported_inspection.wrapper.is_none());
    let mut reader = open_archive(handle.clone(), export_location, export_size)
        .await
        .unwrap();
    let metadata = read_metadata(
        &mut reader,
        exported_inspection.metadata_index,
        limits.metadata_bytes,
    )
    .await
    .unwrap();
    let validated = validate_document(&metadata).unwrap();
    assert_eq!(
        validated.file_ids,
        vec!["data/a%20file.txt".to_string(), "data/copy.txt".to_string()]
    );
    let payload = payload_entries(&exported_inspection);
    let by_path = targets
        .values()
        .map(|target| (target.local_path.clone(), target.clone()))
        .collect::<HashMap<_, _>>();
    let mut targets = HashMap::new();
    for file_id in &validated.file_ids {
        let candidates = file_id_candidates(file_id).unwrap().unwrap();
        let matches = candidates
            .into_iter()
            .filter(|path| payload.contains_key(path))
            .collect::<Vec<_>>();
        assert_eq!(matches.len(), 1, "{file_id}");
        targets.insert(file_id.clone(), by_path[&matches[0]].clone());
    }
    let reimported = rewrite_document(validated.value, &targets).unwrap();
    assert!(reimported.warnings.is_empty());
    assert_eq!(
        serde_json::from_str::<JsonValue>(&reimported.jsonld).unwrap(),
        imported
    );
}

#[tokio::test]
async fn fixture_roundtrip() {
    let fixture = setup_driver_context().await;
    let handle = fixture.driver_context.blob_handle.as_ref().unwrap();
    assert_roundtrip(handle, false, "1.2", 20).await;
    assert_roundtrip(handle, true, "1.1", 40).await;
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
            resolved: aruna_core::structs::ResolvedBackend::node_default(),
            bucket: "remote".to_string(),
            key: "payload".to_string(),
            created_by: owner,
            blob: byte_stream(FIXTURE_BYTES),
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
        HASH_PATHS_INDEX_KEYSPACE,
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
        progress: ProgressReporter::from_progress(&aruna_core::structs::JobProgress {
            current: 0,
            total: None,
            unit: "entries".to_string(),
        }),
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
            resolved: aruna_core::structs::ResolvedBackend::node_default(),
            bucket: "remote".to_string(),
            key: "payload".to_string(),
            created_by: owner,
            blob: byte_stream(FIXTURE_BYTES),
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
    let mut rules = BTreeMap::new();
    load_rules(&ctx, &spec, &mut rules, [*group_id])
        .await
        .unwrap();
    let mut policies = BTreeMap::new();
    load_policies(&ctx, &spec, &mut policies, [*group_id])
        .await
        .unwrap();

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
    let alias = HashPathIndexKey::new(
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
                key_space: HASH_PATHS_INDEX_KEYSPACE.to_string(),
                key: alias.to_bytes().unwrap().into(),
                value: Vec::new().into(),
                txn_id: None,
            })
            .await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    let spec = remote_spec(realm_id, owner);
    let ctx = job_context(node.driver.clone(), node.net.node_id());
    let mut candidates = Vec::new();
    let mut denied = false;
    extend_hash_candidates(
        &ctx,
        &spec,
        hash,
        None,
        &mut candidates,
        &mut denied,
        &mut BTreeMap::new(),
        &mut BTreeMap::new(),
        &mut BTreeSet::new(),
        &mut BTreeSet::new(),
        &mut BTreeMap::new(),
        &mut BTreeMap::new(),
        &mut BTreeMap::new(),
    )
    .await
    .expect("a foreign group must deny instead of failing the export");

    assert!(denied);
    assert!(!candidates.iter().any(|candidate| matches!(
        &candidate.source,
        CandidateSource::Local { group_id, .. } if *group_id == foreign
    )));
    node.net.shutdown().await;
}

#[test]
fn learns_probe_hash() {
    let realm_id = RealmId::from_bytes([2; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
    let version = Ulid::from_bytes([4; 16]);
    let exact = VersionedObjectArn::new(realm_id, node_id, "bucket", "key", version).unwrap();
    let document = json!({"@graph": [{"@id": exact.to_w3id(), "@type": "File"}]});
    let mut entity = recognized_entities(&document, realm_id).unwrap().remove(0);
    entity.candidates.push(ExportCandidate {
        source: CandidateSource::RemoteExact {
            node_id,
            target: exact,
        },
        report_source: ExportReportSource::Remote,
        resolved_version: Some(version),
        expected_blake3: None,
    });
    let hash = [7; 32];

    assert!(learn_probe_hash(&mut entity, 0, hash));
    assert_eq!(entity.hash, Some(hash));
    assert_eq!(entity.hash_realm, Some(realm_id));
    assert_eq!(entity.candidates[0].expected_blake3, Some(hash));
    assert!(matches!(
        &entity.candidates[1],
        ExportCandidate {
            source: CandidateSource::RemoteHash {
                node_id: holder,
                hash: candidate_hash,
            },
            report_source: ExportReportSource::Hash,
            resolved_version: Some(candidate_version),
            expected_blake3: Some(expected),
        } if *holder == node_id
            && *candidate_hash == hash
            && *candidate_version == version
            && *expected == hash
    ));
    assert!(!learn_probe_hash(&mut entity, 0, hash));
    assert_eq!(entity.candidates.len(), 2);
}

#[test]
fn deduplicates_aliases() {
    let realm_id = RealmId::from_bytes([11; 32]);
    let alias = HashPathIndexKey::new(
        [12; 32],
        Ulid::from_bytes([13; 16]),
        realm_id,
        Ulid::from_bytes([14; 16]),
        iroh::SecretKey::from_bytes(&[15; 32]).public(),
        "bucket",
        "key",
    );
    let aliases = vec![alias; MAX_HASH_ALIASES];
    let mut paths = BTreeSet::new();
    let mut keys = BTreeSet::new();
    let distinct = collect_aliases(&aliases, realm_id, &mut paths, &mut keys).unwrap();
    assert_eq!(distinct.len(), 1);
    assert_eq!(paths.len(), 1);
    assert_eq!(keys.len(), 1);
    let repeated = collect_aliases(&aliases, realm_id, &mut paths, &mut keys).unwrap();
    assert!(repeated.is_empty());
}

#[test]
fn bounds_alias_cache() {
    let realm_id = RealmId::from_bytes([11; 32]);
    let alias = HashPathIndexKey::new(
        [12; 32],
        Ulid::from_bytes([13; 16]),
        realm_id,
        Ulid::from_bytes([14; 16]),
        iroh::SecretKey::from_bytes(&[15; 32]).public(),
        "bucket",
        "key",
    );
    let mut cache = BTreeMap::new();
    cache_aliases(&mut cache, [16; 32], vec![alias; MAX_HASH_ALIASES]).unwrap();

    let mut cross_realm = HashPathIndexKey::new(
        [17; 32],
        Ulid::from_bytes([18; 16]),
        RealmId::from_bytes([19; 32]),
        Ulid::from_bytes([20; 16]),
        iroh::SecretKey::from_bytes(&[21; 32]).public(),
        "bucket",
        "key",
    );
    cross_realm.realm_id = RealmId::from_bytes([22; 32]);
    assert!(matches!(
        cache_aliases(&mut cache, [23; 32], vec![cross_realm]),
        Err(ExportFailure::Retryable(message)) if message == "export alias limit exceeded"
    ));
}

#[test]
fn bounds_empty_cache() {
    let mut cache = BTreeMap::new();
    for index in 0..MAX_HASH_ALIASES {
        let mut hash = [0; 32];
        hash[..2].copy_from_slice(&(index as u16).to_be_bytes());
        cache_aliases(&mut cache, hash, Vec::new()).unwrap();
    }
    assert_eq!(cache.len(), MAX_HASH_ALIASES);
    assert!(cache_aliases(&mut cache, [255; 32], Vec::new()).is_err());
}

#[test]
fn caps_repeated_hashes() {
    let realm_id = RealmId::from_bytes([24; 32]);
    let group_id = Ulid::from_bytes([25; 16]);
    let hash = [26; 32];
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/data".to_string(),
        storage_bucket: "bucket".to_string(),
        backend_path: "object".to_string(),
        ulid: Ulid::from_bytes([27; 16]),
        compressed: false,
        encrypted: false,
        created_by: UserId::nil(realm_id),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 0,
        hashes: HashMap::new(),
    };
    let aliases = (0..MAX_HASH_ALIASES)
        .map(|index| ExportCandidate {
            source: CandidateSource::Local {
                location: location.clone(),
                group_id,
                permission_path: format!("/alias/{index}"),
                node_id: iroh::SecretKey::from_bytes(&[28; 32]).public(),
                bucket: "bucket".to_string(),
                key: format!("key-{index}"),
            },
            report_source: ExportReportSource::Hash,
            resolved_version: Some(Ulid::from_bytes([index as u8; 16])),
            expected_blake3: Some(hash),
        })
        .collect::<Vec<_>>();

    for _ in 0..3 {
        let mut candidates = Vec::new();
        merge_candidates(&mut candidates, &aliases, MAX_LOCAL_CANDIDATES);
        assert_eq!(candidates.len(), MAX_LOCAL_CANDIDATES);
        merge_candidates(&mut candidates, &aliases, MAX_LOCAL_CANDIDATES);
        assert_eq!(candidates.len(), MAX_LOCAL_CANDIDATES);
    }
}

#[test]
fn rejects_alias_budget() {
    let realm_id = RealmId::from_bytes([11; 32]);
    let alias = HashPathIndexKey::new(
        [12; 32],
        Ulid::from_bytes([13; 16]),
        realm_id,
        Ulid::from_bytes([14; 16]),
        iroh::SecretKey::from_bytes(&[15; 32]).public(),
        "bucket",
        "key",
    );
    let aliases = (0..MAX_HASH_ALIASES)
        .map(|index| {
            let mut alias = alias.clone();
            alias.key = format!("key-{index}");
            alias
        })
        .collect::<Vec<_>>();
    let mut paths = BTreeSet::new();
    // The budget is cumulative across pages: a primed entry pushes this
    // in-cap page over the limit.
    paths.insert((Ulid::from_bytes([21; 16]), "primed/path".to_string()));
    let mut keys = BTreeSet::new();
    assert!(matches!(
        collect_aliases(&aliases, realm_id, &mut paths, &mut keys),
        Err(ExportFailure::Retryable(message)) if message == "export alias limit exceeded"
    ));
}

async fn sample_archive() -> Vec<u8> {
    let (writer, mut reader) = tokio::io::duplex(4096);
    let task = tokio::spawn(write_archive(
        writer,
        b"metadata".to_vec(),
        vec![
            PlannedEntry {
                entity_index: 0,
                candidate_index: 0,
                path: "data/b".to_string(),
                source: PlannedSource::Ready(byte_stream(b"b")),
                expected_blake3: *blake3::hash(b"b").as_bytes(),
                modified_ms: FIXTURE_MOMENT_MS,
            },
            PlannedEntry {
                entity_index: 1,
                candidate_index: 0,
                path: "data/a".to_string(),
                source: PlannedSource::Ready(byte_stream(b"a")),
                expected_blake3: *blake3::hash(b"a").as_bytes(),
                modified_ms: FIXTURE_MOMENT_MS,
            },
        ],
        Some(b"report".to_vec()),
        tokio_util::sync::CancellationToken::new(),
        tokio_util::sync::CancellationToken::new(),
    ));
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    task.await.unwrap().unwrap();
    bytes
}

#[test]
fn recognizes_identifiers() {
    let realm_id = RealmId::from_bytes([2; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
    let version = Ulid::from_bytes([4; 16]);
    let arn = VersionedObjectArn::new(realm_id, node_id, "bucket", "a b", version).unwrap();
    let foreign = VersionedObjectArn::new(
        RealmId::from_bytes([9; 32]),
        node_id,
        "bucket",
        "foreign",
        version,
    )
    .unwrap();
    let document = json!({
        "@graph": [
            file_entity(&arn.to_w3id(), Some("a b")),
            file_entity(&foreign.to_w3id(), None),
            {"@id": foreign.to_string(), "@type": "File"},
            {"@id": "https://example.org/external", "@type": "File"},
        ]
    });

    let entities = recognized_entities(&document, realm_id).unwrap();

    assert_eq!(entities[0].exact.as_ref(), Some(&arn));
    assert_eq!(entities[0].hash, Some([0x11; 32]));
    assert_eq!(entities[1].omission, None);
    assert_eq!(entities[2].omission, Some(ReasonCode::Unsupported));
    assert_eq!(entities[3].omission, Some(ReasonCode::External));
}

#[test]
fn recognizes_context_aliases() {
    let realm_id = RealmId::from_bytes([2; 32]);
    let document = json!({
        "@context": [
            "https://w3id.org/ro/crate/1.2/context",
            {
                "graphItems": "@graph",
                "idAlias": "@id",
                "typeAlias": "@type",
                "downloadAlias": "http://schema.org/contentUrl",
                "pathAlias": LOCAL_PATH_IRI
            }
        ],
        "graphItems": [{
            "idAlias": "data/a.txt",
            "typeAlias": "File",
            "downloadAlias": format!(
                "{}{}",
                aruna_core::structs::ARUNA_DATA_PREFIX,
                "11".repeat(32)
            ),
            "pathAlias": "data/a.txt"
        }]
    });

    let entities = recognized_entities(&document, realm_id).unwrap();

    assert_eq!(entities[0].hash, Some([0x11; 32]));
    assert_eq!(entities[0].local_path.as_deref(), Some("data/a.txt"));
}

#[test]
fn keeps_import_path() {
    let realm_id = RealmId::from_bytes([2; 32]);
    let mut entity = file_entity(
        &format!(
            "{}{}",
            aruna_core::structs::ARUNA_DATA_PREFIX,
            "11".repeat(32)
        ),
        None,
    );
    entity["localPath"] = json!(["data/canonical.txt", "aaa-original.txt"]);
    let document = json!({"@graph": [entity]});

    let entities = recognized_entities(&document, realm_id).unwrap();

    assert_eq!(
        entities[0].local_path.as_deref(),
        Some("data/canonical.txt")
    );
}

#[test]
fn reports_keyword_aliases() {
    let mut document = json!({
        "@context": [
            "https://w3id.org/ro/crate/1.2/context",
            {"graphItems": "@graph", "idAlias": "@id"}
        ],
        "graphItems": [
            {"idAlias": "urn:dataset:run-42", "@type": "Dataset"},
            {
                "idAlias": METADATA_PATH,
                "@type": "CreativeWork",
                "about": {"idAlias": "urn:dataset:run-42"}
            }
        ]
    });

    add_report(&mut document).unwrap();

    assert_eq!(
        document["graphItems"][0]["subjectOf"]["@id"],
        JsonValue::String("#aruna-export-report".to_string())
    );
    assert_eq!(document["graphItems"][2]["@id"], REPORT_PATH);
    assert_eq!(
        document["graphItems"][3]["about"]["@id"],
        "urn:dataset:run-42"
    );
}

#[test]
fn reports_context_overrides() {
    let mut document = json!({
        "@context": [
            "https://w3id.org/ro/crate/1.2/context",
            {
                "subjectOf": "https://example.test/subject",
                "hasPart": "https://example.test/part",
                "about": "https://example.test/about",
                "encodingFormat": "https://example.test/format",
                "name": "https://example.test/name"
            }
        ],
        "@graph": [
            {
                "@id": "./",
                "@type": "Dataset",
                "hasPart": "preserved"
            },
            {
                "@id": METADATA_PATH,
                "@type": "CreativeWork",
                "http://schema.org/about": {"@id": "./"}
            }
        ]
    });

    add_report(&mut document).unwrap();

    assert_eq!(document["@graph"][0]["hasPart"], "preserved");
    assert_eq!(
        document["@graph"][0][SCHEMA_SUBJECT_HTTPS_IRI]["@id"],
        "#aruna-export-report"
    );
    assert_eq!(
        document["@graph"][0][SCHEMA_HAS_PART_HTTPS_IRI]["@id"],
        REPORT_PATH
    );
    assert_eq!(
        document["@graph"][2][SCHEMA_ENCODING_HTTPS_IRI],
        "application/json"
    );
}

#[test]
fn scan_ignores_aliases() {
    let document = json!({
        "@context": {"node": "@id"},
        "@graph": [
            {"node": "old"},
            {"name": "old"}
        ]
    });
    let found = scan_unrewritten(
        &document,
        &BTreeMap::from([("old".to_string(), "new".to_string())]),
    );

    assert_eq!(found, BTreeSet::from(["old".to_string()]));
}

#[test]
fn rejects_report_collision() {
    let mut document = json!({
        "@graph": [
            {"@id": "./", "@type": "Dataset"},
            {"@id": REPORT_PATH, "@type": "File"}
        ]
    });

    assert!(matches!(
        add_report(&mut document),
        Err(ExportFailure::Permanent(message))
            if message.contains("reserved export report")
    ));
}

fn stored_file(hash: u8, location: &str) -> JsonValue {
    json!({
        "@id": format!(
            "{}{}",
            aruna_core::structs::ARUNA_DATA_PREFIX,
            hex::encode([hash; 32])
        ),
        "@type": "File",
        "name": "payload",
        "contentUrl": location
    })
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

fn planned_paths(realm_id: RealmId, document: &JsonValue) -> Result<Vec<String>, ExportFailure> {
    let spec = remote_spec(realm_id, UserId::nil(realm_id));
    let entities = recognized_entities(document, realm_id).unwrap();
    let opened = entities
        .iter()
        .enumerate()
        .map(|(entity_index, entity)| ProbedEntry {
            entity_index,
            candidate_index: 0,
            size: 1,
            hash: entity.hash.unwrap_or([0; 32]),
            report_source: ExportReportSource::Local,
            resolved_version: None,
        })
        .collect::<Vec<_>>();
    let mut checkpoint = ExportCheckpoint {
        raw_jsonld: Some(document.to_string()),
        entities,
        ..ExportCheckpoint::default()
    };
    plan_export(&spec, &mut checkpoint, &opened)?;
    Ok(checkpoint
        .entities
        .iter()
        .filter_map(|entity| entity.zip_path.clone())
        .collect())
}

#[test]
fn keeps_subcrate_reference() {
    let realm_id = RealmId::from_bytes([26; 32]);
    let child = "https://w3id.org/aruna/01JCHILD0000000000000000A";
    let descriptor = "https://api.example.test/metadata/01JCHILD0000000000000000A/rocrate";
    let file = stored_file(1, "s3://reads/one.csv");
    let document = json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "./",
                "@type": "Dataset",
                "name": "test",
                "description": "test crate",
                "datePublished": "2026-07-23",
                "hasPart": [{"@id": file["@id"]}, {"@id": child}]
            },
            {
                "@id": METADATA_PATH,
                "@type": "CreativeWork",
                "about": {"@id": "./"},
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}
            },
            file,
            {
                "@id": child,
                "@type": "Dataset",
                "name": "restricted child",
                "conformsTo": {"@id": "https://w3id.org/ro/crate"},
                "identifier": "01JCHILD0000000000000000A",
                "subjectOf": {"@id": descriptor}
            },
            {
                "@id": descriptor,
                "@type": "CreativeWork",
                "encodingFormat": "application/ld+json"
            }
        ]
    });

    let entities = recognized_entities(&document, realm_id).unwrap();
    assert_eq!(entities.len(), 1);
    assert_eq!(
        planned_paths(realm_id, &document).unwrap(),
        vec!["one.csv".to_string()]
    );
}

#[test]
fn local_path_wins() {
    let realm_id = RealmId::from_bytes([21; 32]);
    let mut part = stored_file(1, "s3://reads/raw/one.csv");
    part["localPath"] = json!("data/authored.csv");
    let document = crate_document(&[part]);

    let entities = recognized_entities(&document, realm_id).unwrap();
    assert_eq!(entities[0].local_path.as_deref(), Some("data/authored.csv"));
    assert_eq!(
        planned_paths(realm_id, &document).unwrap(),
        vec!["data/authored.csv".to_string()]
    );
}

#[test]
fn drops_shared_prefix() {
    let realm_id = RealmId::from_bytes([22; 32]);
    let document = crate_document(&[
        stored_file(1, "s3://reads/raw/2024/one.csv"),
        stored_file(2, "s3://reads/raw/2025/two.csv"),
    ]);

    assert_eq!(
        planned_paths(realm_id, &document).unwrap(),
        vec!["2024/one.csv".to_string(), "2025/two.csv".to_string()]
    );
}

#[test]
fn separates_buckets() {
    let realm_id = RealmId::from_bytes([23; 32]);
    let document = crate_document(&[
        stored_file(1, "s3://reads/raw/one.csv"),
        stored_file(2, "s3://results/two.csv"),
    ]);

    assert_eq!(
        planned_paths(realm_id, &document).unwrap(),
        vec![
            "reads/raw/one.csv".to_string(),
            "results/two.csv".to_string()
        ]
    );
}

#[test]
fn rejects_path_collision() {
    let realm_id = RealmId::from_bytes([24; 32]);
    let mut authored = stored_file(1, "s3://reads/one.csv");
    authored["localPath"] = json!("two.csv");
    let document = crate_document(&[authored, stored_file(2, "s3://reads/two.csv")]);

    assert!(matches!(
        planned_paths(realm_id, &document),
        Err(ExportFailure::Permanent(message))
            if message.contains("resolve to ZIP path `two.csv`")
    ));
}

#[test]
fn keeps_reserved_names() {
    let realm_id = RealmId::from_bytes([25; 32]);
    let document = crate_document(&[stored_file(1, &format!("s3://reads/{METADATA_PATH}"))]);

    let paths = planned_paths(realm_id, &document).unwrap();

    assert_eq!(paths.len(), 1);
    assert!(paths[0].starts_with("data/"));
}

#[test]
fn plans_ordered_paths() {
    assert_eq!(safe_zip_path("./a/b.txt").as_deref(), Some("a/b.txt"));
    assert_eq!(safe_zip_path("../escape"), None);
    assert_eq!(safe_zip_path("a%2fb"), None);
    assert_eq!(jsonld_path("data/a file.txt"), "data/a%20file.txt");
    assert_ne!(
        synthesized_path([7; 32], "one"),
        synthesized_path([7; 32], "two")
    );
}

#[test]
fn plan_rejects_oversize() {
    let realm_id = RealmId::from_bytes([8; 32]);
    let mut spec = remote_spec(realm_id, UserId::nil(realm_id));
    spec.limits.export_artifact_bytes = 512;
    let entities = [ExportEntity {
        entity_id: "payload".to_string(),
        local_path: Some("data/payload".to_string()),
        storage_key: None,
        exact: None,
        hash: None,
        hash_realm: None,
        candidates: Vec::new(),
        omission: None,
        message: None,
        zip_path: Some("data/payload".to_string()),
        report_source: None,
        resolved_version: None,
        path_synthesized: false,
    }];
    let opened = [ProbedEntry {
        entity_index: 0,
        candidate_index: 0,
        size: 513,
        hash: [0; 32],
        report_source: ExportReportSource::Local,
        resolved_version: None,
    }];

    assert!(matches!(
        precheck_size(&spec, b"{}", None, &opened, &entities),
        Err(ExportFailure::Permanent(message))
            if message == "planned ZIP exceeds the 512 byte artifact limit"
    ));
}

#[test]
fn rewrites_report_links() {
    let mut document = json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "./",
                "@type": "Dataset",
                "name": "test",
                "description": "test crate",
                "datePublished": "2026-07-23",
                "hasPart": {"@id": "old"}
            },
            {
                "@id": METADATA_PATH,
                "@type": "CreativeWork",
                "about": {"@id": "./"},
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}
            },
            {"@id": "old", "@type": "File"}
        ]
    });
    rewrite_ids(
        &mut document,
        &BTreeMap::from([("old".to_string(), "data/file".to_string())]),
    );
    add_report(&mut document).unwrap();

    assert_eq!(
        document["@graph"][0]["hasPart"][0]["@id"],
        JsonValue::String("data/file".to_string())
    );
    assert_eq!(
        document["@graph"][0]["subjectOf"]["@id"],
        JsonValue::String("#aruna-export-report".to_string())
    );
    craqle::validate_rocrate_jsonld(&document.to_string()).unwrap();
}

#[test]
fn counts_omission_rows() {
    let realm_id = RealmId::from_bytes([2; 32]);
    let document = json!({
        "@graph": [
            {"@id": "https://example.org/external", "@type": "File"},
            file_entity("denied", None),
            file_entity("included", None),
        ]
    });
    let mut entities = recognized_entities(&document, realm_id).unwrap();
    entities[1].omission = Some(ReasonCode::Denied);
    let rows = build_rows(&entities, &BTreeSet::new());

    let (included, omitted) = report_counts(&rows);

    assert_eq!(included, 1);
    assert_eq!(omitted.external, 1);
    assert_eq!(omitted.denied, 1);
    assert_eq!(omitted.missing, 0);
}

#[test]
fn checkpoint_prefix_decodes() {
    let bytes = postcard::to_allocvec(&ExportCheckpoint::default()).unwrap();
    let (refs, remaining) = postcard::take_from_bytes::<RoCrateCheckpointRefs>(&bytes).unwrap();

    assert_eq!(refs, RoCrateCheckpointRefs::default());
    assert!(!remaining.is_empty());
}

#[tokio::test]
async fn orders_zip_entries() {
    let bytes = sample_archive().await;
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
    let names = (0..archive.len())
        .map(|index| archive.by_index(index).unwrap().name().to_string())
        .collect::<Vec<_>>();

    assert_eq!(
        names,
        vec![
            METADATA_PATH.to_string(),
            "data/a".to_string(),
            "data/b".to_string(),
            REPORT_PATH.to_string(),
        ]
    );
}

#[test]
fn clamps_zip_dates() {
    assert_eq!(zip_date(FIXTURE_MOMENT_MS).year(), 2026);
    assert_eq!(zip_date(0).year(), 1980);
    assert_eq!(zip_date(u64::MAX).year(), 1980);
    assert_eq!(
        zip_date(Ulid::from_bytes([74; 16]).timestamp_ms()).year(),
        2107
    );
}

#[tokio::test]
async fn stamps_entry_dates() {
    let bytes = sample_archive().await;
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
    for index in 0..archive.len() {
        let entry = archive.by_index(index).unwrap();
        let moment = entry
            .last_modified()
            .expect("entry carries a modification date");
        assert_eq!(moment.year(), 2026);
    }
}

#[tokio::test]
async fn dates_follow_versions() {
    let version = Ulid::from_bytes([74; 16]);
    let (writer, mut reader) = tokio::io::duplex(4096);
    let task = tokio::spawn(write_archive(
        writer,
        b"metadata".to_vec(),
        vec![PlannedEntry {
            entity_index: 0,
            candidate_index: 0,
            path: "data/a".to_string(),
            source: PlannedSource::Ready(byte_stream(b"a")),
            expected_blake3: *blake3::hash(b"a").as_bytes(),
            modified_ms: version.timestamp_ms(),
        }],
        None,
        tokio_util::sync::CancellationToken::new(),
        tokio_util::sync::CancellationToken::new(),
    ));
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    task.await.unwrap().unwrap();

    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
    let metadata_year = archive
        .by_name(METADATA_PATH)
        .unwrap()
        .last_modified()
        .unwrap()
        .year();
    let payload_year = archive
        .by_name("data/a")
        .unwrap()
        .last_modified()
        .unwrap()
        .year();

    assert_eq!(metadata_year, 2026);
    assert_eq!(payload_year, 2107);
}

#[tokio::test]
async fn archives_are_deterministic() {
    assert_eq!(sample_archive().await, sample_archive().await);
}

#[tokio::test]
async fn corrupt_source_retries() {
    let fixture = setup_driver_context().await;
    let realm_id = RealmId::from_bytes([81; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[82; 32]).public();
    let hash = [83; 32];
    let candidate = ExportCandidate {
        source: CandidateSource::RemoteHash { node_id, hash },
        report_source: ExportReportSource::Hash,
        resolved_version: None,
        expected_blake3: Some(hash),
    };
    let mut checkpoint = ExportCheckpoint {
        entities: vec![ExportEntity {
            entity_id: "data/corrupt".to_string(),
            local_path: None,
            storage_key: None,
            exact: None,
            hash: Some(hash),
            hash_realm: Some(realm_id),
            candidates: vec![candidate],
            omission: None,
            message: None,
            zip_path: None,
            report_source: None,
            resolved_version: None,
            path_synthesized: false,
        }],
        ..Default::default()
    };
    let failures = BTreeMap::from([(0, BTreeMap::from([(0, OpenStatus::Corrupt)]))]);
    let ctx = job_context(Arc::new(fixture.driver_context.clone()), node_id);

    assert!(matches!(
        probe_sources(
            &ctx,
            &remote_spec(realm_id, UserId::nil(realm_id)),
            &mut checkpoint,
            &failures,
        )
        .await,
        Err(ExportFailure::Retryable(message))
            if message == "payload integrity check failed"
    ));
}

#[tokio::test]
async fn signals_corrupt_candidate() {
    let (writer, mut reader) = tokio::io::duplex(4096);
    let task = tokio::spawn(write_archive(
        writer,
        b"metadata".to_vec(),
        vec![PlannedEntry {
            entity_index: 4,
            candidate_index: 2,
            path: "data/corrupt".to_string(),
            source: PlannedSource::Ready(byte_stream(b"wrong")),
            expected_blake3: [0; 32],
            modified_ms: FIXTURE_MOMENT_MS,
        }],
        None,
        tokio_util::sync::CancellationToken::new(),
        tokio_util::sync::CancellationToken::new(),
    ));
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();

    assert!(matches!(
        task.await.unwrap(),
        Err(ExportFailure::Candidate {
            entity_index: 4,
            candidate_index: 2,
            status: OpenStatus::Corrupt,
            ..
        })
    ));
}

#[tokio::test]
async fn cancels_streaming_archive() {
    let (writer, mut reader) = tokio::io::duplex(4096);
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let task = tokio::spawn(write_archive(
        writer,
        b"metadata".to_vec(),
        vec![PlannedEntry {
            entity_index: 0,
            candidate_index: 0,
            path: "data/pending".to_string(),
            source: PlannedSource::Ready(BackendStream::new(futures_util::stream::pending::<
                Result<Bytes, std::io::Error>,
            >())),
            expected_blake3: [0; 32],
            modified_ms: FIXTURE_MOMENT_MS,
        }],
        None,
        cancel,
        tokio_util::sync::CancellationToken::new(),
    ));
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();

    assert!(matches!(task.await.unwrap(), Err(ExportFailure::Cancelled)));
}

#[tokio::test]
async fn zip64_interoperates() {
    let mut writer = async_zip::base::write::ZipFileWriter::new(Vec::<u8>::new());
    for index in 0..70_000u32 {
        writer
            .write_entry_whole(
                zip_entry(&format!("data/{index:08}"), FIXTURE_MOMENT_MS),
                &[],
            )
            .await
            .unwrap();
    }
    let bytes = writer.close().await.unwrap();
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(&bytes)).unwrap();
    assert_eq!(archive.len(), 70_000);
    let mut last = String::new();
    archive
        .by_index(69_999)
        .unwrap()
        .read_to_string(&mut last)
        .unwrap();

    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.as_file_mut().write_all(&bytes).unwrap();
    file.as_file_mut().flush().unwrap();
    if let Ok(status) = std::process::Command::new("unzip")
        .arg("-t")
        .arg(file.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
    {
        assert!(status.success());
    }
}

#[tokio::test]
async fn zip64_large_entry() {
    const ENTRY_SIZE: u64 = u32::MAX as u64 + 2;
    const CHUNK_SIZE: usize = 1024 * 1024;

    let file = tempfile::NamedTempFile::new().unwrap();
    let sparse = Arc::new(AtomicBool::new(false));
    let mut writer = async_zip::base::write::ZipFileWriter::new(SparseWriter {
        file: file.reopen().unwrap(),
        enabled: Arc::clone(&sparse),
    });
    let mut entry = writer
        .write_entry_stream(zip_entry("data/large", FIXTURE_MOMENT_MS))
        .await
        .unwrap();
    let zeros = vec![0; CHUNK_SIZE];
    sparse.store(true, Ordering::Relaxed);
    for _ in 0..ENTRY_SIZE / CHUNK_SIZE as u64 {
        entry.write_all(&zeros).await.unwrap();
    }
    entry
        .write_all(&zeros[..(ENTRY_SIZE % CHUNK_SIZE as u64) as usize])
        .await
        .unwrap();
    sparse.store(false, Ordering::Relaxed);
    entry.close().await.unwrap();
    drop(writer.close().await.unwrap());

    let mut archive = zip::ZipArchive::new(std::fs::File::open(file.path()).unwrap()).unwrap();
    assert_eq!(archive.len(), 1);
    let archived = archive.by_name("data/large").unwrap();
    assert_eq!(archived.size(), ENTRY_SIZE);
    assert_eq!(archived.compressed_size(), ENTRY_SIZE);
    assert_eq!(archived.compression(), zip::CompressionMethod::Stored);
    drop(archived);
    drop(archive);

    if let Ok(status) = std::process::Command::new("unzip")
        .arg("-t")
        .arg(file.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
    {
        assert!(status.success());
    }
}
