//! Tests RO-Crate export: archive layout and dates, permission refusals, retries and ZIP64.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::archive::*;
use super::assemble::tests::{ReadyEntry, write_archive};
use super::resolve::CandidateSource;
use super::sources::{
    AliasState, HashCandidatesOperation, HashFound, cache_aliases, collect_aliases,
    learn_probe_hash, merge_candidates,
};
use super::*;
use crate::blob::permission_paths::MAX_HASH_ALIASES;
use crate::driver::DriverContext;
use crate::driver::drive;
use crate::jobs::context::JobContext;
use crate::jobs::repository::test_support::{
    artifact, job_id, job_scope as scoped_job, local_scope, other_node, owner_node, realm,
};
use crate::tests::export::{
    crate_document, destination, job_context, probe_sources, recognized_entities, remote_spec,
    scope_of,
};
use aruna_blob::archive::zip_date;
use aruna_blob::archive::zip_entry;
use aruna_core::archive_path::safe_zip_path;
use aruna_core::archive_path::synthesized_path;
use aruna_core::effects::BlobEffect;
use aruna_core::events::BlobEvent;
use aruna_core::events::Event;
use aruna_core::stream::BackendStream;
use aruna_core::stream::StreamError;
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::blob::HashIndex;
use aruna_core::structs::storage::format::StoredFormat;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::io::AsyncWriteExt;
use serde_json::Value as JsonValue;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::tests::import::{
    RewriteTarget, file_id_candidates, inspect_archive, open_archive, payload_entries,
    read_metadata, rewrite_document, signature_entry, validate_document,
};
use crate::tests::staging::setup_driver_context;
use aruna_blob::blob::BlobHandle;
use aruna_core::UserId;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::data::identity::{ObjectLocation, normalized_id};
use aruna_storage::FjallStorage;
use std::collections::HashMap;
use std::io::{Read, Seek, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use tokio::io::AsyncReadExt;

const FIXTURE_BYTES: &[u8] = b"duplicate fixture payload";
const ROCRATE_12: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/roundtrip-1.2.json"
));
const ROCRATE_13: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/roundtrip-1.3.json"
));

struct SparseWriter {
    file: std::fs::File,
    enabled: Arc<AtomicBool>,
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

fn file_entity(id: &str, local_path: Option<&str>) -> JsonValue {
    let mut entity = json!({
        "@id": id,
        "@type": "File",
        "contentUrl": format!(
            "{}{}",
            aruna_core::structs::storage::replication::ARUNA_DATA_PREFIX,
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
            selection: None,
            destination: None,
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
    let payload_hash = *blake3::hash(FIXTURE_BYTES).as_bytes();
    // Both payloads match, so the second entity keeps its `s3://` URL as `@id`.
    let mut used = BTreeSet::new();
    let targets = described
        .iter()
        .map(|(file_id, path)| {
            let location = ObjectLocation {
                bucket: "fixture".to_string(),
                key: path.clone(),
            };
            (
                file_id.clone(),
                RewriteTarget {
                    id: normalized_id(payload_hash, &location, &mut used),
                    content_url: location.to_url(),
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
        entity
            .storage_key
            .as_ref()
            .is_some_and(|key| key.bucket == "fixture")
            && (entity.hash == Some(payload_hash) || entity.entity_id.starts_with("s3://"))
            && entity
                .local_path
                .as_ref()
                .is_some_and(|path| described.values().any(|value| value == path))
    }));
    let spec = ExportRoCrateSpec {
        selection: None,
        destination: None,
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
        .map(|(entity_index, entity)| ReadyEntry {
            entity_index,
            candidate_index: 0,
            path: entity.zip_path.clone().unwrap(),
            stream: byte_stream(FIXTURE_BYTES),
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
        .map(|index| archive.by_index(index).unwrap().name().unwrap().to_string())
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
    // Every entry unpacks as a readable regular file.
    for index in 0..archive.len() {
        assert_eq!(
            archive.by_index(index).unwrap().unix_mode(),
            Some(0o100_644)
        );
    }
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
async fn selection_skips_discovery() {
    // An export into another realm resolves a hash locally and never asks the DHT for holders,
    // so a node without discovery reports no outage for it.
    let dir = tempfile::tempdir().unwrap();
    let driver = DriverContext {
        storage_handle: FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let user = UserId::local(Ulid::from_bytes([150; 16]), RealmId::from_bytes([151; 32]));
    let ctx = job_context(
        Arc::new(driver),
        iroh::SecretKey::from_bytes(&[152; 32]).public(),
    );
    let mut spec = remote_spec(user.realm_id, user);
    let selection = aruna_core::structs::execution::job::ExportSelection {
        files: Vec::new(),
        audience: RealmId::from_bytes([153; 32]),
        intent_digest: String::new(),
    };
    for (selection, unavailable) in [(None, true), (Some(selection), false)] {
        spec.selection = selection;
        let found = hash_lookup(&ctx, &spec, [154; 32]).await.unwrap();
        assert_eq!(found.unavailable, unavailable);
        assert!(found.candidates.is_empty());
    }
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
    let alias = HashIndex::new(
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
    let alias = HashIndex::new(
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

    let mut cross_realm = HashIndex::new(
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
        format: StoredFormat::default(),
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
    let alias = HashIndex::new(
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
            ReadyEntry {
                entity_index: 0,
                candidate_index: 0,
                path: "data/b".to_string(),
                stream: byte_stream(b"b"),
                expected_blake3: *blake3::hash(b"b").as_bytes(),
                modified_ms: FIXTURE_MOMENT_MS,
            },
            ReadyEntry {
                entity_index: 1,
                candidate_index: 0,
                path: "data/a".to_string(),
                stream: byte_stream(b"a"),
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
                aruna_core::structs::storage::replication::ARUNA_DATA_PREFIX,
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
            aruna_core::structs::storage::replication::ARUNA_DATA_PREFIX,
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
        document["@graph"][0][SUBJECT_HTTPS_IRI]["@id"],
        "#aruna-export-report"
    );
    assert_eq!(document["@graph"][0][PART_HTTPS_IRI]["@id"], REPORT_PATH);
    assert_eq!(
        document["@graph"][2][ENCODING_HTTPS_IRI],
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
            aruna_core::structs::storage::replication::ARUNA_DATA_PREFIX,
            hex::encode([hash; 32])
        ),
        "@type": "File",
        "name": "payload",
        "contentUrl": location
    })
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
        content: None,
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
        .map(|index| archive.by_index(index).unwrap().name().unwrap().to_string())
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
        vec![ReadyEntry {
            entity_index: 0,
            candidate_index: 0,
            path: "data/a".to_string(),
            stream: byte_stream(b"a"),
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

#[test]
fn dates_follow_snapshot() {
    // Every export of one pinned snapshot stamps the same time, never the time of the job.
    let event_id = Ulid::from_bytes([74; 16]);
    let checkpoint = ExportCheckpoint {
        winning_event_id: Some(event_id),
        ..ExportCheckpoint::default()
    };
    let moment = archive::snapshot_moment(&checkpoint);
    assert_eq!(moment.ok(), Some(event_id.timestamp_ms()));
    assert!(archive::snapshot_moment(&ExportCheckpoint::default()).is_err());
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
            content: None,
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
        vec![ReadyEntry {
            entity_index: 4,
            candidate_index: 2,
            path: "data/corrupt".to_string(),
            stream: byte_stream(b"wrong"),
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
        vec![ReadyEntry {
            entity_index: 0,
            candidate_index: 0,
            path: "data/pending".to_string(),
            stream: BackendStream::new(futures_util::stream::pending::<
                Result<Bytes, std::io::Error>,
            >()),
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

#[test]
fn selection_keeps_references() {
    // Every left-out source form becomes a web id with its original identifier; the selected
    // file travels and source-local locations are dropped.
    let realm_id = RealmId::from_bytes([2; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
    let version = Ulid::from_bytes([4; 16]);
    let arn = |key: &str| VersionedObjectArn::new(realm_id, node_id, "bucket", key, version);
    let (kept, left) = (arn("kept").unwrap(), arn("left").unwrap());
    let other_realm = RealmId::from_bytes([9; 32]);
    let foreign = VersionedObjectArn::new(other_realm, node_id, "bucket", "f", version).unwrap();
    let other = VersionedObjectArn::new(other_realm, node_id, "bucket", "g", version).unwrap();
    let located = arn("located").unwrap();
    let mut document = json!({
        "@graph": [
            {"@id": kept.to_string(), "@type": "File"},
            {"@id": left.to_string(), "@type": "File", "localPath": "data/left.csv"},
            {"@id": foreign.to_w3id(), "@type": "File"},
            {"@id": other.to_string(), "@type": "File"},
            {"@id": "s3://bucket/located", "@type": "File",
                "contentUrl": ["s3://bucket/located", "https://example.org/located"]},
        ]
    });
    let mut entities = recognized_entities(&document, realm_id).unwrap();
    // A web identifier of another realm is an external reference, not unsupported.
    assert_eq!(entities[2].omission, Some(ReasonCode::External));
    assert_eq!(entities[3].omission, Some(ReasonCode::Unsupported));

    keep_references(&mut entities, &[kept.to_string()]);
    assert_eq!(entities[0].omission, None);
    assert_eq!(entities[1].omission, Some(ReasonCode::External));
    // A location without a current version cannot be named in another realm.
    let failed = add_references(&mut document.clone(), &entities);
    assert!(matches!(failed, Err(ExportFailure::Permanent(_))));
    entities[4].exact = Some(located.clone());
    add_references(&mut document, &entities).unwrap();

    let graph = document["@graph"].as_array().unwrap();
    assert_eq!(graph[0]["@id"], kept.to_string());
    assert_eq!(graph[1]["@id"], left.to_w3id());
    assert_eq!(graph[1]["identifier"], left.to_string());
    assert!(graph[1].get("localPath").is_none());
    assert_eq!(graph[2]["@id"], foreign.to_w3id());
    assert_eq!(graph[3]["@id"], other.to_w3id());
    assert_eq!(graph[3]["identifier"], other.to_string());
    assert_eq!(graph[4]["@id"], located.to_w3id());
    assert_eq!(
        graph[4]["contentUrl"],
        json!(["https://example.org/located"])
    );
}

#[test]
fn nested_references_cleaned() {
    // Aliased graphs, nested File entities and structured locations are cleaned as well.
    let realm_id = RealmId::from_bytes([2; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
    let version = Ulid::from_bytes([4; 16]);
    let left = VersionedObjectArn::new(realm_id, node_id, "bucket", "left", version).unwrap();
    let mut document = json!({
        "@context": [
            "https://w3id.org/ro/crate/1.2/context",
            {"graphItems": "@graph", "idAlias": "@id", "pathAlias": LOCAL_PATH_IRI}
        ],
        "graphItems": [{
            "idAlias": "./",
            "@type": "Dataset",
            "hasPart": [{
                "idAlias": left.to_string(),
                "@type": "File",
                "pathAlias": "data/left.csv",
                "contentUrl": [
                    {"@id": "s3://bucket/left"},
                    {"@value": "s3://bucket/left"},
                    "https://example.org/left"
                ]
            }]
        }]
    });
    let mut entities = recognized_entities(&document, realm_id).unwrap();
    keep_references(&mut entities, &[]);
    add_references(&mut document, &entities).unwrap();

    let nested = &document["graphItems"][0]["hasPart"][0];
    assert_eq!(nested["idAlias"], left.to_w3id());
    assert_eq!(nested["identifier"], left.to_string());
    assert!(nested.get("pathAlias").is_none());
    assert_eq!(nested["contentUrl"], json!(["https://example.org/left"]));
}

#[test]
fn bare_links_unchanged() {
    // A link in `hasPart` stays a bare id, so the importing realm sees one definition.
    let realm_id = RealmId::from_bytes([2; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
    let version = Ulid::from_bytes([4; 16]);
    let left = VersionedObjectArn::new(realm_id, node_id, "bucket", "left", version).unwrap();
    let mut document = json!({
        "@graph": [
            {"@id": "./", "@type": "Dataset", "hasPart": [{"@id": left.to_string()}]},
            {"@id": left.to_string(), "@type": "File"},
        ]
    });
    let mut entities = recognized_entities(&document, realm_id).unwrap();
    keep_references(&mut entities, &[]);
    add_references(&mut document, &entities).unwrap();

    let graph = document["@graph"].as_array().unwrap();
    assert_eq!(graph[0]["hasPart"], json!([{"@id": left.to_w3id()}]));
    assert_eq!(graph[1]["identifier"], left.to_string());
}

#[test]
fn facts_need_selection() {
    // A grant pins only a finished export that kept every selected file.
    let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
    let version = Ulid::from_bytes([4; 16]);
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/data".to_string(),
        storage_bucket: "storage".to_string(),
        backend_path: "bucket/key".to_string(),
        ulid: version,
        format: StoredFormat::default(),
        created_by: Default::default(),
        created_at: std::time::SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 5,
        hashes: HashMap::new(),
    };
    let entity = ExportEntity {
        entity_id: "data.csv".to_string(),
        local_path: None,
        storage_key: None,
        exact: None,
        hash: None,
        hash_realm: None,
        candidates: vec![ExportCandidate {
            source: CandidateSource::Local {
                location: location.clone(),
                group_id: Ulid::from_bytes([6; 16]),
                permission_path: "/object".to_string(),
                node_id,
                bucket: "bucket".to_string(),
                key: "key".to_string(),
            },
            report_source: ExportReportSource::Local,
            resolved_version: Some(version),
            expected_blake3: None,
        }],
        omission: None,
        message: None,
        zip_path: Some("data.csv".to_string()),
        report_source: Some(ExportReportSource::Local),
        resolved_version: Some(version),
        path_synthesized: false,
        content: Some(([7; 32], 5)),
    };
    let mut checkpoint = ExportCheckpoint {
        winning_event_id: Some(Ulid::from_bytes([8; 16])),
        dataset_digest: Some([9; 32]),
        entities: vec![entity],
        artifact: Some(ArtifactRef {
            location,
            blake3: [1; 32],
            size: 100,
            expires_at_ms: 0,
        }),
        ..Default::default()
    };
    let files = ["data.csv".to_string()];
    let facts = checkpoint.export_facts(&files).unwrap();
    assert_eq!(facts.versions.len(), 1);
    assert_eq!(facts.versions[0].size, 5);
    let source = &facts.sources[0];
    assert_eq!(
        (source.bucket.as_str(), source.key.as_str()),
        ("bucket", "key")
    );
    assert_eq!(
        (source.version_id, source.blake3, source.size),
        (version, [7; 32], 5)
    );
    assert!(checkpoint.export_facts(&["other".to_string()]).is_none());
    // A selected file without a version read on this node pins nothing.
    let local = checkpoint.entities[0].candidates[0].source.clone();
    checkpoint.entities[0].candidates[0].source = CandidateSource::RemoteHash {
        node_id,
        hash: [7; 32],
    };
    assert!(checkpoint.export_facts(&files).is_none());
    checkpoint.entities[0].candidates[0].source = local;
    checkpoint.entities[0].omission = Some(ReasonCode::Denied);
    assert!(checkpoint.export_facts(&files).is_none());
}

// A crate written over REST may name its S3 object through a context prefix or a protocol-relative
// URL; export resolves it as JSON-LD does and still exports the object's bytes.
#[test]
fn resolved_references_export() {
    let realm_id = RealmId::from_bytes([2; 32]);
    let typed = json!({"contentUrl": {"@id": "http://schema.org/contentUrl", "@type": "@id"}});
    let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
    let version = Ulid::from_bytes([4; 16]);
    let arn = VersionedObjectArn::new(realm_id, node_id, "bucket", "data/b.txt", version).unwrap();
    let relative = arn.to_w3id().strip_prefix("https:").unwrap().to_string();
    for (extra, url, key) in [
        (
            json!({"store": "s3://bucket/"}),
            "store:data/a.txt",
            "data/a.txt",
        ),
        (json!({}), relative.as_str(), "data/b.txt"),
    ] {
        let mut context = typed.clone();
        context
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let document = json!({
            "@context": ["https://w3id.org/ro/crate/1.2/context", context],
            "@graph": [{"@id": "data/x.txt", "@type": "File", "contentUrl": url}]
        });
        let entities = recognized_entities(&document, realm_id).unwrap();
        let storage = entities[0].storage_key.as_ref().map(|key| key.key.as_str());
        assert_eq!(storage, Some(key), "{url}");
        assert_eq!(entities[0].omission, None, "{url}");
    }
}

// A dataset's own context may resolve a plugin's literal into another object; export names it
// and then checks it against the exporting user per object, as `denies_local_read` shows.
#[test]
fn context_trick_resolves() {
    let realm_id = RealmId::from_bytes([2; 32]);
    let context = json!({"place": "s3://private/",
        "contentUrl": {"@id": "http://schema.org/contentUrl", "@type": "@id"}});
    let document = json!({
        "@context": ["https://w3id.org/ro/crate/1.2/context", context],
        "@graph": [{"@id": "data/b.txt", "@type": "File", "contentUrl": "place:secret.bin"}]
    });
    let entities = recognized_entities(&document, realm_id).unwrap();
    let key = entities[0].storage_key.as_ref().unwrap();
    assert_eq!(
        (key.bucket.as_str(), key.key.as_str()),
        ("private", "secret.bin")
    );
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

pub(super) fn realm_user() -> AuthContext {
    AuthContext {
        user_id: UserId::local(Ulid::from_parts(8, 8), realm()),
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    }
}

pub(super) fn op_spec() -> ExportRoCrateSpec {
    ExportRoCrateSpec {
        destination: None,
        auth_context: realm_user(),
        document_id: Ulid::from_parts(3, 3),
        limits: RoCrateLimits::default(),
        selection: None,
    }
}

pub(super) fn selected_spec() -> ExportRoCrateSpec {
    let mut spec = op_spec();
    spec.selection = Some(aruna_core::structs::execution::job::ExportSelection {
        files: vec!["a".to_string()],
        audience: RealmId::from_bytes([9; 32]),
        intent_digest: String::new(),
    });
    spec
}

pub(super) fn source_scope() -> SourceScope {
    SourceScope {
        spec: Arc::new(op_spec()),
        owner_node: owner_node(),
    }
}

pub(super) fn selected_scope() -> SourceScope {
    SourceScope {
        spec: Arc::new(selected_spec()),
        owner_node: owner_node(),
    }
}

pub(super) fn export_scope() -> ExportScope {
    ExportScope {
        spec: Arc::new(op_spec()),
        job_id: job_id(1),
        claim_token: Ulid::from_parts(9, 9),
        owner_node: owner_node(),
        local_node: Some(owner_node()),
        scope: local_scope(),
        job: scoped_job(),
    }
}

pub(super) fn plain_entity(id: &str) -> ExportEntity {
    ExportEntity {
        entity_id: id.to_string(),
        local_path: None,
        storage_key: None,
        exact: None,
        hash: None,
        hash_realm: None,
        candidates: Vec::new(),
        omission: None,
        message: None,
        zip_path: None,
        report_source: None,
        resolved_version: None,
        path_synthesized: false,
        content: None,
    }
}

pub(super) fn stored_candidate() -> ExportCandidate {
    ExportCandidate {
        source: CandidateSource::Local {
            location: artifact().location,
            group_id: Ulid::from_parts(2, 2),
            permission_path: "/realm/g/group/object".to_string(),
            node_id: owner_node(),
            bucket: "data".to_string(),
            key: "a".to_string(),
        },
        report_source: ExportReportSource::Local,
        resolved_version: Some(Ulid::from_parts(5, 5)),
        expected_blake3: Some([1; 32]),
    }
}

pub(super) fn remote_candidate() -> ExportCandidate {
    ExportCandidate {
        source: CandidateSource::RemoteHash {
            node_id: other_node(),
            hash: [1; 32],
        },
        report_source: ExportReportSource::Hash,
        resolved_version: None,
        expected_blake3: Some([1; 32]),
    }
}
