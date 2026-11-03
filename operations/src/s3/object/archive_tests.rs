//! Tests selection archive paths, statuses, exact versions and an archive reader that leaves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use crate::tests::access::seed_access;
use crate::tests::staging::{create_test_bucket, setup_driver_context};
use aruna_core::stream::BackendStream;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use std::io::Read;

const BUCKET: &str = "selection";

fn version(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

fn entry(key: &str, version_id: Option<Ulid>) -> SelectionEntry {
    SelectionEntry {
        bucket: BUCKET.to_string(),
        key: key.to_string(),
        version_id,
    }
}

#[test]
fn paths_avoid_collisions() {
    let entries = [
        ("b", "data/a.csv", version(1)),
        ("b", "data/a.csv", version(2)),
        ("b", "data", version(3)),
        ("b", "../escape", version(4)),
        ("b", "plain.txt", version(5)),
        ("c", "plain.txt", version(6)),
        ("b", "plain.txt", version(5)),
    ];
    let paths = plan_paths(&entries);
    assert_eq!(
        paths,
        vec![
            format!("_renamed/b/{}", version(1)),
            format!("_renamed/b/{}", version(2)),
            format!("_renamed/b/{}", version(3)),
            format!("_renamed/b/{}", version(4)),
            "b/plain.txt".to_string(),
            "c/plain.txt".to_string(),
            "b/plain.txt".to_string(),
        ]
    );
    assert_eq!(plan_paths(&entries), paths, "the plan is deterministic");
    let distinct: HashSet<_> = paths[..6].iter().collect();
    assert_eq!(distinct.len(), 6);
}

#[test]
fn selection_is_bounded() {
    assert!(matches!(validate_selection(&[]), Err(SelectionError::Size)));
    let many: Vec<_> = (0..=MAX_ENTRIES)
        .map(|index| entry(&format!("k{index}"), None))
        .collect();
    assert!(matches!(
        validate_selection(&many),
        Err(SelectionError::Size)
    ));
    let twice = [entry("a", None), entry("a", None)];
    assert!(matches!(
        validate_selection(&twice),
        Err(SelectionError::Duplicate(_))
    ));
    let bad = [SelectionEntry {
        bucket: "_x".to_string(),
        ..entry("a", None)
    }];
    assert!(matches!(
        validate_selection(&bad),
        Err(SelectionError::Bucket(_))
    ));
}

async fn put(
    context: &DriverContext,
    auth: &AuthContext,
    scope: (GroupId, NodeId),
    key: &str,
    body: &'static [u8],
) -> Ulid {
    let (group_id, node_id) = scope;
    drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: auth.user_id,
            group_id,
            realm_id: auth.realm_id,
            node_id,
            request: PutObjectInput {
                bucket: BUCKET.to_string(),
                key: key.to_string(),
                content_length: Some(body.len() as u64),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(body))),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        }),
        context,
    )
    .await
    .expect("put drives")
    .version_id
}

struct Fixture {
    staging: crate::tests::staging::StagingTestContext,
    auth: AuthContext,
    realm_id: RealmId,
    node_id: NodeId,
    old_version: Ulid,
}

async fn fixture() -> Fixture {
    let staging = setup_driver_context().await;
    let context = &staging.driver_context;
    let realm_id = RealmId::from_bytes([2; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[6; 32]).public();
    let group_id = Ulid::from_parts(2, 2);
    let denied = object_permission_path(realm_id, group_id, node_id, BUCKET, "secret/**");
    let auth = seed_access(context, (realm_id, group_id, node_id), &denied).await;
    create_test_bucket(context, group_id, auth.user_id, BUCKET).await;
    let scope = (group_id, node_id);
    let old_version = put(context, &auth, scope, "data/a.csv", b"first").await;
    put(context, &auth, scope, "data/a.csv", b"second").await;
    put(context, &auth, scope, "secret/b.csv", b"hidden").await;
    Fixture {
        staging,
        auth,
        realm_id,
        node_id,
        old_version,
    }
}

fn unzip(bytes: &[u8]) -> HashMap<String, Vec<u8>> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).expect("valid archive");
    (0..archive.len())
        .map(|index| {
            let mut file = archive.by_index(index).expect("entry");
            let mut content = Vec::new();
            file.read_to_end(&mut content).expect("entry reads");
            (file.name().expect("utf-8 name").to_string(), content)
        })
        .collect()
}

#[tokio::test]
async fn archive_reports_entries() {
    let fixture = fixture().await;
    let context = &fixture.staging.driver_context;
    let entries = vec![
        entry("data/a.csv", None),
        entry("data/a.csv", Some(fixture.old_version)),
        entry("secret/b.csv", None),
        entry("absent.csv", None),
    ];
    let mut bytes = Vec::new();
    let scope = (&fixture.auth, fixture.realm_id, fixture.node_id);
    let rows = write_selection(context, scope, entries, &mut bytes)
        .await
        .expect("archive is written");
    let statuses: Vec<_> = rows.iter().map(|row| row.status).collect();
    assert_eq!(
        statuses,
        vec![
            EntryStatus::Included,
            EntryStatus::Included,
            EntryStatus::Denied,
            EntryStatus::Missing,
        ]
    );
    let files = unzip(&bytes);
    let current = rows[0].path.as_deref().expect("current path");
    let old = rows[1].path.as_deref().expect("old path");
    assert_ne!(current, old, "two versions of one key never share a path");
    assert_eq!(files[current], b"second");
    assert_eq!(files[old], b"first");
    assert_eq!(rows[1].version_id, Some(fixture.old_version.to_string()));
    assert_eq!(
        rows[1].blake3.as_deref(),
        Some(blake3::hash(b"first").to_hex().as_str())
    );
    assert!(rows[2].path.is_none() && rows[2].size.is_none());
    assert!(!files.keys().any(|name| name.contains("secret")));
    let report: serde_json::Value = serde_json::from_slice(&files[REPORT_PATH]).unwrap();
    assert_eq!(report["entries"][2]["status"], "denied");
    assert_eq!(report["entries"][3]["status"], "missing");
    assert_eq!(files.len(), 3);
}

#[tokio::test]
async fn closed_reader_stops() {
    let fixture = fixture().await;
    let context = &fixture.staging.driver_context;
    let (writer, reader) = tokio::io::duplex(16);
    drop(reader);
    let scope = (&fixture.auth, fixture.realm_id, fixture.node_id);
    let result = write_selection(context, scope, vec![entry("data/a.csv", None)], writer).await;
    assert!(matches!(result, Err(SelectionError::Closed)));
}
