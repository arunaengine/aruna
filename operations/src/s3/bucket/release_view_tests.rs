//! Tests the release file listing with hand-fed crate reads: pinned versions in key order,
//! folders and pages, and crates that are gone, untagged or broken.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::structs::storage::bucket_metadata::{RELEASE_PREFIX_TAG, RELEASE_TAG};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::origin::OriginVersion;
use aruna_core::structs::storage::release::ReleaseFile;
use aruna_core::structs::storage::replication::VersionedObjectArn;

use super::*;
use crate::distribution::test_record::secret;
use crate::s3::bucket::release_publish::{ReleaseSpec, release_crate};

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn version(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

fn config(page: FilePage) -> ViewConfig {
    ViewConfig {
        realm_id: realm(),
        node_id: secret(1).public(),
        bucket: "bakta-db".to_string(),
        info: BucketInfo {
            group_id: Ulid::from_bytes([4; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        },
        auth: AuthContext::anonymous(realm()),
        key: "releases/6.0.crate.json".to_string(),
        version_id: version(9),
        page,
    }
}

/// A crate pinning `keys`, the first by its global id, the others by node ARN.
fn crate_bytes(keys: &[&str]) -> Vec<u8> {
    let files: Vec<ReleaseFile> = (keys.iter().enumerate())
        .map(|(index, key)| {
            let version_id = version(index as u8 + 1);
            let version = match index {
                0 => OriginVersion::Global(GlobalId::Version {
                    realm_id: realm(),
                    version_id,
                }),
                _ => OriginVersion::Node(
                    VersionedObjectArn::new(
                        realm(),
                        secret(1).public(),
                        "bakta-db",
                        *key,
                        version_id,
                    )
                    .unwrap(),
                ),
            };
            ReleaseFile {
                key: key.to_string(),
                version,
                size: 10 + index as u64,
                content: (index == 1).then(|| "blake3:ab".to_string()),
                pin: None,
            }
        })
        .collect();
    let spec = ReleaseSpec {
        label: "6.0".to_string(),
        prefix: "db/".to_string(),
        upstream: Vec::new(),
        preparation: Vec::new(),
    };
    release_crate(&spec, &files)
}

fn tagged() -> HashMap<String, String> {
    HashMap::from([
        (RELEASE_TAG.to_string(), "6.0".to_string()),
        (RELEASE_PREFIX_TAG.to_string(), "db/".to_string()),
    ])
}

fn run(page: FilePage, read: Result<CrateBytes, CrateReadError>) -> Result<ReleaseView, ViewError> {
    let mut operation = ReleaseViewOperation::new(config(page));
    let effects = operation.start();
    let [Effect::Net(NetEffect::Release(effect))] = effects.as_slice() else {
        panic!("the crate is read first: {effects:?}");
    };
    let ReleaseEffect::ReadCrate(asked) = effect.as_ref() else {
        panic!("a crate read is asked");
    };
    assert_eq!(
        (asked.key.as_str(), asked.version_id),
        ("releases/6.0.crate.json", version(9))
    );
    let event = Event::Net(NetEvent::Release(ReleaseEvent::CrateRead(read)));
    assert!(operation.step(event).is_empty());
    assert!(operation.is_complete());
    operation.finalize()
}

fn read(bytes: Vec<u8>, metadata: HashMap<String, String>) -> Result<CrateBytes, CrateReadError> {
    Ok(CrateBytes { metadata, bytes })
}

fn all() -> FilePage {
    FilePage {
        limit: 100,
        ..FilePage::default()
    }
}

#[test]
fn lists_pinned_versions() {
    let bytes = crate_bytes(&["db/b.faa", "db/a.faa", "db/sub/c.tsv"]);
    let view = run(all(), read(bytes, tagged())).unwrap();
    assert_eq!((view.label.as_str(), view.prefix.as_str()), ("6.0", "db/"));
    let keys: Vec<(&str, Ulid, u64)> = (view.files.iter())
        .map(|file| (file.key.as_str(), file.version_id, file.size))
        .collect();
    assert_eq!(
        keys,
        [
            ("db/a.faa", version(2), 11),
            ("db/b.faa", version(1), 10),
            ("db/sub/c.tsv", version(3), 12)
        ]
    );
    assert_eq!(view.files[0].content_id.as_deref(), Some("blake3:ab"));
    assert!(view.files[1].id.starts_with("https://"));
    assert_eq!(view.next, None);
}

#[test]
fn pages_and_folders() {
    let bytes = crate_bytes(&["db/a", "db/sub/x", "db/sub/y", "db/z", "other/q"]);
    let page = |after: Option<&str>, folders| FilePage {
        prefix: "db/".to_string(),
        folders,
        after: after.map(str::to_string),
        limit: 2,
    };
    let first = run(page(None, true), read(bytes.clone(), tagged())).unwrap();
    let keys: Vec<&str> = first.files.iter().map(|file| file.key.as_str()).collect();
    assert_eq!(
        (keys, first.folders.clone()),
        (vec!["db/a"], vec!["db/sub/".to_string()])
    );
    assert_eq!(first.next.as_deref(), Some("db/sub/"));
    let second = run(
        page(first.next.as_deref(), true),
        read(bytes.clone(), tagged()),
    )
    .unwrap();
    let keys: Vec<&str> = second.files.iter().map(|file| file.key.as_str()).collect();
    assert_eq!(
        (keys, second.folders.len(), second.next),
        (vec!["db/z"], 0, None)
    );
    // Without folders every key under the prefix is listed, never keys beside it.
    let flat = run(page(Some("db/a"), false), read(bytes, tagged())).unwrap();
    let keys: Vec<&str> = flat.files.iter().map(|file| file.key.as_str()).collect();
    assert_eq!(keys, ["db/sub/x", "db/sub/y"]);
    assert_eq!(flat.next.as_deref(), Some("db/sub/y"));
}

#[test]
fn refuses_non_releases() {
    let bytes = crate_bytes(&["db/a"]);
    let missing = run(all(), Err(CrateReadError::Missing));
    assert_eq!(missing, Err(ViewError::Missing));
    assert_eq!(
        run(all(), read(bytes, HashMap::new())),
        Err(ViewError::NoRelease)
    );
    assert_eq!(
        run(all(), read(b"{".to_vec(), tagged())),
        Err(ViewError::Malformed)
    );
    let failed = run(all(), Err(CrateReadError::Failed("down".into())));
    assert_eq!(failed, Err(ViewError::Unavailable("down".into())));
    // An event the operation does not wait for ends it with an error.
    let mut operation = ReleaseViewOperation::new(config(all()));
    operation.start();
    operation.step(Event::Net(NetEvent::Release(ReleaseEvent::Holder(Err(
        "x".into(),
    )))));
    assert!(matches!(
        operation.finalize(),
        Err(ViewError::UnexpectedEvent(_))
    ));
}
