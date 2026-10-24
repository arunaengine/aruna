// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::structs::identity::realm::RealmId;

fn global(seed: u8) -> OriginVersion {
    OriginVersion::Global(GlobalId::Version {
        realm_id: RealmId::from_bytes([1; 32]),
        version_id: Ulid::from_bytes([seed; 16]),
    })
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[2; 32]).public()
}

#[test]
fn uploads_and_imports_stay_private() {
    // Uploader, device and storage paths are never projected into datasets.
    let upload = VersionOrigin::Upload {
        node_id: node(),
        device: Some("laptop".to_string()),
    };
    assert!(project(Some(&upload), None, None).is_empty());
    let storage = VersionOrigin::Storage {
        backend_id: Ulid::nil(),
        key: "share/a.txt".to_string(),
    };
    assert!(project(Some(&storage), None, None).is_empty());
}

#[test]
fn copies_and_syncs_name_the_original() {
    let copy = VersionOrigin::Copy { from: global(3) };
    assert_eq!(
        project(Some(&copy), None, None),
        vec![OriginProjection::BasedOn {
            id: global(3).to_id()
        }]
    );
    let hop = SyncHop {
        relationship_id: Ulid::nil(),
        from: global(4),
    };
    let upload = VersionOrigin::Upload {
        node_id: node(),
        device: None,
    };
    assert_eq!(
        project(Some(&upload), Some(&hop), None),
        vec![OriginProjection::BasedOn {
            id: global(4).to_id()
        }]
    );
}

#[test]
fn sources_and_jobs_project() {
    let source = VersionOrigin::Source { database: None };
    let url = "https://s3.example.org/reads/a.fastq".to_string();
    assert_eq!(
        project(Some(&source), None, Some(url.clone())),
        vec![OriginProjection::BasedOn { id: url }]
    );
    let job = VersionOrigin::Job {
        run_id: Ulid::from_bytes([5; 16]),
    };
    assert_eq!(
        project(Some(&job), None, None),
        vec![OriginProjection::CreatedBy {
            run_id: Ulid::from_bytes([5; 16])
        }]
    );
}

#[test]
fn oversized_device_is_unbounded() {
    let upload = VersionOrigin::Upload {
        node_id: node(),
        device: Some("d".repeat(MAX_DEVICE_BYTES + 1)),
    };
    assert!(!upload.bounded());
}
