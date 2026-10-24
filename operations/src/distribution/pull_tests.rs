//! Tests reads through holders: which versions are kept elsewhere, which kept copy a read uses,
//! and which holders a member asks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_record::{binding, replicated, secret, store_record};
use crate::distribution::test_store::{context, open};
use aruna_core::UserId;
use aruna_core::structs::execution::staging::{PortableSourceDescriptor, StagingStrategy};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::format::{Compression, StoredFormat};

fn held(size: u64) -> HeldElsewhere {
    HeldElsewhere {
        set_id: Ulid::from_bytes([1; 16]),
        version_id: Ulid::from_bytes([2; 16]),
        blake3: [3; 32],
        size,
        metadata: HashMap::new(),
        version_created_at: None,
        source_policies: Vec::new(),
    }
}

fn location(size: u64) -> BackendLocation {
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/data".to_string(),
        storage_bucket: "hidden".to_string(),
        backend_path: "_jobs/kept".to_string(),
        ulid: Ulid::nil(),
        format: StoredFormat::default(),
        created_by: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: size,
        hashes: HashMap::new(),
    }
}

fn native(config: &str, value: &str) -> VersionSourceBinding {
    VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::ArunaNative,
            public_config: HashMap::from([(config.to_string(), value.to_string())]),
            source_path: "spire/a".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
        },
        connector_id: None,
    }
}

// Only a record-only version of a set with a known BLAKE3 is read through holders.
#[test]
fn names_set_versions() {
    let set_id = Ulid::from_bytes([1; 16]);
    let source = native(NATIVE_SET_ID, &set_id.to_string());
    assert_eq!(
        HeldElsewhere::of(&source, Some([3; 32])),
        Some((set_id, [3; 32]))
    );
    assert_eq!(HeldElsewhere::of(&source, None), None);
    let relationship = native("relationship_id", &set_id.to_string());
    assert_eq!(HeldElsewhere::of(&relationship, Some([3; 32])), None);
}

// A kept copy serves only when its BLAKE3 and size are the version's; the attachment wins.
#[test]
fn kept_copy_matches() {
    let held = held(9);
    let attached = Attachment {
        blake3: [3; 32],
        location: location(9),
    };
    let entry = CacheEntry {
        key: [3; 32],
        blake3: [3; 32],
        location: location(9),
        size: 9,
        order: Ulid::nil(),
    };
    let row = |value: Option<Vec<u8>>| (Key::from(Vec::new()), value.map(Value::from));
    let both = [
        row(Some(attached.to_bytes().unwrap())),
        row(Some(entry.to_bytes().unwrap())),
    ];
    assert_eq!(kept_location(&held, &both), Some(location(9)));
    let stale = Attachment {
        location: location(8),
        ..attached
    };
    let cached = [
        row(Some(stale.to_bytes().unwrap())),
        row(Some(entry.to_bytes().unwrap())),
    ];
    assert_eq!(kept_location(&held, &cached), Some(location(9)));
    let foreign = CacheEntry {
        blake3: [4; 32],
        ..entry
    };
    assert_eq!(
        kept_location(&held, &[row(None), row(Some(foreign.to_bytes().unwrap()))]),
        None
    );
}

// A kept copy of an external reference serves only when complete and hashed as recorded.
#[test]
fn kept_reference_matches() {
    let mut whole = location(9);
    whole.hashes = HashMap::from([(HASH_BLAKE3.to_string(), vec![5; 32])]);
    let attached = Attachment {
        blake3: [5; 32],
        location: whole.clone(),
    };
    let row = |value: Vec<u8>| (Key::from(Vec::new()), Some(Value::from(value)));
    let none = (Key::from(Vec::new()), None);
    let values = [row(attached.to_bytes().unwrap()), none.clone()];
    assert_eq!(kept_source_location(&values, 9), Some(whole.clone()));
    assert_eq!(kept_source_location(&values, 10), None);
    let forged = Attachment {
        blake3: [6; 32],
        location: whole,
    };
    assert_eq!(
        kept_source_location(&[row(forged.to_bytes().unwrap()), none], 9),
        None
    );
}

// A member asks the holders of the key's range, never itself, and keeps nothing for an
// encrypting bucket.
#[tokio::test]
async fn asks_range_holders() {
    let (_dir, handle) = open();
    let (snapshot, record) = replicated(true, 1);
    store_record(&handle, snapshot.set_id(), record).await;
    let bucket = BucketInfo {
        group_id: Ulid::from_bytes([90; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::nil(RealmId::from_bytes([9; 32])),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: Some(binding(&snapshot, 1, 5)),
    };
    let write = StorageEffect::Write {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: b"spire".to_vec().into(),
        value: bucket.to_bytes().unwrap().into(),
        txn_id: None,
    };
    handle.send_storage_effect(write).await;
    let request = PullRequest {
        bucket: "spire".to_string(),
        key: "x".to_string(),
        range: None,
        auth: AuthContext {
            user_id: UserId::nil(RealmId::from_bytes([9; 32])),
            realm_id: RealmId::from_bytes([9; 32]),
            path_restrictions: None,
            session: None,
        },
        held: HeldElsewhere {
            set_id: snapshot.set_id(),
            ..held(9)
        },
    };

    let member = member_of(&context(&handle), &request).await.unwrap();

    assert_eq!(member.holders, vec![secret(2).public()]);
    assert_eq!(member.strategy, DataStrategy::Copy);
    assert!(member.keeping);
    let stranger = PullRequest {
        held: held(9),
        ..request
    };
    assert_eq!(
        member_of(&context(&handle), &stranger).await,
        Err(GetObjectError::HoldersUnavailable)
    );
}
