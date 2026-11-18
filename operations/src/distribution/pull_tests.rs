//! Tests reads through holders: which versions are kept elsewhere, which kept copy a read uses,
//! and which holders a member asks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::drive;
use aruna_core::effects::StorageEffect;
use crate::connectors::resolver::NATIVE_SET_ID;
use crate::distribution::held::{kept_location, kept_reads, kept_source_location};
use crate::distribution::test_record::{binding, replicated, secret, store_record};
use crate::distribution::test_store::{context, open};
use aruna_core::UserId;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::execution::staging::{PortableSourceDescriptor, StagingStrategy};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::distribution::on_use::{Attachment, CacheEntry};
use aruna_core::structs::storage::format::{Compression, StoredFormat};
use aruna_core::types::Key;
use std::time::SystemTime;

fn held(size: u64) -> HeldElsewhere {
    HeldElsewhere {
        set_id: Ulid::from_bytes([1; 16]),
        version_id: Ulid::from_bytes([2; 16]),
        identity: [3; 32],
        blake3: Some([3; 32]),
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
            root: None,
        },
        connector_id: None,
        mirrors: Vec::new(),
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

// An external reference names no BLAKE3, so its kept copy serves by identity and size alone.
#[test]
fn kept_external_matches() {
    let external = HeldElsewhere {
        blake3: None,
        ..held(9)
    };
    let entry = CacheEntry {
        key: external.identity,
        blake3: [4; 32],
        location: location(9),
        size: 9,
        order: Ulid::nil(),
    };
    let row = |value: Option<Vec<u8>>| (Key::from(Vec::new()), value.map(Value::from));
    let values = [row(None), row(Some(entry.to_bytes().unwrap()))];
    assert_eq!(kept_location(&external, &values), Some(location(9)));
    let short = HeldElsewhere {
        size: 8,
        ..external.clone()
    };
    assert_eq!(kept_location(&short, &values), None);
    assert!(!external.info().hashes.contains_key(HASH_BLAKE3));
    assert_eq!(
        kept_reads(&external)[1].1.to_vec(),
        external.identity.to_vec()
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

    let member = drive(PullMemberOperation::new(request.clone()), &context(&handle)).await.unwrap();

    assert_eq!(member.holders, vec![secret(2).public()]);
    assert_eq!(member.strategy, DataStrategy::Copy);
    assert!(member.keeping);
    let governed = PullRequest {
        held: HeldElsewhere {
            source_policies: vec![PlacementPolicyRef {
                policy_id: Ulid::from_bytes([7; 16]),
                digest: [7; 32],
            }],
            ..request.held.clone()
        },
        ..request.clone()
    };
    assert!(
        !drive(PullMemberOperation::new(governed), &context(&handle))
            .await
            .unwrap()
            .keeping
    );
    let stranger = PullRequest {
        held: held(9),
        ..request
    };
    assert_eq!(
        drive(PullMemberOperation::new(stranger), &context(&handle)).await,
        Err(GetObjectError::HoldersUnavailable)
    );
}

fn pull_request(size: u64) -> PullRequest {
    let realm_id = RealmId::from_bytes([9; 32]);
    PullRequest {
        bucket: "spire".to_string(),
        key: "x".to_string(),
        range: None,
        auth: AuthContext {
            user_id: UserId::nil(realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        },
        held: held(size),
    }
}

fn member() -> Member {
    Member {
        member_id: Ulid::from_bytes([1; 16]),
        strategy: DataStrategy::CacheOnUse,
        behaviour: RangeBehaviour::default(),
        keeping: true,
        holders: vec![secret(2).public()],
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
}

// A copy the cache can never hold is not fetched, and a running fill is left alone.
#[test]
fn fill_skips_unfit() {
    let huge = pull_request(CACHE_CAPACITY_BYTES + 1);
    let mut skipped = FillOperation::new(huge, KeptKind::Cache, member());
    assert_eq!(skipped.first(), Ok(Next::Done(None)));
    let mut busy = FillOperation::new(pull_request(9), KeptKind::Cache, member());
    assert!(matches!(busy.first(), Ok(Next::Call(IoCall::Child(_)))));
    let answer = child(Ok::<_, KeepError>(Claimed::Busy));
    assert_eq!(busy.next(answer), Ok(Next::Done(None)));
}

// With no holder left the collected failures decide the error.
#[test]
fn holders_exhausted_fail() {
    let mut read = FromHoldersOperation::new(&pull_request(9), &[], None);
    assert!(read.first().is_err());
}

// A failed holder is avoided before the next one is asked.
#[test]
fn avoids_failed_holder() {
    let holders = [secret(2).public(), secret(3).public()];
    let mut read = FromHoldersOperation::new(&pull_request(9), &holders, None);
    read.first().unwrap();
    let refused = BaoReadError::Refused(BaoReadRefusal::BackendFailure);
    let answer = child(Err::<BaoReadOutput, _>(refused));
    assert!(matches!(read.next(answer), Ok(Next::Call(IoCall::Effect(_)))));
    assert!(read.avoiding);
}

// A claim is deleted outside any transaction, and a failure leaves it to go stale.
#[test]
fn release_tolerates_failure() {
    let mut release = ReleaseClaimOperation::new(vec![1]);
    assert!(matches!(release.first(), Ok(Next::Call(IoCall::Child(_)))));
    let failed = child(Err::<IoResult, _>(IoError::Unfinished));
    assert_eq!(release.next(failed), Ok(Next::Done(())));
}
