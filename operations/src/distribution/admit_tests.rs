//! Tests member admission against real storage: opened ranges, bucket checks, capacity and
//! signing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::keyspaces::RANGE_INDEX_KEYSPACE;
use aruna_core::structs::storage::distribution::DistributionBinding;
use aruna_core::structs::storage::distribution::accounting::slot_key;
use aruna_core::structs::storage::distribution::capacity::OBJECT_METADATA_BYTES;
use aruna_core::structs::storage::format::Compression;
use aruna_storage::storage::StorageHandle;

use super::*;
use crate::distribution::test_store::{get, open, range, send};

const BUCKET: &str = "spire";

fn secret() -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[4; 32])
}

fn realm() -> RealmId {
    RealmId::from_bytes([9; 32])
}

fn set_id() -> Ulid {
    Ulid::from_bytes([1; 16])
}

fn member_id() -> Ulid {
    Ulid::from_bytes([2; 16])
}

fn ranges() -> Vec<KeyRange> {
    vec![range(10, "", Some("m")), range(11, "m", None)]
}

fn config() -> AdmitConfig {
    AdmitConfig {
        realm_id: realm(),
        node_id: secret().public(),
        bucket: BUCKET.to_string(),
        set_id: set_id(),
        member_id: member_id(),
        intent: [7; 32],
        ranges: ranges(),
        strategy: DataStrategy::Copy,
        coverage: MetadataCoverage::Assigned,
        sizes: ranges().iter().map(|range| empty(range.range_id)).collect(),
        capacity: None,
    }
}

fn empty(range_id: Ulid) -> RangeSize {
    RangeSize {
        range_id,
        ..RangeSize::default()
    }
}

/// Writes the local bucket bound to `member` with `bound` as its incarnation, created as
/// incarnation `created`.
async fn bucket(handle: &StorageHandle, member: Ulid, bound: u128, created: u128) {
    bound_bucket(handle, (BUCKET, set_id(), member), bound, created).await;
}

async fn bound_bucket(
    handle: &StorageHandle,
    (name, set_id, member): (&str, Ulid, Ulid),
    bound: u128,
    created: u128,
) {
    let info = BucketInfo {
        group_id: Ulid::from_bytes([3; 16]),
        created_at: SystemTime::UNIX_EPOCH,
        created_by: UserId::local(Ulid::from_bytes([5; 16]), realm()),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: Some(DistributionBinding {
            set_id,
            member_id: member,
            incarnation: bound,
            revision: Default::default(),
        }),
    };
    let key: Key = name.as_bytes().into();
    let writes = vec![
        (
            S3_BUCKET_KEYSPACE.to_string(),
            key.clone(),
            info.to_bytes().unwrap().into(),
        ),
        (
            BUCKET_CREATION_KEYSPACE.to_string(),
            key,
            created.to_be_bytes().to_vec().into(),
        ),
    ];
    let effect = Effect::Storage(StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    });
    assert!(matches!(
        send(handle, effect).await,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

/// Runs the admission against real storage; `sign` false answers the signing request with an
/// unavailable signer.
async fn run(handle: &StorageHandle, sign: bool) -> Result<SignedStatement, AdmitError> {
    run_with(handle, config(), sign).await
}

async fn run_with(
    handle: &StorageHandle,
    config: AdmitConfig,
    sign: bool,
) -> Result<SignedStatement, AdmitError> {
    let mut operation = AdmitOperation::new(config);
    drive_until(handle, &mut operation, sign, |_| false).await;
    operation.finalize()
}

/// Drives `operation` until `stop` matches the next effect or it ends.
async fn drive_until(
    handle: &StorageHandle,
    operation: &mut AdmitOperation,
    sign: bool,
    stop: impl Fn(&Effect) -> bool,
) -> VecDeque<Effect> {
    let mut queue: VecDeque<Effect> = operation.start().into_iter().collect();
    resume(handle, operation, sign, &mut queue, stop).await;
    queue
}

async fn resume(
    handle: &StorageHandle,
    operation: &mut AdmitOperation,
    sign: bool,
    queue: &mut VecDeque<Effect>,
    stop: impl Fn(&Effect) -> bool,
) {
    while let Some(effect) = queue.pop_front() {
        if stop(&effect) {
            queue.push_front(effect);
            return;
        }
        let event = match effect {
            Effect::Net(NetEffect::StatementSign(claim)) => {
                assert_eq!(claim.signer, secret().public());
                let reply = match sign {
                    true => StatementSignEvent::Signed(Box::new(claim.statement.sign(&secret()))),
                    false => StatementSignEvent::Unavailable("offline".to_string()),
                };
                Event::Net(NetEvent::StatementSign(reply))
            }
            effect => send(handle, effect).await,
        };
        queue.extend(operation.step(event));
    }
}

async fn opened(handle: &StorageHandle, upper: Option<&str>) -> bool {
    get(handle, RANGE_INDEX_KEYSPACE, slot_key(set_id(), upper))
        .await
        .is_some()
}

#[tokio::test]
async fn opens_ranges_once() {
    let (_dir, handle) = open();
    bucket(&handle, member_id(), 42, 42).await;
    let first = run(&handle, true).await.unwrap();
    assert!(first.verified());
    assert_eq!(first.statement.incarnation, 42);
    let expected = Statement::admit(realm(), set_id(), member_id(), 42, [7; 32]);
    assert_eq!(first.statement, expected);
    assert!(opened(&handle, Some("m")).await);
    assert!(opened(&handle, None).await);
    // A retry finds the ranges open and signs the very same statement again.
    assert_eq!(run(&handle, true).await.unwrap(), first);
}

#[tokio::test]
async fn foreign_bucket_refused() {
    let (_dir, handle) = open();
    bucket(&handle, Ulid::from_bytes([8; 16]), 42, 42).await;
    assert_eq!(
        run(&handle, true).await,
        Err(AdmitError::BindingChanged(BUCKET.to_string()))
    );
    assert!(!opened(&handle, None).await);
}

#[tokio::test]
async fn replaced_bucket_refused() {
    let (_dir, handle) = open();
    // The binding names an incarnation that is not this bucket's own creation.
    bucket(&handle, member_id(), 42, 43).await;
    assert_eq!(
        run(&handle, true).await,
        Err(AdmitError::BindingChanged(BUCKET.to_string()))
    );
}

#[tokio::test]
async fn unsigned_opens_nothing() {
    let (_dir, handle) = open();
    bucket(&handle, member_id(), 42, 42).await;
    assert!(matches!(
        run(&handle, false).await,
        Err(AdmitError::SignUnavailable(_))
    ));
    assert!(!opened(&handle, None).await);
}

#[tokio::test]
async fn rebuilding_usage_refused() {
    let (_dir, handle) = open();
    bucket(&handle, member_id(), 42, 42).await;
    let marker = Effect::Storage(StorageEffect::Write {
        key_space: USAGE_STATS_KEYSPACE.to_string(),
        key: USAGE_REBUILD_KEY.to_vec().into(),
        value: Vec::new().into(),
        txn_id: None,
    });
    send(&handle, marker).await;
    assert_eq!(run(&handle, true).await, Err(AdmitError::UsageIncomplete));
    assert!(
        !opened(&handle, None).await,
        "no range opens while usage counters are incomplete"
    );
}

/// A copy replica of both ranges; the first holds one object of 1000 bytes.
fn sized(capacity: Option<u64>) -> AdmitConfig {
    let mut config = config();
    for range in &mut config.ranges {
        range.replicas = vec![config.member_id];
    }
    config.sizes[0] = RangeSize {
        range_id: config.ranges[0].range_id,
        bytes: 1000,
        objects: 1,
    };
    config.capacity = capacity;
    config
}

const NEED: u64 = 1000 + OBJECT_METADATA_BYTES;

async fn reserved(handle: &StorageHandle, key: Vec<u8>) -> Option<Vec<u8>> {
    get(handle, CAPACITY_RESERVATION_KEYSPACE, key).await
}

async fn put(handle: &StorageHandle, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    let effect = Effect::Storage(StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    });
    send(handle, effect).await;
}

/// An admission that fits reserves its member row and one row per opened range.
#[tokio::test]
async fn reserves_within_capacity() {
    let (_dir, handle) = open();
    bucket(&handle, member_id(), 42, 42).await;
    run_with(&handle, sized(Some(NEED)), true).await.unwrap();
    let member = reserved(&handle, set_id().to_bytes().to_vec())
        .await
        .unwrap();
    let member = MemberReservation::from_bytes(&member).unwrap();
    assert_eq!(member.bucket, BUCKET);
    let first = account_key(set_id(), ranges()[0].range_id);
    let empty = account_key(set_id(), ranges()[1].range_id);
    assert_eq!(
        reserved(&handle, first).await,
        Some(NEED.to_be_bytes().to_vec())
    );
    assert_eq!(
        reserved(&handle, empty).await,
        Some(0u64.to_be_bytes().to_vec())
    );
}

/// Stored bytes and the unfilled part of other members' reservations count as used; an
/// admission that does not fit opens and reserves nothing.
#[tokio::test]
async fn counts_unfilled_reservations() {
    let (_dir, handle) = open();
    bucket(&handle, member_id(), 42, 42).await;
    let stored = UsageCounters {
        stored_bytes: 1000,
        ..Default::default()
    };
    let shard = global_shard_keys().remove(0);
    put(
        &handle,
        USAGE_STATS_KEYSPACE,
        shard,
        stored.to_bytes().unwrap(),
    )
    .await;
    let other = Ulid::from_bytes([6; 16]);
    let member = MemberReservation {
        bucket: "other".to_string(),
        cache_bytes: 0,
    };
    let space = CAPACITY_RESERVATION_KEYSPACE;
    put(
        &handle,
        space,
        other.to_bytes().to_vec(),
        member.to_bytes().unwrap(),
    )
    .await;
    let range_row = account_key(other, Ulid::from_bytes([12; 16]));
    put(&handle, space, range_row, 4000u64.to_be_bytes().to_vec()).await;
    let filled = BucketCounters {
        logical_bytes: 1000,
        ..Default::default()
    };
    let key = usage_bucket_key("other");
    put(
        &handle,
        USAGE_STATS_KEYSPACE,
        key,
        filled.to_bytes().unwrap(),
    )
    .await;

    // Used: 1000 stored and 3000 reserved but not filled yet.
    let refused = run_with(&handle, sized(Some(4000 + NEED - 1)), true).await;
    let free = NEED - 1;
    assert_eq!(
        refused,
        Err(AdmitError::InsufficientCapacity { need: NEED, free })
    );
    assert!(!opened(&handle, None).await);
    assert_eq!(reserved(&handle, set_id().to_bytes().to_vec()).await, None);
    run_with(&handle, sized(Some(4000 + NEED)), true)
        .await
        .unwrap();
    assert!(opened(&handle, None).await);
}

/// Without a configured capacity a member with `all` coverage is refused; an assigned one is
/// admitted, as nothing bounds its node.
#[tokio::test]
async fn unknown_capacity_all() {
    let (_dir, handle) = open();
    bucket(&handle, member_id(), 42, 42).await;
    let mut all = sized(None);
    all.coverage = MetadataCoverage::All;
    assert_eq!(
        run_with(&handle, all, true).await,
        Err(AdmitError::CapacityUnknown)
    );
    assert!(!opened(&handle, None).await);
    run_with(&handle, sized(None), true).await.unwrap();
}

/// A range without a size is unknown, not empty: the admission opens and reserves nothing.
#[tokio::test]
async fn unknown_size_refused() {
    let (_dir, handle) = open();
    bucket(&handle, member_id(), 42, 42).await;
    let mut unknown = sized(Some(u64::MAX));
    unknown.sizes.pop();
    let range_id = ranges()[1].range_id;
    let refused = run_with(&handle, unknown, true).await;
    assert_eq!(refused, Err(AdmitError::SizeUnknown(range_id)));
    assert!(!opened(&handle, None).await);
    assert_eq!(reserved(&handle, set_id().to_bytes().to_vec()).await, None);
}

/// Two admissions that each fit alone but not together cannot both commit: the reservations
/// read in one transaction conflict with the other's commit.
#[tokio::test]
async fn concurrent_admissions_conflict() {
    let (_dir, handle) = open();
    bucket(&handle, member_id(), 42, 42).await;
    let (other, other_member) = (Ulid::from_bytes([6; 16]), Ulid::from_bytes([7; 16]));
    bound_bucket(&handle, ("other", other, other_member), 43, 43).await;
    let capacity = Some(NEED + NEED / 2);
    let mut first = AdmitOperation::new(sized(capacity));
    let staged = |effect: &Effect| {
        matches!(
            effect,
            Effect::Storage(StorageEffect::BatchWrite {
                txn_id: Some(_),
                ..
            })
        )
    };
    let mut queue = drive_until(&handle, &mut first, true, staged).await;
    assert!(!queue.is_empty(), "the first admission passed its check");

    let mut second = sized(capacity);
    (second.bucket, second.set_id, second.member_id) = ("other".into(), other, other_member);
    for range in &mut second.ranges {
        range.replicas = vec![other_member];
    }
    run_with(&handle, second, true).await.unwrap();

    resume(&handle, &mut first, true, &mut queue, |_| false).await;
    assert_eq!(
        first.finalize(),
        Err(AdmitError::Storage(StorageError::TransactionConflict))
    );
    assert_eq!(reserved(&handle, set_id().to_bytes().to_vec()).await, None);
    assert!(reserved(&handle, other.to_bytes().to_vec()).await.is_some());
}
