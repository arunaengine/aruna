//! Tests member admission against real storage: opened ranges, bucket checks, capacity and
//! signing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::RANGE_INDEX_KEYSPACE;
use aruna_core::operation::{EffectOperation, Runnable, SubOperation};
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
        fills: Vec::new(),
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

fn bound_info(set_id: Ulid, member: Ulid, bound: u128) -> BucketInfo {
    BucketInfo {
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
    }
}

async fn bound_bucket(
    handle: &StorageHandle,
    (name, set_id, member): (&str, Ulid, Ulid),
    bound: u128,
    created: u128,
) {
    let info = bound_info(set_id, member, bound);
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
    let mut operation = AdmitOperation::new(config).into_machine();
    drive_until(handle, &mut operation, sign, |_| false).await;
    operation.finalize()
}

/// Drives `operation` until `stop` matches the next effect or it ends.
async fn drive_until(
    handle: &StorageHandle,
    operation: &mut impl EffectOperation,
    sign: bool,
    stop: impl Fn(&Effect) -> bool,
) -> VecDeque<Effect> {
    let mut queue: VecDeque<Effect> = operation.start().into_iter().collect();
    resume(handle, operation, sign, &mut queue, stop).await;
    queue
}

/// Runs a read the admission makes outside its transaction.
async fn outside(handle: &StorageHandle, mut child: Box<dyn SubOperation>) -> Event {
    let mut queue: VecDeque<Effect> = child.start().into_iter().collect();
    while let Some(effect) = queue.pop_front() {
        let event = send(handle, effect).await;
        queue.extend(child.step(event));
    }
    child.finalize()
}

async fn resume(
    handle: &StorageHandle,
    operation: &mut impl EffectOperation,
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
            Effect::SubOperation(child) => outside(handle, child).await,
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

/// A member with new copies states their admission, not its own, once their ranges opened.
#[tokio::test]
async fn admits_new_copies() {
    let (_dir, handle) = open();
    bucket(&handle, member_id(), 42, 42).await;
    let mut copies = config();
    copies.fills = vec![RangeFill {
        fill_id: Ulid::from_bytes([30; 16]),
        range_id: ranges()[1].range_id,
        member_id: member_id(),
        admitted: false,
    }];
    let signed = run_with(&handle, copies, true).await.unwrap();
    let identity = (member_id(), 42, [7; 32]);
    let fill_ids = vec![Ulid::from_bytes([30; 16])];
    let expected = Statement::copy_admitted(realm(), set_id(), identity, fill_ids);
    assert_eq!(signed.statement, expected);
    assert!(opened(&handle, None).await);
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

/// An `all` coverage member that becomes a copy of a range it already opened reserves the
/// range's bytes on top of its metadata, and is refused while they do not fit.
#[tokio::test]
async fn copy_reserves_bytes() {
    let (_dir, handle) = open();
    bucket(&handle, member_id(), 42, 42).await;
    let mut covered = sized(Some(NEED - 1));
    covered.coverage = MetadataCoverage::All;
    covered.ranges = ranges();
    run_with(&handle, covered.clone(), true).await.unwrap();
    let first = account_key(set_id(), ranges()[0].range_id);
    let meta = OBJECT_METADATA_BYTES.to_be_bytes().to_vec();
    assert_eq!(reserved(&handle, first.clone()).await, Some(meta.clone()));
    let mut copy = covered;
    copy.ranges[0].replicas = vec![member_id()];
    copy.fills = vec![RangeFill {
        fill_id: Ulid::from_bytes([30; 16]),
        range_id: ranges()[0].range_id,
        member_id: member_id(),
        admitted: false,
    }];
    let refused = run_with(&handle, copy.clone(), true).await;
    let free = NEED - 1 - OBJECT_METADATA_BYTES;
    assert_eq!(
        refused,
        Err(AdmitError::InsufficientCapacity { need: 1000, free })
    );
    assert_eq!(reserved(&handle, first.clone()).await, Some(meta));
    copy.capacity = Some(NEED);
    run_with(&handle, copy, true).await.unwrap();
    assert_eq!(
        reserved(&handle, first).await,
        Some(NEED.to_be_bytes().to_vec())
    );
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
    let mut first = AdmitOperation::new(sized(capacity)).into_machine();
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

/// The answer to the first read of `config`: a bucket bound to `member`, the bucket's creation
/// row, no range accounts and no running usage rebuild.
fn first_read(config: &AdmitConfig, member: Ulid) -> IoResult {
    let creation = 42u128.to_be_bytes().to_vec();
    let info = bound_info(set_id(), member, 42);
    let mut values: Vec<Option<Value>> = vec![
        Some(info.to_bytes().unwrap().into()),
        Some(creation.into()),
    ];
    values.extend(config.ranges.iter().map(|_| None));
    values.push(None);
    IoResult::Values(values)
}

// A bucket bound to another member is refused before anything is written.
#[test]
fn foreign_binding_stops() {
    let mut operation = AdmitOperation::new(config());
    assert_eq!(operation.transaction(), TransactionMode::Write);
    assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
    let read = first_read(&config(), Ulid::from_bytes([99; 16]));
    assert_eq!(
        operation.next(read),
        Err(AdmitError::BindingChanged(BUCKET.to_string()))
    );
}

// With no capacity bound the ranges are opened, then the claim is signed.
#[test]
fn writes_then_signs() {
    let mut unbounded = sized(None);
    unbounded.coverage = MetadataCoverage::Assigned;
    let mut operation = AdmitOperation::new(unbounded.clone());
    operation.first().unwrap();
    let written = operation.next(first_read(&unbounded, member_id()));
    assert!(matches!(written, Ok(Next::Call(IoCall::Write(_)))));
    let Ok(Next::Call(IoCall::Effect(sign))) = operation.next(IoResult::Done) else {
        panic!("expected the signing request");
    };
    let Effect::Net(NetEffect::StatementSign(claim)) = *sign else {
        panic!("expected the claim");
    };
    let signed = claim.statement.sign(&secret());
    let answer = IoResult::Event(Box::new(Event::Net(NetEvent::StatementSign(
        StatementSignEvent::Signed(Box::new(signed.clone())),
    ))));
    assert_eq!(operation.next(answer), Ok(Next::Done(signed)));
}

// The usage rows are read outside the transaction; a full node refuses the member.
#[test]
fn usage_read_outside() {
    let bounded = sized(Some(0));
    let mut operation = AdmitOperation::new(bounded.clone());
    operation.first().unwrap();
    let page = operation.next(first_read(&bounded, member_id()));
    assert!(matches!(page, Ok(Next::Call(IoCall::Page { .. }))));
    let usage = operation.next(IoResult::Page {
        rows: Vec::new(),
        next: None,
    });
    assert!(matches!(usage, Ok(Next::Call(IoCall::Child(_)))));
    let values: Result<IoResult, IoError> =
        Ok(IoResult::Values(vec![None; global_shard_keys().len()]));
    let answer = IoResult::Child(ChildOutcome::new(values));
    assert!(matches!(
        operation.next(answer),
        Err(AdmitError::InsufficientCapacity { .. })
    ));
}
