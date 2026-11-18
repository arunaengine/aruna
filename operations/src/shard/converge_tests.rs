//! Tests shard verification and convergence as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::calls::{
    bucket_realm, called, child, node, runs_child, shard_effect, shard_event,
};
use aruna_core::types::Value;
use ulid::Ulid;

fn realm() -> RealmId {
    RealmId::from_bytes([8; 32])
}

fn strategy_id() -> Ulid {
    Ulid::from_bytes([9; 16])
}

fn placement() -> PlacementRef {
    PlacementRef {
        strategy_id: strategy_id(),
        shard: 0,
    }
}

fn topic() -> irokle::TopicId {
    shard_topic_id(realm(), &placement())
}

fn manifest(digest: u8) -> ShardManifest {
    ShardManifest {
        placement: placement(),
        holder: node(2),
        entries: Vec::new(),
        cursor: postcard::to_allocvec(&irokle::ActorClock::default()).unwrap(),
        digest: [digest; 32],
        updated_at_ms: 0,
    }
}

fn fetched(digest: u8) -> IoResult {
    child(Ok::<_, ManifestError>(manifest(digest)))
}

fn unavailable() -> IoResult {
    child(Err::<ShardManifest, _>(ManifestError("down".to_string())))
}

fn topic_known() -> IoResult {
    shard_event(ShardNetEvent::Existing(BTreeSet::from([topic()])))
}

fn topic_missing() -> IoResult {
    shard_event(ShardNetEvent::Existing(BTreeSet::new()))
}

fn pulled() -> IoResult {
    shard_event(ShardNetEvent::Pulled { applied: true })
}

fn pull_from(peer: NodeId) -> ShardNetEffect {
    ShardNetEffect::Pull {
        topics: vec![topic()],
        peers: vec![peer],
    }
}

fn converging(required: Option<irokle::ActorClock>) -> ConvergeOperation {
    let holders = [node(2), node(3)];
    let mut operation = ConvergeOperation::new(realm(), placement(), &holders, required);
    assert!(runs_child(operation.first()));
    operation
}

/// Runs up to the check that a local genesis exists, with both digests as given.
fn compared(remote: u8, local: u8, required: Option<irokle::ActorClock>) -> ConvergeOperation {
    let mut operation = converging(required);
    assert!(runs_child(operation.next(fetched(remote))));
    assert_eq!(
        shard_effect(operation.next(fetched(local))),
        ShardNetEffect::Existing(vec![topic()])
    );
    operation
}

#[test]
fn no_holders_unverified() {
    let mut operation = ConvergeOperation::new(realm(), placement(), &[], None);
    assert_eq!(operation.first(), Ok(Next::Done(None)));
}

#[test]
fn equal_digests_converge() {
    assert_eq!(
        compared(1, 1, None).next(topic_known()),
        Ok(Next::Done(Some((node(2), [1; 32]))))
    );
}

#[test]
fn missing_genesis_pulls() {
    assert_eq!(
        shard_effect(compared(1, 1, None).next(topic_missing())),
        pull_from(node(2))
    );
}

#[test]
fn different_digests_pull() {
    assert_eq!(
        shard_effect(compared(1, 2, None).next(topic_known())),
        pull_from(node(2))
    );
}

#[test]
fn required_frontier_checked() {
    // A local cursor that cannot be read never satisfies a required frontier.
    let mut operation = converging(Some(irokle::ActorClock::default()));
    operation.next(fetched(1)).unwrap();
    let mut local = manifest(1);
    local.cursor = vec![0xff; 40];
    operation
        .next(child(Ok::<_, ManifestError>(local)))
        .unwrap();
    assert_eq!(
        shard_effect(operation.next(topic_known())),
        pull_from(node(2))
    );
}

#[test]
fn empty_frontier_passes() {
    let required = Some(irokle::ActorClock::default());
    assert_eq!(
        compared(1, 1, required).next(topic_known()),
        Ok(Next::Done(Some((node(2), [1; 32]))))
    );
}

#[test]
fn failed_fetch_continues() {
    let mut operation = converging(None);
    assert!(runs_child(operation.next(unavailable())));
    assert_eq!(operation.holder, Some(node(3)));
}

#[test]
fn all_fetches_failed() {
    let mut operation = converging(None);
    operation.next(unavailable()).unwrap();
    assert_eq!(operation.next(unavailable()), Ok(Next::Done(None)));
}

#[test]
fn local_failure_stops() {
    let mut operation = converging(None);
    operation.next(fetched(1)).unwrap();
    assert_eq!(operation.next(unavailable()), Ok(Next::Done(None)));
}

/// Runs one failed comparison up to the fetch that follows the pull.
fn after_pull(operation: &mut ConvergeOperation) {
    assert!(matches!(
        shard_effect(operation.next(topic_known())),
        ShardNetEffect::Pull { .. }
    ));
    assert!(runs_child(operation.next(pulled())));
}

#[test]
fn failed_refetch_continues() {
    let mut operation = compared(1, 2, None);
    after_pull(&mut operation);
    assert!(runs_child(operation.next(unavailable())));
    assert_eq!(operation.holder, Some(node(3)));
}

#[test]
fn attempts_are_bounded() {
    let mut operation = compared(1, 2, None);
    for _ in 1..VERIFICATION_MAX_ATTEMPTS {
        after_pull(&mut operation);
        assert!(runs_child(operation.next(fetched(1))));
        assert_eq!(operation.holder, Some(node(2)));
        operation.next(fetched(2)).unwrap();
    }
    after_pull(&mut operation);
    assert!(runs_child(operation.next(fetched(1))));
    assert_eq!(operation.holder, Some(node(3)));
    assert_eq!(operation.attempts, 0);
}

fn verifying(co_holders: Vec<NodeId>) -> VerifyShardOperation {
    let mut operation = VerifyShardOperation::new(realm(), placement(), co_holders);
    operation.stamp(5);
    assert!(matches!(called(operation.first()), IoCall::Read(_)));
    operation
}

fn no_marker() -> IoResult {
    IoResult::Values(vec![None])
}

fn marked() -> IoResult {
    IoResult::Values(vec![Some(Value::from(vec![1]))])
}

fn marker_written() -> IoResult {
    child(Ok::<(), IoError>(()))
}

#[test]
fn marked_shard_skipped() {
    assert_eq!(
        verifying(vec![node(2)]).next(marked()),
        Ok(Next::Done(ShardVerdict::Known))
    );
}

#[test]
fn sole_holder_waits() {
    let mut operation = verifying(Vec::new());
    assert_eq!(
        shard_effect(operation.next(no_marker())),
        ShardNetEffect::Existing(vec![topic()])
    );
    assert_eq!(
        operation.next(topic_missing()),
        Ok(Next::Done(ShardVerdict::Open))
    );
}

fn sole_with_genesis() -> VerifyShardOperation {
    let mut operation = verifying(Vec::new());
    operation.next(no_marker()).unwrap();
    assert!(runs_child(operation.next(topic_known())));
    operation
}

#[test]
fn sole_holder_marked() {
    let mut operation = sole_with_genesis();
    assert!(runs_child(operation.next(fetched(1))));
    assert_eq!(operation.against, None);
    assert_eq!(
        operation.next(marker_written()),
        Ok(Next::Done(ShardVerdict::Verified))
    );
}

#[test]
fn sole_failure_open() {
    assert_eq!(
        sole_with_genesis().next(unavailable()),
        Ok(Next::Done(ShardVerdict::Open))
    );
}

fn with_holder() -> VerifyShardOperation {
    let mut operation = verifying(vec![node(2)]);
    assert!(runs_child(operation.next(no_marker())));
    operation
}

#[test]
fn converged_holder_marked() {
    let mut operation = with_holder();
    let converged = Some((node(2), [1u8; 32]));
    assert!(runs_child(
        operation.next(child(Ok::<_, IoError>(converged)))
    ));
    assert_eq!(operation.against, Some(node(2)));
}

#[test]
fn failed_marker_verified() {
    let mut operation = with_holder();
    let converged = Some((node(2), [1u8; 32]));
    operation.next(child(Ok::<_, IoError>(converged))).unwrap();
    let failed = child(Err::<(), _>(IoError::Conflict));
    assert_eq!(
        operation.next(failed),
        Ok(Next::Done(ShardVerdict::Verified))
    );
}

#[test]
fn unconverged_stays_open() {
    let none = child(Ok::<_, IoError>(None::<(NodeId, [u8; 32])>));
    assert_eq!(with_holder().next(none), Ok(Next::Done(ShardVerdict::Open)));
}

#[test]
fn marker_row_written() {
    let record = ShardVerificationRecord {
        placement: placement(),
        verified_against: Some(node(2)),
        digest: [1; 32],
        verified_at_ms: 5,
    };
    let key = verification_key(realm(), &placement());
    let mut operation = MarkVerifiedOperation {
        key: key.clone(),
        record: record.clone(),
    };
    let IoCall::Write(rows) = called(operation.first()) else {
        panic!("expected the marker write");
    };
    assert_eq!(rows[0].0, SHARD_VERIFICATION_KEYSPACE);
    assert_eq!(rows[0].1, key);
    assert_eq!(
        postcard::from_bytes::<ShardVerificationRecord>(&rows[0].2).unwrap(),
        record
    );
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
}

#[test]
fn held_shards_ranked() {
    let config = config(&[node(1), node(2)]);
    let shards = held_shards(&config, node(1));
    assert_eq!(shards, vec![(placement(), vec![node(2)])]);
    assert!(held_shards(&config, node(9)).is_empty());
}

fn verify_held(
    nodes: &[NodeId],
) -> (
    VerifyHeldOperation,
    Result<Next<ShardVerificationSummary>, IoError>,
) {
    let mut operation = VerifyHeldOperation::new(node(1), realm());
    assert!(runs_child(operation.first()));
    let read = child(Ok::<_, GetConfigError>(config(nodes)));
    let turn = operation.next(read);
    (operation, turn)
}

fn verdict(verdict: ShardVerdict) -> GroupResult {
    let outcome = ChildOutcome::new(Ok::<_, IoError>(verdict));
    GroupResult::Done(Box::new(Event::Child(outcome)))
}

#[test]
fn unreadable_config_skips() {
    let mut operation = VerifyHeldOperation::new(node(1), realm());
    operation.first().unwrap();
    let missing = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
    assert_eq!(
        operation.next(child(missing)),
        Ok(Next::Done(ShardVerificationSummary::default()))
    );
}

#[test]
fn nothing_held_finishes() {
    let (_, turn) = verify_held(&[node(2)]);
    assert_eq!(turn, Ok(Next::Done(ShardVerificationSummary::default())));
}

#[test]
fn shards_verify_together() {
    let (_, turn) = verify_held(&[node(1)]);
    let IoCall::Group(group) = called(turn) else {
        panic!("expected the verification group");
    };
    assert_eq!(group.members.len(), 1);
    assert_eq!(group.limit, VERIFICATION_CONCURRENCY_LIMIT);
}

#[test]
fn verdicts_are_counted() {
    let (mut operation, _) = verify_held(&[node(1)]);
    let results = vec![
        verdict(ShardVerdict::Known),
        verdict(ShardVerdict::Verified),
        verdict(ShardVerdict::Open),
        GroupResult::Unfinished,
    ];
    let finished = IoResult::Group(GroupEvent::Finished { results });
    assert_eq!(
        operation.next(finished),
        Ok(Next::Done(ShardVerificationSummary {
            held: 4,
            already_verified: 1,
            newly_verified: 1,
            unverified: 2,
        }))
    );
}

fn marker_row(placement: PlacementRef) -> (Key, Value) {
    let record = ShardVerificationRecord {
        placement,
        verified_against: None,
        digest: [1; 32],
        verified_at_ms: 1,
    };
    let value = postcard::to_allocvec(&record).unwrap().into();
    (verification_key(realm(), &placement), value)
}

#[test]
fn verified_topics_paged() {
    let second = PlacementRef {
        strategy_id: strategy_id(),
        shard: 1,
    };
    let mut operation = VerifiedTopicsOperation::new(realm());
    assert!(matches!(called(operation.first()), IoCall::Page { .. }));
    let garbage = (Key::from(vec![1]), Value::from(vec![0xff]));
    let first_page = IoResult::Page {
        rows: vec![marker_row(placement()), garbage],
        next: Some(Key::from(vec![2])),
    };
    assert!(matches!(
        called(operation.next(first_page)),
        IoCall::Page { .. }
    ));
    let last_page = IoResult::Page {
        rows: vec![marker_row(second)],
        next: None,
    };
    assert_eq!(
        operation.next(last_page),
        Ok(Next::Done(BTreeSet::from([
            topic(),
            shard_topic_id(realm(), &second),
        ])))
    );
}

#[test]
fn marker_presence_answers() {
    let mut operation = ShardVerifiedOperation::new(realm(), &placement());
    assert!(matches!(called(operation.first()), IoCall::Read(_)));
    assert_eq!(operation.next(marked()), Ok(Next::Done(true)));
    assert_eq!(operation.next(no_marker()), Ok(Next::Done(false)));
}

#[test]
fn forgetting_names_marker() {
    let IoCall::Delete(rows) = forget_verification(realm(), &placement()) else {
        panic!("expected a delete");
    };
    assert_eq!(
        rows,
        vec![(
            SHARD_VERIFICATION_KEYSPACE.to_string(),
            verification_key(realm(), &placement()),
        )]
    );
}

fn config(nodes: &[NodeId]) -> RealmConfigDocument {
    bucket_realm(realm(), strategy_id(), nodes)
}
