//! Call and result tests of one job family outbox drain pass.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{
    Event, GroupEvent, GroupResult, JobRecordRejection, MetadataAnswer, NetEvent,
};
use aruna_core::keyspaces::{FAMILY_OUTBOX_KEYSPACE, FAMILY_RECORD_KEYSPACE, NODE_STATE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::record_keys::record_key;
use aruna_core::structs::execution::job::record_rows::{OutboxEntry, to_bytes};
use aruna_core::structs::execution::job::{JobFamilyRecord, JobRecordEnvelope, JobRecordKind};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::types::{Key, Value};

use super::{DrainOutboxOperation, bound_spec, scan};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::outbox::OUTBOX_DRAIN_BATCH;
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};
use crate::metadata::protocol::{MetadataTransportMessage, decode_message, encode_message};
use crate::tests::records::{Family, REALM, actor, node};

type Step = Result<Next<bool>, LifecycleError>;

fn cursor_key() -> Key {
    Key::from(b"job_family_outbox_cursor".to_vec())
}

fn config_read() -> Step {
    let config = DocumentTarget::RealmConfig { realm_id: REALM };
    Ok(Next::Call(IoCall::Read(vec![(
        config.storage_keyspace().to_string(),
        config.storage_key(),
    )])))
}

fn config_row(family: &Family) -> IoResult {
    let bytes = family
        .config
        .to_bytes(&actor(node(1)))
        .expect("config encodes");
    IoResult::Values(vec![Some(Value::from(bytes.as_slice()))])
}

fn cursor_read() -> Step {
    Ok(Next::Call(IoCall::Read(vec![(
        NODE_STATE_KEYSPACE.to_string(),
        cursor_key(),
    )])))
}

fn page(keys: Vec<Key>, next: Option<Key>) -> IoResult {
    IoResult::Page {
        rows: keys
            .into_iter()
            .map(|key| (key, Value::from(b"queued".to_vec())))
            .collect(),
        next,
    }
}

fn record_read(key: &Key) -> Step {
    Ok(Next::Call(IoCall::Read(vec![(
        FAMILY_RECORD_KEYSPACE.to_string(),
        key.clone(),
    )])))
}

fn cursor_write(cursor: Option<Key>) -> Step {
    let row = (NODE_STATE_KEYSPACE.to_string(), cursor_key());
    Ok(Next::Call(match cursor {
        Some(value) => IoCall::Write(vec![(row.0, row.1, value)]),
        None => IoCall::Delete(vec![row]),
    }))
}

/// One signed spec record of `family`, its outbox key and its stored row.
fn queued(family: &Family) -> (Key, IoResult) {
    let envelope = family.sign(
        &family.holder,
        JobFamilyRecord::Spec(Box::new(family.spec())),
    );
    let key = record_key(&envelope.key());
    let bytes = to_bytes(&envelope).expect("record encodes");
    (
        key,
        IoResult::Values(vec![Some(Value::from(bytes.as_slice()))]),
    )
}

/// Runs a drain up to its first scan, which starts after `cursor`.
fn scanning(family: &Family, cursor: Option<Key>) -> DrainOutboxOperation {
    let mut drain = DrainOutboxOperation::new(REALM, node(1));
    assert_eq!(drain.first(), config_read());
    assert_eq!(drain.next(config_row(family)), cursor_read());
    let read = IoResult::Values(vec![cursor.clone()]);
    assert_eq!(drain.next(read), scan(cursor, OUTBOX_DRAIN_BATCH));
    drain
}

#[test]
fn pages_past_cursor() {
    // A persisted continuation lets the next pass reach later rows.
    let family = Family::new([1u8; 32]);
    let blocked = Key::from(b"blocked".to_vec());
    let mut first = scanning(&family, None);
    assert_eq!(
        first.next(page(Vec::new(), Some(blocked.clone()))),
        cursor_write(Some(blocked.clone()))
    );
    assert_eq!(first.next(IoResult::Done), scan(None, 1));
    let later = page(vec![Key::from(b"later".to_vec())], None);
    assert_eq!(first.next(later), Ok(Next::Done(true)));

    let mut second = scanning(&family, Some(blocked));
    assert_eq!(second.next(page(Vec::new(), None)), cursor_write(None));
    assert_eq!(second.next(IoResult::Done), scan(None, 1));
    assert_eq!(second.next(page(Vec::new(), None)), Ok(Next::Done(false)));
}

#[test]
fn offers_queued_record() {
    // A record is offered to the other holders through the publish's calls.
    let family = Family::new([2u8; 32]);
    let (key, row) = queued(&family);
    let mut drain = scanning(&family, None);
    assert_eq!(drain.next(page(vec![key.clone()], None)), record_read(&key));
    let outbox_read = drain.next(row);
    let Ok(Next::Call(IoCall::Read(reads))) = outbox_read else {
        panic!("expected the outbox entry read, got {outbox_read:?}")
    };
    assert_eq!(reads[0].0, FAMILY_OUTBOX_KEYSPACE);
    let entry = OutboxEntry {
        queued_at_ms: 1,
        delivered: Vec::new(),
        next_holder: 0,
        rejections: 0,
    };
    let stored = to_bytes(&entry).expect("entry encodes");
    let offer = drain.next(IoResult::Values(vec![Some(Value::from(stored.as_slice()))]));
    assert!(matches!(offer, Ok(Next::Call(IoCall::Group(_)))));
}

#[test]
fn skips_unreadable_records() {
    // An undecodable key or a missing record is skipped.
    let family = Family::new([3u8; 32]);
    let (key, _) = queued(&family);
    let mut drain = scanning(&family, None);
    let keys = vec![Key::from(b"bad".to_vec()), key.clone()];
    assert_eq!(drain.next(page(keys, None)), record_read(&key));
    assert_eq!(drain.next(IoResult::Values(vec![None])), cursor_write(None));
}

#[test]
fn clears_without_holders() {
    // With no other holder the entry is cleared and the pass goes on.
    let mut family = Family::new([4u8; 32]);
    let mut config = RealmConfigDocument::new(REALM, Vec::new(), 5);
    config.ensure_node(node(1), RealmNodeKind::Server);
    config.seed_default_placement();
    config.snapshot_candidate_map();
    family.placement = config
        .family_placement(family.submission_id)
        .expect("family placement resolves");
    family.config = config;
    let (key, row) = queued(&family);
    let mut drain = scanning(&family, None);
    drain
        .next(page(vec![key.clone()], None))
        .expect("record read");
    assert_eq!(
        drain.next(row),
        Ok(Next::Call(IoCall::Delete(vec![(
            FAMILY_OUTBOX_KEYSPACE.to_string(),
            key,
        )])))
    );
    assert_eq!(drain.next(IoResult::Done), cursor_write(None));
}

#[test]
fn retries_without_config() {
    let mut drain = DrainOutboxOperation::new(REALM, node(1));
    drain.first().expect("config read");
    assert_eq!(
        drain.next(IoResult::Values(vec![None])),
        Ok(Next::Done(true))
    );
}

#[test]
fn rejects_unfit_result() {
    let family = Family::new([5u8; 32]);
    let mut drain = scanning(&family, None);
    assert!(matches!(
        drain.next(IoResult::Woke),
        Err(LifecycleError::UnexpectedEvent { .. })
    ));
}

fn stored_entry() -> IoResult {
    let entry = OutboxEntry {
        queued_at_ms: 1,
        delivered: Vec::new(),
        next_holder: 0,
        rejections: 0,
    };
    let stored = to_bytes(&entry).expect("entry encodes");
    IoResult::Values(vec![Some(Value::from(stored.as_slice()))])
}

/// The holders' answer that they hold no spec of the family.
fn spec_needed(holders: usize) -> IoResult {
    let reply = MetadataTransportMessage::ForwardedJobRecord {
        result: Err(JobRecordRejection::SpecNeeded),
    };
    let answer = || MetadataAnswer {
        peer: node(2),
        elapsed: Duration::ZERO,
        result: Ok(encode_message(&reply).expect("reply encodes")),
    };
    let results = (0..holders)
        .map(|_| {
            GroupResult::Done(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
                answer(),
            )))))
        })
        .collect();
    IoResult::Group(GroupEvent::Finished { results })
}

// A holder without the spec makes the pass read the family's specs and offer the record again
// with the one it binds.
#[test]
fn offers_bound_spec() {
    let family = Family::new([6u8; 32]);
    let spec = family.sign(
        &family.holder,
        JobFamilyRecord::Spec(Box::new(family.spec())),
    );
    let claim = family.sign(
        &family.holder,
        JobFamilyRecord::Claim(family.claim(&family.spec())),
    );
    let key = record_key(&claim.key());
    let row = Value::from(to_bytes(&claim).expect("record encodes").as_slice());
    let mut drain = scanning(&family, None);
    drain.next(page(vec![key], None)).expect("record read");
    drain
        .next(IoResult::Values(vec![Some(row)]))
        .expect("outbox read");
    let Ok(Next::Call(IoCall::Group(group))) = drain.next(stored_entry()) else {
        panic!("the claim is offered");
    };
    let holders = group.members.len();
    let kept = drain.next(spec_needed(holders));
    assert!(matches!(kept, Ok(Next::Call(IoCall::Write(_)))));
    let specs = LoadFamilyOperation::kind(family.family(), JobRecordKind::Spec);
    assert_eq!(
        drain.next(IoResult::Done),
        Ok(Next::Call(IoCall::child(specs)))
    );
    let loaded = Ok::<_, FamilyReadError>(vec![spec.clone()]);
    let reread = drain.next(IoResult::Child(ChildOutcome::new(loaded)));
    assert!(matches!(reread, Ok(Next::Call(IoCall::Read(_)))));
    let Ok(Next::Call(IoCall::Group(group))) = drain.next(stored_entry()) else {
        panic!("the claim is offered again");
    };
    let Effect::Net(NetEffect::MetadataRequest(request)) = &group.members[0] else {
        panic!("a record publish");
    };
    let message = decode_message(&request.frame).expect("publish decodes");
    let MetadataTransportMessage::ForwardJobRecord { attached, .. } = message else {
        panic!("a record publish");
    };
    let attached: Vec<JobRecordEnvelope> = attached
        .into_iter()
        .map(|frame| frame.into_inner())
        .collect();
    assert_eq!(attached, vec![spec]);
}

// A record names its spec by digest; an update names none and takes the family's spec.
#[test]
fn picks_bound_spec() {
    let family = Family::new([7u8; 32]);
    let spec = family.spec();
    let signed = family.sign(
        &family.holder,
        JobFamilyRecord::Spec(Box::new(spec.clone())),
    );
    let mut other = spec.clone();
    other.spec_digest = [9; 32];
    let other = family.sign(&family.holder, JobFamilyRecord::Spec(Box::new(other)));
    let specs = vec![other.clone(), signed.clone()];
    let claim = family.sign(&family.holder, JobFamilyRecord::Claim(family.claim(&spec)));
    let bound = bound_spec(&claim, specs.clone()).map(|frame| frame.into_inner());
    assert_eq!(bound, Some(signed.clone()));
    let update = family.run(
        1,
        0,
        aruna_core::structs::execution::job::PhysicalExecutionState::Failed,
    )[6]
    .clone();
    assert!(matches!(update.record, JobFamilyRecord::Update(_)));
    let bound = bound_spec(&update, specs).map(|frame| frame.into_inner());
    assert_eq!(bound, Some(other));
    assert_eq!(bound_spec(&signed, vec![signed.clone()]), None);
}

fn row_of(envelope: &JobRecordEnvelope) -> IoResult {
    let bytes = to_bytes(envelope).expect("record encodes");
    IoResult::Values(vec![Some(Value::from(bytes.as_slice()))])
}

// A queued receipt goes with the launch it binds and that scheduler's budget.
#[test]
fn bundles_receipt_records() {
    let family = Family::new([8u8; 32]);
    let run = family.run(
        1,
        0,
        aruna_core::structs::execution::job::PhysicalExecutionState::Failed,
    );
    let (budget, launch, receipt) = (&run[2], &run[3], &run[4]);
    let key = record_key(&receipt.key());
    let mut drain = scanning(&family, None);
    drain.next(page(vec![key], None)).expect("record read");
    assert_eq!(
        drain.next(row_of(receipt)),
        record_read(&record_key(&launch.key()))
    );
    assert_eq!(
        drain.next(row_of(launch)),
        record_read(&record_key(&budget.key()))
    );
    let outbox = drain.next(row_of(budget));
    assert!(matches!(outbox, Ok(Next::Call(IoCall::Read(_)))));
    let Ok(Next::Call(IoCall::Group(group))) = drain.next(stored_entry()) else {
        panic!("the receipt is offered");
    };
    let Effect::Net(NetEffect::MetadataRequest(request)) = &group.members[0] else {
        panic!("a record publish");
    };
    let message = decode_message(&request.frame).expect("publish decodes");
    let MetadataTransportMessage::ForwardJobRecord { attached, .. } = message else {
        panic!("a record publish");
    };
    let attached: Vec<JobRecordEnvelope> = attached
        .into_iter()
        .map(|frame| frame.into_inner())
        .collect();
    assert_eq!(attached, vec![launch.clone(), budget.clone()]);
}
