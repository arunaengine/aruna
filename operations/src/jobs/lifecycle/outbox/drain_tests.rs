//! Call and result tests of one job family outbox drain pass.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::keyspaces::{FAMILY_OUTBOX_KEYSPACE, FAMILY_RECORD_KEYSPACE, NODE_STATE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobFamilyRecord;
use aruna_core::structs::execution::job::record_keys::record_key;
use aruna_core::structs::execution::job::record_rows::{OutboxEntry, to_bytes};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::types::{Key, Value};

use super::DrainOutboxOperation;
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::outbox::OUTBOX_DRAIN_BATCH;
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

fn scan(after: Option<Key>, limit: usize) -> Step {
    Ok(Next::Call(IoCall::Page {
        space: FAMILY_OUTBOX_KEYSPACE.to_string(),
        prefix: Key::from(Vec::new()),
        after,
        limit,
    }))
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
