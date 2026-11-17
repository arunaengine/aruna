//! State and event tests of one job family outbox drain pass.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, JobRecordEffect, NetEffect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, JobRecordEvent, NetEvent, StorageEvent};
use aruna_core::keyspaces::{FAMILY_OUTBOX_KEYSPACE, FAMILY_RECORD_KEYSPACE, NODE_STATE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::operation_check::{
    check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
};
use aruna_core::structs::execution::job::JobFamilyRecord;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::types::{Effects, Key, Value};

use super::DrainOutboxOperation;
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::outbox::OUTBOX_DRAIN_BATCH;
use crate::realm::get_config::GetConfigOperation;
use crate::tests::records::{Family, REALM, actor, node};
use aruna_core::structs::execution::job::record_keys::record_key;
use aruna_core::structs::execution::job::record_rows::{OutboxEntry, to_bytes};

fn config_read() -> Effects {
    GetConfigOperation::new(REALM).start()
}

fn config_row(family: &Family) -> Event {
    let bytes = family
        .config
        .to_bytes(&actor(node(1)))
        .expect("config encodes");
    Event::Storage(StorageEvent::ReadResult {
        key: Key::from(b"config".to_vec()),
        value: Some(Value::from(bytes.as_slice())),
    })
}

fn cursor_key() -> Key {
    Key::from(b"job_family_outbox_cursor".to_vec())
}

fn cursor_read() -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: NODE_STATE_KEYSPACE.to_string(),
        key: cursor_key(),
        txn_id: None,
    })
}

fn read(value: Option<Key>) -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: cursor_key(),
        value,
    })
}

fn scan(start_after: Option<Key>, limit: usize) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: FAMILY_OUTBOX_KEYSPACE.to_string(),
        prefix: None,
        start: start_after.map(IterStart::After),
        limit,
        txn_id: None,
    })
}

fn page(keys: Vec<Key>, next: Option<Key>) -> Event {
    Event::Storage(StorageEvent::IterResult {
        values: keys
            .into_iter()
            .map(|key| (key, Value::from(b"queued".to_vec())))
            .collect(),
        next_start_after: next,
    })
}

fn record_read(key: &Key) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: FAMILY_RECORD_KEYSPACE.to_string(),
        key: key.clone(),
        txn_id: None,
    })
}

fn cursor_write(cursor: Option<Key>) -> Effect {
    Effect::Storage(match cursor {
        Some(value) => StorageEffect::Write {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: cursor_key(),
            value,
            txn_id: None,
        },
        None => StorageEffect::Delete {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: cursor_key(),
            txn_id: None,
        },
    })
}

fn written() -> Event {
    Event::Storage(StorageEvent::WriteResult { key: cursor_key() })
}

fn deleted(key: Key) -> Event {
    Event::Storage(StorageEvent::DeleteResult { key })
}

fn failed() -> Event {
    Event::Storage(StorageEvent::Error {
        error: StorageError::Timeout,
    })
}

fn drain(local: u8) -> DrainOutboxOperation {
    DrainOutboxOperation::new(REALM, node(local))
}

/// One signed spec record of `family`, its outbox key and its stored row.
fn queued(family: &Family) -> (Key, Event) {
    let envelope = family.sign(
        &family.holder,
        JobFamilyRecord::Spec(Box::new(family.spec())),
    );
    let key = record_key(&envelope.key());
    let bytes = to_bytes(&envelope).expect("record encodes");
    let row = Event::Storage(StorageEvent::ReadResult {
        key: key.clone(),
        value: Some(Value::from(bytes.as_slice())),
    });
    (key, row)
}

/// Runs a drain up to its first scan, which starts after `cursor`.
fn scanning(family: &Family, local: u8, cursor: Option<Key>) -> DrainOutboxOperation {
    let mut drain = drain(local);
    check_trace(
        &mut drain,
        &config_read(),
        vec![
            (config_row(family), vec![cursor_read()]),
            (read(cursor.clone()), vec![scan(cursor, OUTBOX_DRAIN_BATCH)]),
        ],
    );
    drain
}

#[test]
fn pages_past_cursor() {
    // A persisted continuation lets the next pass reach later rows.
    let family = Family::new([1u8; 32]);
    let blocked = Key::from(b"blocked".to_vec());
    let mut first = scanning(&family, 1, None);
    let effects = first.step(page(Vec::new(), Some(blocked.clone())));
    assert_eq!(effects.as_slice(), &[cursor_write(Some(blocked.clone()))]);
    assert_eq!(first.step(written()).as_slice(), &[scan(None, 1)]);
    assert!(
        first
            .step(page(vec![Key::from(b"later".to_vec())], None))
            .is_empty()
    );
    assert_eq!(first.finalize(), Ok(true));

    let mut second = scanning(&family, 1, Some(blocked));
    assert_eq!(
        second.step(page(Vec::new(), None)).as_slice(),
        &[cursor_write(None)]
    );
    assert_eq!(
        second.step(deleted(cursor_key())).as_slice(),
        &[scan(None, 1)]
    );
    assert!(second.step(page(Vec::new(), None)).is_empty());
    assert_eq!(second.finalize(), Ok(false));
}

#[test]
fn offers_queued_record() {
    // A record is offered to the other holders, then the pass ends once the
    // publish settled and nothing remains.
    let family = Family::new([2u8; 32]);
    let (key, row) = queued(&family);
    let mut drain = scanning(&family, 1, None);
    assert_eq!(
        drain.step(page(vec![key.clone()], None)).as_slice(),
        &[record_read(&key)]
    );
    let effects = drain.step(row);
    let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice() else {
        panic!("expected the outbox entry read, got {effects:?}")
    };
    assert_eq!(key_space, FAMILY_OUTBOX_KEYSPACE);
    let entry = to_bytes(&OutboxEntry {
        queued_at_ms: 1,
        delivered: Vec::new(),
        next_holder: 0,
        rejections: 0,
    })
    .expect("entry encodes");
    let effects = drain.step(Event::Storage(StorageEvent::ReadResult {
        key: key.clone(),
        value: Some(Value::from(entry.as_slice())),
    }));
    let [Effect::Net(NetEffect::JobRecord(publish))] = effects.as_slice() else {
        panic!("expected a record publish, got {effects:?}")
    };
    let JobRecordEffect::Publish { holders, .. } = publish.as_ref() else {
        panic!("expected a publish effect")
    };
    assert!(!holders.as_slice().contains(&node(1)));
    let effects = drain.step(Event::Net(NetEvent::JobRecord(
        JobRecordEvent::Unavailable("unreachable".to_string()),
    )));
    assert!(matches!(
        effects.as_slice(),
        [Effect::Storage(StorageEffect::Write { .. })]
    ));
    assert_eq!(drain.step(written()).as_slice(), &[cursor_write(None)]);
    assert_eq!(
        drain.step(deleted(cursor_key())).as_slice(),
        &[scan(None, 1)]
    );
    assert!(drain.step(page(vec![key], None)).is_empty());
    assert_eq!(drain.finalize(), Ok(true));
}

#[test]
fn skips_unreadable_records() {
    // An undecodable key, a missing record and a failed read are each skipped.
    let family = Family::new([3u8; 32]);
    let (key, _) = queued(&family);
    let mut drain = scanning(&family, 1, None);
    let effects = drain.step(page(
        vec![Key::from(b"bad".to_vec()), key.clone(), key.clone()],
        None,
    ));
    assert_eq!(effects.as_slice(), &[record_read(&key)]);
    let missing = Event::Storage(StorageEvent::ReadResult {
        key: key.clone(),
        value: None,
    });
    assert_eq!(drain.step(missing).as_slice(), &[record_read(&key)]);
    assert_eq!(drain.step(failed()).as_slice(), &[cursor_write(None)]);
}

#[test]
fn clears_without_holders() {
    // With no other holder the entry is cleared; a failed clear ends the pass
    // before the cursor moves past it.
    let mut family = Family::new([4u8; 32]);
    let mut config = RealmConfigDocument::new(REALM, Vec::new(), 5);
    config.ensure_node(node(1), RealmNodeKind::Server);
    config.seed_default_placement();
    config.snapshot_candidate_map();
    family.placement = config
        .family_placement(family.submission_id)
        .expect("family placement resolves");
    assert_eq!(
        crate::placement::resolve_shard_holders(&config, &family.placement),
        vec![node(1)]
    );
    family.config = config;
    let (key, row) = queued(&family);
    let mut drain = scanning(&family, 1, None);
    drain.step(page(vec![key.clone()], None));
    let clear = || {
        Effect::Storage(StorageEffect::Delete {
            key_space: FAMILY_OUTBOX_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: None,
        })
    };
    assert_eq!(drain.step(row).as_slice(), &[clear()]);
    assert_eq!(
        drain.step(deleted(key.clone())).as_slice(),
        &[cursor_write(None)]
    );

    let mut drain = scanning(&family, 1, None);
    drain.step(page(vec![key.clone()], None));
    assert_eq!(drain.step(queued(&family).1).as_slice(), &[clear()]);
    assert!(drain.step(failed()).is_empty());
    assert_eq!(drain.finalize(), Ok(true));
}

#[test]
fn retries_after_failures() {
    // Every local failure ends the pass with entries left for the next one.
    let family = Family::new([5u8; 32]);
    let mut drain = drain(1);
    drain.start();
    assert!(drain.step(failed()).is_empty());
    assert_eq!(drain.finalize(), Ok(true));

    let mut drain = drain_after_config(&family);
    assert!(drain.step(failed()).is_empty());
    assert_eq!(drain.finalize(), Ok(true));

    let mut drain = scanning(&family, 1, None);
    assert!(drain.step(failed()).is_empty());
    assert_eq!(drain.finalize(), Ok(true));

    let mut drain = scanning(&family, 1, None);
    drain.step(page(Vec::new(), None));
    assert!(drain.step(failed()).is_empty());
    assert_eq!(drain.finalize(), Ok(true));

    let mut drain = scanning(&family, 1, None);
    drain.step(page(Vec::new(), None));
    drain.step(deleted(cursor_key()));
    assert!(drain.step(failed()).is_empty());
    assert_eq!(drain.finalize(), Ok(true));
}

fn drain_after_config(family: &Family) -> DrainOutboxOperation {
    let mut drain = drain(1);
    drain.start();
    assert_eq!(drain.step(config_row(family)).as_slice(), &[cursor_read()]);
    drain
}

#[test]
fn rejects_unfit_events() {
    let family = Family::new([6u8; 32]);
    let phases: Vec<(Box<dyn Fn() -> DrainOutboxOperation>, fn(&Event) -> bool)> = vec![
        (
            Box::new(|| drain_after_config(&Family::new([6u8; 32]))),
            |event| {
                matches!(
                    event,
                    Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
                )
            },
        ),
        (
            Box::new(|| scanning(&Family::new([6u8; 32]), 1, None)),
            |event| {
                matches!(
                    event,
                    Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
                )
            },
        ),
        (Box::new(|| drain(1)), |_| false),
    ];
    for (make, fits) in phases {
        let refusals = rejects_events(make, sample_events, fits);
        assert!(!refusals.is_empty());
        for (label, effects, error) in refusals {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert!(
                matches!(error, LifecycleError::UnexpectedEvent { .. }),
                "{label} gave {error:?}"
            );
        }
    }
    // A finished pass refuses any later event.
    let mut done = scanning(&family, 1, None);
    done.step(failed());
    done.step(written());
    assert!(matches!(
        done.finalize(),
        Err(LifecycleError::UnexpectedEvent { .. })
    ));
}

#[test]
fn aborts_without_cleanup() {
    let family = Family::new([7u8; 32]);
    check_abort(&mut drain(1), &[]);
    check_abort(&mut scanning(&family, 1, None), &[]);
}

#[test]
fn rejects_early_finalize() {
    let family = Family::new([8u8; 32]);
    rejects_finalize(drain(1));
    rejects_finalize(scanning(&family, 1, None));
}
