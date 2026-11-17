//! State and event tests of the complete job family read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::FAMILY_RECORD_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::operation_check::{
    check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
};
use aruna_core::structs::execution::job::{
    JobRecordEnvelope, JobRecordKind, PhysicalExecutionState,
};
use aruna_core::types::{Key, Value};

use super::{FamilyReadError, LoadFamilyOperation};
use crate::jobs::records::RECORD_PAGE_SIZE;
use crate::tests::records::Family;
use aruna_core::structs::execution::job::record_keys::{family_prefix, kind_prefix, record_key};
use aruna_core::structs::execution::job::record_rows::to_bytes;

fn fixture() -> Family {
    Family::new([1u8; 32])
}

fn records() -> Vec<JobRecordEnvelope> {
    fixture().run(1, 0, PhysicalExecutionState::Succeeded)
}

fn row(envelope: &JobRecordEnvelope) -> (Key, Value) {
    (
        record_key(&envelope.key()),
        Value::from(to_bytes(envelope).expect("record encodes").as_slice()),
    )
}

/// One page of stored rows, with a continuation when more follow it.
fn page(records: &[JobRecordEnvelope], more: bool) -> Event {
    Event::Storage(StorageEvent::IterResult {
        values: records.iter().map(row).collect(),
        next_start_after: more.then(|| next_key(records)),
    })
}

fn next_key(records: &[JobRecordEnvelope]) -> Key {
    record_key(&records.last().expect("page carries a record").key())
}

fn scan(prefix: Key, cursor: Option<Key>) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: FAMILY_RECORD_KEYSPACE.to_string(),
        prefix: Some(prefix),
        start: cursor.map(IterStart::After),
        limit: RECORD_PAGE_SIZE,
        txn_id: None,
    })
}

fn started() -> LoadFamilyOperation {
    let mut load = LoadFamilyOperation::family(fixture().family());
    load.start();
    load
}

#[test]
fn reads_all_pages() {
    let records = records();
    let (head, tail) = records.split_at(3);
    let prefix = family_prefix(&fixture().family());
    let mut load = LoadFamilyOperation::family(fixture().family());
    check_trace(
        &mut load,
        &[scan(prefix.clone(), None)],
        vec![
            (page(head, true), vec![scan(prefix, Some(next_key(head)))]),
            (page(tail, false), Vec::new()),
        ],
    );
    assert_eq!(load.finalize().expect("both pages load"), records);
}

#[test]
fn reads_kind_prefix() {
    let family = fixture().family();
    let mut load = LoadFamilyOperation::kind(family, JobRecordKind::Receipt);
    check_trace(
        &mut load,
        &[scan(kind_prefix(&family, JobRecordKind::Receipt), None)],
        vec![(page(&[], false), Vec::new())],
    );
    assert_eq!(load.finalize(), Ok(Vec::new()));
}

#[test]
fn rejects_page_error() {
    // A later page failing must never surface as the pages before it: the
    // receipt on page two would otherwise look absent.
    let records = records();
    let mut load = started();
    load.step(page(&records[..3], true));
    let error = StorageError::Timeout;
    assert!(
        load.step(Event::Storage(StorageEvent::Error {
            error: error.clone()
        }))
        .is_empty()
    );
    assert_eq!(
        load.finalize(),
        Err(FamilyReadError::Storage(error.to_string()))
    );
}

#[test]
fn rejects_undecodable_row() {
    let mut values: Vec<(Key, Value)> = records().iter().map(row).collect();
    values[1].1 = Value::from([0xffu8; 8].as_slice());
    let mut load = started();
    load.step(Event::Storage(StorageEvent::IterResult {
        values,
        next_start_after: None,
    }));
    assert!(matches!(load.finalize(), Err(FamilyReadError::Decode(_))));
}

#[test]
fn rejects_malformed_key() {
    let mut values: Vec<(Key, Value)> = records().iter().map(row).collect();
    values[0].0 = Key::from([1u8; 4].as_slice());
    let mut load = started();
    load.step(Event::Storage(StorageEvent::IterResult {
        values,
        next_start_after: None,
    }));
    assert_eq!(load.finalize(), Err(FamilyReadError::MalformedKey));
}

#[test]
fn stops_at_limit() {
    // A family larger than the read bound is reported, never truncated into
    // a decision.
    let records = records();
    let mut load = LoadFamilyOperation::new(Key::from([7u8].as_slice()), 3);
    load.start();
    assert!(load.step(page(&records[..3], true)).is_empty());
    assert_eq!(
        load.finalize(),
        Err(FamilyReadError::LimitExceeded { loaded: 3 })
    );
}

#[test]
fn rejects_unfit_events() {
    let refusals = rejects_events(started, sample_events, |event| {
        matches!(
            event,
            Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
        )
    });
    assert!(!refusals.is_empty());
    for (label, effects, error) in refusals {
        assert!(effects.is_empty(), "{label} emitted effects");
        assert!(
            matches!(error, FamilyReadError::Storage(_)),
            "{label} gave {error:?}"
        );
    }
    let unstarted = || LoadFamilyOperation::family(fixture().family());
    let refusals = rejects_events(unstarted, sample_events, |_| false);
    assert_eq!(refusals.len(), sample_events().len());
    let mut done = started();
    done.step(page(&[], false));
    done.step(page(&[], false));
    assert!(matches!(done.finalize(), Err(FamilyReadError::Storage(_))));
}

#[test]
fn aborts_without_cleanup() {
    check_abort(&mut LoadFamilyOperation::family(fixture().family()), &[]);
    check_abort(&mut started(), &[]);
}

#[test]
fn rejects_early_finalize() {
    rejects_finalize(LoadFamilyOperation::family(fixture().family()));
    rejects_finalize(started());
}
