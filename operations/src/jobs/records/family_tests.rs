//! Call and result tests of the complete job family read.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::FAMILY_RECORD_KEYSPACE;
use aruna_core::operation::Operation;
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
fn page(records: &[JobRecordEnvelope], more: bool) -> IoResult {
    IoResult::Page {
        rows: records.iter().map(row).collect(),
        next: more.then(|| next_key(records)),
    }
}

fn next_key(records: &[JobRecordEnvelope]) -> Key {
    record_key(&records.last().expect("page carries a record").key())
}

fn scan(prefix: Key, after: Option<Key>) -> Next<Vec<JobRecordEnvelope>> {
    Next::Call(IoCall::Page {
        space: FAMILY_RECORD_KEYSPACE.to_string(),
        prefix,
        after,
        limit: RECORD_PAGE_SIZE,
    })
}

fn started() -> LoadFamilyOperation {
    let mut load = LoadFamilyOperation::family(fixture().family());
    load.first().expect("first page");
    load
}

#[test]
fn reads_all_pages() {
    let records = records();
    let (head, tail) = records.split_at(3);
    let prefix = family_prefix(&fixture().family());
    let mut load = LoadFamilyOperation::family(fixture().family());
    assert_eq!(load.first(), Ok(scan(prefix.clone(), None)));
    assert_eq!(
        load.next(page(head, true)),
        Ok(scan(prefix, Some(next_key(head))))
    );
    assert_eq!(load.next(page(tail, false)), Ok(Next::Done(records)));
}

#[test]
fn reads_kind_prefix() {
    let family = fixture().family();
    let mut load = LoadFamilyOperation::kind(family, JobRecordKind::Receipt);
    let prefix = kind_prefix(&family, JobRecordKind::Receipt);
    assert_eq!(load.first(), Ok(scan(prefix, None)));
    assert_eq!(load.next(page(&[], false)), Ok(Next::Done(Vec::new())));
}

#[test]
fn reports_storage_error() {
    // The runner answers a failed page with this error instead of a result.
    let error = StorageError::Timeout;
    assert_eq!(
        FamilyReadError::from(IoError::Storage(error.clone())),
        FamilyReadError::Storage(error.to_string())
    );
}

#[test]
fn rejects_undecodable_row() {
    let mut rows: Vec<(Key, Value)> = records().iter().map(row).collect();
    rows[1].1 = Value::from([0xffu8; 8].as_slice());
    let result = started().next(IoResult::Page { rows, next: None });
    assert!(matches!(result, Err(FamilyReadError::Decode(_))));
}

#[test]
fn rejects_malformed_key() {
    let mut rows: Vec<(Key, Value)> = records().iter().map(row).collect();
    rows[0].0 = Key::from([1u8; 4].as_slice());
    let result = started().next(IoResult::Page { rows, next: None });
    assert_eq!(result, Err(FamilyReadError::MalformedKey));
}

#[test]
fn stops_at_limit() {
    // A family larger than the read bound is reported, never truncated into
    // a decision.
    let records = records();
    let mut load = LoadFamilyOperation::new(Key::from([7u8].as_slice()), 3);
    load.first().expect("first page");
    assert_eq!(
        load.next(page(&records[..3], true)),
        Err(FamilyReadError::LimitExceeded { loaded: 3 })
    );
}

#[test]
fn rejects_unfit_result() {
    assert!(matches!(
        started().next(IoResult::Done),
        Err(FamilyReadError::Storage(_))
    ));
}
