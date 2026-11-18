//! Checks export consent through its calls and answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::tests::{local_candidate, op_spec, plain_entity, selected_spec};
use super::*;
use crate::jobs::repository::test_support::{child, owner_node};

fn checking(selected: bool) -> UnheldBucketOperation {
    let spec = match selected {
        true => selected_spec(),
        false => op_spec(),
    };
    UnheldBucketOperation::new(spec, owner_node(), "{}".to_string())
}

fn resolved(entities: Vec<ExportEntity>) -> IoResult {
    let resolved = Resolved {
        entities,
        aliases: AliasState::default(),
    };
    child(Ok::<_, ExportFailure>(resolved))
}

fn with_copy() -> ExportEntity {
    let mut entity = plain_entity("a");
    entity.candidates = vec![local_candidate()];
    entity
}

fn key_row(value: Option<aruna_core::types::Value>) -> IoResult {
    let event = StorageEvent::ReadResult {
        key: Default::default(),
        value,
    };
    IoResult::Event(Box::new(Event::Storage(event)))
}

// A document that is no JSON-LD cannot be checked.
#[test]
fn invalid_crate_fails() {
    let spec = selected_spec();
    let mut check = UnheldBucketOperation::new(spec, owner_node(), "not json".to_string());
    assert!(check.first().is_err());
}

// Nothing selected has nothing to refuse.
#[test]
fn no_entities_pass() {
    let mut check = checking(true);
    assert_eq!(check.next(resolved(Vec::new())), Ok(Next::Done(None)));
}

// Without a selection every copy is readable.
#[test]
fn unselected_passes() {
    let mut check = checking(false);
    assert_eq!(
        check.next(resolved(vec![with_copy()])),
        Ok(Next::Done(None))
    );
}

// A plain copy of a plain bucket needs no key.
#[test]
fn plain_bucket_passes() {
    let mut check = checking(true);
    let turn = check.next(resolved(vec![with_copy()]));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Effect(_)))));
    assert_eq!(check.next(key_row(None)), Ok(Next::Done(None)));
}

// A copy that needs a key the caller does not hold names its bucket.
#[test]
fn non_holder_refused() {
    let mut check = checking(true);
    check.copies.push_back(LocalCopy {
        bucket: "data".to_string(),
        keyed: true,
    });
    assert!(matches!(
        check.next_copy(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let answer = child(Ok::<_, PlaintextError>(false));
    assert_eq!(check.next(answer), Ok(Next::Done(Some("data".to_string()))));
}

// One readable copy is enough for its entity.
#[test]
fn holder_passes() {
    let mut check = checking(true);
    check.copies.push_back(LocalCopy {
        bucket: "data".to_string(),
        keyed: true,
    });
    check.next_copy().unwrap();
    let answer = child(Ok::<_, PlaintextError>(true));
    assert_eq!(check.next(answer), Ok(Next::Done(None)));
}
