//! Tests of the calls and results of reading a page's write intents.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use byteview::ByteView;

use super::*;

fn object(key: &str) -> ExternalObject {
    ExternalObject {
        key: key.to_string(),
        size: 0,
        etag: None,
        modified_ms: None,
    }
}

fn import_id() -> Ulid {
    Ulid::from_bytes([1; 16])
}

fn intent_key(path: &str, id: u8) -> Key {
    let mut key = object_key(import_id(), path).to_vec();
    key.push(0);
    key.extend_from_slice(&[id; 16]);
    key.into()
}

fn answer(values: Vec<(Key, ByteView)>, next: Option<Key>) -> IoResult {
    IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
        values,
        next_start_after: next,
    })))
}

fn start_of(next: Next<PathIntents>) -> IterStart {
    let Next::Call(IoCall::Effect(effect)) = next else {
        panic!("expected a range read");
    };
    let Effect::Storage(StorageEffect::Iter { start, .. }) = *effect else {
        panic!("expected a range read");
    };
    start.expect("the read names its start")
}

// An empty listing page has no range to read.
#[test]
fn empty_page_skipped() {
    let mut intents = PageIntentsOperation::new(import_id(), &[]);
    assert_eq!(intents.first(), Ok(Next::Done(PathIntents::new())));
}

// The read starts at the page's first path and goes on after the last row of each answer.
#[test]
fn range_continues() {
    let objects = [object("b"), object("a")];
    let mut intents = PageIntentsOperation::new(import_id(), &objects);
    let first = start_of(intents.first().unwrap());
    assert_eq!(first, IterStart::At(object_key(import_id(), "a")));
    let after = intent_key("a", 9);
    let next = intents
        .next(answer(Vec::new(), Some(after.clone())))
        .unwrap();
    assert_eq!(start_of(next), IterStart::After(after));
    assert_eq!(
        intents.next(answer(Vec::new(), None)),
        Ok(Next::Done(PathIntents::new()))
    );
}

// A row past the page's last path ends the read; a malformed key fails it.
#[test]
fn rows_bound_read() {
    let objects = [object("a")];
    let mut intents = PageIntentsOperation::new(import_id(), &objects);
    intents.first().unwrap();
    let past = vec![(intent_key("b", 1), ByteView::from(Vec::new()))];
    assert_eq!(
        intents.next(answer(past, Some(intent_key("b", 1)))),
        Ok(Next::Done(PathIntents::new()))
    );
    let mut broken = PageIntentsOperation::new(import_id(), &objects);
    broken.first().unwrap();
    let bad = vec![(ByteView::from(vec![1, 2]), ByteView::from(Vec::new()))];
    assert!(broken.next(answer(bad, None)).is_err());
}

// An intent key names the path between the import id and the version id.
#[test]
fn intent_key_path() {
    assert_eq!(intent_path(&intent_key("x/y", 3)), Some("x/y"));
    assert_eq!(intent_path(&[0; 4]), None);
}
