//! Tests the reads of synced folder rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::types::Key;

use super::*;

fn folder_id() -> Ulid {
    Ulid::from_bytes([1; 16])
}

fn base(entry: EntryState) -> SyncBase {
    SyncBase {
        synced: None,
        local_version_id: None,
        synced_at_ms: 0,
        entry,
        pending_at: None,
        local: None,
        remote: None,
    }
}

fn row(key: &[u8], base: &SyncBase) -> (Key, Value) {
    (Key::from(key.to_vec()), base.to_bytes().unwrap().into())
}

fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
    IoResult::Page { rows, next }
}

fn entry_key(relative: &str) -> Vec<u8> {
    base_key(folder_id(), relative).to_vec()
}

// Only entries in the asked state are listed, and rows that do not decode are skipped.
#[test]
fn filters_entries() {
    let mut list = list_entries(folder_id(), Some("in_sync"), None);
    let call = folder_page(SYNC_BASE_KEYSPACE, folder_id(), None);
    assert_eq!(list.first(), Ok(Next::Call(call)));
    let synced = base(EntryState::InSync);
    let name = synced.entry.name().to_string();
    let mut list_named = list_entries(folder_id(), Some(&name), None);
    list_named.first().unwrap();
    let rows = vec![
        row(&entry_key("a"), &synced),
        (Key::from(entry_key("b")), Value::from(vec![0xFF])),
    ];
    let next = Some(Key::from(entry_key("b")));
    let listed = list_named.next(page(rows, next.clone()));
    assert_eq!(
        listed,
        Ok(Next::Done((vec![("a".to_string(), synced)], next)))
    );
}

// Counters add the base rows of every page, then every queued upload.
#[test]
fn counts_rows() {
    let mut counters = folder_counters(folder_id());
    counters.first().unwrap();
    let bases = vec![row(&entry_key("a"), &base(EntryState::InSync))];
    assert!(matches!(
        counters.next(page(bases, None)),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let uploads = vec![(Key::from(entry_key("a")), Value::from(vec![0xFF]))];
    let Ok(Next::Done(done)) = counters.next(page(uploads, None)) else {
        panic!("expected the counters");
    };
    assert_eq!((done.in_sync, done.uploading), (1, 1));
}

// An absent or unreadable row is not found.
#[test]
fn missing_row_absent() {
    let mut read = read_entry(folder_id(), "a");
    read.first().unwrap();
    let answer = IoResult::Values(vec![None]);
    let child = IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(answer)));
    assert_eq!(read.next(child), Err(FolderError::NotFound));
}

// With no folder bound there are no transfers to look for.
#[test]
fn empty_without_folders() {
    let mut list = list_transfers();
    assert!(matches!(list.first(), Ok(Next::Call(IoCall::Child(_)))));
    let none = Ok::<Vec<SyncedFolder>, FolderError>(Vec::new());
    let answer = IoResult::Child(ChildOutcome::new(none));
    assert_eq!(list.next(answer), Ok(Next::Done(Vec::new())));
}
