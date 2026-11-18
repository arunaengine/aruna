//! Tests recording offered files and marking vanished ones.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::structs::execution::source_access::SourceEntryKind;
use aruna_core::structs::identity::realm::RealmId;
use byteview::ByteView;

use super::*;

fn scope() -> ObservationScope {
    let realm_id = RealmId::from_bytes([7; 32]);
    ObservationScope {
        bucket: "offer".to_string(),
        group_id: Ulid::from_bytes([2; 16]),
        realm_id,
        node_id: NodeId::from_bytes(&[3; 32]).unwrap(),
        user_id: UserId::new(Ulid::from_bytes([4; 16]), realm_id),
    }
}

fn entry() -> SourceEntry {
    SourceEntry {
        name: "a.txt".to_string(),
        path: "a.txt".to_string(),
        kind: SourceEntryKind::File,
        size: Some(4),
        modified: None,
        etag: None,
        stat: None,
    }
}

fn row(bytes: Vec<u8>) -> IoResult {
    IoResult::Values(vec![Some(ByteView::from(bytes))])
}

fn absent() -> IoResult {
    IoResult::Values(vec![None])
}

fn pointer() -> CurrentVersionPointer {
    CurrentVersionPointer::next_for(None, Ulid::from_bytes([5; 16])).unwrap()
}

fn observed(entry: &SourceEntry) -> BlobVersion {
    let now = SystemTime::UNIX_EPOCH;
    BlobVersion::reference(
        offered_binding("offer", &entry.path, scope().node_id),
        entry_metadata(entry),
        now,
        scope().user_id,
        now,
    )
}

// A file seen unchanged keeps its version and writes nothing.
#[test]
fn unchanged_keeps_version() {
    let mut observe = ObserveFileOperation::new(scope(), &entry());
    observe.first().unwrap();
    let head = row(pointer().to_bytes().unwrap());
    assert!(matches!(
        observe.next(head),
        Ok(Next::Call(IoCall::Read(_)))
    ));
    let version = row(observed(&entry()).to_bytes().unwrap());
    assert_eq!(observe.next(version), Ok(Next::Done(pointer().version_id)));
}

// A new file gets a version and starts with its head transition.
#[test]
fn new_file_writes() {
    let mut observe = ObserveFileOperation::new(scope(), &entry());
    observe.first().unwrap();
    assert!(matches!(observe.next(absent()), Ok(Next::Call(_))));
    assert!(observe.version_id.is_some());
    assert!(observe.change.is_some());
}

// A failed write answer ends the observation with the storage error.
#[test]
fn write_error_fails() {
    let mut observe = ObserveFileOperation::new(scope(), &entry());
    observe.first().unwrap();
    observe.next(absent()).unwrap();
    let error = StorageError::WriteError("full".to_string());
    let failed = Event::Storage(StorageEvent::Error {
        error: error.clone(),
    });
    let answer = observe.next(IoResult::Event(Box::new(failed)));
    assert_eq!(answer, Err(OfferedDirectoryError::Storage(error)));
}

// Only a live object is marked deleted.
#[test]
fn tombstone_needs_live() {
    let mut missing = TombstoneOperation::new(scope(), "a.txt".to_string());
    missing.first().unwrap();
    assert_eq!(missing.next(absent()), Ok(Next::Done(false)));
    let mut deleted = TombstoneOperation::new(scope(), "a.txt".to_string());
    deleted.first().unwrap();
    deleted.next(row(pointer().to_bytes().unwrap())).unwrap();
    let marker = BlobVersion::deleted(SystemTime::UNIX_EPOCH, scope().user_id);
    let answer = deleted.next(row(marker.to_bytes().unwrap()));
    assert_eq!(answer, Ok(Next::Done(false)));
}

// A live object gets a marker through the head transition.
#[test]
fn tombstone_marks_live() {
    let mut live = TombstoneOperation::new(scope(), "a.txt".to_string());
    live.first().unwrap();
    live.next(row(pointer().to_bytes().unwrap())).unwrap();
    let version = row(observed(&entry()).to_bytes().unwrap());
    assert!(matches!(live.next(version), Ok(Next::Call(_))));
    assert!(live.change.is_some());
}
