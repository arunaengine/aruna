//! Tests the steps of offering and withdrawing a directory.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::id::NodeId;
use aruna_core::structs::execution::source_access::SourceEntryKind;
use aruna_core::structs::identity::realm::RealmId;
use byteview::ByteView;

use super::*;

fn input() -> OfferDirectoryInput {
    let realm_id = RealmId::from_bytes([7; 32]);
    OfferDirectoryInput {
        bucket: "offer".to_string(),
        root: "/data".to_string(),
        group_id: Ulid::from_bytes([2; 16]),
        realm_id,
        node_id: NodeId::from_bytes(&[3; 32]).unwrap(),
        user_id: UserId::new(Ulid::from_bytes([4; 16]), realm_id),
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn source(event: StagingSourceEvent) -> IoResult {
    IoResult::Event(Box::new(Event::StagingSource(event)))
}

fn entry(path: &str) -> SourceEntry {
    SourceEntry {
        name: path.to_string(),
        path: path.to_string(),
        kind: SourceEntryKind::File,
        size: Some(1),
        modified: None,
        etag: None,
        stat: None,
    }
}

fn walking() -> OfferDirectoryOperation {
    let mut offer = offer_directory(input());
    offer.first().unwrap();
    offer.next(child(Ok::<_, OfferError>(()))).unwrap();
    offer
}

// The cap refuses a directory only once it is exceeded.
#[test]
fn refuses_over_cap() {
    assert!(!over_cap(MAX_OFFERED_FILES - 1, 1));
    assert!(over_cap(MAX_OFFERED_FILES, 1));
}

// A node without local directory access says so.
#[test]
fn missing_handle_named() {
    let mut check = check_root("/data");
    check.first().unwrap();
    let error = StagingSourceError::HandleMissing;
    let answer = check.next(source(StagingSourceEvent::Error { error }));
    assert_eq!(answer, Err(OfferError::HandleMissing));
}

// The walk follows the cursor and registers the bucket only after the last page.
#[test]
fn walks_all_pages() {
    let mut offer = walking();
    let cursor = ListCursor::Token("next".to_string());
    let first = StagingSourceEvent::ListResult {
        entries: vec![entry("a")],
        next_cursor: Some(cursor),
    };
    offer.next(source(first)).unwrap();
    assert_eq!(offer.step, OfferStep::Walk);
    let last = StagingSourceEvent::ListResult {
        entries: vec![entry("b")],
        next_cursor: None,
    };
    offer.next(source(last)).unwrap();
    assert_eq!(offer.step, OfferStep::Register);
    assert_eq!(offer.entries.len(), 2);
}

// Every file is recorded in walk order before the sweep starts.
#[test]
fn records_then_sweeps() {
    let mut offer = walking();
    let listed = StagingSourceEvent::ListResult {
        entries: vec![entry("a")],
        next_cursor: None,
    };
    offer.next(source(listed)).unwrap();
    offer.next(child(Ok::<_, OfferError>(()))).unwrap();
    assert_eq!(offer.step, OfferStep::Observe);
    let version = Ulid::from_bytes([5; 16]);
    offer.next(child(Ok::<_, OfferError>(version))).unwrap();
    assert_eq!(offer.step, OfferStep::Sweep);
    let Ok(Next::Done(result)) = offer.next(child(Ok::<_, OfferError>(0usize))) else {
        panic!("the offer ends after the sweep");
    };
    assert_eq!(result.files, 1);
    assert_eq!(result.observed[0].version_id, version);
}

// An ordinary bucket of that name is never claimed.
#[test]
fn refuses_taken_bucket() {
    let mut register = RegisterBucketOperation::new(input());
    register.first().unwrap();
    let info = BucketInfo {
        group_id: input().group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: input().user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    let bucket = ByteView::from(info.to_bytes().unwrap());
    let answer = register.next(IoResult::Values(vec![None, Some(bucket)]));
    assert_eq!(answer, Err(OfferError::BucketTaken("offer".to_string())));
}

// The sweep leaves files the walk saw and marks the others.
#[test]
fn sweep_skips_offered() {
    let offered = BTreeSet::from(["a".to_string()]);
    let mut sweep = SweepOperation::new(input().scope(), offered);
    sweep.first().unwrap();
    let row = |key: &str| {
        let head = BlobHeadKey::new("offer", key).to_bytes().unwrap();
        (Key::from(head), Value::from(Vec::new()))
    };
    let rows = vec![row("a"), row("b")];
    let answer = sweep.next(IoResult::Page { rows, next: None });
    assert!(matches!(answer, Ok(Next::Call(IoCall::Child(_)))));
    assert!(sweep.queue.is_empty());
    assert_eq!(
        sweep.next(child(Ok::<_, OfferError>(true))),
        Ok(Next::Done(1))
    );
}

// Withdrawing a bucket that is not offered is refused.
#[test]
fn withdraw_needs_offer() {
    let offer = input();
    let mut withdraw = withdraw_offer(WithdrawOfferInput {
        bucket: offer.bucket,
        realm_id: offer.realm_id,
        node_id: offer.node_id,
        user_id: offer.user_id,
    });
    withdraw.first().unwrap();
    let answer = withdraw.next(IoResult::Values(vec![None]));
    assert_eq!(answer, Err(OfferError::NotOffered("offer".to_string())));
}
