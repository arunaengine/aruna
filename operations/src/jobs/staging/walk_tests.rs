//! Tests of the calls and results of walking one listing page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::staging::initial_checkpoint;
use crate::jobs::staging::pure_tests::{child_answer, test_bucket, test_connector, test_run};
use crate::jobs::store::mutation_error::JobMutationError;
use crate::s3::bucket::get::GetBucketError;
use aruna_core::structs::execution::source_access::ListCursor;

fn walk() -> StagingWalk {
    StagingWalk {
        source_path: "src/".to_string(),
        target_prefix: "copy".to_string(),
        cursor: None,
        queue_row: None,
        from: None,
    }
}

fn walking(flat: bool) -> StagingJobCheckpoint {
    let mut checkpoint = initial_checkpoint(&test_run().spec);
    checkpoint.walk = Some(walk());
    checkpoint.flat = Some(flat);
    checkpoint
}

fn operation(flat: bool) -> WalkPageOperation {
    WalkPageOperation::new(test_run(), WalkPlace::default(), walking(flat), None)
}

fn page(paths: &[&str], next_cursor: Option<&str>) -> ListStagingResult {
    let entries = (paths.iter())
        .map(|path| SourceEntry {
            name: path.to_string(),
            path: path.to_string(),
            kind: SourceEntryKind::File,
            size: Some(2),
            modified: None,
            etag: None,
            stat: None,
        })
        .collect();
    ListStagingResult {
        entries,
        next_cursor: next_cursor.map(|cursor| ListCursor::After(cursor.to_string())),
        connector: test_connector(),
        secret_fingerprint: None,
    }
}

fn listed(page: Result<ListStagingResult, ItemFailure>) -> IoResult {
    child_answer(Ok::<_, PageFailure>(Listing::Page(None, page)))
}

// A walk without a position has nothing to do and writes nothing.
#[test]
fn no_walk_ends() {
    let mut checkpoint = walking(false);
    checkpoint.walk = None;
    let mut page = WalkPageOperation::new(test_run(), WalkPlace::default(), checkpoint, None);
    assert!(matches!(page.first(), Ok(Next::Done(_))));
}

// The end of the coordinated ranges closes the walk with one checkpoint write.
#[test]
fn exhausted_walk_commits() {
    let mut walk = operation(true);
    assert!(matches!(walk.first(), Ok(Next::Call(IoCall::Child(_)))));
    let end = child_answer(Ok::<_, PageFailure>(Listing::Done));
    assert!(matches!(walk.next(end), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(walk.step, WalkStep::Commit);
    assert_eq!(walk.next.walk, None);
    let written = child_answer(Ok::<(), JobMutationError>(()));
    let Ok(Next::Done(walked)) = walk.next(written) else {
        panic!("expected the walked page");
    };
    assert_eq!(walked.checkpoint.walk, None);
}

// A source that refuses the path skips it; the failure is counted, the walk moves on.
#[test]
fn refused_listing_counts() {
    let mut walk = operation(false);
    walk.first().unwrap();
    let refused = listed(Err(ItemFailure::Stage("denied".to_string())));
    assert!(matches!(
        walk.next(refused),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(walk.next.items_failed, 1);
    assert_eq!(walk.step, WalkStep::Commit);
}

// A provider that repeats its cursor would loop forever, so the job fails for good.
#[test]
fn repeated_cursor_fails() {
    let mut checkpoint = walking(false);
    let cursor = ListCursor::After("next".to_string());
    checkpoint.walk.as_mut().unwrap().cursor = Some(cursor);
    let mut walk = WalkPageOperation::new(test_run(), WalkPlace::default(), checkpoint, None);
    walk.first().unwrap();
    let answer = walk.next(listed(Ok(page(&[], Some("next")))));
    assert!(matches!(answer, Err(PageFailure::Permanent(_))));
}

// A flat page is imported while the next page is listed; both answers come back together.
#[test]
fn flat_lists_ahead() {
    let mut walk = operation(true);
    walk.first().unwrap();
    let Ok(Next::Call(IoCall::Group(group))) = walk.next(listed(Ok(page(&["src/a"], Some("two")))))
    else {
        panic!("expected the page and the next listing");
    };
    assert_eq!(group.members.len(), 2);
    assert_eq!(walk.step, WalkStep::Both);
    let checkpoint = walk.next.clone();
    let results = vec![
        GroupResult::Done(Box::new(Event::Child(ChildOutcome::new(Ok::<
            _,
            PageFailure,
        >(
            checkpoint.clone(),
        ))))),
        GroupResult::Done(Box::new(Event::Child(ChildOutcome::new(Ok::<
            _,
            PageFailure,
        >(
            Listing::Done
        ))))),
    ];
    let Ok(Next::Done(walked)) = walk.next(IoResult::Group(GroupEvent::Finished { results }))
    else {
        panic!("expected the walked page");
    };
    assert_eq!(walked.checkpoint, checkpoint);
    assert!(walked.ahead.is_some());
}

// A listing made ahead for this position replaces the request.
#[test]
fn ahead_listing_used() {
    let ahead = Ahead {
        walk: walk(),
        listing: Listing::Done,
    };
    let mut page =
        WalkPageOperation::new(test_run(), WalkPlace::default(), walking(true), Some(ahead));
    assert!(matches!(page.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(page.step, WalkStep::Commit);
}

// A flat reference walk gathers listings until the page is full or the source ends.
#[test]
fn listing_gathers_pages() {
    let run = test_run();
    let mut listing = ListingOperation::new(run, walk(), true, None);
    assert!(matches!(listing.first(), Ok(Next::Call(IoCall::Child(_)))));
    let whole = child_answer(Ok::<_, PageFailure>(Bounds::Whole));
    assert!(matches!(
        listing.next(whole),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let first = child_answer(Ok::<_, ItemFailure>(page(&["src/a"], Some("two"))));
    assert!(matches!(
        listing.next(first),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(listing.step, ListingStep::More);
    let more = child_answer(Ok::<_, ItemFailure>(page(&["src/b"], None)));
    let Ok(Next::Done(Listing::Page(None, Ok(gathered)))) = listing.next(more) else {
        panic!("expected the gathered page");
    };
    assert_eq!(gathered.entries.len(), 2);
    assert_eq!(gathered.next_cursor, None);
}

// A failed follow-up listing ends the gathered page; the walk lists on from its cursor.
#[test]
fn failed_more_keeps() {
    let mut listing = ListingOperation::new(test_run(), walk(), true, None);
    listing.step = ListingStep::More;
    listing.page = Some(page(&["src/a"], Some("two")));
    let failed = child_answer(Err::<ListStagingResult, _>(ItemFailure::System("x".into())));
    let Ok(Next::Done(Listing::Page(None, Ok(kept)))) = listing.next(failed) else {
        panic!("expected the page gathered so far");
    };
    assert_eq!(kept.next_cursor.as_ref().map(|_| ()), Some(()));
}

// An ordinary bucket's first walk lists everything; a lane's bound cuts it.
#[test]
fn segment_of_ordinary() {
    let run = test_run();
    let bucket = || child_answer(Ok::<_, GetBucketError>(test_bucket(&test_run().spec)));
    let mut whole = SegmentOperation::new(run.clone(), walk(), None);
    whole.first().unwrap();
    assert_eq!(whole.next(bucket()), Ok(Next::Done(Bounds::Whole)));
    let mut lane = SegmentOperation::new(run, walk(), Some("copy/m".to_string()));
    lane.first().unwrap();
    let Ok(Next::Done(Bounds::Segment(segment))) = lane.next(bucket()) else {
        panic!("expected the lane's segment");
    };
    assert_eq!(segment.stop.as_deref(), Some("src/m"));
}
