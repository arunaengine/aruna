//! Tests the turns of a storage purge by their calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::job::JobErrorKind;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::format::Compression;
use aruna_core::types::Value;

use super::*;

const NOW_MS: u64 = 7;

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[3; 32]).public()
}

fn group() -> Ulid {
    Ulid::from_bytes([2; 16])
}

fn spec(scope: StoragePurgeScope) -> StoragePurgeSpec {
    let realm_id = RealmId([1; 32]);
    StoragePurgeSpec {
        scope,
        group_id: group(),
        auth_context: AuthContext {
            user_id: aruna_core::UserId::local(Ulid::from_bytes([4; 16]), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        },
        node_id: node(),
    }
}

fn bucket_scope() -> StoragePurgeScope {
    StoragePurgeScope::Bucket {
        bucket: "data".to_string(),
    }
}

fn prefix_scope() -> StoragePurgeScope {
    StoragePurgeScope::Prefix {
        bucket: "data".to_string(),
        prefix: "logs/".to_string(),
    }
}

fn file_scope() -> StoragePurgeScope {
    StoragePurgeScope::File {
        bucket: "data".to_string(),
        key: "a".to_string(),
    }
}

fn info(group_id: Ulid) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: Default::default(),
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    }
}

fn run_at(scope: StoragePurgeScope, phase: PurgePhase) -> PurgeRun {
    let ids = (JobId::from_bytes([1; 16]), Ulid::from_bytes([5; 16]));
    let wiring = (NodeRouting::default(), None);
    let mut run = PurgeRun::new(
        ids,
        &spec(scope),
        false,
        wiring,
        JobProgress::new("objects"),
    );
    run.bucket = Some(info(group()));
    run.phase = phase;
    run
}

fn turn(run: PurgeRun) -> PurgeTurnOperation {
    PurgeTurnOperation::new(run, NOW_MS)
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn is_child(next: &TurnNext) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

fn ended(next: TurnNext) -> TurnEnd {
    match next {
        Ok(Next::Done(end)) => end,
        other => panic!("the turn goes on: {other:?}"),
    }
}

fn failure(next: TurnNext) -> JobError {
    match next {
        Err(PurgeStop::Failed(error)) => error,
        other => panic!("the turn did not fail: {other:?}"),
    }
}

fn version(key: &str) -> ListVersionsItem {
    ListVersionsItem::DeleteMarker {
        key: key.to_string(),
        version_id: Ulid::from_bytes([6; 16]),
        is_latest: true,
        created_at: SystemTime::UNIX_EPOCH,
    }
}

fn versions(items: Vec<ListVersionsItem>, next_key: Option<&str>) -> IoResult {
    child(Ok::<_, ListVersionsError>(ListVersionsResult {
        items,
        common_prefixes: Vec::new(),
        is_truncated: next_key.is_some(),
        next_key_marker: next_key.map(str::to_string),
        next_version_marker: None,
    }))
}

fn no_uploads() -> IoResult {
    child(Ok::<_, ListUploadsError>(ListUploadsResult {
        uploads: Vec::new(),
        common_prefixes: Vec::new(),
        is_truncated: false,
        next_key_marker: None,
        next_upload_marker: None,
    }))
}

fn renewed(cancel_requested: bool) -> IoResult {
    child(Ok::<_, JobMutationError>(RenewOutcome { cancel_requested }))
}

fn stored() -> IoResult {
    child(Ok::<_, JobMutationError>(()))
}

fn count(purpose: CountFor) -> PurgePhase {
    PurgeTurnOperation::count_versions(purpose)
}

fn uploads_phase(purpose: CountFor, versions: u64) -> PurgePhase {
    PurgePhase::CountUploads {
        purpose,
        versions,
        key_marker: None,
        upload_marker: None,
        counted: 0,
    }
}

// A bucket of this group is read with its creation row, then the checkpoint.
#[test]
fn begin_reads_bucket() {
    let mut begin = turn(run_at(prefix_scope(), PurgePhase::Begin));
    assert!(is_child(&begin.first()));
    let read = begin.next(child(Ok::<_, GetBucketError>(info(group()))));
    assert!(matches!(read, Ok(Next::Call(IoCall::Read(_)))));
    let rows = IoResult::Values(vec![Some(Value::from(vec![1])), None]);
    assert!(is_child(&begin.next(rows)));
    let none = child(Ok::<_, JobMutationError>(None::<StoragePurgeCheckpoint>));
    let end = ended(begin.next(none));
    assert_eq!(end.turn, PurgeTurn::Continue);
    assert_eq!(end.run.phase, count(CountFor::Initial));
    assert!(end.run.bucket.is_some());
    assert!(end.run.incarnation.is_some());
}

// A stored checkpoint fixes the total and skips the first count.
#[test]
fn checkpoint_resumes_count() {
    let mut begin = turn(run_at(prefix_scope(), PurgePhase::Begin));
    begin.first().unwrap();
    let missing = child(Err::<BucketInfo, _>(GetBucketError::NotFound));
    assert!(is_child(&begin.next(missing)));
    let checkpoint = StoragePurgeCheckpoint {
        initial_versions: 4,
        initial_multipart_uploads: 1,
        batches_completed: 2,
    };
    let found = child(Ok::<_, JobMutationError>(Some(checkpoint)));
    let end = ended(begin.next(found));
    assert_eq!(end.run.phase, count(CountFor::Remaining));
    assert_eq!(end.run.progress.total, Some(5));
}

// A bucket of another group is refused for good.
#[test]
fn foreign_bucket_refused() {
    let foreign = info(Ulid::from_bytes([9; 16]));
    let mut begin = turn(run_at(prefix_scope(), PurgePhase::Begin));
    begin.first().unwrap();
    let error = failure(begin.next(child(Ok::<_, GetBucketError>(foreign))));
    assert_eq!(error.kind, JobErrorKind::Permanent);
}

// A truncated page without a cursor cannot be continued.
#[test]
fn truncation_needs_cursor() {
    let mut page = turn(run_at(prefix_scope(), count(CountFor::Initial)));
    assert!(is_child(&page.first()));
    let reply = child(Ok::<_, ListVersionsError>(ListVersionsResult {
        items: Vec::new(),
        common_prefixes: Vec::new(),
        is_truncated: true,
        next_key_marker: None,
        next_version_marker: None,
    }));
    let error = failure(page.next(reply));
    assert_eq!(
        error.message,
        "version inventory truncated without a cursor"
    );
}

// Versions are counted page by page, then the uploads.
#[test]
fn counts_all_pages() {
    let mut first = turn(run_at(prefix_scope(), count(CountFor::Initial)));
    first.first().unwrap();
    let end = ended(first.next(versions(vec![version("a")], Some("a"))));
    let PurgePhase::CountVersions {
        counted,
        key_marker,
        ..
    } = &end.run.phase
    else {
        panic!("still counting versions");
    };
    assert_eq!((*counted, key_marker.as_deref()), (1, Some("a")));

    let mut last = turn(*end.run);
    last.first().unwrap();
    let end = ended(last.next(versions(vec![version("b")], None)));
    assert_eq!(end.run.phase, uploads_phase(CountFor::Initial, 2));
}

// The first count is stored as the checkpoint before anything is deleted.
#[test]
fn initial_count_stored() {
    let mut uploads = turn(run_at(bucket_scope(), uploads_phase(CountFor::Initial, 3)));
    uploads.first().unwrap();
    assert!(is_child(&uploads.next(no_uploads())));
    let end = ended(uploads.next(stored()));
    assert_eq!(end.run.checkpoint.initial_versions, 3);
    assert_eq!(end.run.progress.total, Some(4));
    assert_eq!(end.run.phase, count(CountFor::Remaining));
}

// The second count sets the progress, and a cancel seen in its flush stops the purge.
#[test]
fn flush_sees_cancel() {
    let mut run = run_at(prefix_scope(), uploads_phase(CountFor::Remaining, 1));
    run.checkpoint.initial_versions = 3;
    let mut uploads = turn(run);
    uploads.first().unwrap();
    assert!(is_child(&uploads.next(no_uploads())));
    assert_eq!(uploads.run().unwrap().progress.current, 2);
    assert_eq!(uploads.next(renewed(true)), Err(PurgeStop::Cancelled));
}

// Without the bucket a purge goes straight on to prove it gone, with no stop between.
#[test]
fn absent_bucket_proved() {
    let mut run = run_at(bucket_scope(), uploads_phase(CountFor::Remaining, 0));
    run.bucket = None;
    let mut uploads = turn(run);
    uploads.first().unwrap();
    assert!(is_child(&uploads.next(no_uploads())));
    assert!(is_child(&uploads.next(renewed(false))));
    assert!(is_child(&uploads.next(renewed(false))));
    assert_eq!(uploads.step, TurnStep::Proved { deleted: true });
}

// A paused purge ends its turn before it lists anything.
#[test]
fn paused_ends_turn() {
    let mut page = turn(run_at(prefix_scope(), PurgePhase::AbortPage));
    assert!(is_child(&page.first()));
    let end = ended(page.next(child(Ok::<_, PauseError>(true))));
    assert_eq!(end.turn, PurgeTurn::Paused);
    assert_eq!(end.run.phase, PurgePhase::AbortPage);
}

// No upload left moves on to the release check of the versions.
#[test]
fn empty_uploads_guard() {
    let mut page = turn(run_at(prefix_scope(), PurgePhase::AbortPage));
    page.first().unwrap();
    assert!(is_child(&page.next(child(Ok::<_, PauseError>(false)))));
    let end = ended(page.next(no_uploads()));
    assert_eq!(end.run.phase, guards_phase());
}

fn guards_phase() -> PurgePhase {
    PurgePhase::Guards {
        key_marker: None,
        version_marker: None,
    }
}

// A version a release pins refuses the whole purge.
#[test]
fn pinned_version_refuses() {
    let mut page = turn(run_at(prefix_scope(), guards_phase()));
    page.first().unwrap();
    assert!(is_child(&page.next(versions(vec![version("a")], None))));
    let pins = child(Ok::<_, PinnedError>((vec![true], ReleaseGuards::new())));
    assert_eq!(failure(page.next(pins)).kind, JobErrorKind::Permanent);
}

// The first check's guards are kept for every delete.
#[test]
fn first_guards_kept() {
    let mut page = turn(run_at(prefix_scope(), guards_phase()));
    page.first().unwrap();
    page.next(versions(vec![version("a")], None)).unwrap();
    let guard = ("space".to_string(), Key::from(vec![1]), None);
    let pins = child(Ok::<_, PinnedError>((vec![false], vec![guard.clone()])));
    let end = ended(page.next(pins));
    assert_eq!(end.run.guards, Some(vec![guard]));
    assert_eq!(end.run.phase, PurgePhase::DeletePage);
}

/// A delete turn that authorized its one key and waits for the delete.
fn at_delete() -> PurgeTurnOperation {
    let mut page = turn(run_at(prefix_scope(), PurgePhase::DeletePage));
    page.first().unwrap();
    page.next(child(Ok::<_, PauseError>(false))).unwrap();
    assert!(is_child(&page.next(versions(vec![version("a")], None))));
    assert!(is_child(&page.next(child(Ok::<_, AuthorizeError>(())))));
    page
}

// A denied key is a verdict on the purge.
#[test]
fn denied_key_fails() {
    let mut page = turn(run_at(prefix_scope(), PurgePhase::DeletePage));
    page.first().unwrap();
    page.next(child(Ok::<_, PauseError>(false))).unwrap();
    page.next(versions(vec![version("a")], None)).unwrap();
    let denied = child(Err::<(), _>(AuthorizeError::PermissionDenied));
    assert_eq!(failure(page.next(denied)).kind, JobErrorKind::Permanent);
}

// A finished batch advances the progress and its checkpoint.
#[test]
fn delete_records_batch() {
    let mut page = at_delete();
    let deleted = child(Ok::<_, DeleteObjectError>(Vec::<BulkDeleteOutcome>::new()));
    assert_eq!(
        page.next(deleted),
        Ok(Next::Call(IoCall::Delete(Vec::new())))
    );
    assert!(is_child(&page.next(IoResult::Done)));
    assert_eq!(page.run().unwrap().progress.current, 1);
    assert!(is_child(&page.next(renewed(false))));
    let end = ended(page.next(stored()));
    assert_eq!(end.run.checkpoint.batches_completed, 1);
}

// An empty page leads to the final proof that nothing is left.
#[test]
fn final_list_empty() {
    let mut last = turn(run_at(prefix_scope(), PurgePhase::FinalList));
    assert!(is_child(&last.first()));
    let error = failure(last.next(versions(vec![version("a")], None)));
    assert_eq!(error.message, "final purge version re-list was not empty");
}

// A prefix purge keeps its bucket and ends with the audit row.
#[test]
fn prefix_purge_finishes() {
    let mut run = run_at(prefix_scope(), PurgePhase::FinalList);
    run.checkpoint.initial_versions = 2;
    let mut last = turn(run);
    last.first().unwrap();
    assert!(is_child(&last.next(versions(Vec::new(), None))));
    assert!(is_child(&last.next(no_uploads())));
    let end = ended(last.next(child(Ok::<_, IoError>(()))));
    let PurgeTurn::Finished(result) = end.turn else {
        panic!("the purge ends");
    };
    assert!(!result.bucket_deleted && result.emptiness_proven);
    assert_eq!(result.versions_removed, 2);
    assert_eq!(end.run.progress.current, 2);
}

// A bucket purge deletes the bucket and proves that it is gone before the audit.
#[test]
fn bucket_purge_proves() {
    let mut last = turn(run_at(bucket_scope(), PurgePhase::FinalList));
    last.first().unwrap();
    last.next(versions(Vec::new(), None)).unwrap();
    assert!(is_child(&last.next(no_uploads())));
    assert!(is_child(&last.next(child(Ok::<_, DeleteBucketError>(())))));
    assert!(is_child(&last.next(renewed(false))));
    assert!(is_child(&last.next(stored())));
    let still = child(Ok::<_, GetBucketError>(info(group())));
    assert_eq!(
        failure(last.next(still)).message,
        "bucket still exists after purge"
    );
}

// A file purge looks only at its own key and stops paging past it.
#[test]
fn file_scope_narrows() {
    let page = version_page(
        &file_scope(),
        ListVersionsResult {
            items: vec![version("a"), version("b")],
            common_prefixes: Vec::new(),
            is_truncated: true,
            next_key_marker: Some("b".to_string()),
            next_version_marker: None,
        },
    );
    assert_eq!(page.items, vec![version("a")]);
    assert!(!page.is_truncated && page.next_key_marker.is_none());
}
