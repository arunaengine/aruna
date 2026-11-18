//! Tests fetching remote versions and applying planned downloads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::structs::WriteGuard;
use byteview::ByteView;

use super::super::reconcile::pure_tests::folder;
use super::*;

fn realm() -> RealmId {
    RealmId::from_bytes([7; 32])
}

fn version() -> Ulid {
    Ulid::from_bytes([5; 16])
}

fn blob() -> Blob {
    BackendStream(Box::pin(futures_util::stream::empty()))
}

fn download(relative: &str) -> Download {
    Download {
        relative: relative.to_string(),
        remote_version: version(),
        guard: WriteGuard::MustNotExist,
        conflicted: false,
        local: None,
        synced: None,
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn held_by() -> IoResult {
    let refusal = BaoReadRefusal::HeldBy {
        holder: folder().remote.node_id,
        set_id: Ulid::from_bytes([6; 16]),
    };
    child(Err::<BaoReadOutput, _>(BaoReadError::Refused(refusal)))
}

fn base() -> SyncBase {
    SyncBase {
        synced: None,
        local_version_id: None,
        synced_at_ms: 1,
        entry: EntryState::LocalChanged,
        pending_at: None,
        local: None,
        remote: None,
    }
}

// A member naming a holder redirects the read once; a second redirect is a failure.
#[test]
fn follows_holder_once() {
    let mut fetch = FetchRemoteOperation::new(realm(), &folder(), "a.txt", version());
    assert!(matches!(fetch.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert!(matches!(
        fetch.next(held_by()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert!(fetch.next(held_by()).is_err());
}

// A failed fetch is marked on the entry, and the next download still runs.
#[test]
fn failed_fetch_continues() {
    let plan = vec![download("a.txt"), download("b.txt")];
    let mut apply = ApplyDownloadsOperation::new(realm(), folder(), plan);
    apply.first().unwrap();
    let failed = child(Err::<Blob, _>(FetchError("gone".to_string())));
    assert!(matches!(
        apply.next(failed),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(apply.step, DownloadStep::Mark);
    assert!(matches!(
        apply.next(child(Ok::<_, IoError>(()))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(apply.step, DownloadStep::Fetch);
    assert_eq!(apply.current.as_ref().unwrap().relative, "b.txt");
}

// Written and copied entries count; refused and failed writes do not.
#[test]
fn counts_written_entries() {
    let mut apply = ApplyDownloadsOperation::new(realm(), folder(), vec![download("a.txt")]);
    apply.first().unwrap();
    apply.next(child(Ok::<_, FetchError>(blob()))).unwrap();
    assert_eq!(apply.step, DownloadStep::Write);
    let written = child(Ok::<_, MaterializeError>(MaterializeOutcome::Written));
    assert_eq!(apply.next(written), Ok(Next::Done(1)));
    let mut failed = ApplyDownloadsOperation::new(realm(), folder(), vec![download("a.txt")]);
    failed.first().unwrap();
    failed.next(child(Ok::<_, FetchError>(blob()))).unwrap();
    let outcome = MaterializeOutcome::Failed {
        message: "disk".to_string(),
    };
    let answer = child(Ok::<_, MaterializeError>(outcome));
    assert_eq!(failed.next(answer), Ok(Next::Done(0)));
}

// The reason lands on the base row; an entry without a row stays absent.
#[test]
fn marks_existing_row() {
    let id = folder().folder_id;
    let mut mark = MarkFailedOperation::new(id, "a.txt".to_string(), "gone".to_string());
    mark.first().unwrap();
    let row = ByteView::from(base().to_bytes().unwrap());
    let Ok(Next::Call(IoCall::Write(rows))) = mark.next(IoResult::Values(vec![Some(row)])) else {
        panic!("the row is written");
    };
    let stored = SyncBase::from_bytes(&rows[0].2).unwrap();
    assert_eq!(
        stored.entry,
        EntryState::Error {
            reason: "gone".to_string()
        }
    );
    assert_eq!(mark.next(IoResult::Done), Ok(Next::Done(())));
    let mut absent = MarkFailedOperation::new(id, "a.txt".to_string(), "gone".to_string());
    absent.first().unwrap();
    let answer = absent.next(IoResult::Values(vec![None]));
    assert_eq!(answer, Ok(Next::Done(())));
}
