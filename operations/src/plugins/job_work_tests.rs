//! Tests job status, outputs and cancel: a queued copy, its kept summary, outputs on this node
//! only, a cancel before the copy ran, and ids the install does not own.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::OutputObject;

use super::*;
use crate::plugins::calls::*;
use crate::plugins::install::tests::Fixture;
use crate::plugins::submit::WorkRequest;
use crate::plugins::submit::tests::{ready, request, scope};
use crate::plugins::work::tests::edit_work;
use crate::plugins::work::{FinalState, WorkState};

fn output(node: NodeId, key: &str) -> OutputObject {
    OutputObject {
        node_id: node,
        bucket: "out".into(),
        key: key.into(),
        version_id: Ulid::from_parts(4, 4),
        execution_id: Ulid::from_parts(5, 5),
        container_path: format!("/out/{key}"),
        size: 7,
        digest: None,
    }
}

#[tokio::test]
async fn status_from_row() {
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let copy = request(&fixture, &install, "out/a.txt");
    let id = start_work(
        &fixture.ctx,
        &scope(&fixture, &install),
        WorkRequest::Copy(copy),
    )
    .await
    .unwrap();
    let local = fixture.actor.node_id;
    let place = (&install, local);
    let queued = job_status(&fixture.ctx, place, &id.to_string())
        .await
        .unwrap();
    let shown = (
        queued.kind.as_str(),
        queued.path.as_str(),
        queued.state.as_str(),
    );
    assert_eq!(shown, ("copy", "out/a.txt", "queued"));

    // Once settled, the kept summary answers, with the outputs on this node.
    let remote = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    let settle = edit_work(&fixture.ctx, (install.install_id, id), |row| {
        let mut summary = summary_of(None).unwrap();
        summary.state = FinalState::Succeeded;
        (summary.message, summary.bytes) = (None, Some(5));
        summary.outputs = vec![output(local, "a.txt"), output(remote, "b.txt")];
        (row.state, row.summary) = (WorkState::Settled, Some(summary));
    });
    settle.await.unwrap();
    let done = job_status(&fixture.ctx, place, &id.to_string())
        .await
        .unwrap();
    assert_eq!((done.state.as_str(), done.bytes), ("succeeded", Some(5)));
    let page = job_outputs(&fixture.ctx, place, &id.to_string(), None).await;
    let page = page.unwrap();
    let paths: Vec<_> = page
        .objects
        .iter()
        .map(|object| object.path.as_str())
        .collect();
    assert_eq!((paths, page.cursor), (vec!["out/a.txt"], None));
    let cursor = job_outputs(&fixture.ctx, place, &id.to_string(), Some("x".into())).await;
    assert_eq!(
        cursor.map(drop).map_err(|e| e.kind),
        Err(ErrorKind::Invalid)
    );

    for unknown in ["nope".to_string(), Ulid::from_parts(8, 8).to_string()] {
        let missing = job_status(&fixture.ctx, place, &unknown).await;
        assert_eq!(
            missing.map(drop).map_err(|e| e.kind),
            Err(ErrorKind::NotFound)
        );
    }
    // A reinstall under the same id has another account and does not see the job.
    let mut other = install.clone();
    other.account = aruna_core::UserId::local(Ulid::from_parts(6, 6), fixture.actor.realm_id);
    let foreign = job_status(&fixture.ctx, (&other, local), &id.to_string()).await;
    assert_eq!(
        foreign.map(drop).map_err(|e| e.kind),
        Err(ErrorKind::NotFound)
    );
}

#[tokio::test]
async fn cancel_stops_job() {
    // A queued copy that never ran ends cancelled; asking again, or after it ended, is no error.
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let copy = request(&fixture, &install, "out/a.txt");
    let id = start_work(
        &fixture.ctx,
        &scope(&fixture, &install),
        WorkRequest::Copy(copy),
    )
    .await;
    let id = id.unwrap().to_string();
    let place = (&install, fixture.actor.node_id);
    for _ in 0..2 {
        cancel_job(&fixture.ctx, &install, &id, 5).await.unwrap();
    }
    let status = job_status(&fixture.ctx, place, &id).await.unwrap();
    assert_eq!(status.state, "cancelled");
    let missing = cancel_job(&fixture.ctx, &install, "nope", 5).await;
    assert_eq!(missing.map_err(|e| e.kind), Err(ErrorKind::NotFound));
}

mod operations {
    use aruna_core::structs::execution::job::JobRecord;
    use serde_json::json;

    use super::*;
    use crate::plugins::fixtures::{self, is_child, node};
    use crate::plugins::rows::tests::child_result;
    use crate::plugins::work::tests::proposed;

    fn job() -> JobId {
        JobId::from_bytes(Ulid::from_parts(8, 8).to_bytes())
    }

    fn row() -> WorkRow {
        proposed(&fixtures::install(), 1)
    }

    fn row_result(row: WorkRow) -> IoResult {
        child_result(Ok::<_, CallError>(row))
    }

    fn encoded(row: &WorkRow) -> Vec<u8> {
        crate::plugins::rows::encode(row).unwrap()
    }

    fn key_of(row: &WorkRow) -> Vec<u8> {
        let run_id = row.work_id;
        let identity = crate::plugins::work::Identity::Call { run_id, ordinal: 0 };
        crate::plugins::work::row_key(fixtures::install().install_id, &identity).unwrap()
    }

    fn scanned(rows: &[WorkRow]) -> IoResult {
        let rows: Rows = (rows.iter())
            .map(|row| (key_of(row), encoded(row)))
            .collect();
        child_result(Ok::<_, StoreError>(rows))
    }

    fn no_record() -> IoResult {
        child_result(Ok::<Option<JobRecord>, JobMutationError>(None))
    }

    fn updated() -> IoResult {
        child_result(Ok::<Option<WorkRow>, StoreError>(None))
    }

    fn reconciled(job: Option<JobId>) -> IoResult {
        child_result(Ok::<_, StoreError>(job))
    }

    fn dispatched() -> WorkRow {
        let mut row = row();
        (row.state, row.job) = (WorkState::Dispatched, Some(job()));
        row
    }

    #[test]
    fn state_reads_record() {
        let install = fixtures::install();
        let mut state = JobState::new((&install, node(3)), "id");
        assert!(is_child(&state.first()));
        assert!(is_child(&state.next(row_result(dispatched()))));
        let Ok(Next::Done(status)) = state.next(no_record()) else {
            panic!("the record ends the status");
        };
        assert_eq!(status.id, dispatched().work_id.to_string());
    }

    #[test]
    fn reconcile_records_job() {
        let row = row();
        let mut reconcile = ReconcileWork::new(fixtures::install().install_id, &row);
        assert!(is_child(&reconcile.first()));
        let found = Ok::<_, SubmitJobError>(Some((job(), [0u8; 32])));
        assert!(is_child(&reconcile.next(child_result(found))));
        assert_eq!(reconcile.next(updated()), Ok(Next::Done(Some(job()))));
        let mut absent = ReconcileWork::new(fixtures::install().install_id, &row);
        absent.first().unwrap();
        let none = Ok::<Option<(JobId, [u8; 32])>, SubmitJobError>(None);
        assert_eq!(absent.next(child_result(none)), Ok(Next::Done(None)));
    }

    fn cancel() -> CancelWork {
        let mut cancel = CancelWork::new(&fixtures::install(), "id", 5);
        assert!(is_child(&cancel.first()));
        cancel
    }

    #[test]
    fn cancel_asks_job() {
        let mut settled = dispatched();
        settled.state = WorkState::Settled;
        assert_eq!(cancel().next(row_result(settled)), Ok(Next::Done(())));
        let mut running = cancel();
        assert!(is_child(&running.next(row_result(dispatched()))));
        let gone = Err::<CancelRequestOutcome, _>(JobMutationError::NotFound);
        assert_eq!(running.next(child_result(gone)), Ok(Next::Done(())));
    }

    #[test]
    fn cancel_waits_submission() {
        let mut reserved = cancel();
        assert!(is_child(&reserved.next(row_result(row()))));
        let refused = reserved.next(reconciled(None));
        let Err(CallError(error)) = refused else {
            panic!("a work without a job cannot stop yet");
        };
        assert_eq!(error.kind, ErrorKind::Unavailable);
    }

    #[test]
    fn settle_skips_running() {
        let mut settle = SettleWork::new(5);
        assert!(is_child(&settle.first()));
        assert!(is_child(&settle.next(scanned(&[dispatched()]))));
        let running = settle.next(no_record());
        // A missing record settles as gone, so the row takes that summary.
        assert!(is_child(&running));
        assert_eq!(settle.next(updated()), Ok(Next::Done(0)));
    }

    #[test]
    fn settle_reconciles_reserved() {
        let mut settle = SettleWork::new(5);
        settle.first().unwrap();
        assert!(is_child(&settle.next(scanned(&[row()]))));
        assert_eq!(settle.next(reconciled(None)), Ok(Next::Done(0)));
    }

    fn deleted() -> IoResult {
        child_result(Ok::<(), StoreError>(()))
    }

    #[test]
    fn retire_keeps_live() {
        let config = fixtures::config(&fixtures::install(), json!({}));
        let mut retire = RetireWork::new(&config, 5);
        retire.first().unwrap();
        assert!(is_child(&retire.next(scanned(&[dispatched()]))));
        assert_eq!(retire.next(deleted()), Ok(Next::Done(0)));
    }

    #[test]
    fn retire_drops_gone() {
        let mut config = fixtures::config(&fixtures::install(), json!({}));
        config.installs.clear();
        let mut retire = RetireWork::new(&config, 5);
        retire.first().unwrap();
        assert!(is_child(&retire.next(scanned(&[dispatched()]))));
        assert!(is_child(&retire.next(no_record())));
        assert_eq!(retire.next(deleted()), Ok(Next::Done(1)));
    }

    #[test]
    fn retire_cancels_job() {
        let mut config = fixtures::config(&fixtures::install(), json!({}));
        config.installs.clear();
        let mut retire = RetireWork::new(&config, 5);
        retire.phase = RetirePhase::Cancel(
            Box::new(RetiredRow {
                key: key_of(&dispatched()),
                install_id: fixtures::install().install_id,
                row: dispatched(),
            }),
            job(),
        );
        let gone = Err::<CancelRequestOutcome, _>(JobMutationError::NotFound);
        assert!(is_child(&retire.next(child_result(gone))));
        assert_eq!(retire.next(deleted()), Ok(Next::Done(1)));
    }

    #[test]
    fn limit_refuses_start() {
        let error = submit_error(SubmitJobError::ActiveJobLimit { limit: 3 });
        assert_eq!(error.0.kind, ErrorKind::Limit);
    }
}
