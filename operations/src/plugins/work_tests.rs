//! Tests work rows: adoption, conflicts and dispatch; settlement before the job is pruned; how
//! long settled rows stay; and the cancel a removed install's work gets first.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};

use super::*;
use crate::driver::{DriverContext, drive};
use crate::jobs::executor::PayloadHandler;
use crate::plugin_host::jobs::{retire_work, settle_work};
use crate::plugin_host::plugin_work::PluginWorkHandler;
use crate::plugin_host::worker::tests::installed;
use crate::plugins::install::tests::Fixture;
use crate::plugins::rows::tests::{page_result, values_result};
use crate::plugins::rows::{page_call, read_call, read_rows, write_rows};
use crate::plugins::store::Txn;
use aruna_core::structs::identity::realm::RealmConfigDocument;

const SOURCE: &str = "/realm/g/group/data/node/in/a.txt";
const TARGET: &str = "/realm/g/group/data/node/out/a.txt";

pub(crate) fn proposed(install: &PluginInstall, digest: u8) -> WorkRow {
    let mut row = WorkRow::reserved(install, WorkKind::Copy, Ulid::generate(), 1);
    row.source = Some(SOURCE.into());
    row.target = Some(TARGET.into());
    row.digest = [digest; 32];
    row.version_id = Some(Ulid::generate());
    row
}

/// The identity of call `ordinal` of one fixed run.
fn call(ordinal: u32) -> Identity {
    Identity::Call {
        run_id: Ulid::from_parts(9, 9),
        ordinal,
    }
}

/// Changes the row of work `ids` as a test needs; `None` when it is gone.
pub(crate) async fn edit_work(
    ctx: &DriverContext,
    (install_id, work_id): (Ulid, Ulid),
    change: impl FnOnce(&mut WorkRow),
) -> Result<Option<WorkRow>, StoreError> {
    let index = vec![(PLUGIN_WORK_KEYSPACE, index_key(install_id, work_id))];
    let [key] = <[_; 1]>::try_from(drive(read_rows(index), ctx).await?).unwrap();
    let Some(key) = key else {
        return Ok(None);
    };
    let read = vec![(PLUGIN_WORK_KEYSPACE, key.clone())];
    let [value] = <[_; 1]>::try_from(drive(read_rows(read), ctx).await?).unwrap();
    let Some(value) = value else {
        return Ok(None);
    };
    let mut row: WorkRow = decode(&value)?;
    change(&mut row);
    drive(write_rows(vec![row_of(key, &row)?]), ctx).await?;
    Ok(Some(row))
}

fn job(nonce: u64) -> JobId {
    let handle = PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap();
    JobId::from_parts(1_700_000_000_000, handle, BucketId::new(0).unwrap(), nonce).unwrap()
}

/// The job handlers whose holds the prune asks: the plugin work one.
fn plugin_handlers() -> [std::sync::Arc<dyn PayloadHandler>; 1] {
    [std::sync::Arc::new(PluginWorkHandler)]
}

#[tokio::test]
async fn repeated_call_adopts() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let identity = call(1);
    let first = proposed(&install, 1);
    let reserved = drive(
        reserve((&install, &identity), first.clone(), None).unwrap(),
        &fixture.ctx,
    )
    .await;
    assert_eq!(reserved, Ok((first.clone(), Reserve::Fresh)));
    // A rerun of the call mints new ids, but the stored ones answer.
    let again = drive(
        reserve((&install, &identity), proposed(&install, 1), None).unwrap(),
        &fixture.ctx,
    )
    .await;
    assert_eq!(again, Ok((first.clone(), Reserve::Earlier)));
    let other = drive(
        reserve((&install, &identity), proposed(&install, 2), None).unwrap(),
        &fixture.ctx,
    )
    .await;
    assert_eq!(other, Err(WorkError::Conflict));
    let next = drive(
        reserve((&install, &call(3)), proposed(&install, 2), None).unwrap(),
        &fixture.ctx,
    )
    .await;
    assert_eq!(next.map(|(_, reserve)| reserve), Ok(Reserve::Fresh));
    let found = drive(read_work(install.install_id, first.work_id), &fixture.ctx).await;
    assert_eq!(found, Ok(Some(first)));
    let missing = drive(
        read_work(install.install_id, Ulid::generate()),
        &fixture.ctx,
    )
    .await;
    assert_eq!(missing, Ok(None));
}

#[tokio::test]
async fn reinstall_adopts_nothing() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let identity = call(1);
    let first = proposed(&install, 1);
    drive(
        reserve((&install, &identity), first.clone(), None).unwrap(),
        &fixture.ctx,
    )
    .await
    .unwrap();
    let mut reinstalled = install.clone();
    reinstalled.account = UserId::local(Ulid::generate(), install.account.realm_id);
    let other = proposed(&install, 2);
    let second = call(2);
    drive(
        reserve((&install, &second), other.clone(), None).unwrap(),
        &fixture.ctx,
    )
    .await
    .unwrap();
    let fresh = proposed(&reinstalled, 1);
    let reserved = drive(
        reserve((&reinstalled, &identity), fresh.clone(), None).unwrap(),
        &fixture.ctx,
    )
    .await;
    assert_eq!(reserved, Ok((fresh.clone(), Reserve::Fresh)));
    let former = index_key(install.install_id, first.work_id);
    let txn = Txn::direct(&fixture.ctx);
    let former = txn.read(PLUGIN_WORK_KEYSPACE, &former).await;
    assert_eq!(former, Ok(None), "the former index goes with its row");
    // Paging one row at a time lists each live row once, in order, and nothing of the former.
    let (mut listed, mut after) = (Vec::new(), None);
    loop {
        let page = drive(list_work(install.install_id, after, 1), &fixture.ctx).await;
        let (rows, next) = page.unwrap();
        listed.extend(rows.into_iter().map(|row| row.work_id));
        match next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    // Rows are listed by work id, which orders ids of one millisecond at random.
    let mut live = [other.work_id, fresh.work_id];
    live.sort();
    assert_eq!(listed, live);
}

#[tokio::test]
async fn dispatch_records_once() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let row = proposed(&install, 1);
    let identity = call(1);
    drive(
        reserve((&install, &identity), row.clone(), None).unwrap(),
        &fixture.ctx,
    )
    .await
    .unwrap();
    let ids = (install.install_id, row.work_id);
    let first = drive(
        update_work(ids, WorkChange::Dispatched(job(1))),
        &fixture.ctx,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        (first.state, first.job),
        (WorkState::Dispatched, Some(job(1)))
    );
    let later = drive(
        update_work(ids, WorkChange::Dispatched(job(2))),
        &fixture.ctx,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(later.job, Some(job(1)));
    let gone = drive(
        update_work(
            (install.install_id, Ulid::generate()),
            WorkChange::Dispatched(job(3)),
        ),
        &fixture.ctx,
    )
    .await;
    assert_eq!(gone, Ok(None));
}

fn record(spec: PluginWorkSpec, job: JobId, state: JobState) -> JobRecord {
    let account = UserId::local(Ulid::from_parts(1, 1), spec.realm_id);
    let node = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
    let payload = aruna_core::structs::execution::job::JobPayload::PluginWork(spec);
    let mut record = JobRecord::new(job, payload, account, node, 1, 1, None);
    record.state = state;
    record
}

fn spec_of(install: &PluginInstall, row: &WorkRow) -> PluginWorkSpec {
    PluginWorkSpec {
        realm_id: install.account.realm_id,
        install_id: install.install_id,
        work_id: row.work_id,
        group_id: install.group_id,
        action: aruna_core::structs::execution::job::PluginWorkAction::Copy {
            source: "a".into(),
            source_version: None,
            target: "b".into(),
            version_id: Ulid::generate(),
        },
    }
}

#[tokio::test]
async fn summary_follows_job() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let row = proposed(&install, 1);
    let mut job_record = record(spec_of(&install, &row), job(1), JobState::Running);
    assert_eq!(summary_of(Some(&job_record)), None);
    job_record.state = JobState::Indeterminate;
    assert_eq!(summary_of(Some(&job_record)), None);
    job_record.locally_exhausted = true;
    let unknown = summary_of(Some(&job_record)).unwrap();
    assert_eq!(unknown.state, FinalState::Unknown);
    job_record.state = JobState::Succeeded;
    let version = Ulid::generate();
    job_record.result = Some(JobResultPayload::PluginWork {
        bytes: 12,
        version_id: Some(version),
    });
    let done = summary_of(Some(&job_record)).unwrap();
    assert_eq!(
        (done.state, done.bytes, done.version_id),
        (FinalState::Succeeded, Some(12), Some(version))
    );
    let gone = summary_of(None).unwrap();
    assert_eq!(gone.state, FinalState::Unknown);
}

#[tokio::test]
async fn settles_before_pruning() {
    // A finished job's row keeps its summary; until then the job record is held.
    use crate::jobs::prune::tests::prune_job_batch;
    use crate::jobs::store::query::insert_job;
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let row = proposed(&install, 1);
    let identity = call(1);
    drive(
        reserve((&install, &identity), row.clone(), None).unwrap(),
        &fixture.ctx,
    )
    .await
    .unwrap();
    let ids = (install.install_id, row.work_id);
    drive(
        update_work(ids, WorkChange::Dispatched(job(1))),
        &fixture.ctx,
    )
    .await
    .unwrap();
    let spec = spec_of(&install, &row);
    let mut job_record = record(spec.clone(), job(1), JobState::Running);
    insert_job(&fixture.ctx, &job_record).await.unwrap();
    assert_eq!(settle_work(&fixture.ctx, 5).await, Ok(0));
    assert_eq!(drive(holds_job(&spec), &fixture.ctx).await, Ok(true));
    // The job ended long ago, so its retention passed; the prune keeps it until the row settled.
    job_record.state = JobState::Failed;
    job_record.finished_at_ms = Some(1);
    insert_job(&fixture.ctx, &job_record).await.unwrap();
    let pruned = prune_job_batch(&fixture.ctx, &plugin_handlers())
        .await
        .unwrap();
    assert_eq!(pruned.pruned, 0);
    assert_eq!(settle_work(&fixture.ctx, 5).await, Ok(1));
    let settled = drive(read_work(ids.0, ids.1), &fixture.ctx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.state, WorkState::Settled);
    assert_eq!(settled.settled_ms, Some(5));
    assert_eq!(
        settled.summary.map(|summary| summary.state),
        Some(FinalState::Failed)
    );
    assert_eq!(drive(holds_job(&spec), &fixture.ctx).await, Ok(false));
    assert_eq!(settle_work(&fixture.ctx, 6).await, Ok(0));
    let pruned = prune_job_batch(&fixture.ctx, &plugin_handlers())
        .await
        .unwrap();
    assert_eq!(pruned.pruned, 1);
    // The job record is gone; the row still answers the paths, also from its summary.
    let read = drive(read_work(ids.0, ids.1), &fixture.ctx)
        .await
        .unwrap()
        .unwrap();
    let summary = read.summary.unwrap();
    assert_eq!(
        (read.source.as_deref(), read.target.as_deref()),
        (Some(SOURCE), Some(TARGET))
    );
    assert_eq!(
        (summary.source.as_deref(), summary.target.as_deref()),
        (Some(SOURCE), Some(TARGET))
    );
}

#[tokio::test]
async fn prunes_settled_rows() {
    // A settled row goes once it aged and its run is pruned, since no rerun can ask for it; until
    // then it answers its paths, also after its job record was pruned.
    use crate::jobs::prune::tests::prune_job_batch;
    use crate::jobs::store::query::insert_job;
    use aruna_core::plugin_runs::RUN_RETENTION_MS;
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let ctx = &fixture.ctx;
    let mut rows = Vec::new();
    for seq in [1, 2] {
        let run = crate::plugins::runs::tests::queued(install.install_id, seq);
        drive(crate::plugins::runs::admit(run.clone()), ctx)
            .await
            .unwrap();
        let identity = Identity::Call {
            run_id: run.run_id,
            ordinal: 0,
        };
        let row = proposed(&install, 1);
        drive(
            reserve((&install, &identity), row.clone(), None).unwrap(),
            ctx,
        )
        .await
        .unwrap();
        drive(
            update_work(
                (install.install_id, row.work_id),
                WorkChange::Dispatched(job(seq)),
            ),
            ctx,
        )
        .await
        .unwrap();
        let mut job_record = record(spec_of(&install, &row), job(seq), JobState::Succeeded);
        job_record.finished_at_ms = Some(1);
        insert_job(&ctx, &job_record).await.unwrap();
        rows.push((run.run_id, row));
    }
    assert_eq!(settle_work(ctx, 1).await, Ok(2));
    let pruned = prune_job_batch(ctx, &plugin_handlers()).await.unwrap();
    assert_eq!(pruned.pruned, 2);
    // Delivered as `job_finished`; an undelivered row is never pruned.
    for (_, row) in &rows {
        let delivered = edit_work(ctx, (install.install_id, row.work_id), |row| {
            row.notified = true;
        });
        delivered.await.unwrap();
    }
    let late = 1 + RUN_RETENTION_MS;
    assert_eq!(drive(prune_work(late - 1), ctx).await, Ok(0));
    // Both runs still exist, so a rerun could ask for the work again.
    assert_eq!(drive(prune_work(late), ctx).await, Ok(0));
    // The run prune removed the first run's record only.
    let key = run_key(install.install_id, rows[0].0);
    Txn::direct(ctx)
        .delete(PLUGIN_RUNS_KEYSPACE, vec![key])
        .await
        .unwrap();
    assert_eq!(drive(prune_work(late), ctx).await, Ok(1));
    let read = |row: &WorkRow| drive(read_work(install.install_id, row.work_id), ctx);
    assert_eq!(read(&rows[0].1).await, Ok(None));
    let kept = read(&rows[1].1).await.unwrap().unwrap();
    let summary = kept.summary.unwrap();
    assert_eq!(
        (summary.source.as_deref(), summary.target.as_deref()),
        (Some(SOURCE), Some(TARGET))
    );
}

#[tokio::test]
async fn removal_cancels_first() {
    // A removed install's running work is cancelled and its row stays until the job settled;
    // settled rows and work that never got a job go at once.
    use crate::jobs::store::query::insert_job;
    use crate::jobs::store::state::read_job_record;
    use crate::plugins::bound::tests::edit_config;
    use aruna_core::plugin_installs::InstallState;
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let ctx = &fixture.ctx;
    let mut rows = Vec::new();
    // Calls 0, 1 and 2: running, settled and reserved work.
    for ordinal in 0..3 {
        let row = proposed(&install, 1);
        drive(
            reserve((&install, &call(ordinal)), row.clone(), None).unwrap(),
            ctx,
        )
        .await
        .unwrap();
        rows.push(row);
    }
    let ids = |row: &WorkRow| (install.install_id, row.work_id);
    drive(
        update_work(ids(&rows[0]), WorkChange::Dispatched(job(1))),
        ctx,
    )
    .await
    .unwrap();
    let job_record = record(spec_of(&install, &rows[0]), job(1), JobState::Queued);
    insert_job(&ctx, &job_record).await.unwrap();
    let settle = |row: &mut WorkRow| row.state = WorkState::Settled;
    edit_work(ctx, ids(&rows[1]), settle).await.unwrap();
    let removing = |config: &mut RealmConfigDocument| {
        config.installs[0].state = InstallState::Removing;
    };
    edit_config(&fixture, removing).await;
    let config = crate::plugins::calls::read_config(ctx, fixture.actor.realm_id);
    let config = config.await.unwrap();
    assert_eq!(retire_work(ctx, &config, 5).await, Ok(2));
    let kept = drive(read_work(ids(&rows[0]).0, ids(&rows[0]).1), ctx).await;
    assert!(kept.unwrap().unwrap().cancel);
    let cancelled = read_job_record(&ctx, job(1), None).await;
    assert_eq!(cancelled.unwrap().unwrap().state, JobState::Cancelled);
    // The job settled; its summary is kept, then the row goes.
    assert_eq!(retire_work(ctx, &config, 6).await, Ok(0));
    assert_eq!(settle_work(ctx, 7).await, Ok(1));
    assert_eq!(retire_work(ctx, &config, 8).await, Ok(1));
    assert_eq!(
        drive(read_work(ids(&rows[0]).0, ids(&rows[0]).1), ctx).await,
        Ok(None)
    );
}

fn finished(install: &PluginInstall, row: &WorkRow, job_id: JobId) -> JobRecord {
    let mut job_record = record(spec_of(install, row), job_id, JobState::Succeeded);
    job_record.created_by = install.account;
    job_record.dedup_key = Some(dedup_key(row.work_id));
    job_record
}

#[tokio::test]
async fn reserved_work_settles() {
    // The submission committed but its answer was lost, so the row stayed reserved; settling
    // finds the job by its dedup key without the call being repeated.
    use crate::jobs::store::query::insert_job;
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let row = proposed(&install, 1);
    let identity = call(1);
    drive(
        reserve((&install, &identity), row.clone(), None).unwrap(),
        &fixture.ctx,
    )
    .await
    .unwrap();
    let job_record = finished(&install, &row, job(1));
    insert_job(&fixture.ctx, &job_record).await.unwrap();
    let spec = spec_of(&install, &row);
    assert_eq!(drive(holds_job(&spec), &fixture.ctx).await, Ok(true));
    assert_eq!(settle_work(&fixture.ctx, 5).await, Ok(1));
    let settled = drive(read_work(install.install_id, row.work_id), &fixture.ctx).await;
    let settled = settled.unwrap().unwrap();
    assert_eq!(
        (settled.state, settled.job),
        (WorkState::Settled, Some(job(1)))
    );
    assert_eq!(drive(holds_job(&spec), &fixture.ctx).await, Ok(false));
}

#[tokio::test]
async fn removal_keeps_finished() {
    // A removed install's reserved work whose job already finished keeps the job, settles, and
    // only then goes.
    use crate::jobs::store::query::insert_job;
    use crate::plugins::bound::tests::edit_config;
    use aruna_core::plugin_installs::InstallState;
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let ctx = &fixture.ctx;
    let row = proposed(&install, 1);
    let identity = call(1);
    drive(
        reserve((&install, &identity), row.clone(), None).unwrap(),
        ctx,
    )
    .await
    .unwrap();
    insert_job(&ctx, &finished(&install, &row, job(1)))
        .await
        .unwrap();
    let removing = |config: &mut RealmConfigDocument| {
        config.installs[0].state = InstallState::Removing;
    };
    edit_config(&fixture, removing).await;
    let config = crate::plugins::calls::read_config(ctx, fixture.actor.realm_id);
    let config = config.await.unwrap();
    assert_eq!(retire_work(ctx, &config, 5).await, Ok(0));
    let kept = drive(read_work(install.install_id, row.work_id), ctx).await;
    let kept = kept.unwrap().unwrap();
    assert_eq!(
        (kept.state, kept.job),
        (WorkState::Dispatched, Some(job(1)))
    );
    assert_eq!(settle_work(ctx, 6).await, Ok(1));
    assert_eq!(retire_work(ctx, &config, 7).await, Ok(1));
}

#[tokio::test]
async fn boundary_sends_once() {
    // A request that may not go twice is sent by the first attempt only; one that finds the
    // boundary left open stays undecided, one that finds the answer decides by it. A request
    // that may go twice sends again until it has an answer.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let row = proposed(&install, 1);
    drive(
        reserve((&install, &call(1)), row.clone(), None).unwrap(),
        &fixture.ctx,
    )
    .await
    .unwrap();
    let ids = (install.install_id, row.work_id);
    assert_eq!(
        drive(begin_send(ids, false), &fixture.ctx).await,
        Ok(Some(Send::Now))
    );
    assert_eq!(
        drive(begin_send(ids, false), &fixture.ctx).await,
        Ok(Some(Send::Undecided))
    );
    assert_eq!(
        drive(begin_send(ids, true), &fixture.ctx).await,
        Ok(Some(Send::Now))
    );
    let stored = drive(read_work(ids.0, ids.1), &fixture.ctx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.boundary, Some(SendBoundary::Sending { attempt: 2 }));
    let reply = Some(r#"{"status":201}"#.to_string());
    let sent = WorkChange::Sent {
        status: 201,
        reply: reply.clone(),
        bytes: 8,
        version: Ulid::nil(),
    };
    drive(update_work(ids, sent), &fixture.ctx).await.unwrap();
    let answered = Send::Answered {
        status: 201,
        reply: reply.clone(),
        bytes: 8,
        version: Ulid::nil(),
    };
    assert_eq!(
        drive(begin_send(ids, false), &fixture.ctx).await,
        Ok(Some(answered))
    );
    let gone = (install.install_id, Ulid::generate());
    assert_eq!(drive(begin_send(gone, false), &fixture.ctx).await, Ok(None));
}

#[tokio::test]
async fn settles_answer() {
    // A settled upload keeps its answer, so its status outlives the job record.
    use crate::jobs::store::query::insert_job;
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let row = proposed(&install, 1);
    drive(
        reserve((&install, &call(1)), row.clone(), None).unwrap(),
        &fixture.ctx,
    )
    .await
    .unwrap();
    let ids = (install.install_id, row.work_id);
    drive(
        update_work(ids, WorkChange::Dispatched(job(1))),
        &fixture.ctx,
    )
    .await
    .unwrap();
    let reply = Some(r#"{"status":201}"#.to_string());
    let sent = WorkChange::Sent {
        status: 201,
        reply: reply.clone(),
        bytes: 8,
        version: Ulid::nil(),
    };
    drive(update_work(ids, sent), &fixture.ctx).await.unwrap();
    let mut job_record = record(spec_of(&install, &row), job(1), JobState::Succeeded);
    job_record.finished_at_ms = Some(1);
    insert_job(&fixture.ctx, &job_record).await.unwrap();
    assert_eq!(settle_work(&fixture.ctx, 5).await, Ok(1));
    let settled = drive(read_work(ids.0, ids.1), &fixture.ctx)
        .await
        .unwrap()
        .unwrap();
    let summary = settled.summary.unwrap();
    assert_eq!((summary.status, summary.reply), (Some(201), reply));
}

/// A work row's index answer and row answer for `row` under `key`.
fn found(key: &[u8], row: &WorkRow) -> [IoResult; 2] {
    [
        values_result(vec![Some(key.to_vec())]),
        values_result(vec![Some(encode(row).unwrap())]),
    ]
}

#[test]
fn lookup_follows_index() {
    let install_id = Ulid::from_parts(1, 1);
    let mut row = WorkRow::reserved(&sample_install(install_id), WorkKind::Copy, Ulid::nil(), 1);
    row.work_id = Ulid::from_parts(2, 2);
    let mut lookup = WorkLookup::new(install_id, row.work_id);
    let index = index_key(install_id, row.work_id);
    assert_eq!(lookup.ask(), read_call(vec![(PLUGIN_WORK_KEYSPACE, index)]));
    let [by_index, by_key] = found(b"key", &row);
    let next = lookup.take(by_index).unwrap();
    assert_eq!(
        next,
        Found::Ask(read_call(vec![(PLUGIN_WORK_KEYSPACE, b"key".to_vec())]))
    );
    assert_eq!(
        lookup.take(by_key).unwrap(),
        Found::Row(Some((b"key".to_vec(), row.clone())))
    );
    // A missing index, and a row of another work under the key, find nothing.
    let mut gone = WorkLookup::new(install_id, row.work_id);
    assert_eq!(
        gone.take(values_result(vec![None])).unwrap(),
        Found::Row(None)
    );
    let mut other = WorkLookup::new(install_id, Ulid::from_parts(3, 3));
    let [by_index, by_key] = found(b"key", &row);
    other.take(by_index).unwrap();
    assert_eq!(other.take(by_key).unwrap(), Found::Row(None));
    assert!(other.take(IoResult::Done).is_err());
}

#[test]
fn send_boundary_decides() {
    let install_id = Ulid::from_parts(1, 1);
    let mut row = WorkRow::reserved(
        &sample_install(install_id),
        WorkKind::Upload,
        Ulid::nil(),
        1,
    );
    row.boundary = Some(SendBoundary::Sending { attempt: 1 });
    let ids = (install_id, row.work_id);
    // A begun send is undecided for a request that may not go twice.
    let mut once = BeginSend {
        lookup: WorkLookup::new(ids.0, ids.1),
        again: false,
        written: false,
    };
    let [by_index, by_key] = found(b"key", &row);
    once.first().unwrap();
    once.next(by_index).unwrap();
    assert_eq!(
        once.next(by_key).unwrap(),
        Next::Done(Some(Send::Undecided))
    );
    // One that may go twice moves the boundary to the next attempt first.
    let mut again = BeginSend {
        lookup: WorkLookup::new(ids.0, ids.1),
        again: true,
        written: false,
    };
    let [by_index, by_key] = found(b"key", &row);
    again.next(by_index).unwrap();
    let Next::Call(IoCall::Write(rows)) = again.next(by_key).unwrap() else {
        panic!("the boundary is written before sending");
    };
    let written: WorkRow = decode(&rows[0].2).unwrap();
    assert_eq!(written.boundary, Some(SendBoundary::Sending { attempt: 2 }));
    assert_eq!(
        again.next(IoResult::Done).unwrap(),
        Next::Done(Some(Send::Now))
    );
}

#[test]
fn reserve_decisions() {
    let install = sample_install(Ulid::from_parts(1, 1));
    let row = proposed(&install, 1);
    let reserve_with = |row: WorkRow| reserve((&install, &call(0)), row, None).unwrap();
    // A row of the same account and digest is adopted without a write.
    let mut adopt = reserve_with(row.clone());
    assert!(matches!(
        adopt.first().unwrap(),
        Next::Call(IoCall::Read(_))
    ));
    let mut plan = ReserveWork {
        key: row_key(install.install_id, &call(0)).unwrap(),
        install_id: install.install_id,
        account: install.account,
        proposed: Some(Box::new(proposed(&install, 1))),
        mark: None,
        phase: ReservePhase::Reading,
    };
    let stored = values_result(vec![Some(encode(&row).unwrap())]);
    assert_eq!(
        plan.next(stored).unwrap(),
        Next::Done((row.clone(), Reserve::Earlier))
    );
    let mut conflict = ReserveWork {
        proposed: Some(Box::new(proposed(&install, 2))),
        phase: ReservePhase::Reading,
        ..plan
    };
    let stored = values_result(vec![Some(encode(&row).unwrap())]);
    assert_eq!(conflict.next(stored), Err(WorkError::Conflict));
    // A new row writes its index and itself.
    let mut fresh = ReserveWork {
        proposed: Some(Box::new(row.clone())),
        phase: ReservePhase::Reading,
        ..conflict
    };
    let Next::Call(IoCall::Write(rows)) = fresh.next(values_result(vec![None])).unwrap() else {
        panic!("a new row is written");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(
        fresh.next(IoResult::Done).unwrap(),
        Next::Done((row, Reserve::Fresh))
    );
}

pub(crate) fn sample_install(install_id: Ulid) -> PluginInstall {
    let realm_id = aruna_core::structs::identity::realm::RealmId::from_bytes([1u8; 32]);
    let node = iroh::SecretKey::from_bytes(&[3u8; 32]).public();
    PluginInstall {
        install_id,
        group_id: Ulid::from_parts(1, 9),
        plugin: "copier".into(),
        version: "0.1.0".into(),
        digest: format!("sha256:{}", "ab".repeat(32)),
        state: aruna_core::plugin_installs::InstallState::Active,
        paused: false,
        settings: "{}".into(),
        grants: Default::default(),
        account: UserId::local(install_id, realm_id),
        role_id: install_id,
        receiver: node,
        granted_by: UserId::local(Ulid::from_parts(1, 3), realm_id),
        request: String::new(),
        revision: 1,
        active_since_ms: Some(5),
        failed: None,
        prerequisites: Vec::new(),
        activated_by: Some(node),
        widened: None,
        secrets: Default::default(),
    }
}

#[test]
fn change_applies_once() {
    let install_id = Ulid::from_parts(1, 1);
    let row = WorkRow::reserved(&sample_install(install_id), WorkKind::Copy, Ulid::nil(), 1);
    let mut plan = UpdateWork {
        lookup: WorkLookup::new(install_id, row.work_id),
        change: Some(WorkChange::Dispatched(job(1))),
        written: None,
    };
    plan.first().unwrap();
    let [by_index, by_key] = found(b"key", &row);
    plan.next(by_index).unwrap();
    let Next::Call(IoCall::Write(rows)) = plan.next(by_key).unwrap() else {
        panic!("the changed row is written");
    };
    let written: WorkRow = decode(&rows[0].2).unwrap();
    assert_eq!(
        (written.state, written.job),
        (WorkState::Dispatched, Some(job(1)))
    );
    assert_eq!(
        plan.next(IoResult::Done).unwrap(),
        Next::Done(Some(written))
    );
    // A gone row is not changed.
    let mut gone = UpdateWork {
        lookup: WorkLookup::new(install_id, row.work_id),
        change: Some(WorkChange::Reopened),
        written: None,
    };
    assert_eq!(
        gone.next(values_result(vec![None])).unwrap(),
        Next::Done(None)
    );
    // A later state keeps its job.
    let mut settled = row.clone();
    settled.state = WorkState::Settled;
    WorkChange::Dispatched(job(2)).apply(&mut settled);
    assert_eq!((settled.state, settled.job), (WorkState::Settled, None));
}

#[test]
fn listed_by_index() {
    let install_id = Ulid::from_parts(1, 1);
    let row = WorkRow::reserved(&sample_install(install_id), WorkKind::Copy, Ulid::nil(), 1);
    let mut plan = ListWork {
        page: Some(page_call(PLUGIN_WORK_KEYSPACE, Vec::new(), None, 2)),
        indexes: Vec::new(),
        next: None,
    };
    plan.first().unwrap();
    let index = index_key(install_id, row.work_id);
    let other = index_key(install_id, Ulid::from_parts(9, 9));
    let page = vec![(index.clone(), b"k1".to_vec()), (other, b"k2".to_vec())];
    let Next::Call(IoCall::Read(reads)) = plan.next(page_result(page, Some(Vec::new()))).unwrap()
    else {
        panic!("the rows of the index page are read");
    };
    assert_eq!(reads.len(), 2);
    // A row another work replaced under the key, and a gone one, are left out.
    let values = vec![Some(encode(&row).unwrap()), Some(encode(&row).unwrap())];
    let Next::Done((works, next)) = plan.next(values_result(values)).unwrap() else {
        panic!("the page ends the listing");
    };
    assert_eq!(works, [row]);
    assert_eq!(next, Some(Ulid::from_parts(9, 9)));
}
