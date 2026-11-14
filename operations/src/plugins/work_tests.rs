//! Tests work rows: adoption, conflicts and dispatch; settlement before the job is pruned; how
//! long settled rows stay; and the cancel a removed install's work gets first.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};

use super::*;
use crate::plugins::install::tests::Fixture;
use crate::plugins::worker::tests::installed;

const SOURCE: &str = "/realm/g/group/data/node/in/a.txt";
const TARGET: &str = "/realm/g/group/data/node/out/a.txt";

fn proposed(install: &PluginInstall, digest: u8) -> WorkRow {
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

fn job(nonce: u64) -> JobId {
    let handle = PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap();
    JobId::from_parts(1_700_000_000_000, handle, BucketId::new(0).unwrap(), nonce).unwrap()
}

#[tokio::test]
async fn repeated_call_adopts() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let identity = call(1);
    let first = proposed(&install, 1);
    let reserved = reserve(&fixture.ctx, (&install, &identity), first.clone()).await;
    assert_eq!(reserved, Ok((first.clone(), Reserve::Fresh)));
    // A rerun of the call mints new ids, but the stored ones answer.
    let again = reserve(&fixture.ctx, (&install, &identity), proposed(&install, 1)).await;
    assert_eq!(again, Ok((first.clone(), Reserve::Earlier)));
    let other = reserve(&fixture.ctx, (&install, &identity), proposed(&install, 2)).await;
    assert_eq!(other, Err(WorkError::Conflict));
    let next = reserve(&fixture.ctx, (&install, &call(3)), proposed(&install, 2)).await;
    assert_eq!(next.map(|(_, reserve)| reserve), Ok(Reserve::Fresh));
    let found = read_work(&fixture.ctx, install.install_id, first.work_id).await;
    assert_eq!(found, Ok(Some(first)));
    let missing = read_work(&fixture.ctx, install.install_id, Ulid::generate()).await;
    assert_eq!(missing, Ok(None));
}

#[tokio::test]
async fn reinstall_adopts_nothing() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let identity = call(1);
    let first = proposed(&install, 1);
    reserve(&fixture.ctx, (&install, &identity), first.clone())
        .await
        .unwrap();
    let mut reinstalled = install.clone();
    reinstalled.account = UserId::local(Ulid::generate(), install.account.realm_id);
    let other = proposed(&install, 2);
    let second = call(2);
    reserve(&fixture.ctx, (&install, &second), other.clone())
        .await
        .unwrap();
    let fresh = proposed(&reinstalled, 1);
    let reserved = reserve(&fixture.ctx, (&reinstalled, &identity), fresh.clone()).await;
    assert_eq!(reserved, Ok((fresh.clone(), Reserve::Fresh)));
    let former = index_key(install.install_id, first.work_id);
    let txn = Txn::direct(&fixture.ctx);
    let former = txn.read(PLUGIN_WORK_KEYSPACE, &former).await;
    assert_eq!(former, Ok(None), "the former index goes with its row");
    // Paging one row at a time lists each live row once, in order, and nothing of the former.
    let (mut listed, mut after) = (Vec::new(), None);
    loop {
        let page = list_work(&fixture.ctx, install.install_id, after, 1).await;
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
    reserve(&fixture.ctx, (&install, &identity), row.clone())
        .await
        .unwrap();
    let ids = (install.install_id, row.work_id);
    let first = dispatched(&fixture.ctx, ids, job(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (first.state, first.job),
        (WorkState::Dispatched, Some(job(1)))
    );
    let later = dispatched(&fixture.ctx, ids, job(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(later.job, Some(job(1)));
    let gone = dispatched(&fixture.ctx, (install.install_id, Ulid::generate()), job(3)).await;
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
    use crate::jobs::prune::prune_job_batch;
    use crate::jobs::store::insert_job;
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let row = proposed(&install, 1);
    let identity = call(1);
    reserve(&fixture.ctx, (&install, &identity), row.clone())
        .await
        .unwrap();
    let ids = (install.install_id, row.work_id);
    dispatched(&fixture.ctx, ids, job(1)).await.unwrap();
    let spec = spec_of(&install, &row);
    let mut job_record = record(spec.clone(), job(1), JobState::Running);
    insert_job(&fixture.ctx.storage_handle, &job_record)
        .await
        .unwrap();
    assert_eq!(settle_work(&fixture.ctx, 5).await, Ok(0));
    assert_eq!(holds_job(&fixture.ctx, &spec).await, Ok(true));
    // The job ended long ago, so its retention passed; the prune keeps it until the row settled.
    job_record.state = JobState::Failed;
    job_record.finished_at_ms = Some(1);
    insert_job(&fixture.ctx.storage_handle, &job_record)
        .await
        .unwrap();
    let pruned = prune_job_batch(&fixture.ctx).await.unwrap();
    assert_eq!(pruned.pruned, 0);
    assert_eq!(settle_work(&fixture.ctx, 5).await, Ok(1));
    let settled = read_work(&fixture.ctx, ids.0, ids.1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled.state, WorkState::Settled);
    assert_eq!(settled.settled_ms, Some(5));
    assert_eq!(
        settled.summary.map(|summary| summary.state),
        Some(FinalState::Failed)
    );
    assert_eq!(holds_job(&fixture.ctx, &spec).await, Ok(false));
    assert_eq!(settle_work(&fixture.ctx, 6).await, Ok(0));
    assert_eq!(prune_job_batch(&fixture.ctx).await.unwrap().pruned, 1);
    // The job record is gone; the row still answers the paths, also from its summary.
    let read = read_work(&fixture.ctx, ids.0, ids.1)
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
    use crate::jobs::prune::prune_job_batch;
    use crate::jobs::store::insert_job;
    use aruna_core::plugin_runs::RUN_RETENTION_MS;
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let ctx = &fixture.ctx;
    let mut rows = Vec::new();
    for seq in [1, 2] {
        let run = crate::plugins::runs::tests::queued(install.install_id, seq);
        crate::plugins::runs::admit(ctx, &run).await.unwrap();
        let identity = Identity::Call {
            run_id: run.run_id,
            ordinal: 0,
        };
        let row = proposed(&install, 1);
        reserve(ctx, (&install, &identity), row.clone())
            .await
            .unwrap();
        dispatched(ctx, (install.install_id, row.work_id), job(seq))
            .await
            .unwrap();
        let mut job_record = record(spec_of(&install, &row), job(seq), JobState::Succeeded);
        job_record.finished_at_ms = Some(1);
        insert_job(&ctx.storage_handle, &job_record).await.unwrap();
        rows.push((run.run_id, row));
    }
    assert_eq!(settle_work(ctx, 1).await, Ok(2));
    assert_eq!(prune_job_batch(ctx).await.unwrap().pruned, 2);
    let late = 1 + RUN_RETENTION_MS;
    assert_eq!(prune_work(ctx, late - 1).await, Ok(0));
    // Both runs still exist, so a rerun could ask for the work again.
    assert_eq!(prune_work(ctx, late).await, Ok(0));
    // The run prune removed the first run's record only.
    let key = run_key(install.install_id, rows[0].0);
    Txn::direct(ctx)
        .delete(PLUGIN_RUNS_KEYSPACE, vec![key])
        .await
        .unwrap();
    assert_eq!(prune_work(ctx, late).await, Ok(1));
    let read = |row: &WorkRow| read_work(ctx, install.install_id, row.work_id);
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
    use crate::jobs::store::{insert_job, read_job_record};
    use crate::plugins::bound::tests::edit_config;
    use aruna_core::plugin_installs::InstallState;
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let ctx = &fixture.ctx;
    let mut rows = Vec::new();
    // Calls 0, 1 and 2: running, settled and reserved work.
    for ordinal in 0..3 {
        let row = proposed(&install, 1);
        reserve(ctx, (&install, &call(ordinal)), row.clone())
            .await
            .unwrap();
        rows.push(row);
    }
    let ids = |row: &WorkRow| (install.install_id, row.work_id);
    dispatched(ctx, ids(&rows[0]), job(1)).await.unwrap();
    let job_record = record(spec_of(&install, &rows[0]), job(1), JobState::Queued);
    insert_job(&ctx.storage_handle, &job_record).await.unwrap();
    let settle = |row: &mut WorkRow| row.state = WorkState::Settled;
    update_work(ctx, ids(&rows[1]), settle).await.unwrap();
    let removing = |config: &mut RealmConfigDocument| {
        config.installs[0].state = InstallState::Removing;
    };
    edit_config(&fixture, removing).await;
    let config = crate::plugins::install::read_config(ctx, fixture.actor.realm_id);
    let config = config.await.unwrap();
    assert_eq!(retire_work(ctx, &config, 5).await, Ok(2));
    let kept = read_work(ctx, ids(&rows[0]).0, ids(&rows[0]).1).await;
    assert!(kept.unwrap().unwrap().cancel);
    let cancelled = read_job_record(&ctx.storage_handle, job(1), None).await;
    assert_eq!(cancelled.unwrap().unwrap().state, JobState::Cancelled);
    // The job settled; its summary is kept, then the row goes.
    assert_eq!(retire_work(ctx, &config, 6).await, Ok(0));
    assert_eq!(settle_work(ctx, 7).await, Ok(1));
    assert_eq!(retire_work(ctx, &config, 8).await, Ok(1));
    assert_eq!(
        read_work(ctx, ids(&rows[0]).0, ids(&rows[0]).1).await,
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
    use crate::jobs::store::insert_job;
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let row = proposed(&install, 1);
    let identity = call(1);
    reserve(&fixture.ctx, (&install, &identity), row.clone())
        .await
        .unwrap();
    let job_record = finished(&install, &row, job(1));
    insert_job(&fixture.ctx.storage_handle, &job_record)
        .await
        .unwrap();
    let spec = spec_of(&install, &row);
    assert_eq!(holds_job(&fixture.ctx, &spec).await, Ok(true));
    assert_eq!(settle_work(&fixture.ctx, 5).await, Ok(1));
    let settled = read_work(&fixture.ctx, install.install_id, row.work_id).await;
    let settled = settled.unwrap().unwrap();
    assert_eq!(
        (settled.state, settled.job),
        (WorkState::Settled, Some(job(1)))
    );
    assert_eq!(holds_job(&fixture.ctx, &spec).await, Ok(false));
}

#[tokio::test]
async fn removal_keeps_finished() {
    // A removed install's reserved work whose job already finished keeps the job, settles, and
    // only then goes.
    use crate::jobs::store::insert_job;
    use crate::plugins::bound::tests::edit_config;
    use aruna_core::plugin_installs::InstallState;
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let ctx = &fixture.ctx;
    let row = proposed(&install, 1);
    let identity = call(1);
    reserve(ctx, (&install, &identity), row.clone())
        .await
        .unwrap();
    insert_job(&ctx.storage_handle, &finished(&install, &row, job(1)))
        .await
        .unwrap();
    let removing = |config: &mut RealmConfigDocument| {
        config.installs[0].state = InstallState::Removing;
    };
    edit_config(&fixture, removing).await;
    let config = crate::plugins::install::read_config(ctx, fixture.actor.realm_id);
    let config = config.await.unwrap();
    assert_eq!(retire_work(ctx, &config, 5).await, Ok(0));
    let kept = read_work(ctx, install.install_id, row.work_id).await;
    let kept = kept.unwrap().unwrap();
    assert_eq!(
        (kept.state, kept.job),
        (WorkState::Dispatched, Some(job(1)))
    );
    assert_eq!(settle_work(ctx, 6).await, Ok(1));
    assert_eq!(retire_work(ctx, &config, 7).await, Ok(1));
}
