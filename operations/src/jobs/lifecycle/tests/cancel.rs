//! Tests that cancelling a family stops local launches and leaves a running execution alone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::effects::{JobRecordFrame, LaunchFrame};
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::JOB_RESERVATION_KEYSPACE;
use aruna_core::structs::execution::job::{
    JobFamilyRecord, JobId, JobRecordKind, JobState, PhysicalExecutionState,
};
use aruna_core::structs::identity::auth::AuthContext;

use super::admission_race::{config, envelope, rows, seed};
use super::terminal::{node_context, physical, reserve_execution, seed_family};
use crate::driver::{DriverContext, drive};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::cancel::cancel_family;
use crate::jobs::lifecycle::target::{admit_launch, commit_receipt, commit_with};
use crate::jobs::records::transport::serve_job_record;
use crate::jobs::records::{
    Admission, AppendRecordConfig, AppendRecordOperation, RecordOrigin, load_kind_complete,
};
use crate::jobs::store::read_job_record;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::tests::records::{Family, REALM, user};

fn auth(user_id: UserId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id: REALM,
        path_restrictions: None,
        session: None,
    }
}

/// Whether the local physical row of the seeded execution is flagged.
async fn flagged(ctx: &DriverContext) -> bool {
    read_job_record(&ctx.storage_handle, physical(), None)
        .await
        .expect("physical row read")
        .expect("physical row exists")
        .cancel_requested
}

#[tokio::test]
async fn cancel_stops_local() {
    // The execution runs on the node that publishes the cancellation, so no
    // network cancel can reach it: its own row has to be flagged here.
    let family = Family::new([41u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.target).await;
    let receipt = seed_family(&ctx, &family).await;
    reserve_execution(&ctx, &family, &receipt).await;
    let spec = family.spec();

    cancel_family(&ctx, &auth(user()), spec.job_id, None)
        .await
        .expect("alias names a family")
        .expect("cancellation publishes");

    let record = read_job_record(&ctx.storage_handle, physical(), None)
        .await
        .expect("physical row read")
        .expect("physical row exists");
    assert!(record.cancel_requested);
    assert_eq!(record.state, JobState::Running);
}

#[tokio::test]
async fn repeated_cancel_quiet() {
    // A second cancellation must leave the already flagged row untouched.
    let family = Family::new([42u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.target).await;
    let receipt = seed_family(&ctx, &family).await;
    reserve_execution(&ctx, &family, &receipt).await;
    let spec = family.spec();

    cancel_family(&ctx, &auth(user()), spec.job_id, None)
        .await
        .expect("alias names a family")
        .expect("cancellation publishes");
    let first = read_job_record(&ctx.storage_handle, physical(), None)
        .await
        .expect("physical row read")
        .expect("physical row exists");
    cancel_family(&ctx, &auth(user()), spec.job_id, None)
        .await
        .expect("alias names a family")
        .expect("cancellation publishes");

    let second = read_job_record(&ctx.storage_handle, physical(), None)
        .await
        .expect("physical row read")
        .expect("physical row exists");
    assert_eq!(first, second);
    assert!(second.cancel_requested);
}

/// Delivers a signed cancel record the way replication does.
async fn admit_cancel(ctx: &Arc<DriverContext>, family: &Family) {
    let record = JobRecordFrame::new(family.sign(
        &family.holder,
        JobFamilyRecord::Cancel(family.cancel(&family.spec())),
    ))
    .expect("bounded record");

    let reply = serve_job_record(
        ctx,
        family.holder.public(),
        MetadataTransportMessage::ForwardJobRecord {
            placement: family.placement,
            record: Box::new(record),
        },
    )
    .await;

    assert!(matches!(
        reply,
        MetadataTransportMessage::ForwardedJobRecord { result: Ok(()) }
    ));
}

#[tokio::test]
async fn admitted_cancel_stops() {
    // The record arrives after this node already admitted the execution, so
    // admission itself has to apply it to the row that is already running.
    let family = Family::new([43u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.target).await;
    let receipt = seed_family(&ctx, &family).await;
    reserve_execution(&ctx, &family, &receipt).await;
    assert!(!flagged(&ctx).await);
    let ctx = Arc::new(ctx);

    let reply = serve_job_record(
        &ctx,
        family.holder.public(),
        MetadataTransportMessage::ForwardJobRecord {
            placement: family.placement,
            record: Box::new(
                JobRecordFrame::new(family.sign(
                    &family.holder,
                    JobFamilyRecord::Spec(Box::new(family.spec())),
                ))
                .unwrap(),
            ),
        },
    )
    .await;
    assert!(matches!(
        reply,
        MetadataTransportMessage::ForwardedJobRecord { result: Ok(()) }
    ));
    assert!(!flagged(&ctx).await);

    admit_cancel(&ctx, &family).await;

    assert!(flagged(&ctx).await);
}

#[tokio::test]
async fn promoted_cancel_stops() {
    let family = Family::new([46u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.target).await;
    let receipt = seed_family(&ctx, &family).await;
    reserve_execution(&ctx, &family, &receipt).await;
    let spec = family.spec_for(JobId::from_bytes([47u8; 16]), family.holder.public());
    let ctx = Arc::new(ctx);

    for (record, accepted) in [
        (JobFamilyRecord::Cancel(family.cancel(&spec)), false),
        (JobFamilyRecord::Spec(Box::new(spec)), true),
    ] {
        let reply = serve_job_record(
            &ctx,
            family.holder.public(),
            MetadataTransportMessage::ForwardJobRecord {
                placement: family.placement,
                record: Box::new(JobRecordFrame::new(family.sign(&family.holder, record)).unwrap()),
            },
        )
        .await;
        assert_eq!(
            matches!(
                reply,
                MetadataTransportMessage::ForwardedJobRecord { result: Ok(()) }
            ),
            accepted
        );
        assert_eq!(flagged(&ctx).await, accepted);
    }
}

async fn store_cancel(ctx: &DriverContext, family: &Family) {
    let outcome = drive(
        AppendRecordOperation::new(AppendRecordConfig {
            realm_id: REALM,
            local_node_id: family.target.public(),
            record: JobRecordFrame::new(family.sign(
                &family.holder,
                JobFamilyRecord::Cancel(family.cancel(&family.spec())),
            ))
            .unwrap(),
            local: None,
            origin: RecordOrigin::Peer(family.holder.public()),
            now_ms: 5_000,
        }),
        ctx,
    )
    .await
    .unwrap();
    assert_eq!(outcome.admission, Admission::Authentic);
}

#[tokio::test]
async fn duplicate_cancel_recovers() {
    let family = Family::new([48u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.target).await;
    let receipt = seed_family(&ctx, &family).await;
    reserve_execution(&ctx, &family, &receipt).await;
    // The record committed, but its physical side effect did not run.
    store_cancel(&ctx, &family).await;
    assert!(!flagged(&ctx).await);

    let ctx = Arc::new(ctx);
    admit_cancel(&ctx, &family).await;

    assert!(flagged(&ctx).await);
}

#[tokio::test]
async fn replay_cancel_recovers() {
    let family = Family::new([49u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.target).await;
    let receipt = seed_family(&ctx, &family).await;
    reserve_execution(&ctx, &family, &receipt).await;
    store_cancel(&ctx, &family).await;
    assert!(!flagged(&ctx).await);
    let launch = family.launch(&family.spec(), family.holder.public(), 0);
    let ctx = Arc::new(ctx);

    let accepted = admit_launch(
        &ctx,
        LaunchFrame::new(family.sign(&family.holder, JobFamilyRecord::Launch(Box::new(launch))))
            .unwrap(),
    )
    .await
    .expect("replay decides")
    .expect("stored receipt answers");

    assert!(flagged(&ctx).await);
    assert!(
        matches!(&accepted.envelope().record, JobFamilyRecord::Receipt(stored) if stored.execution_id == receipt.execution_id)
    );
}

#[tokio::test]
async fn winner_cancel_recovers() {
    let family = Family::new([50u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.target).await;
    let (launch, _) = seed(&ctx, &family).await;
    let ctx = Arc::new(ctx);
    let mut winner = config(&family, &launch, 1, envelope(4));
    winner.record.state = JobState::Running;
    let accepted = commit_receipt(&ctx, winner, &launch)
        .await
        .unwrap()
        .unwrap();
    store_cancel(&ctx, &family).await;

    // Only a concurrent commit conflicts, so the lost race is injected here.
    let raced = commit_with(
        &ctx,
        config(&family, &launch, 2, envelope(4)),
        &launch,
        |_| async { Err(LifecycleError::Storage(StorageError::TransactionConflict)) },
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(accepted, raced);
    let record = read_job_record(&ctx.storage_handle, JobId::from_bytes([1u8; 16]), None)
        .await
        .unwrap()
        .unwrap();
    assert!(record.cancel_requested);
    assert!(
        read_job_record(&ctx.storage_handle, JobId::from_bytes([2u8; 16]), None)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn late_cancel_flags() {
    // The cancel is admitted while the launch is still being decided, so it
    // finds no receipt to stop; the committed launch must flag its own row.
    let family = Family::new([45u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.target).await;
    let (launch, _) = seed(&ctx, &family).await;
    let ctx = Arc::new(ctx);
    admit_cancel(&ctx, &family).await;

    commit_receipt(&ctx, config(&family, &launch, 1, envelope(4)), &launch)
        .await
        .expect("commit decides")
        .expect("receipt commits");

    let record = read_job_record(&ctx.storage_handle, JobId::from_bytes([1u8; 16]), None)
        .await
        .expect("physical row read")
        .expect("physical row exists");
    assert!(record.cancel_requested);
    assert_eq!(rows(&ctx, JOB_RESERVATION_KEYSPACE, None).await, 0);
    let updates = load_kind_complete(&ctx, family.family(), JobRecordKind::Update)
        .await
        .unwrap();
    assert!(updates.iter().any(|envelope| matches!(
        &envelope.record,
        JobFamilyRecord::Update(update) if update.state == PhysicalExecutionState::Cancelled
    )));
}

#[tokio::test]
async fn remote_run_untouched() {
    // Another node's execution is stopped by the network cancel only: a holder
    // that runs nothing must not flag a row it does not execute.
    let family = Family::new([44u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.target).await;
    let receipt = seed_family(&ctx, &family).await;
    reserve_execution(&ctx, &family, &receipt).await;
    let spec = family.spec();
    let (_holder_dir, holder_ctx) = node_context(&family, &family.holder).await;
    let holder_ctx = DriverContext {
        storage_handle: ctx.storage_handle.clone(),
        ..holder_ctx
    };

    cancel_family(&holder_ctx, &auth(user()), spec.job_id, None)
        .await
        .expect("alias names a family")
        .expect("cancellation publishes");

    assert!(!flagged(&ctx).await);
}

#[tokio::test]
async fn deletion_hides_family() {
    // Once its submitter deleted the run, a read by id answers absent; nobody else may delete.
    use crate::jobs::JobRouteError;
    use crate::jobs::lifecycle::cancel::publish_deletion;
    use crate::jobs::lifecycle::report::family_report;
    let family = Family::new([43u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.holder).await;
    seed_family(&ctx, &family).await;
    let job_id = family.spec().job_id;
    let owner = auth(user());
    let read = || family_report(&ctx, &owner, job_id);
    assert!(matches!(read().await, Some(Ok(_))));
    let stranger = auth(UserId::local(ulid::Ulid::from_bytes([44; 16]), REALM));
    let refused = publish_deletion(&ctx, &stranger, job_id).await;
    assert_eq!(refused, Err(JobRouteError::NotFound));
    assert!(matches!(read().await, Some(Ok(_))));

    publish_deletion(&ctx, &owner, job_id)
        .await
        .expect("deletion publishes");
    assert!(matches!(read().await, Some(Err(JobRouteError::NotFound))));
}

#[tokio::test]
async fn unrecorded_deletion_kept() {
    // A node that cannot record the deletion in the run's family keeps the run, so a retried
    // delete records it again; a holder records it before the run goes.
    use crate::jobs::service::delete_owned_run;
    use crate::jobs::store::{RunDelete, insert_job};
    use crate::tests::records::secret;
    use aruna_core::structs::execution::job::{
        ComputeResources, ExecutionSpec, JobPayload, JobRecord,
    };
    let family = Family::new([45u8; 32]);
    let job_id = family.spec().job_id;
    let finished = |node| {
        let spec = ExecutionSpec {
            group_id: ulid::Ulid::from_bytes([2u8; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine:3".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            env: Default::default(),
            resources: ComputeResources::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        };
        let payload = JobPayload::Execution(spec);
        let mut record = JobRecord::new(job_id, payload, user(), node, 1_000, 1_000, None);
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(2_000);
        record
    };
    let owner = auth(user());

    let outsider = secret(9);
    let (_dir, ctx) = node_context(&family, &outsider).await;
    seed_family(&ctx, &family).await;
    insert_job(&ctx.storage_handle, &finished(outsider.public()))
        .await
        .expect("run row written");
    let kept = delete_owned_run(&ctx, &owner, job_id)
        .await
        .expect("delete runs");
    assert!(matches!(kept, RunDelete::Unrecorded(_)), "{kept:?}");
    let row = read_job_record(&ctx.storage_handle, job_id, None).await;
    assert!(row.expect("run row read").is_some());

    let (_dir, ctx) = node_context(&family, &family.holder).await;
    seed_family(&ctx, &family).await;
    insert_job(&ctx.storage_handle, &finished(family.holder.public()))
        .await
        .expect("run row written");
    let deleted = delete_owned_run(&ctx, &owner, job_id)
        .await
        .expect("delete runs");
    assert_eq!(deleted, RunDelete::Deleted);
    let cancels = load_kind_complete(&ctx, family.family(), JobRecordKind::Cancel).await;
    assert_eq!(cancels.expect("cancel records load").len(), 1);
}
