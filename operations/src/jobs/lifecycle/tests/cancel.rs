//! Cancelling a family has to reach the executions this node itself runs: the
//! replicated record stops further launches, it does not stop a running one.

use std::sync::Arc;

use aruna_core::effects::JobRecordFrame;
use aruna_core::structs::{AuthContext, JobFamilyRecord, JobId, JobState};
use aruna_core::types::UserId;

use super::admission_race::{config, envelope, seed};
use super::terminal::{node_context, physical, reserve_execution, seed_family};
use crate::driver::DriverContext;
use crate::jobs::lifecycle::cancel::cancel_family;
use crate::jobs::lifecycle::target::commit_receipt;
use crate::jobs::records::tests::fixture::{Family, REALM, user};
use crate::jobs::records::transport::serve_job_record;
use crate::jobs::store::read_job_record;
use crate::metadata::protocol::MetadataTransportMessage;

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
async fn family_cancel_stops_local() {
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
async fn repeated_cancel_is_quiet() {
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
async fn admitted_cancel_stops_local() {
    // The record arrives after this node already admitted the execution, so
    // admission itself has to apply it to the row that is already running.
    let family = Family::new([43u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.target).await;
    let receipt = seed_family(&ctx, &family).await;
    reserve_execution(&ctx, &family, &receipt).await;
    assert!(!flagged(&ctx).await);
    let ctx = Arc::new(ctx);

    admit_cancel(&ctx, &family).await;

    assert!(flagged(&ctx).await);
}

#[tokio::test]
async fn late_cancel_flags_launch() {
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
}

#[tokio::test]
async fn remote_run_stays_untouched() {
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
