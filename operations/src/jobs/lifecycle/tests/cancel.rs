//! Cancelling a family has to reach the executions this node itself runs: the
//! replicated record stops further launches, it does not stop a running one.

use aruna_core::structs::{AuthContext, JobState};
use aruna_core::types::UserId;

use super::terminal::{node_context, physical, reserve_execution, seed_family};
use crate::driver::DriverContext;
use crate::jobs::lifecycle::cancel::cancel_family;
use crate::jobs::records::tests::fixture::{Family, REALM, user};
use crate::jobs::store::read_job_record;

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
