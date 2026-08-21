//! Two admissions racing one target: the storage conflict that stops the second
//! commit must be answered from the receipt that won, never as a drain.

use std::sync::Arc;

use aruna_core::compute::ResourceEnvelope;
use aruna_core::effects::{JobRecordFrame, ReceiptFrame};
use aruna_core::events::LaunchDecline;
use aruna_core::keyspaces::{JOB_FAMILY_RECORD_KEYSPACE, JOB_RESERVATION_KEYSPACE};
use aruna_core::structs::{
    EffectiveResources, JobFamilyRecord, JobId, JobPayload, JobRecord, JobRecordKind, LaunchIntent,
};
use aruna_core::types::Key;
use tokio::sync::Barrier;

use crate::driver::{DriverContext, drive};
use crate::jobs::lifecycle::reservation::ReserveExecutionConfig;
use crate::jobs::lifecycle::target::commit_receipt;
use crate::jobs::records::keys::kind_prefix;
use crate::jobs::records::tests::fixture::{Family, REALM, context};
use crate::jobs::records::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};
use crate::jobs::store::{iter_prefix_page, read_job_record};

fn envelope(max_concurrent: u32) -> ResourceEnvelope {
    ResourceEnvelope {
        max_cpu_cores: Some(8),
        max_ram_bytes: Some(8_192),
        max_disk_bytes: Some(16_384),
        max_concurrent: Some(max_concurrent),
    }
}

fn resources() -> EffectiveResources {
    EffectiveResources {
        cpu_cores: 1,
        ram_bytes: 1024,
        disk_bytes: 2048,
        max_walltime_ms: 60_000,
        preemptible: false,
    }
}

/// The physical job one caller mints for itself, distinct per caller so only
/// the committed one leaves a local row behind.
fn physical(execution: u8) -> JobId {
    JobId::from_bytes([execution; 16])
}

/// Stores the spec, claim, budget, and two launches a receipt may chain to.
async fn seed(ctx: &DriverContext, family: &Family) -> (LaunchIntent, LaunchIntent) {
    let spec = family.spec();
    let first = family.launch(&spec, family.holder.public(), 0);
    let second = family.launch(&spec, family.holder.public(), 1);
    for record in [
        JobFamilyRecord::Spec(Box::new(spec.clone())),
        JobFamilyRecord::Claim(family.claim(&spec)),
        JobFamilyRecord::Budget(family.budget(&spec, family.holder.public())),
        JobFamilyRecord::Launch(Box::new(first.clone())),
        JobFamilyRecord::Launch(Box::new(second.clone())),
    ] {
        let frame =
            JobRecordFrame::new(family.sign(&family.holder, record)).expect("bounded record");
        drive(
            AppendRecordOperation::new(AppendRecordConfig {
                realm_id: REALM,
                local_node_id: family.holder.public(),
                record: frame,
                local: None,
                origin: RecordOrigin::Local,
                now_ms: 3_000,
            }),
            ctx,
        )
        .await
        .expect("append completes");
    }
    (first, second)
}

fn config(
    family: &Family,
    launch: &LaunchIntent,
    execution: u8,
    envelope: ResourceEnvelope,
) -> ReserveExecutionConfig {
    let receipt = family.receipt(launch, execution);
    let frame = JobRecordFrame::new(family.sign(
        &family.target,
        JobFamilyRecord::Receipt(Box::new(receipt.clone())),
    ))
    .expect("bounded receipt");
    let spec = family.spec();
    let record = JobRecord::new(
        physical(execution),
        JobPayload::Execution(spec.payload),
        spec.created_by,
        family.target.public(),
        4_000,
        4_000,
        None,
    );
    ReserveExecutionConfig {
        realm_id: REALM,
        local_node_id: family.target.public(),
        envelope,
        receipt: frame,
        launch: Box::new(launch.clone()),
        job_id: physical(execution),
        logical_job_id: receipt.job_id,
        execution_id: receipt.execution_id,
        resources: resources(),
        subject_generation: receipt.subject_generation,
        subject_digest: receipt.subject_digest,
        record: Box::new(record),
        now_ms: 4_000,
    }
}

/// One admission that starts only once the other one is ready to start too.
async fn admit(
    ctx: &Arc<DriverContext>,
    barrier: &Barrier,
    config: ReserveExecutionConfig,
    launch: &LaunchIntent,
) -> Option<Result<ReceiptFrame, LaunchDecline>> {
    barrier.wait().await;
    commit_receipt(ctx, config, launch).await
}

async fn rows(ctx: &DriverContext, key_space: &str, prefix: Option<Key>) -> usize {
    iter_prefix_page(&ctx.storage_handle, key_space, prefix, None, 64, None)
        .await
        .expect("row scan")
        .0
        .len()
}

async fn receipts(ctx: &DriverContext, family: &Family) -> usize {
    let prefix = kind_prefix(&family.family(), JobRecordKind::Receipt);
    rows(ctx, JOB_FAMILY_RECORD_KEYSPACE, Some(prefix)).await
}

/// Whether exactly one of the two minted physical jobs became durable.
async fn minted(ctx: &DriverContext) -> usize {
    let mut count = 0;
    for execution in [1u8, 2u8] {
        if read_job_record(&ctx.storage_handle, physical(execution), None)
            .await
            .expect("job row read")
            .is_some()
        {
            count += 1;
        }
    }
    count
}

#[tokio::test]
async fn admits_launch_once() {
    // One launch offered twice may commit only one execution, and both callers
    // must answer with the receipt that actually committed.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let ctx = Arc::new(ctx);
    let (launch, _) = seed(&ctx, &family).await;
    let barrier = Barrier::new(2);

    let (first, second) = tokio::join!(
        admit(
            &ctx,
            &barrier,
            config(&family, &launch, 1, envelope(4)),
            &launch
        ),
        admit(
            &ctx,
            &barrier,
            config(&family, &launch, 2, envelope(4)),
            &launch
        ),
    );

    let first = first.expect("first offer is decided").expect("admitted");
    let second = second.expect("second offer is decided").expect("admitted");
    assert_eq!(first.envelope().digest(), second.envelope().digest());
    assert_eq!(rows(&ctx, JOB_RESERVATION_KEYSPACE, None).await, 1);
    assert_eq!(receipts(&ctx, &family).await, 1);
    assert_eq!(minted(&ctx).await, 1);
}

#[tokio::test]
async fn admits_both_launches() {
    // Two different launches race the same commit, but the loser has no receipt
    // of its own to find, so it retries and is admitted on its own capacity.
    let family = Family::new([2u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let ctx = Arc::new(ctx);
    let (first_launch, second_launch) = seed(&ctx, &family).await;
    let barrier = Barrier::new(2);

    let (first, second) = tokio::join!(
        admit(
            &ctx,
            &barrier,
            config(&family, &first_launch, 1, envelope(4)),
            &first_launch
        ),
        admit(
            &ctx,
            &barrier,
            config(&family, &second_launch, 2, envelope(4)),
            &second_launch
        ),
    );

    assert!(first.expect("first offer is decided").is_ok());
    assert!(second.expect("second offer is decided").is_ok());
    assert_eq!(rows(&ctx, JOB_RESERVATION_KEYSPACE, None).await, 2);
    assert_eq!(receipts(&ctx, &family).await, 2);
    assert_eq!(minted(&ctx).await, 2);
}

#[tokio::test]
async fn declines_over_capacity() {
    // With room for one execution the loser must be refused for capacity, which
    // is the one verdict a full backend may report.
    let family = Family::new([3u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let ctx = Arc::new(ctx);
    let (first_launch, second_launch) = seed(&ctx, &family).await;
    let barrier = Barrier::new(2);

    let (first, second) = tokio::join!(
        admit(
            &ctx,
            &barrier,
            config(&family, &first_launch, 1, envelope(1)),
            &first_launch
        ),
        admit(
            &ctx,
            &barrier,
            config(&family, &second_launch, 2, envelope(1)),
            &second_launch
        ),
    );

    let decided: Vec<Result<ReceiptFrame, LaunchDecline>> = vec![
        first.expect("first offer is decided"),
        second.expect("second offer is decided"),
    ];
    assert_eq!(decided.iter().filter(|result| result.is_ok()).count(), 1);
    assert!(
        decided
            .iter()
            .any(|result| matches!(result, Err(LaunchDecline::Capacity)))
    );
    assert_eq!(rows(&ctx, JOB_RESERVATION_KEYSPACE, None).await, 1);
    assert_eq!(minted(&ctx).await, 1);
}
