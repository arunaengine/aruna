//! Exact target admission: the reservation, the receipt it commits with, and
//! the release that frees it again.

use aruna_core::compute::ResourceEnvelope;
use aruna_core::effects::JobRecordFrame;
use aruna_core::keyspaces::{JOB_FAMILY_RECORD_KEYSPACE, JOB_RESERVATION_KEYSPACE};
use aruna_core::structs::{
    EffectiveResources, JobFamilyRecord, JobRecordBody, LaunchIntent, LogicalJobSpec,
};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::reservation::{
    ExecutionReservation, ReleaseExecutionOperation, ReserveExecutionConfig,
    ReserveExecutionOperation, fits, held_reservations, job_reservation,
};
use crate::jobs::lifecycle::updates::chain_for;
use crate::jobs::records::tests::fixture::{Family, REALM, context};
use crate::jobs::records::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};
use crate::jobs::store::iter_prefix_page;

fn envelope(max_concurrent: u32) -> ResourceEnvelope {
    ResourceEnvelope {
        max_cpu_cores: Some(4),
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

/// Stores the spec, claim, budget, and launch a receipt must chain to.
async fn seed(ctx: &DriverContext, family: &Family) -> (LogicalJobSpec, LaunchIntent) {
    let spec = family.spec();
    let claim = family.claim(&spec);
    let budget = family.budget(&spec, family.holder.public());
    let launch = family.launch(&spec, family.holder.public(), 0);
    for record in [
        JobFamilyRecord::Spec(Box::new(spec.clone())),
        JobFamilyRecord::Claim(claim),
        JobFamilyRecord::Budget(budget),
        JobFamilyRecord::Launch(Box::new(launch.clone())),
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
    (spec, launch)
}

async fn reserve(
    ctx: &DriverContext,
    family: &Family,
    launch: &LaunchIntent,
    execution: u8,
    envelope: ResourceEnvelope,
) -> Result<Ulid, LifecycleError> {
    let receipt = family.receipt(launch, execution);
    let frame = JobRecordFrame::new(family.sign(
        &family.target,
        JobFamilyRecord::Receipt(Box::new(receipt.clone())),
    ))
    .expect("bounded receipt");
    drive(
        ReserveExecutionOperation::new(ReserveExecutionConfig {
            realm_id: REALM,
            local_node_id: family.target.public(),
            envelope,
            receipt: frame,
            launch: Box::new(launch.clone()),
            job_id: receipt.job_id,
            execution_id: receipt.execution_id,
            resources: resources(),
            now_ms: 4_000,
        }),
        ctx,
    )
    .await
}

#[test]
fn holds_static_ceilings() {
    // An unmeasured ceiling never filters; a measured one is exact and a zero
    // concurrency ceiling admits nothing at all.
    let held = vec![ExecutionReservation {
        execution_id: Ulid::from_bytes([1u8; 16]),
        job_id: aruna_core::structs::JobId::from_bytes([2u8; 16]),
        resources: resources(),
        created_at_ms: 1,
    }];
    assert!(fits(&held, &resources(), &envelope(2)));
    assert!(!fits(&held, &resources(), &envelope(1)));
    assert!(fits(&held, &resources(), &ResourceEnvelope::default()));
    assert!(!fits(
        &[],
        &resources(),
        &ResourceEnvelope {
            max_ram_bytes: Some(512),
            ..ResourceEnvelope::default()
        }
    ));
}

#[tokio::test]
async fn reserves_exact_capacity() {
    // Two offers competing for one slot cannot both be admitted, and the
    // refused one leaves neither a reservation nor a receipt behind.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let (_spec, launch) = seed(&ctx, &family).await;

    reserve(&ctx, &family, &launch, 1, envelope(1))
        .await
        .expect("first offer is admitted");
    let refused = reserve(&ctx, &family, &launch, 2, envelope(1))
        .await
        .expect_err("second offer exceeds the ceiling");

    assert_eq!(refused, LifecycleError::Capacity);
    assert_eq!(held_reservations(&ctx).await.len(), 1);
}

#[tokio::test]
async fn persists_receipt_first() {
    // The reservation and the signed receipt become visible together, so work
    // can never start before its receipt is durable.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let (spec, launch) = seed(&ctx, &family).await;
    let execution_id = reserve(&ctx, &family, &launch, 1, envelope(2))
        .await
        .expect("offer is admitted");

    let reservation = job_reservation(&ctx, spec.job_id)
        .await
        .expect("reservation is durable");
    assert_eq!(reservation.execution_id, execution_id);
    let (records, _) = iter_prefix_page(
        &ctx.storage_handle,
        JOB_FAMILY_RECORD_KEYSPACE,
        None,
        None,
        32,
        None,
    )
    .await
    .expect("record scan");
    assert_eq!(records.len(), 5);
}

#[tokio::test]
async fn releases_on_terminal() {
    // Releasing frees the exact capacity once, and a second release is a no-op
    // rather than a second free slot.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let (_spec, launch) = seed(&ctx, &family).await;
    let execution_id = reserve(&ctx, &family, &launch, 1, envelope(1))
        .await
        .expect("offer is admitted");

    assert!(
        drive(ReleaseExecutionOperation::new(execution_id), &ctx)
            .await
            .expect("release completes")
    );
    assert!(
        !drive(ReleaseExecutionOperation::new(execution_id), &ctx)
            .await
            .expect("release completes")
    );
    let (rows, _) = iter_prefix_page(
        &ctx.storage_handle,
        JOB_RESERVATION_KEYSPACE,
        None,
        None,
        8,
        None,
    )
    .await
    .expect("reservation scan");
    assert!(rows.is_empty());
}

#[tokio::test]
async fn output_binds_receipt() {
    // The output record's identity now comes from the real chain: the family
    // the receipt names, its spec digest, and the receipt's own digest.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let (spec, launch) = seed(&ctx, &family).await;
    let execution_id = reserve(&ctx, &family, &launch, 1, envelope(2))
        .await
        .expect("offer is admitted");

    let chain = chain_for(&ctx, spec.job_id, execution_id)
        .await
        .expect("chain resolves");
    let receipt = family.receipt(&launch, 1);
    assert_eq!(chain.family, family.family());
    assert_eq!(chain.spec_digest, spec.spec_digest);
    assert_eq!(chain.receipt_digest, receipt.digest().expect("digest"));
}
