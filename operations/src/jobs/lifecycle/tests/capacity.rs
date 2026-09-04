//! Exact target admission: the reservation, the receipt it commits with, and
//! the release that frees it again.

use aruna_core::compute::ResourceEnvelope;
use aruna_core::effects::{JobRecordFrame, StorageEffect};
use aruna_core::keyspaces::{JOB_FAMILY_RECORD_KEYSPACE, JOB_RESERVATION_KEYSPACE};
use aruna_core::scheduling::PlannedInput;
use aruna_core::structs::{
    CapturedInput, EffectiveResources, JobFamilyRecord, JobPayload, JobRecord, JobRecordBody,
    LaunchIntent, LogicalJobSpec, PhysicalExecutionState, VersionedObjectArn,
};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::reservation::{
    ExecutionReservation, MAX_RESERVATION_SCAN, ReleaseExecutionOperation, ReserveExecutionConfig,
    ReserveExecutionOperation, fits, held_reservations, job_reservation,
};
use crate::jobs::lifecycle::stage::read_targets;
use crate::jobs::lifecycle::target::{already_running, existing_receipt, pin_matches};
use crate::jobs::lifecycle::updates::chain_for;
use crate::jobs::records::tests::fixture::{Family, REALM, context, node};
use crate::jobs::records::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};
use crate::jobs::store::iter_prefix_page;
use crate::replication::protocol::BaoReadTarget;

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
    let spec = family.spec();
    let record = JobRecord::new(
        receipt.job_id,
        JobPayload::Execution(spec.payload),
        spec.created_by,
        family.target.public(),
        4_000,
        4_000,
        None,
    );
    drive(
        ReserveExecutionOperation::new(ReserveExecutionConfig {
            realm_id: REALM,
            local_node_id: family.target.public(),
            envelope,
            receipt: frame,
            launch: Box::new(launch.clone()),
            job_id: receipt.job_id,
            logical_job_id: receipt.job_id,
            execution_id: receipt.execution_id,
            resources: resources(),
            subject_generation: receipt.subject_generation,
            subject_digest: receipt.subject_digest,
            record: Box::new(record),
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
        logical_job_id: aruna_core::structs::JobId::from_bytes([2u8; 16]),
        resources: resources(),
        created_at_ms: 1,
        subject_generation: 1,
        subject_digest: [0u8; 32],
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
async fn pages_reservations() {
    // Capacity accounting must include reservations beyond one storage page.
    let family = Family::new([9u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let writes = (0..=MAX_RESERVATION_SCAN)
        .map(|index| {
            let execution_id = Ulid::from(index as u128 + 1);
            let reservation = ExecutionReservation {
                execution_id,
                job_id: family.job_id,
                logical_job_id: family.job_id,
                resources: resources(),
                created_at_ms: index as u64,
                subject_generation: 1,
                subject_digest: [0u8; 32],
            };
            (
                JOB_RESERVATION_KEYSPACE.to_string(),
                execution_id.to_bytes().as_slice().into(),
                postcard::to_allocvec(&reservation)
                    .expect("reservation encodes")
                    .into(),
            )
        })
        .collect();
    ctx.storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await;

    assert_eq!(
        held_reservations(&ctx)
            .await
            .expect("reservation scan")
            .len(),
        MAX_RESERVATION_SCAN + 1
    );
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
    assert_eq!(
        held_reservations(&ctx)
            .await
            .expect("reservation scan")
            .len(),
        1
    );
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
        .expect("reservation scan")
        .expect("reservation is durable");
    assert_eq!(reservation.execution_id, execution_id);
    assert!(
        crate::jobs::store::read_job_record(&ctx.storage_handle, spec.job_id, None)
            .await
            .expect("physical row read")
            .is_some()
    );
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

#[test]
fn declines_second_launch() {
    // A family this node still runs, or already ran successfully, refuses a
    // second launch; a retryable error leaves the node free to run it again.
    let family = Family::new([2u8; 32]);
    let target = family.target.public();
    let running = family.run(1, 0, PhysicalExecutionState::Running);
    let succeeded = family.run(1, 0, PhysicalExecutionState::Succeeded);
    let errored = family.run(1, 0, PhysicalExecutionState::Error);

    assert!(already_running(family.family(), &running, target));
    assert!(already_running(family.family(), &succeeded, target));
    assert!(!already_running(family.family(), &errored, target));
    assert!(!already_running(family.family(), &running, node(2)));
}

#[test]
fn declines_after_failure() {
    // A permanent failure suppresses retry, so this node refuses the family
    // again even though its execution is terminal.
    let family = Family::new([2u8; 32]);
    let failed = family.run(1, 0, PhysicalExecutionState::Failed);

    assert!(already_running(
        family.family(),
        &failed,
        family.target.public()
    ));
    assert!(!already_running(family.family(), &failed, node(2)));
}

#[test]
fn accepts_copy_pin() {
    // A registered copy on any node may be the pinned source, but the captured
    // version, hash and size still bind the bytes, and a pin naming this target
    // itself is never a remote read.
    let family = Family::new([3u8; 32]);
    let ingress = family.holder.public();
    let local = family.target.public();
    let captured = CapturedInput {
        destination_key: "in/reads.fastq".to_string(),
        source_node_id: ingress,
        version_id: Ulid::from_bytes([4u8; 16]),
        blake3: [5u8; 32],
        bytes: 128,
        policies: Vec::new(),
    };
    let pin = PlannedInput {
        destination_key: captured.destination_key.clone(),
        version_id: captured.version_id,
        blake3: captured.blake3,
        bytes: captured.bytes,
        policies: Vec::new(),
        source_node_id: Some(node(2)),
        transfer_ms: 7,
        known_link: true,
    };

    assert!(pin_matches(&captured, &pin, ingress, local));
    let mut here = pin.clone();
    here.source_node_id = Some(local);
    assert!(!pin_matches(&captured, &here, ingress, local));
    let mut other_bytes = pin.clone();
    other_bytes.blake3 = [6u8; 32];
    assert!(!pin_matches(&captured, &other_bytes, ingress, local));
}

#[test]
fn stages_by_hash() {
    // The endpoint that owns the object identity is asked for its exact
    // version; any other holder is also asked for the same bytes by hash.
    let family = Family::new([4u8; 32]);
    let source = VersionedObjectArn {
        realm_id: REALM,
        node_id: family.holder.public(),
        bucket: "inputs".to_string(),
        key: "reads.fastq".to_string(),
        version: Ulid::from_bytes([4u8; 16]),
    };

    assert_eq!(read_targets(source.clone(), true, [5u8; 32]).len(), 1);
    assert_eq!(
        read_targets(source, false, [5u8; 32]).pop(),
        Some(BaoReadTarget::Blake3([5u8; 32]))
    );
}

#[test]
fn replays_launch_offer() {
    // Replaying one launch returns the receipt already issued for it, while the
    // same launch id under different bytes is a conflict, never a second run.
    let family = Family::new([1u8; 32]);
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let receipt = family.receipt(&launch, 1);
    let records = vec![family.sign(
        &family.target,
        JobFamilyRecord::Receipt(Box::new(receipt.clone())),
    )];

    let replayed = existing_receipt(&records, &launch).expect("receipt is known");
    assert_eq!(
        replayed.expect("receipt frame").envelope().digest(),
        family
            .sign(
                &family.target,
                JobFamilyRecord::Receipt(Box::new(receipt.clone()))
            )
            .digest()
    );

    let mut altered = launch.clone();
    altered.plan_digest = [9u8; 32];
    assert_eq!(
        existing_receipt(&records, &altered),
        Some(Err(aruna_core::events::LaunchDecline::LaunchConflict))
    );
}
