//! The terminal publication a receipted execution owes its family: the stored
//! output record, the update that names its digest, and the projection both
//! must reduce to.

use aruna_core::compute::ResourceEnvelope;
use aruna_core::effects::JobRecordFrame;
use aruna_core::keyspaces::JOB_RESERVATION_KEYSPACE;
use aruna_core::structs::{
    AttemptIntent, EffectiveResources, ExecutionReceipt, JobClaim, JobFamilyRecord, JobId,
    JobPayload, JobRecord, JobResultPayload, JobState, LogicalJobState, OutputObject,
    PhysicalExecutionState,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use tempfile::TempDir;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::lifecycle::reservation::{ReserveExecutionConfig, ReserveExecutionOperation};
use crate::jobs::lifecycle::updates::publish_terminal;
use crate::jobs::output_record::store_outputs;
use crate::jobs::records::reduce::reduce_family;
use crate::jobs::records::tests::fixture::{Family, REALM, context};
use crate::jobs::records::{
    AppendRecordConfig, AppendRecordOperation, RecordOrigin, load_family_complete,
};
use crate::jobs::store::{iter_prefix_page, record_attempt_intent, reserve_output_commits};

const TOKEN: Ulid = Ulid(0x7E12);

/// The local physical row a target mints for an admitted launch. It is never
/// the logical job id the family records are keyed by.
fn physical() -> JobId {
    JobId::from_bytes([31u8; 16])
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

/// The execution target itself, with a live net handle so it can sign the
/// records only the receipted executor may publish.
async fn target_context(family: &Family) -> (TempDir, DriverContext) {
    let (dir, ctx) = context(&family.config, family.holder.public()).await;
    let net_handle = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("loopback address"),
            secret_key: Some(family.target.clone()),
            realm_id: REALM,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        ctx.storage_handle.clone(),
    )
    .await
    .expect("net handle starts");
    (
        dir,
        DriverContext {
            net_handle: Some(net_handle),
            ..ctx
        },
    )
}

/// Stores every family record the receipt chains to, exactly as replication
/// delivers them to the target before it admits the launch.
async fn seed_family(ctx: &DriverContext, family: &Family) -> ExecutionReceipt {
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    for record in [
        JobFamilyRecord::Spec(Box::new(spec.clone())),
        JobFamilyRecord::Claim(family.claim(&spec)),
        JobFamilyRecord::Budget(family.budget(&spec, family.holder.public())),
        JobFamilyRecord::Launch(Box::new(launch.clone())),
    ] {
        let frame =
            JobRecordFrame::new(family.sign(&family.holder, record)).expect("bounded record");
        drive(
            AppendRecordOperation::new(AppendRecordConfig {
                realm_id: REALM,
                local_node_id: family.target.public(),
                record: frame,
                local: None,
                origin: RecordOrigin::Peer(family.holder.public()),
                now_ms: 3_000,
            }),
            ctx,
        )
        .await
        .expect("append completes");
    }
    family.receipt(&launch, 1)
}

/// Commits the receipt with the reservation that binds the local physical row
/// to the logical job the family knows.
async fn reserve_execution(
    ctx: &DriverContext,
    family: &Family,
    receipt: &ExecutionReceipt,
) -> JobRecord {
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let frame = JobRecordFrame::new(family.sign(
        &family.target,
        JobFamilyRecord::Receipt(Box::new(receipt.clone())),
    ))
    .expect("bounded receipt");
    let mut record = JobRecord::new(
        physical(),
        JobPayload::Execution(spec.payload.clone()),
        spec.created_by,
        family.target.public(),
        4_000,
        4_000,
        None,
    );
    record.state = JobState::Running;
    record.claim = Some(JobClaim {
        holder_node_id: family.target.public(),
        claim_token: TOKEN,
        lease_expires_at_ms: 100_000,
    });
    drive(
        ReserveExecutionOperation::new(ReserveExecutionConfig {
            realm_id: REALM,
            local_node_id: family.target.public(),
            envelope: ResourceEnvelope::default(),
            receipt: frame,
            launch: Box::new(launch),
            job_id: physical(),
            logical_job_id: spec.job_id,
            execution_id: receipt.execution_id,
            resources: resources(),
            subject_generation: receipt.subject_generation,
            subject_digest: receipt.subject_digest,
            record: Box::new(record.clone()),
            now_ms: 4_000,
        }),
        ctx,
    )
    .await
    .expect("offer is admitted");
    record
}

#[tokio::test]
async fn publishes_terminal_success() {
    // The whole terminal path of one receipted execution: the local physical
    // job id must never be read as the logical alias the family is keyed by.
    let family = Family::new([21u8; 32]);
    let (_dir, ctx) = target_context(&family).await;
    let receipt = seed_family(&ctx, &family).await;
    let record = reserve_execution(&ctx, &family, &receipt).await;
    let intent = AttemptIntent {
        attempt_no: 1,
        external_name: "aruna-terminal-1".to_string(),
        executor_kind: "docker".to_string(),
        pinned_image: "alpine@sha256:0".to_string(),
        attempt_epoch: 0,
    };
    let commit = record_attempt_intent(
        &ctx.storage_handle,
        physical(),
        TOKEN,
        intent,
        Some(receipt.execution_id),
        4_100,
    )
    .await
    .expect("attempt intent commits");
    let executor = family.target.public();
    let destinations = vec![(executor, "ws".to_string(), "out.txt".to_string())];
    let control = reserve_output_commits(&ctx.storage_handle, physical(), &destinations)
        .await
        .expect("output commits reserved");
    let outputs = vec![OutputObject {
        node_id: executor,
        bucket: "ws".to_string(),
        key: "out.txt".to_string(),
        version_id: control.output_commits[0].version_id,
        execution_id: control.execution_id,
        container_path: "/out/out.txt".to_string(),
        size: 3,
        digest: None,
    }];

    let output_digest = store_outputs(&ctx, &commit.record, &control, &outputs)
        .await
        .expect("outputs stored");
    let mut terminal = record;
    terminal.state = JobState::Succeeded;
    terminal.finished_at_ms = Some(5_000);
    terminal.result = Some(JobResultPayload::Execution {
        exit_code: Some(0),
        workspace_bucket: None,
        outputs,
        stdout: String::new(),
        stderr: String::new(),
        output_digest: Some(output_digest),
    });
    assert!(publish_terminal(&ctx, &terminal).await);

    let records = load_family_complete(&ctx, family.family())
        .await
        .expect("family read completes");
    assert!(records.iter().any(|envelope| matches!(
        &envelope.record,
        JobFamilyRecord::Update(update)
            if update.state == PhysicalExecutionState::Succeeded
                && update.execution_id == receipt.execution_id
    )));
    let projection = reduce_family(family.family(), &records)
        .expect("family reduces")
        .expect("family has a projection");
    assert_eq!(projection.state, LogicalJobState::Succeeded);
    assert_eq!(
        projection.canonical_execution_id,
        Some(receipt.execution_id)
    );
    assert_eq!(projection.outputs.as_slice().len(), 1);
    let (held, _) = iter_prefix_page(
        &ctx.storage_handle,
        JOB_RESERVATION_KEYSPACE,
        None,
        None,
        8,
        None,
    )
    .await
    .expect("reservation scan");
    assert!(held.is_empty());
}
