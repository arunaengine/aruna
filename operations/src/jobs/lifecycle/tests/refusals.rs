//! Checks that the reservation and its release refuse an event before they start.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::ResourceEnvelope;
use aruna_core::effects::JobRecordFrame;
use aruna_core::operation_check::{rejects_events, sample_events};
use aruna_core::structs::execution::job::{
    EffectiveResources, JobFamilyRecord, JobPayload, JobRecord,
};
use ulid::Ulid;

use crate::jobs::lifecycle::reservation::{
    ReleaseExecutionOperation, ReserveExecutionConfig, ReserveExecutionOperation,
};
use crate::tests::records::{Family, REALM};

fn reserve() -> ReserveExecutionOperation {
    let family = Family::new([33u8; 32]);
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let receipt = family.receipt(&launch, 1);
    let frame = JobRecordFrame::new(family.sign(
        &family.target,
        JobFamilyRecord::Receipt(Box::new(receipt.clone())),
    ))
    .expect("bounded receipt");
    let record = JobRecord::new(
        receipt.job_id,
        JobPayload::Execution(spec.payload),
        spec.created_by,
        family.target.public(),
        4_000,
        4_000,
        None,
    );
    ReserveExecutionOperation::new(ReserveExecutionConfig {
        realm_id: REALM,
        local_node_id: family.target.public(),
        envelope: ResourceEnvelope::default(),
        receipt: frame,
        launch: Box::new(launch),
        job_id: receipt.job_id,
        logical_job_id: receipt.job_id,
        execution_id: receipt.execution_id,
        resources: EffectiveResources {
            cpu_cores: 1,
            ram_bytes: 0,
            disk_bytes: 0,
            max_walltime_ms: 1_000,
            preemptible: false,
        },
        subject_generation: receipt.subject_generation,
        subject_digest: receipt.subject_digest,
        record: Box::new(record),
        now_ms: 4_000,
    })
}

#[test]
fn rejects_unstarted_reserve() {
    let refusals = rejects_events(reserve, sample_events, |_| false);
    assert_eq!(refusals.len(), sample_events().len());
}

#[test]
fn rejects_unstarted_release() {
    let release = || ReleaseExecutionOperation::new(Ulid::from_bytes([5u8; 16]));
    let refusals = rejects_events(release, sample_events, |_| false);
    assert_eq!(refusals.len(), sample_events().len());
}
