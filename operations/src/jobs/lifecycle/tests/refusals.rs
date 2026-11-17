//! Checks the first calls of a reservation and that it refuses an unfit result.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::ResourceEnvelope;
use aruna_core::effects::{Effect, JobRecordFrame, StorageEffect};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    EffectiveResources, JobFamilyRecord, JobPayload, JobRecord,
};

use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::reservation::{ReserveExecutionConfig, ReserveExecutionOperation};
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
fn reads_config_outside() {
    // The realm view is read outside the reservation transaction.
    let mut operation = reserve();
    assert!(operation.transaction());
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Effect(effect)))
            if matches!(*effect, Effect::Storage(StorageEffect::Read { txn_id: None, .. }))
    ));
}

#[test]
fn rejects_unfit_result() {
    let mut operation = reserve();
    operation.first().expect("config read");
    assert!(matches!(
        operation.next(IoResult::Done),
        Err(LifecycleError::UnexpectedEvent { .. })
    ));
}
