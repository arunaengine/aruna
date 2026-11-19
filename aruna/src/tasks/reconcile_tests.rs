//! Tests that the compute reconciler spares a lost attempt it cannot observe.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_clock::unix_timestamp_millis;

use super::workflow_fixtures::{execution_spec, node_id};
use crate::tasks::runner::ComputeReconciler;
use aruna_compute::registry::ExecutorRegistry;
use aruna_core::structs::execution::job::{
    AttemptIntent, JobClaim, JobId, JobPayload, JobRecord, JobState,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::user::id::UserId;
use aruna_drivers::driver::DriverContext;
use aruna_drivers::jobs::context::ExternalReconciler;
use aruna_drivers::jobs::runtime::JobsRuntime;
use aruna_operations::jobs::JOB_MAX_ATTEMPTS;
use aruna_storage::storage::worker::FjallStorage;
use aruna_tasks::TaskHandle;
use aruna_test_support::jobs::{insert_job, read_job_record, record_attempt_intent};
use std::sync::Arc;
use tempfile::tempdir;
use ulid::Ulid;

fn job_id() -> JobId {
    aruna_operations::testing::fresh_job_id(
        PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
        BucketId::new(0).unwrap(),
    )
    .unwrap()
}

// A node whose registry lacks the executor cannot observe the container, so no
// number of sweeps by it may spend an attempt or terminalize the job.
#[tokio::test]
async fn missing_backend_spares() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: Some(Arc::new(ExecutorRegistry::new())),
        outbound_handle: None,
    });
    let job_id = job_id();
    let token = Ulid::generate();
    let mut record = JobRecord::new(
        job_id,
        JobPayload::Execution(execution_spec()),
        UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
        node_id(7),
        1,
        1,
        None,
    );
    record.state = JobState::Running;
    record.claim = Some(JobClaim {
        holder_node_id: node_id(7),
        claim_token: token,
        lease_expires_ms: 1,
    });
    insert_job(&context, &record).await.unwrap();
    record_attempt_intent(
        &context,
        job_id,
        token,
        AttemptIntent {
            attempt_no: 1,
            external_name: job_id.to_string().to_lowercase(),
            executor_kind: "kubernetes".to_string(),
            pinned_image: "alpine@sha256:digest".to_string(),
            attempt_epoch: 0,
        },
        None,
        None,
        unix_timestamp_millis(),
    )
    .await
    .unwrap();

    let runtime = JobsRuntime::new();
    let reconciler = ComputeReconciler::new(context.clone(), Arc::downgrade(&runtime));
    for sweep in 0..(JOB_MAX_ATTEMPTS + 2) {
        let lost = read_job_record(&context, job_id, None)
            .await
            .unwrap()
            .unwrap();
        reconciler.reconcile_lost_attempt(&context, lost).await;
        let after = read_job_record(&context, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.state, JobState::Running, "sweep {sweep}");
        assert_eq!(after.attempts, 0, "sweep {sweep}");
        assert!(after.attempt_intent.is_some(), "sweep {sweep}");
    }
}
