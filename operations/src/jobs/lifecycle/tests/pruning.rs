//! Tests that local work of the executing node holds its job family through the prune pass.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{JobRecordFrame, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{FAMILY_OUTBOX_KEYSPACE, JOB_KEYSPACE, JOB_RESERVATION_KEYSPACE};
use aruna_core::structs::execution::job::family::{
    ExecutionStop, JobFamilyRecord, JobRecordBody, PhysicalExecutionState, StopEvidence,
};
use aruna_core::structs::execution::job::local::job_record_key;
use aruna_core::structs::execution::job::record_keys::family_prefix;
use aruna_core::types::Key;
use aruna_drivers::driver::{DriverContext, drive};
use aruna_storage::scan::iter_prefix_page;

use super::terminal::{node_context, reserve_execution, seed_family};
use crate::jobs::lifecycle::reservation::reservation_key;
use crate::jobs::records::admit::Admission;
use crate::jobs::records::append::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};
use crate::jobs::records::tests::prune::{index, pass, tombstone, write};
use crate::tests::records::{Family, REALM};

async fn append(ctx: &DriverContext, family: &Family, record: JobFamilyRecord) {
    let frame = JobRecordFrame::new(family.sign(&family.target, record)).expect("bounded");
    let operation = AppendRecordOperation::new(AppendRecordConfig {
        realm_id: REALM,
        local_node_id: family.target.public(),
        record: frame,
        local: None,
        origin: RecordOrigin::Local,
        now_ms: 5_000,
    });
    let outcome = drive(operation, ctx).await.expect("append completes");
    assert_eq!(outcome.admission, Admission::Authentic);
}

async fn delete(ctx: &DriverContext, deletes: Vec<(String, Key)>) {
    let effect = StorageEffect::BatchDelete {
        deletes,
        txn_id: None,
    };
    let event = ctx.storage_handle.send_storage_effect(effect).await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchDeleteResult { .. })
    ));
}

#[tokio::test]
async fn local_job_holds() {
    // A settled, stopped and delivered family stays while its executor still holds the local job
    // record and reservation, and goes once both are gone.
    let family = Family::new([96u8; 32]);
    let (_dir, ctx) = node_context(&family, &family.target).await;
    let receipt = seed_family(&ctx, &family).await;
    let job = reserve_execution(&ctx, &family, &receipt).await;
    let root = receipt.digest().expect("receipt digest");
    let failed = PhysicalExecutionState::Failed;
    let update = family.update(&receipt, 0, root, failed, None);
    append(
        &ctx,
        &family,
        JobFamilyRecord::Update(Box::new(update.clone())),
    )
    .await;
    let stop = ExecutionStop {
        execution_id: receipt.execution_id,
        submission_id: family.submission_id,
        request_digest: family.request_digest,
        executor_node_id: family.target.public(),
        receipt_digest: root,
        update_sequence: 0,
        update_digest: update.digest().expect("update digest"),
        output_digest: None,
        evidence: StopEvidence::Tombstoned,
        stopped_at_ms: 1_100,
    };
    append(&ctx, &family, JobFamilyRecord::Stop(Box::new(stop))).await;
    let prefix = Some(family_prefix(&family.family()));
    let page = iter_prefix_page(
        &ctx.storage_handle,
        FAMILY_OUTBOX_KEYSPACE,
        prefix,
        None,
        64,
        None,
    );
    let (outbox, _) = page.await.expect("outbox read");
    let delivered = (outbox.into_iter()).map(|(key, _)| (FAMILY_OUTBOX_KEYSPACE.to_string(), key));
    delete(&ctx, delivered.collect()).await;
    write(&ctx, index(&family, 2_000)).await;

    pass(&ctx).await;
    let held = tombstone(&ctx, &family).await;
    assert!(
        held.is_none(),
        "the local job record and reservation hold the family"
    );

    delete(
        &ctx,
        vec![
            (JOB_KEYSPACE.to_string(), job_record_key(job.job_id)),
            (
                JOB_RESERVATION_KEYSPACE.to_string(),
                reservation_key(receipt.execution_id),
            ),
        ],
    )
    .await;
    // The held row moved a retention ahead; it is made due again for the next pass.
    write(&ctx, index(&family, 2_000)).await;
    pass(&ctx).await;
    assert!(
        tombstone(&ctx, &family).await.is_some(),
        "without local work the family is pruned"
    );
}
