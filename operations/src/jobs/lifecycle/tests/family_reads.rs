//! Family reads that must be complete before anything is decided from them.

use aruna_core::effects::{JobRecordFrame, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::JOB_FAMILY_RECORD_KEYSPACE;
use aruna_core::structs::{
    ExecutionReceipt, JobFamilyRecord, JobRecordKey, JobRecordKind, LogicalJobSpec,
};
use aruna_core::types::Value;

use crate::driver::{DriverContext, drive};
use crate::jobs::lifecycle::updates::chain_for;
use crate::jobs::records::keys::record_key;
use crate::jobs::records::tests::fixture::{Family, REALM, context};
use crate::jobs::records::{
    AppendRecordConfig, AppendRecordOperation, FamilyReadError, RecordOrigin, load_family_complete,
    load_kind_complete,
};

/// Spec, claim, budget, launch, and the receipt that authorizes one execution.
async fn seed(ctx: &DriverContext, family: &Family) -> (LogicalJobSpec, ExecutionReceipt) {
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let receipt = family.receipt(&launch, 1);
    let published = [
        (
            &family.holder,
            JobFamilyRecord::Spec(Box::new(spec.clone())),
        ),
        (&family.holder, JobFamilyRecord::Claim(family.claim(&spec))),
        (
            &family.holder,
            JobFamilyRecord::Budget(family.budget(&spec, family.holder.public())),
        ),
        (
            &family.holder,
            JobFamilyRecord::Launch(Box::new(launch.clone())),
        ),
        (
            &family.target,
            JobFamilyRecord::Receipt(Box::new(receipt.clone())),
        ),
    ];
    for (key, record) in published {
        let frame = JobRecordFrame::new(family.sign(key, record)).expect("bounded record");
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
    (spec, receipt)
}

/// Stores bytes no record decoder can read under a well-formed receipt key.
async fn poison(ctx: &DriverContext, family: &Family) {
    let key = record_key(&JobRecordKey {
        family: family.family(),
        kind: JobRecordKind::Receipt,
        subject: [42u8; 32],
        sequence: 0,
    });
    let event = ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: JOB_FAMILY_RECORD_KEYSPACE.to_string(),
            key,
            value: Value::from([0xffu8; 16].as_slice()),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

#[tokio::test]
async fn reads_kind_only() {
    // An exact question reads its own kind, so unrelated family history can
    // never push the answer past the bounded read.
    let family = Family::new([5u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let (_spec, receipt) = seed(&ctx, &family).await;

    let all = load_family_complete(&ctx, family.family())
        .await
        .expect("family reads completely");
    let receipts = load_kind_complete(&ctx, family.family(), JobRecordKind::Receipt)
        .await
        .expect("receipts read completely");
    assert_eq!(all.len(), 5);
    assert_eq!(receipts.len(), 1);
    assert!(matches!(
        &receipts[0].record,
        JobFamilyRecord::Receipt(stored) if stored.execution_id == receipt.execution_id
    ));
}

#[tokio::test]
async fn undecodable_blocks_chain() {
    // An undecodable row is missing evidence, not an empty family: the chain
    // must stop resolving instead of publishing from what happened to load.
    let family = Family::new([6u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let (spec, receipt) = seed(&ctx, &family).await;
    assert!(
        chain_for(&ctx, spec.job_id, receipt.execution_id)
            .await
            .is_some()
    );

    poison(&ctx, &family).await;

    assert!(matches!(
        load_family_complete(&ctx, family.family()).await,
        Err(FamilyReadError::Decode(_))
    ));
    assert!(matches!(
        load_kind_complete(&ctx, family.family(), JobRecordKind::Receipt).await,
        Err(FamilyReadError::Decode(_))
    ));
    assert!(
        chain_for(&ctx, spec.job_id, receipt.execution_id)
            .await
            .is_none()
    );
}
