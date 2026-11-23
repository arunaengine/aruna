//! Tests that every writer of family records and pending rows writes the family prune index.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::JobRecordFrame;
use aruna_core::keyspaces::FAMILY_PRUNE_KEYSPACE;
use aruna_core::structs::execution::job::family::{JobFamilyId, JobFamilyRecord};
use aruna_core::structs::execution::job::identity::RETENTION_MS;
use aruna_core::structs::execution::job::prune::TOMBSTONE_RETENTION_MS;
use aruna_core::structs::execution::job::record_keys::{
    PRUNE_DUE_PREFIX, parse_prune_due, prune_slot_key,
};
use aruna_core::types::Key;
use aruna_runner::driver::{DriverContext, drive};
use aruna_storage::scan::iter_prefix_page;

use super::admission::admit;
use super::terminal::{node_context, reserve_execution, seed_family};
use crate::jobs::records::admit::Admission;
use crate::jobs::records::append::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};
use crate::jobs::records::tests::prune::pass;
use crate::tests::records::{Family, REALM, context, read_row};

/// The due time the family's slot row names, if any.
async fn slot_due(ctx: &DriverContext, family: &JobFamilyId) -> Option<u64> {
    let key = prune_slot_key(family).to_vec();
    let value = read_row(ctx, FAMILY_PRUNE_KEYSPACE, key)
        .await
        .expect("slot read")?;
    Some(u64::from_be_bytes(value.try_into().expect("8 bytes")))
}

async fn due_rows(ctx: &DriverContext) -> Vec<(u64, JobFamilyId)> {
    let prefix = Key::from([PRUNE_DUE_PREFIX].as_slice());
    let page = iter_prefix_page(
        &ctx.storage_handle,
        FAMILY_PRUNE_KEYSPACE,
        Some(prefix),
        None,
        64,
        None,
    );
    let (rows, _) = page.await.expect("index read");
    (rows.iter())
        .filter_map(|(key, _)| parse_prune_due(key))
        .collect()
}

async fn append(
    ctx: &DriverContext,
    family: &Family,
    record: JobFamilyRecord,
    now_ms: u64,
) -> Admission {
    let frame = JobRecordFrame::new(family.sign(&family.holder, record)).expect("bounded");
    let operation = AppendRecordOperation::new(AppendRecordConfig {
        realm_id: REALM,
        local_node_id: family.holder.public(),
        record: frame,
        local: None,
        origin: RecordOrigin::Local,
        now_ms,
    });
    drive(operation, ctx)
        .await
        .expect("append completes")
        .admission
}

#[tokio::test]
async fn indexes_pending_family() {
    // A family holding only a pending row is indexed from that row on, and a later write
    // moves the slot to its later due time.
    let family = Family::new([80u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let spec = family.spec();
    let claim = JobFamilyRecord::Claim(family.claim(&spec));
    let pending = append(&ctx, &family, claim, 1_000).await;
    assert!(matches!(pending, Admission::Pending(_)));
    let first = 1_000 + RETENTION_MS;
    assert_eq!(slot_due(&ctx, &family.family()).await, Some(first));
    assert_eq!(due_rows(&ctx).await, vec![(first, family.family())]);

    let admitted = append(&ctx, &family, JobFamilyRecord::Spec(Box::new(spec)), 5_000).await;
    assert_eq!(admitted, Admission::Authentic);
    let later = 5_000 + RETENTION_MS;
    assert_eq!(slot_due(&ctx, &family.family()).await, Some(later));
    // The earlier row is stale, not rewritten.
    assert_eq!(due_rows(&ctx).await.len(), 2);
}

#[tokio::test]
async fn indexes_admission_writes() {
    // The submission admission and the receipt with its reservation write the index too.
    let family = Family::new([81u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    admit(&ctx, &family, family.job_id).await.expect("admitted");
    let due = slot_due(&ctx, &family.family()).await;
    // The admission runs at the runner's time, never before its configured one.
    assert!(due.is_some_and(|due| due >= 3_000 + RETENTION_MS));

    let receipted = Family::new([82u8; 32]);
    let (_dir, ctx) = node_context(&receipted, &receipted.target).await;
    let receipt = seed_family(&ctx, &receipted).await;
    let before = slot_due(&ctx, &receipted.family()).await;
    reserve_execution(&ctx, &receipted, &receipt).await;
    let after = slot_due(&ctx, &receipted.family()).await;
    assert!(
        after.is_some() && after != before,
        "the reservation wrote the slot"
    );
}

#[tokio::test]
async fn retries_keep_expiry() {
    // A pending record offered again keeps its first sight, so a late retry never schedules the
    // family past the pending expiry, and the pass then deletes the row with the index.
    let family = Family::new([83u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let claim = JobFamilyRecord::Claim(family.claim(&family.spec()));
    let expiry = 1_000 + TOMBSTONE_RETENTION_MS;
    append(&ctx, &family, claim.clone(), 1_000).await;

    let retried = append(&ctx, &family, claim, expiry - 1).await;

    assert!(matches!(retried, Admission::Pending(_)));
    let due = slot_due(&ctx, &family.family()).await.expect("indexed");
    assert!(
        due <= expiry,
        "the retry moved the family to {due}, past {expiry}"
    );
    // The pass runs at the runner's time, long past the fixture's expiry.
    pass(&ctx).await;
    assert_eq!(slot_due(&ctx, &family.family()).await, None);
    assert!(due_rows(&ctx).await.is_empty());
}
