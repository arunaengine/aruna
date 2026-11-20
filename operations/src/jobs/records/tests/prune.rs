//! Tests the family prune pass against storage: the entry step, body pages, held families and
//! stale rows. The pass runs at the runner's real time, which lies far past every fixture record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{JobRecordFrame, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    FAMILY_ALIAS_KEYSPACE, FAMILY_PROJECTION_KEYSPACE, FAMILY_PRUNE_KEYSPACE,
    FAMILY_RECORD_KEYSPACE, FAMILY_TOMBSTONE_KEYSPACE, INPUT_PAGE_KEYSPACE,
};
use aruna_core::structs::execution::job::family::{
    JobFamilyRecord, JobRecordEnvelope, PhysicalExecutionState,
};
use aruna_core::structs::execution::job::identity::RETENTION_MS;
use aruna_core::structs::execution::job::pages::{InputPage, PageEntries, PageManifest};
use aruna_core::structs::execution::job::prune::FamilyTombstone;
use aruna_core::structs::execution::job::record_keys::{
    alias_key, family_prefix, prune_due_key, prune_slot_key, record_key,
};
use aruna_core::structs::execution::job::record_rows::{from_bytes, to_bytes};
use aruna_core::types::{Key, Value};
use aruna_core::user::id::UserId;
use aruna_drivers::driver::{DriverContext, drive};
use aruna_storage::scan::iter_prefix_page;

use crate::jobs::lifecycle::family_reads::FamilyProjectionOperation;
use crate::jobs::records::admit::Admission;
use crate::jobs::records::append::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};
use crate::jobs::records::pages::{page_key, page_rows};
use crate::jobs::records::prune::FamilyPruneOutcome;
use crate::jobs::records::prune::pass::PruneFamiliesOperation;
use crate::jobs::route::JobRouteError;
use crate::tests::records::{Family, REALM, context, read_row, user};

pub(crate) async fn write(ctx: &DriverContext, writes: Vec<(String, Key, Value)>) {
    let effect = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    let event = ctx.storage_handle.send_storage_effect(effect).await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

/// The index rows that make `family` due at `due_ms`.
pub(crate) fn index(family: &Family, due_ms: u64) -> Vec<(String, Key, Value)> {
    vec![
        (
            FAMILY_PRUNE_KEYSPACE.to_string(),
            prune_due_key(due_ms, &family.family()),
            Value::from(&[][..]),
        ),
        (
            FAMILY_PRUNE_KEYSPACE.to_string(),
            prune_slot_key(&family.family()),
            Value::from(due_ms.to_be_bytes().as_slice()),
        ),
    ]
}

/// Stores `records`, the alias of the family's job and an index row due at `due_ms`.
async fn seed(ctx: &DriverContext, family: &Family, records: &[JobRecordEnvelope], due_ms: u64) {
    let mut writes: Vec<(String, Key, Value)> = (records.iter())
        .map(|envelope| {
            let bytes = to_bytes(envelope).expect("record encodes");
            let key = record_key(&envelope.key());
            (
                FAMILY_RECORD_KEYSPACE.to_string(),
                key,
                Value::from(bytes.as_slice()),
            )
        })
        .collect();
    writes.push((
        FAMILY_ALIAS_KEYSPACE.to_string(),
        alias_key(family.job_id, &family.family()),
        Value::from(&[][..]),
    ));
    writes.extend(index(family, due_ms));
    write(ctx, writes).await;
}

async fn rows(ctx: &DriverContext, key_space: &str, family: &Family) -> usize {
    let prefix = Some(family_prefix(&family.family()));
    let page = iter_prefix_page(&ctx.storage_handle, key_space, prefix, None, 512, None);
    page.await.expect("rows read").0.len()
}

async fn slot(ctx: &DriverContext, family: &Family) -> Option<u64> {
    let key = prune_slot_key(&family.family()).to_vec();
    let value = read_row(ctx, FAMILY_PRUNE_KEYSPACE, key)
        .await
        .expect("slot read")?;
    Some(u64::from_be_bytes(value.try_into().expect("8 bytes")))
}

async fn index_rows(ctx: &DriverContext) -> usize {
    let page = iter_prefix_page(
        &ctx.storage_handle,
        FAMILY_PRUNE_KEYSPACE,
        None,
        None,
        64,
        None,
    );
    page.await.expect("index read").0.len()
}

pub(crate) async fn tombstone(ctx: &DriverContext, family: &Family) -> Option<FamilyTombstone> {
    let key = family_prefix(&family.family()).to_vec();
    let value = read_row(ctx, FAMILY_TOMBSTONE_KEYSPACE, key)
        .await
        .expect("tombstone read")?;
    Some(from_bytes(&value).expect("tombstone decodes"))
}

pub(crate) async fn pass(ctx: &DriverContext) -> FamilyPruneOutcome {
    let serving = ctx.node_and_realm();
    drive(PruneFamiliesOperation::new(serving), ctx)
        .await
        .expect("prune pass")
}

async fn projected(
    ctx: &DriverContext,
    family: &Family,
    reader: UserId,
) -> Result<(), JobRouteError> {
    let read = FamilyProjectionOperation::new(family.job_id, 1).for_reader(reader);
    drive(read, ctx).await.map(|_| ())
}

#[tokio::test]
async fn prunes_in_steps() {
    // The entry step leaves a tombstone and an index row due at once; each later pass deletes a
    // page, and the index row moves to the tombstone's expiry only with the last page.
    let family = Family::new([90u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    seed(&ctx, &family, &family.stopped_run(), 2_000).await;

    let entered = pass(&ctx).await;
    assert!(
        tombstone(&ctx, &family)
            .await
            .is_some_and(|tombstone| !tombstone.finished)
    );
    assert_eq!(entered.pruned, 0);
    assert!(
        slot(&ctx, &family)
            .await
            .is_some_and(|due| due > RETENTION_MS)
    );
    assert_eq!(rows(&ctx, FAMILY_PROJECTION_KEYSPACE, &family).await, 0);
    assert_eq!(
        projected(&ctx, &family, user()).await,
        Err(JobRouteError::Pruned)
    );

    pass(&ctx).await;
    assert_eq!(rows(&ctx, FAMILY_RECORD_KEYSPACE, &family).await, 0);
    assert!(
        slot(&ctx, &family).await.is_some(),
        "the index row waits for the last page"
    );

    let last = pass(&ctx).await;
    assert_eq!(last.pruned, 1);
    let kept = tombstone(&ctx, &family).await.expect("tombstone kept");
    assert!(kept.finished);
    assert_eq!(kept.aliases, vec![family.job_id]);
    assert_eq!(index_rows(&ctx).await, 2);
    assert_eq!(slot(&ctx, &family).await, Some(kept.expires_at_ms()));
    assert_eq!(
        projected(&ctx, &family, user()).await,
        Err(JobRouteError::Pruned)
    );
}

#[tokio::test]
async fn expires_tombstone() {
    // A finished tombstone past its expiry goes with its aliases, and the old spec replayed after
    // that is refused as stale without it.
    let family = Family::new([99u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    seed(&ctx, &family, &[], 2_000).await;
    let finished = FamilyTombstone {
        pruned_at_ms: 1_000,
        last_record_ms: 900,
        finished: true,
        created_by: None,
        aliases: vec![family.job_id],
    };
    let value = Value::from(to_bytes(&finished).expect("tombstone encodes").as_slice());
    let prefix = family_prefix(&family.family());
    write(
        &ctx,
        vec![(FAMILY_TOMBSTONE_KEYSPACE.to_string(), prefix, value)],
    )
    .await;

    assert_eq!(pass(&ctx).await.pruned, 1);
    assert_eq!(tombstone(&ctx, &family).await, None);
    assert_eq!(index_rows(&ctx).await, 0);
    let alias = alias_key(family.job_id, &family.family()).to_vec();
    let alias = read_row(&ctx, FAMILY_ALIAS_KEYSPACE, alias).await;
    assert_eq!(alias.expect("alias read"), None);

    let spec = family.stopped_run()[0].clone();
    let replay = AppendRecordOperation::new(AppendRecordConfig {
        realm_id: REALM,
        local_node_id: family.holder.public(),
        record: JobRecordFrame::new(spec).expect("record frame"),
        local: None,
        origin: RecordOrigin::Local,
        now_ms: aruna_core::time::unix_timestamp_millis(),
    });
    let outcome = drive(replay, &ctx).await.expect("replay judged");
    assert_eq!(outcome.admission, Admission::Stale);
    assert_eq!(rows(&ctx, FAMILY_RECORD_KEYSPACE, &family).await, 0);
}

// Pruning a new incarnation keeps the aliases of the tombstone it replaces, so its expiry
// deletes the alias rows of every incarnation.
#[tokio::test]
async fn keeps_old_aliases() {
    let family = Family::new([96u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let old_job = aruna_core::structs::execution::job::identity::JobId::from_bytes([77u8; 16]);
    let previous = FamilyTombstone {
        pruned_at_ms: 500,
        last_record_ms: 400,
        finished: true,
        created_by: None,
        aliases: vec![old_job],
    };
    let encoded = Value::from(to_bytes(&previous).expect("tombstone encodes").as_slice());
    let prefix = family_prefix(&family.family());
    write(
        &ctx,
        vec![(FAMILY_TOMBSTONE_KEYSPACE.to_string(), prefix, encoded)],
    )
    .await;
    seed(&ctx, &family, &family.stopped_run(), 2_000).await;
    for _ in 0..3 {
        pass(&ctx).await;
    }
    let kept = tombstone(&ctx, &family).await.expect("tombstone kept");
    assert!(kept.finished);
    assert_eq!(kept.aliases, vec![family.job_id, old_job]);
}

#[tokio::test]
async fn moves_held_family() {
    // An execution without a stop record holds the family, and its index row moves a retention
    // later instead of being read on every pass.
    let family = Family::new([91u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let records = family.run(1, 0, PhysicalExecutionState::Succeeded);
    seed(&ctx, &family, &records, 2_000).await;

    pass(&ctx).await;

    assert!(
        slot(&ctx, &family)
            .await
            .is_some_and(|due| due > 2 * RETENTION_MS)
    );
    assert_eq!(
        index_rows(&ctx).await,
        2,
        "the old due row is gone, the new one written"
    );
    assert_eq!(
        rows(&ctx, FAMILY_RECORD_KEYSPACE, &family).await,
        records.len()
    );
    assert!(tombstone(&ctx, &family).await.is_none());
}

#[tokio::test]
async fn drops_stale_row() {
    // A due row the family's slot no longer names is dropped without deciding the family.
    let family = Family::new([92u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let later = u64::MAX / 2;
    seed(&ctx, &family, &family.stopped_run(), later).await;
    write(&ctx, vec![index(&family, 2_000).remove(0)]).await;

    pass(&ctx).await;

    assert_eq!(slot(&ctx, &family).await, Some(later));
    assert_eq!(index_rows(&ctx).await, 2);
    assert!(tombstone(&ctx, &family).await.is_none());
}

/// An input page with no entries and its one-page manifest.
fn input_page(index: u32) -> (InputPage, PageManifest) {
    let page = InputPage {
        index,
        entries: PageEntries::Launch(Vec::new()),
        next: None,
    };
    let first = page.digest().expect("page digest");
    let manifest = PageManifest {
        count: 0,
        pages: 1,
        first,
    };
    (page, manifest)
}

/// Re-signs the record at `index` after `change` edited it.
fn resign(
    family: &Family,
    records: &mut [JobRecordEnvelope],
    index: usize,
    change: impl FnOnce(&mut JobFamilyRecord),
) {
    let mut record = records[index].record.clone();
    change(&mut record);
    records[index] = family.sign(&family.holder, record);
}

async fn stored_page(ctx: &DriverContext, family: &Family, page: &InputPage) -> bool {
    let digest = page.digest().expect("page digest");
    let key = page_key(&family.submission_id, &digest).to_vec();
    read_row(ctx, INPUT_PAGE_KEYSPACE, key)
        .await
        .expect("page read")
        .is_some()
}

#[tokio::test]
async fn prunes_input_pages() {
    // The pruned family's own input pages go with it; a page another family of the same
    // submission still names stays.
    let family = Family::new([94u8; 32]);
    let sibling = Family::new([98u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let ((own, own_manifest), (shared, shared_manifest)) = (input_page(1), input_page(2));
    let submission = family.submission_id;
    write(
        &ctx,
        page_rows(&submission, &[own.clone(), shared.clone()]).unwrap(),
    )
    .await;
    let mut records = family.stopped_run();
    resign(&family, &mut records, 0, |record| {
        if let JobFamilyRecord::Spec(spec) = record {
            spec.input_pages = Some(own_manifest);
        }
    });
    resign(&family, &mut records, 3, |record| {
        if let JobFamilyRecord::Launch(launch) = record {
            launch.input_pages = Some(shared_manifest);
        }
    });
    let mut other = sibling.spec();
    other.input_pages = Some(shared_manifest);
    let other = sibling.sign(&sibling.holder, JobFamilyRecord::Spec(Box::new(other)));
    seed(&ctx, &family, &records, 2_000).await;
    let other_value = Value::from(to_bytes(&other).unwrap().as_slice());
    let other_row = (
        FAMILY_RECORD_KEYSPACE.to_string(),
        record_key(&other.key()),
        other_value,
    );
    write(&ctx, vec![other_row]).await;

    for _ in 0..4 {
        pass(&ctx).await;
    }

    assert!(
        tombstone(&ctx, &family)
            .await
            .is_some_and(|tombstone| tombstone.finished)
    );
    assert!(!stored_page(&ctx, &family, &own).await);
    assert!(stored_page(&ctx, &family, &shared).await);
}
