//! The append, projection, and audit operations against real storage.

use aruna_core::effects::{JobRecordFrame, PageLimit, StorageEffect};
use aruna_core::keyspaces::JOB_KEYSPACE;
use aruna_core::structs::{
    JobFamilyRecord, JobId, JobRecordEnvelope, JobState, LogicalJobState, PhysicalExecutionState,
    job_record_key,
};

use super::fixture::{Family, REALM};
use crate::driver::{DriverContext, drive};
use crate::jobs::records::admit::Admission;
use crate::jobs::records::audit::{AuditScope, FamilyAuditConfig, FamilyAuditOperation};
use crate::jobs::records::project::{FamilyRef, ProjectFamilyConfig, ProjectFamilyOperation};
use crate::jobs::records::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};

use super::fixture::context as fixture;

async fn append(
    context: &DriverContext,
    family: &Family,
    envelope: JobRecordEnvelope,
) -> Admission {
    let record = JobRecordFrame::new(envelope).expect("bounded record");
    drive(
        AppendRecordOperation::new(AppendRecordConfig {
            realm_id: REALM,
            local_node_id: family.holder.public(),
            record,
            local: None,
            origin: RecordOrigin::Local,
            now_ms: 3_000,
        }),
        context,
    )
    .await
    .expect("append completes")
    .admission
}

async fn project(context: &DriverContext, family: &Family, rebuild: bool) -> LogicalJobState {
    let projected = drive(
        ProjectFamilyOperation::new(ProjectFamilyConfig {
            family: FamilyRef::Alias(family.job_id),
            now_ms: 3_100,
            rebuild,
        }),
        context,
    )
    .await
    .expect("projection completes");
    projected
        .projection
        .expect("family has an accepted alias")
        .state
}

#[tokio::test]
async fn admits_out_of_order() {
    // Records arriving before their evidence are retained and then admitted by
    // the append that supplies it, and the alias resolves to the family.
    let family = Family::new([1u8; 32]);
    let (_dir, context) = fixture(&family.config, family.holder.public()).await;
    let mut records = family.run(1, 0, PhysicalExecutionState::Succeeded);
    records.reverse();

    for envelope in records {
        append(&context, &family, envelope).await;
    }
    assert_eq!(
        project(&context, &family, false).await,
        LogicalJobState::Succeeded
    );
}

#[tokio::test]
async fn replays_without_change() {
    // A replayed record is a no-op, and the projection it feeds is stable.
    let family = Family::new([2u8; 32]);
    let (_dir, context) = fixture(&family.config, family.holder.public()).await;
    let records = family.run(1, 0, PhysicalExecutionState::Succeeded);
    for envelope in records.clone() {
        append(&context, &family, envelope).await;
    }
    let first = drive(
        ProjectFamilyOperation::new(ProjectFamilyConfig {
            family: FamilyRef::Family(family.family()),
            now_ms: 3_100,
            rebuild: false,
        }),
        &context,
    )
    .await
    .expect("projection completes");

    for envelope in records {
        assert_eq!(
            append(&context, &family, envelope).await,
            Admission::Duplicate
        );
    }
    let again = drive(
        ProjectFamilyOperation::new(ProjectFamilyConfig {
            family: FamilyRef::Family(family.family()),
            now_ms: 3_200,
            rebuild: false,
        }),
        &context,
    )
    .await
    .expect("projection completes");
    assert_eq!(first.projection, again.projection);
    assert_eq!(first.revision, again.revision);
    assert!(again.cached);
}

#[tokio::test]
async fn keeps_attempt_state() {
    // Family projection may settle its logical cache but never a physical attempt.
    let family = Family::new([3u8; 32]);
    let (_dir, context) = fixture(&family.config, family.holder.public()).await;
    let mut logical = aruna_core::structs::JobRecord::new(
        family.job_id,
        aruna_core::structs::JobPayload::Execution(super::fixture::payload()),
        super::fixture::user(),
        family.holder.public(),
        1_000,
        1_000,
        None,
    );
    logical.state = JobState::Queued;
    context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: JOB_KEYSPACE.to_string(),
            key: job_record_key(logical.job_id),
            value: logical.to_bytes().expect("logical row encodes").into(),
            txn_id: None,
        })
        .await;
    let physical_id = JobId::from_bytes([10u8; 16]);
    let mut physical = logical.clone();
    physical.job_id = physical_id;
    physical.state = JobState::Running;
    crate::jobs::store::insert_job(&context.storage_handle, &physical)
        .await
        .expect("physical row inserted");

    for envelope in family.run(1, 0, PhysicalExecutionState::Succeeded) {
        append(&context, &family, envelope).await;
    }
    assert_eq!(
        project(&context, &family, false).await,
        LogicalJobState::Succeeded
    );

    let stored = crate::jobs::store::read_job_record(&context.storage_handle, family.job_id, None)
        .await
        .expect("job row read")
        .expect("job row exists");
    assert_eq!(stored.state, JobState::Succeeded);
    let physical = crate::jobs::store::read_job_record(&context.storage_handle, physical_id, None)
        .await
        .expect("physical row read")
        .expect("physical row exists");
    assert_eq!(physical.state, JobState::Running);
}

#[tokio::test]
async fn pages_family_audit() {
    // Audit pages the immutable log in stable key order, and its cursor
    // resumes exactly where the previous page ended.
    let family = Family::new([4u8; 32]);
    let (_dir, context) = fixture(&family.config, family.holder.public()).await;
    let records = family.run(1, 0, PhysicalExecutionState::Succeeded);
    let total = records.len();
    for envelope in records {
        append(&context, &family, envelope).await;
    }

    let first = drive(
        FamilyAuditOperation::new(FamilyAuditConfig {
            scope: AuditScope::Submission(family.submission_id),
            cursor: None,
            limit: PageLimit::try_from(3).expect("bounded limit"),
        }),
        &context,
    )
    .await
    .expect("audit completes");
    assert_eq!(first.records.len(), 3);
    assert!(first.next.is_some());

    let mut seen = first.records.len();
    let mut cursor = first.next;
    while let Some(next) = cursor {
        let page = drive(
            FamilyAuditOperation::new(FamilyAuditConfig {
                scope: AuditScope::Submission(family.submission_id),
                cursor: Some(next),
                limit: PageLimit::try_from(3).expect("bounded limit"),
            }),
            &context,
        )
        .await
        .expect("audit completes");
        seen += page.records.len();
        cursor = page.next;
    }
    assert_eq!(seen, total);
}

#[tokio::test]
async fn retains_conflict_row() {
    // A same-key/different-digest record is retained as evidence and never
    // replaces the record already stored under that key.
    let family = Family::new([5u8; 32]);
    let (_dir, context) = fixture(&family.config, family.holder.public()).await;
    let spec = family.spec();
    let claim = family.claim(&spec);
    append(
        &context,
        &family,
        family.sign(&family.holder, JobFamilyRecord::Spec(Box::new(spec))),
    )
    .await;
    append(
        &context,
        &family,
        family.sign(&family.holder, JobFamilyRecord::Claim(claim)),
    )
    .await;

    let mut other = claim;
    other.accepted_at_ms = 9_999;
    assert_eq!(
        append(
            &context,
            &family,
            family.sign(&family.holder, JobFamilyRecord::Claim(other))
        )
        .await,
        Admission::Conflict
    );

    let page = drive(
        FamilyAuditOperation::new(FamilyAuditConfig {
            scope: AuditScope::Family(family.family()),
            cursor: None,
            limit: PageLimit::default(),
        }),
        &context,
    )
    .await
    .expect("audit completes");
    assert_eq!(page.conflicts.len(), 1);
    let stored: Vec<&JobRecordEnvelope> = page
        .records
        .iter()
        .filter(|envelope| matches!(envelope.record, JobFamilyRecord::Claim(_)))
        .collect();
    assert_eq!(stored.len(), 1);
    let JobFamilyRecord::Claim(kept) = &stored[0].record else {
        panic!("the stored record is a claim");
    };
    assert_eq!(kept.accepted_at_ms, claim.accepted_at_ms);
}

#[test]
fn names_transaction_conflict() {
    // Only a lost optimistic transaction is retryable; a status read must not
    // retry an unknown alias or a read failure.
    use crate::jobs::records::RecordStoreError;
    use aruna_core::errors::StorageError;

    assert!(RecordStoreError::Storage(StorageError::TransactionConflict).is_conflict());
    assert!(!RecordStoreError::Storage(StorageError::KeyNotFound).is_conflict());
    assert!(!RecordStoreError::UnknownAlias.is_conflict());
}
