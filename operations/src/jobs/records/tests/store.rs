//! The append, projection, and audit operations against real storage.

use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, JobRecordFrame, PageLimit, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::structs::{
    JobFamilyRecord, JobRecordEnvelope, JobState, LogicalJobState, PhysicalExecutionState,
    RealmConfigDocument,
};
use aruna_storage::{FjallStorage, StorageHandle};
use tempfile::TempDir;

use super::fixture::{Family, REALM};
use crate::driver::{DriverContext, drive};
use crate::jobs::records::admit::Admission;
use crate::jobs::records::audit::{AuditScope, FamilyAuditConfig, FamilyAuditOperation};
use crate::jobs::records::project::{FamilyRef, ProjectFamilyConfig, ProjectFamilyOperation};
use crate::jobs::records::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};

fn actor(node_id: aruna_core::NodeId) -> aruna_core::structs::Actor {
    aruna_core::structs::Actor {
        node_id,
        user_id: super::fixture::user(),
        realm_id: REALM,
    }
}

async fn fixture(
    config: &RealmConfigDocument,
    family_holder: aruna_core::NodeId,
) -> (TempDir, DriverContext) {
    let dir = tempfile::tempdir().expect("temp dir");
    let storage: StorageHandle =
        FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
    let target = DocumentSyncTarget::RealmConfig { realm_id: REALM };
    let event = storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: config
                .to_bytes(&actor(family_holder))
                .expect("config encodes")
                .into(),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    (
        dir,
        DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        },
    )
}

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
async fn bridges_local_row() {
    // The mutable job row is a local cache of the projection: a succeeded
    // family settles the row that the existing status surfaces read.
    let family = Family::new([3u8; 32]);
    let (_dir, context) = fixture(&family.config, family.holder.public()).await;
    let mut row = aruna_core::structs::JobRecord::new(
        family.job_id,
        aruna_core::structs::JobPayload::Execution(execution_payload()),
        super::fixture::user(),
        family.holder.public(),
        1_000,
        1_000,
        None,
    );
    row.state = JobState::Running;
    crate::jobs::store::insert_job(&context.storage_handle, &row)
        .await
        .expect("job row inserted");

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

fn execution_payload() -> aruna_core::structs::ExecutionSpec {
    aruna_core::structs::ExecutionSpec {
        group_id: ulid::Ulid::from_bytes([2u8; 16]),
        name: None,
        description: None,
        tags: Default::default(),
        image: "alpine:3".to_string(),
        entrypoint: None,
        command: Vec::new(),
        workdir: None,
        env: Default::default(),
        resources: Default::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    }
}
