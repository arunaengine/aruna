//! What the external surfaces read back from one family, including the
//! responder-local diagnostics that stay outside the projection digest.

use aruna_core::effects::JobRecordFrame;
use aruna_core::structs::{
    AuthContext, JobFamilyId, JobRecordEnvelope, JobRecordKey, JobRecordKind, LogicalJobState,
    PhysicalExecutionState, SubmissionId,
};
use aruna_core::types::UserId;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::lifecycle::report::{AuditPaging, AuditRange, family_audit, family_report};
use crate::jobs::records::tests::fixture::{Family, REALM, context, user};
use crate::jobs::records::{AppendRecordConfig, AppendRecordOperation, RecordOrigin};

async fn append(context: &DriverContext, family: &Family, envelope: JobRecordEnvelope) {
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
    .expect("append completes");
}

async fn seed(context: &DriverContext, family: &Family, terminal: PhysicalExecutionState) {
    for envelope in family.run(1, 0, terminal) {
        append(context, family, envelope).await;
    }
}

fn auth() -> AuthContext {
    AuthContext {
        user_id: user(),
        realm_id: REALM,
        path_restrictions: None,
    }
}

#[tokio::test]
async fn reports_exact_outputs() {
    // A successful family reports its canonical execution and the exact
    // VersionIds that execution wrote, not whatever the object head became.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    seed(&ctx, &family, PhysicalExecutionState::Succeeded).await;

    let report = family_report(&ctx, &auth(), family.job_id)
        .await
        .expect("alias names a family")
        .expect("report is readable");

    assert_eq!(report.state, LogicalJobState::Succeeded);
    assert!(report.canonical_execution_id.is_some());
    assert_eq!(
        report
            .canonical_result
            .as_ref()
            .and_then(|result| result.exit_code),
        Some(0)
    );
    assert!(report.job.finished_at_ms.is_some());
    assert_eq!(report.submission_id, family.submission_id);
    assert_eq!(report.canonical_job_id, family.job_id);
    assert!(!report.outputs.is_empty());
    assert!(report.outputs.iter().all(|output| {
        !output.version_id.is_nil() && Some(output.execution_id) == report.canonical_execution_id
    }));
    assert!(!report.locally_exhausted);
    assert!(!report.partial);
}

#[tokio::test]
async fn marks_local_exhaustion() {
    // Every known execution terminal without success and no retry armed here is
    // a responder-local diagnostic, never a converged failure.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    seed(&ctx, &family, PhysicalExecutionState::Error).await;

    let report = family_report(&ctx, &auth(), family.job_id)
        .await
        .expect("alias names a family")
        .expect("report is readable");

    assert_eq!(report.state, LogicalJobState::Indeterminate);
    assert!(report.locally_exhausted);
    assert_eq!(report.executions, 1);
    assert_eq!(report.duplicate_successes, 0);
}

#[tokio::test]
async fn counts_duplicate_success() {
    // A second successful execution stays visible as a duplicate instead of
    // replacing the canonical one.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    seed(&ctx, &family, PhysicalExecutionState::Succeeded).await;
    for envelope in family.run(2, 1, PhysicalExecutionState::Succeeded) {
        append(&ctx, &family, envelope).await;
    }

    let report = family_report(&ctx, &auth(), family.job_id)
        .await
        .expect("alias names a family")
        .expect("report is readable");

    assert_eq!(report.executions, 2);
    assert_eq!(report.duplicate_successes, 1);
    assert!(!report.locally_exhausted);
}

#[tokio::test]
async fn audit_pages_records() {
    // The audit pages the immutable log by stable record key and refuses to
    // answer for a caller that did not submit the job.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    seed(&ctx, &family, PhysicalExecutionState::Succeeded).await;

    let first = family_audit(
        &ctx,
        &auth(),
        family.job_id,
        AuditRange::Family,
        AuditPaging::new(None, Some(3)).expect("paging is valid"),
    )
    .await
    .expect("alias names a family")
    .expect("audit is readable");
    assert_eq!(first.records.len(), 3);
    let cursor = first.next.expect("a full page carries a cursor");

    let second = family_audit(
        &ctx,
        &auth(),
        family.job_id,
        AuditRange::Family,
        AuditPaging::new(Some(cursor.as_slice().to_vec()), None).expect("paging is valid"),
    )
    .await
    .expect("alias names a family")
    .expect("audit is readable");
    assert!(!second.records.is_empty());
    assert!(second.next.is_none());

    let stranger = AuthContext {
        user_id: UserId::new(Ulid::from_bytes([12u8; 16]), REALM),
        realm_id: REALM,
        path_restrictions: None,
    };
    assert!(matches!(
        family_audit(
            &ctx,
            &stranger,
            family.job_id,
            AuditRange::Family,
            AuditPaging::new(None, None).expect("paging is valid"),
        )
        .await,
        Some(Err(crate::jobs::JobRouteError::NotFound))
    ));
}

#[test]
fn rejects_scope_mismatch() {
    let family = JobFamilyId {
        submission_id: SubmissionId([1u8; 32]),
        request_digest: [2u8; 32],
    };
    let cursor = JobRecordKey {
        family,
        kind: JobRecordKind::Spec,
        subject: [3u8; 32],
        sequence: 0,
    };
    let paging = AuditPaging::new(Some(cursor.to_bytes().to_vec()), None).expect("valid cursor");

    assert!(paging.validate_scope(AuditRange::Family, family).is_ok());
    assert!(
        paging
            .validate_scope(
                AuditRange::Family,
                JobFamilyId {
                    submission_id: family.submission_id,
                    request_digest: [4u8; 32],
                },
            )
            .is_err()
    );
    assert!(
        paging
            .validate_scope(
                AuditRange::Submission,
                JobFamilyId {
                    submission_id: family.submission_id,
                    request_digest: [4u8; 32],
                },
            )
            .is_ok()
    );
}
