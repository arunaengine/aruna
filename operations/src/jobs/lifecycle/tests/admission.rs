//! Local admission, idempotent replay, conflict visibility, and the reads the
//! family projection answers afterwards.

use aruna_core::effects::{JobRecordFrame, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{JOB_ADMISSION_QUOTA_KEYSPACE, JOB_FAMILY_OUTBOX_KEYSPACE};
use aruna_core::structs::{
    AuthContext, JobFamilyRecord, JobId, JobInputFact, JobState, LogicalJobSpec, SubmissionClaim,
    WorkspaceMode,
};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::lifecycle::admit::{
    AdmissionCandidate, AdmitSubmissionConfig, AdmitSubmissionOperation,
};
use crate::jobs::lifecycle::ids::{SubmissionRequest, SubmissionScope, seal_workspace};
use crate::jobs::lifecycle::routing::{family_of_alias, family_status};
use crate::jobs::lifecycle::{LifecycleError, submit_external_job};
use crate::jobs::records::tests::fixture::{Family, REALM, context, payload, user};
use crate::jobs::store::iter_prefix_page;

fn frame(record: JobFamilyRecord, family: &Family) -> JobRecordFrame {
    JobRecordFrame::new(family.sign(&family.holder, record)).expect("bounded record")
}

fn candidate(family: &Family, spec: LogicalJobSpec, claim: SubmissionClaim) -> AdmissionCandidate {
    AdmissionCandidate {
        job_id: spec.job_id,
        spec: frame(JobFamilyRecord::Spec(Box::new(spec)), family),
        claim: frame(JobFamilyRecord::Claim(claim), family),
    }
}

async fn admit(
    context: &DriverContext,
    family: &Family,
    job_id: JobId,
) -> Result<(JobId, bool), LifecycleError> {
    let spec = family.spec_for(job_id, family.holder.public());
    let claim = family.claim(&spec);
    drive(
        AdmitSubmissionOperation::new(AdmitSubmissionConfig {
            realm_id: REALM,
            local_node_id: family.holder.public(),
            submission_id: family.submission_id,
            request_digest: family.request_digest,
            candidate: Box::new(candidate(family, spec, claim)),
            now_ms: 3_000,
            quota_refusal: None,
            quota_revision: None,
        }),
        context,
    )
    .await
    .map(|admitted| (admitted.job_id, admitted.created))
}

#[tokio::test]
async fn rejects_stale_quota() {
    // A concurrent fresh admission invalidates the allow decision.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let spec = family.spec();
    let claim = family.claim(&spec);
    let event = ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: JOB_ADMISSION_QUOTA_KEYSPACE.to_string(),
            key: spec.group_id.to_bytes().as_slice().into(),
            value: postcard::to_allocvec(&1u64)
                .expect("revision encodes")
                .into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    let error = drive(
        AdmitSubmissionOperation::new(AdmitSubmissionConfig {
            realm_id: REALM,
            local_node_id: family.holder.public(),
            submission_id: family.submission_id,
            request_digest: family.request_digest,
            candidate: Box::new(candidate(&family, spec, claim)),
            now_ms: 3_000,
            quota_refusal: None,
            quota_revision: Some(0),
        }),
        &ctx,
    )
    .await
    .expect_err("stale quota check is refused");

    assert_eq!(
        error,
        LifecycleError::Storage(StorageError::TransactionConflict)
    );
    assert_eq!(family_of_alias(&ctx, family.job_id).await, Ok(None));
}

#[tokio::test]
async fn admits_local_claim() {
    // A holder commits the spec, the claim, the alias, and both replication
    // entries in one transaction before it may answer success.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;

    let (job_id, created) = admit(&ctx, &family, family.job_id).await.expect("admitted");

    assert!(created);
    assert_eq!(job_id, family.job_id);
    assert_eq!(
        family_of_alias(&ctx, job_id).await,
        Ok(Some(family.family()))
    );
    let record = crate::jobs::store::read_job_record(&ctx.storage_handle, job_id, None)
        .await
        .expect("logical row read")
        .expect("logical row exists");
    assert_eq!(record.state, JobState::Queued);
    assert_eq!(record.retention_ms, family.spec().retention_ms);
    let (queued, _) = iter_prefix_page(
        &ctx.storage_handle,
        JOB_FAMILY_OUTBOX_KEYSPACE,
        None,
        None,
        8,
        None,
    )
    .await
    .expect("outbox scan");
    assert_eq!(queued.len(), 2);
}

#[tokio::test]
async fn replays_matching_claim() {
    // A second admission of the same request returns the canonical alias and
    // commits nothing new, whichever alias the replay would have minted.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let (first, _) = admit(&ctx, &family, family.job_id).await.expect("admitted");

    let (second, created) = admit(&ctx, &family, JobId::from_bytes([4u8; 16]))
        .await
        .expect("replay admitted");

    assert!(!created);
    assert_eq!(second, first);
    assert_eq!(
        family_of_alias(&ctx, JobId::from_bytes([4u8; 16])).await,
        Ok(None)
    );
}

#[tokio::test]
async fn reports_key_conflict() {
    // The same idempotency key with a different request is a visible conflict,
    // and the already admitted job stays untouched.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    let (first, _) = admit(&ctx, &family, family.job_id).await.expect("admitted");

    let other = Family::new([2u8; 32]);
    let error = admit(&ctx, &other, JobId::from_bytes([5u8; 16]))
        .await
        .expect_err("different request conflicts");

    assert_eq!(
        error,
        LifecycleError::IdempotencyConflict {
            existing_job_id: first
        }
    );
    assert_eq!(
        family_of_alias(&ctx, first).await,
        Ok(Some(family.family()))
    );
}

#[tokio::test]
async fn refuses_undeliverable_submit() {
    // Without a reachable holder the ingress accepts nothing: no alias, no
    // queued record, and an availability error rather than a queued success.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;

    let error = submit_external_job(
        &ctx,
        payload(),
        user(),
        Some("idempotency".to_string()),
        WorkspaceMode::Kept,
        None,
        60_000,
        None,
    )
    .await
    .expect_err("no holder is reachable");

    assert!(matches!(
        error,
        crate::jobs::submit::SubmitJobError::PlacementUnavailable(_)
    ));
    let (queued, _) = iter_prefix_page(
        &ctx.storage_handle,
        JOB_FAMILY_OUTBOX_KEYSPACE,
        None,
        None,
        8,
        None,
    )
    .await
    .expect("outbox scan");
    assert!(queued.is_empty());
}

#[tokio::test]
async fn answers_by_alias() {
    // Status is reduced from the family, so any node holding the records
    // answers for the alias, and another submitter's job stays absent.
    let family = Family::new([1u8; 32]);
    let (_dir, ctx) = context(&family.config, family.holder.public()).await;
    admit(&ctx, &family, family.job_id).await.expect("admitted");
    let auth = AuthContext {
        user_id: user(),
        realm_id: REALM,
        path_restrictions: None,
    };

    let status = family_status(&ctx, &auth, family.job_id)
        .await
        .expect("alias names a family")
        .expect("status is readable");
    assert_eq!(status.job.state, JobState::Queued);
    assert_eq!(status.job.job_id, family.job_id);

    let stranger = AuthContext {
        user_id: aruna_core::types::UserId::new(Ulid::from_bytes([12u8; 16]), REALM),
        realm_id: REALM,
        path_restrictions: None,
    };
    assert!(matches!(
        family_status(&ctx, &stranger, family.job_id).await,
        Some(Err(crate::jobs::JobRouteError::NotFound))
    ));
}

#[test]
fn identity_is_deterministic() {
    // The same normalized request yields one identity everywhere, and the
    // sealed workspace choice is part of the digest it commits to.
    let mut spec = payload();
    seal_workspace(&mut spec, WorkspaceMode::Kept, None).expect("workspace seals");
    let request = SubmissionRequest {
        created_by: user(),
        spec: spec.clone(),
        scope: SubmissionScope::Keyed("idempotency".to_string()),
        retention_ms: 60_000,
        ingress_node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
        input_facts: Vec::new(),
        output_policies: Vec::new(),
    };
    let identity = request.identity().expect("identity derives");

    assert_eq!(identity, request.identity().expect("identity derives"));
    let mut other = spec.clone();
    other
        .tags
        .insert("aruna-engine.org/label/zone".to_string(), "eu".to_string());
    let changed = SubmissionRequest {
        spec: other,
        ..request.clone()
    }
    .identity()
    .expect("identity derives");
    assert_eq!(changed.submission_id, identity.submission_id);
    assert_ne!(changed.request_digest, identity.request_digest);

    let mut moved = request.clone();
    moved.ingress_node_id = iroh::SecretKey::from_bytes(&[10u8; 32]).public();
    assert_ne!(moved.identity().expect("identity derives"), identity);

    let mut pinned = request;
    pinned.input_facts.push(JobInputFact {
        destination_key: "reads.fastq".to_string(),
        source_node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
        version_id: Ulid::from_bytes([1u8; 16]),
        blake3: [2u8; 32],
        bytes: 3,
        policies: Vec::new(),
    });
    assert_ne!(pinned.identity().expect("identity derives"), identity);
}

#[test]
fn refuses_reserved_tags() {
    // A caller may not preset the reserved scheduling tags and steer another
    // workspace than the one the ingress sealed.
    let mut spec = payload();
    seal_workspace(&mut spec, WorkspaceMode::Kept, None).expect("workspace seals");
    assert!(seal_workspace(&mut spec, WorkspaceMode::Kept, None).is_err());
}
