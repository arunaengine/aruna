//! Admission is decided against this node's own view and evidence only.

use std::collections::BTreeMap;

use aruna_core::structs::{
    JobFamilyRecord, JobRecordEnvelope, JobRecordError, JobRecordKey, JobRecordKind, LocalExecution,
};

use super::fixture::{Family, secret};
use crate::jobs::records::admit::{Admission, FamilyState, plan_append, relayable};
use crate::jobs::records::rows::{PendingNeed, PendingRecord};
use crate::jobs::records::verify::FamilyView;

type Stored = BTreeMap<JobRecordKey, JobRecordEnvelope>;

fn stored(records: &[JobRecordEnvelope]) -> Stored {
    records
        .iter()
        .map(|envelope| (envelope.key(), envelope.clone()))
        .collect()
}

fn state<'a>(view: Option<&'a FamilyView>, records: &'a Stored) -> FamilyState<'a> {
    FamilyState {
        view,
        stored: records,
        local: None,
        now_ms: 2_000,
    }
}

#[test]
fn pends_then_admits() {
    // A claim that arrives before its spec is retained, not refused, and the
    // spec's own admission then admits it in the same commit.
    let family = Family::new([1u8; 32]);
    let view = family.view();
    let spec = family.spec();
    let claim = family.sign(&family.holder, JobFamilyRecord::Claim(family.claim(&spec)));
    let empty = Stored::new();

    let (admission, plan) = plan_append(&state(Some(&view), &empty), &[], claim.clone(), None);
    assert_eq!(
        admission,
        Admission::Pending(PendingNeed::Evidence(JobRecordKind::Spec))
    );
    assert_eq!(plan.pending.len(), 1);
    assert!(plan.admitted.is_empty());

    let retained: Vec<(JobRecordKey, PendingRecord)> = plan.pending;
    let sealed = family.sign(&family.holder, JobFamilyRecord::Spec(Box::new(spec)));
    let (admission, plan) =
        plan_append(&state(Some(&view), &empty), &retained, sealed.clone(), None);
    assert_eq!(admission, Admission::Authentic);
    assert_eq!(plan.admitted.len(), 2);
    assert_eq!(plan.cleared, vec![claim.key()]);
    assert!(plan.admitted.iter().all(|admitted| admitted.authentic));
}

#[test]
fn retains_conflict() {
    // Two different bytes under one key: the stored record stays, and the
    // refused one is retained as explicit evidence instead of overwriting it.
    let family = Family::new([2u8; 32]);
    let view = family.view();
    let spec = family.spec();
    let claim = family.claim(&spec);
    let mut other = claim;
    other.accepted_at_ms = 9_999;
    let records = stored(&[
        family.sign(&family.holder, JobFamilyRecord::Spec(Box::new(spec))),
        family.sign(&family.holder, JobFamilyRecord::Claim(claim)),
    ]);
    let candidate = family.sign(&family.holder, JobFamilyRecord::Claim(other));

    let (admission, plan) = plan_append(
        &state(Some(&view), &records),
        &[],
        candidate.clone(),
        Some(secret(2).public()),
    );
    assert_eq!(admission, Admission::Conflict);
    assert!(plan.admitted.is_empty());
    assert_eq!(plan.conflicts.len(), 1);
    let (key, row) = &plan.conflicts[0];
    assert_eq!(*key, candidate.key());
    assert_eq!(row.retained, records[key].digest().expect("stored digest"));
    assert_eq!(row.relayed_by, Some(secret(2).public()));
}

#[test]
fn replays_idempotently() {
    // The same key with the same digest is a no-op, however often it arrives.
    let family = Family::new([3u8; 32]);
    let view = family.view();
    let spec = family.sign(
        &family.holder,
        JobFamilyRecord::Spec(Box::new(family.spec())),
    );
    let records = stored(std::slice::from_ref(&spec));

    let (admission, plan) = plan_append(&state(Some(&view), &records), &[], spec, None);
    assert_eq!(admission, Admission::Duplicate);
    assert!(plan.admitted.is_empty());
    assert!(plan.conflicts.is_empty());
    assert!(plan.pending.is_empty());
}

#[test]
fn defers_without_view() {
    // A node that cannot resolve its holder view must retry later; verifying
    // against an empty view would reject every holder-authored record.
    let family = Family::new([4u8; 32]);
    let spec = family.sign(
        &family.holder,
        JobFamilyRecord::Spec(Box::new(family.spec())),
    );
    let empty = Stored::new();

    let (admission, plan) = plan_append(&state(None, &empty), &[], spec, None);
    assert_eq!(admission, Admission::Pending(PendingNeed::LocalView));
    assert_eq!(plan.pending.len(), 1);
    assert_eq!(plan.pending[0].1.need, PendingNeed::LocalView);
}

#[test]
fn retains_pending_conflict() {
    // A same-key conflict must not replace the first record's pending retry row.
    let family = Family::new([8u8; 32]);
    let spec = family.spec();
    let claim = family.claim(&spec);
    let mut conflicting_claim = claim;
    conflicting_claim.accepted_at_ms = 9_999;
    let first = family.sign(&family.holder, JobFamilyRecord::Claim(claim));
    let conflicting = family.sign(&family.holder, JobFamilyRecord::Claim(conflicting_claim));
    let empty = Stored::new();
    let (admission, first_plan) = plan_append(&state(None, &empty), &[], first.clone(), None);
    assert_eq!(admission, Admission::Pending(PendingNeed::LocalView));
    let mut retained = first_plan.pending;
    retained[0].1.attempts = 7;

    let (admission, plan) = plan_append(&state(None, &empty), &retained, conflicting, None);

    assert_eq!(admission, Admission::Conflict);
    assert!(plan.pending.is_empty());
    assert_eq!(plan.conflicts.len(), 1);
    assert_eq!(
        plan.conflicts[0].1.retained,
        first.digest().expect("first digest")
    );
    assert_eq!(retained[0].1.attempts, 7);
}

#[test]
fn refuses_non_holder() {
    // A valid realm key is identity, never family-holder authority.
    let family = Family::new([5u8; 32]);
    let view = family.view();
    let outsider = secret(9);
    let spec = family.spec_for(family.job_id, outsider.public());
    let candidate = family.sign(&outsider, JobFamilyRecord::Spec(Box::new(spec)));
    let empty = Stored::new();

    let (admission, plan) = plan_append(&state(Some(&view), &empty), &[], candidate, None);
    assert_eq!(
        admission,
        Admission::Rejected(JobRecordError::NotHolder(JobRecordKind::Spec))
    );
    assert!(plan.admitted.is_empty());
    assert!(plan.pending.is_empty());
    assert!(plan.conflicts.is_empty());
}

#[test]
fn keeps_local_record() {
    // An output proven only by this node's own fence is stored and projected
    // here, and is never queued for replication.
    let family = Family::new([6u8; 32]);
    let view = family.view();
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let receipt = family.receipt(&launch, 1);
    let output = family.output(&receipt);
    let local = LocalExecution {
        node_id: family.target.public(),
        execution_id: output.execution_id,
        fence_digest: output.receipt_digest,
        spec_digest: output.spec_digest,
    };
    let candidate = family.sign(&family.target, JobFamilyRecord::Output(Box::new(output)));
    let empty = Stored::new();
    let state = FamilyState {
        view: Some(&view),
        stored: &empty,
        local: Some(&local),
        now_ms: 2_000,
    };

    let (admission, plan) = plan_append(&state, &[], candidate, None);
    assert_eq!(admission, Admission::Local);
    assert_eq!(plan.admitted.len(), 1);
    assert!(!plan.admitted[0].authentic);
    assert!(relayable(&plan, family.target.public()).is_empty());
}

#[test]
fn waits_for_predecessor() {
    // A later execution update cannot become durable authority before sequence zero.
    let family = Family::new([7u8; 32]);
    let view = family.view();
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);
    let receipt = family.receipt(&launch, 1);
    let records = stored(&[family.sign(
        &family.target,
        JobFamilyRecord::Receipt(Box::new(receipt.clone())),
    )]);
    let update = family.update(
        &receipt,
        1,
        [9u8; 32],
        aruna_core::structs::PhysicalExecutionState::Running,
        None,
    );
    let candidate = family.sign(&family.target, JobFamilyRecord::Update(Box::new(update)));

    let (admission, plan) = plan_append(&state(Some(&view), &records), &[], candidate, None);

    assert_eq!(
        admission,
        Admission::Pending(PendingNeed::Evidence(JobRecordKind::Update))
    );
    assert!(plan.admitted.is_empty());
}
