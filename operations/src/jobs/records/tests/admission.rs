//! Tests that admission decides only from this node's stored evidence and holder view.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::structs::execution::job::{
    CLOCK_MARGIN_MS, ExecutionStop, FamilyTombstone, JobFamilyRecord, JobRecordBody,
    JobRecordEnvelope, JobRecordError, JobRecordKey, JobRecordKind, LocalExecution,
    PhysicalExecutionState, StopEvidence, TOMBSTONE_RETENTION_MS,
};

use crate::jobs::records::admit::{
    Admission, AppendPlan, FamilyState, plan_append, plan_bundle, relayable,
};
use crate::jobs::records::verify::FamilyView;
use crate::tests::records::{Family, node, secret};
use aruna_core::structs::execution::job::record_rows::{PendingNeed, PendingRecord};

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
        tombstone: None,
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
    let signed = family.sign(&family.holder, JobFamilyRecord::Spec(Box::new(spec)));
    let (admission, plan) =
        plan_append(&state(Some(&view), &empty), &retained, signed.clone(), None);
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
fn retains_non_holder() {
    // Holder authority is view-relative: a member this view does not rank is
    // retained and judged again, never projected and never dropped.
    let mut family = Family::new([5u8; 32]);
    family.config.ensure_node(
        node(7),
        aruna_core::structs::identity::realm::RealmNodeKind::Server,
    );
    let view = family.view();
    assert!(view.is_member(node(7)) && !view.holds(node(7)));
    let member = secret(7);
    let spec = family.spec_for(family.job_id, member.public());
    let candidate = family.sign(&member, JobFamilyRecord::Spec(Box::new(spec)));
    let empty = Stored::new();

    let (admission, plan) = plan_append(&state(Some(&view), &empty), &[], candidate, None);
    assert_eq!(admission, Admission::Pending(PendingNeed::HolderView));
    assert!(plan.admitted.is_empty());
    assert_eq!(plan.pending.len(), 1);
    assert!(plan.conflicts.is_empty());
}

#[test]
fn rejects_outsider() {
    // A key outside the realm can never become a holder, so no later view
    // could admit it: the record is refused, not retained.
    let family = Family::new([6u8; 32]);
    let view = family.view();
    let outsider = secret(9);
    let spec = family.spec_for(family.job_id, outsider.public());
    let candidate = family.sign(&outsider, JobFamilyRecord::Spec(Box::new(spec)));
    let empty = Stored::new();

    let (admission, plan) = plan_append(&state(Some(&view), &empty), &[], candidate, None);
    assert!(matches!(admission, Admission::Rejected(_)));
    assert!(plan.admitted.is_empty());
    assert!(plan.pending.is_empty());
}

#[test]
fn replays_stay_pending() {
    // A retained record that arrives again is not acknowledged as stored: the
    // publisher must keep offering it, and its attempt budget is not reset.
    let family = Family::new([9u8; 32]);
    let view = family.view();
    let spec = family.spec();
    let claim = family.sign(&family.holder, JobFamilyRecord::Claim(family.claim(&spec)));
    let empty = Stored::new();
    let (_, first) = plan_append(&state(Some(&view), &empty), &[], claim.clone(), None);
    let mut retained: Vec<(JobRecordKey, PendingRecord)> = first.pending;
    retained[0].1.attempts = 5;

    let (admission, plan) = plan_append(&state(Some(&view), &empty), &retained, claim, None);

    assert_eq!(
        admission,
        Admission::Pending(PendingNeed::Evidence(JobRecordKind::Spec))
    );
    assert_eq!(plan.pending.len(), 1);
    assert_eq!(plan.pending[0].1.attempts, 5);
    assert!(plan.admitted.is_empty());
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
        tombstone: None,
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
        aruna_core::structs::execution::job::PhysicalExecutionState::Running,
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

#[test]
fn stop_waits_terminal() {
    // A stop is admitted only next to the receipt and the terminal update it names, and only
    // when the receipted executor signed it.
    let family = Family::new([41u8; 32]);
    let view = family.view();
    let run = family.run(1, 0, PhysicalExecutionState::Failed);
    let (JobFamilyRecord::Receipt(receipt), JobFamilyRecord::Update(update)) =
        (&run[4].record, &run[7].record)
    else {
        panic!("the run fixture keeps its order");
    };
    let stop = JobFamilyRecord::Stop(Box::new(ExecutionStop {
        execution_id: receipt.execution_id,
        submission_id: family.submission_id,
        request_digest: family.request_digest,
        executor_node_id: family.target.public(),
        receipt_digest: receipt.digest().expect("receipt digest"),
        update_sequence: update.sequence,
        update_digest: update.digest().expect("update digest"),
        output_digest: None,
        evidence: StopEvidence::Tombstoned,
        stopped_at_ms: 1_100,
    }));
    let partial = stored(&run[..7]);
    let signed = family.sign(&family.target, stop.clone());

    let (admission, plan) = plan_append(&state(Some(&view), &partial), &[], signed, None);
    assert_eq!(
        admission,
        Admission::Pending(PendingNeed::Evidence(JobRecordKind::Update))
    );
    let (admission, plan) = plan_append(
        &state(Some(&view), &partial),
        &plan.pending,
        run[7].clone(),
        None,
    );
    assert_eq!(admission, Admission::Authentic);
    assert_eq!(
        plan.admitted.len(),
        2,
        "the terminal update admits the stop"
    );

    let forged = family.sign(&family.holder, stop);
    let (admission, _) = plan_append(&state(Some(&view), &stored(&run)), &[], forged, None);
    assert_eq!(
        admission,
        Admission::Rejected(JobRecordError::WrongPublisher(JobRecordKind::Stop))
    );
}

fn tombstone(last_record_ms: u64, finished: bool) -> FamilyTombstone {
    FamilyTombstone {
        pruned_at_ms: last_record_ms + 1,
        last_record_ms,
        finished,
        created_by: None,
        aliases: Vec::new(),
    }
}

// A replayed spec of a pruned family is refused for good, and a peer's other record of it waits
// for its spec instead of becoming a pending row.
#[test]
fn refuses_pruned_family() {
    let family = Family::new([42u8; 32]);
    let view = family.view();
    let spec = family.spec();
    let pruned = tombstone(1_100, true);
    let empty = Stored::new();
    let state = FamilyState {
        tombstone: Some(&pruned),
        ..state(Some(&view), &empty)
    };
    let replay = JobFamilyRecord::Spec(Box::new(spec.clone()));
    let replay = family.sign(&family.holder, replay);
    let claim = family.sign(&family.holder, JobFamilyRecord::Claim(family.claim(&spec)));

    let (admission, plan) = plan_append(&state, &[], replay.clone(), Some(node(2)));
    assert_eq!(
        (admission, plan),
        (Admission::Pruned, AppendPlan::default())
    );
    let (admission, plan) = plan_append(&state, &[], claim.clone(), Some(node(2)));
    assert_eq!(admission, Admission::SpecNeeded);
    assert!(
        plan.pending.is_empty(),
        "a missing spec makes no pending row"
    );
    let (admission, plan) = plan_bundle(
        &state,
        &[],
        claim,
        std::slice::from_ref(&replay),
        Some(node(2)),
    );
    assert_eq!(
        (admission, plan),
        (Admission::Pruned, AppendPlan::default())
    );
}

// A fresh spec offered with the record that binds it admits both in one plan; a spec older than
// the horizon refuses both for good.
#[test]
fn bundles_fresh_spec() {
    let family = Family::new([43u8; 32]);
    let view = family.view();
    let spec = family.spec();
    let empty = Stored::new();
    let signed = JobFamilyRecord::Spec(Box::new(spec.clone()));
    let signed = family.sign(&family.holder, signed);
    let budget = family.budget(&spec, family.holder.public());
    let budget = family.sign(&family.holder, JobFamilyRecord::Budget(budget));

    let fresh = state(Some(&view), &empty);
    let (admission, plan) = plan_bundle(
        &fresh,
        &[],
        budget.clone(),
        std::slice::from_ref(&signed),
        Some(node(2)),
    );
    assert_eq!(admission, Admission::Authentic);
    assert_eq!(plan.admitted.len(), 2);
    let late = FamilyState {
        now_ms: spec.created_at_ms + TOMBSTONE_RETENTION_MS,
        ..fresh
    };
    let (admission, plan) = plan_bundle(
        &late,
        &[],
        budget,
        std::slice::from_ref(&signed),
        Some(node(2)),
    );
    assert_eq!((admission, plan), (Admission::Stale, AppendPlan::default()));
}

// While a family is being deleted every record of it, a fresh spec too, is refused for now.
#[test]
fn unfinished_refuses_now() {
    let family = Family::new([44u8; 32]);
    let view = family.view();
    let deleting = tombstone(500, false);
    let empty = Stored::new();
    let state = FamilyState {
        tombstone: Some(&deleting),
        ..state(Some(&view), &empty)
    };
    let spec = JobFamilyRecord::Spec(Box::new(family.spec()));
    let spec = family.sign(&family.holder, spec);

    let local = plan_append(&state, &[], spec.clone(), None).0;
    assert_eq!(local, Admission::Pruning);
    let relayed = plan_append(&state, &[], spec, Some(node(2))).0;
    assert_eq!(relayed, Admission::Pruning);
}

// After the deletion finished, a spec newer than the pruned records plus the clock margin starts
// a new incarnation of the same family.
#[test]
fn starts_new_incarnation() {
    let family = Family::new([45u8; 32]);
    let view = family.view();
    let pruned = tombstone(1_000, true);
    let empty = Stored::new();
    let state = FamilyState {
        tombstone: Some(&pruned),
        ..state(Some(&view), &empty)
    };
    let mut spec = family.spec();
    spec.created_at_ms = 1_000 + CLOCK_MARGIN_MS + 1;
    let spec = spec.store_digest().expect("spec digest stored");
    let spec = family.sign(&family.holder, JobFamilyRecord::Spec(Box::new(spec)));

    let admission = plan_append(&state, &[], spec, Some(node(2))).0;
    assert_eq!(admission, Admission::Authentic);
}

/// A scheduler that left the realm: its budget and launch reached no holder, its target's
/// receipt did. Returns the held spec and the budget, launch and receipt.
fn left_scheduler(family: &Family) -> (JobRecordEnvelope, [JobRecordEnvelope; 3]) {
    let spec = family.spec();
    let scheduler = secret(9);
    let budget = family.budget(&spec, scheduler.public());
    let launch = family.launch(&spec, scheduler.public(), 0);
    let receipt = family.receipt(&launch, 1);
    (
        family.sign(&family.holder, JobFamilyRecord::Spec(Box::new(spec))),
        [
            family.sign(&scheduler, JobFamilyRecord::Budget(budget)),
            family.sign(&scheduler, JobFamilyRecord::Launch(Box::new(launch))),
            family.sign(&family.target, JobFamilyRecord::Receipt(Box::new(receipt))),
        ],
    )
}

// A holder that kept only the spec admits budget, launch and receipt together from the
// executor's offer; the launch alone from a scheduler that is no holder is still refused.
#[test]
fn recovers_left_scheduler() {
    let family = Family::new([46u8; 32]);
    let view = family.view();
    let (spec, [budget, launch, receipt]) = left_scheduler(&family);
    let held = stored(&[spec]);
    let state = state(Some(&view), &held);

    let (admission, _) = plan_append(&state, &[], launch.clone(), Some(node(2)));
    assert!(matches!(admission, Admission::Rejected(_)));
    let bundle = [launch, budget];
    let (admission, plan) = plan_bundle(&state, &[], receipt, &bundle, Some(node(2)));
    assert_eq!(admission, Admission::Authentic);
    let kinds: Vec<_> = (plan.admitted.iter())
        .map(|admitted| admitted.envelope.key().kind)
        .collect();
    let expected = [
        JobRecordKind::Budget,
        JobRecordKind::Launch,
        JobRecordKind::Receipt,
    ];
    assert_eq!(kinds, expected);
}

// A bundle with any bad budget or a launch the receipt does not bind admits nothing.
#[test]
fn refuses_broken_bundle() {
    let family = Family::new([47u8; 32]);
    let view = family.view();
    let (spec, [budget, launch, receipt]) = left_scheduler(&family);
    let held = stored(std::slice::from_ref(&spec));
    let state = state(Some(&view), &held);
    let JobFamilyRecord::Budget(fields) = &budget.record else {
        panic!("budget fixture")
    };
    let JobFamilyRecord::Receipt(bound) = &receipt.record else {
        panic!("receipt fixture")
    };
    let rebudget = |change: fn(&mut aruna_core::structs::execution::job::WitnessBudgetRecord)| {
        let mut fields = *fields;
        change(&mut fields);
        family.sign(&secret(9), JobFamilyRecord::Budget(fields))
    };
    // The budget allows sequences 0 and 1 only.
    let late = family.launch(&family.spec(), secret(9).public(), 2);
    let late_receipt = family.receipt(&late, 2);
    let late = family.sign(&secret(9), JobFamilyRecord::Launch(Box::new(late)));
    let late_receipt = JobFamilyRecord::Receipt(Box::new(late_receipt));
    let late_receipt = family.sign(&family.target, late_receipt);
    let mut other = (**bound).clone();
    other.launch_digest = [7u8; 32];
    let pair = || vec![launch.clone(), budget.clone()];
    let broken = [
        (
            vec![launch.clone(), rebudget(|budget| budget.max_launches = 0)],
            receipt.clone(),
        ),
        (
            vec![launch.clone(), rebudget(|budget| budget.max_launches = 3)],
            receipt.clone(),
        ),
        (
            vec![
                launch.clone(),
                rebudget(|budget| budget.source_spec_digest = [7u8; 32]),
            ],
            receipt.clone(),
        ),
        (vec![late.clone(), budget.clone()], late_receipt),
        (
            vec![
                launch.clone(),
                family.sign(&secret(8), budget.record.clone()),
            ],
            receipt.clone(),
        ),
        (
            vec![
                family.sign(&secret(8), launch.record.clone()),
                budget.clone(),
            ],
            receipt.clone(),
        ),
        (pair(), family.sign(&family.holder, receipt.record.clone())),
        (
            pair(),
            family.sign(&family.target, JobFamilyRecord::Receipt(Box::new(other))),
        ),
    ];
    for (bundle, receipt) in broken {
        let (admission, plan) = plan_bundle(&state, &[], receipt, &bundle, Some(node(2)));
        assert_ne!(admission, Admission::Authentic);
        assert!(
            plan.admitted.is_empty(),
            "nothing of {admission:?} is admitted"
        );
    }
}
