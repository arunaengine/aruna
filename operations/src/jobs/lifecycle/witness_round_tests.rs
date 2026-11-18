//! Call and result tests of one witness round.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;

use super::*;
use crate::tests::records::{Family, REALM, node};

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn is_child(next: &RoundNext) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

fn outcome(next: RoundNext) -> RoundOutcome {
    match next {
        Ok(Next::Done(outcome)) => outcome,
        other => panic!("unexpected step: {other:?}"),
    }
}

fn witness(local: NodeId) -> WitnessNode {
    WitnessNode {
        local,
        realm_id: REALM,
        fresh: BTreeMap::new(),
        limits: BTreeMap::new(),
    }
}

fn round(fixture: &Family, local: NodeId) -> WitnessRoundOperation {
    let family = fixture.family();
    WitnessRoundOperation::new(family, Some(witness(local)), 5_000)
}

/// A round of the holder that has read its config and family.
fn configured(fixture: &Family) -> WitnessRoundOperation {
    let mut operation = round(fixture, fixture.holder.public());
    assert!(is_child(&operation.first()));
    let config = child(Ok::<_, GetConfigError>(fixture.config.clone()));
    assert!(is_child(&operation.next(config)));
    assert_eq!(operation.step, RoundStep::Family);
    operation
}

fn offering(fixture: &Family, retry_after_ms: u64) -> WitnessRoundOperation {
    let mut operation = configured(fixture);
    let facts = operation.facts().unwrap();
    facts.offer = Some(OfferState {
        target: fixture.target_id(),
        retry_after_ms,
    });
    operation
}

/// One family with a launch by `holder` that no receipt answers.
fn foreign(fixture: &Family) -> (LaunchIntent, Vec<JobRecordEnvelope>) {
    let spec = fixture.spec();
    let launch = fixture.launch(&spec, fixture.holder.public(), 0);
    let records = vec![fixture.sign(
        &fixture.holder,
        JobFamilyRecord::Launch(Box::new(launch.clone())),
    )];
    (launch, records)
}

// A node without a network has nothing to schedule.
#[test]
fn offline_round_ends() {
    let fixture = Family::new([21u8; 32]);
    let mut operation = WitnessRoundOperation::new(fixture.family(), None, 1);
    assert_eq!(outcome(operation.first()), RoundOutcome::Done);
}

// A missing config is retried soon, and a node that holds nothing stops.
#[test]
fn config_decides_start() {
    let fixture = Family::new([22u8; 32]);
    let mut operation = round(&fixture, fixture.holder.public());
    let missing = child(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    assert_eq!(
        outcome(operation.next(missing)),
        RoundOutcome::Retry { after_ms: 1_000 }
    );
    let mut stranger = round(&fixture, node(9));
    let config = child(Ok::<_, GetConfigError>(fixture.config.clone()));
    assert_eq!(outcome(stranger.next(config)), RoundOutcome::Done);
}

// An incomplete family read decides nothing, and an empty family has no spec to run.
#[test]
fn family_read_required() {
    let fixture = Family::new([23u8; 32]);
    let base = fixture.config.compute.witness_delay_ms;
    let mut operation = configured(&fixture);
    let partial = child(Err::<Vec<JobRecordEnvelope>, _>(
        FamilyReadError::NotFinished,
    ));
    assert_eq!(
        outcome(operation.next(partial)),
        RoundOutcome::Retry { after_ms: base }
    );
    let mut operation = configured(&fixture);
    let empty = child(Ok::<_, FamilyReadError>(Vec::<JobRecordEnvelope>::new()));
    assert_eq!(
        outcome(operation.next(empty)),
        RoundOutcome::Retry { after_ms: base }
    );
}

// A first round stores this witness's launch budget before anything else.
#[test]
fn budget_signed_first() {
    let fixture = Family::new([24u8; 32]);
    let mut operation = configured(&fixture);
    let spec = fixture.sign(
        &fixture.holder,
        JobFamilyRecord::Spec(Box::new(fixture.spec())),
    );
    let records = child(Ok::<_, FamilyReadError>(vec![spec]));
    let next = operation.next(records).unwrap();
    let Next::Call(IoCall::Effect(effect)) = next else {
        panic!("expected the signature request");
    };
    assert!(matches!(
        *effect,
        Effect::Net(NetEffect::ExecutionSign(claim))
            if matches!(claim.record, JobFamilyRecord::Budget(_))
    ));
    assert_eq!(operation.step, RoundStep::BudgetSigned);
}

// A refused signature leaves the budget for a later round.
#[test]
fn unsigned_budget_retries() {
    let fixture = Family::new([25u8; 32]);
    let base = fixture.config.compute.witness_delay_ms;
    let mut operation = configured(&fixture);
    operation.step = RoundStep::BudgetSigned;
    let refused = Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Unavailable(
        "no key".to_string(),
    )));
    assert_eq!(
        outcome(operation.next(IoResult::Event(Box::new(refused)))),
        RoundOutcome::Retry { after_ms: base }
    );
}

// Only nodes silent for longer than the window stop suppressing a launch.
#[test]
fn silence_needs_window() {
    let running = BTreeSet::from([node(1), node(2), node(3)]);
    let silence = BTreeMap::from([(node(1), 11), (node(2), 10)]);
    assert_eq!(
        silent_nodes(&silence, &running, 10),
        BTreeSet::from([node(1)])
    );
}

// A launch planned onto this node is admitted here, any other goes out as an offer.
#[test]
fn offer_picks_route() {
    let fixture = Family::new([26u8; 32]);
    let (launch, records) = foreign(&fixture);
    let frame = JobRecordFrame::new(records[0].clone()).unwrap();

    let mut local = configured(&fixture);
    let target = ExecutionTargetId {
        node_id: fixture.holder.public(),
        ..launch.target.clone()
    };
    assert!(is_child(&local.offer(frame.clone(), target, 7)));
    assert_eq!(local.step, RoundStep::AdmittedHere);

    let mut remote = configured(&fixture);
    let target = ExecutionTargetId {
        node_id: node(9),
        ..launch.target
    };
    assert!(is_child(&remote.offer(frame, target, 7)));
    assert_eq!(remote.step, RoundStep::Offered);
}

// An accepted offer stores its receipt, and losing that write retries the offer's delay.
#[test]
fn receipt_stored_after_accept() {
    let fixture = Family::new([27u8; 32]);
    let (_, records) = foreign(&fixture);
    let receipt = JobRecordFrame::new(records[0].clone()).unwrap();
    let mut operation = offering(&fixture, 7);
    assert!(is_child(&operation.settle(OfferOutcome::Accepted(receipt))));
    assert_eq!(operation.step, RoundStep::ReceiptStored);
    let lost = child(Err::<AppendOutcome, _>(RecordStoreError::NotFinished));
    assert_eq!(
        outcome(operation.next(lost)),
        RoundOutcome::Retry { after_ms: 7 }
    );
}

// A cancelled launch ends the round, and a passing refusal waits for the next one.
#[test]
fn declines_settle_round() {
    let fixture = Family::new([28u8; 32]);
    let cancelled = OfferOutcome::Declined(LaunchDecline::Cancelled);
    let settled = offering(&fixture, 7).settle(cancelled);
    assert_eq!(outcome(settled), RoundOutcome::Done);
    let busy = OfferOutcome::Declined(LaunchDecline::Capacity);
    let settled = offering(&fixture, 7).settle(busy);
    assert_eq!(outcome(settled), RoundOutcome::Retry { after_ms: 7 });
    let settled = offering(&fixture, 7).settle(OfferOutcome::Unavailable);
    assert_eq!(outcome(settled), RoundOutcome::Retry { after_ms: 7 });
}

// A target that refused for good is remembered, so the next round plans around it.
#[test]
fn permanent_decline_recorded() {
    let fixture = Family::new([29u8; 32]);
    let mut operation = offering(&fixture, 7);
    let refused = OfferOutcome::Declined(LaunchDecline::Unauthorized);
    assert!(is_child(&operation.settle(refused)));
    assert_eq!(operation.step, RoundStep::DeclineRead);
    let explain = WitnessExplain {
        sequence: 0,
        plan: empty_plan(),
        declined: Vec::new(),
        overlapping: false,
        stored_at_ms: 1,
    };
    let stored = child(Ok::<_, JobMutationError>(Some(explain)));
    assert!(is_child(&operation.next(stored)));
    assert_eq!(operation.step, RoundStep::DeclineStored);
    let written = child(Ok::<_, JobMutationError>(()));
    assert_eq!(
        outcome(operation.next(written)),
        RoundOutcome::Retry { after_ms: 0 }
    );
}

// Without an explain row there is nothing to add the refusal to.
#[test]
fn decline_needs_explain() {
    let fixture = Family::new([30u8; 32]);
    let mut operation = offering(&fixture, 7);
    operation.decline(fixture.target_id()).unwrap();
    let missing = child(Ok::<Option<WitnessExplain>, JobMutationError>(None));
    assert_eq!(
        outcome(operation.next(missing)),
        RoundOutcome::Retry { after_ms: 0 }
    );
}

#[test]
fn defers_to_launch() {
    // Another witness's unreceipted launch holds the family until its
    // window ends; this node's own launch never defers the round.
    let fixture = Family::new([3u8; 32]);
    let (launch, records) = foreign(&fixture);
    let now = launch.created_at_ms + 10;

    assert_eq!(
        foreign_launch_wait(&records, node(2), now, 1_000),
        Some(990)
    );
    assert_eq!(
        foreign_launch_wait(&records, fixture.holder.public(), now, 1_000),
        None
    );
}

#[test]
fn plans_after_window() {
    // Past the window, and once a receipt exists, the round plans again.
    let fixture = Family::new([4u8; 32]);
    let (launch, mut records) = foreign(&fixture);

    assert_eq!(
        foreign_launch_wait(&records, node(2), launch.created_at_ms + 1_000, 1_000),
        None
    );
    records.push(fixture.sign(
        &fixture.target,
        JobFamilyRecord::Receipt(Box::new(fixture.receipt(&launch, 1))),
    ));
    assert_eq!(
        foreign_launch_wait(&records, node(2), launch.created_at_ms + 10, 1_000),
        None
    );
}

#[test]
fn lost_target_expires() {
    // A target keeps its launch for the whole configured catch-up window.
    let base = 5_000;
    let window = 300_000;
    assert!(!lost_target_expired(10, 9 + window, window));
    assert!(lost_target_expired(10, 10 + window, window));
    assert_eq!(lost_retry_ms(10, 10, base, window), base);
    assert_eq!(lost_retry_ms(10, 9 + window, base, window), 1);
}
