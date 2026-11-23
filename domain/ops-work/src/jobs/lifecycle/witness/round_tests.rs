//! Call and result tests of one witness round.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::job::family::LAUNCH_WINDOW_MS;

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
    assert_eq!(operation.next(config), Ok(Next::Call(fence_read())));
    assert!(is_child(&operation.next(IoResult::Values(vec![None]))));
    assert_eq!(operation.step, RoundStep::Family);
    operation
}

// Behind the start fence the round plans nothing and looks again after its base delay.
#[test]
fn fenced_round_waits() {
    let fixture = Family::new([27u8; 32]);
    let base = fixture.config.compute.witness_delay_ms;
    let mut operation = round(&fixture, fixture.holder.public());
    operation.first().unwrap();
    let config = child(Ok::<_, GetConfigError>(fixture.config.clone()));
    operation.next(config).unwrap();
    let fenced = IoResult::Values(vec![Some(Value::from(&[][..]))]);
    assert_eq!(
        outcome(operation.next(fenced)),
        RoundOutcome::Retry { after_ms: base }
    );
}

/// Answers the family read with `records`, then the membership read with `view`.
fn viewed(
    operation: &mut WitnessRoundOperation,
    records: Vec<JobRecordEnvelope>,
    view: WriterView,
) -> RoundNext {
    let read = IoCall::child(MembershipViewOperation::new(REALM));
    let family = operation.next(child(Ok::<_, FamilyReadError>(records)));
    assert_eq!(family, Ok(Next::Call(read)));
    operation.next(child(Ok::<_, IoError>(Some(view))))
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
    let empty = viewed(&mut operation, Vec::new(), WriterView::default());
    assert_eq!(outcome(empty), RoundOutcome::Retry { after_ms: base });
    // An unreadable membership view is no verdict either.
    let mut operation = configured(&fixture);
    operation
        .next(child(Ok::<_, FamilyReadError>(
            Vec::<JobRecordEnvelope>::new(),
        )))
        .unwrap();
    let unknown = operation.next(child(Ok::<Option<WriterView>, IoError>(None)));
    assert_eq!(outcome(unknown), RoundOutcome::Retry { after_ms: base });
}

// A family whose only open launch went to a target that left the realm is not run.
#[test]
fn left_target_ends() {
    use aruna_core::admin_documents::{AdminDocumentDot, NodeInterval};
    let fixture = Family::new([26u8; 32]);
    let origin = fixture.holder.public();
    let dot = |origin_seq| AdminDocumentDot {
        event_id: Ulid::nil(),
        origin_node_id: origin,
        origin_seq,
    };
    let (mut cited, mut clock) = (AdminDocumentClock::default(), AdminDocumentClock::default());
    cited.advance(origin, 1);
    clock.advance(origin, 2);
    let mut launch = fixture.launch(&fixture.spec(), origin, 0);
    launch.config_clock = cited;
    let target = launch.target.node_id;
    let record = fixture.sign(&fixture.holder, JobFamilyRecord::Launch(Box::new(launch)));
    let interval = NodeInterval {
        node_id: target,
        added: dot(1),
        removed: vec![dot(2)],
    };
    let left = WriterView {
        clock,
        intervals: vec![interval],
    };
    let mut operation = configured(&fixture);
    let answer = viewed(&mut operation, vec![record], left);
    assert_eq!(outcome(answer), RoundOutcome::Done);
}

// A first round stores this witness's launch budget before anything else.
#[test]
fn budget_signed_first() {
    let fixture = Family::new([24u8; 32]);
    let mut operation = configured(&fixture);
    let spec = fixture.spec();
    let claim = JobFamilyRecord::Claim(fixture.claim(&spec));
    let family = vec![
        fixture.sign(&fixture.holder, JobFamilyRecord::Spec(Box::new(spec))),
        fixture.sign(&fixture.holder, claim),
    ];
    let next = viewed(&mut operation, family, WriterView::default()).unwrap();
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
    local.seed(3);
    let target = ExecutionTargetId {
        node_id: fixture.holder.public(),
        ..launch.target.clone()
    };
    let here = LaunchNode {
        local: fixture.holder.public(),
        realm_id: REALM,
        now_ms: 5_000,
        execution_id: Ulid::from_parts(5_000, 4),
        limits: None,
        config_digest: None,
    };
    let admit = AdmitLaunchOperation::new(LaunchFrame::new(records[0].clone()).unwrap(), here);
    assert_eq!(
        local.offer(frame.clone(), target, 7),
        Ok(Next::Call(IoCall::child(admit)))
    );
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
fn stores_accepted_receipt() {
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

/// An explain row with `deferred` and nothing else.
fn explain_of(deferred: Vec<DeferredTarget>) -> WitnessExplain {
    WitnessExplain {
        sequence: 0,
        plan: empty_plan(),
        declined: Vec::new(),
        deferred,
        answer: None,
        overlapping: false,
        stored_at_ms: 1,
    }
}

// A cancelled launch ends the round; a passing refusal or no answer is kept as the last answer
// and the round waits for the next one.
#[test]
fn declines_settle_round() {
    let fixture = Family::new([28u8; 32]);
    let cancelled = OfferOutcome::Declined(LaunchDecline::Cancelled);
    let settled = offering(&fixture, 7).settle(cancelled);
    assert_eq!(outcome(settled), RoundOutcome::Done);
    for (answer, kind) in [
        (
            OfferOutcome::Declined(LaunchDecline::Capacity),
            DeferKind::Capacity,
        ),
        (OfferOutcome::Unavailable, DeferKind::Silent),
    ] {
        let mut operation = offering(&fixture, 7);
        assert!(is_child(&operation.settle(answer)));
        assert_eq!(operation.step, RoundStep::AmendRead);
        let mut expected = explain_of(Vec::new());
        expected.answer = Some(kind);
        let write = operation.explain_write(&expected).unwrap();
        let stored = child(Ok::<_, JobMutationError>(Some(explain_of(Vec::new()))));
        assert_eq!(operation.next(stored), Ok(Next::Call(write)));
        let written = child(Ok::<_, JobMutationError>(()));
        let after = RoundOutcome::Retry { after_ms: 7 };
        assert_eq!(outcome(operation.next(written)), after);
    }
}

// A temporary answer of a deferred target renews its deferral.
#[test]
fn answer_renews_deferral() {
    let fixture = Family::new([31u8; 32]);
    let mut operation = offering(&fixture, 7);
    let old = DeferredTarget {
        target: fixture.target_id(),
        kind: DeferKind::Silent,
        at_ms: 1,
    };
    operation
        .settle(OfferOutcome::Declined(LaunchDecline::Draining))
        .unwrap();
    let renewed = DeferredTarget {
        kind: DeferKind::Draining,
        at_ms: 5_000,
        ..old.clone()
    };
    let mut expected = explain_of(vec![renewed]);
    expected.answer = Some(DeferKind::Draining);
    let write = operation.explain_write(&expected).unwrap();
    let stored = child(Ok::<_, JobMutationError>(Some(explain_of(vec![old]))));
    assert_eq!(operation.next(stored), Ok(Next::Call(write)));
}

/// A configured round holding its own launch of the fixture spec, created at `created_at_ms`.
fn launched(fixture: &Family, created_at_ms: u64) -> (WitnessRoundOperation, LaunchIntent) {
    let mut operation = configured(fixture);
    let spec = fixture.spec();
    let mut launch = fixture.launch(&spec, fixture.holder.public(), 0);
    launch.created_at_ms = created_at_ms;
    let envelope = fixture.sign(
        &fixture.holder,
        JobFamilyRecord::Launch(Box::new(launch.clone())),
    );
    let facts = operation.facts().unwrap();
    facts.records = vec![envelope];
    facts.spec = Some(spec);
    facts.window_ms = 1;
    facts.max_launches = 3;
    (operation, launch)
}

// A launch whose receipt wait ended without a receipt sets its target aside with the last
// answer, silent when none came.
#[test]
fn expired_wait_defers() {
    let fixture = Family::new([32u8; 32]);
    let (mut operation, launch) = launched(&fixture, 1_000);
    assert!(is_child(&operation.explained(None)));
    assert_eq!(operation.step, RoundStep::AmendRead);
    let mut stored = explain_of(Vec::new());
    stored.sequence = 1;
    let entry = DeferredTarget {
        target: launch.target,
        kind: DeferKind::Silent,
        at_ms: 5_000,
    };
    let mut expected = stored.clone();
    expected.deferred = vec![entry];
    let write = operation.explain_write(&expected).unwrap();
    let read = child(Ok::<_, JobMutationError>(Some(stored)));
    assert_eq!(operation.next(read), Ok(Next::Call(write)));
}

// Inside the launch window a deferred target is screened and then gets the same launch again.
#[test]
fn deferred_gets_launch() {
    let fixture = Family::new([33u8; 32]);
    let (mut operation, launch) = launched(&fixture, 4_000);
    let entry = DeferredTarget {
        target: launch.target.clone(),
        kind: DeferKind::Silent,
        at_ms: 4_500,
    };
    assert!(is_child(
        &operation.explained(Some(explain_of(vec![entry])))
    ));
    assert_eq!(operation.step, RoundStep::Ready);
    let ready = operation.next(child(Ok::<_, IoError>(true)));
    assert!(is_child(&ready), "{ready:?}");
    assert_eq!(operation.step, RoundStep::Offered);
}

// Past the launch window a new launch waits for the target's closure, unless the target left.
#[test]
fn deferred_gates_launch() {
    let fixture = Family::new([34u8; 32]);
    let entry = |launch: &LaunchIntent| DeferredTarget {
        target: launch.target.clone(),
        kind: DeferKind::Silent,
        at_ms: 1,
    };
    let (mut gated, launch) = launched(&fixture, 1);
    gated.now_ms = 2 + LAUNCH_WINDOW_MS;
    assert!(is_child(
        &gated.explained(Some(explain_of(vec![entry(&launch)])))
    ));
    assert_eq!(gated.step, RoundStep::Gate(launch.launch_id));
    let open = child(Ok::<_, LifecycleError>(BTreeSet::<Ulid>::new()));
    let base = fixture.config.compute.witness_delay_ms;
    assert_eq!(
        outcome(gated.next(open)),
        RoundOutcome::Retry { after_ms: base }
    );
    let (mut left, launch) = launched(&fixture, 1);
    left.now_ms = 2 + LAUNCH_WINDOW_MS;
    left.facts()
        .unwrap()
        .not_run
        .launches
        .insert(launch.launch_id);
    assert!(is_child(
        &left.explained(Some(explain_of(vec![entry(&launch)])))
    ));
    assert_eq!(left.step, RoundStep::Inputs);
}

// A target that refused for good is remembered, so the next round plans around it.
#[test]
fn permanent_decline_recorded() {
    let fixture = Family::new([29u8; 32]);
    let mut operation = offering(&fixture, 7);
    let refused = OfferOutcome::Declined(LaunchDecline::Unauthorized);
    assert!(is_child(&operation.settle(refused)));
    assert_eq!(operation.step, RoundStep::AmendRead);
    let explain = WitnessExplain {
        sequence: 0,
        plan: empty_plan(),
        declined: Vec::new(),
        deferred: Vec::new(),
        answer: None,
        overlapping: false,
        stored_at_ms: 1,
    };
    let stored = child(Ok::<_, JobMutationError>(Some(explain)));
    assert!(is_child(&operation.next(stored)));
    assert_eq!(operation.step, RoundStep::AmendStored(0));
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

// A used-up budget cancels the family with an expiry once its work settled; a running
// execution keeps the round waiting a window.
#[test]
fn used_budget_expires() {
    let fixture = Family::new([26u8; 32]);
    let spec = fixture.spec();
    let claim = JobFamilyRecord::Claim(fixture.claim(&spec));
    let records = vec![
        fixture.sign(
            &fixture.holder,
            JobFamilyRecord::Spec(Box::new(spec.clone())),
        ),
        fixture.sign(&fixture.holder, claim),
    ];
    let serving = Some((fixture.holder.public(), REALM));
    let used = |records: &[JobRecordEnvelope]| {
        let mut operation = configured(&fixture);
        let facts = operation.facts().unwrap();
        facts.records = records.to_vec();
        facts.spec = Some(spec.clone());
        let close = CloseLaunchesOperation::new(fixture.family(), records.to_vec(), serving);
        assert_eq!(
            operation.explained(None),
            Ok(Next::Call(IoCall::child(close)))
        );
        operation
    };
    let mut settled = used(&records);
    let closed = child(Ok::<_, LifecycleError>(BTreeSet::<Ulid>::new()));
    let reason = ExpiryReason::BudgetUsed;
    let cancel = PublishCancelOperation::expiry(spec.clone(), reason, serving, 5_000);
    assert_eq!(settled.next(closed), Ok(Next::Call(IoCall::child(cancel))));
    let published = child(Ok::<_, JobRouteError>(()));
    assert_eq!(outcome(settled.next(published)), RoundOutcome::Done);

    let launch = fixture.launch(&spec, node(2), 0);
    let receipt = JobFamilyRecord::Receipt(Box::new(fixture.receipt(&launch, 1)));
    let mut running = records.clone();
    running.push(fixture.sign(&fixture.target, receipt));
    let mut waiting = used(&running);
    let window = fixture.config.compute.catch_up_ms;
    let closed = child(Ok::<_, LifecycleError>(BTreeSet::<Ulid>::new()));
    let after = RoundOutcome::Retry { after_ms: window };
    assert_eq!(outcome(waiting.next(closed)), after);
}
