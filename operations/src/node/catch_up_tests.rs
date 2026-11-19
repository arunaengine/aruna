//! Tests the realm config catch-up by its calls and results for each answer of each step.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;

use super::*;
use crate::tests::records::REALM;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn target(seq: u64) -> irokle::ActorClock {
    let mut clock = irokle::ActorClock::default();
    let peer = irokle::PeerId::from_bytes([1u8; 32]);
    let topic = CatchUpOperation::new(node(1), REALM).topic();
    clock.set(irokle::actor_id_for(topic, peer), seq);
    clock
}

/// A realm config with node 1 as `local` and `managers` as management nodes.
fn config(local: RealmNodeKind, managers: &[u8]) -> IoResult {
    let mut config = RealmConfigDocument::new(REALM, Vec::new(), 5);
    config.ensure_node(node(1), local);
    for seed in managers {
        config.ensure_node(node(*seed), RealmNodeKind::Management);
    }
    IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(config)))
}

fn answer(event: CatchUpEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Net(NetEvent::ConfigCatchUp(event))))
}

fn ask(effect: CatchUpEffect) -> CatchUpNext {
    let effect = Effect::Net(NetEffect::ConfigCatchUp(Box::new(effect)));
    Ok(Next::Call(IoCall::Effect(Box::new(effect))))
}

fn round(peer: u8) -> CatchUpNext {
    let topic = CatchUpOperation::new(node(1), REALM).topic();
    ask(CatchUpEffect::Round {
        topic,
        peer: node(peer),
    })
}

fn config_read() -> CatchUpNext {
    Ok(Next::Call(IoCall::child(GetConfigOperation::new(REALM))))
}

fn contact() -> CatchUpNext {
    let value = Value::from(7_000u64.to_be_bytes().as_slice());
    let row = (
        COMPUTE_DEPARTURE_KEYSPACE.to_string(),
        Key::from(LAST_CONTACT_KEY),
        value,
    );
    Ok(Next::Call(IoCall::Write(vec![row])))
}

fn started() -> CatchUpOperation {
    let mut operation = CatchUpOperation::new(node(1), REALM);
    operation.stamp(7_000);
    assert_eq!(operation.first(), config_read());
    operation
}

fn outcome(caught_up: bool, unanswered: Vec<NodeId>) -> CatchUpNext {
    Ok(Next::Done(CatchUpOutcome {
        caught_up,
        unanswered,
    }))
}

// Every answering management node counts; once the applied cursor covers its target and the
// config names no new manager, the contact row is refreshed.
#[test]
fn counts_answering_managers() {
    let mut operation = started();
    assert_eq!(
        operation.next(config(RealmNodeKind::Server, &[2, 3])),
        round(2)
    );
    assert_eq!(
        operation.next(answer(CatchUpEvent::Counted { target: target(4) })),
        round(3)
    );
    let topic = operation.topic();
    let applied = ask(CatchUpEffect::Applied {
        topic,
        targets: vec![target(4)],
    });
    assert_eq!(
        operation.next(answer(CatchUpEvent::Failed("down".into()))),
        applied
    );
    let covered = answer(CatchUpEvent::Applied { covered: true });
    assert_eq!(operation.next(covered), config_read());
    assert_eq!(
        operation.next(config(RealmNodeKind::Server, &[2, 3])),
        contact()
    );
    let lift = IoCall::child(LiftFenceOperation::new(node(1), REALM));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Call(lift)));
    let lifted = IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(true)));
    assert_eq!(operation.next(lifted), outcome(true, vec![node(3)]));
}

// Without a counted round, or with targets not applied yet, the node has not caught up.
#[test]
fn uncounted_waits() {
    let mut partial = started();
    partial.next(config(RealmNodeKind::Server, &[2])).unwrap();
    let none = partial.next(answer(CatchUpEvent::Partial));
    assert_eq!(none, outcome(false, vec![node(2)]));
    let mut behind = started();
    behind.next(config(RealmNodeKind::Server, &[2])).unwrap();
    behind
        .next(answer(CatchUpEvent::Counted { target: target(1) }))
        .unwrap();
    let uncovered = behind.next(answer(CatchUpEvent::Applied { covered: false }));
    assert_eq!(uncovered, outcome(false, Vec::new()));
}

// A lone management node and a device have no one to wait on and refresh the contact at once.
#[test]
fn alone_refreshes_contact() {
    let mut manager = started();
    assert_eq!(
        manager.next(config(RealmNodeKind::Management, &[])),
        contact()
    );
    let owner = aruna_core::UserId::local(ulid::Ulid::from_bytes([4; 16]), REALM);
    let mut device = started();
    assert_eq!(
        device.next(config(RealmNodeKind::User { owner }, &[2])),
        contact()
    );
}

// A manager the sync revealed is asked too, and a round with a replaced manager does not count.
#[test]
fn replaced_manager_ignored() {
    let mut operation = started();
    operation.next(config(RealmNodeKind::Server, &[2])).unwrap();
    operation
        .next(answer(CatchUpEvent::Counted { target: target(2) }))
        .unwrap();
    operation
        .next(answer(CatchUpEvent::Applied { covered: true }))
        .unwrap();
    assert_eq!(
        operation.next(config(RealmNodeKind::Server, &[4])),
        round(4)
    );
    let unanswered = operation.next(answer(CatchUpEvent::Partial));
    assert_eq!(unanswered, outcome(false, vec![node(4)]));
}

// A caught-up node passes again after the refresh spacing, any other after the retry spacing.
#[test]
fn spaces_next_pass() {
    let caught = Ok(CatchUpOutcome {
        caught_up: true,
        unanswered: Vec::new(),
    });
    assert_eq!(next_pass(&caught), CATCH_UP_REFRESH);
    let waiting = Ok(CatchUpOutcome {
        caught_up: false,
        unanswered: Vec::new(),
    });
    assert_eq!(next_pass(&waiting), CATCH_UP_RETRY);
    assert_eq!(next_pass(&Err(IoError::Unfinished)), CATCH_UP_RETRY);
}
