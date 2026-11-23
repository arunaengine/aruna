//! Tests resolving shard topics and ensuring a genesis as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::calls::{child, node, runs_child, shard_effect, shard_event};

pub(crate) fn topic(seed: u8) -> TopicId {
    TopicId::from_bytes([seed; 32])
}

pub(crate) fn existing(topics: &[TopicId]) -> IoResult {
    shard_event(ShardNetEvent::Existing(topics.iter().copied().collect()))
}

pub(crate) fn applied() -> IoResult {
    shard_event(ShardNetEvent::Outcome(Ok(())))
}

pub(crate) fn refused() -> IoResult {
    shard_event(ShardNetEvent::Outcome(Err("refused".to_string())))
}

pub(crate) fn pulled() -> IoResult {
    IoResult::Child(aruna_core::calls::ChildOutcome::new(Ok::<
        bool,
        aruna_core::calls::IoError,
    >(true)))
}

fn probed(probe: GenesisProbe) -> IoResult {
    shard_event(ShardNetEvent::Probed(probe))
}

fn resolving(co_members: Vec<NodeId>, may_mint: bool) -> ResolveTopicsOperation {
    let topics = vec![topic(1)];
    let mut operation = ResolveTopicsOperation::new(co_members, topics, may_mint, BTreeSet::new());
    assert_eq!(
        shard_effect(operation.first()),
        ShardNetEffect::Existing(vec![topic(1)])
    );
    operation
}

fn resolved(to_ensure: Vec<TopicId>, withheld: bool) -> ResolvedTopics {
    ResolvedTopics {
        to_ensure,
        withheld,
        unreachable: BTreeSet::new(),
    }
}

/// Runs up to the answer of the probe of `node(2)`.
fn probing() -> ResolveTopicsOperation {
    let mut operation = resolving(vec![node(2)], true);
    assert_eq!(
        shard_effect(operation.next(existing(&[]))),
        ShardNetEffect::Probe {
            topics: vec![topic(1)],
            peers: vec![node(2)],
        }
    );
    operation
}

#[test]
fn known_topics_kept() {
    let mut operation = resolving(vec![node(2)], true);
    assert_eq!(
        operation.next(existing(&[topic(1)])),
        Ok(Next::Done(resolved(vec![topic(1)], false)))
    );
}

#[test]
fn sole_holder_mints() {
    let mut operation = resolving(Vec::new(), true);
    assert_eq!(
        operation.next(existing(&[])),
        Ok(Next::Done(resolved(vec![topic(1)], false)))
    );
}

#[test]
fn sole_joiner_withholds() {
    let mut operation = resolving(Vec::new(), false);
    assert_eq!(
        operation.next(existing(&[])),
        Ok(Next::Done(resolved(Vec::new(), true)))
    );
}

#[test]
fn missing_network_withholds() {
    let mut operation = resolving(vec![node(2)], true);
    let turn = operation.next(shard_event(ShardNetEvent::NoNetwork));
    assert_eq!(turn, Ok(Next::Done(resolved(Vec::new(), true))));
}

#[test]
fn absent_topic_minted() {
    let mut operation = probing();
    assert_eq!(
        operation.next(probed(GenesisProbe::default())),
        Ok(Next::Done(resolved(vec![topic(1)], false)))
    );
}

#[test]
fn unconfirmed_topic_withheld() {
    let mut operation = probing();
    let probe = GenesisProbe {
        unconfirmed: BTreeSet::from([topic(1)]),
        ..GenesisProbe::default()
    };
    assert_eq!(
        operation.next(probed(probe)),
        Ok(Next::Done(resolved(Vec::new(), true)))
    );
}

#[test]
fn unreachable_peer_withholds() {
    let mut operation = probing();
    let probe = GenesisProbe {
        unreachable: vec![node(2)],
        ..GenesisProbe::default()
    };
    let Ok(Next::Done(answer)) = operation.next(probed(probe)) else {
        panic!("expected the resolved topics");
    };
    assert!(answer.withheld);
    assert!(answer.to_ensure.is_empty());
    assert_eq!(answer.unreachable, BTreeSet::from([node(2)]));
}

#[test]
fn dead_peers_unprobed() {
    // A peer already found unreachable in this pass is not asked again and still withholds.
    let dead = BTreeSet::from([node(2)]);
    let mut operation = ResolveTopicsOperation::new(vec![node(2)], vec![topic(1)], true, dead);
    operation.first().unwrap();
    let Ok(Next::Done(answer)) = operation.next(existing(&[])) else {
        panic!("expected no probe");
    };
    assert!(answer.withheld);
    assert!(answer.to_ensure.is_empty());
}

fn adopting() -> ResolveTopicsOperation {
    let mut operation = probing();
    let probe = GenesisProbe {
        known_co_holder: BTreeSet::from([topic(1)]),
        ..GenesisProbe::default()
    };
    assert_eq!(
        operation.next(probed(probe)),
        Ok(Next::Call(IoCall::child(
            crate::shard::pull::PullTopicsOperation::new(vec![topic(1)], vec![node(2)])
        )))
    );
    assert_eq!(
        shard_effect(operation.next(pulled())),
        ShardNetEffect::Existing(vec![topic(1)])
    );
    operation
}

#[test]
fn probe_adopts_known() {
    assert_eq!(
        adopting().next(existing(&[topic(1)])),
        Ok(Next::Done(resolved(vec![topic(1)], false)))
    );
}

#[test]
fn failed_adopt_withheld() {
    // A topic a peer holds is never minted here, even when the pull brought nothing.
    assert_eq!(
        adopting().next(existing(&[])),
        Ok(Next::Done(resolved(Vec::new(), true)))
    );
}

fn genesis(co_members: Vec<NodeId>) -> EnsureGenesisOperation {
    let mut operation = EnsureGenesisOperation::new(
        node(1),
        co_members,
        vec![node(1)],
        vec![topic(1)],
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
    );
    assert!(matches!(
        shard_effect(operation.first()),
        ShardNetEffect::Membership(_)
    ));
    assert!(runs_child(operation.next(applied())));
    operation
}

fn resolved_answer(to_ensure: Vec<TopicId>) -> IoResult {
    child(Ok::<_, IoError>(resolved(to_ensure, false)))
}

#[test]
fn withheld_genesis_reported() {
    let mut operation = genesis(vec![node(2)]);
    let answer = ResolvedTopics {
        to_ensure: Vec::new(),
        withheld: true,
        unreachable: BTreeSet::from([node(3)]),
    };
    assert_eq!(
        operation.next(child(Ok::<_, IoError>(answer))),
        Ok(Next::Done((true, BTreeSet::from([node(3)]))))
    );
}

#[test]
fn lone_genesis_local() {
    let mut operation = genesis(Vec::new());
    assert_eq!(
        shard_effect(operation.next(resolved_answer(vec![topic(1)]))),
        ShardNetEffect::EnsureLocal(vec![topic(1)])
    );
    assert!(matches!(
        shard_effect(operation.next(applied())),
        ShardNetEffect::Membership(_)
    ));
    assert_eq!(
        operation.next(applied()),
        Ok(Next::Done((false, BTreeSet::new())))
    );
}

#[test]
fn shared_genesis_peers() {
    let mut operation = genesis(vec![node(2)]);
    assert_eq!(
        shard_effect(operation.next(resolved_answer(vec![topic(1)]))),
        ShardNetEffect::EnsureShared {
            topics: vec![topic(1)],
            peers: vec![node(2)],
        }
    );
}

#[test]
fn failed_ensure_withheld() {
    let mut operation = genesis(vec![node(2)]);
    operation.next(resolved_answer(vec![topic(1)])).unwrap();
    assert_eq!(
        operation.next(refused()),
        Ok(Next::Done((true, BTreeSet::new())))
    );
}

#[test]
fn failed_members_withheld() {
    let mut operation = genesis(vec![node(2)]);
    operation.next(resolved_answer(vec![topic(1)])).unwrap();
    operation.next(applied()).unwrap();
    assert_eq!(
        operation.next(refused()),
        Ok(Next::Done((true, BTreeSet::new())))
    );
}
