//! Drives node discovery and the node fanout through config, presence and node answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::metadata::MetadataEffect;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

/// Asks every node for one number; the test answers with the number or the error it wants.
#[derive(Debug, PartialEq)]
struct Numbers;

impl NodeCalls for Numbers {
    type Part = u8;

    fn local(&self, _node: NodeId) -> Effect {
        Effect::Metadata(MetadataEffect::FlushGraph)
    }

    fn remote(&self, _node: NodeId, _timeout: Duration) -> Effect {
        Effect::Metadata(MetadataEffect::CacheGeneration)
    }

    fn read(&self, _node: NodeId, _local: bool, event: Event) -> Result<u8, MetadataReadError> {
        match event {
            Event::Child(outcome) => outcome.take().ok_or(MetadataReadError::Unavailable)?,
            _ => Err(MetadataReadError::Unavailable),
        }
    }
}

fn answer(result: Result<u8, MetadataReadError>) -> GroupResult {
    GroupResult::Done(Box::new(Event::Child(ChildOutcome::new(result))))
}

fn finished(results: Vec<GroupResult>) -> IoResult {
    IoResult::Group(GroupEvent::Finished { results })
}

fn fanout(
    mode: ApiQueryMode,
    targets: Option<Vec<NodeId>>,
    partial: bool,
) -> NodeFanoutOperation<Numbers> {
    let scope = MetadataFanoutScope::new(Some(mode), targets, partial);
    let label = MetadataFanoutOperation::Query;
    let mut fanout =
        NodeFanoutOperation::new((realm(), node(1)), scope, label, Numbers, map_read_error);
    fanout.stamp(1_000);
    fanout
}

fn group(next: Result<Next<FanoutParts<u8>>, MetadataApiError>) -> EffectGroup {
    match next {
        Ok(Next::Call(IoCall::Group(group))) => group,
        _ => panic!("expected a group"),
    }
}

#[test]
fn discovery_without_config() {
    let mut discovery = DiscoveryOperation::new(realm(), node(1));
    assert!(matches!(
        discovery.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let missing = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
    let Ok(Next::Done(found)) = discovery.next(IoResult::Child(ChildOutcome::new(missing))) else {
        panic!("expected a local-only discovery");
    };
    assert_eq!(
        found,
        RealmNodeDiscovery {
            nodes: vec![node(1)],
            failed: true
        }
    );
}

#[test]
fn late_discovery_local() {
    let mut fanout = fanout(ApiQueryMode::Distributed, None, true);
    group(fanout.first());
    let asked = group(fanout.next(finished(vec![GroupResult::Unfinished])));
    assert_eq!(asked.members.len(), 1);
    let Ok(Next::Done((parts, stats))) = fanout.next(finished(vec![answer(Ok(3))])) else {
        panic!("expected this node's answer");
    };
    assert_eq!(parts, vec![(node(1), 3)]);
    assert!(stats.discovery_failed);
    assert_eq!(stats.nodes_failed, 1);
}

#[test]
fn strict_discovery_fails() {
    let mut fanout = fanout(ApiQueryMode::Distributed, None, false);
    group(fanout.first());
    let failed = fanout.next(finished(vec![GroupResult::Unfinished]));
    assert!(matches!(failed, Err(MetadataApiError::ServiceUnavailable)));
}

#[test]
fn local_mode_asks_self() {
    let mut fanout = fanout(ApiQueryMode::Local, None, false);
    let asked = group(fanout.first());
    assert!(matches!(
        asked.members.as_slice(),
        [Effect::Metadata(MetadataEffect::FlushGraph)]
    ));
    assert_eq!(
        asked.form,
        GroupForm::All {
            deadline: Some(DISTRIBUTED_QUERY_DEADLINE)
        }
    );
    let Ok(Next::Done((parts, stats))) = fanout.next(finished(vec![answer(Ok(7))])) else {
        panic!("expected this node's answer");
    };
    assert_eq!(parts, vec![(node(1), 7)]);
    assert_eq!(stats.nodes_queried, 1);
}

#[test]
fn local_error_maps() {
    let mut fanout = fanout(ApiQueryMode::Local, None, true);
    group(fanout.first());
    let failed = fanout.next(finished(vec![answer(Err(MetadataReadError::Forbidden))]));
    assert!(matches!(failed, Err(MetadataApiError::Forbidden)));
}

#[test]
fn targets_ask_peers() {
    let mut fanout = fanout(
        ApiQueryMode::Distributed,
        Some(vec![node(2), node(1), node(2)]),
        true,
    );
    let asked = group(fanout.first());
    assert_eq!(asked.members.len(), 2);
    let Ok(Next::Done((parts, stats))) =
        fanout.next(finished(vec![answer(Ok(1)), GroupResult::Unfinished]))
    else {
        panic!("expected a partial answer");
    };
    assert_eq!(parts.len(), 1);
    assert_eq!((stats.nodes_queried, stats.nodes_failed), (2, 1));
    assert_eq!(stats.failed_partitions.len(), 1);
}

#[test]
fn strict_fanout_fails() {
    let mut fanout = fanout(
        ApiQueryMode::Distributed,
        Some(vec![node(1), node(2)]),
        false,
    );
    group(fanout.first());
    let failed = fanout.next(finished(vec![answer(Ok(1)), GroupResult::Unfinished]));
    assert!(matches!(failed, Err(MetadataApiError::ServiceUnavailable)));
}

#[test]
fn peer_denial_fails() {
    let mut fanout = fanout(
        ApiQueryMode::Distributed,
        Some(vec![node(1), node(2)]),
        true,
    );
    group(fanout.first());
    let denied = vec![answer(Ok(1)), answer(Err(MetadataReadError::Forbidden))];
    assert!(matches!(
        fanout.next(finished(denied)),
        Err(MetadataApiError::Forbidden)
    ));
}

#[test]
fn rejected_token_skipped() {
    let mut fanout = fanout(
        ApiQueryMode::Distributed,
        Some(vec![node(1), node(2)]),
        true,
    );
    group(fanout.first());
    let rejected = vec![answer(Ok(1)), answer(Err(MetadataReadError::Unauthorized))];
    let Ok(Next::Done((parts, stats))) = fanout.next(finished(rejected)) else {
        panic!("expected a partial answer");
    };
    assert_eq!(parts.len(), 1);
    assert_eq!(stats.nodes_failed, 1);
}

#[test]
fn skipped_nodes_unasked() {
    let scope = MetadataFanoutScope::new(
        Some(ApiQueryMode::Distributed),
        Some(vec![node(1), node(2)]),
        true,
    )
    .with_skip(vec![node(2)]);
    let label = MetadataFanoutOperation::Query;
    let mut fanout =
        NodeFanoutOperation::new((realm(), node(1)), scope, label, Numbers, map_read_error);
    fanout.stamp(1_000);
    assert_eq!(group(fanout.first()).members.len(), 1);
}

#[test]
fn deadline_bounds_peers() {
    let mut fanout = fanout(ApiQueryMode::Distributed, None, true).until(4_000);
    fanout.stamp(1_000);
    let discovery = group(fanout.first());
    assert_eq!(
        discovery.form,
        GroupForm::All {
            deadline: Some(Duration::from_secs(3))
        }
    );
}

/// More realm nodes than the fanout cap, this node first.
fn many_nodes() -> Vec<NodeId> {
    (0..=QUERY_MAX_NODES as u8)
        .map(|index| node(60 + index))
        .collect()
}

fn capped(skip: Vec<NodeId>, partial: bool) -> NodeFanoutOperation<Numbers> {
    let nodes = many_nodes();
    let scope = MetadataFanoutScope::new(
        Some(ApiQueryMode::Distributed),
        Some(nodes.clone()),
        partial,
    )
    .with_skip(skip);
    let label = MetadataFanoutOperation::BucketSearch;
    let mut fanout =
        NodeFanoutOperation::new((realm(), nodes[0]), scope, label, Numbers, map_read_error);
    fanout.stamp(1_000);
    fanout
}

/// The nodes a capped fanout asked, answered each with a number.
fn asked(fanout: &mut NodeFanoutOperation<Numbers>) -> Vec<NodeId> {
    let members = group(fanout.first()).members.len();
    let FanoutStage::Nodes(nodes) = &fanout.stage else {
        panic!("expected the node group");
    };
    assert_eq!(nodes.len(), members);
    nodes.clone()
}

#[test]
fn capped_fanout_incomplete() {
    // A cut node set must not reach a caller that refused partial results as complete.
    let mut fanout = capped(Vec::new(), false);
    let nodes = asked(&mut fanout);
    let answers = nodes.iter().map(|_| answer(Ok(1))).collect();
    assert!(matches!(
        fanout.next(finished(answers)),
        Err(MetadataApiError::ServiceUnavailable)
    ));
}

#[test]
fn names_capped_nodes() {
    // The nodes the cap leaves out are named, and skipping the asked ones reaches them next.
    let mut fanout = capped(Vec::new(), true);
    let first = asked(&mut fanout);
    assert_eq!(first.len(), QUERY_MAX_NODES);
    let answers = first.iter().map(|_| answer(Ok(1))).collect();
    let Ok(Next::Done((_, stats))) = fanout.next(finished(answers)) else {
        panic!("expected a partial answer");
    };
    let [left_out] = stats.omitted[..] else {
        panic!("one node is left out: {:?}", stats.omitted);
    };
    assert!(!first.contains(&left_out));
    let mut next = capped(first, true);
    let second = asked(&mut next);
    assert!(second.contains(&left_out));
}
