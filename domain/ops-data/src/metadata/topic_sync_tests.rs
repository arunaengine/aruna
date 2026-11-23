//! Drives the graph sync through its topic checks, genesis, member top-up and retries.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::calls::{child, node, runs_child};
use aruna_core::events::TopicPeerError;

const GRAPH: &str = "urn:graph:synced";

fn topic() -> irokle::TopicId {
    irokle::TopicId::from_bytes([9; 32])
}

fn net(answer: NetEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Net(answer)))
}

fn graph(answer: MetadataEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Metadata(answer)))
}

fn existing(held: bool) -> IoResult {
    let topics = held.then(topic).into_iter().collect();
    net(NetEvent::Shard(Box::new(ShardNetEvent::Existing(topics))))
}

// The lower and the higher of two test nodes, so a test picks who may mint.
fn ordered() -> (NodeId, NodeId) {
    let (left, right) = (node(1), node(2));
    match left.as_bytes() < right.as_bytes() {
        true => (left, right),
        false => (right, left),
    }
}

// Walks a sync of `peers` from `local` to the answer of its first topic check.
fn checked(local: NodeId, peers: Vec<NodeId>) -> GraphSyncOperation {
    let mut sync = GraphSyncOperation::new(GRAPH.to_string(), peers);
    assert_eq!(sync.first(), Ok(Next::Call(net_call(NetEffect::LocalNode))));
    assert!(runs_child(sync.next(net(NetEvent::LocalNode(Some(local))))));
    let topic_call = graph_call(MetadataEffect::GraphTopic {
        graph_iri: GRAPH.to_string(),
    });
    let live = sync.next(child(Ok::<_, GraphDeletedError>(false)));
    assert_eq!(live, Ok(Next::Call(topic_call)));
    let exists = shard_call(ShardNetEffect::Existing(vec![topic()]));
    let topic_answer = sync.next(graph(MetadataEvent::GraphTopic { topic: topic() }));
    assert_eq!(topic_answer, Ok(Next::Call(exists)));
    sync
}

#[test]
fn local_peer_skipped() {
    let local = node(1);
    let mut sync = GraphSyncOperation::new(GRAPH.to_string(), vec![local, local]);
    sync.first().unwrap();
    let done = sync.next(net(NetEvent::LocalNode(Some(local))));
    assert_eq!(done, Ok(Next::Done(())));
}

#[test]
fn deleted_graph_skipped() {
    let mut sync = GraphSyncOperation::new(GRAPH.to_string(), vec![node(2)]);
    sync.first().unwrap();
    sync.next(net(NetEvent::LocalNode(Some(node(1))))).unwrap();
    let done = sync.next(child(Ok::<_, GraphDeletedError>(true)));
    assert_eq!(done, Ok(Next::Done(())));
}

#[test]
fn held_topic_syncs() {
    let mut sync = checked(node(1), vec![node(2)]);
    let missing = net_call(NetEffect::MissingTopicPeers {
        topic: topic(),
        peers: vec![node(2)],
    });
    assert_eq!(sync.next(existing(true)), Ok(Next::Call(missing)));
    let add = graph_call(MetadataEffect::AddTopicPeers {
        graph_iri: GRAPH.to_string(),
        peers: vec![node(2)],
    });
    let lacking = sync.next(net(NetEvent::MissingTopicPeers(Ok(Some(vec![node(2)])))));
    assert_eq!(lacking, Ok(Next::Call(add)));
    let sync_call = net_call(NetEffect::SyncTopic {
        topic: topic(),
        peers: vec![node(2)],
    });
    assert_eq!(
        sync.next(graph(MetadataEvent::TopicChanged)),
        Ok(Next::Call(sync_call))
    );
    assert_eq!(
        sync.next(net(NetEvent::TopicPeers(Ok(())))),
        Ok(Next::Done(()))
    );
}

#[test]
fn lowest_node_mints() {
    let (low, high) = ordered();
    let mut sync = checked(low, vec![high]);
    let join = net_call(NetEffect::SyncTopic {
        topic: topic(),
        peers: vec![high],
    });
    assert_eq!(sync.next(existing(false)), Ok(Next::Call(join)));
    let refused = TopicPeerError::Failed("no peer".to_string());
    let bind = graph_call(MetadataEffect::BindTopic {
        graph_iri: GRAPH.to_string(),
    });
    assert_eq!(
        sync.next(net(NetEvent::TopicPeers(Err(refused)))),
        Ok(Next::Call(bind))
    );
    sync.next(graph(MetadataEvent::TopicChanged)).unwrap();
    let probe = shard_call(ShardNetEffect::Probe {
        topics: vec![topic()],
        peers: vec![high],
    });
    assert_eq!(sync.next(existing(false)), Ok(Next::Call(probe)));
    let mint = graph_call(MetadataEffect::MintTopic {
        graph_iri: GRAPH.to_string(),
        members: BTreeSet::from([low, high]),
    });
    let probed = ShardNetEvent::Probed(GenesisProbe::default());
    assert_eq!(
        sync.next(net(NetEvent::Shard(Box::new(probed)))),
        Ok(Next::Call(mint))
    );
}

#[test]
fn higher_node_waits() {
    let (low, high) = ordered();
    let mut sync = checked(high, vec![low]);
    sync.next(existing(false)).unwrap();
    sync.next(net(NetEvent::TopicPeers(Ok(())))).unwrap();
    sync.next(graph(MetadataEvent::TopicChanged)).unwrap();
    let missing = net_call(NetEffect::MissingTopicPeers {
        topic: topic(),
        peers: vec![low],
    });
    assert_eq!(sync.next(existing(false)), Ok(Next::Call(missing)));
}

#[test]
fn failed_attempt_retries() {
    let mut sync = checked(node(1), vec![node(2)]);
    sync.next(existing(true)).unwrap();
    sync.next(net(NetEvent::MissingTopicPeers(Ok(None))))
        .unwrap();
    let refused = || net(NetEvent::TopicPeers(Err(TopicPeerError::NoNetwork)));
    assert_eq!(
        sync.next(refused()),
        Ok(Next::Call(IoCall::Delay(SYNC_AFTER)))
    );
    assert!(runs_child(sync.next(IoResult::Woke)));
    // The last attempt gives up instead of failing the run that started it.
    sync.attempt = SYNC_ATTEMPTS;
    sync.stage = SyncStage::Sync;
    assert_eq!(sync.next(refused()), Ok(Next::Done(())));
}
