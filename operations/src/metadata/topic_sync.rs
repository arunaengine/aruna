//! Syncs one graph with its peers in the background, binding or minting the graph's sync topic
//! first, and retries a failed attempt a few times.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect, ShardNetEffect};
use aruna_core::events::{Event, GenesisProbe, NetEvent, ShardNetEvent};
use aruna_core::id::NodeId;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::operation::Operation;
use tracing::{debug, warn};

use crate::metadata::graph_deleted::{GraphDeletedError, GraphDeletedOperation};

/// Sync attempts of one graph before the sync gives up.
const SYNC_ATTEMPTS: usize = 3;
/// The pause between two sync attempts.
const SYNC_AFTER: Duration = Duration::from_millis(250);

type Turn = Result<Next<()>, MetadataError>;

fn graph_call(effect: MetadataEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Metadata(effect)))
}

fn net_call(effect: NetEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Net(effect)))
}

fn shard_call(effect: ShardNetEffect) -> IoCall {
    net_call(NetEffect::Shard(Box::new(effect)))
}

fn net_answer(result: IoResult) -> Result<NetEvent, MetadataError> {
    match result.event()? {
        Event::Net(answer) => Ok(answer),
        other => Err(unexpected(other.label())),
    }
}

fn graph_answer(result: IoResult) -> Result<MetadataEvent, MetadataError> {
    match result.event()? {
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error),
        Event::Metadata(answer) => Ok(answer),
        other => Err(unexpected(other.label())),
    }
}

fn unexpected(label: &str) -> MetadataError {
    MetadataError::Backend(format!("unexpected graph sync answer: {label}"))
}

#[derive(Debug, PartialEq)]
enum SyncStage {
    Local,
    Deleted,
    Topic,
    Exists,
    Join,
    Bind,
    Rejoined,
    Probe,
    Adopt,
    Adopted,
    Mint,
    Members,
    AddPeers,
    Sync,
    Wait,
}

/// Syncs one graph's document sync topic with `peers`: binds the topic this node holds, joins
/// or mints it when missing (only the lowest node id mints), adds new peers and syncs. A graph
/// whose lifecycle row is a tombstone is not synced.
#[derive(Debug, PartialEq)]
pub struct GraphSyncOperation {
    graph_iri: String,
    peers: Vec<NodeId>,
    local: Option<NodeId>,
    topic: Option<irokle::TopicId>,
    attempt: usize,
    stage: SyncStage,
}

impl GraphSyncOperation {
    pub fn new(graph_iri: String, peers: Vec<NodeId>) -> Self {
        Self {
            graph_iri,
            peers,
            local: None,
            topic: None,
            attempt: 1,
            stage: SyncStage::Local,
        }
    }

    fn topic(&self) -> Result<irokle::TopicId, MetadataError> {
        self.topic.ok_or_else(|| unexpected("no topic"))
    }

    fn at(&mut self, stage: SyncStage, call: IoCall) -> Turn {
        self.stage = stage;
        Ok(Next::Call(call))
    }

    fn graph_effect(&mut self, stage: SyncStage, effect: MetadataEffect) -> Turn {
        self.at(stage, graph_call(effect))
    }

    fn sync_topic(&mut self, stage: SyncStage) -> Turn {
        let (topic, peers) = (self.topic()?, self.peers.clone());
        self.at(stage, net_call(NetEffect::SyncTopic { topic, peers }))
    }

    fn exists(&mut self, stage: SyncStage) -> Turn {
        let topic = self.topic()?;
        self.at(stage, shard_call(ShardNetEffect::Existing(vec![topic])))
    }

    fn held(&self, result: IoResult) -> Result<bool, MetadataError> {
        match net_answer(result)? {
            NetEvent::Shard(answer) => match *answer {
                ShardNetEvent::Existing(topics) => Ok(topics.contains(&self.topic()?)),
                other => Err(unexpected(&format!("{other:?}"))),
            },
            other => Err(unexpected(Event::Net(other).label())),
        }
    }

    fn joined(&self, result: IoResult) -> Result<(), MetadataError> {
        if let NetEvent::TopicPeers(Err(error)) = net_answer(result)? {
            debug!(graph_iri = %self.graph_iri, error = ?error, "graph topic join attempt failed");
        }
        Ok(())
    }

    fn start(&mut self) -> Turn {
        let read = GraphDeletedOperation::new(&self.graph_iri);
        self.at(SyncStage::Deleted, IoCall::child(read))
    }

    // Only the lowest node id mints a topic, and only when every co-holder said it holds none.
    fn genesis(&mut self) -> Turn {
        let local = self.local.ok_or_else(|| unexpected("no local node"))?;
        if !(self.peers.iter()).all(|peer| local.as_bytes() < peer.as_bytes()) {
            return self.members();
        }
        let probe = ShardNetEffect::Probe {
            topics: vec![self.topic()?],
            peers: self.peers.clone(),
        };
        self.at(SyncStage::Probe, shard_call(probe))
    }

    fn probed(&mut self, probe: GenesisProbe) -> Turn {
        let topic = self.topic()?;
        if probe.known_co_holder.contains(&topic) {
            return self.sync_topic(SyncStage::Adopt);
        }
        if !probe.unreachable.is_empty() || probe.unconfirmed.contains(&topic) {
            return Err(MetadataError::Backend(format!(
                "withholding graph topic {topic} genesis: co-holder unreachable or unconfirmed"
            )));
        }
        let local = self.local.ok_or_else(|| unexpected("no local node"))?;
        let mut members = self.peers.iter().copied().collect::<BTreeSet<_>>();
        members.insert(local);
        let graph_iri = self.graph_iri.clone();
        self.graph_effect(
            SyncStage::Mint,
            MetadataEffect::MintTopic { graph_iri, members },
        )
    }

    fn members(&mut self) -> Turn {
        let (topic, peers) = (self.topic()?, self.peers.clone());
        let missing = NetEffect::MissingTopicPeers { topic, peers };
        self.at(SyncStage::Members, net_call(missing))
    }

    // A failed attempt waits and starts over; the last one gives up with a warning.
    fn failed(&mut self, error: &MetadataError) -> Turn {
        warn!(
            graph_iri = %self.graph_iri,
            attempt = self.attempt,
            attempts = SYNC_ATTEMPTS,
            error = ?error,
            "Metadata graph sync attempt failed"
        );
        if self.attempt >= SYNC_ATTEMPTS {
            warn!(
                graph_iri = %self.graph_iri,
                peer_count = self.peers.len(),
                "Metadata graph sync retries exhausted"
            );
            return Ok(Next::Done(()));
        }
        self.attempt += 1;
        self.at(SyncStage::Wait, IoCall::Delay(SYNC_AFTER))
    }

    fn step(&mut self, result: IoResult) -> Turn {
        let graph_iri = self.graph_iri.clone();
        match std::mem::replace(&mut self.stage, SyncStage::Wait) {
            SyncStage::Local => {
                let NetEvent::LocalNode(local) = net_answer(result)? else {
                    return Err(unexpected("local node"));
                };
                if let Some(local) = local {
                    self.peers.retain(|peer| *peer != local);
                }
                self.peers
                    .sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
                self.peers.dedup();
                self.local = local;
                if self.peers.is_empty() {
                    return Ok(Next::Done(()));
                }
                self.start()
            }
            SyncStage::Deleted => match result.child::<bool, GraphDeletedError>()?? {
                true => Ok(Next::Done(())),
                false => {
                    self.graph_effect(SyncStage::Topic, MetadataEffect::GraphTopic { graph_iri })
                }
            },
            SyncStage::Topic => match graph_answer(result)? {
                MetadataEvent::GraphTopic { topic } => {
                    self.topic = Some(topic);
                    self.exists(SyncStage::Exists)
                }
                other => Err(unexpected(Event::Metadata(other).label())),
            },
            SyncStage::Exists => match self.held(result)? {
                true => self.members(),
                false => self.sync_topic(SyncStage::Join),
            },
            SyncStage::Rejoined => match self.held(result)? {
                true => self.members(),
                false => self.genesis(),
            },
            // A failed join or adopt still binds what arrived; the next check decides.
            SyncStage::Join => {
                self.joined(result)?;
                self.graph_effect(SyncStage::Bind, MetadataEffect::BindTopic { graph_iri })
            }
            SyncStage::Adopt => {
                self.joined(result)?;
                self.graph_effect(SyncStage::Adopted, MetadataEffect::BindTopic { graph_iri })
            }
            SyncStage::Bind => {
                graph_answer(result)?;
                self.exists(SyncStage::Rejoined)
            }
            SyncStage::Probe => match net_answer(result)? {
                NetEvent::Shard(answer) => match *answer {
                    ShardNetEvent::Probed(probe) => self.probed(probe),
                    other => Err(unexpected(&format!("{other:?}"))),
                },
                other => Err(unexpected(Event::Net(other).label())),
            },
            SyncStage::Adopted | SyncStage::Mint => {
                graph_answer(result)?;
                self.members()
            }
            SyncStage::Members => match net_answer(result)? {
                NetEvent::MissingTopicPeers(Ok(Some(peers))) if !peers.is_empty() => {
                    let add = MetadataEffect::AddTopicPeers { graph_iri, peers };
                    self.graph_effect(SyncStage::AddPeers, add)
                }
                NetEvent::MissingTopicPeers(Ok(_)) => self.sync_topic(SyncStage::Sync),
                NetEvent::MissingTopicPeers(Err(error)) => Err(MetadataError::Backend(error)),
                other => Err(unexpected(Event::Net(other).label())),
            },
            SyncStage::AddPeers => {
                graph_answer(result)?;
                self.sync_topic(SyncStage::Sync)
            }
            SyncStage::Sync => match net_answer(result)? {
                NetEvent::TopicPeers(Ok(())) => Ok(Next::Done(())),
                NetEvent::TopicPeers(Err(error)) => {
                    Err(MetadataError::Backend(format!("{error:?}")))
                }
                other => Err(unexpected(Event::Net(other).label())),
            },
            SyncStage::Wait => self.start(),
        }
    }
}

impl Operation for GraphSyncOperation {
    type Output = ();
    type Error = MetadataError;

    fn first(&mut self) -> Turn {
        if self.peers.is_empty() {
            return Ok(Next::Done(()));
        }
        Ok(Next::Call(net_call(NetEffect::LocalNode)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.step(result) {
            Err(error) => self.failed(&error),
            turn => turn,
        }
    }
}

#[cfg(test)]
#[path = "topic_sync_tests.rs"]
mod call_tests;
