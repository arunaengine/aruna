//! Chooses which holder serves a read or a download: of two random fresh candidates, the one
//! with more free download slots wins and location links break ties (power of two choices).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Holders that failed lately or report slot refusals are skipped.

use std::collections::BTreeMap;

use crate::NodeId;
use crate::document::DocumentTarget;
use crate::effects::{Effect, NetEffect, StorageEffect};
use crate::events::{Event, NetEvent, StorageEvent};
use crate::heartbeat::{HeartbeatEffect, HeartbeatEvent, HeldHeartbeat, HolderFacts};
use crate::keyspaces::NODE_INFO_KEYSPACE;
use crate::operation::Operation;
use crate::structs::identity::realm::RealmConfigDocument;
use crate::structs::placement::compute_config::RealmComputeConfig;
use crate::structs::storage::node_info::{
    LOCATION_LABEL_KEY, NodeInfoDocument, NodeUrls, node_info_key,
};
use crate::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

/// A heartbeat older than this does not make a holder a candidate.
pub(crate) const HEARTBEAT_STALE_MS: u64 = 180_000;
/// At most this many holders are looked at for one choice.
pub(crate) const MAX_CANDIDATES: usize = 16;

/// One holder that may serve.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HolderCandidate {
    pub node_id: NodeId,
    pub urls: NodeUrls,
    /// Free download slots from the fresh heartbeat; unknown counts as none.
    pub free_slots: u32,
    /// Bandwidth from the holder's location to this node's; it only breaks ties.
    pub bandwidth: u64,
}

/// The candidate of a holder: a heartbeat younger than the staleness bound that reports no
/// slot refusals, from a node that is not leaving and not on the avoid list.
pub fn candidate(
    document: &NodeInfoDocument,
    held: Option<&HeldHeartbeat>,
    avoided: bool,
    bandwidth: u64,
) -> Option<HolderCandidate> {
    let held = held.filter(|held| held.age_ms < HEARTBEAT_STALE_MS)?;
    let slots = held.heartbeat.utilization.download_slots;
    if avoided || document.leaving || slots.is_some_and(|slots| slots.refused > 0) {
        return None;
    }
    Some(HolderCandidate {
        node_id: document.node_id,
        urls: document.urls.clone(),
        free_slots: slots.map_or(0, |slots| slots.free()),
        bandwidth,
    })
}

/// Power of two choices over two candidates drawn with `seed`, which the caller supplies at
/// random: more free slots wins, then more bandwidth.
pub fn pick(mut candidates: Vec<HolderCandidate>, seed: u128) -> Option<HolderCandidate> {
    if candidates.is_empty() {
        return None;
    }
    let len = candidates.len() as u128;
    let first = candidates.swap_remove((seed % len) as usize);
    if candidates.is_empty() {
        return Some(first);
    }
    let rest = candidates.len() as u128;
    let second = candidates.swap_remove(((seed / len) % rest) as usize);
    Some(
        match (second.free_slots, second.bandwidth) > (first.free_slots, first.bandwidth) {
            true => second,
            false => first,
        },
    )
}

/// The bandwidth from `from` to `to`: unlimited within one location, the configured link
/// otherwise, and the realm's pessimistic value for a link it never configured.
fn link_bandwidth(
    config: Option<&RealmComputeConfig>,
    from: Option<&str>,
    to: Option<&str>,
) -> u64 {
    let (Some(config), Some(from), Some(to)) = (config, from, to) else {
        return 0;
    };
    if from == to {
        return u64::MAX;
    }
    config
        .links
        .iter()
        .find(|link| link.from == from && link.to == to)
        .map_or(config.pessimistic_per_sec, |link| link.bandwidth_per_sec)
}

#[derive(Debug, Error, PartialEq)]
pub enum HolderChoiceError {
    #[error("unexpected holder choice event {0}")]
    UnexpectedEvent(&'static str),
    #[error("the holder choice has not finished")]
    Unfinished,
}

#[derive(Debug, PartialEq)]
enum Phase {
    Idle,
    Facts,
    Documents(Box<HolderFacts>),
    Done(Result<Vec<HolderCandidate>, HolderChoiceError>),
}

/// The candidates among `holders`, at most [`MAX_CANDIDATES`] of them looked at: this node's
/// heartbeat state first, then their info documents, this node's and the realm config in one
/// batch read. Without a network or with unreadable documents there is no candidate.
#[derive(Debug, PartialEq)]
pub struct HolderChoiceOperation {
    holders: Vec<NodeId>,
    now_ms: u64,
    phase: Phase,
}

impl HolderChoiceOperation {
    pub fn new(holders: &[NodeId], now_ms: u64) -> Self {
        Self {
            holders: holders.iter().take(MAX_CANDIDATES).copied().collect(),
            now_ms,
            phase: Phase::Idle,
        }
    }

    fn finish(&mut self, candidates: Vec<HolderCandidate>) -> Effects {
        self.phase = Phase::Done(Ok(candidates));
        smallvec![]
    }

    fn read(&mut self, facts: Box<HolderFacts>) -> Effects {
        let config = DocumentTarget::RealmConfig {
            realm_id: facts.realm_id,
        };
        let mut reads: Vec<_> = (self.holders.iter())
            .chain(std::iter::once(&facts.local))
            .map(|node_id| {
                let key = Key::from(node_info_key(*node_id));
                (NODE_INFO_KEYSPACE.to_string(), key)
            })
            .collect();
        reads.push((config.storage_keyspace().to_string(), config.storage_key()));
        self.phase = Phase::Documents(facts);
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn choose(
        &self,
        facts: &HolderFacts,
        values: Vec<(Key, Option<Value>)>,
    ) -> Vec<HolderCandidate> {
        let mut values = values;
        let Some((_, config)) = values.pop() else {
            return Vec::new();
        };
        let config = config.and_then(|bytes| match RealmConfigDocument::from_bytes(&bytes) {
            Ok(config) => Some(config),
            Err(error) => {
                warn!(%error, "realm config unreadable for the holder choice");
                None
            }
        });
        let nodes = self.holders.iter().chain(std::iter::once(&facts.local));
        let mut documents = BTreeMap::new();
        for (node_id, (_, value)) in nodes.zip(values) {
            let Some(bytes) = value else {
                continue;
            };
            // An unreadable advertisement leaves no candidate, as a failed read did.
            match postcard::from_bytes::<NodeInfoDocument>(bytes.as_ref()) {
                Ok(document) => documents.insert(*node_id, document),
                Err(_) => return Vec::new(),
            };
        }
        let compute = config.as_ref().map(|config| &config.compute);
        let location = |node_id: &NodeId| {
            documents
                .get(node_id)
                .and_then(|document| document.labels.get(LOCATION_LABEL_KEY))
                .map(String::as_str)
        };
        let held: BTreeMap<NodeId, &HeldHeartbeat> =
            facts.held.iter().map(|held| (held.node_id, held)).collect();
        self.holders
            .iter()
            .filter_map(|node_id| {
                let document = documents.get(node_id)?;
                let avoided = facts.avoided.contains(node_id);
                let bandwidth = link_bandwidth(compute, location(node_id), location(&facts.local));
                candidate(document, held.get(node_id).copied(), avoided, bandwidth)
            })
            .collect()
    }
}

impl Operation for HolderChoiceOperation {
    type Output = Vec<HolderCandidate>;
    type Error = HolderChoiceError;

    fn start(&mut self) -> Effects {
        self.phase = Phase::Facts;
        smallvec![Effect::Net(NetEffect::Heartbeat(Box::new(
            HeartbeatEffect::Holders {
                holders: self.holders.clone(),
                now_ms: self.now_ms,
            }
        )))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::replace(&mut self.phase, Phase::Idle), event) {
            (Phase::Facts, Event::Net(NetEvent::Heartbeat(HeartbeatEvent::Holders(facts)))) => {
                self.read(facts)
            }
            // Without a network no holder is a candidate.
            (Phase::Facts, Event::Net(NetEvent::Error(_))) => self.finish(Vec::new()),
            (Phase::Documents(facts), Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let candidates = self.choose(&facts, values);
                self.finish(candidates)
            }
            (Phase::Documents(_), Event::Storage(StorageEvent::Error { .. })) => {
                self.finish(Vec::new())
            }
            (_, event) => {
                let error = HolderChoiceError::UnexpectedEvent(event.label());
                self.phase = Phase::Done(Err(error));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.phase, Phase::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.phase {
            Phase::Done(result) => result,
            _ => Err(HolderChoiceError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "holder_choice_tests.rs"]
mod tests;
