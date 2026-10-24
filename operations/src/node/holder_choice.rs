//! Chooses which holder serves a read or a download: of two random fresh candidates, the one
//! with more free download slots wins and location links break ties (power of two choices).
//! Holders that failed lately or report slot refusals are skipped.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::heartbeat::HeldHeartbeat;
use aruna_core::structs::placement::compute_config::RealmComputeConfig;
use aruna_core::structs::storage::node_info::{LOCATION_LABEL_KEY, NodeInfoDocument, NodeUrls};
use rand::seq::SliceRandom;

use crate::driver::DriverContext;
use crate::metadata::api::load_realm_config;
use crate::node::heartbeat::held_heartbeats;
use crate::node::node_info::read_info_documents;

/// A heartbeat older than this does not make a holder a candidate.
pub const HEARTBEAT_STALE_MS: u64 = 180_000;
/// At most this many holders are looked at for one choice.
pub const MAX_CANDIDATES: usize = 16;

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

/// Power of two choices over shuffled candidates: more free slots wins, then more bandwidth.
pub fn pick(mut candidates: Vec<HolderCandidate>) -> Option<HolderCandidate> {
    candidates.shuffle(&mut rand::rng());
    let mut first_two = candidates.into_iter().take(2);
    let first = first_two.next()?;
    Some(match first_two.next() {
        Some(second)
            if (second.free_slots, second.bandwidth) > (first.free_slots, first.bandwidth) =>
        {
            second
        }
        _ => first,
    })
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

/// The candidates among `holders`, at most [`MAX_CANDIDATES`] of them looked at.
pub async fn candidates(
    context: &DriverContext,
    holders: &[NodeId],
    now_ms: u64,
) -> Vec<HolderCandidate> {
    let Some(net) = context.net_handle.as_ref() else {
        return Vec::new();
    };
    let local = net.node_id();
    let mut wanted: Vec<NodeId> = holders.iter().take(MAX_CANDIDATES).copied().collect();
    wanted.push(local);
    let documents: BTreeMap<NodeId, NodeInfoDocument> = read_info_documents(context, &wanted)
        .await
        .unwrap_or_default();
    let held = held_heartbeats(Some(net));
    let config = load_realm_config(context, *net.realm_id()).await;
    let compute = config.as_ref().map(|config| &config.compute);
    let location = |node_id: &NodeId| {
        documents
            .get(node_id)
            .and_then(|document| document.labels.get(LOCATION_LABEL_KEY))
            .map(String::as_str)
    };
    holders
        .iter()
        .take(MAX_CANDIDATES)
        .filter_map(|node_id| {
            let document = documents.get(node_id)?;
            let avoided = net.avoided_holders().avoided(node_id, now_ms);
            let bandwidth = link_bandwidth(compute, location(node_id), location(&local));
            candidate(document, held.get(node_id), avoided, bandwidth)
        })
        .collect()
}

/// The holder that should serve, chosen among `holders`; `None` without a fresh candidate.
pub async fn choose_holder(
    context: &DriverContext,
    holders: &[NodeId],
    now_ms: u64,
) -> Option<HolderCandidate> {
    pick(candidates(context, holders, now_ms).await)
}

/// Skips `node_id` for a while after this node saw it fail or refuse a slot.
pub fn avoid_holder(context: &DriverContext, node_id: NodeId, now_ms: u64) {
    if let Some(net) = context.net_handle.as_ref() {
        net.avoided_holders().avoid(node_id, now_ms);
    }
}

#[cfg(test)]
#[path = "holder_choice_tests.rs"]
mod tests;
