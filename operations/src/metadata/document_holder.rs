//! Chooses which holder serves a public document's RO-Crate when its w3id resolves: two
//! random holders with a fresh heartbeat, the less loaded one wins. The chosen holder serves
//! its own copy and never chooses again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::structs::storage::node_info::NodeInfoDocument;
use rand::seq::SliceRandom;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::get_document::load_document_record;
use crate::node::node_info::read_info_document;

/// A heartbeat older than this does not make a holder a candidate.
pub const HEARTBEAT_STALE_MS: u64 = 180_000;
/// At most this many holders are looked at.
const MAX_HOLDERS: usize = 16;

/// One holder that may serve: its node, API URL and load.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HolderCandidate {
    pub node_id: NodeId,
    pub api_url: String,
    /// Load in permille; unknown counts as fully loaded.
    pub load_permille: u32,
}

/// The candidate of a holder's info document: an API URL and a heartbeat younger than the
/// staleness bound.
pub fn candidate(document: &NodeInfoDocument, now_ms: u64) -> Option<HolderCandidate> {
    let fresh = now_ms.saturating_sub(document.utilization.heartbeat_at_ms) < HEARTBEAT_STALE_MS;
    let api_url = document.urls.api.clone()?;
    (fresh && !document.leaving).then(|| HolderCandidate {
        node_id: document.node_id,
        api_url,
        load_permille: document.utilization.load_permille.unwrap_or(1000),
    })
}

/// Power of two choices over shuffled candidates: of the first two, the less loaded wins.
pub fn pick(mut candidates: Vec<HolderCandidate>) -> Option<HolderCandidate> {
    candidates.shuffle(&mut rand::rng());
    let mut first_two = candidates.into_iter().take(2);
    let first = first_two.next()?;
    Some(match first_two.next() {
        Some(second) if second.load_permille < first.load_permille => second,
        _ => first,
    })
}

/// The holder that should serve `document_id`, or `None` to serve it here: this node holds
/// no registry record of it, no holder is a fresh candidate, or this node was chosen.
pub async fn choose_holder(
    context: &DriverContext,
    local_node_id: NodeId,
    document_id: Ulid,
    now_ms: u64,
) -> Option<HolderCandidate> {
    let record = load_document_record(context, document_id).await.ok()??;
    let mut candidates = Vec::new();
    for node_id in record.holder_node_ids.iter().take(MAX_HOLDERS) {
        if let Ok(Some(document)) = read_info_document(&context.storage_handle, *node_id).await
            && let Some(candidate) = candidate(&document, now_ms)
        {
            candidates.push(candidate);
        }
    }
    pick(candidates).filter(|chosen| chosen.node_id != local_node_id)
}

#[cfg(test)]
mod tests {
    use super::{HolderCandidate, pick};

    fn holder(seed: u8, load: u32) -> HolderCandidate {
        HolderCandidate {
            node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
            api_url: format!("https://node-{seed}.example.org/api/v1"),
            load_permille: load,
        }
    }

    #[test]
    fn less_loaded_of_two_wins() {
        // With exactly two candidates both are compared, whatever the shuffle order.
        for _ in 0..16 {
            let chosen = pick(vec![holder(1, 900), holder(2, 100)]).unwrap();
            assert_eq!(chosen, holder(2, 100));
        }
    }

    #[test]
    fn no_candidate_serves_here() {
        assert_eq!(pick(Vec::new()), None);
        assert_eq!(pick(vec![holder(3, 500)]), Some(holder(3, 500)));
    }
}
