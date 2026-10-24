//! Chooses which holder serves a public document's RO-Crate when its w3id resolves, with the
//! same choice as downloads: two random fresh holders, the one with more free download slots
//! wins. The chosen holder serves its own copy and never chooses again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::get_document::load_document_record;
use crate::node::holder_choice::{HolderCandidate, choose_holder as choose};

/// The holder that should serve `document_id`, or `None` to serve it here: this node holds
/// no registry record of it, no holder is a fresh candidate, or this node was chosen.
pub async fn choose_holder(
    context: &DriverContext,
    local_node_id: NodeId,
    document_id: Ulid,
    now_ms: u64,
) -> Option<HolderCandidate> {
    let record = load_document_record(context, document_id).await.ok()??;
    choose(context, &record.holder_node_ids, now_ms)
        .await
        .filter(|chosen| chosen.node_id != local_node_id && chosen.urls.api.is_some())
}
