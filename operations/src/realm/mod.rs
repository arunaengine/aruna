//! Groups the realm operations: config, authorization, roles, placement and presence.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod add_role;
pub mod announce_presence;
pub mod assign_role;
pub mod claim_admin;
#[cfg(test)]
mod commit_retry;
pub mod create_realm;
pub mod ensure_config;
pub mod get_nodes;
pub mod load_config;
pub mod mutate_placement;
pub mod node_name;
pub mod peer_trust;
pub mod promote_node;
pub mod promotion;
pub mod read_authorization;
pub mod set_admin;
pub mod set_compute;
pub mod set_email;
pub mod set_federation;
pub mod set_policies;
pub mod set_portal;
pub mod set_provider;
pub mod set_quota;

use aruna_core::calls::{IoCall, IoResult};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::Effect;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::RealmId;

use crate::sync::replicate_documents::replicate_documents_effect;

/// Commits one realm config write may try. Background placement and sync writes change the same
/// document; each attempt rereads, validates and reapplies its change to the newer document.
const COMMIT_ATTEMPTS: usize = 3;

/// The call that announces the realm authorization document of `realm_id`, for a role change
/// whose admin events did not reach the outbox.
pub(crate) fn announce_call(actor: &Actor, realm_id: RealmId) -> Option<IoCall> {
    let document = DocumentTarget::RealmAuthorization { realm_id };
    match replicate_documents_effect(actor.realm_id, actor.node_id, vec![document]) {
        Effect::SubOperation(child) => Some(IoCall::Child(child)),
        _ => None,
    }
}

/// The outcome of [`announce_call`]: the failure reason, or the label of another answer.
pub(crate) fn announced(result: IoResult) -> Result<Result<(), String>, &'static str> {
    let IoResult::Event(event) = result else {
        return Err(result.label());
    };
    match *event {
        Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) => Ok(result),
        other => Err(other.label()),
    }
}
