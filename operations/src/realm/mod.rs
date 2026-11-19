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
mod write_stage;

use aruna_core::calls::{IoCall, IoResult};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::Effect;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::structs::identity::auth::Actor;

use crate::sync::replicate_documents::replicate_documents_effect;

/// Reads a realm document and the reducer state of its admin target.
pub(crate) fn document_reads(
    document: DocumentTarget,
    target: &aruna_core::admin_documents::AdminDocumentTarget,
) -> IoCall {
    IoCall::Read(vec![
        (
            document.storage_keyspace().to_string(),
            document.storage_key(),
        ),
        (
            aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE.to_string(),
            aruna_core::storage_entries::reducer_state_key(target),
        ),
    ])
}

/// Reads the realm authorization of `realm_id` and the reducer state of its admin document.
pub(crate) fn authorization_reads(
    realm_id: aruna_core::structs::identity::realm::RealmId,
) -> IoCall {
    let target = aruna_core::admin_documents::AdminDocumentTarget::Realm { realm_id };
    IoCall::Read(vec![
        (
            aruna_core::keyspaces::AUTH_KEYSPACE.to_string(),
            (*realm_id.as_bytes()).into(),
        ),
        (
            aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE.to_string(),
            aruna_core::storage_entries::reducer_state_key(&target),
        ),
    ])
}

/// Commits one realm config write may try. Background placement and sync writes change the same
/// document; each attempt rereads, validates and reapplies its change to the newer document.
const COMMIT_ATTEMPTS: usize = 3;

/// The call that announces an authorization `document`, for a role change whose admin events did
/// not reach the outbox.
pub(crate) fn announce_call(actor: &Actor, document: DocumentTarget) -> Option<IoCall> {
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
