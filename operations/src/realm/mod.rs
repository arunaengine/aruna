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

use aruna_core::events::{Event, StorageEvent};
use aruna_core::types::Value;

/// Commits one realm config write may try. Background placement and sync writes change the same
/// document; each attempt rereads, validates and reapplies its change to the newer document.
const COMMIT_ATTEMPTS: usize = 3;

/// The auth document and admin state values a realm role change reads in one batch; any other
/// event is refused with what was expected and the event's label.
pub(crate) fn role_reads(event: Event) -> Result<[Option<Value>; 2], (&'static str, &'static str)> {
    let got = event.label();
    let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
        return Err(("Event::Storage(StorageEvent::BatchReadResult)", got));
    };
    let Ok([(_, auth_doc), (_, reducer_state)]) = <[_; 2]>::try_from(values) else {
        let expected =
            "Event::Storage(StorageEvent::BatchReadResult) with auth doc and admin state values";
        return Err((expected, got));
    };
    Ok([auth_doc, reducer_state])
}
