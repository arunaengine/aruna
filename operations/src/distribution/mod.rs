//! Publishes distributed bucket records: the set's publisher validates requests against its
//! head and commits each signed revision with its outbox row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::SnapshotSignEvent;
use aruna_core::structs::storage::distribution::SignedSnapshot;
use aruna_core::structs::storage::distribution::snapshot::SnapshotClaim;

use crate::driver::DriverContext;

pub mod handoff;
pub mod inventory;
pub mod publish;
pub mod rebuild;
#[cfg(test)]
pub(crate) mod test_store;

/// Signs a snapshot with this node's key, only when this node is the set's bound publisher.
pub(crate) fn sign_snapshot(context: &DriverContext, claim: SnapshotClaim) -> SnapshotSignEvent {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return SnapshotSignEvent::Unavailable("net transport unavailable".to_string());
    };
    if claim.snapshot.root.publisher != net_handle.node_id() {
        return SnapshotSignEvent::Unavailable("snapshot names another publisher".to_string());
    }
    let Ok(bytes) = claim.snapshot.signing_bytes(claim.realm_id) else {
        return SnapshotSignEvent::Unavailable("snapshot does not encode".to_string());
    };
    SnapshotSignEvent::Signed(Box::new(SignedSnapshot {
        signature: net_handle.sign(&bytes),
        snapshot: claim.snapshot,
    }))
}
