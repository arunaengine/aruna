//! Publishes distributed bucket records: the set's publisher validates requests against its
//! head and commits each signed revision with its outbox row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::events::{SnapshotSignEvent, StatementSignEvent};
use aruna_core::structs::storage::distribution::snapshot::SnapshotClaim;
use aruna_core::structs::storage::distribution::{SignedSnapshot, SignedStatement, StatementClaim};

use crate::driver::DriverContext;

pub mod admit;
pub mod balance;
pub mod change;
pub mod create;
pub mod delegate;
mod feed_store;
pub mod forward;
pub mod guard;
pub mod handoff;
pub mod index_feed;
pub mod inventory;
pub mod keep;
pub mod key_index;
pub mod lifecycle;
pub mod publish;
pub mod pull;
pub mod purge;
pub mod rebuild;
pub mod refill;
pub mod resolve;
pub mod roots;
pub mod sharding;
pub mod source_keep;
pub mod split;
pub mod status;
#[cfg(test)]
pub(crate) mod test_record;
#[cfg(test)]
pub(crate) mod test_store;
pub mod ticket;
pub mod usage;
pub mod version_key;
pub mod versions;

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

/// Signs a member statement with this node's key, only when the claim names this node.
pub(crate) fn sign_statement(context: &DriverContext, claim: StatementClaim) -> StatementSignEvent {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return StatementSignEvent::Unavailable("net transport unavailable".to_string());
    };
    if claim.signer != net_handle.node_id() {
        return StatementSignEvent::Unavailable("statement names another signer".to_string());
    }
    let Ok(bytes) = claim.statement.signing_bytes() else {
        return StatementSignEvent::Unavailable("statement does not encode".to_string());
    };
    StatementSignEvent::Signed(Box::new(SignedStatement {
        signature: net_handle.sign(&bytes),
        signer: claim.signer,
        statement: claim.statement,
    }))
}
