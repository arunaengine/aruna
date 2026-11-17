//! Looks up the nodes that announce presence for a realm in the DHT.
//! A stale snapshot is a fan-out hint only: it proves no membership and no write authority.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;
use std::ops::Deref;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{DhtEffect, DhtGetOptions, Effect, NetEffect};
use aruna_core::errors::DhtError;
use aruna_core::events::{DhtEvent, Event, NetEvent};
use aruna_core::keys::realm_presence_key;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use thiserror::Error;

// Budget for a cold presence lookup in an interactive request path, enforced by
// the DHT driver and by the caller racing the drive against it.
pub const REALM_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(4);

/// Realm presence candidates plus whether they came from a bounded-stale
/// snapshot. Stale candidates are fan-out hints only: they never prove a peer is
/// connected and never grant membership, placement, or write authority.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RealmPresence {
    nodes: HashSet<NodeId>,
    stale: bool,
}

impl RealmPresence {
    pub fn new(nodes: HashSet<NodeId>, stale: bool) -> Self {
        Self { nodes, stale }
    }

    pub fn is_stale(&self) -> bool {
        self.stale
    }

    pub fn into_nodes(self) -> HashSet<NodeId> {
        self.nodes
    }
}

impl Deref for RealmPresence {
    type Target = HashSet<NodeId>;

    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

impl PartialEq<HashSet<NodeId>> for RealmPresence {
    fn eq(&self, other: &HashSet<NodeId>) -> bool {
        self.nodes == *other
    }
}

#[derive(Debug, PartialEq)]
pub struct GetNodesOperation {
    realm_id: RealmId,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetNodesError {
    #[error(transparent)]
    DhtError(#[from] DhtError),
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for GetNodesError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ReadDocument".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

impl GetNodesOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self { realm_id }
    }
}

impl Operation for GetNodesOperation {
    type Output = RealmPresence;
    type Error = GetNodesError;

    fn first(&mut self) -> Result<Next<RealmPresence>, GetNodesError> {
        Ok(Next::Call(IoCall::Effect(Effect::Net(NetEffect::Dht(
            DhtEffect::Get {
                key: realm_presence_key(&self.realm_id),
                realm_filter: Some(self.realm_id),
                // Multi-publisher presence must stay exhaustive; the driver enforces
                // the same budget the caller races this operation against.
                options: DhtGetOptions::presence(REALM_DISCOVERY_TIMEOUT, self.realm_id),
            },
        )))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RealmPresence>, GetNodesError> {
        match result {
            IoResult::Event(Event::Net(NetEvent::Dht(DhtEvent::GetResult {
                values,
                stale,
                ..
            }))) => {
                let nodes = values.into_iter().map(|entry| entry.node_id).collect();
                Ok(Next::Done(RealmPresence::new(nodes, stale)))
            }
            IoResult::Event(Event::Net(NetEvent::Dht(DhtEvent::Error { error }))) => {
                Err(error.into())
            }
            IoResult::Event(other) => Err(GetNodesError::UnexpectedEvent {
                state: "ReadDocument".to_string(),
                expected: "dht get result",
                got: other.label(),
            }),
            _ => Err(GetNodesError::NotFinished),
        }
    }

    /// A lookup has no transaction, so a failure is never a conflict to retry.
    fn is_conflict(_error: &GetNodesError) -> bool {
        false
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    use aruna_core::effects::DhtCompletion;
    use aruna_core::events::DhtEntry;
    use aruna_core::id::DhtKeyId;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn presence(realm_id: RealmId, stale: bool) -> RealmPresence {
        let mut operation = GetNodesOperation::new(realm_id);
        operation.first().unwrap();
        let answer = Event::Net(NetEvent::Dht(DhtEvent::GetResult {
            key: DhtKeyId::from_data(b"presence"),
            values: vec![DhtEntry {
                node_id: node(1),
                realm_id,
                value: Vec::new(),
                expires_at: 0,
            }],
            stale,
        }));
        match operation.next(IoResult::Event(answer)) {
            Ok(Next::Done(presence)) => presence,
            other => panic!("expected the presence, got {other:?}"),
        }
    }

    #[test]
    fn requests_snapshot() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let mut operation = GetNodesOperation::new(realm_id);

        let Ok(Next::Call(IoCall::Effect(effect))) = operation.first() else {
            panic!("expected a DHT lookup");
        };
        assert!(matches!(
            effect,
            Effect::Net(NetEffect::Dht(DhtEffect::Get { options, .. }))
                if options.presence == Some(realm_id)
                    && options.deadline == REALM_DISCOVERY_TIMEOUT
                    && options.completion == DhtCompletion::Exhaustive
        ));
    }

    #[test]
    fn stale_flag_propagates() {
        // The caller decides what a stale candidate may be used for, so the
        // freshness of the answer has to survive the operation.
        let realm_id = RealmId::from_bytes([5u8; 32]);

        assert!(!presence(realm_id, false).is_stale());
        let stale = presence(realm_id, true);
        assert!(stale.is_stale());
        assert!(stale.contains(&node(1)));
    }

    #[test]
    fn lookup_failures_end() {
        // A DHT error or an answer of another kind fails the lookup.
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let mut operation = GetNodesOperation::new(realm_id);
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(GetNodesError::NotFinished)
        ));
        let mut operation = GetNodesOperation::new(realm_id);
        operation.first().unwrap();
        let other = IoResult::Event(Event::Net(NetEvent::InboxWoken));
        assert!(matches!(
            operation.next(other),
            Err(GetNodesError::UnexpectedEvent { .. })
        ));
    }
}
