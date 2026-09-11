use std::collections::HashSet;
use std::ops::Deref;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{DhtEffect, DhtGetOptions, Effect, NetEffect};
use aruna_core::errors::DhtError;
use aruna_core::events::{DhtEvent, Event, NetEvent};
use aruna_core::keys::realm_presence_key;
use aruna_core::operation::Operation;
use aruna_core::structs::RealmId;
use aruna_core::types::Effects;
use smallvec::smallvec;
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
pub struct GetRealmNodesOperation {
    realm_id: RealmId,
    state: GetRealmNodesState,
    output: Option<Result<RealmPresence, GetRealmNodesError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum GetRealmNodesState {
    Init,
    ReadDocument,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetRealmNodesError {
    #[error(transparent)]
    DhtError(#[from] DhtError),
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl GetRealmNodesOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            state: GetRealmNodesState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: GetRealmNodesError) -> Effects {
        self.state = GetRealmNodesState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(GetRealmNodesError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for GetRealmNodesOperation {
    type Output = RealmPresence;
    type Error = GetRealmNodesError;

    fn start(&mut self) -> Effects {
        self.state = GetRealmNodesState::ReadDocument;
        smallvec![Effect::Net(NetEffect::Dht(DhtEffect::Get {
            key: realm_presence_key(&self.realm_id),
            realm_filter: Some(self.realm_id),
            // Multi-publisher presence must stay exhaustive; the driver enforces
            // the same budget the caller races this operation against.
            options: DhtGetOptions::presence(REALM_DISCOVERY_TIMEOUT, self.realm_id),
        }))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetRealmNodesState::ReadDocument => match event {
                Event::Net(NetEvent::Dht(DhtEvent::GetResult { values, stale, .. })) => {
                    self.state = GetRealmNodesState::Finish;
                    self.output = Some(Ok(RealmPresence::new(
                        values.into_iter().map(|entry| entry.node_id).collect(),
                        stale,
                    )));
                    smallvec![]
                }
                Event::Net(NetEvent::Dht(DhtEvent::Error { error })) => self.fail(error.into()),
                other => self.unexpected_event("dht get result", format!("{other:?}")),
            },
            GetRealmNodesState::Finish | GetRealmNodesState::Error | GetRealmNodesState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetRealmNodesState::Finish | GetRealmNodesState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(GetRealmNodesError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use aruna_core::effects::DhtCompletion;
    use aruna_core::events::DhtEntry;
    use aruna_core::id::DhtKeyId;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn presence(realm_id: RealmId, stale: bool) -> RealmPresence {
        let mut operation = GetRealmNodesOperation::new(realm_id);
        let _ = operation.start();
        let _ = operation.step(Event::Net(NetEvent::Dht(DhtEvent::GetResult {
            key: DhtKeyId::from_data(b"presence"),
            values: vec![DhtEntry {
                node_id: node(1),
                realm_id,
                value: Vec::new(),
                expires_at: 0,
            }],
            stale,
        })));
        operation.finalize().expect("presence output")
    }

    #[test]
    fn requests_snapshot() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let mut operation = GetRealmNodesOperation::new(realm_id);

        assert!(matches!(
            operation.start().as_slice(),
            [Effect::Net(NetEffect::Dht(DhtEffect::Get { options, .. }))]
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
}
