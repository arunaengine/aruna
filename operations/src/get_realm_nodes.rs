use std::collections::HashSet;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{DhtEffect, Effect, NetEffect};
use aruna_core::errors::DhtError;
use aruna_core::events::{DhtEvent, Event, NetEvent};
use aruna_core::keys::realm_presence_key;
use aruna_core::operation::Operation;
use aruna_core::structs::RealmId;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

// Ceiling for driving this operation in an interactive request path. The DHT
// lookup has no wall-clock deadline of its own and can walk offline peers for
// minutes; callers race the drive against this and fall back to local-only
// presence, keeping realm endpoints responsive while nodes are down.
pub const REALM_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(4);

#[derive(Debug, PartialEq)]
pub struct GetRealmNodesOperation {
    realm_id: RealmId,
    state: GetRealmNodesState,
    output: Option<Result<HashSet<NodeId>, GetRealmNodesError>>,
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
    type Output = HashSet<NodeId>;
    type Error = GetRealmNodesError;

    fn start(&mut self) -> Effects {
        self.state = GetRealmNodesState::ReadDocument;
        smallvec![Effect::Net(NetEffect::Dht(DhtEffect::Get {
            key: realm_presence_key(&self.realm_id),
            realm_filter: Some(self.realm_id),
        }))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetRealmNodesState::ReadDocument => match event {
                Event::Net(NetEvent::Dht(DhtEvent::GetResult { values, .. })) => {
                    self.state = GetRealmNodesState::Finish;
                    self.output = Some(Ok(values.into_iter().map(|entry| entry.node_id).collect()));
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
        self.output
            .expect("realm nodes get operation must set output")
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
