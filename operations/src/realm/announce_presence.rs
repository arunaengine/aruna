//! Publishes this node's realm presence to the DHT and schedules the refresh before it expires.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{DhtEffect, Effect, NetEffect};
use aruna_core::errors::DhtError;
use aruna_core::events::{DhtEvent, Event, NetEvent};
use aruna_core::id::DhtKeyId;
use aruna_core::keys::realm_presence_key;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use thiserror::Error;

const REALM_PRESENCE_TTL: Duration = Duration::from_secs(60);
pub(crate) const PRESENCE_REFRESH_AFTER: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq)]
pub struct AnnouncePresenceConfig {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub schedule_refresh: bool,
}

#[derive(Debug, PartialEq)]
pub struct AnnouncePresenceOperation {
    config: AnnouncePresenceConfig,
    /// Set once the put was sent again after a failure.
    retried: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum AnnouncePresenceError {
    #[error("failed to announce realm presence: {0}")]
    PutFailed(DhtError),
    #[error("failed to schedule realm presence refresh: {0}")]
    ScheduleFailed(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for AnnouncePresenceError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "AnnouncePresence".to_string(),
                expected,
                got,
            },
            other => Self::PutFailed(DhtError::Other(other.to_string())),
        }
    }
}

impl AnnouncePresenceOperation {
    pub fn new(config: AnnouncePresenceConfig) -> Self {
        Self {
            config,
            retried: false,
        }
    }

    fn presence_key(&self) -> DhtKeyId {
        realm_presence_key(&self.config.realm_id)
    }

    fn put_presence(&self) -> Next<()> {
        let put = Effect::Net(NetEffect::Dht(DhtEffect::Put {
            key: self.presence_key(),
            realm_id: self.config.realm_id,
            value: self.config.node_id.as_bytes().to_vec(),
            ttl: REALM_PRESENCE_TTL,
        }));
        Next::Call(IoCall::Effect(Box::new(put)))
    }

    fn put_done(&mut self) -> Next<()> {
        if !self.config.schedule_refresh {
            return Next::Done(());
        }
        let key = TaskKey::RealmPresence {
            realm_id: self.config.realm_id,
            node_id: self.config.node_id,
        };
        let after = PRESENCE_REFRESH_AFTER;
        let timer = Effect::Task(TaskEffect::ResetTimer { key, after });
        Next::Call(IoCall::Effect(Box::new(timer)))
    }
}

impl Operation for AnnouncePresenceOperation {
    type Output = ();
    type Error = AnnouncePresenceError;

    fn first(&mut self) -> Result<Next<()>, AnnouncePresenceError> {
        Ok(self.put_presence())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, AnnouncePresenceError> {
        let IoResult::Event(event) = result else {
            return Err(AnnouncePresenceError::UnexpectedEvent {
                state: "AnnouncePresence".to_string(),
                expected: "dht put or task scheduling result",
                got: result.label(),
            });
        };
        match *event {
            Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. })) => Ok(self.put_done()),
            // A concurrent announce of this node, such as its refresh timer, can store a newer
            // revision first; this put is then refused as stale. A second put replaces it.
            Event::Net(NetEvent::Dht(DhtEvent::Error { .. })) if !self.retried => {
                self.retried = true;
                Ok(self.put_presence())
            }
            Event::Net(NetEvent::Dht(DhtEvent::Error { error })) => {
                Err(AnnouncePresenceError::PutFailed(error))
            }
            Event::Task(TaskEvent::TimerScheduled { .. }) => Ok(Next::Done(())),
            Event::Task(TaskEvent::Error { message, .. }) => {
                Err(AnnouncePresenceError::ScheduleFailed(message))
            }
            other => Err(AnnouncePresenceError::UnexpectedEvent {
                state: "AnnouncePresence".to_string(),
                expected: "dht put or task scheduling result",
                got: other.label(),
            }),
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    fn operation(schedule_refresh: bool) -> AnnouncePresenceOperation {
        AnnouncePresenceOperation::new(AnnouncePresenceConfig {
            realm_id: RealmId([1u8; 32]),
            node_id: iroh::SecretKey::from_bytes(&[2u8; 32]).public(),
            schedule_refresh,
        })
    }

    fn dht(event: DhtEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Net(NetEvent::Dht(event))))
    }

    fn boom() -> IoResult {
        dht(DhtEvent::Error {
            error: DhtError::Other("boom".to_string()),
        })
    }

    #[test]
    fn presence_ttl_sufficient() {
        let mut op = operation(true);
        let Ok(Next::Call(IoCall::Effect(effect))) = op.first() else {
            panic!("expected one DHT put effect");
        };
        let Effect::Net(NetEffect::Dht(DhtEffect::Put { ttl, .. })) = *effect else {
            panic!("expected one DHT put effect");
        };
        assert!(ttl > PRESENCE_REFRESH_AFTER);
        assert!(ttl > aruna_net::dht::constants::DRIVER_TICK_INTERVAL);
    }

    #[test]
    fn dht_error_fails() {
        let mut op = operation(true);
        let put = op.first();
        assert_eq!(op.next(boom()), put);
        assert!(matches!(
            op.next(boom()),
            Err(AnnouncePresenceError::PutFailed(DhtError::Other(message))) if message == "boom"
        ));
    }

    #[test]
    fn stale_put_retries() {
        // A newer revision stored by a concurrent announce refuses this put once.
        let mut op = operation(false);
        let first = op.first();
        let stale = dht(DhtEvent::Error {
            error: DhtError::StoreFailed(
                "DHT error: invalid request: record version is stale or conflicting".to_string(),
            ),
        });
        assert_eq!(op.next(stale), first);
        let done = dht(DhtEvent::PutComplete {
            key: op.presence_key(),
            remote_attempt_count: 0,
            remote_store_count: 0,
        });
        assert_eq!(op.next(done), Ok(Next::Done(())));
    }
}
