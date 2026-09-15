use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{DhtEffect, Effect, NetEffect};
use aruna_core::errors::DhtError;
use aruna_core::events::{DhtEvent, Event, NetEvent};
use aruna_core::id::DhtKeyId;
use aruna_core::keys::realm_presence_key;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::Effects;
use smallvec::smallvec;
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
    state: AnnouncePresenceState,
    output: Option<Result<(), AnnouncePresenceError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum AnnouncePresenceState {
    Init,
    PutPresence,
    ScheduleRefresh,
    Finish,
    Error,
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
        got: String,
    },
}

impl AnnouncePresenceOperation {
    pub fn new(config: AnnouncePresenceConfig) -> Self {
        Self {
            config,
            state: AnnouncePresenceState::Init,
            output: None,
        }
    }

    fn presence_key(&self) -> DhtKeyId {
        realm_presence_key(&self.config.realm_id)
    }

    fn task_key(&self) -> TaskKey {
        TaskKey::RealmPresence {
            realm_id: self.config.realm_id,
            node_id: self.config.node_id,
        }
    }

    fn finish_success(&mut self) -> Effects {
        self.state = AnnouncePresenceState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }

    fn fail(&mut self, error: AnnouncePresenceError) -> Effects {
        self.state = AnnouncePresenceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(AnnouncePresenceError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for AnnouncePresenceOperation {
    type Output = ();
    type Error = AnnouncePresenceError;

    fn start(&mut self) -> Effects {
        self.state = AnnouncePresenceState::PutPresence;
        smallvec![Effect::Net(NetEffect::Dht(DhtEffect::Put {
            key: self.presence_key(),
            realm_id: self.config.realm_id,
            value: self.config.node_id.as_bytes().to_vec(),
            ttl: REALM_PRESENCE_TTL,
        }))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            AnnouncePresenceState::PutPresence => match event {
                Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. })) => {
                    if self.config.schedule_refresh {
                        self.state = AnnouncePresenceState::ScheduleRefresh;
                        smallvec![Effect::Task(TaskEffect::ResetTimer {
                            key: self.task_key(),
                            after: PRESENCE_REFRESH_AFTER,
                        })]
                    } else {
                        self.finish_success()
                    }
                }
                Event::Net(NetEvent::Dht(DhtEvent::Error { error })) => {
                    self.fail(AnnouncePresenceError::PutFailed(error))
                }
                other => self.unexpected_event("dht put result", format!("{other:?}")),
            },
            AnnouncePresenceState::ScheduleRefresh => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => self.finish_success(),
                Event::Task(TaskEvent::Error { message, .. }) => {
                    self.fail(AnnouncePresenceError::ScheduleFailed(message))
                }
                other => self.unexpected_event("task scheduling result", format!("{other:?}")),
            },
            AnnouncePresenceState::Finish
            | AnnouncePresenceState::Error
            | AnnouncePresenceState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AnnouncePresenceState::Finish | AnnouncePresenceState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    #[test]
    fn presence_ttl_sufficient() {
        let realm_id = RealmId([1u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let mut op = AnnouncePresenceOperation::new(AnnouncePresenceConfig {
            realm_id,
            node_id,
            schedule_refresh: true,
        });

        let effects = op.start();

        let [Effect::Net(NetEffect::Dht(DhtEffect::Put { ttl, .. }))] = effects.as_slice() else {
            panic!("expected one DHT put effect");
        };
        assert!(*ttl > PRESENCE_REFRESH_AFTER);
        assert!(*ttl > aruna_net::dht::constants::DRIVER_TICK_INTERVAL);
    }

    #[test]
    fn dht_error_fails() {
        let realm_id = RealmId([1u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let mut op = AnnouncePresenceOperation::new(AnnouncePresenceConfig {
            realm_id,
            node_id,
            schedule_refresh: true,
        });

        let effects = op.start();
        assert_eq!(effects.len(), 1);

        let effects = op.step(Event::Net(NetEvent::Dht(DhtEvent::Error {
            error: DhtError::Other("boom".to_string()),
        })));
        assert!(effects.is_empty());
        assert!(matches!(
            op.finalize(),
            Err(AnnouncePresenceError::PutFailed(DhtError::Other(message))) if message == "boom"
        ));
    }
}
