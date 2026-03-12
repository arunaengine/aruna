use std::time::Duration;

use aruna_core::effects::{DhtEffect, Effect, NetEffect};
use aruna_core::events::{DhtEvent, Event, NetEvent};
use aruna_core::keys::realm_presence_key;
use aruna_core::operation::Operation;
use aruna_core::structs::RealmId;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::DhtKey;
use smallvec::smallvec;
use thiserror::Error;

const REALM_PRESENCE_TTL: Duration = Duration::from_secs(30);
const REALM_PRESENCE_REFRESH_AFTER: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq)]
pub struct AnnounceRealmPresenceConfig {
    pub realm_id: RealmId,
    pub node_id: aruna_core::NodeId,
    pub schedule_refresh: bool,
}

#[derive(Debug, PartialEq)]
pub struct AnnounceRealmPresenceOperation {
    config: AnnounceRealmPresenceConfig,
    state: AnnounceRealmPresenceState,
    output: Option<Result<(), AnnounceRealmPresenceError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum AnnounceRealmPresenceState {
    Init,
    PutPresence,
    ScheduleRefresh,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum AnnounceRealmPresenceError {
    #[error("failed to schedule realm presence refresh: {0}")]
    ScheduleFailed(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl AnnounceRealmPresenceOperation {
    pub fn new(config: AnnounceRealmPresenceConfig) -> Self {
        Self {
            config,
            state: AnnounceRealmPresenceState::Init,
            output: None,
        }
    }

    fn presence_key(&self) -> DhtKey {
        *realm_presence_key(&self.config.realm_id).as_bytes()
    }

    fn task_key(&self) -> TaskKey {
        TaskKey::RealmPresence {
            realm_id: self.config.realm_id.clone(),
            node_id: self.config.node_id,
        }
    }

    fn finish_success(&mut self) -> aruna_core::types::Effects {
        self.state = AnnounceRealmPresenceState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }

    fn fail(&mut self, error: AnnounceRealmPresenceError) -> aruna_core::types::Effects {
        self.state = AnnounceRealmPresenceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(AnnounceRealmPresenceError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for AnnounceRealmPresenceOperation {
    type Output = ();
    type Error = AnnounceRealmPresenceError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = AnnounceRealmPresenceState::PutPresence;
        smallvec![Effect::Net(NetEffect::Dht(DhtEffect::Put {
            key: self.presence_key(),
            value: self.config.node_id.as_bytes().to_vec(),
            ttl: REALM_PRESENCE_TTL,
        }))]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            AnnounceRealmPresenceState::PutPresence => match event {
                Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
                | Event::Net(NetEvent::Dht(DhtEvent::Error { .. })) => {
                    if self.config.schedule_refresh {
                        self.state = AnnounceRealmPresenceState::ScheduleRefresh;
                        smallvec![Effect::Task(TaskEffect::ResetTimer {
                            key: self.task_key(),
                            after: REALM_PRESENCE_REFRESH_AFTER,
                        })]
                    } else {
                        self.finish_success()
                    }
                }
                other => self.unexpected_event("dht put result", format!("{other:?}")),
            },
            AnnounceRealmPresenceState::ScheduleRefresh => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => self.finish_success(),
                Event::Task(TaskEvent::Error { message, .. }) => {
                    self.fail(AnnounceRealmPresenceError::ScheduleFailed(message))
                }
                other => self.unexpected_event("task scheduling result", format!("{other:?}")),
            },
            AnnounceRealmPresenceState::Finish
            | AnnounceRealmPresenceState::Error
            | AnnounceRealmPresenceState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AnnounceRealmPresenceState::Finish | AnnounceRealmPresenceState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        smallvec![]
    }
}
