use std::time::Duration;

use aruna_core::automerge::{AutomergeDocumentVariant, AutomergeState};
use aruna_core::effects::{Effect, GossipEffect, NetEffect};
use aruna_core::errors::{ConversionError, GossipError, StorageError};
use aruna_core::events::{Event, GossipEvent, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::task::{TaskEffect, TaskEvent};
use smallvec::smallvec;
use thiserror::Error;

use crate::automerge::repository::{automerge_clock, read_effect};
use aruna_core::NodeId;
use aruna_core::events::NetEvent;
use aruna_core::types::Effects;

pub const AUTOMERGE_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(30);
pub const AUTOMERGE_ANNOUNCE_SHORT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq)]
pub struct AnnounceAutomergeDocumentOperation {
    document: AutomergeDocumentVariant,
    local_node_id: NodeId,
    state: AnnounceAutomergeDocumentState,
    output: Option<Result<(), AnnounceAutomergeDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum AnnounceAutomergeDocumentState {
    Init,
    ResetTimer,
    Subscribe,
    ReadDocument,
    Broadcast,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum AnnounceAutomergeDocumentError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    GossipError(#[from] GossipError),
    #[error("document not found")]
    DocumentNotFound,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl AnnounceAutomergeDocumentOperation {
    pub fn new(document: AutomergeDocumentVariant, local_node_id: NodeId) -> Self {
        Self {
            document,
            local_node_id,
            state: AnnounceAutomergeDocumentState::Init,
            output: None,
        }
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.state = AnnounceAutomergeDocumentState::Error;
        self.output = Some(Err(AnnounceAutomergeDocumentError::UnexpectedEvent {
            state,
            expected,
            got,
        }));
        smallvec![]
    }

    fn fail(&mut self, error: AnnounceAutomergeDocumentError) -> Effects {
        self.state = AnnounceAutomergeDocumentState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for AnnounceAutomergeDocumentOperation {
    type Output = ();
    type Error = AnnounceAutomergeDocumentError;

    fn start(&mut self) -> Effects {
        self.state = AnnounceAutomergeDocumentState::ResetTimer;
        smallvec![Effect::Task(TaskEffect::ResetTimer {
            key: self.document.announce_timer_key(),
            after: AUTOMERGE_ANNOUNCE_INTERVAL,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            AnnounceAutomergeDocumentState::ResetTimer => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = AnnounceAutomergeDocumentState::Subscribe;
                    smallvec![Effect::Net(NetEffect::Gossip(GossipEffect::Subscribe {
                        topic: self.document.topic_id(),
                    }))]
                }
                Event::Task(TaskEvent::Error { message, .. }) => self.fail(
                    AnnounceAutomergeDocumentError::GossipError(GossipError::Other(message)),
                ),
                other => self.unexpected_event("task timer acknowledgement", format!("{other:?}")),
            },
            AnnounceAutomergeDocumentState::Subscribe => match event {
                Event::Net(NetEvent::Gossip(GossipEvent::Subscribed { .. }))
                | Event::Net(NetEvent::Gossip(GossipEvent::Error {
                    error: GossipError::AlreadySubscribed,
                })) => {
                    self.state = AnnounceAutomergeDocumentState::ReadDocument;
                    smallvec![read_effect(&self.document, None)]
                }
                Event::Net(NetEvent::Gossip(GossipEvent::Error { error })) => {
                    self.fail(error.into())
                }
                other => {
                    self.unexpected_event("gossip subscribe acknowledgement", format!("{other:?}"))
                }
            },
            AnnounceAutomergeDocumentState::ReadDocument => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(value) = value else {
                        return self.fail(AnnounceAutomergeDocumentError::DocumentNotFound);
                    };
                    let clock = match automerge_clock(&value) {
                        Ok(clock) => clock,
                        Err(error) => return self.fail(error.into()),
                    };
                    let message = match postcard::to_allocvec(&AutomergeState::new(
                        clock.heads,
                        clock.change_count,
                        self.local_node_id,
                    )) {
                        Ok(message) => message,
                        Err(error) => return self.fail(ConversionError::from(error).into()),
                    };
                    self.state = AnnounceAutomergeDocumentState::Broadcast;
                    smallvec![Effect::Net(NetEffect::Gossip(GossipEffect::Broadcast {
                        topic: self.document.topic_id(),
                        message,
                    }))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            AnnounceAutomergeDocumentState::Broadcast => match event {
                Event::Net(NetEvent::Gossip(GossipEvent::BroadcastComplete { .. })) => {
                    self.state = AnnounceAutomergeDocumentState::Finish;
                    self.output = Some(Ok(()));
                    smallvec![]
                }
                Event::Net(NetEvent::Gossip(GossipEvent::Error { error })) => {
                    self.fail(error.into())
                }
                other => {
                    self.unexpected_event("gossip broadcast acknowledgement", format!("{other:?}"))
                }
            },
            AnnounceAutomergeDocumentState::Finish | AnnounceAutomergeDocumentState::Error => {
                smallvec![]
            }
            AnnounceAutomergeDocumentState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AnnounceAutomergeDocumentState::Finish | AnnounceAutomergeDocumentState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
