use aruna_core::automerge::AutomergeState;
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{boxed_suboperation, Operation};
use aruna_core::task::{TaskEffect, TaskEvent};
use smallvec::smallvec;
use thiserror::Error;

use crate::automerge::repository::{automerge_heads, read_effect};
use crate::automerge_announce::AUTOMERGE_ANNOUNCE_INTERVAL;
use crate::outgoing_automerge::OutgoingAutomergeOperation;

#[derive(Debug, PartialEq)]
pub struct IncomingGossipOperation {
    topic: aruna_core::TopicId,
    sender: aruna_core::NodeId,
    data: Vec<u8>,
    announcement: Option<AutomergeState>,
    state: IncomingGossipState,
    output: Option<Result<(), IncomingGossipError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum IncomingGossipState {
    Init,
    ResetTimer,
    ReadDocument,
    WaitForSync,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum IncomingGossipError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("invalid gossip announcement")]
    InvalidAnnouncement,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl IncomingGossipOperation {
    pub fn new(topic: aruna_core::TopicId, sender: aruna_core::NodeId, data: Vec<u8>) -> Self {
        Self {
            topic,
            sender,
            data,
            announcement: None,
            state: IncomingGossipState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: IncomingGossipError) -> aruna_core::types::Effects {
        self.state = IncomingGossipState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(IncomingGossipError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for IncomingGossipOperation {
    type Output = ();
    type Error = IncomingGossipError;

    fn start(&mut self) -> aruna_core::types::Effects {
        let announcement: AutomergeState = match postcard::from_bytes(&self.data) {
            Ok(announcement) => announcement,
            Err(_) => {
                self.state = IncomingGossipState::Finish;
                self.output = Some(Ok(()));
                return smallvec![];
            }
        };

        if announcement.document.topic_id() != self.topic {
            self.state = IncomingGossipState::Finish;
            self.output = Some(Ok(()));
            return smallvec![];
        }

        self.announcement = Some(announcement.clone());
        self.state = IncomingGossipState::ResetTimer;
        smallvec![Effect::Task(TaskEffect::ResetTimer {
            key: announcement.document.announce_timer_key(),
            after: AUTOMERGE_ANNOUNCE_INTERVAL,
        })]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            IncomingGossipState::ResetTimer => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    let Some(announcement) = self.announcement.as_ref() else {
                        return self.fail(IncomingGossipError::InvalidAnnouncement);
                    };
                    self.state = IncomingGossipState::ReadDocument;
                    smallvec![read_effect(&announcement.document, None)]
                }
                Event::Task(TaskEvent::Error { message, .. }) => self.fail(
                    IncomingGossipError::ConversionError(ConversionError::RoCrateError(message)),
                ),
                other => self.unexpected_event("task timer acknowledgement", format!("{other:?}")),
            },
            IncomingGossipState::ReadDocument => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(announcement) = self.announcement.as_ref() else {
                        return self.fail(IncomingGossipError::InvalidAnnouncement);
                    };
                    let value = value.map(|value| value.to_vec()).unwrap_or_default();
                    let heads = match automerge_heads(&value) {
                        Ok(heads) => heads,
                        Err(error) => return self.fail(error.into()),
                    };
                    if heads == announcement.heads {
                        self.state = IncomingGossipState::Finish;
                        self.output = Some(Ok(()));
                        return smallvec![];
                    }

                    self.state = IncomingGossipState::WaitForSync;
                    smallvec![Effect::SubOperation(boxed_suboperation(
                        OutgoingAutomergeOperation::new(self.sender, announcement.document.clone(),),
                        |result| {
                            Event::SubOperation(SubOperationEvent::AutomergeSyncResult {
                                result: result.map_err(|error| error.to_string()),
                            })
                        },
                    ))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            IncomingGossipState::WaitForSync => match event {
                Event::SubOperation(SubOperationEvent::AutomergeSyncResult { .. }) => {
                    self.state = IncomingGossipState::Finish;
                    self.output = Some(Ok(()));
                    smallvec![]
                }
                other => self.unexpected_event("automerge sync result", format!("{other:?}")),
            },
            IncomingGossipState::Finish
            | IncomingGossipState::Error
            | IncomingGossipState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            IncomingGossipState::Finish | IncomingGossipState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        smallvec![]
    }
}
