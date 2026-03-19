use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{Effect, GossipEffect, NetEffect};
use aruna_core::errors::{GossipError, StorageError};
use aruna_core::events::{Event, GossipEvent, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::task::{TaskEffect, TaskEvent};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::automerge::repository::delete_effect;

#[derive(Debug, PartialEq)]
pub struct DeleteMetadataDocumentOperation {
    document: AutomergeDocumentVariant,
    state: DeleteMetadataDocumentState,
    output: Option<Result<(), DeleteMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum DeleteMetadataDocumentState {
    Init,
    DeleteStorage,
    CancelTimer,
    Unsubscribe,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteMetadataDocumentError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    GossipError(#[from] GossipError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl DeleteMetadataDocumentOperation {
    pub fn new(group_id: Ulid, document_id: Ulid) -> Self {
        Self {
            document: AutomergeDocumentVariant::Metadata {
                group_id,
                document_id,
            },
            state: DeleteMetadataDocumentState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: DeleteMetadataDocumentError) -> aruna_core::types::Effects {
        self.state = DeleteMetadataDocumentState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(DeleteMetadataDocumentError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for DeleteMetadataDocumentOperation {
    type Output = ();
    type Error = DeleteMetadataDocumentError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = DeleteMetadataDocumentState::DeleteStorage;
        smallvec![delete_effect(&self.document, None)]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            DeleteMetadataDocumentState::DeleteStorage => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    self.state = DeleteMetadataDocumentState::CancelTimer;
                    smallvec![Effect::Task(TaskEffect::CancelTimer {
                        key: self.document.announce_timer_key(),
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage delete result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::CancelTimer => match event {
                Event::Task(TaskEvent::TimerCancelled { .. })
                | Event::Task(TaskEvent::Error { .. }) => {
                    self.state = DeleteMetadataDocumentState::Unsubscribe;
                    smallvec![Effect::Net(NetEffect::Gossip(GossipEffect::Unsubscribe {
                        topic: self.document.topic_id(),
                    }))]
                }
                other => self.unexpected_event("task timer result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::Unsubscribe => match event {
                Event::Net(aruna_core::events::NetEvent::Gossip(GossipEvent::Unsubscribed {
                    ..
                }))
                | Event::Net(aruna_core::events::NetEvent::Gossip(GossipEvent::Error {
                    error: GossipError::NotSubscribed,
                })) => {
                    self.state = DeleteMetadataDocumentState::Finish;
                    self.output = Some(Ok(()));
                    smallvec![]
                }
                Event::Net(aruna_core::events::NetEvent::Gossip(GossipEvent::Error { error })) => {
                    self.fail(error.into())
                }
                other => self.unexpected_event("gossip unsubscribe result", format!("{other:?}")),
            },
            DeleteMetadataDocumentState::Finish
            | DeleteMetadataDocumentState::Error
            | DeleteMetadataDocumentState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteMetadataDocumentState::Finish | DeleteMetadataDocumentState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        smallvec![]
    }
}
