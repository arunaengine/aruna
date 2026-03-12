use aruna_core::automerge::{AutomergeEffect, AutomergeEvent, AutomergeInit, AutomergeSyncError};
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::automerge::repository::{automerge_heads, read_effect, write_effect};

#[derive(Debug, PartialEq)]
pub struct IncomingAutomergeOperation {
    sync_id: Ulid,
    node_id: aruna_core::NodeId,
    state: IncomingAutomergeState,
    remote_init: Option<AutomergeInit>,
    local_document: Option<Vec<u8>>,
    synced_document: Option<Vec<u8>>,
    output: Option<Result<(), IncomingAutomergeError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum IncomingAutomergeState {
    Init,
    AwaitInit,
    LoadLocal,
    RunSync,
    ReconcileRead,
    Persist,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum IncomingAutomergeError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("automerge sync error: {0:?}")]
    Sync(AutomergeSyncError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl IncomingAutomergeOperation {
    pub fn new(sync_id: Ulid, node_id: aruna_core::NodeId) -> Self {
        Self {
            sync_id,
            node_id,
            state: IncomingAutomergeState::Init,
            remote_init: None,
            local_document: None,
            synced_document: None,
            output: None,
        }
    }

    fn fail(&mut self, error: IncomingAutomergeError) -> aruna_core::types::Effects {
        self.state = IncomingAutomergeState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(IncomingAutomergeError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for IncomingAutomergeOperation {
    type Output = ();
    type Error = IncomingAutomergeError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = IncomingAutomergeState::AwaitInit;
        smallvec![Effect::Automerge(AutomergeEffect::StartInboundSync {
            sync_id: self.sync_id,
        })]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            IncomingAutomergeState::AwaitInit => match event {
                Event::Automerge(AutomergeEvent::SyncInitialized { remote_init, .. }) => {
                    let document = remote_init.document.clone();
                    self.remote_init = Some(remote_init);
                    self.state = IncomingAutomergeState::LoadLocal;
                    smallvec![read_effect(&document, None)]
                }
                Event::Automerge(AutomergeEvent::SyncRejected { error, .. }) => {
                    self.fail(IncomingAutomergeError::Sync(error))
                }
                other => self.unexpected_event("automerge init", format!("{other:?}")),
            },
            IncomingAutomergeState::LoadLocal => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(remote_init) = self.remote_init.clone() else {
                        return self.fail(IncomingAutomergeError::Sync(
                            AutomergeSyncError::InvalidInit,
                        ));
                    };
                    let local_document = value.map(|value| value.to_vec()).unwrap_or_default();
                    let heads = match automerge_heads(&local_document) {
                        Ok(heads) => heads,
                        Err(error) => return self.fail(error.into()),
                    };

                    self.local_document = Some(local_document.clone());
                    self.state = IncomingAutomergeState::RunSync;
                    smallvec![Effect::Automerge(AutomergeEffect::RunSync {
                        sync_id: self.sync_id,
                        local_document,
                        response_init: Some(AutomergeInit::new(remote_init.document, heads)),
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                Event::Automerge(AutomergeEvent::SyncRejected { error, .. }) => {
                    self.fail(IncomingAutomergeError::Sync(error))
                }
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            IncomingAutomergeState::RunSync => match event {
                Event::Automerge(AutomergeEvent::SyncFinished {
                    changed,
                    updated_document,
                    ..
                }) => {
                    if !changed {
                        self.state = IncomingAutomergeState::Finish;
                        self.output = Some(Ok(()));
                        return smallvec![];
                    }

                    self.synced_document = Some(updated_document);
                    let Some(remote_init) = self.remote_init.as_ref() else {
                        return self.fail(IncomingAutomergeError::Sync(
                            AutomergeSyncError::InvalidInit,
                        ));
                    };
                    self.state = IncomingAutomergeState::ReconcileRead;
                    smallvec![read_effect(&remote_init.document, None)]
                }
                Event::Automerge(AutomergeEvent::SyncRejected { error, .. }) => {
                    self.fail(IncomingAutomergeError::Sync(error))
                }
                other => {
                    self.unexpected_event("automerge session completion", format!("{other:?}"))
                }
            },
            IncomingAutomergeState::ReconcileRead => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let current = value.map(|value| value.to_vec()).unwrap_or_default();
                    let merged =
                        match reconcile_documents(&current, self.synced_document.as_deref()) {
                            Ok(merged) => merged,
                            Err(error) => return self.fail(error.into()),
                        };
                    let Some(remote_init) = self.remote_init.as_ref() else {
                        return self.fail(IncomingAutomergeError::Sync(
                            AutomergeSyncError::InvalidInit,
                        ));
                    };
                    self.state = IncomingAutomergeState::Persist;
                    smallvec![write_effect(&remote_init.document, merged, None)]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            IncomingAutomergeState::Persist => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    self.state = IncomingAutomergeState::Finish;
                    self.output = Some(Ok(()));
                    smallvec![]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage write result", format!("{other:?}")),
            },
            IncomingAutomergeState::Finish
            | IncomingAutomergeState::Error
            | IncomingAutomergeState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            IncomingAutomergeState::Finish | IncomingAutomergeState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        smallvec![]
    }
}

fn reconcile_documents(current: &[u8], session: Option<&[u8]>) -> Result<Vec<u8>, ConversionError> {
    let Some(session) = session else {
        return Ok(current.to_vec());
    };
    if current.is_empty() {
        return Ok(session.to_vec());
    }

    let mut current_doc = automerge::Automerge::load(current)?;
    let mut session_doc = automerge::Automerge::load(session)?;
    current_doc.merge(&mut session_doc)?;
    Ok(current_doc.save())
}
