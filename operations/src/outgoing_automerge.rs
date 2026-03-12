use aruna_core::automerge::{
    AutomergeDocumentVariant, AutomergeEffect, AutomergeEvent, AutomergeInit, AutomergeSyncError,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use smallvec::smallvec;
use thiserror::Error;

use crate::automerge::repository::{automerge_heads, read_effect, write_effect};

#[derive(Debug, PartialEq)]
pub struct OutgoingAutomergeOperation {
    peer: aruna_core::NodeId,
    document: AutomergeDocumentVariant,
    state: OutgoingAutomergeState,
    local_document: Option<Vec<u8>>,
    persist_txn_id: Option<aruna_core::types::TxnId>,
    synced_document: Option<Vec<u8>>,
    output: Option<Result<(), OutgoingAutomergeError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum OutgoingAutomergeState {
    Init,
    ReadLocal,
    InitializeSession,
    RunSync,
    StartPersistTransaction,
    ReconcileRead,
    Persist,
    CommitPersist,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum OutgoingAutomergeError {
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

impl OutgoingAutomergeOperation {
    pub fn new(peer: aruna_core::NodeId, document: AutomergeDocumentVariant) -> Self {
        Self {
            peer,
            document,
            state: OutgoingAutomergeState::Init,
            local_document: None,
            persist_txn_id: None,
            synced_document: None,
            output: None,
        }
    }

    fn fail(&mut self, error: OutgoingAutomergeError) -> aruna_core::types::Effects {
        let cleanup = self.abort();
        self.state = OutgoingAutomergeState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(OutgoingAutomergeError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for OutgoingAutomergeOperation {
    type Output = ();
    type Error = OutgoingAutomergeError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = OutgoingAutomergeState::ReadLocal;
        smallvec![read_effect(&self.document, None)]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            OutgoingAutomergeState::ReadLocal => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let bytes = value.map(|value| value.to_vec()).unwrap_or_default();
                    let heads = match automerge_heads(&bytes) {
                        Ok(heads) => heads,
                        Err(error) => return self.fail(error.into()),
                    };
                    self.local_document = Some(bytes);
                    self.state = OutgoingAutomergeState::InitializeSession;
                    smallvec![Effect::Automerge(AutomergeEffect::StartOutboundSync {
                        peer: self.peer,
                        init: AutomergeInit::new(self.document.clone(), heads),
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            OutgoingAutomergeState::InitializeSession => match event {
                Event::Automerge(AutomergeEvent::SyncInitialized { sync_id, .. }) => {
                    self.state = OutgoingAutomergeState::RunSync;
                    smallvec![Effect::Automerge(AutomergeEffect::RunSync {
                        sync_id,
                        local_document: self.local_document.clone().unwrap_or_default(),
                        response_init: None,
                    })]
                }
                Event::Automerge(AutomergeEvent::SyncRejected { error, .. }) => {
                    self.fail(OutgoingAutomergeError::Sync(error))
                }
                other => {
                    self.unexpected_event("automerge session initialization", format!("{other:?}"))
                }
            },
            OutgoingAutomergeState::RunSync => match event {
                Event::Automerge(AutomergeEvent::SyncFinished {
                    changed,
                    updated_document,
                    ..
                }) => {
                    if !changed {
                        self.state = OutgoingAutomergeState::Finish;
                        self.output = Some(Ok(()));
                        return smallvec![];
                    }

                    self.synced_document = Some(updated_document);
                    self.state = OutgoingAutomergeState::StartPersistTransaction;
                    smallvec![Effect::Storage(StorageEffect::StartTransaction {
                        read: false,
                    })]
                }
                Event::Automerge(AutomergeEvent::SyncRejected { error, .. }) => {
                    self.fail(OutgoingAutomergeError::Sync(error))
                }
                other => {
                    self.unexpected_event("automerge session completion", format!("{other:?}"))
                }
            },
            OutgoingAutomergeState::StartPersistTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.persist_txn_id = Some(txn_id);
                    self.state = OutgoingAutomergeState::ReconcileRead;
                    smallvec![read_effect(&self.document, Some(txn_id))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            OutgoingAutomergeState::ReconcileRead => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let current = value.map(|value| value.to_vec()).unwrap_or_default();
                    let merged =
                        match reconcile_documents(&current, self.synced_document.as_deref()) {
                            Ok(merged) => merged,
                            Err(error) => return self.fail(error.into()),
                        };

                    let Some(txn_id) = self.persist_txn_id else {
                        return self.fail(OutgoingAutomergeError::StorageError(
                            StorageError::TransactionNotFound,
                        ));
                    };
                    self.state = OutgoingAutomergeState::Persist;
                    smallvec![write_effect(&self.document, merged, Some(txn_id))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            OutgoingAutomergeState::Persist => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.persist_txn_id else {
                        return self.fail(OutgoingAutomergeError::StorageError(
                            StorageError::TransactionNotFound,
                        ));
                    };
                    self.state = OutgoingAutomergeState::CommitPersist;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage write result", format!("{other:?}")),
            },
            OutgoingAutomergeState::CommitPersist => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.persist_txn_id = None;
                    self.state = OutgoingAutomergeState::Finish;
                    self.output = Some(Ok(()));
                    smallvec![]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.persist_txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            OutgoingAutomergeState::Finish
            | OutgoingAutomergeState::Error
            | OutgoingAutomergeState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            OutgoingAutomergeState::Finish | OutgoingAutomergeState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        match self.persist_txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
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
