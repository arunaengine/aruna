use aruna_core::automerge::{
    AutomergeDocumentVariant, AutomergeEffect, AutomergeEvent, AutomergeInit, AutomergeSyncError,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::onboarding::OnboardingSyncTicket;
use aruna_core::operation::{Operation, boxed_suboperation};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::announce::AnnounceTopicOperation;
use crate::automerge::repository::{automerge_heads, read_effect, write_effect};
use aruna_core::NodeId;
use aruna_core::types::Effects;
use aruna_core::types::TxnId;

#[derive(Debug, PartialEq)]
pub struct IncomingAutomergeOperation {
    sync_id: Ulid,
    node_id: NodeId,
    local_node_id: NodeId,
    state: IncomingAutomergeState,
    remote_init: Option<AutomergeInit>,
    local_document: Option<Vec<u8>>,
    persist_txn_id: Option<TxnId>,
    synced_document: Option<Vec<u8>>,
    output: Option<Result<(), IncomingAutomergeError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum IncomingAutomergeState {
    Init,
    AwaitInit,
    LoadLocal,
    RunSync,
    StartPersistTransaction,
    ReconcileRead,
    Persist,
    CommitPersist,
    Announce,
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
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl IncomingAutomergeOperation {
    pub fn new(sync_id: Ulid, node_id: NodeId, local_node_id: NodeId) -> Self {
        Self {
            sync_id,
            node_id,
            local_node_id,
            state: IncomingAutomergeState::Init,
            remote_init: None,
            local_document: None,
            persist_txn_id: None,
            synced_document: None,
            output: None,
        }
    }

    fn fail(&mut self, error: IncomingAutomergeError) -> Effects {
        let cleanup = self.abort();
        self.state = IncomingAutomergeState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(IncomingAutomergeError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }

    fn reject_unauthorized(&mut self) -> Effects {
        self.state = IncomingAutomergeState::Error;
        self.output = Some(Err(IncomingAutomergeError::Sync(
            AutomergeSyncError::Unauthorized,
        )));
        smallvec![Effect::Automerge(AutomergeEffect::RejectSync {
            sync_id: self.sync_id,
            reason: aruna_core::automerge::AutomergeRejectReason::Unauthorized,
        })]
    }

    fn requires_onboarding_auth(document: &AutomergeDocumentVariant, heads_empty: bool) -> bool {
        heads_empty
            && matches!(
                document,
                AutomergeDocumentVariant::RealmAuthorization { .. }
                    | AutomergeDocumentVariant::RealmConfig { .. }
            )
    }

    fn validate_onboarding_auth(
        &self,
        remote_init: &AutomergeInit,
    ) -> Result<(), IncomingAutomergeError> {
        if !Self::requires_onboarding_auth(&remote_init.document, remote_init.heads.is_empty()) {
            return Ok(());
        }

        let auth = remote_init
            .auth
            .as_ref()
            .ok_or(IncomingAutomergeError::Sync(
                AutomergeSyncError::Unauthorized,
            ))?;
        let ticket = OnboardingSyncTicket::from_auth_proof(auth)
            .map_err(|_| IncomingAutomergeError::Sync(AutomergeSyncError::Unauthorized))?;
        ticket
            .verify(
                self.node_id,
                &remote_init.document,
                chrono::Utc::now().timestamp().max(0) as u64,
            )
            .map_err(|_| IncomingAutomergeError::Sync(AutomergeSyncError::Unauthorized))
    }
}

impl Operation for IncomingAutomergeOperation {
    type Output = ();
    type Error = IncomingAutomergeError;

    fn start(&mut self) -> Effects {
        self.state = IncomingAutomergeState::AwaitInit;
        smallvec![Effect::Automerge(AutomergeEffect::StartInboundSync {
            sync_id: self.sync_id,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            IncomingAutomergeState::AwaitInit => match event {
                Event::Automerge(AutomergeEvent::SyncInitialized { remote_init, .. }) => {
                    if self.validate_onboarding_auth(&remote_init).is_err() {
                        return self.reject_unauthorized();
                    }
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
                    self.state = IncomingAutomergeState::StartPersistTransaction;
                    smallvec![Effect::Storage(StorageEffect::StartTransaction {
                        read: false,
                    })]
                }
                Event::Automerge(AutomergeEvent::SyncRejected { error, .. }) => {
                    self.fail(IncomingAutomergeError::Sync(error))
                }
                other => {
                    self.unexpected_event("automerge session completion", format!("{other:?}"))
                }
            },
            IncomingAutomergeState::StartPersistTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    let Some(remote_init) = self.remote_init.as_ref() else {
                        return self.fail(IncomingAutomergeError::Sync(
                            AutomergeSyncError::InvalidInit,
                        ));
                    };
                    self.persist_txn_id = Some(txn_id);
                    self.state = IncomingAutomergeState::ReconcileRead;
                    smallvec![read_effect(&remote_init.document, Some(txn_id))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
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
                    let Some(txn_id) = self.persist_txn_id else {
                        return self.fail(IncomingAutomergeError::StorageError(
                            StorageError::TransactionNotFound,
                        ));
                    };
                    self.state = IncomingAutomergeState::Persist;
                    smallvec![write_effect(&remote_init.document, merged, Some(txn_id))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            IncomingAutomergeState::Persist => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.persist_txn_id else {
                        return self.fail(IncomingAutomergeError::StorageError(
                            StorageError::TransactionNotFound,
                        ));
                    };
                    self.state = IncomingAutomergeState::CommitPersist;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage write result", format!("{other:?}")),
            },
            IncomingAutomergeState::CommitPersist => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.persist_txn_id = None;
                    let Some(remote_init) = self.remote_init.as_ref() else {
                        return self.fail(IncomingAutomergeError::Sync(
                            AutomergeSyncError::InvalidInit,
                        ));
                    };
                    self.state = IncomingAutomergeState::Announce;
                    smallvec![Effect::SubOperation(boxed_suboperation(
                        AnnounceTopicOperation::new(
                            remote_init.document.topic_id(),
                            self.local_node_id,
                        ),
                        |result| {
                            Event::SubOperation(SubOperationEvent::TopicAnnouncementResult {
                                result: result.map_err(|error| error.to_string()),
                            })
                        },
                    ))]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.persist_txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            IncomingAutomergeState::Announce => match event {
                Event::SubOperation(SubOperationEvent::TopicAnnouncementResult { result }) => {
                    match result {
                        Ok(()) => {
                            self.state = IncomingAutomergeState::Finish;
                            self.output = Some(Ok(()));
                            smallvec![]
                        }
                        Err(error) => self.fail(IncomingAutomergeError::TopicAnnouncement(error)),
                    }
                }
                other => {
                    self.unexpected_event("automerge announcement result", format!("{other:?}"))
                }
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

    fn abort(&mut self) -> Effects {
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
