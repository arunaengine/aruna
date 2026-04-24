use aruna_core::automerge::{
    AutomergeDocumentVariant, AutomergeEffect, AutomergeEvent, AutomergeInit, AutomergeSyncError,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::onboarding::OnboardingSyncTicket;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::User;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::announce::AnnounceTopicOperation;
use crate::automerge::repository::{automerge_heads, read_effect, write_effect};
use crate::user_subject_index::rewrite_subject_index_effects;
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
    previous_user: Option<Option<User>>,
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
    UpdateDerivedState,
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
            previous_user: None,
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

    fn emit_derived_user_updates(
        &mut self,
        document: &AutomergeDocumentVariant,
        txn_id: TxnId,
    ) -> Result<Effects, IncomingAutomergeError> {
        if !matches!(document, AutomergeDocumentVariant::User { .. }) {
            self.state = IncomingAutomergeState::CommitPersist;
            return Ok(smallvec![Effect::Storage(
                StorageEffect::CommitTransaction { txn_id }
            )]);
        }

        let previous_user = match self.local_document.as_deref() {
            Some(bytes) if !bytes.is_empty() => Some(User::from_bytes(bytes)?),
            _ => None,
        };
        let current_user = User::from_bytes(self.synced_document.as_deref().unwrap_or_default())?;
        self.previous_user = Some(previous_user);
        let effects = rewrite_subject_index_effects(
            self.previous_user.as_ref().and_then(|user| user.as_ref()),
            &current_user,
            txn_id,
        )?;
        if effects.is_empty() {
            self.state = IncomingAutomergeState::CommitPersist;
            Ok(smallvec![Effect::Storage(
                StorageEffect::CommitTransaction { txn_id }
            )])
        } else {
            self.state = IncomingAutomergeState::UpdateDerivedState;
            Ok(effects)
        }
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
                    self.local_document = Some(current);
                    self.synced_document = Some(merged.clone());
                    self.state = IncomingAutomergeState::Persist;
                    smallvec![write_effect(&remote_init.document, merged, Some(txn_id))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            IncomingAutomergeState::Persist => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(document) =
                        self.remote_init.as_ref().map(|init| init.document.clone())
                    else {
                        return self.fail(IncomingAutomergeError::Sync(
                            AutomergeSyncError::InvalidInit,
                        ));
                    };
                    let Some(txn_id) = self.persist_txn_id else {
                        return self.fail(IncomingAutomergeError::StorageError(
                            StorageError::TransactionNotFound,
                        ));
                    };
                    match self.emit_derived_user_updates(&document, txn_id) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage write result", format!("{other:?}")),
            },
            IncomingAutomergeState::UpdateDerivedState => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. })
                | Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    let Some(txn_id) = self.persist_txn_id else {
                        return self.fail(IncomingAutomergeError::StorageError(
                            StorageError::TransactionNotFound,
                        ));
                    };
                    self.state = IncomingAutomergeState::CommitPersist;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("subject index update result", format!("{other:?}")),
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
                        AnnounceTopicOperation::new_for_document(
                            remote_init.document.topic_id(),
                            self.local_node_id,
                            Some(remote_init.document.clone()),
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

#[cfg(test)]
mod tests {
    use super::IncomingAutomergeOperation;
    use aruna_core::UserId;
    use aruna_core::automerge::{AutomergeDocumentVariant, AutomergeEvent, AutomergeInit};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{Actor, RealmId, User};
    use byteview::ByteView;
    use ulid::Ulid;

    fn node_id(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn user_bytes(user_id: UserId, subject_ids: Vec<String>) -> Vec<u8> {
        User {
            user_id,
            name: "Alice".to_string(),
            subject_ids,
            attributes: Default::default(),
        }
        .to_bytes(&Actor {
            node_id: node_id(1),
            user_id,
            realm_id: user_id.realm_id,
        })
        .unwrap()
    }

    #[test]
    fn incoming_user_sync_commits_when_no_subject_index_updates_exist() {
        let sync_id = Ulid::new();
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let document = AutomergeDocumentVariant::User { user_id };
        let bytes = user_bytes(user_id, Vec::new());
        let txn_id = Ulid::new();
        let mut operation = IncomingAutomergeOperation::new(sync_id, node_id(4), node_id(5));

        operation.start();
        operation.step(Event::Automerge(AutomergeEvent::SyncInitialized {
            sync_id,
            peer: node_id(4),
            remote_init: AutomergeInit::new(document.clone(), Vec::new()),
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(user_id.to_bytes()),
            value: None,
        }));
        operation.step(Event::Automerge(AutomergeEvent::SyncFinished {
            sync_id,
            document,
            before_heads: Vec::new(),
            after_heads: Vec::new(),
            updated_document: bytes,
            changed: true,
        }));
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(user_id.to_bytes()),
            value: None,
        }));
        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: ByteView::from(user_id.to_bytes()),
        }));

        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::CommitTransaction { txn_id: id }))
                if *id == txn_id
        ));
    }
}
