use aruna_core::automerge::{
    AutomergeDocumentVariant, AutomergeEffect, AutomergeEvent, AutomergeInit, AutomergeSyncError,
    InitAuthProof,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::User;
use smallvec::smallvec;
use thiserror::Error;

use crate::automerge::repository::{automerge_heads, read_effect, write_effect};
use crate::user_subject_index::rewrite_subject_index_effects;

#[derive(Debug, PartialEq)]
pub struct OutgoingAutomergeOperation {
    peer: aruna_core::NodeId,
    document: AutomergeDocumentVariant,
    auth_proof: Option<InitAuthProof>,
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
    UpdateDerivedState,
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
            auth_proof: None,
            state: OutgoingAutomergeState::Init,
            local_document: None,
            persist_txn_id: None,
            synced_document: None,
            output: None,
        }
    }

    pub fn new_with_auth(
        peer: aruna_core::NodeId,
        document: AutomergeDocumentVariant,
        auth_proof: Option<InitAuthProof>,
    ) -> Self {
        Self {
            peer,
            document,
            auth_proof,
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

    fn emit_derived_user_updates(
        &mut self,
        txn_id: aruna_core::types::TxnId,
    ) -> Result<aruna_core::types::Effects, OutgoingAutomergeError> {
        if !matches!(self.document, AutomergeDocumentVariant::User { .. }) {
            self.state = OutgoingAutomergeState::CommitPersist;
            return Ok(smallvec![Effect::Storage(
                StorageEffect::CommitTransaction { txn_id }
            )]);
        }

        let previous_user = match self.local_document.as_deref() {
            Some(bytes) if !bytes.is_empty() => Some(User::from_bytes(bytes)?),
            _ => None,
        };
        let current_user = User::from_bytes(self.synced_document.as_deref().unwrap_or_default())?;
        let effects = rewrite_subject_index_effects(previous_user.as_ref(), &current_user, txn_id)?;
        if effects.is_empty() {
            self.state = OutgoingAutomergeState::CommitPersist;
            Ok(smallvec![Effect::Storage(
                StorageEffect::CommitTransaction { txn_id }
            )])
        } else {
            self.state = OutgoingAutomergeState::UpdateDerivedState;
            Ok(effects)
        }
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
                    let mut init = AutomergeInit::new(self.document.clone(), heads);
                    if let Some(auth_proof) = self.auth_proof.clone() {
                        init.capabilities
                            .push(aruna_core::automerge::AutomergeSyncFeature::InitAuthProof);
                        init.auth = Some(auth_proof);
                    }
                    smallvec![Effect::Automerge(AutomergeEffect::StartOutboundSync {
                        peer: self.peer,
                        init,
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
                    self.local_document = Some(current);
                    self.synced_document = Some(merged.clone());
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
                    match self.emit_derived_user_updates(txn_id) {
                        Ok(effects) => effects,
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage write result", format!("{other:?}")),
            },
            OutgoingAutomergeState::UpdateDerivedState => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. })
                | Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                    let Some(txn_id) = self.persist_txn_id else {
                        return self.fail(OutgoingAutomergeError::StorageError(
                            StorageError::TransactionNotFound,
                        ));
                    };
                    self.state = OutgoingAutomergeState::CommitPersist;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("subject index update result", format!("{other:?}")),
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

#[cfg(test)]
mod tests {
    use super::OutgoingAutomergeOperation;
    use aruna_core::automerge::{AutomergeDocumentVariant, AutomergeEvent, AutomergeInit};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{Actor, RealmId, User, oidc_subject_key};
    use aruna_core::{USER_SUBJECT_INDEX_KEYSPACE, UserId};
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
    fn outgoing_user_sync_rewrites_subject_index_before_commit() {
        let sync_id = Ulid::new();
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let subject_key = oidc_subject_key("https://issuer.example", "subject-1").unwrap();
        let document = AutomergeDocumentVariant::User { user_id };
        let bytes = user_bytes(user_id, vec![subject_key.clone()]);
        let txn_id = Ulid::new();
        let mut operation = OutgoingAutomergeOperation::new(node_id(4), document.clone());

        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(user_id.to_bytes()),
            value: None,
        }));
        operation.step(Event::Automerge(AutomergeEvent::SyncInitialized {
            sync_id,
            peer: node_id(4),
            remote_init: AutomergeInit::new(document.clone(), Vec::new()),
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

        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite { writes, txn_id: id }) => {
                assert_eq!(id, &Some(txn_id));
                assert_eq!(writes.len(), 1);
                let (key_space, key, value) = &writes[0];
                assert_eq!(key_space, USER_SUBJECT_INDEX_KEYSPACE);
                assert_eq!(key.as_ref(), subject_key.as_bytes());
                assert_eq!(value.as_ref(), user_id.to_string().as_bytes());
            }
            other => panic!("unexpected effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: vec![(
                USER_SUBJECT_INDEX_KEYSPACE.to_string(),
                ByteView::from(subject_key.into_bytes()),
            )],
        }));
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::CommitTransaction { txn_id: id }))
                if *id == txn_id
        ));
    }
}
