use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, RealmConfigDocument};
use smallvec::smallvec;
use thiserror::Error;

use crate::automerge::repository::{read_effect, write_effect};
use crate::automerge_announce::AnnounceAutomergeDocumentOperation;
use crate::outgoing_automerge::OutgoingAutomergeOperation;

#[derive(Debug, Clone, PartialEq)]
pub struct EnsureRealmConfigConfig {
    pub actor: Actor,
    pub bootstrap_peers: Vec<aruna_core::NodeId>,
    pub default_metadata_replication_factor: u32,
}

#[derive(Debug, PartialEq)]
pub struct EnsureRealmConfigOperation {
    config: EnsureRealmConfigConfig,
    txn_id: Option<aruna_core::types::TxnId>,
    state: EnsureRealmConfigState,
    replication_targets: Vec<aruna_core::NodeId>,
    output: Option<Result<RealmConfigDocument, EnsureRealmConfigError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum EnsureRealmConfigState {
    Init,
    StartTransaction,
    ReadCurrent,
    WriteDocument,
    CommitTransaction,
    Announce,
    Replicate,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum EnsureRealmConfigError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("automerge announcement failed: {0}")]
    AutomergeState(String),
    #[error("automerge replication failed: {0}")]
    AutomergeSync(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl EnsureRealmConfigOperation {
    pub fn new(config: EnsureRealmConfigConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: EnsureRealmConfigState::Init,
            replication_targets: Vec::new(),
            output: None,
        }
    }

    fn document_ref(&self) -> AutomergeDocumentVariant {
        AutomergeDocumentVariant::RealmConfig {
            realm_id: self.config.actor.realm_id.clone(),
        }
    }

    fn fail(&mut self, error: EnsureRealmConfigError) -> aruna_core::types::Effects {
        let cleanup = self.abort();
        self.state = EnsureRealmConfigState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(EnsureRealmConfigError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for EnsureRealmConfigOperation {
    type Output = RealmConfigDocument;
    type Error = EnsureRealmConfigError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = EnsureRealmConfigState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            EnsureRealmConfigState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.txn_id = Some(txn_id);
                    self.state = EnsureRealmConfigState::ReadCurrent;
                    smallvec![read_effect(&self.document_ref(), Some(txn_id))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            EnsureRealmConfigState::ReadCurrent => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => match value.as_deref() {
                    Some(value) => match RealmConfigDocument::from_bytes(value) {
                        Ok(document) => {
                            self.output = Some(Ok(document));
                            let Some(txn_id) = self.txn_id else {
                                return self.fail(EnsureRealmConfigError::MissingTransaction);
                            };
                            self.state = EnsureRealmConfigState::CommitTransaction;
                            smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                        }
                        Err(error) => self.fail(error.into()),
                    },
                    None => {
                        let document = RealmConfigDocument::new(
                            self.config.actor.realm_id.clone(),
                            self.config.default_metadata_replication_factor,
                        );
                        let bytes = match document.to_bytes(&self.config.actor) {
                            Ok(bytes) => bytes,
                            Err(error) => return self.fail(error.into()),
                        };
                        let Some(txn_id) = self.txn_id else {
                            return self.fail(EnsureRealmConfigError::MissingTransaction);
                        };
                        self.output = Some(Ok(document));
                        self.state = EnsureRealmConfigState::WriteDocument;
                        smallvec![write_effect(&self.document_ref(), bytes, Some(txn_id))]
                    }
                },
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            EnsureRealmConfigState::WriteDocument => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(EnsureRealmConfigError::MissingTransaction);
                    };
                    self.state = EnsureRealmConfigState::CommitTransaction;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage write result", format!("{other:?}")),
            },
            EnsureRealmConfigState::CommitTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = EnsureRealmConfigState::Announce;
                    smallvec![Effect::SubOperation(boxed_suboperation(
                        AnnounceAutomergeDocumentOperation::new(self.document_ref()),
                        |result| {
                            Event::SubOperation(SubOperationEvent::AutomergeStateResult {
                                result: result.map_err(|error| error.to_string()),
                            })
                        },
                    ))]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            EnsureRealmConfigState::Announce => match event {
                Event::SubOperation(SubOperationEvent::AutomergeStateResult { result }) => {
                    match result {
                        Ok(()) => {
                            self.replication_targets = self
                                .config
                                .bootstrap_peers
                                .iter()
                                .copied()
                                .filter(|node_id| *node_id != self.config.actor.node_id)
                                .collect();
                            if self.replication_targets.is_empty() {
                                self.state = EnsureRealmConfigState::Finish;
                                smallvec![]
                            } else {
                                self.state = EnsureRealmConfigState::Replicate;
                                let document = self.document_ref();
                                emit_next_replication(&mut self.replication_targets, document)
                            }
                        }
                        Err(error) => self.fail(EnsureRealmConfigError::AutomergeState(error)),
                    }
                }
                other => {
                    self.unexpected_event("automerge announcement result", format!("{other:?}"))
                }
            },
            EnsureRealmConfigState::Replicate => match event {
                Event::SubOperation(SubOperationEvent::AutomergeSyncResult { result }) => {
                    match result {
                        Ok(()) => {
                            if self.replication_targets.is_empty() {
                                self.state = EnsureRealmConfigState::Finish;
                                smallvec![]
                            } else {
                                let document = self.document_ref();
                                emit_next_replication(&mut self.replication_targets, document)
                            }
                        }
                        Err(error) => self.fail(EnsureRealmConfigError::AutomergeSync(error)),
                    }
                }
                other => self.unexpected_event("automerge sync result", format!("{other:?}")),
            },
            EnsureRealmConfigState::Finish
            | EnsureRealmConfigState::Error
            | EnsureRealmConfigState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            EnsureRealmConfigState::Finish | EnsureRealmConfigState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or_else(|| {
            Ok(RealmConfigDocument::default_for_realm(
                self.config.actor.realm_id,
            ))
        })
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

fn emit_next_replication(
    targets: &mut Vec<aruna_core::NodeId>,
    document: AutomergeDocumentVariant,
) -> aruna_core::types::Effects {
    let Some(target) = targets.pop() else {
        return smallvec![];
    };

    smallvec![Effect::SubOperation(boxed_suboperation(
        OutgoingAutomergeOperation::new(target, document),
        |result| {
            Event::SubOperation(SubOperationEvent::AutomergeSyncResult {
                result: result.map_err(|error| error.to_string()),
            })
        },
    ))]
}
