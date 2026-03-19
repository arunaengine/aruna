use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{DhtEffect, Effect, NetEffect, StorageEffect};
use aruna_core::errors::{ConversionError, DhtError, StorageError};
use aruna_core::events::{DhtEvent, Event, NetEvent, StorageEvent, SubOperationEvent};
use aruna_core::keys::realm_presence_key;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, MetadataDocument, RealmConfigDocument};
use rand::seq::SliceRandom;
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;

use crate::automerge::repository::{read_effect, write_effect};
use crate::automerge_announce::AnnounceAutomergeDocumentOperation;
use crate::outgoing_automerge::OutgoingAutomergeOperation;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateMetadataDocumentConfig {
    pub actor: Actor,
    pub document: MetadataDocument,
}

#[derive(Debug, PartialEq)]
pub struct CreateMetadataDocumentOperation {
    config: CreateMetadataDocumentConfig,
    txn_id: Option<aruna_core::types::TxnId>,
    state: CreateMetadataDocumentState,
    replication_targets: Vec<aruna_core::NodeId>,
    selected_replication_factor: usize,
    output: Option<Result<MetadataDocument, CreateMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum CreateMetadataDocumentState {
    Init,
    StartTransaction,
    CheckExisting,
    WriteDocument,
    CommitTransaction,
    Announce,
    LoadRealmConfig,
    LoadReplicationTargets,
    Replicate,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateMetadataDocumentError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    DhtError(#[from] DhtError),
    #[error("document already exists")]
    DocumentAlreadyExists,
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

impl CreateMetadataDocumentOperation {
    pub fn new(config: CreateMetadataDocumentConfig) -> Self {
        Self {
            config,
            txn_id: None,
            state: CreateMetadataDocumentState::Init,
            replication_targets: Vec::new(),
            selected_replication_factor: 1,
            output: None,
        }
    }

    fn document_ref(&self) -> AutomergeDocumentVariant {
        AutomergeDocumentVariant::Metadata {
            group_id: self.config.document.group_id,
            document_id: self.config.document.document_id,
        }
    }

    fn realm_config_ref(&self) -> AutomergeDocumentVariant {
        AutomergeDocumentVariant::RealmConfig {
            realm_id: self.config.actor.realm_id.clone(),
        }
    }

    fn finish_success(&mut self) -> aruna_core::types::Effects {
        self.state = CreateMetadataDocumentState::Finish;
        if self.output.is_none() {
            self.output = Some(Ok(self.config.document.clone()));
        }
        smallvec![]
    }

    fn fail(&mut self, error: CreateMetadataDocumentError) -> aruna_core::types::Effects {
        let cleanup = self.abort();
        self.state = CreateMetadataDocumentState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(
        &mut self,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let state = format!("{:?}", self.state);
        self.fail(CreateMetadataDocumentError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for CreateMetadataDocumentOperation {
    type Output = MetadataDocument;
    type Error = CreateMetadataDocumentError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = CreateMetadataDocumentState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        match self.state {
            CreateMetadataDocumentState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.txn_id = Some(txn_id);
                    self.state = CreateMetadataDocumentState::CheckExisting;
                    smallvec![read_effect(&self.document_ref(), Some(txn_id))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::CheckExisting => match event {
                Event::Storage(StorageEvent::ReadResult { value: Some(_), .. }) => {
                    self.fail(CreateMetadataDocumentError::DocumentAlreadyExists)
                }
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
                    let bytes = match self
                        .config
                        .document
                        .reconcile_bytes(None, &self.config.actor)
                    {
                        Ok(bytes) => bytes,
                        Err(error) => return self.fail(error.into()),
                    };
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(CreateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = CreateMetadataDocumentState::WriteDocument;
                    smallvec![write_effect(&self.document_ref(), bytes, Some(txn_id))]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::WriteDocument => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(CreateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = CreateMetadataDocumentState::CommitTransaction;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage write result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::CommitTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.output = Some(Ok(self.config.document.clone()));
                    self.state = CreateMetadataDocumentState::Announce;
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
            CreateMetadataDocumentState::Announce => match event {
                Event::SubOperation(SubOperationEvent::AutomergeStateResult { result }) => {
                    match result {
                        Ok(()) => {
                            self.state = CreateMetadataDocumentState::LoadRealmConfig;
                            smallvec![read_effect(&self.realm_config_ref(), None)]
                        }
                        Err(error) => self.fail(CreateMetadataDocumentError::AutomergeState(error)),
                    }
                }
                other => {
                    self.unexpected_event("automerge announcement result", format!("{other:?}"))
                }
            },
            CreateMetadataDocumentState::LoadRealmConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let realm_config = match value.as_deref() {
                        Some(bytes) => {
                            RealmConfigDocument::from_bytes(bytes).unwrap_or_else(|_| {
                                RealmConfigDocument::default_for_realm(
                                    self.config.actor.realm_id.clone(),
                                )
                            })
                        }
                        None => RealmConfigDocument::default_for_realm(
                            self.config.actor.realm_id.clone(),
                        ),
                    };
                    self.selected_replication_factor = realm_config
                        .metadata_replication_factor_for(self.config.document.group_id, None);
                    self.state = CreateMetadataDocumentState::LoadReplicationTargets;
                    smallvec![Effect::Net(NetEffect::Dht(DhtEffect::Get {
                        key: *realm_presence_key(&self.config.actor.realm_id).as_bytes(),
                    }))]
                }
                Event::Storage(StorageEvent::Error { .. }) => {
                    self.selected_replication_factor =
                        RealmConfigDocument::default_for_realm(self.config.actor.realm_id.clone())
                            .metadata_replication_factor_for(self.config.document.group_id, None);
                    self.state = CreateMetadataDocumentState::LoadReplicationTargets;
                    smallvec![Effect::Net(NetEffect::Dht(DhtEffect::Get {
                        key: *realm_presence_key(&self.config.actor.realm_id).as_bytes(),
                    }))]
                }
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::LoadReplicationTargets => match event {
                Event::Net(NetEvent::Dht(DhtEvent::GetResult { values, .. })) => {
                    self.replication_targets = select_replication_targets(
                        values.into_iter().map(|entry| entry.node_id).collect(),
                        self.config.actor.node_id,
                        self.selected_replication_factor,
                    );
                    if self.replication_targets.is_empty() {
                        return self.finish_success();
                    }

                    self.state = CreateMetadataDocumentState::Replicate;
                    let document = self.document_ref();
                    emit_next_replication(&mut self.replication_targets, document)
                }
                Event::Net(NetEvent::Dht(DhtEvent::Error { error })) => self.fail(error.into()),
                other => self.unexpected_event("dht get result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::Replicate => match event {
                Event::SubOperation(SubOperationEvent::AutomergeSyncResult { result }) => {
                    match result {
                        Ok(()) => {
                            if self.replication_targets.is_empty() {
                                self.finish_success()
                            } else {
                                let document = self.document_ref();
                                emit_next_replication(&mut self.replication_targets, document)
                            }
                        }
                        Err(error) => self.fail(CreateMetadataDocumentError::AutomergeSync(error)),
                    }
                }
                other => self.unexpected_event("automerge sync result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::Finish
            | CreateMetadataDocumentState::Error
            | CreateMetadataDocumentState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateMetadataDocumentState::Finish | CreateMetadataDocumentState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(self.config.document))
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

pub(crate) fn select_replication_targets(
    realm_nodes: HashSet<aruna_core::NodeId>,
    local_node_id: aruna_core::NodeId,
    replication_factor: usize,
) -> Vec<aruna_core::NodeId> {
    let remote_target_count = replication_factor.max(1).saturating_sub(1);
    if remote_target_count == 0 {
        return Vec::new();
    }

    let mut candidates: Vec<_> = realm_nodes
        .into_iter()
        .filter(|node_id| *node_id != local_node_id)
        .collect();
    let mut rng = rand::rng();
    candidates.shuffle(&mut rng);
    candidates.truncate(remote_target_count);
    candidates
}

#[cfg(test)]
mod tests {
    use super::select_replication_targets;
    use std::collections::HashSet;

    #[test]
    fn select_replication_targets_excludes_local_node() {
        let local = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let remote_a = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let remote_b = iroh::SecretKey::from_bytes(&[3u8; 32]).public();

        let targets =
            select_replication_targets(HashSet::from([local, remote_a, remote_b]), local, 3);

        assert_eq!(targets.len(), 2);
        assert!(!targets.contains(&local));
    }
}
