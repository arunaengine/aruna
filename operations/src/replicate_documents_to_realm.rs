use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncTarget, PendingTopicPlacement};
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{RealmConfigDocument, RealmId};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

use crate::announce::AnnounceTopicOperation;
use crate::document_repository::read_effect;
use crate::sync_placement::{
    delete_pending_placement_effect, desired_peer_count, pending_placement_record,
    select_sync_peers, sort_node_ids, write_pending_placement_effect,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ReplicateDocumentsToRealmConfig {
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
    pub excluded_peers: Vec<NodeId>,
    pub documents: Vec<DocumentSyncTarget>,
}

#[derive(Debug, PartialEq)]
pub struct ReplicateDocumentsToRealmOperation {
    config: ReplicateDocumentsToRealmConfig,
    state: ReplicateDocumentsToRealmState,
    pending_documents: Vec<DocumentSyncTarget>,
    realm_nodes: Vec<NodeId>,
    placement_action: Option<PlacementAction>,
    output: Option<Result<(), ReplicateDocumentsToRealmError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ReplicateDocumentsToRealmState {
    Init,
    LoadRealmConfig,
    Publish,
    StorePlacement,
    Finish,
    Error,
}

#[derive(Debug, Clone, PartialEq)]
enum PlacementAction {
    Write(PendingTopicPlacement),
    Delete(DocumentSyncTarget),
}

#[derive(Debug, Error, PartialEq)]
pub enum ReplicateDocumentsToRealmError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("realm config document not found")]
    RealmConfigNotFound,
    #[error("document sync failed: {0}")]
    DocumentSync(String),
    #[error("placement persistence failed: {0}")]
    Placement(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

pub fn replicate_documents_to_realm_effect(
    realm_id: RealmId,
    local_node_id: NodeId,
    documents: Vec<DocumentSyncTarget>,
) -> Effect {
    Effect::SubOperation(boxed_suboperation(
        ReplicateDocumentsToRealmOperation::new(ReplicateDocumentsToRealmConfig {
            realm_id,
            local_node_id,
            excluded_peers: Vec::new(),
            documents,
        }),
        |result| {
            Event::SubOperation(SubOperationEvent::DocumentSyncResult {
                result: result.map_err(|error| error.to_string()),
            })
        },
    ))
}

impl ReplicateDocumentsToRealmOperation {
    pub fn new(config: ReplicateDocumentsToRealmConfig) -> Self {
        Self {
            pending_documents: config.documents.clone().into_iter().rev().collect(),
            config,
            state: ReplicateDocumentsToRealmState::Init,
            realm_nodes: Vec::new(),
            placement_action: None,
            output: None,
        }
    }

    fn fail(&mut self, error: ReplicateDocumentsToRealmError) -> Effects {
        self.state = ReplicateDocumentsToRealmState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(ReplicateDocumentsToRealmError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn finish_success(&mut self) -> Effects {
        self.state = ReplicateDocumentsToRealmState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }

    fn emit_next_publish(&mut self) -> Effects {
        let Some(document) = self.pending_documents.pop() else {
            return self.finish_success();
        };

        let desired_count = desired_peer_count(&document);
        if desired_count == 0 {
            return self.emit_next_publish();
        }

        let selected_peers = select_sync_peers(
            &document,
            self.config.local_node_id,
            &self.realm_nodes,
            &self.config.excluded_peers,
            desired_count,
        );
        self.placement_action = if selected_peers.len() < desired_count {
            Some(PlacementAction::Write(pending_placement_record(
                document.clone(),
                desired_count,
                selected_peers.clone(),
            )))
        } else {
            Some(PlacementAction::Delete(document.clone()))
        };

        if selected_peers.is_empty() {
            return match self.emit_placement_update() {
                Ok(effects) => effects,
                Err(error) => self.fail(error),
            };
        }

        self.state = ReplicateDocumentsToRealmState::Publish;
        smallvec![Effect::SubOperation(boxed_suboperation(
            AnnounceTopicOperation::new_for_document_with_peers(
                document.topic_id(),
                self.config.local_node_id,
                Some(document),
                selected_peers,
            ),
            |result| Event::SubOperation(SubOperationEvent::DocumentSyncResult {
                result: result.map_err(|error| error.to_string()),
            }),
        ))]
    }

    fn emit_placement_update(&mut self) -> Result<Effects, ReplicateDocumentsToRealmError> {
        let Some(action) = self.placement_action.take() else {
            return Ok(self.emit_next_publish());
        };
        self.state = ReplicateDocumentsToRealmState::StorePlacement;
        match action {
            PlacementAction::Write(record) => {
                Ok(smallvec![write_pending_placement_effect(&record).map_err(
                    |error| ReplicateDocumentsToRealmError::Placement(error.to_string())
                )?])
            }
            PlacementAction::Delete(target) => {
                Ok(smallvec![delete_pending_placement_effect(&target)])
            }
        }
    }
}

impl Operation for ReplicateDocumentsToRealmOperation {
    type Output = ();
    type Error = ReplicateDocumentsToRealmError;

    fn start(&mut self) -> Effects {
        self.state = ReplicateDocumentsToRealmState::LoadRealmConfig;
        smallvec![read_effect(
            &DocumentSyncTarget::RealmConfig {
                realm_id: self.config.realm_id,
            },
            None,
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReplicateDocumentsToRealmState::LoadRealmConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(value) = value else {
                        self.realm_nodes.clear();
                        return self.emit_next_publish();
                    };
                    let document = match RealmConfigDocument::from_bytes(&value) {
                        Ok(document) => document,
                        Err(error) => return self.fail(error.into()),
                    };
                    let mut nodes = match document.node_ids() {
                        Ok(nodes) => nodes,
                        Err(error) => return self.fail(error.into()),
                    };
                    nodes.retain(|node_id| *node_id != self.config.local_node_id);
                    sort_node_ids(&mut nodes);
                    self.realm_nodes = nodes;
                    self.emit_next_publish()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("realm config read result", format!("{other:?}")),
            },
            ReplicateDocumentsToRealmState::Publish => match event {
                Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) => {
                    match result {
                        Ok(()) => match self.emit_placement_update() {
                            Ok(effects) => effects,
                            Err(error) => self.fail(error),
                        },
                        Err(error) => {
                            self.fail(ReplicateDocumentsToRealmError::DocumentSync(error))
                        }
                    }
                }
                other => self.unexpected_event("document sync result", format!("{other:?}")),
            },
            ReplicateDocumentsToRealmState::StorePlacement => match event {
                Event::Storage(StorageEvent::WriteResult { .. })
                | Event::Storage(StorageEvent::DeleteResult { .. }) => self.emit_next_publish(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("placement storage result", format!("{other:?}")),
            },
            ReplicateDocumentsToRealmState::Init
            | ReplicateDocumentsToRealmState::Finish
            | ReplicateDocumentsToRealmState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReplicateDocumentsToRealmState::Finish | ReplicateDocumentsToRealmState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
