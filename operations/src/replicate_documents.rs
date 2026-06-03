use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncTarget, PendingTopicPlacement};
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{RealmConfigDocument, RealmId};
use aruna_core::task::TaskEvent;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::announce::AnnounceTopicOperation;
use crate::document_repository::read_effect;
use crate::sync_placement::{
    delete_placement_effect, desired_peer_count, new_placement, schedule_placement_retry_effect,
    select_sync_peers, sort_node_ids, write_placement_effect,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ReplicateDocumentsConfig {
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
    pub excluded_peers: Vec<NodeId>,
    pub documents: Vec<DocumentSyncTarget>,
}

#[derive(Debug, PartialEq)]
pub struct ReplicateDocumentsOperation {
    config: ReplicateDocumentsConfig,
    state: ReplicateDocumentsState,
    pending_documents: Vec<DocumentSyncTarget>,
    realm_nodes: Vec<NodeId>,
    placement_action: Option<PlacementAction>,
    retry_needed: bool,
    output: Option<Result<(), ReplicateDocumentsError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ReplicateDocumentsState {
    Init,
    LoadRealmConfig,
    Publish,
    StorePlacement,
    ScheduleRetry,
    Finish,
    Error,
}

#[derive(Debug, Clone, PartialEq)]
enum PlacementAction {
    Write(PendingTopicPlacement),
    Delete(DocumentSyncTarget),
}

#[derive(Debug, Error, PartialEq)]
pub enum ReplicateDocumentsError {
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

pub fn replicate_documents_effect(
    realm_id: RealmId,
    local_node_id: NodeId,
    documents: Vec<DocumentSyncTarget>,
) -> Effect {
    Effect::SubOperation(boxed_suboperation(
        ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
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

impl ReplicateDocumentsOperation {
    pub fn new(config: ReplicateDocumentsConfig) -> Self {
        Self {
            pending_documents: config.documents.clone().into_iter().rev().collect(),
            config,
            state: ReplicateDocumentsState::Init,
            realm_nodes: Vec::new(),
            placement_action: None,
            retry_needed: false,
            output: None,
        }
    }

    fn fail(&mut self, error: ReplicateDocumentsError) -> Effects {
        self.state = ReplicateDocumentsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(ReplicateDocumentsError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn finish_success(&mut self) -> Effects {
        self.state = ReplicateDocumentsState::Finish;
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
            Some(PlacementAction::Write(new_placement(
                self.config.realm_id,
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

        self.state = ReplicateDocumentsState::Publish;
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

    fn emit_placement_update(&mut self) -> Result<Effects, ReplicateDocumentsError> {
        let Some(action) = self.placement_action.take() else {
            return Ok(self.emit_next_publish());
        };
        self.state = ReplicateDocumentsState::StorePlacement;
        match action {
            PlacementAction::Write(record) => {
                self.retry_needed = true;
                Ok(smallvec![write_placement_effect(&record).map_err(
                    |error| ReplicateDocumentsError::Placement(error.to_string())
                )?])
            }
            PlacementAction::Delete(target) => {
                self.retry_needed = false;
                Ok(smallvec![delete_placement_effect(
                    self.config.realm_id,
                    &target
                )])
            }
        }
    }

    fn emit_failed_publish_retry(&mut self, error: String) -> Effects {
        let Some(action) = self.placement_action.take() else {
            return self.fail(ReplicateDocumentsError::DocumentSync(error));
        };
        let target = match action {
            PlacementAction::Write(record) => record.target,
            PlacementAction::Delete(target) => target,
        };
        warn!(target = ?target, error = %error, "Document sync failed; queued placement retry");
        let desired_count = desired_peer_count(&target);
        self.placement_action = Some(PlacementAction::Write(new_placement(
            self.config.realm_id,
            target,
            desired_count,
            Vec::new(),
        )));
        match self.emit_placement_update() {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }
}

impl Operation for ReplicateDocumentsOperation {
    type Output = ();
    type Error = ReplicateDocumentsError;

    fn start(&mut self) -> Effects {
        self.state = ReplicateDocumentsState::LoadRealmConfig;
        smallvec![read_effect(
            &DocumentSyncTarget::RealmConfig {
                realm_id: self.config.realm_id,
            },
            None,
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReplicateDocumentsState::LoadRealmConfig => match event {
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
            ReplicateDocumentsState::Publish => match event {
                Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) => {
                    match result {
                        Ok(()) => match self.emit_placement_update() {
                            Ok(effects) => effects,
                            Err(error) => self.fail(error),
                        },
                        Err(error) => self.emit_failed_publish_retry(error),
                    }
                }
                other => self.unexpected_event("document sync result", format!("{other:?}")),
            },
            ReplicateDocumentsState::StorePlacement => match event {
                Event::Storage(StorageEvent::WriteResult { .. })
                | Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    if self.retry_needed {
                        self.state = ReplicateDocumentsState::ScheduleRetry;
                        smallvec![schedule_placement_retry_effect(
                            self.config.realm_id,
                            self.config.local_node_id,
                        )]
                    } else {
                        self.emit_next_publish()
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("placement storage result", format!("{other:?}")),
            },
            ReplicateDocumentsState::ScheduleRetry => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.retry_needed = false;
                    self.emit_next_publish()
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(message = %message, "Failed to schedule placement retry; pending placement remains durable");
                    self.retry_needed = false;
                    self.emit_next_publish()
                }
                other => self.unexpected_event("task timer schedule result", format!("{other:?}")),
            },
            ReplicateDocumentsState::Init
            | ReplicateDocumentsState::Finish
            | ReplicateDocumentsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReplicateDocumentsState::Finish | ReplicateDocumentsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Ok(()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::task::TaskEvent;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn task_schedule_error_is_non_blocking_after_placement_write() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: node(1),
            excluded_peers: Vec::new(),
            documents: Vec::new(),
        });
        operation.state = ReplicateDocumentsState::ScheduleRetry;
        operation.retry_needed = true;

        let effects = operation.step(Event::Task(TaskEvent::Error {
            key: None,
            message: "task handle unavailable".to_string(),
        }));

        assert!(effects.is_empty());
        assert_eq!(operation.state, ReplicateDocumentsState::Finish);
        assert_eq!(operation.finalize(), Ok(()));
    }
}
