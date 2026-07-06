use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncTarget, PendingDocumentPlacement};
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
use crate::placement::{placement_ref_for_target, plan_target_placement};
use crate::sync_placement::{
    delete_placement_effect, new_placement, placement_satisfied, schedule_placement_retry_effect,
    write_placement_effect,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ReplicateDocumentsConfig {
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
    pub excluded_peers: Vec<NodeId>,
    pub documents: Vec<DocumentSyncTarget>,
    /// Whether announces this run may mint a missing topic genesis. True for the
    /// document's origin; for the shared node-usage topic only the realm-bootstrap
    /// node passes true, so joining nodes ride the TopicNotReady retry instead of
    /// forking the genesis.
    pub allow_genesis: bool,
}

#[derive(Debug, PartialEq)]
pub struct ReplicateDocumentsOperation {
    config: ReplicateDocumentsConfig,
    state: ReplicateDocumentsState,
    pending_documents: Vec<DocumentSyncTarget>,
    realm_config: Option<RealmConfigDocument>,
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
    Write(PendingDocumentPlacement),
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
            // The node authoring the change originates every document it replicates
            // here, so it may mint any missing topic genesis.
            allow_genesis: true,
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
            realm_config: None,
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

        // Admin documents replicate as operations over their shared topic; they never
        // take placements, so skip them without a placement write or publish attempt.
        if document.is_admin_document() {
            return self.emit_next_publish();
        }

        let Some(realm_config) = self.realm_config.as_ref() else {
            return self.emit_next_publish();
        };
        // Placement plan for the document's bound strategy. `None` means the
        // realm has no strategy for this target (skip, like the old
        // desired_peer_count == 0 case).
        let Some(plan) = plan_target_placement(realm_config, &[], &document, None) else {
            return self.emit_next_publish();
        };
        let desired_count = plan.desired_count;
        let placement = plan.placement;

        // The origin is the authoritative holder; replicate to the remaining
        // top-ranked holders (excluding self and any explicit exclusions).
        let local_node_id = self.config.local_node_id;
        let excluded_peers = &self.config.excluded_peers;
        let selected_peers: Vec<NodeId> = plan
            .holders
            .into_iter()
            .filter(|node_id| *node_id != local_node_id && !excluded_peers.contains(node_id))
            .take(desired_count.saturating_sub(1))
            .collect();

        self.placement_action = if placement_satisfied(selected_peers.len(), desired_count) {
            Some(PlacementAction::Delete(document.clone()))
        } else {
            Some(PlacementAction::Write(new_placement(
                self.config.realm_id,
                document.clone(),
                local_node_id,
                desired_count,
                selected_peers.clone(),
                placement,
            )))
        };

        if selected_peers.is_empty()
            && !matches!(
                document,
                DocumentSyncTarget::NodeUsage { .. } | DocumentSyncTarget::WatchInterest { .. }
            )
        {
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
                self.config.allow_genesis,
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
        let (desired_count, placement) = match self.realm_config.as_ref() {
            Some(realm_config) => match plan_target_placement(realm_config, &[], &target, None) {
                Some(plan) => (plan.desired_count.max(1), plan.placement),
                None => (1, placement_ref_for_target(realm_config, &target, None)),
            },
            None => (1, aruna_core::structs::PlacementRef::NIL),
        };
        self.placement_action = Some(PlacementAction::Write(new_placement(
            self.config.realm_id,
            target,
            self.config.local_node_id,
            desired_count,
            Vec::new(),
            placement,
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
                        self.realm_config = None;
                        return self.emit_next_publish();
                    };
                    let config = match RealmConfigDocument::from_bytes(&value) {
                        Ok(config) => config,
                        Err(error) => return self.fail(error.into()),
                    };
                    self.realm_config = Some(config);
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
    use aruna_core::structs::{PlacementStrategy, RealmNodeKind};
    use aruna_core::task::TaskEvent;
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn group_target(seed: u8) -> DocumentSyncTarget {
        DocumentSyncTarget::Group {
            group_id: Ulid::from_bytes([seed; 16]),
        }
    }

    fn metadata_target(seed: u8) -> DocumentSyncTarget {
        DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: Ulid::from_bytes([seed; 16]),
        }
    }

    fn node_usage_target(realm_id: RealmId, node_id: NodeId) -> DocumentSyncTarget {
        DocumentSyncTarget::NodeUsage {
            realm_id,
            node_id,
            group_id: None,
        }
    }

    fn watch_interest_target(realm_id: RealmId, node_id: NodeId) -> DocumentSyncTarget {
        DocumentSyncTarget::WatchInterest { realm_id, node_id }
    }

    fn config_with(nodes: &[NodeId], replica: Option<u32>) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([7u8; 32]), Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([9u8; 16]),
            name: "default".to_string(),
            replica_count: replica,
            distinct_locations: false,
            affinity: Vec::new(),
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy];
        for node_id in nodes {
            config.ensure_node(*node_id, RealmNodeKind::Server);
        }
        config
    }

    #[test]
    fn task_schedule_error_is_non_blocking_after_placement_write() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: node(1),
            excluded_peers: Vec::new(),
            documents: Vec::new(),
            allow_genesis: true,
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

    #[test]
    fn two_remote_peers_satisfy_default_document_placement() {
        let target = metadata_target(4);
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id: RealmId::from_bytes([7u8; 32]),
            local_node_id: node(1),
            excluded_peers: Vec::new(),
            documents: vec![target.clone()],
            allow_genesis: true,
        });
        operation.realm_config = Some(config_with(&[node(1), node(2), node(3)], Some(3)));

        let effects = operation.emit_next_publish();

        assert!(
            matches!(operation.placement_action, Some(PlacementAction::Delete(ref delete_target)) if *delete_target == target)
        );
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
    }

    #[test]
    fn pending_placement_records_authoritative_origin() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let target = metadata_target(5);
        let local_node_id = node(1);
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id,
            excluded_peers: Vec::new(),
            documents: vec![target],
            allow_genesis: true,
        });
        // Replica target of three but only two eligible nodes ⇒ stays pending.
        operation.realm_config = Some(config_with(&[local_node_id, node(2)], Some(3)));

        let effects = operation.emit_next_publish();

        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let Some(PlacementAction::Write(record)) = operation.placement_action else {
            panic!("expected pending placement write");
        };
        assert_eq!(record.authoritative_node_id, local_node_id);
        assert_eq!(record.selected_peers, vec![node(2)]);
    }

    #[test]
    fn shared_node_usage_publishes_without_remote_peers() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let local_node_id = node(1);
        let target = node_usage_target(realm_id, local_node_id);
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id,
            excluded_peers: Vec::new(),
            documents: vec![target.clone()],
            allow_genesis: true,
        });
        operation.realm_nodes = vec![local_node_id];

        let effects = operation.emit_next_publish();

        assert!(
            matches!(operation.placement_action, Some(PlacementAction::Write(ref record)) if record.target == target)
        );
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
    }

    #[test]
    fn shared_watch_interest_publishes_without_remote_peers() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let local_node_id = node(1);
        let target = watch_interest_target(realm_id, local_node_id);
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id,
            excluded_peers: Vec::new(),
            documents: vec![target.clone()],
            allow_genesis: true,
        });
        operation.realm_nodes = vec![local_node_id];

        let effects = operation.emit_next_publish();

        assert!(
            matches!(operation.placement_action, Some(PlacementAction::Write(ref record)) if record.target == target)
        );
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
    }

    #[test]
    fn publish_failure_keeps_authoritative_origin() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let target = metadata_target(6);
        let local_node_id = node(1);
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id,
            excluded_peers: Vec::new(),
            documents: vec![target.clone()],
            allow_genesis: true,
        });
        operation.realm_config = Some(config_with(&[local_node_id, node(2)], Some(3)));
        operation.state = ReplicateDocumentsState::Publish;
        operation.placement_action = Some(PlacementAction::Delete(target));

        let effects = operation.step(Event::SubOperation(SubOperationEvent::DocumentSyncResult {
            result: Err("publish failed".to_string()),
        }));

        let [Effect::Storage(aruna_core::effects::StorageEffect::Write { value, .. })] =
            effects.as_slice()
        else {
            panic!("expected placement write");
        };
        let record =
            crate::sync_placement::decode_placement(value.as_ref()).expect("placement decodes");
        assert_eq!(record.authoritative_node_id, local_node_id);
        assert!(record.selected_peers.is_empty());
    }

    #[test]
    fn replicate_documents_selection_uses_rendezvous_not_local_salt() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let target = metadata_target(7);

        let mut first = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: node(1),
            excluded_peers: Vec::new(),
            documents: vec![target.clone()],
            allow_genesis: true,
        });
        first.realm_config = Some(config_with(&[node(1), node(2)], Some(3)));
        let _ = first.emit_next_publish();

        let mut second = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: node(9),
            excluded_peers: Vec::new(),
            documents: vec![target],
            allow_genesis: true,
        });
        second.realm_config = Some(config_with(&[node(9), node(2)], Some(3)));
        let _ = second.emit_next_publish();

        let Some(PlacementAction::Write(first_record)) = first.placement_action else {
            panic!("expected first placement");
        };
        let Some(PlacementAction::Write(second_record)) = second.placement_action else {
            panic!("expected second placement");
        };
        assert_eq!(first_record.selected_peers, second_record.selected_peers);
        assert_ne!(
            first_record.authoritative_node_id,
            second_record.authoritative_node_id
        );
    }

    #[test]
    fn admin_target_creates_no_placement() {
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id: RealmId::from_bytes([7u8; 32]),
            local_node_id: node(1),
            excluded_peers: Vec::new(),
            documents: vec![group_target(4)],
            allow_genesis: true,
        });

        let effects = operation.emit_next_publish();

        assert!(effects.is_empty());
        assert!(operation.placement_action.is_none());
        assert_eq!(operation.state, ReplicateDocumentsState::Finish);
        assert_eq!(operation.finalize(), Ok(()));
    }
}
