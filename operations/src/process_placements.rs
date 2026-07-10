use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncTarget, PendingDocumentPlacement};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::SYNC_PLACEMENT_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{PlacementRef, RealmConfigDocument, RealmId};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

use crate::announce::AnnounceTopicOperation;
use crate::document_repository::read_effect;
use crate::placement::{PlacementResolutionContext, rank_eligible_holders};
use crate::sync_placement::{
    decode_placement, delete_placement_effect, missing_holder_count, new_placement,
    placement_prefix, placement_satisfied, schedule_placement_retry_after, sort_node_ids,
    write_placement_effect,
};
use std::time::Duration;
use tracing::warn;

const PENDING_PLACEMENT_PAGE_SIZE: usize = 256;

#[derive(Debug, Clone, PartialEq)]
pub struct PlacementConfig {
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
    /// In-memory backoff duration the driver uses for the retry re-arm; not
    /// persisted anywhere.
    pub retry_after: Duration,
}

#[derive(Debug, PartialEq)]
pub struct ProcessPlacementsOperation {
    config: PlacementConfig,
    state: PlacementState,
    realm_config: Option<RealmConfigDocument>,
    records: Vec<PendingDocumentPlacement>,
    next_start_after: Option<Key>,
    current: Option<CurrentPlacement>,
    retry_needed: bool,
    rearmed: bool,
    output: Option<Result<(), PlacementError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum PlacementState {
    Init,
    LoadRealmConfig,
    ListPending,
    Publish,
    StorePlacement,
    ScheduleRetry,
    Finish,
    Error,
}

#[derive(Debug, Clone, PartialEq)]
struct CurrentPlacement {
    target: DocumentSyncTarget,
    origin_node_id: NodeId,
    desired_holder_count: usize,
    selected_holders: Vec<NodeId>,
    newly_selected: Vec<NodeId>,
    placement: PlacementRef,
}

#[derive(Debug, Error, PartialEq)]
pub enum PlacementError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("pending placement decode failed: {0}")]
    Decode(String),
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

impl ProcessPlacementsOperation {
    pub fn new(config: PlacementConfig) -> Self {
        Self {
            config,
            state: PlacementState::Init,
            realm_config: None,
            records: Vec::new(),
            next_start_after: None,
            current: None,
            retry_needed: false,
            rearmed: false,
            output: None,
        }
    }

    fn fail(&mut self, error: PlacementError) -> Effects {
        self.state = PlacementState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(PlacementError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn emit_list_pending(&mut self) -> Effects {
        self.state = PlacementState::ListPending;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: SYNC_PLACEMENT_KEYSPACE.to_string(),
            prefix: Some(placement_prefix(self.config.realm_id)),
            start: self.next_start_after.take().map(IterStart::After),
            limit: PENDING_PLACEMENT_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn emit_next_record(&mut self) -> Effects {
        let Some(record) = self.records.pop() else {
            if self.next_start_after.is_some() {
                return self.emit_list_pending();
            }
            self.state = PlacementState::Finish;
            self.output = Some(Ok(()));
            return smallvec![];
        };
        if record.realm_id != self.config.realm_id {
            warn!(
                record_realm_id = %record.realm_id,
                config_realm_id = %self.config.realm_id,
                "Skipping pending placement for a different realm"
            );
            return self.emit_next_record();
        }
        if record.target.is_admin_document() {
            // Admin documents never take placements; drain any stale row instead of
            // retrying it forever. The satisfied-delete path continues the sweep once
            // the delete result arrives.
            self.current = None;
            self.retry_needed = false;
            self.state = PlacementState::StorePlacement;
            return smallvec![delete_placement_effect(
                self.config.realm_id,
                &record.target
            )];
        }

        let missing_holder_count = missing_holder_count(&record);
        let mut selected_holders = record.selected_holders;
        sort_node_ids(&mut selected_holders);
        let newly_selected: Vec<NodeId> = match self.realm_config.as_ref() {
            Some(config) => rank_eligible_holders(
                config,
                &record.target,
                PlacementResolutionContext::default(),
            )
            .into_iter()
            .filter(|node_id| !selected_holders.contains(node_id))
            .take(missing_holder_count)
            .collect(),
            None => Vec::new(),
        };
        let network_peers: Vec<NodeId> = newly_selected
            .iter()
            .copied()
            .filter(|node_id| *node_id != self.config.local_node_id)
            .collect();
        self.current = Some(CurrentPlacement {
            target: record.target.clone(),
            origin_node_id: record.origin_node_id,
            desired_holder_count: record.desired_holder_count,
            selected_holders,
            newly_selected: newly_selected.clone(),
            placement: record.placement,
        });

        if network_peers.is_empty() {
            return self.emit_placement_update();
        }

        self.state = PlacementState::Publish;
        // NodeInfo genesis is reserved for the explicit core-document bootstrap;
        // ordinary documents retain origin-derived genesis on placement retry.
        let allow_genesis = !matches!(&record.target, DocumentSyncTarget::NodeInfo { .. })
            && record.origin_node_id == self.config.local_node_id;
        smallvec![Effect::SubOperation(boxed_suboperation(
            AnnounceTopicOperation::new_for_document_with_peers_and_placement(
                record.target.topic_id(),
                self.config.local_node_id,
                Some(record.target),
                network_peers,
                record.placement,
                allow_genesis,
            ),
            |result| Event::SubOperation(SubOperationEvent::DocumentSyncResult {
                result: result.map_err(|error| error.to_string()),
            }),
        ))]
    }

    fn emit_placement_update(&mut self) -> Effects {
        let Some(mut current) = self.current.take() else {
            return self.emit_next_record();
        };
        current.selected_holders.append(&mut current.newly_selected);
        sort_node_ids(&mut current.selected_holders);

        self.state = PlacementState::StorePlacement;
        if placement_satisfied(current.selected_holders.len(), current.desired_holder_count) {
            self.retry_needed = false;
            return smallvec![delete_placement_effect(
                self.config.realm_id,
                &current.target
            )];
        }

        let record = new_placement(
            self.config.realm_id,
            current.target,
            current.origin_node_id,
            current.desired_holder_count,
            current.selected_holders,
            current.placement,
        );
        self.retry_needed = true;
        match write_placement_effect(&record) {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(PlacementError::Placement(error.to_string())),
        }
    }
}

impl Operation for ProcessPlacementsOperation {
    /// `true` when this tick re-armed the placement retry timer.
    type Output = bool;
    type Error = PlacementError;

    fn start(&mut self) -> Effects {
        self.state = PlacementState::LoadRealmConfig;
        smallvec![read_effect(
            &DocumentSyncTarget::RealmConfig {
                realm_id: self.config.realm_id,
            },
            None,
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PlacementState::LoadRealmConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(value) = value else {
                        return self.fail(PlacementError::RealmConfigNotFound);
                    };
                    let config = match RealmConfigDocument::from_bytes(&value) {
                        Ok(config) => config,
                        Err(error) => return self.fail(error.into()),
                    };
                    self.realm_config = Some(config);
                    self.emit_list_pending()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("realm config read result", format!("{other:?}")),
            },
            PlacementState::ListPending => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    self.next_start_after = next_start_after;
                    self.records.clear();
                    for (_, value) in values.into_iter().rev() {
                        let record = match decode_placement(&value) {
                            Ok(record) => record,
                            Err(error) => {
                                return self.fail(PlacementError::Decode(error.to_string()));
                            }
                        };
                        self.records.push(record);
                    }
                    self.emit_next_record()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("pending placement iter result", format!("{other:?}"))
                }
            },
            PlacementState::Publish => match event {
                Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) => {
                    match result {
                        Ok(()) => self.emit_placement_update(),
                        Err(error) => {
                            warn!(error = %error, "Document sync failed; keeping placement pending");
                            if let Some(current) = self.current.as_mut() {
                                current
                                    .newly_selected
                                    .retain(|node_id| *node_id == self.config.local_node_id);
                            }
                            self.emit_placement_update()
                        }
                    }
                }
                other => self.unexpected_event("document sync result", format!("{other:?}")),
            },
            PlacementState::StorePlacement => match event {
                Event::Storage(StorageEvent::WriteResult { .. })
                | Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    if self.retry_needed {
                        self.rearmed = true;
                        self.state = PlacementState::ScheduleRetry;
                        smallvec![schedule_placement_retry_after(
                            self.config.realm_id,
                            self.config.local_node_id,
                            self.config.retry_after,
                        )]
                    } else {
                        self.emit_next_record()
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("placement storage result", format!("{other:?}")),
            },
            PlacementState::ScheduleRetry => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.retry_needed = false;
                    self.emit_next_record()
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(message = %message, "Failed to schedule placement retry; pending placement remains durable");
                    self.retry_needed = false;
                    self.emit_next_record()
                }
                other => self.unexpected_event("task timer schedule result", format!("{other:?}")),
            },
            PlacementState::Init | PlacementState::Finish | PlacementState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PlacementState::Finish | PlacementState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Err(error)) => Err(error),
            _ => Ok(self.rearmed),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_placement::SYNC_PLACEMENT_RETRY_AFTER;
    use aruna_core::structs::{PlacementStrategy, RealmNodeKind};
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

    fn config_with(nodes: &[NodeId]) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([8u8; 32]), Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([9u8; 16]),
            name: "default".to_string(),
            replica_count: None,
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
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: node(1),
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.state = PlacementState::ScheduleRetry;
        operation.retry_needed = true;

        let effects = operation.step(Event::Task(TaskEvent::Error {
            key: None,
            message: "task handle unavailable".to_string(),
        }));

        assert!(effects.is_empty());
        assert_eq!(operation.state, PlacementState::Finish);
        assert_eq!(operation.finalize(), Ok(false));
    }

    #[test]
    fn three_selected_holders_complete_existing_pending_placement() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let target = metadata_target(4);
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: node(1),
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.current = Some(CurrentPlacement {
            target: target.clone(),
            origin_node_id: node(1),
            desired_holder_count: 3,
            selected_holders: vec![node(1), node(2), node(3)],
            newly_selected: Vec::new(),
            placement: PlacementRef::NIL,
        });

        let effects = operation.emit_placement_update();

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Delete { key_space, .. })]
                if key_space == SYNC_PLACEMENT_KEYSPACE
        ));
        assert!(!operation.retry_needed);
    }

    #[test]
    fn process_placement_may_select_origin_as_holder() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let origin = node(1);
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: node(9),
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.realm_config = Some(config_with(&[origin, node(2)]));
        operation.records = vec![new_placement(
            realm_id,
            metadata_target(5),
            origin,
            3,
            vec![node(2)],
            PlacementRef::NIL,
        )];

        let effects = operation.emit_next_record();

        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let current = operation.current.expect("placement is active");
        assert_eq!(current.origin_node_id, origin);
        assert_eq!(current.selected_holders, vec![node(2)]);
        assert!(current.newly_selected.contains(&origin));
        assert!(!current.newly_selected.contains(&node(2)));
    }

    #[test]
    fn process_placement_does_not_replace_existing_holders() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let target = metadata_target(6);
        let origin = node(1);
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: node(9),
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.current = Some(CurrentPlacement {
            target,
            origin_node_id: origin,
            desired_holder_count: 5,
            selected_holders: vec![node(2)],
            newly_selected: vec![node(3)],
            placement: PlacementRef::NIL,
        });

        let effects = operation.emit_placement_update();

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected placement write");
        };
        let record = decode_placement(value.as_ref()).expect("placement decodes");
        assert_eq!(record.origin_node_id, origin);
        assert_eq!(record.selected_holders, vec![node(2), node(3)]);
    }

    #[test]
    fn process_placement_does_not_implicitly_count_origin() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let origin = node(1);
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: node(9),
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.realm_config = Some(config_with(&[origin, node(2)]));
        operation.records = vec![new_placement(
            realm_id,
            metadata_target(7),
            origin,
            2,
            Vec::new(),
            PlacementRef::NIL,
        )];

        let effects = operation.emit_next_record();

        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let current = operation.current.expect("placement is active");
        assert_eq!(current.newly_selected.len(), 2);
        assert!(current.newly_selected.contains(&origin));
        assert!(current.newly_selected.contains(&node(2)));
    }

    #[test]
    fn admin_target_placement_is_drained_not_retried() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: node(1),
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.records = vec![new_placement(
            realm_id,
            group_target(4),
            node(1),
            3,
            Vec::new(),
            PlacementRef::NIL,
        )];

        let effects = operation.emit_next_record();

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Delete { key_space, .. })]
                if key_space == SYNC_PLACEMENT_KEYSPACE
        ));
        assert!(!operation.retry_needed);
        assert_eq!(operation.state, PlacementState::StorePlacement);
        assert!(operation.current.is_none());
    }

    fn announce_outbox_record(
        effects: Effects,
        document_bytes: Vec<u8>,
    ) -> aruna_core::document::DocumentSyncOutboxRecord {
        let Some(Effect::SubOperation(mut sub)) = effects.into_iter().next() else {
            panic!("expected announce sub-operation");
        };
        let _read = sub.start();
        let write_effects = sub.step(Event::Storage(StorageEvent::ReadResult {
            key: Key::from(vec![0u8]),
            value: Some(aruna_core::types::Value::from(document_bytes)),
        }));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = write_effects.as_slice() else {
            panic!("expected announce outbox write, got {write_effects:?}");
        };
        let record: aruna_core::document::DocumentSyncOutboxRecord =
            postcard::from_bytes(value.as_ref()).expect("outbox record decodes");
        record
    }

    fn announce_outbox_allow_genesis(effects: Effects, document_bytes: Vec<u8>) -> bool {
        announce_outbox_record(effects, document_bytes).allow_genesis
    }

    fn graph_lifecycle_fixture() -> (DocumentSyncTarget, Vec<u8>) {
        let record = aruna_core::metadata::MetadataGraphLifecycleRecord::deleted(
            "urn:graph:placement-test".to_string(),
            RealmId::from_bytes([8u8; 32]),
            aruna_core::types::GroupId::new(),
            Ulid::from_bytes([9u8; 16]),
            42,
        );
        let bytes = postcard::to_allocvec(&record).expect("lifecycle record serializes");
        let target = DocumentSyncTarget::MetadataGraphLifecycle {
            graph_iri: record.graph_iri.clone(),
        };
        (target, bytes)
    }

    #[test]
    fn process_local_holder_is_counted_but_not_a_network_peer() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let local = node(1);
        let remote = node(2);
        let (target, bytes) = graph_lifecycle_fixture();
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: local,
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.realm_config = Some(config_with(&[local, remote]));
        let placement = PlacementRef {
            strategy_id: Ulid::from_bytes([7; 16]),
            epoch: 4,
        };
        operation.records = vec![new_placement(
            realm_id,
            target,
            node(9),
            2,
            Vec::new(),
            placement,
        )];

        let effects = operation.emit_next_record();

        let current = operation.current.as_ref().expect("placement is active");
        assert!(current.newly_selected.contains(&local));
        assert!(current.newly_selected.contains(&remote));
        let outbox = announce_outbox_record(effects, bytes);
        assert_eq!(outbox.peers, vec![remote]);
        let aruna_core::document::DocumentSyncOutboxEvent::Upsert { change, .. } = outbox.event
        else {
            panic!("expected upsert outbox event");
        };
        assert_eq!(change.placement, placement);
    }

    fn placement_announce_allow_genesis(
        local: NodeId,
        origin: NodeId,
        target: DocumentSyncTarget,
        document_bytes: Vec<u8>,
    ) -> bool {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let mut operation = ProcessPlacementsOperation::new(PlacementConfig {
            realm_id,
            local_node_id: local,
            retry_after: SYNC_PLACEMENT_RETRY_AFTER,
        });
        operation.realm_config = Some(config_with(&[node(1), node(2), node(3), local]));
        operation.records = vec![new_placement(
            realm_id,
            target,
            origin,
            3,
            Vec::new(),
            PlacementRef::NIL,
        )];
        announce_outbox_allow_genesis(operation.emit_next_record(), document_bytes)
    }

    #[test]
    fn ordinary_document_retry_allow_genesis_tracks_origin() {
        let local = node(1);
        let (target, bytes) = graph_lifecycle_fixture();
        assert!(
            placement_announce_allow_genesis(local, local, target.clone(), bytes.clone()),
            "local origin may mint genesis"
        );
        assert!(
            !placement_announce_allow_genesis(node(9), local, target, bytes),
            "non-origin publisher must not mint genesis"
        );
    }

    #[test]
    fn node_info_origin_retry_disallows_genesis() {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let local = node(1);
        let target = DocumentSyncTarget::NodeInfo {
            realm_id,
            node_id: local,
        };

        assert!(
            !placement_announce_allow_genesis(local, local, target, b"node info".to_vec()),
            "NodeInfo retries must not mint shared-topic genesis"
        );
    }
}
