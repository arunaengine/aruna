//! Publishes a realm's documents to chosen peers and stores the resulting shard placement.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentTarget, PendingShardPlacement};
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::keyspaces::SYNC_PLACEMENT_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::placement::{document_class, plan_target_placement};
use aruna_core::rows::{done, encoded_row, read_row, values};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::{DocumentClass, PlacementRef};
use aruna_core::task::TaskEvent;
use thiserror::Error;
use tracing::warn;

use crate::sync::announce::AnnounceTopicOperation;
use crate::sync::shard_placement::{
    new_placement, placement_key, placement_satisfied, schedule_retry_effect,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ReplicateDocumentsConfig {
    pub realm_id: RealmId,
    pub local_node_id: NodeId,
    pub excluded_peers: Vec<NodeId>,
    pub documents: Vec<DocumentTarget>,
    /// Whether announces this run may mint a missing topic genesis. True for a
    /// document's origin; for shared node-usage only the realm-bootstrap node, so
    /// joiners ride the TopicNotReady retry instead of forking.
    pub allow_genesis: bool,
}

#[derive(Debug, PartialEq)]
pub struct ReplicateDocumentsOperation {
    config: ReplicateDocumentsConfig,
    phase: Phase,
    pending_documents: Vec<DocumentTarget>,
    realm_config: Option<RealmConfigDocument>,
    placement_action: Option<PlacementAction>,
    retry_needed: bool,
    /// Fail when a document was not handed to the outbox instead of deferring it.
    strict: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Phase {
    LoadRealmConfig,
    Publish,
    StorePlacement,
    ScheduleRetry,
}

#[derive(Debug, Clone, PartialEq)]
enum PlacementAction {
    Write(PendingShardPlacement),
    Delete(PlacementRef),
}

#[derive(Debug, Error, PartialEq)]
pub enum ReplicateDocumentsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("realm config document not found")]
    ConfigMissing,
    #[error("document sync failed: {0}")]
    DocumentSync(String),
    #[error("placement persistence failed: {0}")]
    Placement(String),
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for ReplicateDocumentsError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            other => Self::Io(other),
        }
    }
}

pub fn replicate_documents_effect(
    realm_id: RealmId,
    local_node_id: NodeId,
    documents: Vec<DocumentTarget>,
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
            phase: Phase::LoadRealmConfig,
            realm_config: None,
            placement_action: None,
            retry_needed: false,
            strict: false,
        }
    }

    /// A failed or deferred announce fails the operation, so the caller keeps its own retry: a
    /// placement retry repairs topic membership but never queues the document again.
    pub fn strict(mut self) -> Self {
        self.strict = true;
        self
    }

    fn emit_next_publish(&mut self) -> Result<Next<()>, ReplicateDocumentsError> {
        while let Some(document) = self.pending_documents.pop() {
            if let Some(next) = self.publish(document)? {
                return Ok(next);
            }
        }
        Ok(Next::Done(()))
    }

    /// The call that publishes or records the placement of `document`; none skips it.
    fn publish(
        &mut self,
        document: DocumentTarget,
    ) -> Result<Option<Next<()>>, ReplicateDocumentsError> {
        // Admin documents replicate as operations over their shared topic; they never
        // take placements, so skip them without a placement write or publish attempt.
        if document.is_admin_document() {
            return Ok(None);
        }

        // Metadata uses its recorded bucket and its own outbox, never a rederived target bucket.
        if matches!(
            document_class(&document),
            DocumentClass::Metadata | DocumentClass::MetadataRegistry
        ) {
            debug_assert!(
                false,
                "metadata target {document:?} must publish from its stored placement"
            );
            return Ok(None);
        }

        let Some(realm_config) = self.realm_config.as_ref() else {
            return Ok(None);
        };
        // No bound strategy means this target has no placement work.
        let plan = match plan_target_placement(realm_config, &document, Default::default()) {
            Ok(Some(plan)) => plan,
            Ok(None) => return Ok(None),
            Err(error) if self.strict => {
                return Err(ReplicateDocumentsError::DocumentSync(error.to_string()));
            }
            Err(_) => {
                // Unresolvable bucket: keep a durable pending record so the
                // reconciler retries, and announce nothing to nonholders.
                let placement = aruna_core::placement::target_placement_ref(
                    realm_config,
                    &document,
                    Default::default(),
                );
                self.placement_action = Some(PlacementAction::Write(new_placement(
                    self.config.realm_id,
                    placement,
                    self.config.local_node_id,
                    Vec::new(),
                )));
                return self.emit_placement_update().map(Some);
            }
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
            Some(PlacementAction::Delete(placement))
        } else {
            Some(PlacementAction::Write(new_placement(
                self.config.realm_id,
                placement,
                local_node_id,
                selected_peers.clone(),
            )))
        };

        if selected_peers.is_empty()
            && !matches!(
                document,
                DocumentTarget::NodeUsage { .. }
                    | DocumentTarget::WatchInterest { .. }
                    | DocumentTarget::NodeInfo { .. }
            )
        {
            return self.emit_placement_update().map(Some);
        }

        self.phase = Phase::Publish;
        let announce = AnnounceTopicOperation::new(
            document,
            self.config.local_node_id,
            selected_peers,
            placement,
            self.config.allow_genesis,
        );
        Ok(Some(Next::Call(IoCall::Child(boxed_suboperation(
            announce,
            |result| {
                Event::SubOperation(SubOperationEvent::DocumentSyncResult {
                    result: result.map_err(|error| error.to_string()),
                })
            },
        )))))
    }

    fn emit_placement_update(&mut self) -> Result<Next<()>, ReplicateDocumentsError> {
        let Some(action) = self.placement_action.take() else {
            return self.emit_next_publish();
        };
        self.phase = Phase::StorePlacement;
        let call = match action {
            PlacementAction::Write(record) => {
                self.retry_needed = true;
                let key = placement_key(record.realm_id, &record.placement);
                let row = encoded_row(SYNC_PLACEMENT_KEYSPACE, key, &record)
                    .map_err(|error| ReplicateDocumentsError::Placement(error.to_string()))?;
                IoCall::Write(vec![row])
            }
            PlacementAction::Delete(placement) => {
                self.retry_needed = false;
                let key = placement_key(self.config.realm_id, &placement);
                IoCall::Delete(vec![(SYNC_PLACEMENT_KEYSPACE.to_string(), key)])
            }
        };
        Ok(Next::Call(call))
    }

    fn retry_failed_publish(&mut self, error: String) -> Result<Next<()>, ReplicateDocumentsError> {
        let Some(action) = self.placement_action.take() else {
            return Err(ReplicateDocumentsError::DocumentSync(error));
        };
        let placement = match action {
            PlacementAction::Write(record) => record.placement,
            PlacementAction::Delete(placement) => placement,
        };
        warn!(placement = ?placement, error = %error, "Document sync failed; queued shard placement retry");
        // Re-queue the shard with no selected co-holders so the placement
        // reconciler re-resolves and re-ensures topic membership.
        self.placement_action = Some(PlacementAction::Write(new_placement(
            self.config.realm_id,
            placement,
            self.config.local_node_id,
            Vec::new(),
        )));
        self.emit_placement_update()
    }

    fn publish_result(&mut self, event: Event) -> Result<Next<()>, ReplicateDocumentsError> {
        let Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) = event else {
            return Err(unexpected(
                "SubOperationEvent::DocumentSyncResult",
                event.label(),
            ));
        };
        match result {
            Ok(()) => self.emit_placement_update(),
            Err(error) if self.strict => Err(ReplicateDocumentsError::DocumentSync(error)),
            Err(error) => self.retry_failed_publish(error),
        }
    }

    fn retry_scheduled(&mut self, event: Event) -> Result<Next<()>, ReplicateDocumentsError> {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. }) => {}
            Event::Task(TaskEvent::Error { message, .. }) => {
                warn!(message = %message, "Failed to schedule placement retry; pending placement remains durable");
            }
            other => return Err(unexpected("TaskEvent::TimerScheduled", other.label())),
        }
        self.retry_needed = false;
        self.emit_next_publish()
    }
}

impl Operation for ReplicateDocumentsOperation {
    type Output = ();
    type Error = ReplicateDocumentsError;

    fn first(&mut self) -> Result<Next<()>, ReplicateDocumentsError> {
        let config = DocumentTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        let read = read_row(config.storage_keyspace(), config.storage_key());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ReplicateDocumentsError> {
        match self.phase {
            Phase::LoadRealmConfig => {
                let [value] = values::<1>(result)?;
                self.realm_config = value
                    .map(|value| RealmConfigDocument::from_bytes(&value))
                    .transpose()?;
                self.emit_next_publish()
            }
            Phase::Publish => self.publish_result(event(result)?),
            Phase::StorePlacement => {
                done(result)?;
                if !self.retry_needed {
                    return self.emit_next_publish();
                }
                self.phase = Phase::ScheduleRetry;
                let schedule =
                    schedule_retry_effect(self.config.realm_id, self.config.local_node_id);
                Ok(Next::Call(IoCall::Effect(Box::new(schedule))))
            }
            Phase::ScheduleRetry => self.retry_scheduled(event(result)?),
        }
    }
}

/// The event that answered a child or a passed-through effect.
fn event(result: IoResult) -> Result<Event, ReplicateDocumentsError> {
    match result {
        IoResult::Event(event) => Ok(*event),
        other => Err(unexpected("IoResult::Event", other.label())),
    }
}

fn unexpected(expected: &'static str, got: &'static str) -> ReplicateDocumentsError {
    ReplicateDocumentsError::Io(IoError::Unexpected { expected, got })
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmNodeKind;
    use aruna_core::structs::placement::record::PlacementStrategy;
    use aruna_core::task::TaskEvent;
    use ulid::Ulid;

    fn synced(result: Result<(), String>) -> IoResult {
        let event = Event::SubOperation(SubOperationEvent::DocumentSyncResult { result });
        IoResult::Event(Box::new(event))
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn group_target(seed: u8) -> DocumentTarget {
        DocumentTarget::Group {
            group_id: Ulid::from_bytes([seed; 16]),
        }
    }

    fn node_usage_target(realm_id: RealmId, node_id: NodeId) -> DocumentTarget {
        DocumentTarget::NodeUsage {
            realm_id,
            node_id,
            group_id: None,
        }
    }

    fn watch_interest_target(realm_id: RealmId, node_id: NodeId) -> DocumentTarget {
        DocumentTarget::WatchInterest { realm_id, node_id }
    }

    fn node_info_target(realm_id: RealmId, node_id: NodeId) -> DocumentTarget {
        DocumentTarget::NodeInfo { realm_id, node_id }
    }

    fn config_with(nodes: &[NodeId], replica: Option<u32>) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([7u8; 32]), Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([9u8; 16]),
            name: "default".to_string(),
            replica_count: replica,
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy];
        for node_id in nodes {
            config.ensure_node(*node_id, RealmNodeKind::Server);
        }
        config
    }

    #[test]
    fn reads_realm_config() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: node(1),
            excluded_peers: Vec::new(),
            documents: vec![node_usage_target(realm_id, node(1))],
            allow_genesis: true,
        });
        let config = DocumentTarget::RealmConfig { realm_id };
        assert_eq!(
            operation.first(),
            Ok(Next::Call(read_row(
                config.storage_keyspace(),
                config.storage_key()
            )))
        );
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(ReplicateDocumentsError::Io(IoError::Unexpected { .. }))
        ));
        // Without a realm config no document has placement work.
        let missing = IoResult::Values(vec![None]);
        assert_eq!(operation.next(missing), Ok(Next::Done(())));
    }

    #[test]
    fn schedule_error_nonblocking() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: node(1),
            excluded_peers: Vec::new(),
            documents: Vec::new(),
            allow_genesis: true,
        });
        operation.phase = Phase::ScheduleRetry;
        operation.retry_needed = true;

        let refused = Event::Task(TaskEvent::Error {
            key: None,
            message: "task handle unavailable".to_string(),
        });
        assert_eq!(
            operation.next(IoResult::Event(Box::new(refused))),
            Ok(Next::Done(()))
        );
    }

    #[test]
    fn unresolved_keeps_record() {
        // A governed target whose bucket cannot resolve writes the durable
        // pending record with no peers and announces nothing.
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let target = node_usage_target(realm_id, node(5));
        let local_node_id = node(1);
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id,
            excluded_peers: Vec::new(),
            documents: vec![target],
            allow_genesis: true,
        });
        let mut config = config_with(&[local_node_id, node(2)], Some(3));
        // A published map without an activation makes the bucket unresolvable.
        config.candidate_maps.push(config.freeze_map(1));
        operation.realm_config = Some(config);

        let next = operation.emit_next_publish();

        assert!(
            matches!(next, Ok(Next::Call(IoCall::Write(_)))),
            "expected the durable pending placement write, no announce"
        );
        assert_eq!(operation.phase, Phase::StorePlacement);
        assert!(operation.retry_needed);
    }

    #[test]
    fn peers_satisfy_placement() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let target = node_usage_target(realm_id, node(4));
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: node(1),
            excluded_peers: Vec::new(),
            documents: vec![target.clone()],
            allow_genesis: true,
        });
        operation.realm_config = Some(config_with(&[node(1), node(2), node(3)], Some(3)));

        let next = operation.emit_next_publish();

        assert!(matches!(
            operation.placement_action,
            Some(PlacementAction::Delete(_))
        ));
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
    }

    #[test]
    fn pending_records_origin() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let target = node_usage_target(realm_id, node(5));
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

        let next = operation.emit_next_publish();

        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
        let Some(PlacementAction::Write(record)) = operation.placement_action else {
            panic!("expected pending placement write");
        };
        assert_eq!(record.authoritative_node_id, local_node_id);
        assert_eq!(record.selected_peers, vec![node(2)]);
    }

    #[test]
    fn node_usage_peerless() {
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
        operation.realm_config = Some(config_with(&[local_node_id], Some(3)));

        let next = operation.emit_next_publish();

        let Some(PlacementAction::Write(record)) = operation.placement_action else {
            panic!("expected pending placement write");
        };
        assert_eq!(record.authoritative_node_id, local_node_id);
        assert!(record.selected_peers.is_empty());
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
    }

    #[test]
    fn watch_interest_peerless() {
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
        operation.realm_config = Some(config_with(&[local_node_id], Some(3)));

        let next = operation.emit_next_publish();

        let Some(PlacementAction::Write(record)) = operation.placement_action else {
            panic!("expected pending placement write");
        };
        assert_eq!(record.authoritative_node_id, local_node_id);
        assert!(record.selected_peers.is_empty());
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
    }

    // Node info rides the shared realm topic, so a single-node realm still
    // publishes it instead of parking it behind a placement retry.
    #[test]
    fn node_info_peerless() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let local_node_id = node(1);
        let target = node_info_target(realm_id, local_node_id);
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id,
            excluded_peers: Vec::new(),
            documents: vec![target.clone()],
            allow_genesis: true,
        });
        operation.realm_config = Some(config_with(&[local_node_id], Some(3)));

        let next = operation.emit_next_publish();

        let Some(PlacementAction::Write(record)) = operation.placement_action else {
            panic!("expected pending placement write");
        };
        assert_eq!(record.authoritative_node_id, local_node_id);
        assert!(record.selected_peers.is_empty());
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
    }

    #[test]
    fn failure_keeps_origin() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let target = node_usage_target(realm_id, node(6));
        let local_node_id = node(1);
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id,
            excluded_peers: Vec::new(),
            documents: vec![target.clone()],
            allow_genesis: true,
        });
        operation.realm_config = Some(config_with(&[local_node_id, node(2)], Some(3)));
        operation.phase = Phase::Publish;
        operation.placement_action = Some(PlacementAction::Delete(PlacementRef {
            strategy_id: ulid::Ulid::from_bytes([9u8; 16]),
            shard: 1,
        }));

        let next = operation.next(synced(Err("publish failed".to_string())));

        let Ok(Next::Call(IoCall::Write(rows))) = next else {
            panic!("expected placement write");
        };
        let [(_, _, value)] = rows.as_slice() else {
            panic!("expected one placement row");
        };
        let record = crate::sync::shard_placement::decode_placement(value.as_ref())
            .expect("placement decodes");
        assert_eq!(record.authoritative_node_id, local_node_id);
        assert!(record.selected_peers.is_empty());
    }

    #[test]
    fn strict_fails_publish() {
        // A strict caller keeps its own retry, so a failed announce fails instead of deferring.
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let local_node_id = node(1);
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id,
            excluded_peers: Vec::new(),
            documents: vec![node_info_target(realm_id, local_node_id)],
            allow_genesis: false,
        })
        .strict();
        operation.realm_config = Some(config_with(&[local_node_id, node(2)], Some(3)));
        operation.phase = Phase::Publish;
        operation.placement_action = Some(PlacementAction::Delete(PlacementRef {
            strategy_id: ulid::Ulid::from_bytes([9u8; 16]),
            shard: 1,
        }));

        assert!(matches!(
            operation.next(synced(Err("outbox write failed".to_string()))),
            Err(ReplicateDocumentsError::DocumentSync(_))
        ));
    }

    #[test]
    fn selection_uses_rendezvous() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let target = node_usage_target(realm_id, node(7));

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
    fn admin_skips_placement() {
        let mut operation = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id: RealmId::from_bytes([7u8; 32]),
            local_node_id: node(1),
            excluded_peers: Vec::new(),
            documents: vec![group_target(4)],
            allow_genesis: true,
        });

        assert_eq!(operation.emit_next_publish(), Ok(Next::Done(())));
        assert!(operation.placement_action.is_none());
    }
}
