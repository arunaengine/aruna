use std::collections::VecDeque;

use aruna_core::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent, DocumentSyncOutboxRecord,
    DocumentSyncRevision, DocumentSyncTarget,
};
use aruna_core::effects::{Effect, IterStart, NetEffect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::metadata::MetadataError;
use aruna_core::metadata::{
    MetadataCreateEventRecord, MetadataDocumentLifecycleRecord, MetadataGraphLifecycleRecord,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::metadata_document_lifecycle_revision_change;
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::structs::PlacementRef;
use aruna_core::structs::RealmId;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, UserId};
use aruna_core::{DocumentSyncEffect, DocumentSyncNetEvent};
use aruna_core::{NodeId, TopicId, USER_KEYSPACE};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::document_repository;
use crate::document_sync_outbox::{
    new_outbox_record, schedule_outbox_drain_effect, write_outbox_effect,
};

const USER_SYNC_PAGE_SIZE: usize = 256;

#[derive(Debug, Clone, PartialEq)]
enum PendingDocumentSync {
    Document {
        document: DocumentSyncTarget,
        bytes: Option<Vec<u8>>,
    },
    UserPage {
        realm_id: RealmId,
        start_after: Option<Key>,
    },
}

#[derive(Debug, PartialEq)]
pub struct AnnounceTopicOperation {
    topic: TopicId,
    document: Option<DocumentSyncTarget>,
    local_node_id: NodeId,
    peers: Vec<NodeId>,
    document_bytes: Option<Vec<u8>>,
    placement: PlacementRef,
    allow_genesis: bool,
    require_delivery: bool,
    transfer_outbox: Option<DocumentSyncOutboxRecord>,
    state: AnnounceTopicState,
    pending: VecDeque<PendingDocumentSync>,
    current: Option<DocumentSyncTarget>,
    output: Option<Result<(), AnnounceTopicError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum AnnounceTopicState {
    Init,
    ReadDocument,
    ListUsers,
    WriteOutbox,
    ScheduleSync,
    PublishDocument,
    SyncDocument,
    DeleteTransferredOutbox,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum AnnounceTopicError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    MetadataError(#[from] MetadataError),
    #[error("document sync failed: {0}")]
    DocumentSync(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl AnnounceTopicOperation {
    pub fn new(topic: TopicId, local_node_id: NodeId, allow_genesis: bool) -> Self {
        Self::new_for_document(topic, local_node_id, None, allow_genesis)
    }

    pub fn new_for_document(
        topic: TopicId,
        local_node_id: NodeId,
        document: Option<DocumentSyncTarget>,
        allow_genesis: bool,
    ) -> Self {
        Self::new_for_document_with_peers(topic, local_node_id, document, Vec::new(), allow_genesis)
    }

    pub fn new_for_document_with_peers(
        topic: TopicId,
        local_node_id: NodeId,
        document: Option<DocumentSyncTarget>,
        peers: Vec<NodeId>,
        allow_genesis: bool,
    ) -> Self {
        Self::new_for_document_with_peers_and_placement(
            topic,
            local_node_id,
            document,
            peers,
            PlacementRef::NIL,
            allow_genesis,
        )
    }

    pub fn new_for_document_with_peers_and_placement(
        topic: TopicId,
        local_node_id: NodeId,
        document: Option<DocumentSyncTarget>,
        peers: Vec<NodeId>,
        placement: PlacementRef,
        allow_genesis: bool,
    ) -> Self {
        Self {
            topic,
            document,
            local_node_id,
            peers,
            document_bytes: None,
            placement,
            allow_genesis,
            require_delivery: false,
            transfer_outbox: None,
            state: AnnounceTopicState::Init,
            pending: VecDeque::new(),
            current: None,
            output: None,
        }
    }

    pub fn new_for_document_with_peers_and_bytes(
        topic: TopicId,
        local_node_id: NodeId,
        document: DocumentSyncTarget,
        peers: Vec<NodeId>,
        bytes: Vec<u8>,
        allow_genesis: bool,
    ) -> Self {
        Self {
            topic,
            document: Some(document),
            local_node_id,
            peers,
            document_bytes: Some(bytes),
            placement: PlacementRef::NIL,
            allow_genesis,
            require_delivery: false,
            transfer_outbox: None,
            state: AnnounceTopicState::Init,
            pending: VecDeque::new(),
            current: None,
            output: None,
        }
    }

    pub(crate) fn new_for_document_transfer_with_peers_and_placement(
        topic: TopicId,
        local_node_id: NodeId,
        document: DocumentSyncTarget,
        peers: Vec<NodeId>,
        placement: PlacementRef,
        allow_genesis: bool,
    ) -> Self {
        let mut operation = Self::new_for_document_with_peers_and_placement(
            topic,
            local_node_id,
            Some(document),
            peers,
            placement,
            allow_genesis,
        );
        operation.require_delivery = true;
        operation
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.state = AnnounceTopicState::Error;
        self.output = Some(Err(AnnounceTopicError::UnexpectedEvent {
            state,
            expected,
            got,
        }));
        smallvec![]
    }

    fn fail(&mut self, error: AnnounceTopicError) -> Effects {
        self.state = AnnounceTopicState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn fail_transfer(&mut self, error: AnnounceTopicError) -> Effects {
        self.state = AnnounceTopicState::Error;
        self.output = Some(Err(error));
        if self.transfer_outbox.is_some() {
            smallvec![schedule_outbox_drain_effect()]
        } else {
            smallvec![]
        }
    }

    fn finish(&mut self) -> Effects {
        self.state = AnnounceTopicState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }

    fn queue_topic_documents(&mut self) {
        if !self.pending.is_empty() {
            return;
        }

        if let Some(document) = self.document.clone() {
            self.pending.push_back(PendingDocumentSync::Document {
                document,
                bytes: self.document_bytes.take(),
            });
        }
    }

    fn write_document_outbox_effect(
        &mut self,
        document: DocumentSyncTarget,
        bytes: Vec<u8>,
    ) -> Effects {
        let change = match self.document_upsert_change(&document, &bytes) {
            Ok(change) => change,
            Err(error) => return self.fail(error),
        };
        self.write_document_outbox_event_effect(
            document,
            DocumentSyncOutboxEvent::Upsert { bytes, change },
        )
    }

    fn publish_persisted_outbox_effect(&mut self) -> Effects {
        let Some(record) = self.transfer_outbox.as_ref() else {
            return self.unexpected_event(
                "persisted document sync outbox record",
                "missing transfer outbox record".to_string(),
            );
        };
        self.state = AnnounceTopicState::PublishDocument;
        smallvec![Effect::Net(NetEffect::DocumentSync(
            DocumentSyncEffect::PublishDocuments {
                documents: vec![crate::task_incoming::document_publish_from_outbox(record)],
                peers: self.peers.clone(),
            }
        ))]
    }

    fn write_document_outbox_event_effect(
        &mut self,
        document: DocumentSyncTarget,
        event: DocumentSyncOutboxEvent,
    ) -> Effects {
        self.current = Some(document.clone());
        self.state = AnnounceTopicState::WriteOutbox;
        // Announce only ever emits Upsert here, so the record mirrors the
        // change's placement; the admin fallback is unused.
        let record = new_outbox_record(
            self.local_node_id,
            document,
            self.peers.clone(),
            event,
            PlacementRef::NIL,
            self.allow_genesis,
        );
        if self.require_delivery {
            self.transfer_outbox = Some(record.clone());
        }
        match write_outbox_effect(&record) {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(AnnounceTopicError::ConversionError(error.into())),
        }
    }

    fn document_upsert_change(
        &self,
        document: &DocumentSyncTarget,
        bytes: &[u8],
    ) -> Result<DocumentSyncChange, AnnounceTopicError> {
        match document {
            DocumentSyncTarget::Group { .. }
            | DocumentSyncTarget::GroupAuthorization { .. }
            | DocumentSyncTarget::RealmAuthorization { .. }
            | DocumentSyncTarget::RealmConfig { .. }
            | DocumentSyncTarget::User { .. } => Err(AnnounceTopicError::DocumentSync(
                "whole-document admin sync is unsupported; admin documents must sync as operations"
                    .to_string(),
            )),
            DocumentSyncTarget::WatchSubscription { .. } => Err(AnnounceTopicError::DocumentSync(
                "watch subscriptions must sync through atomic watch CRUD outbox records"
                    .to_string(),
            )),
            DocumentSyncTarget::MetadataRegistry {
                group_id,
                document_id,
            } => {
                let record: MetadataRegistryRecord = postcard::from_bytes(bytes)
                    .map_err(|error| AnnounceTopicError::ConversionError(error.into()))?;
                if record.group_id != *group_id || record.document_id != *document_id {
                    return Err(AnnounceTopicError::DocumentSync(format!(
                        "metadata registry target {group_id}/{document_id} does not match payload {}/{}",
                        record.group_id, record.document_id
                    )));
                }
                Ok(DocumentSyncChange {
                    base: None,
                    current: DocumentSyncRevision {
                        generation: record.updated_at_ms,
                        event_id: record.last_event_id,
                        actor: self.local_node_id,
                        updated_at_ms: record.updated_at_ms,
                    },
                    kind: DocumentSyncChangeKind::Upsert,
                    placement: self.placement,
                })
            }
            DocumentSyncTarget::MetadataCreateEvent {
                document_id,
                event_id,
            } => {
                let record: MetadataCreateEventRecord = postcard::from_bytes(bytes)
                    .map_err(|error| AnnounceTopicError::ConversionError(error.into()))?;
                if record.record.document_id != *document_id || record.event_id != *event_id {
                    return Err(AnnounceTopicError::DocumentSync(format!(
                        "metadata create-event target {document_id}/{event_id} does not match payload {}/{}",
                        record.record.document_id, record.event_id
                    )));
                }
                Ok(DocumentSyncChange {
                    base: None,
                    current: DocumentSyncRevision {
                        generation: record.record.updated_at_ms,
                        event_id: record.event_id,
                        actor: record.node_id,
                        updated_at_ms: record.occurred_at_ms,
                    },
                    kind: DocumentSyncChangeKind::Upsert,
                    placement: self.placement,
                })
            }
            DocumentSyncTarget::MetadataDocumentLifecycle { document_id } => {
                let record: MetadataDocumentLifecycleRecord = postcard::from_bytes(bytes)
                    .map_err(|error| AnnounceTopicError::ConversionError(error.into()))?;
                if record.document_id() != *document_id {
                    return Err(AnnounceTopicError::DocumentSync(format!(
                        "metadata document lifecycle target {document_id} does not match payload document {}",
                        record.document_id()
                    )));
                }
                Ok(metadata_document_lifecycle_revision_change(
                    &record,
                    self.local_node_id,
                    self.placement,
                ))
            }
            DocumentSyncTarget::MetadataGraphLifecycle { graph_iri } => {
                let record: MetadataGraphLifecycleRecord = postcard::from_bytes(bytes)
                    .map_err(|error| AnnounceTopicError::ConversionError(error.into()))?;
                if record.graph_iri != *graph_iri {
                    return Err(AnnounceTopicError::DocumentSync(format!(
                        "metadata graph lifecycle target `{graph_iri}` does not match payload graph `{}`",
                        record.graph_iri
                    )));
                }
                Ok(DocumentSyncChange {
                    base: None,
                    current: DocumentSyncRevision {
                        generation: record.updated_at_ms,
                        event_id: Ulid::r#gen(),
                        actor: self.local_node_id,
                        updated_at_ms: record.updated_at_ms,
                    },
                    kind: DocumentSyncChangeKind::Upsert,
                    placement: self.placement,
                })
            }
            // Node usage snapshots, watch-interest digests, and node info/heartbeat
            // documents are single-writer per key and applied as plain upserts
            // (last event wins), so the change only needs a monotonic wall-clock
            // generation from this node.
            DocumentSyncTarget::NodeUsage { .. }
            | DocumentSyncTarget::WatchInterest { .. }
            | DocumentSyncTarget::NodeInfo { .. } => {
                let now = aruna_core::util::unix_timestamp_millis();
                Ok(DocumentSyncChange {
                    base: None,
                    current: DocumentSyncRevision {
                        generation: now,
                        event_id: Ulid::r#gen(),
                        actor: self.local_node_id,
                        updated_at_ms: now,
                    },
                    kind: DocumentSyncChangeKind::Upsert,
                    placement: self.placement,
                })
            }
        }
    }

    fn next_effect(&mut self) -> Effects {
        match self.pending.pop_front() {
            Some(PendingDocumentSync::Document { document, bytes }) => {
                if let Some(bytes) = bytes {
                    if self.require_delivery && self.peers.is_empty() {
                        match self.document_upsert_change(&document, &bytes) {
                            Ok(_) => self.finish(),
                            Err(error) => self.fail(error),
                        }
                    } else {
                        self.write_document_outbox_effect(document, bytes)
                    }
                } else {
                    self.current = Some(document.clone());
                    self.state = AnnounceTopicState::ReadDocument;
                    smallvec![document_repository::read_effect(&document, None)]
                }
            }
            Some(PendingDocumentSync::UserPage {
                realm_id,
                start_after,
            }) => {
                self.state = AnnounceTopicState::ListUsers;
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: USER_KEYSPACE.to_string(),
                    prefix: Some(UserId::storage_prefix(realm_id)),
                    start: start_after.map(IterStart::After),
                    limit: USER_SYNC_PAGE_SIZE,
                    txn_id: None,
                })]
            }
            None => self.finish(),
        }
    }
}

impl Operation for AnnounceTopicOperation {
    type Output = ();
    type Error = AnnounceTopicError;

    fn start(&mut self) -> Effects {
        self.queue_topic_documents();
        self.next_effect()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            AnnounceTopicState::ReadDocument => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    let Some(document) = self.current.clone() else {
                        return self.unexpected_event(
                            "tracked document sync target",
                            "missing current document".to_string(),
                        );
                    };
                    let Some(bytes) = value else {
                        if self.require_delivery {
                            return self.fail(AnnounceTopicError::DocumentSync(format!(
                                "required transfer source is missing for {document:?}"
                            )));
                        }
                        return self.next_effect();
                    };
                    if self.require_delivery {
                        if self.peers.is_empty() {
                            match self.document_upsert_change(&document, &bytes) {
                                Ok(_) => self.finish(),
                                Err(error) => self.fail(error),
                            }
                        } else {
                            self.write_document_outbox_effect(document, bytes.to_vec())
                        }
                    } else {
                        self.write_document_outbox_effect(document, bytes.to_vec())
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            AnnounceTopicState::ListUsers => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    let TopicId::Users(realm_id) = self.topic else {
                        return self.unexpected_event(
                            "users topic",
                            format!("unexpected topic {:?}", self.topic),
                        );
                    };
                    for (key, _) in values {
                        let user_id = match UserId::from_storage_key(&key) {
                            Ok(user_id) => user_id,
                            Err(error) => return self.fail(error.into()),
                        };
                        if user_id.realm_id == realm_id {
                            self.pending.push_back(PendingDocumentSync::Document {
                                document: DocumentSyncTarget::User { user_id },
                                bytes: None,
                            });
                        }
                    }
                    if let Some(start_after) = next_start_after {
                        self.pending.push_back(PendingDocumentSync::UserPage {
                            realm_id,
                            start_after: Some(start_after),
                        });
                    }
                    self.next_effect()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage iter result", format!("{other:?}")),
            },
            AnnounceTopicState::WriteOutbox => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    if self.current.is_none() {
                        return self.unexpected_event(
                            "tracked document sync target",
                            "missing current document".to_string(),
                        );
                    }
                    if self.require_delivery {
                        return self.publish_persisted_outbox_effect();
                    }
                    self.state = AnnounceTopicState::ScheduleSync;
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => {
                    self.unexpected_event("document sync outbox write result", format!("{other:?}"))
                }
            },
            AnnounceTopicState::ScheduleSync => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.current = None;
                    self.next_effect()
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    self.fail(AnnounceTopicError::DocumentSync(format!(
                        "durable document sync scheduling failed: {message}"
                    )))
                }
                other => {
                    self.unexpected_event("document sync timer schedule", format!("{other:?}"))
                }
            },
            AnnounceTopicState::PublishDocument => match event {
                Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsPublished {
                    ..
                })) => {
                    let Some(document) = self.current.clone() else {
                        return self.unexpected_event(
                            "tracked document sync target",
                            "missing current document".to_string(),
                        );
                    };
                    self.state = AnnounceTopicState::SyncDocument;
                    smallvec![Effect::Net(NetEffect::DocumentSync(
                        DocumentSyncEffect::SyncDocument {
                            target: document,
                            peers: self.peers.clone(),
                        }
                    ))]
                }
                Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::Error {
                    error, ..
                })) => self.fail_transfer(AnnounceTopicError::DocumentSync(error)),
                Event::Net(NetEvent::Error(error)) => {
                    self.fail_transfer(AnnounceTopicError::DocumentSync(format!("{error:?}")))
                }
                other => self.fail_transfer(AnnounceTopicError::DocumentSync(format!(
                    "unexpected document publish result: {other:?}"
                ))),
            },
            AnnounceTopicState::SyncDocument => match event {
                Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsReconciled {
                    ..
                })) => {
                    let Some(record) = self.transfer_outbox.as_ref() else {
                        return self.unexpected_event(
                            "persisted document sync outbox record",
                            "missing transfer outbox record".to_string(),
                        );
                    };
                    self.state = AnnounceTopicState::DeleteTransferredOutbox;
                    smallvec![crate::document_sync_outbox::delete_outbox_effect(record)]
                }
                Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::Error {
                    error, ..
                })) => self.fail_transfer(AnnounceTopicError::DocumentSync(error)),
                Event::Net(NetEvent::Error(error)) => {
                    self.fail_transfer(AnnounceTopicError::DocumentSync(format!("{error:?}")))
                }
                other => self.fail_transfer(AnnounceTopicError::DocumentSync(format!(
                    "unexpected document sync result: {other:?}"
                ))),
            },
            AnnounceTopicState::DeleteTransferredOutbox => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    self.transfer_outbox = None;
                    self.finish()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail_transfer(error.into()),
                other => self.fail_transfer(AnnounceTopicError::DocumentSync(format!(
                    "unexpected transfer outbox delete result: {other:?}"
                ))),
            },
            AnnounceTopicState::Finish | AnnounceTopicState::Error | AnnounceTopicState::Init => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AnnounceTopicState::Finish | AnnounceTopicState::Error
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

    use aruna_core::document::DocumentSyncOutboxRecord;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::keyspaces::DOCUMENT_SYNC_OUTBOX_KEYSPACE;
    use aruna_core::metadata::MetadataGraphLifecycleRecord;
    use aruna_core::types::GroupId;
    use ulid::Ulid;

    fn local_node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[1u8; 32]).public()
    }

    fn user_document() -> (UserId, DocumentSyncTarget) {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        (user_id, DocumentSyncTarget::User { user_id })
    }

    fn written_outbox_record(effects: &[Effect]) -> DocumentSyncOutboxRecord {
        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                value,
                txn_id,
                ..
            }),
        ] = effects
        else {
            panic!("expected one outbox write, got {effects:?}");
        };
        assert_eq!(key_space, DOCUMENT_SYNC_OUTBOX_KEYSPACE);
        assert_eq!(txn_id, &None);
        postcard::from_bytes(value.as_ref()).expect("outbox record decodes")
    }

    #[test]
    fn provided_document_bytes_skip_readback_before_outbox_write() {
        for allow_genesis in [false, true] {
            let local_node_id = local_node_id();
            let lifecycle = MetadataGraphLifecycleRecord::deleted(
                "urn:graph:announce".to_string(),
                RealmId::from_bytes([2u8; 32]),
                GroupId::r#gen(),
                Ulid::r#gen(),
                42,
            );
            let document = DocumentSyncTarget::MetadataGraphLifecycle {
                graph_iri: lifecycle.graph_iri.clone(),
            };
            let bytes = postcard::to_allocvec(&lifecycle).expect("lifecycle serializes");
            let mut operation = AnnounceTopicOperation::new_for_document_with_peers_and_bytes(
                document.topic_id(),
                local_node_id,
                document.clone(),
                Vec::new(),
                bytes.clone(),
                allow_genesis,
            );

            let effects = operation.start();

            let record = written_outbox_record(effects.as_slice());
            assert_eq!(record.target, document);
            assert_eq!(record.allow_genesis, allow_genesis);
            let DocumentSyncOutboxEvent::Upsert {
                bytes: actual,
                change,
            } = record.event
            else {
                panic!("expected revisioned upsert");
            };
            assert_eq!(actual, bytes);
            assert_eq!(change.kind, DocumentSyncChangeKind::Upsert);
        }
    }

    #[test]
    fn placement_aware_announce_stamps_resolved_reference() {
        let local_node_id = local_node_id();
        let lifecycle = MetadataGraphLifecycleRecord::deleted(
            "urn:graph:placed-announce".to_string(),
            RealmId::from_bytes([2u8; 32]),
            GroupId::r#gen(),
            Ulid::r#gen(),
            42,
        );
        let document = DocumentSyncTarget::MetadataGraphLifecycle {
            graph_iri: lifecycle.graph_iri.clone(),
        };
        let bytes = postcard::to_allocvec(&lifecycle).expect("lifecycle serializes");
        let placement = PlacementRef {
            strategy_id: Ulid::from_bytes([8; 16]),
            epoch: 3,
            shard: 5,
        };
        let mut operation = AnnounceTopicOperation::new_for_document_with_peers_and_placement(
            document.topic_id(),
            local_node_id,
            Some(document),
            Vec::new(),
            placement,
            true,
        );

        assert!(matches!(
            operation.start().as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Key::from(Vec::new()),
            value: Some(bytes.into()),
        }));
        let record = written_outbox_record(effects.as_slice());
        let DocumentSyncOutboxEvent::Upsert { change, .. } = record.event else {
            panic!("expected revisioned upsert");
        };
        assert_eq!(change.placement, placement);
    }

    fn document_lifecycle_fixture() -> (DocumentSyncTarget, Vec<u8>) {
        let document_id = Ulid::from_bytes([7; 16]);
        let lifecycle = MetadataDocumentLifecycleRecord::Delete {
            event: aruna_core::metadata::MetadataDocumentDeleteRecord {
                event_id: Ulid::from_bytes([8; 16]),
                tombstone: MetadataGraphLifecycleRecord::deleted(
                    "urn:graph:transfer".to_string(),
                    RealmId::from_bytes([2; 32]),
                    GroupId::from_bytes([6; 16]),
                    document_id,
                    42,
                ),
                deleted_after_event_id: Ulid::from_bytes([5; 16]),
            },
        };
        (
            DocumentSyncTarget::MetadataDocumentLifecycle { document_id },
            postcard::to_allocvec(&lifecycle).expect("lifecycle serializes"),
        )
    }

    #[test]
    fn required_document_transfer_fails_without_source() {
        let (document, _) = document_lifecycle_fixture();
        let mut operation =
            AnnounceTopicOperation::new_for_document_transfer_with_peers_and_placement(
                document.topic_id(),
                local_node_id(),
                document,
                vec![iroh::SecretKey::from_bytes(&[2; 32]).public()],
                PlacementRef::NIL,
                false,
            );

        assert!(matches!(
            operation.start().as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Key::from(Vec::new()),
            value: None,
        }));

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(AnnounceTopicError::DocumentSync(error))
                if error.contains("required transfer source is missing")
        ));
    }

    #[test]
    fn required_document_transfer_waits_for_peer_reconciliation() {
        let (document, bytes) = document_lifecycle_fixture();
        let peer = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let mut operation =
            AnnounceTopicOperation::new_for_document_transfer_with_peers_and_placement(
                document.topic_id(),
                local_node_id(),
                document.clone(),
                vec![peer],
                PlacementRef::NIL,
                false,
            );
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Key::from(Vec::new()),
            value: Some(bytes.into()),
        }));
        let outbox = written_outbox_record(effects.as_slice());
        assert_eq!(outbox.target, document);
        assert_eq!(outbox.peers, vec![peer]);
        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: crate::document_sync_outbox::outbox_key(&outbox),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Net(NetEffect::DocumentSync(
                DocumentSyncEffect::PublishDocuments { peers, .. }
            ))] if peers == &vec![peer]
        ));
        let effects = operation.step(Event::Net(NetEvent::DocumentSync(
            DocumentSyncNetEvent::DocumentsPublished {
                targets: vec![document.clone()],
            },
        )));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Net(NetEffect::DocumentSync(
                DocumentSyncEffect::SyncDocument { target, peers }
            ))] if target == &document && peers == &vec![peer]
        ));
        assert!(!operation.is_complete());

        let effects = operation.step(Event::Net(NetEvent::DocumentSync(
            DocumentSyncNetEvent::DocumentsReconciled {
                applied: 0,
                targets: Vec::new(),
                metadata_create_events: Vec::new(),
                metadata_graph_tombstones: Vec::new(),
            },
        )));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Delete { key_space, key, .. })]
                if key_space == DOCUMENT_SYNC_OUTBOX_KEYSPACE
                    && key == &crate::document_sync_outbox::outbox_key(&outbox)
        ));
        assert!(!operation.is_complete());
        let effects = operation.step(Event::Storage(StorageEvent::DeleteResult {
            key: crate::document_sync_outbox::outbox_key(&outbox),
        }));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn every_admin_document_target_refuses_whole_document_announce() {
        let local_node_id = local_node_id();
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let group_id = GroupId::r#gen();
        let (user_id, _) = user_document();
        let admin_targets = [
            DocumentSyncTarget::Group { group_id },
            DocumentSyncTarget::GroupAuthorization { group_id },
            DocumentSyncTarget::RealmAuthorization { realm_id },
            DocumentSyncTarget::RealmConfig { realm_id },
            DocumentSyncTarget::User { user_id },
        ];

        for target in admin_targets {
            assert!(target.is_admin_document(), "misclassified {target:?}");
            let mut operation = AnnounceTopicOperation::new_for_document_with_peers_and_bytes(
                target.topic_id(),
                local_node_id,
                target.clone(),
                Vec::new(),
                b"whole admin document".to_vec(),
                true,
            );

            let effects = operation.start();
            assert!(effects.is_empty(), "unexpected outbox write for {target:?}");
            assert!(operation.is_complete());
            assert!(
                matches!(
                    operation.finalize(),
                    Err(AnnounceTopicError::DocumentSync(error))
                        if error.contains("admin documents must sync as operations")
                ),
                "whole-document announce must refuse {target:?}"
            );
        }
    }

    #[test]
    fn user_document_announcement_fails_without_revision() {
        let local_node_id = local_node_id();
        let (_, document) = user_document();
        let mut operation = AnnounceTopicOperation::new_for_document_with_peers_and_bytes(
            document.topic_id(),
            local_node_id,
            document,
            Vec::new(),
            b"user whole document".to_vec(),
            true,
        );

        let effects = operation.start();
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(AnnounceTopicError::DocumentSync(error))
                if error.contains("admin documents must sync as operations")
        ));
    }

    #[test]
    fn admin_document_announcement_fails_without_revision() {
        let local_node_id = local_node_id();
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let document = DocumentSyncTarget::RealmConfig { realm_id };
        let mut operation = AnnounceTopicOperation::new_for_document_with_peers_and_bytes(
            document.topic_id(),
            local_node_id,
            document,
            Vec::new(),
            b"realm config whole document".to_vec(),
            true,
        );

        let effects = operation.start();
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(AnnounceTopicError::DocumentSync(error))
                if error.contains("admin documents must sync as operations")
        ));
    }
}
