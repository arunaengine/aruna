use std::collections::VecDeque;

use aruna_core::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent, DocumentSyncTarget,
};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::metadata::MetadataError;
use aruna_core::operation::Operation;
use aruna_core::storage_entries::document_sync_revision_key;
use aruna_core::structs::RealmId;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, UserId};
use aruna_core::{DOCUMENT_SYNC_REVISION_KEYSPACE, NodeId, TopicId, USER_KEYSPACE};
use smallvec::smallvec;
use thiserror::Error;

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
    state: AnnounceTopicState,
    pending: VecDeque<PendingDocumentSync>,
    current: Option<DocumentSyncTarget>,
    output: Option<Result<(), AnnounceTopicError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum AnnounceTopicState {
    Init,
    ReadDocument,
    ReadUserDocumentAndRevision,
    ListUsers,
    WriteOutbox,
    ScheduleSync,
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
    pub fn new(topic: TopicId, _local_node_id: NodeId) -> Self {
        Self::new_for_document(topic, _local_node_id, None)
    }

    pub fn new_for_document(
        topic: TopicId,
        local_node_id: NodeId,
        document: Option<DocumentSyncTarget>,
    ) -> Self {
        Self::new_for_document_with_peers(topic, local_node_id, document, Vec::new())
    }

    pub fn new_for_document_with_peers(
        topic: TopicId,
        local_node_id: NodeId,
        document: Option<DocumentSyncTarget>,
        peers: Vec<NodeId>,
    ) -> Self {
        Self {
            topic,
            document,
            local_node_id,
            peers,
            document_bytes: None,
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
    ) -> Self {
        Self {
            topic,
            document: Some(document),
            local_node_id,
            peers,
            document_bytes: Some(bytes),
            state: AnnounceTopicState::Init,
            pending: VecDeque::new(),
            current: None,
            output: None,
        }
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
            return;
        }

        match &self.topic {
            TopicId::Realm(realm_id) => {
                self.pending.push_back(PendingDocumentSync::Document {
                    document: DocumentSyncTarget::RealmAuthorization {
                        realm_id: *realm_id,
                    },
                    bytes: None,
                });
                self.pending.push_back(PendingDocumentSync::Document {
                    document: DocumentSyncTarget::RealmConfig {
                        realm_id: *realm_id,
                    },
                    bytes: None,
                });
            }
            TopicId::Group(group_id) => {
                self.pending.push_back(PendingDocumentSync::Document {
                    document: DocumentSyncTarget::Group {
                        group_id: *group_id,
                    },
                    bytes: None,
                });
                self.pending.push_back(PendingDocumentSync::Document {
                    document: DocumentSyncTarget::GroupAuthorization {
                        group_id: *group_id,
                    },
                    bytes: None,
                });
            }
            TopicId::Users(realm_id) => self.pending.push_back(PendingDocumentSync::UserPage {
                realm_id: *realm_id,
                start_after: None,
            }),
            TopicId::Metadata(_) | TopicId::Node(_) => {}
        }
    }

    fn write_document_outbox_effect(
        &mut self,
        document: DocumentSyncTarget,
        bytes: Vec<u8>,
    ) -> Effects {
        self.write_document_outbox_event_effect(document, DocumentSyncOutboxEvent::Upsert { bytes })
    }

    fn write_document_outbox_event_effect(
        &mut self,
        document: DocumentSyncTarget,
        event: DocumentSyncOutboxEvent,
    ) -> Effects {
        self.current = Some(document.clone());
        self.state = AnnounceTopicState::WriteOutbox;
        let record = new_outbox_record(self.local_node_id, document, self.peers.clone(), event);
        match write_outbox_effect(&record) {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(AnnounceTopicError::ConversionError(error.into())),
        }
    }

    fn read_user_document_with_revision_effect(document: &DocumentSyncTarget) -> Effect {
        Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    document_repository::storage_keyspace(document).to_string(),
                    document_repository::storage_key(document),
                ),
                (
                    DOCUMENT_SYNC_REVISION_KEYSPACE.to_string(),
                    document_sync_revision_key(document),
                ),
            ],
            txn_id: None,
        })
    }

    fn user_outbox_event(
        bytes: Vec<u8>,
        revision_value: Option<&aruna_core::types::Value>,
    ) -> DocumentSyncOutboxEvent {
        match revision_value
            .and_then(|value| postcard::from_bytes::<DocumentSyncChange>(value.as_ref()).ok())
            .filter(|change| change.kind == DocumentSyncChangeKind::Upsert)
        {
            Some(change) => DocumentSyncOutboxEvent::UpsertWithRevision { bytes, change },
            None => DocumentSyncOutboxEvent::Upsert { bytes },
        }
    }

    fn next_effect(&mut self) -> Effects {
        match self.pending.pop_front() {
            Some(PendingDocumentSync::Document { document, bytes }) => {
                if let Some(bytes) = bytes {
                    self.write_document_outbox_effect(document, bytes)
                } else if matches!(document, DocumentSyncTarget::User { .. }) {
                    self.current = Some(document.clone());
                    self.state = AnnounceTopicState::ReadUserDocumentAndRevision;
                    smallvec![Self::read_user_document_with_revision_effect(&document)]
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
                        return self.next_effect();
                    };
                    self.write_document_outbox_effect(document, bytes.to_vec())
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage read result", format!("{other:?}")),
            },
            AnnounceTopicState::ReadUserDocumentAndRevision => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let Some(document) = self.current.clone() else {
                        return self.unexpected_event(
                            "tracked document sync target",
                            "missing current document".to_string(),
                        );
                    };
                    let [(_, user_value), (_, revision_value)] = values.as_slice() else {
                        return self.unexpected_event(
                            "storage batch read result with user document and revision",
                            format!("{values:?}"),
                        );
                    };
                    let Some(bytes) = user_value else {
                        return self.next_effect();
                    };
                    let event = Self::user_outbox_event(bytes.to_vec(), revision_value.as_ref());
                    self.write_document_outbox_event_effect(document, event)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("storage batch read result", format!("{other:?}")),
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

    use aruna_core::document::{
        DocumentSyncChange, DocumentSyncOutboxRecord, DocumentSyncRevision,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::keyspaces::{
        DOCUMENT_SYNC_OUTBOX_KEYSPACE, DOCUMENT_SYNC_REVISION_KEYSPACE, USER_KEYSPACE,
    };
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

    fn upsert_change(actor: NodeId) -> DocumentSyncChange {
        DocumentSyncChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 7,
                event_id: Ulid::from_bytes([4u8; 16]),
                actor,
                updated_at_ms: 123,
            },
            kind: DocumentSyncChangeKind::Upsert,
        }
    }

    fn assert_user_batch_read(effects: &[Effect], user_id: UserId, document: &DocumentSyncTarget) {
        let [Effect::Storage(StorageEffect::BatchRead { reads, txn_id })] = effects else {
            panic!("expected user batch read, got {effects:?}");
        };
        assert_eq!(txn_id, &None);
        assert_eq!(reads.len(), 2);
        assert_eq!(reads[0].0, USER_KEYSPACE);
        assert_eq!(reads[0].1.as_ref(), user_id.to_bytes().as_slice());
        assert_eq!(reads[1].0, DOCUMENT_SYNC_REVISION_KEYSPACE);
        assert_eq!(reads[1].1, document_sync_revision_key(document));
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
        let local_node_id = local_node_id();
        let document = DocumentSyncTarget::MetadataRegistry {
            group_id: GroupId::new(),
            document_id: Ulid::new(),
        };
        let bytes = vec![1, 2, 3, 4];
        let mut operation = AnnounceTopicOperation::new_for_document_with_peers_and_bytes(
            document.topic_id(),
            local_node_id,
            document.clone(),
            Vec::new(),
            bytes.clone(),
        );

        let effects = operation.start();

        let record = written_outbox_record(effects.as_slice());
        assert_eq!(record.target, document);
        assert_eq!(record.event, DocumentSyncOutboxEvent::Upsert { bytes });
    }

    #[test]
    fn user_with_upsert_sidecar_writes_upsert_with_revision() {
        let local_node_id = local_node_id();
        let (user_id, document) = user_document();
        let bytes = vec![9, 8, 7];
        let change = upsert_change(local_node_id);
        let mut operation = AnnounceTopicOperation::new_for_document_with_peers(
            document.topic_id(),
            local_node_id,
            Some(document.clone()),
            Vec::new(),
        );

        let effects = operation.start();
        assert_user_batch_read(effects.as_slice(), user_id, &document);

        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (user_id.to_bytes().into(), Some(bytes.clone().into())),
                (
                    document_sync_revision_key(&document),
                    Some(postcard::to_allocvec(&change).unwrap().into()),
                ),
            ],
        }));

        let record = written_outbox_record(effects.as_slice());
        assert_eq!(record.target, document);
        assert_eq!(
            record.event,
            DocumentSyncOutboxEvent::UpsertWithRevision { bytes, change }
        );
    }

    #[test]
    fn user_without_sidecar_writes_legacy_upsert() {
        let local_node_id = local_node_id();
        let (user_id, document) = user_document();
        let bytes = vec![5, 6, 7];
        let mut operation = AnnounceTopicOperation::new_for_document_with_peers(
            document.topic_id(),
            local_node_id,
            Some(document.clone()),
            Vec::new(),
        );

        let effects = operation.start();
        assert_user_batch_read(effects.as_slice(), user_id, &document);

        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (user_id.to_bytes().into(), Some(bytes.clone().into())),
                (document_sync_revision_key(&document), None),
            ],
        }));

        let record = written_outbox_record(effects.as_slice());
        assert_eq!(record.target, document);
        assert_eq!(record.event, DocumentSyncOutboxEvent::Upsert { bytes });
    }
}
