//! Writes one document to the sync outbox and arms the outbox drain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{
    DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentSyncRevision, DocumentTarget,
};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::metadata::{GraphLifecycleRecord, MetadataEventRecord, MetadataLifecycleRecord};
use aruna_core::operation::Operation;
use aruna_core::repository::RepositoryLink;
use aruna_core::rows::{done, read_row, values};
use aruna_core::storage_entries::lifecycle_revision_change;
use aruna_core::structs::PersistentIdMapping;
use aruna_core::structs::identity::user::vault::{UserKeyRecord, VaultRevision};
use aruna_core::structs::persistent_id_change;
use aruna_core::structs::placement::policy::document::{
    PlacementPolicyDocument, placement_policy_change,
};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::task::TaskEvent;
use aruna_core::types::Value;
use thiserror::Error;
use ulid::Ulid;

use crate::sync::outbox_record::{new_outbox_record, outbox_write_entry, schedule_drain_effect};

#[derive(Debug, Clone, Copy, PartialEq)]
enum Phase {
    Reading,
    Writing,
    Scheduling,
}

/// Reads `document` and writes it to the sync outbox for `peers` with its revision change,
/// then arms the outbox drain; a missing document has nothing to announce.
#[derive(Debug, PartialEq)]
pub struct AnnounceTopicOperation {
    document: DocumentTarget,
    local_node_id: NodeId,
    peers: Vec<NodeId>,
    placement: PlacementRef,
    allow_genesis: bool,
    /// The run's time, the revision of single-writer documents.
    now_ms: u64,
    phase: Phase,
}

#[derive(Debug, Error, PartialEq)]
pub enum AnnounceTopicError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("document sync failed: {0}")]
    DocumentSync(String),
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for AnnounceTopicError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            other => Self::Io(other),
        }
    }
}

impl AnnounceTopicOperation {
    pub fn new(
        document: DocumentTarget,
        local_node_id: NodeId,
        peers: Vec<NodeId>,
        placement: PlacementRef,
        allow_genesis: bool,
    ) -> Self {
        Self {
            document,
            local_node_id,
            peers,
            placement,
            allow_genesis,
            now_ms: 0,
            phase: Phase::Reading,
        }
    }

    fn write_outbox(&mut self, bytes: Value) -> Result<Next<()>, AnnounceTopicError> {
        let bytes = bytes.to_vec();
        let change = self.document_upsert_change(&self.document, &bytes)?;
        // Announce only ever emits Upsert here, so the record mirrors the
        // change's placement; the admin fallback is unused.
        let record = new_outbox_record(
            self.local_node_id,
            self.document.clone(),
            self.peers.clone(),
            DocumentOutboxEvent::Upsert { bytes, change },
            PlacementRef::NIL,
            self.allow_genesis,
        );
        let row = outbox_write_entry(&record).map_err(ConversionError::from)?;
        self.phase = Phase::Writing;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn document_upsert_change(
        &self,
        document: &DocumentTarget,
        bytes: &[u8],
    ) -> Result<DocumentChange, AnnounceTopicError> {
        match document {
            DocumentTarget::Group { .. }
            | DocumentTarget::GroupAuthorization { .. }
            | DocumentTarget::RealmAuthorization { .. }
            | DocumentTarget::RealmConfig { .. }
            | DocumentTarget::User { .. } => Err(AnnounceTopicError::DocumentSync(
                "whole-document admin sync is unsupported; admin documents must sync as operations"
                    .to_string(),
            )),
            DocumentTarget::WatchSubscription { .. } => Err(AnnounceTopicError::DocumentSync(
                "watch subscriptions must sync through atomic watch CRUD outbox records"
                    .to_string(),
            )),
            DocumentTarget::EmailPreference { .. } | DocumentTarget::WebhookSubscription { .. } => {
                Err(AnnounceTopicError::DocumentSync(
                    "notification settings sync only through their settings outbox records"
                        .to_string(),
                ))
            }
            DocumentTarget::OwnerSecret { .. } => Err(AnnounceTopicError::DocumentSync(
                "owner secrets sync only through their register outbox records".to_string(),
            )),
            DocumentTarget::MetadataRegistry {
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
                Ok(DocumentChange {
                    base: None,
                    current: DocumentSyncRevision {
                        generation: record.updated_at_ms,
                        event_id: record.last_event_id,
                        actor: self.local_node_id,
                        updated_at_ms: record.updated_at_ms,
                    },
                    kind: DocumentChangeKind::Upsert,
                    placement: self.placement,
                })
            }
            DocumentTarget::MetadataCreateEvent {
                document_id,
                event_id,
            } => {
                let record: MetadataEventRecord = postcard::from_bytes(bytes)
                    .map_err(|error| AnnounceTopicError::ConversionError(error.into()))?;
                if record.record.document_id != *document_id || record.event_id != *event_id {
                    return Err(AnnounceTopicError::DocumentSync(format!(
                        "metadata create-event target {document_id}/{event_id} does not match payload {}/{}",
                        record.record.document_id, record.event_id
                    )));
                }
                Ok(DocumentChange {
                    base: None,
                    current: DocumentSyncRevision {
                        generation: record.record.updated_at_ms,
                        event_id: record.event_id,
                        actor: record.node_id,
                        updated_at_ms: record.occurred_at_ms,
                    },
                    kind: DocumentChangeKind::Upsert,
                    placement: self.placement,
                })
            }
            DocumentTarget::GitRecord {
                document_id,
                event_id,
            } => {
                let record: aruna_core::git::GitRecord = postcard::from_bytes(bytes)
                    .map_err(|error| AnnounceTopicError::ConversionError(error.into()))?;
                if record.document_id != *document_id || record.event_id != *event_id {
                    return Err(AnnounceTopicError::DocumentSync(format!(
                        "Git record target {document_id}/{event_id} does not match payload {}/{}",
                        record.document_id, record.event_id
                    )));
                }
                Ok(aruna_core::git::record_change(&record))
            }
            DocumentTarget::GitPack {
                document_id,
                sha256,
            } => {
                let pack: aruna_core::git::GitPackRecord = postcard::from_bytes(bytes)
                    .map_err(|error| AnnounceTopicError::ConversionError(error.into()))?;
                if pack.document_id != *document_id || !pack.valid(sha256) {
                    return Err(AnnounceTopicError::DocumentSync(format!(
                        "Git pack target of {document_id} does not match its payload"
                    )));
                }
                Ok(pack.change())
            }
            DocumentTarget::MetadataDocumentLifecycle { document_id } => {
                let record: MetadataLifecycleRecord = postcard::from_bytes(bytes)
                    .map_err(|error| AnnounceTopicError::ConversionError(error.into()))?;
                if record.document_id() != *document_id {
                    return Err(AnnounceTopicError::DocumentSync(format!(
                        "metadata document lifecycle target {document_id} does not match payload document {}",
                        record.document_id()
                    )));
                }
                Ok(lifecycle_revision_change(
                    &record,
                    self.local_node_id,
                    self.placement,
                ))
            }
            DocumentTarget::MetadataGraphLifecycle { graph_iri } => {
                let record: GraphLifecycleRecord = postcard::from_bytes(bytes)
                    .map_err(|error| AnnounceTopicError::ConversionError(error.into()))?;
                if record.graph_iri != *graph_iri {
                    return Err(AnnounceTopicError::DocumentSync(format!(
                        "metadata graph lifecycle target `{graph_iri}` does not match payload graph `{}`",
                        record.graph_iri
                    )));
                }
                Ok(DocumentChange {
                    base: None,
                    current: DocumentSyncRevision {
                        generation: record.updated_at_ms,
                        event_id: Ulid::generate(),
                        actor: self.local_node_id,
                        updated_at_ms: record.updated_at_ms,
                    },
                    kind: DocumentChangeKind::Upsert,
                    placement: self.placement,
                })
            }
            DocumentTarget::PersistentIdMapping { document_id } => {
                let mapping = PersistentIdMapping::from_bytes(bytes)
                    .map_err(AnnounceTopicError::ConversionError)?;
                if mapping.target != *document_id {
                    return Err(AnnounceTopicError::DocumentSync(format!(
                        "persistent id mapping target {document_id} does not match payload document {}",
                        mapping.target
                    )));
                }
                Ok(persistent_id_change(&mapping, self.placement))
            }
            DocumentTarget::PlacementPolicy { policy_id } => {
                let document = PlacementPolicyDocument::from_bytes(bytes)
                    .map_err(AnnounceTopicError::ConversionError)?;
                if document.policy_id() != *policy_id {
                    return Err(AnnounceTopicError::DocumentSync(format!(
                        "placement policy target {policy_id} does not match payload policy {}",
                        document.policy_id()
                    )));
                }
                Ok(placement_policy_change(&document, self.placement))
            }
            DocumentTarget::RepositoryLink {
                document_id,
                link_id,
            } => {
                let link = RepositoryLink::from_bytes(bytes)
                    .map_err(AnnounceTopicError::ConversionError)?;
                if link.document_id != *document_id || link.link_id != *link_id {
                    return Err(AnnounceTopicError::DocumentSync(format!(
                        "repository link target {document_id}/{link_id} does not match its payload"
                    )));
                }
                Ok(link.sync_change(self.placement))
            }
            DocumentTarget::VaultRevision { .. } => {
                let revision = VaultRevision::from_bytes(bytes)
                    .map_err(AnnounceTopicError::ConversionError)?;
                if revision.target() != *document {
                    return Err(AnnounceTopicError::DocumentSync(
                        "vault revision target does not match its payload".to_string(),
                    ));
                }
                Ok(revision.sync_change())
            }
            DocumentTarget::UserKey { .. } => {
                let record = UserKeyRecord::from_bytes(bytes)
                    .map_err(AnnounceTopicError::ConversionError)?;
                if record.target() != *document {
                    return Err(AnnounceTopicError::DocumentSync(
                        "user key target does not match its payload".to_string(),
                    ));
                }
                Ok(record.sync_change())
            }
            // Single-writer upserts need only this node's monotonic wall-clock generation.
            DocumentTarget::NodeUsage { .. }
            | DocumentTarget::WatchInterest { .. }
            | DocumentTarget::NodeInfo { .. } => Ok(DocumentChange {
                base: None,
                current: DocumentSyncRevision {
                    generation: self.now_ms,
                    event_id: Ulid::generate(),
                    actor: self.local_node_id,
                    updated_at_ms: self.now_ms,
                },
                kind: DocumentChangeKind::Upsert,
                placement: self.placement,
            }),
        }
    }
}

impl Operation for AnnounceTopicOperation {
    type Output = ();
    type Error = AnnounceTopicError;

    fn first(&mut self) -> Result<Next<()>, AnnounceTopicError> {
        let read = read_row(
            self.document.storage_keyspace(),
            self.document.storage_key(),
        );
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, AnnounceTopicError> {
        match self.phase {
            Phase::Reading => match values::<1>(result)? {
                [Some(bytes)] => self.write_outbox(bytes),
                [None] => Ok(Next::Done(())),
            },
            Phase::Writing => {
                done(result)?;
                self.phase = Phase::Scheduling;
                let schedule = Box::new(schedule_drain_effect());
                Ok(Next::Call(IoCall::Effect(schedule)))
            }
            Phase::Scheduling => {
                let IoResult::Event(event) = result else {
                    return Err(unexpected(result.label()));
                };
                match *event {
                    Event::Task(TaskEvent::TimerScheduled { .. }) => Ok(Next::Done(())),
                    Event::Task(TaskEvent::Error { message, .. }) => {
                        Err(AnnounceTopicError::DocumentSync(format!(
                            "durable document sync scheduling failed: {message}"
                        )))
                    }
                    other => Err(unexpected(other.label())),
                }
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

fn unexpected(got: &'static str) -> AnnounceTopicError {
    AnnounceTopicError::Io(IoError::Unexpected {
        expected: "TaskEvent::TimerScheduled",
        got,
    })
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    use aruna_core::UserId;
    use aruna_core::document::DocumentOutboxRecord;
    use aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::GroupId;
    use std::time::Duration;

    fn local_node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[1u8; 32]).public()
    }

    fn read(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(Value::from)])
    }

    fn written_record(next: Result<Next<()>, AnnounceTopicError>) -> DocumentOutboxRecord {
        let Ok(Next::Call(IoCall::Write(rows))) = next else {
            panic!("expected one outbox write");
        };
        let [(space, _, value)] = rows.as_slice() else {
            panic!("expected one outbox row");
        };
        assert_eq!(space, SYNC_OUTBOX_KEYSPACE);
        postcard::from_bytes(value.as_ref()).expect("outbox record decodes")
    }

    fn lifecycle() -> (DocumentTarget, Vec<u8>) {
        let lifecycle = GraphLifecycleRecord::deleted(
            "urn:graph:placed-announce".to_string(),
            RealmId::from_bytes([2u8; 32]),
            GroupId::generate(),
            Ulid::from_parts(2, 2),
            42,
        );
        let document = DocumentTarget::MetadataGraphLifecycle {
            graph_iri: lifecycle.graph_iri.clone(),
        };
        let bytes = postcard::to_allocvec(&lifecycle).expect("lifecycle serializes");
        (document, bytes)
    }

    #[test]
    fn announce_stamps_placement() {
        let (document, bytes) = lifecycle();
        let placement = PlacementRef {
            strategy_id: Ulid::from_bytes([8; 16]),
            shard: 5,
        };
        for allow_genesis in [false, true] {
            let mut operation = AnnounceTopicOperation::new(
                document.clone(),
                local_node_id(),
                Vec::new(),
                placement,
                allow_genesis,
            );
            let call = read_row(document.storage_keyspace(), document.storage_key());
            assert_eq!(operation.first(), Ok(Next::Call(call)));
            let record = written_record(operation.next(read(Some(bytes.clone()))));
            assert_eq!(record.target, document);
            assert_eq!(record.allow_genesis, allow_genesis);
            let DocumentOutboxEvent::Upsert {
                bytes: actual,
                change,
            } = record.event
            else {
                panic!("expected revisioned upsert");
            };
            assert_eq!(actual, bytes);
            assert_eq!(change.kind, DocumentChangeKind::Upsert);
            assert_eq!(change.placement, placement);
            let schedule = IoCall::Effect(Box::new(schedule_drain_effect()));
            assert_eq!(operation.next(IoResult::Done), Ok(Next::Call(schedule)));
            let scheduled = Event::Task(TaskEvent::TimerScheduled {
                key: aruna_core::task::TaskKey::DrainSyncOutbox,
                after: Duration::ZERO,
            });
            let scheduled = IoResult::Event(Box::new(scheduled));
            assert_eq!(operation.next(scheduled), Ok(Next::Done(())));
        }
    }

    #[test]
    fn stamps_single_writer() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let node_id = local_node_id();
        let document = DocumentTarget::NodeInfo { realm_id, node_id };
        let mut operation =
            AnnounceTopicOperation::new(document, node_id, Vec::new(), PlacementRef::NIL, false);
        operation.stamp(77);
        let record = written_record(operation.next(read(Some(b"info".to_vec()))));
        let DocumentOutboxEvent::Upsert { change, .. } = record.event else {
            panic!("expected revisioned upsert");
        };
        assert_eq!(change.current.generation, 77);
        assert_eq!(change.current.updated_at_ms, 77);
    }

    #[test]
    fn missing_document_ends() {
        let (document, _) = lifecycle();
        let mut operation = AnnounceTopicOperation::new(
            document,
            local_node_id(),
            Vec::new(),
            PlacementRef::NIL,
            true,
        );
        assert_eq!(operation.next(read(None)), Ok(Next::Done(())));
    }

    #[test]
    fn admin_refuses_announce() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let group_id = GroupId::generate();
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let admin_targets = [
            DocumentTarget::Group { group_id },
            DocumentTarget::GroupAuthorization { group_id },
            DocumentTarget::RealmAuthorization { realm_id },
            DocumentTarget::RealmConfig { realm_id },
            DocumentTarget::User { user_id },
        ];

        for target in admin_targets {
            assert!(target.is_admin_document(), "misclassified {target:?}");
            let mut operation = AnnounceTopicOperation::new(
                target.clone(),
                local_node_id(),
                Vec::new(),
                PlacementRef::NIL,
                true,
            );
            let whole = read(Some(b"whole admin document".to_vec()));
            assert!(
                matches!(
                    operation.next(whole),
                    Err(AnnounceTopicError::DocumentSync(error))
                        if error.contains("admin documents must sync as operations")
                ),
                "whole-document announce must refuse {target:?}"
            );
        }
    }

    #[test]
    fn refuses_bad_results() {
        let (document, bytes) = lifecycle();
        let mut operation = AnnounceTopicOperation::new(
            document,
            local_node_id(),
            Vec::new(),
            PlacementRef::NIL,
            true,
        );
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(AnnounceTopicError::Io(IoError::Unexpected { .. }))
        ));
        operation.next(read(Some(bytes))).unwrap();
        operation.next(IoResult::Done).unwrap();
        let refused = Event::Task(TaskEvent::Error {
            key: None,
            message: "no task handle".to_string(),
        });
        assert!(matches!(
            operation.next(IoResult::Event(Box::new(refused))),
            Err(AnnounceTopicError::DocumentSync(_))
        ));
    }
}
