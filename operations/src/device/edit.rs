//! Applies one owner edit to a local replica and queues it for the realm.
//! The holder re-checks authority when the queued edit is forwarded.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::keyspaces::METADATA_ACTOR_KEYSPACE;
use aruna_core::metadata::{
    MetadataActor, MetadataBatch, MetadataBatchSource, MetadataEffect, MetadataError, MetadataEvent,
};
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::rows::{done, read_row, value};
use aruna_core::storage_entries::{metadata_actor_entry, metadata_actor_key};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use craqle::ActorId;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::device::enqueue_draft::{EnqueueDraftError, EnqueueDraftInput, EnqueueDraftOperation};
use crate::device::publish_queue::{PublishEntry, PublishKind, PublishState};
use crate::device::replica::{ReplicaRecord, mark_edited};
use crate::device::replica_store::store_replica;
use crate::device::sync_status::read_publish_entries;
use crate::metadata::update_document::UpdateDocumentMutation;

#[derive(Debug, Error, PartialEq)]
pub enum DeviceEditError {
    #[error("this device holds no usable replica of the document")]
    NoReplica,
    #[error("the device store is unavailable")]
    Unavailable,
    #[error("{0}")]
    Invalid(String),
    #[error("the authoring queue already holds the maximum of {limit} entries")]
    QueueFull { limit: usize },
}

impl From<IoError> for DeviceEditError {
    fn from(_: IoError) -> Self {
        Self::Unavailable
    }
}

fn metadata_call(effect: MetadataEffect) -> IoCall {
    IoCall::from(Effect::Metadata(effect))
}

fn metadata_event(result: IoResult) -> Result<MetadataEvent, IoError> {
    match result.event()? {
        Event::Metadata(event) => Ok(event),
        other => Err(IoError::Unexpected {
            expected: "Event::Metadata",
            got: other.label(),
        }),
    }
}

fn warn_unflushed(event: &MetadataEvent) {
    if !matches!(event, MetadataEvent::GraphFlushed) {
        warn!(event = ?event, "Could not persist an offline edit's graph");
    }
}

/// What the pending call of an offline edit answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EditStep {
    Actor,
    Plan,
    Queue,
    Merge,
    Flush,
    Render,
    Store,
    Arm,
}

/// Applies one edit to the local replica and queues it for the realm. The holder merges the
/// same craqle batch so both sides converge; the drain confirms the record.
pub fn apply_local_edit(
    owner: UserId,
    node_id: NodeId,
    replica: &ReplicaRecord,
    mutation: UpdateDocumentMutation,
    issued_at: u64,
) -> ApplyEditOperation {
    ApplyEditOperation {
        owner,
        node_id,
        replica: replica.clone(),
        authored: authored_source(mutation),
        issued_at,
        step: EditStep::Actor,
        record: replica.record.as_deref().cloned(),
        pending: None,
        batch: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct ApplyEditOperation {
    owner: UserId,
    node_id: NodeId,
    replica: ReplicaRecord,
    authored: Option<MetadataBatchSource>,
    issued_at: u64,
    step: EditStep,
    record: Option<MetadataRegistryRecord>,
    pending: Option<(MetadataActor, Ulid)>,
    batch: Option<MetadataBatch>,
}

type EditTurn = Result<Next<MetadataRegistryRecord>, DeviceEditError>;

impl ApplyEditOperation {
    fn record(&self) -> Result<&MetadataRegistryRecord, IoError> {
        self.record.as_ref().ok_or(IoError::Unfinished)
    }

    fn authored(&self) -> Result<&MetadataBatchSource, IoError> {
        self.authored.as_ref().ok_or(IoError::Unfinished)
    }

    /// Picks the next dot of this device's actor and plans the edit without changing the graph.
    fn plan(&mut self, current: Option<MetadataActor>) -> EditTurn {
        let record = self.record()?;
        let mut draft_id = Ulid::generate();
        if let Some(current) = &current
            && draft_id <= current.last_event_id
        {
            draft_id =
                (current.last_event_id.increment()).map_err(|_| DeviceEditError::Unavailable)?;
        }
        let document_id = record.document_id;
        let actor = MetadataActor::next(current.as_ref(), document_id, self.node_id, draft_id)
            .ok_or(DeviceEditError::Unavailable)?;
        let plan = MetadataEffect::PlanBatch {
            graph_iri: record.graph_iri.clone(),
            actor: actor.actor,
            counter: actor.counter,
            source: self.authored()?.clone(),
        };
        self.pending = Some((actor, draft_id));
        self.step = EditStep::Plan;
        Ok(Next::Call(metadata_call(plan)))
    }

    /// Queues the planned batch durably; holders never get refused dots, so a new edit must
    /// not wait for them.
    fn queue(&mut self, event: MetadataEvent) -> EditTurn {
        let mut batch = match event {
            MetadataEvent::BatchPlanned { batch, .. } => batch,
            MetadataEvent::Error {
                error: MetadataError::InvalidInput(message),
                ..
            } => return Err(DeviceEditError::Invalid(message)),
            MetadataEvent::Error {
                error: MetadataError::Validation(violations),
                ..
            } => return Err(DeviceEditError::Invalid(format!("{violations:?}"))),
            other => {
                let document_id = self.record()?.document_id;
                warn!(%document_id, event = ?other, "An offline edit could not be planned");
                return Err(DeviceEditError::Unavailable);
            }
        };
        let (actor, draft_id) = self.pending.take().ok_or(IoError::Unfinished)?;
        actor.strip(&mut batch.base_clock);
        let authored = self.authored()?.clone();
        let entry = PublishEntry::edit(
            draft_id,
            self.owner,
            self.record()?,
            batch.clone(),
            authored,
        );
        let enqueue = EnqueueDraftOperation::new(EnqueueDraftInput { entry })
            .with_actor(actor)
            .with_fence(self.issued_at);
        self.batch = Some(batch);
        self.step = EditStep::Queue;
        Ok(Next::Call(IoCall::child(enqueue)))
    }

    fn merge(&mut self) -> EditTurn {
        let batch = self.batch.as_ref().ok_or(IoError::Unfinished)?;
        let merge = MetadataEffect::MergeBatch {
            graph_iri: batch.graph_iri.clone(),
            batch: batch.clone(),
        };
        self.step = EditStep::Merge;
        Ok(Next::Call(metadata_call(merge)))
    }

    /// Stores the replica with the merged graph this device displays from now on.
    fn store(&mut self, jsonld: String) -> EditTurn {
        let record = self.record()?.clone();
        let batch = self.batch.as_ref().ok_or(IoError::Unfinished)?;
        let mut local_clock = batch.base_clock.clone();
        local_clock.advance(ActorId::from_bytes(batch.actor), batch.counter);
        let mut edited = self.replica.clone();
        edited.record = Some(Box::new(record));
        mark_edited(&mut edited, local_clock);
        edited.dataset_digest = craqle::canonicalize_jsonld(&jsonld)
            .ok()
            .map(|canonical| canonical.digest);
        edited.displayed_jsonld = jsonld;
        self.step = EditStep::Store;
        Ok(Next::Call(IoCall::child(store_replica(&edited))))
    }
}

impl Operation for ApplyEditOperation {
    type Output = MetadataRegistryRecord;
    type Error = DeviceEditError;

    fn hold_scope(&self) -> Option<HoldScope> {
        let record = self.record.as_ref()?;
        Some(HoldScope::DocumentWrite(record.document_id))
    }

    fn first(&mut self) -> EditTurn {
        let document_id = (self.record.as_ref())
            .ok_or(DeviceEditError::NoReplica)?
            .document_id;
        if self.authored.is_none() {
            let reason = "this mutation is not authored on a device".to_string();
            return Err(DeviceEditError::Invalid(reason));
        }
        Ok(Next::Call(actor_call(document_id)))
    }

    fn next(&mut self, result: IoResult) -> EditTurn {
        match self.step {
            EditStep::Actor => self.plan(actor_answer(result)?),
            EditStep::Plan => self.queue(metadata_event(result)?),
            EditStep::Queue => match result.child::<PublishEntry, EnqueueDraftError>()? {
                Ok(_) => self.merge(),
                Err(EnqueueDraftError::QueueFull { limit }) => {
                    Err(DeviceEditError::QueueFull { limit })
                }
                Err(error) => {
                    warn!(%error, "Could not queue an offline edit");
                    Err(DeviceEditError::Unavailable)
                }
            },
            EditStep::Merge => match metadata_event(result)? {
                MetadataEvent::BatchMerged { .. } => {
                    self.step = EditStep::Flush;
                    Ok(Next::Call(metadata_call(MetadataEffect::FlushGraph)))
                }
                other => {
                    warn!(event = ?other, "A queued offline edit did not apply");
                    Err(DeviceEditError::Unavailable)
                }
            },
            // Best effort: a failure leaves exactly the queued work the replay covers.
            EditStep::Flush => {
                warn_unflushed(&metadata_event(result)?);
                self.step = EditStep::Render;
                let render = render(&self.record()?.graph_iri);
                Ok(Next::Call(IoCall::child(render)))
            }
            EditStep::Render => self.store(result.child::<String, DeviceEditError>()??),
            EditStep::Store => {
                result.child::<(), IoError>()?.ok();
                self.step = EditStep::Arm;
                let arm = TaskEffect::ScheduleIdleTimer {
                    key: TaskKey::DrainDeviceIntake,
                    after: Duration::ZERO,
                };
                Ok(Next::Call(IoCall::from(Effect::Task(arm))))
            }
            // Wakes the publish drain so a reachable realm sees the edit at once.
            EditStep::Arm => {
                if let Event::Task(TaskEvent::Error { message, .. }) = result.event()? {
                    warn!(%message, "Failed to arm the device publish drain");
                }
                Ok(Next::Done(self.record.take().ok_or(IoError::Unfinished)?))
            }
        }
    }
}

/// The submission behind an edit, or `None` for a mutation only a holder
/// originates.
fn authored_source(mutation: UpdateDocumentMutation) -> Option<MetadataBatchSource> {
    match mutation {
        UpdateDocumentMutation::ReplaceRoCrate { jsonld } => {
            Some(MetadataBatchSource::ReplaceRoCrate { jsonld })
        }
        UpdateDocumentMutation::UpsertDataEntity { jsonld } => {
            Some(MetadataBatchSource::UpsertDataEntity { jsonld })
        }
        UpdateDocumentMutation::UpsertContextualEntity { jsonld } => {
            Some(MetadataBatchSource::UpsertContextualEntity { jsonld })
        }
        _ => None,
    }
}

fn actor_call(document_id: Ulid) -> IoCall {
    read_row(METADATA_ACTOR_KEYSPACE, metadata_actor_key(document_id))
}

fn actor_answer(result: IoResult) -> Result<Option<MetadataActor>, DeviceEditError> {
    value(result)?
        .map(|bytes| postcard::from_bytes(&bytes).map_err(|_| DeviceEditError::Unavailable))
        .transpose()
}

/// This device's CRDT actor for the document, if it edited the document before.
pub(super) fn read_actor(document_id: Ulid) -> ReadActorOperation {
    ReadActorOperation { document_id }
}

#[derive(Debug, PartialEq)]
pub(super) struct ReadActorOperation {
    document_id: Ulid,
}

impl Operation for ReadActorOperation {
    type Output = Option<MetadataActor>;
    type Error = DeviceEditError;

    fn first(&mut self) -> Result<Next<Option<MetadataActor>>, DeviceEditError> {
        Ok(Next::Call(actor_call(self.document_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<MetadataActor>>, DeviceEditError> {
        Ok(Next::Done(actor_answer(result)?))
    }
}

/// Marks an edit the realm permanently refused, so later edits start a fresh actor and never
/// depend on it. Outputs whether the actor record was updated.
pub(super) fn reject_edit(document_id: Ulid, dot: ([u8; 32], u64)) -> RejectEditOperation {
    RejectEditOperation {
        document_id,
        dot,
        writing: false,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct RejectEditOperation {
    document_id: Ulid,
    dot: ([u8; 32], u64),
    writing: bool,
}

impl Operation for RejectEditOperation {
    type Output = bool;
    type Error = IoError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::DocumentWrite(self.document_id))
    }

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        Ok(Next::Call(actor_call(self.document_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        if self.writing {
            done(result)?;
            return Ok(Next::Done(true));
        }
        let Ok(Some(mut current)) = actor_answer(result) else {
            return Ok(Next::Done(false));
        };
        current.reject(self.dot.0, self.dot.1);
        let Ok(row) = metadata_actor_entry(&current) else {
            return Ok(Next::Done(false));
        };
        self.writing = true;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

/// The render of a graph as this device displays it.
pub(super) fn render(graph_iri: &str) -> RenderGraphOperation {
    RenderGraphOperation {
        graph_iri: graph_iri.to_string(),
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct RenderGraphOperation {
    graph_iri: String,
}

impl Operation for RenderGraphOperation {
    type Output = String;
    type Error = DeviceEditError;

    fn first(&mut self) -> Result<Next<String>, DeviceEditError> {
        let export = MetadataEffect::ExportRoCrate {
            graph_iri: self.graph_iri.clone(),
        };
        Ok(Next::Call(metadata_call(export)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, DeviceEditError> {
        match metadata_event(result)? {
            MetadataEvent::RoCrateExportResult { jsonld, .. } => Ok(Next::Done(jsonld)),
            other => {
                warn!(event = ?other, "Could not render an edited replica");
                Err(DeviceEditError::Unavailable)
            }
        }
    }
}

/// Whether one queued entry's batch must be re-merged after a restart: only an
/// edit still on its way to the realm, which nothing else restores. A published
/// entry returns by refresh; a parked one is not this device's state.
fn replays_edit(entry: &PublishEntry) -> bool {
    matches!(entry.kind, PublishKind::Edit { .. })
        && matches!(
            entry.state,
            PublishState::Pending { .. } | PublishState::Publishing { .. }
        )
}

/// Re-merges every queued edit into this device's replicas, outputting how many applied. A crash
/// between the WAL write and persist can leave the graph behind; merging is idempotent by dot.
pub fn replay_queued_edits() -> ReplayEditsOperation {
    ReplayEditsOperation {
        step: ReplayStep::Entries,
        queue: VecDeque::new(),
        asked: None,
        replayed: 0,
    }
}

/// What the pending call of replaying edits answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReplayStep {
    Entries,
    Merge,
    Flush,
}

#[derive(Debug, PartialEq)]
pub struct ReplayEditsOperation {
    step: ReplayStep,
    queue: VecDeque<PublishEntry>,
    asked: Option<Ulid>,
    replayed: usize,
}

impl ReplayEditsOperation {
    fn next_edit(&mut self) -> Result<Next<usize>, IoError> {
        while let Some(entry) = self.queue.pop_front() {
            let PublishKind::Edit { batch, .. } = &entry.kind else {
                continue;
            };
            if !replays_edit(&entry) {
                continue;
            }
            let merge = MetadataEffect::MergeBatch {
                graph_iri: batch.graph_iri.clone(),
                batch: (**batch).clone(),
            };
            self.asked = Some(entry.draft_id);
            self.step = ReplayStep::Merge;
            return Ok(Next::Call(metadata_call(merge)));
        }
        if self.replayed == 0 {
            return Ok(Next::Done(0));
        }
        self.step = ReplayStep::Flush;
        Ok(Next::Call(metadata_call(MetadataEffect::FlushGraph)))
    }
}

impl Operation for ReplayEditsOperation {
    type Output = usize;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<usize>, IoError> {
        Ok(Next::Call(IoCall::child(read_publish_entries())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, IoError> {
        match self.step {
            ReplayStep::Entries => {
                let entries = result.child::<Vec<PublishEntry>, IoError>()?;
                self.queue = entries.unwrap_or_default().into();
                self.next_edit()
            }
            ReplayStep::Merge => {
                match metadata_event(result)? {
                    MetadataEvent::BatchMerged { applied, .. } => {
                        self.replayed += usize::from(applied);
                    }
                    other => {
                        let draft_id = self.asked.take();
                        warn!(?draft_id, event = ?other, "Could not replay a queued edit");
                    }
                }
                self.next_edit()
            }
            ReplayStep::Flush => {
                warn_unflushed(&metadata_event(result)?);
                Ok(Next::Done(self.replayed))
            }
        }
    }
}

/// Whether this replica may take the edit locally: the owner selected it, or it is a usable copy
/// the owner read and the owner edits it, and a refresh has left it something to edit.
pub fn accepts_edits(replica: &ReplicaRecord, by_owner: bool) -> bool {
    replica.record.is_some() && (replica.selected || by_owner && replica.serves_reads())
}

#[cfg(test)]
mod tests {
    use super::{
        ApplyEditOperation, DeviceEditError, EditStep, accepts_edits, apply_local_edit,
        authored_source, read_actor, reject_edit, render, replay_queued_edits, replays_edit,
    };
    use crate::device::publish_queue::{
        MAX_PUBLISH_ENTRIES, PublishEntry, PublishKind, PublishState, publish_entry,
    };
    use crate::device::replica::{ReplicaOrigin, ReplicaRecord};
    use crate::driver::DriverContext;
    use crate::driver::drive;
    use crate::metadata::MetadataHandle;
    use crate::tests::device::record;
    use aruna_core::UserId;
    use aruna_core::calls::IoResult;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::metadata::{
        MetadataBatch, MetadataBatchSource, MetadataCrateRequest, MetadataEffect, MetadataEvent,
        MetadataGraphPolicy, MetadataRequestDurability,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
    use craqle::VectorClock;
    use std::sync::Arc;
    use ulid::Ulid;

    fn node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn edit(state: PublishState) -> PublishEntry {
        let mut entry = PublishEntry::edit(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([3u8; 32])),
            &record(),
            MetadataBatch {
                graph_iri: record().graph_iri,
                actor: [7u8; 32],
                counter: 1,
                base_clock: VectorClock::default(),
                ops: Vec::new(),
                timestamp_millis: 9,
            },
            MetadataBatchSource::UpsertDataEntity {
                jsonld: "{}".to_string(),
            },
        );
        entry.state = state;
        entry
    }

    /// A device node whose replica already holds the document's graph.
    async fn device() -> (
        tempfile::TempDir,
        Arc<DriverContext>,
        MetadataRegistryRecord,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(
            dir.path().join("storage").to_str().expect("storage path"),
        )
        .unwrap();
        let graph = aruna_graph::GraphHandle::open(
            dir.path().join("metadata"),
            node(1),
            None,
            None,
            aruna_graph::GraphOptions::default()
                .with_search_storage(aruna_graph::MetadataSearchStorage::Memory),
        )
        .unwrap();
        let metadata = MetadataHandle::new(
            dir.path().join("metadata"),
            storage.clone(),
            None,
            Default::default(),
        );
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata),
            graph_handle: Some(graph),
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        });
        let record = record();
        assert!(matches!(
            context
                .metadata_handle
                .as_ref()
                .unwrap()
                .run_effect(
                    &context,
                    MetadataEffect::CreateCrate {
                        request: MetadataCrateRequest {
                            graph_iri: record.graph_iri.clone(),
                            name: "Notes".to_string(),
                            description: "Offline notes".to_string(),
                            date_published: "2026-08-29".to_string(),
                            license: None,
                            policy: MetadataGraphPolicy {
                                public: false,
                                permission_paths: vec![record.permission_path.clone()],
                            },
                            durability: MetadataRequestDurability::Durable,
                            deterministic_actor: Some([1u8; 32]),
                        },
                    }
                )
                .await,
            Event::Metadata(MetadataEvent::CreateCrateResult { .. })
        ));
        (dir, context, record)
    }

    fn contact(name: &str) -> crate::metadata::update_document::UpdateDocumentMutation {
        crate::metadata::update_document::UpdateDocumentMutation::UpsertContextualEntity {
            jsonld: format!(r##"{{"@id":"#{name}","@type":"Person","name":"{name}"}}"##),
        }
    }

    fn replica(record: &MetadataRegistryRecord) -> ReplicaRecord {
        let mut replica = ReplicaRecord::new(
            record.document_id,
            record.group_id,
            record.document_path.clone(),
            ReplicaOrigin::Realm,
        );
        replica.record = Some(Box::new(record.clone()));
        replica
    }

    // A copy the owner read is edited offline by the owner alone; a selected one as before.
    #[tokio::test]
    async fn admits_read_copy() {
        let (_dir, _context, record) = device().await;
        let mut read = replica(&record);
        read.selected = false;
        read.displayed_jsonld = r#"{"@graph":[]}"#.to_string();
        assert!(accepts_edits(&read, true));
        assert!(!accepts_edits(&read, false));
        read.displayed_jsonld.clear();
        assert!(
            !accepts_edits(&read, true),
            "an empty copy has nothing to edit"
        );
        assert!(accepts_edits(&replica(&record), false));
    }

    // A visibility change only a holder can apply is never queued as a local edit: offline it
    // fails, while a plain edit of the same read copy still queues.
    #[tokio::test]
    async fn forwards_visibility_change() {
        use crate::metadata::forward::routed::update::{DocumentUpdate, RouteUpdateOperation};
        use aruna_core::document::DocumentTarget;
        use aruna_core::structs::identity::auth::Actor;
        use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};

        let (_dir, context, record) = device().await;
        let owner = UserId::local(Ulid::generate(), record.realm_id);
        let actor = Actor {
            node_id: node(1),
            user_id: owner,
            realm_id: record.realm_id,
        };
        let mut config = RealmConfigDocument::new(record.realm_id, Vec::new(), 1);
        config.ensure_node(node(1), RealmNodeKind::User { owner });
        let target = DocumentTarget::RealmConfig {
            realm_id: record.realm_id,
        };
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;
        let mut read = replica(&record);
        read.selected = false;
        read.displayed_jsonld = r#"{"@graph":[]}"#.to_string();
        assert!(
            drive(crate::device::replica_store::store_replica(&read), &context)
                .await
                .is_ok()
        );
        let update = |public: Option<bool>| {
            crate::driver::drive(
                RouteUpdateOperation::new(
                    (
                        actor.clone(),
                        context.net_handle.as_ref().map(|net| net.node_id()),
                    ),
                    (None, record.document_id),
                    DocumentUpdate {
                        public,
                        mutation: contact("ada"),
                        expected_revision: None,
                        commit_message: None,
                    },
                    None,
                ),
                &context,
            )
        };

        assert!(update(Some(false)).await.is_err());
        assert!(
            drive(crate::device::sync_status::read_publish_entries(), &context)
                .await
                .unwrap_or_default()
                .is_empty()
        );
        update(None).await.expect("a plain edit queues locally");
        assert_eq!(
            drive(crate::device::sync_status::read_publish_entries(), &context)
                .await
                .unwrap_or_default()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn edits_share_actor() {
        // Reusing the device's actor keeps clocks small; its dots must stay ordered.
        let (_dir, context, record) = device().await;
        let owner = UserId::local(Ulid::generate(), record.realm_id);
        for name in ["ada", "grace"] {
            let edit = apply_local_edit(owner, node(1), &replica(&record), contact(name), 0);
            drive(edit, &context).await.expect("edit applies");
        }
        let batches: Vec<MetadataBatch> =
            drive(crate::device::sync_status::read_publish_entries(), &context)
                .await
                .unwrap_or_default()
                .into_iter()
                .filter_map(|entry| match entry.kind {
                    PublishKind::Edit { batch, .. } => Some(*batch),
                    PublishKind::Create => None,
                })
                .collect();
        let [first, second] = batches.as_slice() else {
            panic!("expected two queued edits, got {batches:?}");
        };
        assert_eq!((first.actor, first.counter), (second.actor, 1));
        assert_eq!(second.counter, 2);
        assert!(second.base_clock.contains(&craqle::Dot {
            actor: craqle::ActorId::from_bytes(first.actor),
            counter: 1,
        }));
        let stored = drive(read_actor(record.document_id), &context)
            .await
            .expect("actor readable")
            .expect("actor stored");
        assert_eq!((stored.actor, stored.counter), (first.actor, 2));
    }

    #[tokio::test]
    async fn queue_preserves_graph() {
        // Planning must not expose an edit that cannot enter the durable queue.
        let (_dir, context, record) = device().await;
        let owner = UserId::local(Ulid::generate(), record.realm_id);
        for _ in 0..MAX_PUBLISH_ENTRIES {
            let entry = PublishEntry::new(
                Ulid::generate(),
                owner,
                record.group_id,
                "queued".to_string(),
                false,
                "{}".to_string(),
            );
            let (key_space, key, value) = publish_entry(&entry).unwrap();
            assert!(matches!(
                context
                    .storage_handle
                    .send_storage_effect(StorageEffect::Write {
                        key_space,
                        key,
                        value,
                        txn_id: None,
                    })
                    .await,
                Event::Storage(StorageEvent::WriteResult { .. })
            ));
        }
        let mut replica = ReplicaRecord::new(
            record.document_id,
            record.group_id,
            record.document_path.clone(),
            ReplicaOrigin::Realm,
        );
        replica.record = Some(Box::new(record.clone()));
        let before = drive(render(&record.graph_iri), &context).await.unwrap();

        assert_eq!(
            drive(
                apply_local_edit(owner, node(1), &replica, contact("ada"), 0,),
                &context,
            )
            .await,
            Err(DeviceEditError::QueueFull {
                limit: MAX_PUBLISH_ENTRIES
            })
        );
        // The refused edit must not consume a dot either.
        assert_eq!(
            drive(read_actor(record.document_id), &context).await,
            Ok(None)
        );
        assert_eq!(
            drive(render(&record.graph_iri), &context).await.unwrap(),
            before
        );
    }

    #[test]
    fn replays_unpublished_edits() {
        // Only an edit still on its way to the realm is local state nothing
        // else restores; a create carries no batch to replay at all.
        assert!(replays_edit(&edit(PublishState::Pending {
            due_at_ms: 0,
            attempts: 0,
            last_error: None,
        })));
        assert!(replays_edit(&edit(PublishState::Publishing {
            document_id: record().document_id,
            due_at_ms: 0,
            attempts: 1,
        })));
        assert!(!replays_edit(&edit(PublishState::Published {
            document_id: record().document_id,
        })));
        assert!(!replays_edit(&edit(PublishState::Failed {
            reason: "denied".to_string(),
            retryable: false,
            document_id: Some(record().document_id),
        })));

        let create = PublishEntry::new(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([3u8; 32])),
            Ulid::from_bytes([1u8; 16]),
            "notes".to_string(),
            false,
            "{}".to_string(),
        );
        assert!(matches!(create.kind, PublishKind::Create));
        assert!(!replays_edit(&create));
    }

    fn editing(with_record: bool) -> ApplyEditOperation {
        let mut read = ReplicaRecord::new(
            record().document_id,
            record().group_id,
            record().document_path,
            ReplicaOrigin::Realm,
        );
        if with_record {
            read.record = Some(Box::new(record()));
        }
        let owner = UserId::local(Ulid::from_bytes([1; 16]), RealmId::from_bytes([3; 32]));
        apply_local_edit(owner, node(1), &read, contact("ada"), 0)
    }

    fn metadata(event: MetadataEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Metadata(event)))
    }

    // Only a replica with a record can take an edit, and it holds the document while it does.
    #[test]
    fn edit_needs_record() {
        use aruna_core::operation::{HoldScope, Operation};
        let mut empty = editing(false);
        assert_eq!(empty.hold_scope(), None);
        assert_eq!(empty.first(), Err(DeviceEditError::NoReplica));
        let held = editing(true);
        let scope = HoldScope::DocumentWrite(record().document_id);
        assert_eq!(held.hold_scope(), Some(scope));
    }

    // A mutation only a holder originates is refused before any read.
    #[test]
    fn holder_mutation_refused() {
        use aruna_core::operation::Operation;
        let mut edit = editing(true);
        edit.authored = None;
        assert!(matches!(edit.first(), Err(DeviceEditError::Invalid(_))));
        assert!(authored_source(contact("ada")).is_some());
    }

    // A planning refusal reaches the owner and queues nothing.
    #[test]
    fn plan_refusal_surfaces() {
        use aruna_core::calls::IoResult as Answer;
        use aruna_core::metadata::MetadataError;
        use aruna_core::operation::Operation;
        let mut edit = editing(true);
        edit.first().unwrap();
        edit.next(Answer::Values(vec![None])).unwrap();
        assert_eq!(edit.step, EditStep::Plan);
        let refused = MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::InvalidInput("bad".to_string()),
        };
        let answer = edit.next(metadata(refused));
        assert_eq!(answer, Err(DeviceEditError::Invalid("bad".to_string())));
    }

    // A refused edit without an actor row changes nothing.
    #[test]
    fn reject_needs_actor() {
        use aruna_core::calls::Next;
        use aruna_core::operation::Operation;
        let mut reject = reject_edit(record().document_id, ([1; 32], 1));
        reject.first().unwrap();
        let answer = reject.next(IoResult::Values(vec![None]));
        assert_eq!(answer, Ok(Next::Done(false)));
    }

    // With no queued edit nothing merges and nothing is flushed.
    #[test]
    fn replay_empty_queue() {
        use aruna_core::calls::{ChildOutcome, Next};
        use aruna_core::operation::Operation;
        let mut replay = replay_queued_edits();
        replay.first().unwrap();
        let none = Ok::<_, aruna_core::calls::IoError>(Vec::<PublishEntry>::new());
        let answer = replay.next(IoResult::Child(ChildOutcome::new(none)));
        assert_eq!(answer, Ok(Next::Done(0)));
    }
}
