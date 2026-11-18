//! Projects logged metadata events into the registry, and drains and replays the event log
//! into that projection.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    COMMIT_MESSAGE_KEYSPACE, DOCUMENT_LIFECYCLE_KEYSPACE, EVENT_LOG_KEYSPACE,
    GRAPH_LIFECYCLE_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE, PENDING_PROJECTION_KEYSPACE,
};
use aruna_core::metadata::{
    GraphLifecycleRecord, MaterializationStatusRecord, MetadataEffect, MetadataError,
    MetadataEvent, MetadataEventPayload, MetadataEventRecord, MetadataLifecycleRecord,
    MetadataMaterializationRecord,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::placement::fence::WriteFence;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::plugin_runs::{DEDUP_RETENTION_MS, SourceEvent, journal_entries, source_key};
use aruna_core::rows::{done, read_row, value};
use aruna_core::storage_entries::{
    delete_projection_entry, document_job_entry, document_lifecycle_entry, document_lifecycle_key,
    event_log_key, graph_lifecycle_key, materialization_job_entry, materialization_status_key,
    pending_projection_key, pending_projection_target, registry_delete_entries,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::storage::metadata_registry::{
    MetadataAuditRecord, MetadataRegistryRecord,
};
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::metadata::call_results::{effect_event, page_rows};
use crate::metadata::graph_deleted::{GraphDeletedError, GraphDeletedOperation};
use crate::metadata::record_reads::DocumentRecordOperation;
use crate::metadata::repository::{
    create_materialization_entries, create_outbox_entries, create_outbox_record,
    registry_outbox_record, schedule_materialization, schedule_projection,
};
use crate::realm::mutate_placement::node_kind;
use crate::sync::outbox_record::schedule_drain_effect;
use crate::sync::shard_placement::sort_node_ids;
use aruna_core::errors::StorageReadError;

const REPLAY_PAGE_SIZE: usize = 1_024;
const PROJECTION_PAGE_SIZE: usize = 256;
pub const PROJECTION_RETRY_AFTER: Duration = Duration::from_secs(5);

/// Conflict resolution is last-writer-wins on wall-clock time, so an event stamped far ahead
/// would win forever. Events beyond the configured skew are deferred until retry; operators must
/// run NTP.
const MAX_SKEW_SECS: u64 = 300;

static CLOCK_SKEW_REJECTIONS: AtomicU64 = AtomicU64::new(0);

/// The largest clock skew a projection accepts, from `MAX_CLOCK_SKEW_SECS`; read by the shells
/// that start a projection.
pub fn max_clock_skew() -> u64 {
    static SKEW_MS: OnceLock<u64> = OnceLock::new();
    *SKEW_MS.get_or_init(|| {
        std::env::var("MAX_CLOCK_SKEW_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(MAX_SKEW_SECS)
            .saturating_mul(1000)
    })
}

fn exceeds_clock_skew(event: &MetadataEventRecord, now_ms: u64, max_skew_ms: u64) -> bool {
    let limit = now_ms.saturating_add(max_skew_ms);
    event.record.updated_at_ms > limit || event.occurred_at_ms > limit
}

#[derive(Debug, Error, PartialEq)]
pub enum MetadataProjectionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("{0}")]
    Conversion(String),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error("metadata create event log record not found for {document_id}/{event_id}")]
    CreateEventMissing { document_id: Ulid, event_id: Ulid },
    #[error("deferred {deferred} metadata create event(s) stamped too far in the future")]
    ClockSkewDeferred { deferred: usize },
    #[error("the document's bucket cut over to a new holder set; retry the projection")]
    PlacementFenced,
    #[error("unexpected event while projecting metadata create event: {0}")]
    UnexpectedEvent(String),
}

impl From<ConversionError> for MetadataProjectionError {
    fn from(error: ConversionError) -> Self {
        Self::Conversion(error.to_string())
    }
}

impl From<StorageReadError> for MetadataProjectionError {
    fn from(error: StorageReadError) -> Self {
        match error {
            StorageReadError::Storage(error) => error.into(),
            StorageReadError::Conversion(error) => error.into(),
        }
    }
}

impl From<IoError> for MetadataProjectionError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            other => Self::UnexpectedEvent(other.to_string()),
        }
    }
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, MetadataProjectionError> {
    Ok(postcard::from_bytes(bytes).map_err(ConversionError::from)?)
}

/// A realm config read result where a missing config is `None`.
fn optional_config(
    result: Result<RealmConfigDocument, GetConfigError>,
) -> Result<Option<RealmConfigDocument>, MetadataProjectionError> {
    match result {
        Ok(config) => Ok(Some(config)),
        Err(GetConfigError::DocumentNotFound) => Ok(None),
        Err(GetConfigError::StorageError(error)) => Err(error.into()),
        Err(GetConfigError::ConversionError(error)) => Err(error.into()),
        Err(other) => Err(MetadataProjectionError::UnexpectedEvent(other.to_string())),
    }
}

/// Sorts the holders an event carries: the create event freezes its origins, so replays never
/// mint new holders.
fn expand_event_holders(mut event: MetadataEventRecord) -> MetadataEventRecord {
    event.record.last_event_id = event.event_id;
    let mut holders = event.record.holder_node_ids.clone();
    sort_node_ids(&mut holders);
    event.record.holder_node_ids = holders;
    event
}

fn audit_record(event: &MetadataEventRecord, message: Option<&str>) -> MetadataAuditRecord {
    let kind = format!(
        "kind={} holders={}",
        event.payload.materialization_kind(),
        event.record.holder_node_ids.len()
    );
    MetadataAuditRecord {
        realm_id: event.record.realm_id,
        group_id: event.record.group_id,
        document_id: event.record.document_id,
        graph_iri: event.record.graph_iri.clone(),
        user_id: event.user_id,
        node_id: event.node_id,
        operation: event.payload.audit_operation(),
        occurred_at_ms: event.occurred_at_ms,
        details: Some(match message {
            Some(message) => format!("{kind}; {message}"),
            None => kind,
        }),
    }
}

fn effect_call(effect: Effect) -> IoCall {
    IoCall::Effect(Box::new(effect))
}

/// Where one event's projection stands; each step waits for the answer of one call.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Step {
    /// Whether the event's graph is deleted.
    Lifecycle,
    /// The registry row a deleted graph's repair removes.
    DeletedRegistry,
    /// The realm config of an event authored here.
    Config,
    Registry,
    Status,
    /// The realm config that says whether this node journals.
    JournalConfig,
    /// Whether this node already journaled the event's source.
    Journaled,
    /// Whether a superseded batch is already in the graph.
    Dot,
    /// Whether a surviving marker means a deferred lifecycle mint.
    Marker,
    /// The author's commit message for the lifecycle outbox.
    OutboxMessage,
    /// The commit message the audit row carries.
    Message,
    /// The replicated lifecycle record that may carry the commit message.
    MessageLifecycle,
}

/// One event in progress with what its steps learned.
#[derive(Debug, PartialEq)]
struct Current {
    event: MetadataEventRecord,
    step: Step,
    registry: Option<MetadataRegistryRecord>,
    needs_materialization: bool,
    needs_projection: bool,
    mint_pending: bool,
    source: Option<SourceEvent>,
    outbox_message: Option<String>,
}

/// A drain the projection wakes after its writes.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Wake {
    Outbox,
    Materialization,
    /// A later pass that retries deferred lifecycle mints.
    Projection,
}

impl Wake {
    fn effect(self) -> Effect {
        match self {
            Wake::Outbox => schedule_drain_effect(),
            Wake::Materialization => schedule_materialization(),
            Wake::Projection => schedule_projection(PROJECTION_RETRY_AFTER),
        }
    }
}

/// How a tail call is answered.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Answer {
    Done,
    Child,
    /// A cache answer; the cache refills from storage when it missed a change.
    Ignored,
    Wake,
}

/// What the projection does after its events: each call is answered in turn.
#[derive(Debug, PartialEq)]
enum Tail {
    /// Removes the rows of documents whose graph is deleted.
    Repair,
    /// Drops one repaired document from the registry cache.
    Uncache(Ulid),
    /// Commits the projection writes in their fenced transaction.
    Commit,
    /// Caches the projected rows.
    Cache,
    /// Wakes a drain; the durable rows cover a failed wake.
    Wake(Wake),
    /// Keeps markers for a later retry.
    Markers(Vec<(Ulid, Ulid)>),
    /// Removes the markers of projected events.
    Unmark,
}

/// Projects logged metadata events into the registry: their registry rows, materialization
/// jobs, outboxes and audit rows, fenced on the graph lifecycle and the bucket fence.
#[derive(Debug, PartialEq)]
pub struct ProjectEventsOperation {
    events: VecDeque<MetadataEventRecord>,
    local_node: Option<NodeId>,
    max_skew_ms: u64,
    now_ms: u64,
    started: bool,
    cache_generation: Option<u64>,
    realm_configs: BTreeMap<RealmId, Option<RealmConfigDocument>>,
    lifecycle_cache: BTreeMap<String, bool>,
    registry_cache: BTreeMap<Ulid, Option<MetadataRegistryRecord>>,
    status_cache: BTreeMap<Ulid, Option<MaterializationStatusRecord>>,
    writes: Vec<(String, ByteView, ByteView)>,
    repair_deletes: Vec<(String, ByteView)>,
    repaired: Vec<Ulid>,
    has_outboxes: bool,
    pending_deletes: BTreeSet<(Ulid, Ulid)>,
    pending_retries: BTreeSet<(Ulid, Ulid)>,
    mint_retries: BTreeSet<(Ulid, Ulid)>,
    needs_materialization_drain: bool,
    projected: usize,
    projected_records: Vec<MetadataRegistryRecord>,
    late_graphs: BTreeSet<String>,
    fence: WriteFence,
    journaled: BTreeSet<String>,
    current: Option<Current>,
    tail: VecDeque<Tail>,
    finishing: Option<Answer>,
    generation_read: bool,
}

type ProjectTurn = Result<Next<usize>, MetadataProjectionError>;

/// What a step decided.
enum Flow {
    Call(IoCall),
    Continue,
    EventDone,
}

impl ProjectEventsOperation {
    /// `max_skew_ms` comes from [`max_clock_skew`]; `local_node` is this node on the network.
    pub fn new(
        events: Vec<MetadataEventRecord>,
        local_node: Option<NodeId>,
        max_skew_ms: u64,
    ) -> Self {
        Self {
            events: events.into(),
            local_node,
            max_skew_ms,
            now_ms: 0,
            started: false,
            cache_generation: None,
            realm_configs: BTreeMap::new(),
            lifecycle_cache: BTreeMap::new(),
            registry_cache: BTreeMap::new(),
            status_cache: BTreeMap::new(),
            writes: Vec::new(),
            repair_deletes: Vec::new(),
            repaired: Vec::new(),
            has_outboxes: false,
            pending_deletes: BTreeSet::new(),
            pending_retries: BTreeSet::new(),
            mint_retries: BTreeSet::new(),
            needs_materialization_drain: false,
            projected: 0,
            projected_records: Vec::new(),
            late_graphs: BTreeSet::new(),
            fence: WriteFence::default(),
            journaled: BTreeSet::new(),
            current: None,
            tail: VecDeque::new(),
            finishing: None,
            generation_read: false,
        }
    }

    fn authored_here(&self, event: &MetadataEventRecord) -> bool {
        self.local_node == Some(event.node_id)
    }

    fn config_for(&self, realm_id: &RealmId) -> Option<&RealmConfigDocument> {
        self.realm_configs.get(realm_id).and_then(Option::as_ref)
    }

    /// Runs the events' steps until one needs a call, then the tail.
    fn advance(&mut self, mut answer: Option<IoResult>) -> ProjectTurn {
        loop {
            if let Some(finishing) = self.finishing.take() {
                self.tail_answered(finishing, answer.take())?;
                return self.next_tail();
            }
            let Some(mut current) = self.current.take() else {
                let Some(event) = self.events.pop_front() else {
                    self.plan_tail();
                    return self.next_tail();
                };
                if let Some(current) = self.begin(event) {
                    self.current = Some(current);
                }
                continue;
            };
            match self.step(&mut current, answer.take())? {
                Flow::Call(call) => {
                    self.current = Some(current);
                    return Ok(Next::Call(call));
                }
                Flow::Continue => self.current = Some(current),
                Flow::EventDone => {}
            }
        }
    }

    /// Starts one event, or defers it when it is stamped too far in the future.
    fn begin(&mut self, event: MetadataEventRecord) -> Option<Current> {
        let document_id = event.record.document_id;
        if exceeds_clock_skew(&event, self.now_ms, self.max_skew_ms) {
            let rejected_total = CLOCK_SKEW_REJECTIONS.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(
                event = "metadata.event.rejected",
                reason = "clock_skew",
                event_id = %event.event_id,
                document_id = %document_id,
                node_id = %event.node_id,
                updated_at_ms = event.record.updated_at_ms,
                occurred_at_ms = event.occurred_at_ms,
                now_ms = self.now_ms,
                rejected_total,
                "Deferring metadata event stamped too far in the future; check NTP on the emitting node"
            );
            self.pending_retries.insert((document_id, event.event_id));
            return None;
        }
        self.pending_deletes.insert((document_id, event.event_id));
        Some(Current {
            event,
            step: Step::Lifecycle,
            registry: None,
            needs_materialization: false,
            needs_projection: false,
            mint_pending: false,
            source: None,
            outbox_message: None,
        })
    }

    fn step(
        &mut self,
        current: &mut Current,
        answer: Option<IoResult>,
    ) -> Result<Flow, MetadataProjectionError> {
        match current.step {
            Step::Lifecycle => self.lifecycle_step(current, answer),
            Step::DeletedRegistry => self.deleted_registry_step(current, answer),
            Step::Config => self.config_step(current, answer),
            Step::Registry => self.registry_step(current, answer),
            Step::Status => self.status_step(current, answer),
            Step::JournalConfig => self.journal_config_step(current, answer),
            Step::Journaled => self.journaled_step(current, answer),
            Step::Dot => self.dot_step(current, answer),
            Step::Marker => self.marker_step(current, answer),
            Step::OutboxMessage => self.outbox_message_step(current, answer),
            Step::Message => self.message_step(current, answer),
            Step::MessageLifecycle => self.message_lifecycle_step(current, answer),
        }
    }

    fn lifecycle_step(
        &mut self,
        current: &mut Current,
        answer: Option<IoResult>,
    ) -> Result<Flow, MetadataProjectionError> {
        let graph_iri = current.event.record.graph_iri.clone();
        let deleted = match (self.lifecycle_cache.get(&graph_iri), answer) {
            (Some(deleted), _) => *deleted,
            (None, None) => {
                return Ok(Flow::Call(IoCall::child(GraphDeletedOperation::new(
                    &graph_iri,
                ))));
            }
            (None, Some(result)) => {
                let deleted = result
                    .child::<bool, GraphDeletedError>()?
                    .map_err(GraphDeletedError::into_error::<MetadataProjectionError>)?;
                self.lifecycle_cache.insert(graph_iri, deleted);
                deleted
            }
        };
        if deleted {
            current.step = Step::DeletedRegistry;
            return Ok(Flow::Continue);
        }
        current.event = expand_event_holders(current.event.clone());
        current.step = Step::Config;
        Ok(Flow::Continue)
    }

    fn deleted_registry_step(
        &mut self,
        current: &mut Current,
        answer: Option<IoResult>,
    ) -> Result<Flow, MetadataProjectionError> {
        let document_id = current.event.record.document_id;
        let existing = match (self.registry_cache.get(&document_id), answer) {
            (Some(record), _) => record.clone(),
            (None, None) => {
                return Ok(Flow::Call(IoCall::child(DocumentRecordOperation::new(
                    document_id,
                ))));
            }
            (None, Some(result)) => {
                result.child::<Option<MetadataRegistryRecord>, StorageReadError>()??
            }
        };
        let stale = existing.unwrap_or_else(|| current.event.record.clone());
        self.repair_deletes.extend(registry_delete_entries(&stale));
        self.repaired.push(stale.document_id);
        self.registry_cache.insert(document_id, None);
        self.status_cache.insert(document_id, None);
        Ok(Flow::EventDone)
    }

    fn config_step(
        &mut self,
        current: &mut Current,
        answer: Option<IoResult>,
    ) -> Result<Flow, MetadataProjectionError> {
        let realm_id = current.event.record.realm_id;
        if self.authored_here(&current.event) && !self.realm_configs.contains_key(&realm_id) {
            let Some(result) = answer else {
                return Ok(Flow::Call(IoCall::child(GetConfigOperation::new(realm_id))));
            };
            let config = optional_config(result.child::<RealmConfigDocument, GetConfigError>()?)?;
            self.realm_configs.insert(realm_id, config);
        }
        current.step = Step::Registry;
        Ok(Flow::Continue)
    }

    fn registry_step(
        &mut self,
        current: &mut Current,
        answer: Option<IoResult>,
    ) -> Result<Flow, MetadataProjectionError> {
        let document_id = current.event.record.document_id;
        let existing = match (self.registry_cache.get(&document_id), answer) {
            (Some(record), _) => record.clone(),
            (None, None) => {
                return Ok(Flow::Call(IoCall::child(DocumentRecordOperation::new(
                    document_id,
                ))));
            }
            (None, Some(result)) => {
                let record =
                    result.child::<Option<MetadataRegistryRecord>, StorageReadError>()??;
                self.registry_cache.insert(document_id, record.clone());
                record
            }
        };
        current.registry = existing;
        current.step = Step::Status;
        Ok(Flow::Continue)
    }

    /// The materialization status tracks the newest event whose materialization was enqueued or
    /// finished, so re-deliveries decide the skip path from storage.
    fn status_step(
        &mut self,
        current: &mut Current,
        answer: Option<IoResult>,
    ) -> Result<Flow, MetadataProjectionError> {
        let document_id = current.event.record.document_id;
        current.needs_materialization = match &current.registry {
            None => true,
            Some(_) => {
                let status = match (self.status_cache.get(&document_id), answer) {
                    (Some(status), _) => status.clone(),
                    (None, None) => {
                        let key = materialization_status_key(document_id);
                        return Ok(Flow::Call(read_row(MATERIALIZATION_STATUS_KEYSPACE, key)));
                    }
                    (None, Some(result)) => {
                        let status = value(result)?
                            .map(|bytes| decode::<MaterializationStatusRecord>(&bytes))
                            .transpose()?;
                        self.status_cache.insert(document_id, status.clone());
                        status
                    }
                };
                status.is_none_or(|status| status.event_id < current.event.event_id)
            }
        };
        // Each infrastructure holder journals a create or change once by its source id, also an
        // older merge, while a receiver could still run it; a late one counts a gap.
        current.source = SourceEvent::from_metadata(&current.event, self.now_ms)
            .filter(|source| self.now_ms <= source.anchor_ms.saturating_add(DEDUP_RETENTION_MS))
            .filter(|source| self.journaled.insert(source.source.clone()));
        current.step = Step::JournalConfig;
        Ok(Flow::Continue)
    }

    /// Servers and management nodes keep a plugin event journal; devices do not, since their
    /// commits are journaled by the holders that accept them.
    fn journal_config_step(
        &mut self,
        current: &mut Current,
        answer: Option<IoResult>,
    ) -> Result<Flow, MetadataProjectionError> {
        let (Some(_), Some(local)) = (current.source.as_ref(), self.local_node) else {
            current.step = Step::Dot;
            return Ok(Flow::Continue);
        };
        let realm_id = current.event.record.realm_id;
        if !self.realm_configs.contains_key(&realm_id) {
            let Some(result) = answer else {
                return Ok(Flow::Call(IoCall::child(GetConfigOperation::new(realm_id))));
            };
            let config = optional_config(result.child::<RealmConfigDocument, GetConfigError>()?)?;
            self.realm_configs.insert(realm_id, config);
        }
        let kind = self
            .config_for(&realm_id)
            .and_then(|config| node_kind(config, local));
        if !matches!(
            kind,
            Some(RealmNodeKind::Server | RealmNodeKind::Management)
        ) {
            current.step = Step::Dot;
            return Ok(Flow::Continue);
        }
        current.step = Step::Journaled;
        let key = current
            .source
            .as_ref()
            .map(|source| source_key(&source.source));
        let key = key.unwrap_or_default();
        Ok(Flow::Call(read_row(
            aruna_core::keyspaces::EVENT_JOURNAL_KEYSPACE,
            key,
        )))
    }

    fn journaled_step(
        &mut self,
        current: &mut Current,
        answer: Option<IoResult>,
    ) -> Result<Flow, MetadataProjectionError> {
        let journaled = match answer {
            Some(result) => value(result)?.is_some(),
            None => true,
        };
        if !journaled && let Some(source) = current.source.as_ref() {
            self.writes.extend(journal_entries(source)?);
            // A skipped projection still fences the batch on the document's lifecycle.
            self.late_graphs
                .insert(current.event.record.graph_iri.clone());
        }
        current.step = Step::Dot;
        Ok(Flow::Continue)
    }

    /// A merge older than the recorded status still has to apply, so it only queues its own
    /// job; an equal status means that event's job is already queued.
    fn dot_step(
        &mut self,
        current: &mut Current,
        answer: Option<IoResult>,
    ) -> Result<Flow, MetadataProjectionError> {
        let document_id = current.event.record.document_id;
        let superseded = (self.status_cache.get(&document_id))
            .and_then(Option::as_ref)
            .is_some_and(|status| status.event_id > current.event.event_id);
        if superseded && let MetadataEventPayload::ApplyBatch { batch, .. } = &current.event.payload
        {
            let Some(result) = answer else {
                return Ok(Flow::Call(effect_call(Effect::Metadata(
                    MetadataEffect::ContainsDot {
                        graph_iri: current.event.record.graph_iri.clone(),
                        actor: batch.actor,
                        counter: batch.counter,
                    },
                ))));
            };
            if !dot_answer(effect_event(result)?)? {
                let job = MetadataMaterializationRecord::new(&current.event, self.now_ms);
                self.writes.push(materialization_job_entry(&job)?);
                self.writes.push(document_job_entry(&job)?);
                self.late_graphs
                    .insert(current.event.record.graph_iri.clone());
                self.needs_materialization_drain = true;
                return Ok(Flow::EventDone);
            }
        }
        let event_is_newer = current.registry.as_ref().is_none_or(|record| {
            (current.event.record.updated_at_ms, current.event.event_id)
                > (record.updated_at_ms, record.last_event_id)
        });
        let holders_changed = (current.registry.as_ref())
            .is_some_and(|record| record.holder_node_ids != current.event.record.holder_node_ids);
        current.needs_projection = current.registry.is_none()
            || event_is_newer
            || holders_changed
            || current.needs_materialization;
        current.step = Step::Marker;
        Ok(Flow::Continue)
    }

    /// A surviving marker on a locally authored event means its one-shot lifecycle mint was
    /// deferred; the skip path must re-attempt it.
    fn marker_step(
        &mut self,
        current: &mut Current,
        answer: Option<IoResult>,
    ) -> Result<Flow, MetadataProjectionError> {
        if !current.needs_projection && self.authored_here(&current.event) {
            let Some(result) = answer else {
                let key = pending_projection_key(
                    current.event.record.document_id,
                    current.event.event_id,
                );
                return Ok(Flow::Call(read_row(PENDING_PROJECTION_KEYSPACE, key)));
            };
            current.mint_pending = value(result)?.is_some();
        }
        if !current.needs_projection && !current.mint_pending {
            return Ok(Flow::EventDone);
        }
        current.step = Step::OutboxMessage;
        Ok(Flow::Continue)
    }

    fn authored_change(&self, current: &Current) -> bool {
        let holders_changed = (current.registry.as_ref())
            .is_some_and(|record| record.holder_node_ids != current.event.record.holder_node_ids);
        self.authored_here(&current.event)
            && (current.registry.is_none()
                || current.needs_materialization
                || holders_changed
                || current.mint_pending)
    }

    fn live_holders(&self, current: &Current) -> bool {
        self.config_for(&current.event.record.realm_id)
            .is_some_and(|config| {
                !resolve_shard_holders(config, &current.event.record.placement).is_empty()
            })
    }

    /// The local node authored this event, so it originates the document's lifecycle sync topic
    /// and may mint its genesis; that outbox carries the author's commit message.
    fn outbox_message_step(
        &mut self,
        current: &mut Current,
        answer: Option<IoResult>,
    ) -> Result<Flow, MetadataProjectionError> {
        if self.authored_change(current) && self.live_holders(current) {
            let Some(result) = answer else {
                return Ok(Flow::Call(author_message_read(&current.event)));
            };
            current.outbox_message = message_value(result)?;
        }
        current.step = Step::Message;
        Ok(Flow::Continue)
    }

    /// The commit message of an event: the author's own row, or on another holder the
    /// replicated lifecycle record when it still carries this event.
    fn message_step(
        &mut self,
        current: &mut Current,
        answer: Option<IoResult>,
    ) -> Result<Flow, MetadataProjectionError> {
        let Some(result) = answer else {
            return Ok(Flow::Call(author_message_read(&current.event)));
        };
        match message_value(result)? {
            Some(message) => self.finish_event(current, Some(message)),
            None => {
                current.step = Step::MessageLifecycle;
                let key = document_lifecycle_key(current.event.record.document_id);
                Ok(Flow::Call(read_row(DOCUMENT_LIFECYCLE_KEYSPACE, key)))
            }
        }
    }

    fn message_lifecycle_step(
        &mut self,
        current: &mut Current,
        answer: Option<IoResult>,
    ) -> Result<Flow, MetadataProjectionError> {
        let lifecycle = match answer {
            Some(result) => value(result)?
                .and_then(|bytes| postcard::from_bytes::<MetadataLifecycleRecord>(&bytes).ok()),
            None => None,
        };
        let message = lifecycle
            .filter(|lifecycle| lifecycle.event_id() == current.event.event_id)
            .and_then(|lifecycle| lifecycle.message().map(str::to_owned));
        self.finish_event(current, message)
    }

    /// Plans the rows of one projected event.
    fn finish_event(
        &mut self,
        current: &mut Current,
        message: Option<String>,
    ) -> Result<Flow, MetadataProjectionError> {
        let event = current.event.clone();
        let document_id = event.record.document_id;
        let realm_id = event.record.realm_id;
        let authored_here = self.authored_change(current);
        let has_live_holders = self.live_holders(current);
        if authored_here && let Some(config) = self.config_for(&realm_id).cloned() {
            self.fence.add(
                realm_id,
                &config,
                [
                    event.record.placement,
                    aruna_core::placement::registry_placement(&config, &event.record),
                ],
            );
        }
        let realm_config = self.config_for(&realm_id).cloned();
        let outbox = if authored_here && has_live_holders {
            Some(
                create_outbox_record(
                    &event,
                    current.outbox_message.as_deref(),
                    realm_config.as_ref(),
                    true,
                )
                .fenced_at(self.fence.generation(&realm_id, &event.record.placement)),
            )
        } else {
            if authored_here {
                // No live holder can accept the genesis yet; keep the marker so a later
                // placement change retries the lifecycle mint.
                self.mint_retries.insert((document_id, event.event_id));
                self.pending_deletes.remove(&(document_id, event.event_id));
            }
            None
        };
        // The registry row rides its own everywhere-bound topic, so it goes out even when the
        // document's own bucket has no holders to publish to.
        let registry_outbox = authored_here
            .then(|| {
                registry_outbox_record(&event, realm_config.as_ref(), true).map(|record| {
                    let generation = self.fence.generation(&realm_id, &record.placement);
                    record.fenced_at(generation)
                })
            })
            .flatten();
        let audit = audit_record(&event, message.as_deref());
        if current.needs_materialization {
            let status = MaterializationStatusRecord::pending(&event, self.now_ms);
            let job = MetadataMaterializationRecord::new(&event, self.now_ms);
            self.writes.extend(create_materialization_entries(
                &event.record,
                &audit,
                event.event_id,
                outbox.as_ref(),
                &status,
                &job,
            )?);
            self.needs_materialization_drain = true;
            self.status_cache.insert(document_id, Some(status));
        } else {
            self.writes.extend(create_outbox_entries(
                &event.record,
                &audit,
                event.event_id,
                outbox.as_ref(),
            )?);
        }
        self.has_outboxes |= outbox.is_some();
        if let Some(registry_outbox) = registry_outbox {
            self.writes.push(
                crate::sync::outbox_record::outbox_write_entry(&registry_outbox)
                    .map_err(ConversionError::from)?,
            );
            self.has_outboxes = true;
        }
        if self.authored_here(&event) {
            self.writes.push(document_lifecycle_entry(
                &MetadataLifecycleRecord::Upsert {
                    event: Box::new(event.clone()),
                },
            )?);
        }
        self.registry_cache
            .insert(document_id, Some(event.record.clone()));
        self.projected_records.push(event.record);
        self.projected = self.projected.saturating_add(1);
        Ok(Flow::EventDone)
    }

    fn plan_tail(&mut self) {
        if !self.repair_deletes.is_empty() {
            self.tail.push_back(Tail::Repair);
            for document_id in std::mem::take(&mut self.repaired) {
                self.tail.push_back(Tail::Uncache(document_id));
            }
        }
        if !self.writes.is_empty() {
            self.tail.push_back(Tail::Commit);
            self.tail.push_back(Tail::Cache);
        }
        if self.has_outboxes {
            self.tail.push_back(Tail::Wake(Wake::Outbox));
        }
        if self.needs_materialization_drain {
            self.tail.push_back(Tail::Wake(Wake::Materialization));
        }
        for markers in [&self.pending_retries, &self.mint_retries] {
            if !markers.is_empty() {
                self.tail
                    .push_back(Tail::Markers(markers.iter().copied().collect()));
            }
        }
        if !self.pending_deletes.is_empty() {
            self.tail.push_back(Tail::Unmark);
        }
        if !self.mint_retries.is_empty() {
            self.tail.push_back(Tail::Wake(Wake::Projection));
        }
    }

    fn next_tail(&mut self) -> ProjectTurn {
        let Some(tail) = self.tail.pop_front() else {
            if !self.pending_retries.is_empty() {
                return Err(MetadataProjectionError::ClockSkewDeferred {
                    deferred: self.pending_retries.len(),
                });
            }
            return Ok(Next::Done(self.projected));
        };
        let (call, answer) = match tail {
            Tail::Repair => (
                IoCall::Delete(std::mem::take(&mut self.repair_deletes)),
                Answer::Done,
            ),
            Tail::Uncache(document_id) => (
                effect_call(Effect::Metadata(MetadataEffect::UncacheRecord {
                    document_id,
                })),
                Answer::Ignored,
            ),
            Tail::Commit => {
                let graph_iris = (self.projected_records.iter())
                    .map(|record| record.graph_iri.clone())
                    .chain(std::mem::take(&mut self.late_graphs))
                    .collect();
                let commit = ProjectionCommitOperation {
                    writes: std::mem::take(&mut self.writes),
                    graph_iris,
                    fence: self.fence.clone(),
                };
                (IoCall::child(commit), Answer::Child)
            }
            Tail::Cache => (
                effect_call(Effect::Metadata(MetadataEffect::CacheRecords {
                    records: std::mem::take(&mut self.projected_records),
                    generation: self.cache_generation,
                })),
                Answer::Ignored,
            ),
            Tail::Wake(wake) => (effect_call(wake.effect()), Answer::Wake),
            Tail::Markers(targets) => {
                let rows = (targets.iter())
                    .map(|(document_id, event_id)| {
                        (
                            PENDING_PROJECTION_KEYSPACE.to_string(),
                            pending_projection_key(*document_id, *event_id),
                            ByteView::from(Vec::new()),
                        )
                    })
                    .collect();
                (IoCall::Write(rows), Answer::Done)
            }
            Tail::Unmark => {
                let deletes = std::mem::take(&mut self.pending_deletes)
                    .into_iter()
                    .map(|(document_id, event_id)| delete_projection_entry(document_id, event_id))
                    .collect();
                (IoCall::Delete(deletes), Answer::Done)
            }
        };
        self.finishing = Some(answer);
        Ok(Next::Call(call))
    }

    fn tail_answered(
        &mut self,
        expected: Answer,
        answer: Option<IoResult>,
    ) -> Result<(), MetadataProjectionError> {
        let Some(result) = answer else {
            return Err(MetadataProjectionError::UnexpectedEvent(
                "no answer".to_string(),
            ));
        };
        match expected {
            Answer::Done => Ok(done(result)?),
            Answer::Child => Ok(result.child::<(), MetadataProjectionError>()??),
            Answer::Ignored => {
                let _ = effect_event(result);
                Ok(())
            }
            Answer::Wake => {
                if let Ok(Event::Task(aruna_core::task::TaskEvent::Error { message, .. })) =
                    effect_event(result)
                {
                    warn!(%message, "Failed to wake a metadata drain after projection");
                }
                Ok(())
            }
        }
    }
}

/// Whether a batch's dot is in the graph; a node without the graph store has nothing to apply.
fn dot_answer(event: Event) -> Result<bool, MetadataProjectionError> {
    match event {
        Event::Metadata(MetadataEvent::ContainsDotResult { contains, .. }) => Ok(contains),
        Event::Metadata(MetadataEvent::Error {
            error: MetadataError::HandleMissing,
            ..
        }) => Ok(true),
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error.into()),
        other => Err(MetadataProjectionError::UnexpectedEvent(
            other.label().to_string(),
        )),
    }
}

fn author_message_read(event: &MetadataEventRecord) -> IoCall {
    let key = event_log_key(event.record.document_id, event.event_id);
    read_row(COMMIT_MESSAGE_KEYSPACE, key)
}

fn message_value(result: IoResult) -> Result<Option<String>, MetadataProjectionError> {
    value(result)?
        .map(|bytes| decode::<String>(&bytes))
        .transpose()
}

impl Operation for ProjectEventsOperation {
    type Output = usize;
    type Error = MetadataProjectionError;

    fn stamp(&mut self, now_ms: u64) {
        if !self.started {
            self.now_ms = now_ms;
        }
    }

    fn first(&mut self) -> ProjectTurn {
        self.started = true;
        if self.events.is_empty() {
            return Ok(Next::Done(0));
        }
        Ok(Next::Call(effect_call(Effect::Metadata(
            MetadataEffect::CacheGeneration,
        ))))
    }

    fn next(&mut self, result: IoResult) -> ProjectTurn {
        if !self.generation_read {
            // The answer to the cache generation read; a node without the cache has none.
            if let Ok(Event::Metadata(MetadataEvent::CacheGeneration { generation })) =
                effect_event(result)
            {
                self.cache_generation = Some(generation);
            }
            self.generation_read = true;
            return self.advance(None);
        }
        self.advance(Some(result))
    }
}

/// Writes a projection's rows in one transaction that reads the graph lifecycle of every
/// projected graph and the bucket fence, so a delete commit or a departing holder's close
/// conflicts the write instead of resurrecting a document.
#[derive(Debug, PartialEq)]
pub struct ProjectionCommitOperation {
    writes: Vec<(String, ByteView, ByteView)>,
    graph_iris: BTreeSet<String>,
    fence: WriteFence,
}

impl Operation for ProjectionCommitOperation {
    type Output = ();
    type Error = MetadataProjectionError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, MetadataProjectionError> {
        if self.graph_iris.is_empty() {
            return Err(MetadataProjectionError::UnexpectedEvent(
                "metadata projection write missing lifecycle fence".to_string(),
            ));
        }
        let mut reads: Vec<(String, Key)> = (self.graph_iris.iter())
            .map(|graph_iri| {
                (
                    GRAPH_LIFECYCLE_KEYSPACE.to_string(),
                    graph_lifecycle_key(graph_iri),
                )
            })
            .collect();
        reads.extend(self.fence.reads());
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, MetadataProjectionError> {
        if self.writes.is_empty() {
            done(result)?;
            return Ok(Next::Done(()));
        }
        let read: Vec<Option<Value>> = aruna_core::rows::all_values(result)?;
        let lifecycle_reads = self.graph_iris.len();
        if read.len() != lifecycle_reads + self.fence.reads().len() {
            return Err(MetadataProjectionError::UnexpectedEvent(
                "a short projection fence read".to_string(),
            ));
        }
        let fence_reads: Vec<(Key, Option<Value>)> = (self.fence.reads().into_iter())
            .map(|(_, key)| key)
            .zip(read[lifecycle_reads..].iter().cloned())
            .collect();
        if !self.fence.admits(&fence_reads) {
            return Err(MetadataProjectionError::PlacementFenced);
        }
        for value in read[..lifecycle_reads].iter().flatten() {
            if decode::<GraphLifecycleRecord>(value)?.is_deleted() {
                return Err(StorageError::TransactionConflict.into());
            }
        }
        Ok(Next::Call(IoCall::Write(std::mem::take(&mut self.writes))))
    }
}

/// Reads logged events by their targets and projects them. A marker whose event is gone is an
/// orphan: a drain deletes it, a direct projection fails.
#[derive(Debug, PartialEq)]
pub struct ProjectLoggedOperation {
    targets: Vec<(Ulid, Ulid)>,
    delete_orphans: bool,
    local_node: Option<NodeId>,
    max_skew_ms: u64,
    stage: LoggedStage,
    existing: usize,
    /// The projection that runs once the orphan markers are gone.
    project: Option<ProjectEventsOperation>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum LoggedStage {
    Events,
    Orphans,
    Project,
}

/// What a logged projection did: the events it projected and the events it found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoggedProjection {
    pub projected: usize,
    pub existing: usize,
}

type LoggedTurn = Result<Next<LoggedProjection>, MetadataProjectionError>;

impl ProjectLoggedOperation {
    pub fn new(
        targets: impl IntoIterator<Item = (Ulid, Ulid)>,
        delete_orphans: bool,
        local_node: Option<NodeId>,
        max_skew_ms: u64,
    ) -> Self {
        let mut seen = BTreeSet::new();
        let targets = targets
            .into_iter()
            .filter(|target| seen.insert(*target))
            .collect();
        Self {
            targets,
            delete_orphans,
            local_node,
            max_skew_ms,
            stage: LoggedStage::Events,
            existing: 0,
            project: None,
        }
    }

    fn events_read(&mut self, result: IoResult) -> LoggedTurn {
        let read = aruna_core::rows::all_values(result)?;
        let mut events = Vec::new();
        let mut orphans = Vec::new();
        for ((document_id, event_id), value) in self.targets.iter().copied().zip(read) {
            let Some(value) = value else {
                if !self.delete_orphans {
                    return Err(MetadataProjectionError::CreateEventMissing {
                        document_id,
                        event_id,
                    });
                }
                warn!(%document_id, %event_id, "Deleting orphan metadata pending projection marker");
                orphans.push(delete_projection_entry(document_id, event_id));
                continue;
            };
            let event: MetadataEventRecord = decode(&value)?;
            if event.record.document_id != document_id || event.event_id != event_id {
                return Err(MetadataProjectionError::UnexpectedEvent(format!(
                    "metadata create event log target {document_id}/{event_id} did not match payload {}/{}",
                    event.record.document_id, event.event_id
                )));
            }
            events.push(event);
        }
        self.existing = events.len();
        let project = ProjectEventsOperation::new(events, self.local_node, self.max_skew_ms);
        if orphans.is_empty() {
            self.stage = LoggedStage::Project;
            return Ok(Next::Call(IoCall::child(project)));
        }
        self.project = Some(project);
        self.stage = LoggedStage::Orphans;
        Ok(Next::Call(IoCall::Delete(orphans)))
    }
}

impl Operation for ProjectLoggedOperation {
    type Output = LoggedProjection;
    type Error = MetadataProjectionError;

    fn first(&mut self) -> LoggedTurn {
        if self.targets.is_empty() {
            return Ok(Next::Done(LoggedProjection {
                projected: 0,
                existing: 0,
            }));
        }
        let reads = (self.targets.iter())
            .map(|(document_id, event_id)| {
                (
                    EVENT_LOG_KEYSPACE.to_string(),
                    event_log_key(*document_id, *event_id),
                )
            })
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> LoggedTurn {
        match self.stage {
            LoggedStage::Events => self.events_read(result),
            LoggedStage::Orphans => {
                done(result)?;
                let project = self.project.take().ok_or_else(|| {
                    MetadataProjectionError::UnexpectedEvent("the projection is missing".into())
                })?;
                self.stage = LoggedStage::Project;
                Ok(Next::Call(IoCall::child(project)))
            }
            LoggedStage::Project => {
                let projected = result.child::<usize, MetadataProjectionError>()??;
                Ok(Next::Done(LoggedProjection {
                    projected,
                    existing: self.existing,
                }))
            }
        }
    }
}

/// One drain pass of the pending projection queue: projects the events of the first page of
/// markers and deletes malformed and orphan markers.
#[derive(Debug, PartialEq)]
pub struct DrainProjectionOperation {
    local_node: Option<NodeId>,
    max_skew_ms: u64,
    stage: DrainStage,
    has_more: bool,
}

#[derive(Debug, PartialEq)]
enum DrainStage {
    Page,
    Malformed(Vec<(Ulid, Ulid)>),
    Project,
}

/// What one drain pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingProjectionResult {
    pub markers_examined: usize,
    pub projected: usize,
    pub has_more: bool,
}

type DrainTurn = Result<Next<PendingProjectionResult>, MetadataProjectionError>;

impl DrainProjectionOperation {
    pub fn new(local_node: Option<NodeId>, max_skew_ms: u64) -> Self {
        Self {
            local_node,
            max_skew_ms,
            stage: DrainStage::Page,
            has_more: false,
        }
    }

    fn project(&mut self, targets: Vec<(Ulid, Ulid)>) -> DrainTurn {
        self.stage = DrainStage::Project;
        let logged = ProjectLoggedOperation::new(targets, true, self.local_node, self.max_skew_ms);
        Ok(Next::Call(IoCall::child(logged)))
    }
}

impl Operation for DrainProjectionOperation {
    type Output = PendingProjectionResult;
    type Error = MetadataProjectionError;

    fn first(&mut self) -> DrainTurn {
        Ok(Next::Call(IoCall::Page {
            space: PENDING_PROJECTION_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: PROJECTION_PAGE_SIZE,
        }))
    }

    fn next(&mut self, result: IoResult) -> DrainTurn {
        match std::mem::replace(&mut self.stage, DrainStage::Project) {
            DrainStage::Page => {
                let (rows, next) = page_rows(result)?;
                self.has_more = next.is_some();
                let mut targets = Vec::with_capacity(rows.len());
                let mut malformed = Vec::new();
                for (key, _) in rows {
                    match pending_projection_target(key.as_ref()) {
                        Some(target) => targets.push(target),
                        None => {
                            warn!(key = ?key.to_vec(), "Deleting malformed metadata pending projection marker");
                            malformed.push((PENDING_PROJECTION_KEYSPACE.to_string(), key));
                        }
                    }
                }
                if malformed.is_empty() {
                    return self.project(targets);
                }
                self.stage = DrainStage::Malformed(targets);
                Ok(Next::Call(IoCall::Delete(malformed)))
            }
            DrainStage::Malformed(targets) => {
                done(result)?;
                self.project(targets)
            }
            DrainStage::Project => {
                let logged = result.child::<LoggedProjection, MetadataProjectionError>()??;
                Ok(Next::Done(PendingProjectionResult {
                    markers_examined: logged.existing,
                    projected: logged.projected,
                    has_more: self.has_more,
                }))
            }
        }
    }
}

/// Replays one page of the event log into the projection; answers what it projected and where
/// the next page starts, so a shell can stop at a page boundary.
#[derive(Debug, PartialEq)]
pub struct ReplayPageOperation {
    after: Option<Key>,
    local_node: Option<NodeId>,
    max_skew_ms: u64,
    next: Option<Key>,
    projecting: bool,
}

impl ReplayPageOperation {
    pub fn new(after: Option<Key>, local_node: Option<NodeId>, max_skew_ms: u64) -> Self {
        Self {
            after,
            local_node,
            max_skew_ms,
            next: None,
            projecting: false,
        }
    }
}

impl Operation for ReplayPageOperation {
    type Output = (usize, Option<Key>);
    type Error = MetadataProjectionError;

    fn first(&mut self) -> Result<Next<Self::Output>, MetadataProjectionError> {
        Ok(Next::Call(IoCall::Page {
            space: EVENT_LOG_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.after.clone(),
            limit: REPLAY_PAGE_SIZE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, MetadataProjectionError> {
        if self.projecting {
            let projected = result.child::<usize, MetadataProjectionError>()??;
            return Ok(Next::Done((projected, self.next.take())));
        }
        let (rows, next) = page_rows(result)?;
        self.next = next;
        let events = (rows.iter())
            .map(|(_, value)| decode::<MetadataEventRecord>(value))
            .collect::<Result<Vec<_>, _>>()?;
        self.projecting = true;
        let project = ProjectEventsOperation::new(events, self.local_node, self.max_skew_ms);
        Ok(Next::Call(IoCall::child(project)))
    }
}

/// Whether a batch's dot is in the local graph; a node without the graph store has nothing to
/// apply, and a deleted graph applies nothing more.
#[derive(Debug, PartialEq)]
pub struct DotAppliedOperation {
    graph_iri: String,
    actor: [u8; 32],
    counter: u64,
}

impl DotAppliedOperation {
    pub fn new(graph_iri: &str, batch: &aruna_core::metadata::MetadataBatch) -> Self {
        Self {
            graph_iri: graph_iri.to_string(),
            actor: batch.actor,
            counter: batch.counter,
        }
    }
}

impl Operation for DotAppliedOperation {
    type Output = bool;
    type Error = MetadataProjectionError;

    fn first(&mut self) -> Result<Next<bool>, MetadataProjectionError> {
        Ok(Next::Call(effect_call(Effect::Metadata(
            MetadataEffect::ContainsDot {
                graph_iri: self.graph_iri.clone(),
                actor: self.actor,
                counter: self.counter,
            },
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, MetadataProjectionError> {
        Ok(Next::Done(dot_answer(effect_event(result)?)?))
    }
}

/// Wakes the projection drain after `after`; the runner persists the timer first.
#[derive(Debug, PartialEq)]
pub struct ProjectionWakeOperation {
    after: Duration,
}

impl ProjectionWakeOperation {
    pub fn new(after: Duration) -> Self {
        Self { after }
    }
}

impl Operation for ProjectionWakeOperation {
    type Output = ();
    type Error = MetadataProjectionError;

    fn first(&mut self) -> Result<Next<()>, MetadataProjectionError> {
        Ok(Next::Call(effect_call(schedule_projection(self.after))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, MetadataProjectionError> {
        match effect_event(result)? {
            Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) => {
                Err(MetadataProjectionError::UnexpectedEvent(message))
            }
            _ => Ok(Next::Done(())),
        }
    }
}

/// Arms the projection drain at start when a pending marker survived the restart.
#[derive(Debug, PartialEq)]
pub struct ProjectionTimerOperation {
    waking: bool,
}

impl ProjectionTimerOperation {
    pub fn new() -> Self {
        Self { waking: false }
    }
}

impl Default for ProjectionTimerOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation for ProjectionTimerOperation {
    type Output = ();
    type Error = MetadataProjectionError;

    fn first(&mut self) -> Result<Next<()>, MetadataProjectionError> {
        Ok(Next::Call(IoCall::Page {
            space: PENDING_PROJECTION_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: 1,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, MetadataProjectionError> {
        if self.waking {
            if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) =
                effect_event(result)?
            {
                warn!(message = %message, "Failed to restore metadata projection timer");
            }
            return Ok(Next::Done(()));
        }
        let (rows, _) = page_rows(result)?;
        if rows.is_empty() {
            return Ok(Next::Done(()));
        }
        self.waking = true;
        Ok(Next::Call(effect_call(schedule_projection(Duration::ZERO))))
    }
}

#[cfg(test)]
#[path = "projection_tests.rs"]
mod tests;
