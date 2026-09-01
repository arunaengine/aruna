use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    METADATA_DOCUMENT_LIFECYCLE_KEYSPACE, METADATA_EVENT_LOG_KEYSPACE,
    METADATA_GRAPH_LIFECYCLE_KEYSPACE, METADATA_RAW_REVISION_KEYSPACE,
};
use aruna_core::metadata::{
    METADATA_RAW_BYTES_LIMIT, METADATA_RAW_EVENT_LIMIT, MetadataCreateEventPayload,
    MetadataCreateEventRecord, MetadataDocumentLifecycleRecord, MetadataError,
    MetadataGraphLifecycleRecord, MetadataMaterializationState, MetadataMergedRevision,
    MetadataRawRevision, apply_raw_upsert, raw_context_digest, raw_upsert_entity,
    resolve_raw_revision,
};
use aruna_core::storage_entries::{
    metadata_document_lifecycle_key, metadata_event_log_key, metadata_event_log_prefix,
    metadata_graph_lifecycle_key, metadata_pending_projection_target, raw_revision_key,
};
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::types::{Key, TxnId};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::repository::{
    parse_materialization_status_read, read_materialization_status_effect,
};

const RAW_PAGE_SIZE: usize = 1;
const RAW_EVENT_LIMIT: usize = METADATA_RAW_EVENT_LIMIT as usize;
const RAW_ENCODED_LIMIT: usize = METADATA_RAW_BYTES_LIMIT as usize;

#[derive(Debug, Default)]
struct RawLoadBudget {
    events: usize,
    encoded_bytes: usize,
}

impl RawLoadBudget {
    fn inspect(&mut self, encoded_bytes: usize) -> Result<(), MetadataRawReadError> {
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| budget_error("encoded bytes overflow"))?;
        if self.encoded_bytes > RAW_ENCODED_LIMIT {
            return Err(budget_error("encoded byte limit exceeded"));
        }
        Ok(())
    }

    fn accept(&mut self) -> Result<(), MetadataRawReadError> {
        self.events = self
            .events
            .checked_add(1)
            .ok_or_else(|| budget_error("event count overflow"))?;
        if self.events > RAW_EVENT_LIMIT {
            return Err(budget_error("event count limit exceeded"));
        }
        Ok(())
    }
}

fn budget_error(message: &str) -> MetadataRawReadError {
    MetadataRawReadError::LimitExceeded(message.to_string())
}

fn event_size(event: &MetadataCreateEventRecord) -> Result<usize, MetadataRawReadError> {
    Ok(postcard::experimental::serialized_size(event).map_err(ConversionError::from)?)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct RawBaseIdentity {
    updated_at_ms: u64,
    event_id: Ulid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RawRevisionState {
    base: Option<RawBaseIdentity>,
    last_event_id: Ulid,
    revision: Option<MetadataRawRevision>,
    /// Set once a batch has merged into the graph. From then on the rendered
    /// graph owns this document's revision and no text event rebuilds it.
    merged: bool,
}

#[derive(Debug, Default)]
pub(crate) struct RawStateCache {
    loaded: bool,
    state: Option<RawRevisionState>,
}

pub(crate) struct RawEventPlan {
    pub(crate) revision: Option<MetadataRawRevision>,
    pub(crate) rebuild: bool,
    pub(crate) state_write: (String, ByteView, ByteView),
    state: RawRevisionState,
}

impl RawEventPlan {
    pub(crate) fn cache(&self, cache: &mut RawStateCache) {
        cache.loaded = true;
        cache.state = Some(self.state.clone());
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataRawView {
    pub revision: MetadataRawRevision,
    pub projection_state: MetadataMaterializationState,
    pub projected_event_id: Option<Ulid>,
}

#[derive(Debug, Error)]
pub enum MetadataRawReadError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error("unexpected metadata raw-read event: {0}")]
    UnexpectedEvent(String),
    #[error("metadata raw-read limit exceeded: {0}")]
    LimitExceeded(String),
    /// The event log itself is inconsistent, so retrying the same read cannot
    /// help. Kept apart from [`Self::UnexpectedEvent`], which is adapter noise.
    #[error("inconsistent metadata raw event log: {0}")]
    InconsistentLog(String),
}

pub async fn load_raw_view(
    context: &DriverContext,
    document_id: Ulid,
    txn_id: Option<TxnId>,
) -> Result<Option<MetadataRawView>, MetadataRawReadError> {
    let status = parse_materialization_status_read(
        context
            .storage_handle
            .send_effect(read_materialization_status_effect(document_id, txn_id))
            .await,
    )
    .map_err(|error| match error {
        crate::metadata::repository::StorageReadError::Storage(error) => {
            MetadataRawReadError::Storage(error)
        }
        crate::metadata::repository::StorageReadError::Conversion(error) => {
            MetadataRawReadError::Conversion(error)
        }
    })?;
    let revision = load_raw_revision(context, document_id, txn_id).await?;
    let Some(revision) = revision else {
        return Ok(None);
    };
    let projection_state = status
        .as_ref()
        .map(|status| status.state)
        .unwrap_or(MetadataMaterializationState::Pending);
    let projected_event_id = status
        .filter(|status| {
            status.state == MetadataMaterializationState::Materialized
                && status.event_id == revision.winning_event_id
                && status.context_digest == Some(revision.context_digest)
                && status.dataset_digest == revision.dataset_digest
        })
        .map(|status| status.event_id);
    Ok(Some(MetadataRawView {
        revision,
        projection_state,
        projected_event_id,
    }))
}

pub async fn load_raw_revision(
    context: &DriverContext,
    document_id: Ulid,
    txn_id: Option<TxnId>,
) -> Result<Option<MetadataRawRevision>, MetadataRawReadError> {
    if raw_deleted(context, document_id, txn_id).await? {
        return Ok(None);
    }
    // Once a batch has merged, the rendered graph is the document's revision;
    // the authored-text replay only serves a document until then.
    if let Some(state) = read_raw_state(context, document_id, txn_id).await?
        && state.merged
    {
        return Ok(state.revision);
    }
    let events = load_raw_events(
        context,
        document_id,
        None,
        None,
        txn_id,
        RawLoadBudget::default(),
    )
    .await?;
    resolve_raw_revision(&events).map_err(Into::into)
}

/// Digest of the displayed revision, read without replaying the event log.
pub(crate) async fn load_raw_digest(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<Option<[u8; 32]>, MetadataRawReadError> {
    Ok(read_raw_state(context, document_id, None)
        .await?
        .and_then(|state| state.revision)
        .and_then(|revision| revision.dataset_digest))
}

async fn raw_deleted(
    context: &DriverContext,
    document_id: Ulid,
    txn_id: Option<TxnId>,
) -> Result<bool, MetadataRawReadError> {
    let document = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
            key: metadata_document_lifecycle_key(document_id),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| {
                postcard::from_bytes::<MetadataDocumentLifecycleRecord>(&value)
                    .map(|record| matches!(record, MetadataDocumentLifecycleRecord::Delete { .. }))
                    .map_err(ConversionError::from)
            })
            .transpose()?
            .unwrap_or(false),
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => return Err(MetadataRawReadError::UnexpectedEvent(format!("{other:?}"))),
    };
    if document {
        return Ok(true);
    }
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            key: metadata_graph_lifecycle_key(&MetadataRegistryRecord::graph_iri_for(document_id)),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| {
                postcard::from_bytes::<MetadataGraphLifecycleRecord>(&value)
                    .map(|record| record.is_deleted())
                    .map_err(ConversionError::from)
            })
            .transpose()
            .map(|deleted| deleted.unwrap_or(false))
            .map_err(MetadataRawReadError::from),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataRawReadError::UnexpectedEvent(format!("{other:?}"))),
    }
}

pub(crate) async fn prepare_raw_event(
    context: &DriverContext,
    event: &MetadataCreateEventRecord,
    cache: &mut RawStateCache,
) -> Result<RawEventPlan, MetadataRawReadError> {
    if !cache.loaded {
        cache.state = read_raw_state(context, event.record.document_id, None).await?;
        cache.loaded = true;
    }
    let (state, rebuild) = match cache.state.as_ref() {
        // A merged document's revision comes from the graph, so a replayed text
        // event must neither rebuild it nor overwrite it.
        Some(state) if state.merged => (state.clone(), false),
        Some(state) if event.event_id > state.last_event_id => {
            let events = load_raw_events(
                context,
                event.record.document_id,
                Some(state.last_event_id),
                Some(event.event_id),
                None,
                RawLoadBudget::default(),
            )
            .await?;
            if !events
                .iter()
                .any(|candidate| candidate.event_id == event.event_id)
            {
                return Err(MetadataRawReadError::InconsistentLog(format!(
                    "raw event log is missing {}",
                    event.event_id
                )));
            }
            let (next, base_changed) = advance_raw_state(state.clone(), &events)?;
            let simple = events.len() == 1 && !base_changed;
            let rebuild = if simple {
                !event_matches_state(event, &next)
            } else {
                next.revision.is_some()
            };
            (next, rebuild)
        }
        Some(state) => {
            let next = rebuild_raw_state(context, state, event).await?;
            let rebuild = next.revision.is_some();
            (next, rebuild)
        }
        None => initial_raw_state(context, event).await?,
    };
    let state_write = (
        METADATA_RAW_REVISION_KEYSPACE.to_string(),
        raw_revision_key(event.record.document_id),
        ByteView::from(postcard::to_allocvec(&state).map_err(ConversionError::from)?),
    );
    Ok(RawEventPlan {
        revision: state.revision.clone(),
        rebuild,
        state_write,
        state,
    })
}

/// Raw state after `event`'s batch merged and the graph re-rendered.
///
/// Out-of-order arrival needs no replay: the OR-Set already converged, so the
/// render describes every batch this node has merged.
pub(crate) async fn prepare_merged_event(
    context: &DriverContext,
    event: &MetadataCreateEventRecord,
    render: String,
    findings: u32,
    cache: &mut RawStateCache,
) -> Result<RawEventPlan, MetadataRawReadError> {
    if !cache.loaded {
        cache.state = read_raw_state(context, event.record.document_id, None).await?;
        cache.loaded = true;
    }
    let previous = cache.state.clone().unwrap_or(RawRevisionState {
        base: None,
        last_event_id: event.event_id,
        revision: None,
        merged: true,
    });
    let state = merged_raw_state(previous, event, render, findings)?;
    let state_write = (
        METADATA_RAW_REVISION_KEYSPACE.to_string(),
        raw_revision_key(event.record.document_id),
        ByteView::from(postcard::to_allocvec(&state).map_err(ConversionError::from)?),
    );
    Ok(RawEventPlan {
        revision: state.revision.clone(),
        rebuild: false,
        state_write,
        state,
    })
}

/// Only a valid render is displayed: an invalid one is flagged beside the last
/// valid revision so the editor can open it and the export stays sound.
fn merged_raw_state(
    mut state: RawRevisionState,
    event: &MetadataCreateEventRecord,
    render: String,
    findings: u32,
) -> Result<RawRevisionState, MetadataError> {
    state.merged = true;
    state.last_event_id = state.last_event_id.max(event.event_id);
    match state.revision.as_mut() {
        Some(revision) if findings > 0 => {
            revision.merged = Some(MetadataMergedRevision {
                jsonld: render,
                findings,
            });
        }
        _ => state.revision = Some(rendered_revision(render, state.last_event_id)?),
    }
    Ok(state)
}

fn rendered_revision(
    jsonld: String,
    winning_event_id: Ulid,
) -> Result<MetadataRawRevision, MetadataError> {
    let context_digest = raw_context_digest(&jsonld)?;
    let dataset_digest = craqle::canonicalize_jsonld(&jsonld)
        .ok()
        .map(|canonical| canonical.digest);
    Ok(MetadataRawRevision {
        jsonld,
        winning_event_id,
        context_digest,
        dataset_digest,
        merged: None,
    })
}

async fn initial_raw_state(
    context: &DriverContext,
    event: &MetadataCreateEventRecord,
) -> Result<(RawRevisionState, bool), MetadataRawReadError> {
    let events = match &event.payload {
        MetadataCreateEventPayload::Scaffold { .. }
        | MetadataCreateEventPayload::RoCrate { .. } => {
            let mut budget = RawLoadBudget::default();
            budget.inspect(event_size(event)?)?;
            budget.accept()?;
            vec![event.clone()]
        }
        _ => {
            load_raw_events(
                context,
                event.record.document_id,
                None,
                Some(event.event_id),
                None,
                RawLoadBudget::default(),
            )
            .await?
        }
    };
    let state = resolve_raw_state(&events)?.ok_or_else(|| {
        MetadataRawReadError::InconsistentLog(format!(
            "raw event log is missing {}",
            event.event_id
        ))
    })?;
    let rebuild = state.revision.is_some() && !event_matches_state(event, &state);
    Ok((state, rebuild))
}

async fn rebuild_raw_state(
    context: &DriverContext,
    state: &RawRevisionState,
    event: &MetadataCreateEventRecord,
) -> Result<RawRevisionState, MetadataRawReadError> {
    let incoming_base = raw_base_identity(event);
    let Some(start_event_id) = state
        .base
        .map(|base| base.event_id)
        .into_iter()
        .chain(incoming_base.map(|base| base.event_id))
        .min()
    else {
        return Ok(state.clone());
    };
    let end_event_id = state.last_event_id.max(event.event_id);
    let first = read_raw_event(context, event.record.document_id, start_event_id).await?;
    let mut budget = RawLoadBudget::default();
    budget.inspect(event_size(&first)?)?;
    budget.accept()?;
    let mut events = vec![first];
    events.extend(
        load_raw_events(
            context,
            event.record.document_id,
            Some(start_event_id),
            Some(end_event_id),
            None,
            budget,
        )
        .await?,
    );
    resolve_raw_state(&events)?.ok_or_else(|| {
        MetadataRawReadError::InconsistentLog(format!(
            "raw base event is missing for {}",
            event.record.document_id
        ))
    })
}

fn advance_raw_state(
    mut state: RawRevisionState,
    events: &[MetadataCreateEventRecord],
) -> Result<(RawRevisionState, bool), MetadataError> {
    let next_base = events
        .iter()
        .filter_map(raw_base_identity)
        .chain(state.base)
        .max();
    let base_changed = next_base != state.base;
    if base_changed {
        state.revision = resolve_raw_revision(events)?;
        state.base = next_base;
    } else if let (Some(base), Some(revision)) = (state.base, state.revision.as_mut()) {
        let mut document: serde_json::Value = serde_json::from_str(&revision.jsonld)
            .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
        let mut changed = false;
        for event in events {
            if event.event_id <= base.event_id {
                continue;
            }
            let Some((jsonld, link_root)) = raw_upsert_entity(&event.payload) else {
                continue;
            };
            apply_raw_upsert(&mut document, jsonld, link_root)?;
            revision.winning_event_id = event.event_id;
            changed = true;
        }
        if changed {
            revision.jsonld = serde_json::to_string(&document)
                .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
            revision.dataset_digest = craqle::canonicalize_jsonld(&revision.jsonld)
                .ok()
                .map(|canonical| canonical.digest);
        }
    }
    if let Some(last_event_id) = events.iter().map(|event| event.event_id).max() {
        state.last_event_id = state.last_event_id.max(last_event_id);
    }
    Ok((state, base_changed))
}

fn resolve_raw_state(
    events: &[MetadataCreateEventRecord],
) -> Result<Option<RawRevisionState>, MetadataError> {
    let Some(last_event_id) = events.iter().map(|event| event.event_id).max() else {
        return Ok(None);
    };
    let base = events.iter().filter_map(raw_base_identity).max();
    let revision = resolve_raw_revision(events)?;
    Ok(Some(RawRevisionState {
        base,
        last_event_id,
        revision,
        merged: false,
    }))
}

fn raw_base_identity(event: &MetadataCreateEventRecord) -> Option<RawBaseIdentity> {
    matches!(
        &event.payload,
        MetadataCreateEventPayload::RoCrate { .. }
            | MetadataCreateEventPayload::ReplaceRoCrate { .. }
    )
    .then_some(RawBaseIdentity {
        updated_at_ms: event.record.updated_at_ms,
        event_id: event.event_id,
    })
}

fn event_matches_state(event: &MetadataCreateEventRecord, state: &RawRevisionState) -> bool {
    match &event.payload {
        MetadataCreateEventPayload::Scaffold { .. } => state.revision.is_none(),
        MetadataCreateEventPayload::RoCrate { .. }
        | MetadataCreateEventPayload::ReplaceRoCrate { .. } => {
            state.base == raw_base_identity(event)
                && state
                    .revision
                    .as_ref()
                    .is_some_and(|revision| revision.winning_event_id == event.event_id)
        }
        MetadataCreateEventPayload::UpsertDataEntity { .. }
        | MetadataCreateEventPayload::UpsertContextualEntity { .. }
        | MetadataCreateEventPayload::ApplyBatch { .. } => true,
    }
}

async fn read_raw_state(
    context: &DriverContext,
    document_id: Ulid,
    txn_id: Option<TxnId>,
) -> Result<Option<RawRevisionState>, MetadataRawReadError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_RAW_REVISION_KEYSPACE.to_string(),
            key: raw_revision_key(document_id),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => Ok(Some(
            postcard::from_bytes(&value).map_err(ConversionError::from)?,
        )),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataRawReadError::UnexpectedEvent(format!("{other:?}"))),
    }
}

async fn read_raw_event(
    context: &DriverContext,
    document_id: Ulid,
    event_id: Ulid,
) -> Result<MetadataCreateEventRecord, MetadataRawReadError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_EVENT_LOG_KEYSPACE.to_string(),
            key: metadata_event_log_key(document_id, event_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            if value.len() > RAW_ENCODED_LIMIT {
                return Err(budget_error("encoded byte limit exceeded"));
            }
            let event: MetadataCreateEventRecord =
                postcard::from_bytes(&value).map_err(ConversionError::from)?;
            validate_raw_entry(
                document_id,
                &metadata_event_log_key(document_id, event_id),
                &event,
            )?;
            Ok(event)
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Err(
            MetadataRawReadError::InconsistentLog(format!("raw event {event_id} is missing")),
        ),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataRawReadError::UnexpectedEvent(format!("{other:?}"))),
    }
}

async fn load_raw_events(
    context: &DriverContext,
    document_id: Ulid,
    start_after: Option<Ulid>,
    event_cursor: Option<Ulid>,
    txn_id: Option<TxnId>,
    mut budget: RawLoadBudget,
) -> Result<Vec<MetadataCreateEventRecord>, MetadataRawReadError> {
    let prefix = metadata_event_log_prefix(document_id);
    let mut start: Option<Key> =
        start_after.map(|event_id| metadata_event_log_key(document_id, event_id));
    let mut events = Vec::new();
    loop {
        let remaining = RAW_EVENT_LIMIT.saturating_sub(budget.events);
        if remaining == 0 {
            return Err(budget_error("event count limit exceeded"));
        }
        let page_limit = remaining.min(RAW_PAGE_SIZE);
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_EVENT_LOG_KEYSPACE.to_string(),
                prefix: Some(prefix.clone()),
                start: start.take().map(IterStart::After),
                limit: page_limit,
                txn_id,
            })
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(MetadataRawReadError::UnexpectedEvent(format!("{other:?}")));
            }
        };
        if values.len() > page_limit {
            return Err(budget_error("page size limit exceeded"));
        }
        if values.is_empty() && next_start_after.is_some() {
            return Err(budget_error("empty page has continuation"));
        }
        let mut reached_cursor = false;
        for (key, value) in values {
            budget.inspect(value.len())?;
            let event: MetadataCreateEventRecord =
                postcard::from_bytes(&value).map_err(ConversionError::from)?;
            validate_raw_entry(document_id, &key, &event)?;
            if event_cursor.is_some_and(|cursor| event.event_id > cursor) {
                reached_cursor = true;
                break;
            }
            budget.accept()?;
            events.push(event);
        }
        if reached_cursor {
            break;
        }
        let Some(next) = next_start_after else {
            break;
        };
        start = Some(next);
    }
    Ok(events)
}

fn validate_raw_entry(
    document_id: Ulid,
    key: &[u8],
    event: &MetadataCreateEventRecord,
) -> Result<(), MetadataRawReadError> {
    let Some((key_document_id, key_event_id)) = metadata_pending_projection_target(key) else {
        return Err(MetadataRawReadError::InconsistentLog(
            "raw event key has invalid identity".to_string(),
        ));
    };
    if key_document_id != document_id {
        return Err(MetadataRawReadError::InconsistentLog(format!(
            "raw event key belongs to document {key_document_id}"
        )));
    }
    if event.record.document_id != document_id {
        return Err(MetadataRawReadError::InconsistentLog(format!(
            "raw event belongs to document {}",
            event.record.document_id
        )));
    }
    if event.event_id != key_event_id {
        return Err(MetadataRawReadError::InconsistentLog(format!(
            "raw event key mismatch for {document_id}/{key_event_id}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::storage_entries::metadata_create_event_write_entry;
    use aruna_core::structs::{MetadataRegistryRecord, PlacementRef, RealmId};
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    fn test_event(event_id: Ulid, updated_at_ms: u64) -> MetadataCreateEventRecord {
        let realm_id = RealmId::from_bytes([1; 32]);
        let group_id = Ulid::from_parts(1, 1);
        let document_id = Ulid::from_parts(1, 2);
        let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
        MetadataCreateEventRecord {
            event_id,
            record: MetadataRegistryRecord {
                realm_id,
                group_id,
                document_id,
                document_path: "datasets/raw".to_string(),
                graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
                public: true,
                permission_path: MetadataRegistryRecord::permission_path_for(
                    &realm_id,
                    group_id,
                    "datasets/raw",
                    document_id,
                ),
                placement: PlacementRef::NIL,
                holder_node_ids: vec![node_id],
                created_at_ms: 1,
                updated_at_ms,
                establishing_event_id: event_id,
                last_event_id: event_id,
            },
            user_id: aruna_core::UserId::local(Ulid::from_parts(1, 3), realm_id),
            node_id,
            payload: MetadataCreateEventPayload::Scaffold {
                name: "raw".to_string(),
                description: "raw state".to_string(),
                date_published: "2026-07-24".to_string(),
                license: None,
            },
            occurred_at_ms: updated_at_ms,
        }
    }

    fn crate_payload(name: &str) -> MetadataCreateEventPayload {
        MetadataCreateEventPayload::RoCrate {
            jsonld: serde_json::json!({
                "@context": "https://w3id.org/ro/crate/1.2/context",
                "@graph": [{
                    "@id": "./",
                    "@type": "Dataset",
                    "name": name
                }]
            })
            .to_string(),
        }
    }

    #[test]
    fn folds_in_order() {
        let mut base = test_event(Ulid::from_parts(1, 10), 1);
        base.payload = crate_payload("base");
        let state = resolve_raw_state(&[base]).unwrap().unwrap();
        let mut update = test_event(Ulid::from_parts(2, 10), 2);
        update.payload = MetadataCreateEventPayload::UpsertDataEntity {
            jsonld: serde_json::json!({
                "@id": "data/file.txt",
                "@type": "File",
                "name": "file"
            })
            .to_string(),
        };

        let (state, base_changed) =
            advance_raw_state(state, std::slice::from_ref(&update)).unwrap();

        assert!(!base_changed);
        assert_eq!(state.last_event_id, update.event_id);
        let revision = state.revision.unwrap();
        assert_eq!(revision.winning_event_id, update.event_id);
        assert!(revision.jsonld.contains("data/file.txt"));
    }

    #[test]
    fn replaces_new_base() {
        let mut base = test_event(Ulid::from_parts(1, 10), 1);
        base.payload = crate_payload("old");
        let state = resolve_raw_state(&[base]).unwrap().unwrap();
        let mut replacement = test_event(Ulid::from_parts(2, 10), 10);
        replacement.payload = match crate_payload("new") {
            MetadataCreateEventPayload::RoCrate { jsonld } => {
                MetadataCreateEventPayload::ReplaceRoCrate { jsonld }
            }
            _ => unreachable!(),
        };

        let (state, base_changed) =
            advance_raw_state(state, std::slice::from_ref(&replacement)).unwrap();

        assert!(base_changed);
        assert_eq!(state.base, raw_base_identity(&replacement));
        let revision = state.revision.unwrap();
        assert_eq!(revision.winning_event_id, replacement.event_id);
        assert!(revision.jsonld.contains("\"name\":\"new\""));
        assert!(!revision.jsonld.contains("\"name\":\"old\""));
    }

    #[test]
    fn keeps_no_base() {
        let scaffold = test_event(Ulid::from_parts(1, 10), 1);
        let state = resolve_raw_state(&[scaffold]).unwrap().unwrap();
        let mut update = test_event(Ulid::from_parts(2, 10), 2);
        update.payload = MetadataCreateEventPayload::UpsertContextualEntity {
            jsonld: r##"{"@id":"#person","@type":"Person"}"##.to_string(),
        };

        let (state, base_changed) =
            advance_raw_state(state, std::slice::from_ref(&update)).unwrap();

        assert!(!base_changed);
        assert_eq!(state.last_event_id, update.event_id);
        assert!(state.base.is_none());
        assert!(state.revision.is_none());
    }

    #[test]
    fn invalid_render_flagged() {
        // Only a valid crate is displayed; an invalid merge is flagged beside it.
        let mut base = test_event(Ulid::from_parts(1, 10), 1);
        base.payload = crate_payload("displayed");
        let state = resolve_raw_state(&[base]).unwrap().unwrap();
        let event = test_event(Ulid::from_parts(2, 10), 2);
        let render = serde_json::json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": [{ "@id": "./", "@type": "Dataset", "name": "merged" }]
        })
        .to_string();

        let flagged = merged_raw_state(state, &event, render.clone(), 2).unwrap();

        assert!(flagged.merged);
        let revision = flagged.revision.clone().unwrap();
        assert!(revision.jsonld.contains("\"name\":\"displayed\""));
        assert_eq!(revision.winning_event_id, Ulid::from_parts(1, 10));
        let merged = revision.merged.unwrap();
        assert_eq!(merged.findings, 2);
        assert_eq!(merged.jsonld, render);

        let later = test_event(Ulid::from_parts(3, 10), 3);
        let repaired = merged_raw_state(flagged, &later, render.clone(), 0).unwrap();

        let revision = repaired.revision.unwrap();
        assert_eq!(revision.jsonld, render);
        assert_eq!(revision.winning_event_id, later.event_id);
        assert!(revision.merged.is_none());
    }

    #[test]
    fn rejects_raw_budget() {
        assert_eq!(RAW_PAGE_SIZE, 1);

        let mut bytes = RawLoadBudget::default();
        bytes.inspect(RAW_ENCODED_LIMIT).unwrap();
        assert!(matches!(
            bytes.inspect(1),
            Err(MetadataRawReadError::LimitExceeded(_))
        ));

        let mut events = RawLoadBudget::default();
        for _ in 0..RAW_EVENT_LIMIT {
            events.accept().unwrap();
        }
        assert!(events.accept().is_err());
    }

    #[test]
    fn rejects_raw_key() {
        let event_id = Ulid::from_parts(1, 10);
        let event = test_event(event_id, 1);
        let key = metadata_event_log_key(event.record.document_id, Ulid::from_parts(2, 10));

        assert!(matches!(
            validate_raw_entry(event.record.document_id, &key, &event),
            Err(MetadataRawReadError::InconsistentLog(_))
        ));
    }

    #[tokio::test]
    async fn rebuilds_out_order() {
        let mut base = test_event(Ulid::from_parts(1, 10), 1);
        base.payload = crate_payload("base");
        let mut late = test_event(Ulid::from_parts(2, 10), 2);
        late.payload = MetadataCreateEventPayload::UpsertDataEntity {
            jsonld: r#"{"@id":"data/file.txt","@type":"File","name":"late"}"#.to_string(),
        };
        let mut latest = test_event(Ulid::from_parts(3, 10), 3);
        latest.payload = MetadataCreateEventPayload::UpsertDataEntity {
            jsonld: r#"{"@id":"data/file.txt","@type":"File","name":"latest"}"#.to_string(),
        };
        let cached = resolve_raw_state(&[base.clone(), latest.clone()])
            .unwrap()
            .unwrap();
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let mut writes = [&base, &late, &latest]
            .into_iter()
            .map(metadata_create_event_write_entry)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        writes.push((
            METADATA_RAW_REVISION_KEYSPACE.to_string(),
            raw_revision_key(base.record.document_id),
            ByteView::from(postcard::to_allocvec(&cached).unwrap()),
        ));
        let event = storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));

        let plan = prepare_raw_event(&context, &late, &mut RawStateCache::default())
            .await
            .unwrap();

        assert!(plan.rebuild);
        assert_eq!(plan.state.last_event_id, latest.event_id);
        let jsonld = plan.revision.unwrap().jsonld;
        assert!(jsonld.contains("\"name\":\"latest\""));
        assert!(!jsonld.contains("\"name\":\"late\""));
    }
}
