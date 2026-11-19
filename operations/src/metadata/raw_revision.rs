//! Rebuilds raw metadata revisions from the event log and prepares new raw events.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{EVENT_LOG_KEYSPACE, RAW_REVISION_KEYSPACE};
use aruna_core::metadata::{
    MetadataError, MetadataEventPayload, MetadataEventRecord, MetadataMergedRevision,
    MetadataRawRevision, apply_raw_event, raw_context_digest, resolve_raw_revision,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{event_log_key, raw_revision_key};
use byteview::ByteView;
use ulid::Ulid;

use crate::metadata::raw_events::{
    RAW_ENCODED_LIMIT, RawBaseIdentity, RawEventsOperation, RawLoadBudget, RawReadError,
    RawRevisionState, budget_error, validate_raw_entry,
};
use crate::metadata::revision_read::RawStateOperation;

fn event_size(event: &MetadataEventRecord) -> Result<usize, RawReadError> {
    Ok(postcard::experimental::serialized_size(event).map_err(ConversionError::from)?)
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct RawStateCache {
    loaded: bool,
    state: Option<RawRevisionState>,
}

#[derive(Debug, PartialEq)]
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

    fn new(
        event: &MetadataEventRecord,
        state: RawRevisionState,
        rebuild: bool,
    ) -> Result<Self, RawReadError> {
        let state_write = (
            RAW_REVISION_KEYSPACE.to_string(),
            raw_revision_key(event.record.document_id),
            ByteView::from(postcard::to_allocvec(&state).map_err(ConversionError::from)?),
        );
        Ok(Self {
            revision: state.revision.clone(),
            rebuild,
            state_write,
            state,
        })
    }
}

type PlanTurn = Result<Next<RawEventPlan>, RawReadError>;

#[derive(Debug, PartialEq)]
enum PrepareStage {
    State,
    /// The events after the cached state up to this one.
    Advance(RawRevisionState),
    /// The base event an older event's rebuild starts at, and the newest event it covers.
    Base(Ulid, Ulid),
    /// The events after the base event, with the base event itself.
    Rebuild(MetadataEventRecord),
    /// Every event up to this one, for a document without cached state.
    Initial,
}

/// The raw state after `event` and whether the graph must be rebuilt from it. The state the
/// group cached is reused; an older event rebuilds the state from its base event.
#[derive(Debug, PartialEq)]
pub(crate) struct PrepareRawOperation {
    event: MetadataEventRecord,
    cache: RawStateCache,
    stage: PrepareStage,
}

impl PrepareRawOperation {
    pub(crate) fn new(event: MetadataEventRecord, cache: RawStateCache) -> Self {
        Self {
            event,
            cache,
            stage: PrepareStage::State,
        }
    }

    fn events(&self, after: Option<Ulid>, until: Ulid, budget: RawLoadBudget) -> IoCall {
        let document_id = self.event.record.document_id;
        let events = RawEventsOperation::new(document_id, after, Some(until), None, budget);
        IoCall::child(events)
    }

    fn decide(&mut self, state: Option<RawRevisionState>) -> PlanTurn {
        let event = &self.event;
        match state {
            // A merged document's revision comes from the graph, so a replayed text event must
            // neither rebuild it nor overwrite it.
            Some(state) if state.merged => Ok(Next::Done(RawEventPlan::new(event, state, false)?)),
            Some(state) if event.event_id > state.last_event_id => {
                let (after, until) = (state.last_event_id, event.event_id);
                self.stage = PrepareStage::Advance(state);
                Ok(Next::Call(self.events(
                    Some(after),
                    until,
                    RawLoadBudget::default(),
                )))
            }
            Some(state) => {
                let incoming_base = raw_base_identity(event);
                let start = (state.base.map(|base| base.event_id).into_iter())
                    .chain(incoming_base.map(|base| base.event_id))
                    .min();
                let Some(start) = start else {
                    let rebuild = state.revision.is_some();
                    return Ok(Next::Done(RawEventPlan::new(event, state, rebuild)?));
                };
                let end = state.last_event_id.max(event.event_id);
                let document_id = event.record.document_id;
                self.stage = PrepareStage::Base(start, end);
                let read = (
                    EVENT_LOG_KEYSPACE.to_string(),
                    event_log_key(document_id, start),
                );
                Ok(Next::Call(IoCall::Read(vec![read])))
            }
            None => match &event.payload {
                MetadataEventPayload::Scaffold { .. } | MetadataEventPayload::RoCrate { .. } => {
                    let mut budget = RawLoadBudget::default();
                    budget.inspect(event_size(event)?)?;
                    budget.accept()?;
                    let events = vec![event.clone()];
                    self.initial(&events)
                }
                _ => {
                    self.stage = PrepareStage::Initial;
                    let until = event.event_id;
                    Ok(Next::Call(self.events(
                        None,
                        until,
                        RawLoadBudget::default(),
                    )))
                }
            },
        }
    }

    fn initial(&self, events: &[MetadataEventRecord]) -> PlanTurn {
        let event = &self.event;
        let state = resolve_raw_state(events)?.ok_or_else(|| {
            RawReadError::InconsistentLog(format!("raw event log is missing {}", event.event_id))
        })?;
        let rebuild = state.revision.is_some() && !event_matches_state(event, &state);
        Ok(Next::Done(RawEventPlan::new(event, state, rebuild)?))
    }
}

impl Operation for PrepareRawOperation {
    type Output = RawEventPlan;
    type Error = RawReadError;

    fn first(&mut self) -> PlanTurn {
        if self.cache.loaded {
            let state = self.cache.state.clone();
            return self.decide(state);
        }
        let read = RawStateOperation::new(self.event.record.document_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> PlanTurn {
        let document_id = self.event.record.document_id;
        match std::mem::replace(&mut self.stage, PrepareStage::Initial) {
            PrepareStage::State => {
                let state = result.child::<Option<RawRevisionState>, RawReadError>()??;
                self.decide(state)
            }
            PrepareStage::Advance(state) => {
                let events = result.child::<Vec<MetadataEventRecord>, RawReadError>()??;
                let event = &self.event;
                if !events
                    .iter()
                    .any(|candidate| candidate.event_id == event.event_id)
                {
                    return Err(RawReadError::InconsistentLog(format!(
                        "raw event log is missing {}",
                        event.event_id
                    )));
                }
                let (next, base_changed) = advance_raw_state(state, &events)?;
                let simple = events.len() == 1 && !base_changed;
                let rebuild = match simple {
                    true => !event_matches_state(event, &next),
                    false => next.revision.is_some(),
                };
                Ok(Next::Done(RawEventPlan::new(event, next, rebuild)?))
            }
            PrepareStage::Base(start, end) => {
                let value = result.value()?;
                let first = raw_event(document_id, start, value)?;
                let mut budget = RawLoadBudget::default();
                budget.inspect(event_size(&first)?)?;
                budget.accept()?;
                self.stage = PrepareStage::Rebuild(first);
                Ok(Next::Call(self.events(Some(start), end, budget)))
            }
            PrepareStage::Rebuild(first) => {
                let later = result.child::<Vec<MetadataEventRecord>, RawReadError>()??;
                let mut events = vec![first];
                events.extend(later);
                let next = resolve_raw_state(&events)?.ok_or_else(|| {
                    RawReadError::InconsistentLog(format!(
                        "raw base event is missing for {document_id}"
                    ))
                })?;
                let rebuild = next.revision.is_some();
                Ok(Next::Done(RawEventPlan::new(&self.event, next, rebuild)?))
            }
            PrepareStage::Initial => {
                let events = result.child::<Vec<MetadataEventRecord>, RawReadError>()??;
                self.initial(&events)
            }
        }
    }
}

/// Raw state after `event`'s batch merged and the graph re-rendered. Out-of-order arrival
/// needs no replay: the render describes every batch this node has merged.
#[derive(Debug, PartialEq)]
pub(crate) struct PrepareMergedOperation {
    event: MetadataEventRecord,
    render: String,
    findings: u32,
    cache: RawStateCache,
}

impl PrepareMergedOperation {
    pub(crate) fn new(
        event: MetadataEventRecord,
        (render, findings): (String, u32),
        cache: RawStateCache,
    ) -> Self {
        Self {
            event,
            render,
            findings,
            cache,
        }
    }

    fn merged(&mut self, cached: Option<RawRevisionState>) -> PlanTurn {
        let event = &self.event;
        let previous = cached.unwrap_or(RawRevisionState {
            base: None,
            last_event_id: event.event_id,
            revision: None,
            merged: true,
        });
        let render = std::mem::take(&mut self.render);
        let state = merged_raw_state(previous, event, render, self.findings)?;
        Ok(Next::Done(RawEventPlan::new(event, state, false)?))
    }
}

impl Operation for PrepareMergedOperation {
    type Output = RawEventPlan;
    type Error = RawReadError;

    fn first(&mut self) -> PlanTurn {
        if self.cache.loaded {
            let state = self.cache.state.clone();
            return self.merged(state);
        }
        let read = RawStateOperation::new(self.event.record.document_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> PlanTurn {
        let state = result.child::<Option<RawRevisionState>, RawReadError>()??;
        self.merged(state)
    }
}

/// Only a valid render is displayed: an invalid one is flagged beside the last
/// valid revision so the editor can open it and the export stays sound.
fn merged_raw_state(
    mut state: RawRevisionState,
    event: &MetadataEventRecord,
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

fn advance_raw_state(
    mut state: RawRevisionState,
    events: &[MetadataEventRecord],
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
            if !apply_raw_event(&mut document, &event.payload)? {
                continue;
            }
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
    events: &[MetadataEventRecord],
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

fn raw_base_identity(event: &MetadataEventRecord) -> Option<RawBaseIdentity> {
    matches!(
        &event.payload,
        MetadataEventPayload::RoCrate { .. } | MetadataEventPayload::ReplaceRoCrate { .. }
    )
    .then_some(RawBaseIdentity {
        updated_at_ms: event.record.updated_at_ms,
        event_id: event.event_id,
    })
}

fn event_matches_state(event: &MetadataEventRecord, state: &RawRevisionState) -> bool {
    match &event.payload {
        MetadataEventPayload::Scaffold { .. } => state.revision.is_none(),
        MetadataEventPayload::RoCrate { .. } | MetadataEventPayload::ReplaceRoCrate { .. } => {
            state.base == raw_base_identity(event)
                && state
                    .revision
                    .as_ref()
                    .is_some_and(|revision| revision.winning_event_id == event.event_id)
        }
        MetadataEventPayload::UpsertDataEntity { .. }
        | MetadataEventPayload::UpsertContextualEntity { .. }
        | MetadataEventPayload::ApplyBatch { .. }
        | MetadataEventPayload::Checkpoint { .. } => true,
    }
}

/// The event a read of its event log row answered, checked as a raw event.
fn raw_event(
    document_id: Ulid,
    event_id: Ulid,
    value: Option<ByteView>,
) -> Result<MetadataEventRecord, RawReadError> {
    let Some(value) = value else {
        return Err(RawReadError::InconsistentLog(format!(
            "raw event {event_id} is missing"
        )));
    };
    if value.len() > RAW_ENCODED_LIMIT {
        return Err(budget_error("encoded byte limit exceeded"));
    }
    let event: MetadataEventRecord = postcard::from_bytes(&value).map_err(ConversionError::from)?;
    validate_raw_entry(document_id, &event_log_key(document_id, event_id), &event)?;
    Ok(event)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};

    /// Prepares `event` as a materialization does, keeping the state in `cache` afterwards.
    pub(crate) async fn prepare_raw_event(
        context: &DriverContext,
        event: &MetadataEventRecord,
        cache: &mut RawStateCache,
    ) -> Result<RawEventPlan, RawReadError> {
        let prepare = PrepareRawOperation::new(event.clone(), cache.clone());
        let plan = drive(prepare, context).await?;
        plan.cache(cache);
        Ok(plan)
    }

    /// Prepares a merged `event` as a materialization does, keeping the state in `cache`.
    pub(crate) async fn prepare_merged_event(
        context: &DriverContext,
        event: &MetadataEventRecord,
        render: String,
        findings: u32,
        cache: &mut RawStateCache,
    ) -> Result<RawEventPlan, RawReadError> {
        let prepare = PrepareMergedOperation::new(event.clone(), (render, findings), cache.clone());
        let plan = drive(prepare, context).await?;
        plan.cache(cache);
        Ok(plan)
    }
    use crate::metadata::raw_events::{RAW_EVENT_LIMIT, RAW_PAGE_SIZE};
    use aruna_core::storage_entries::create_event_entry;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    pub(crate) fn test_event(event_id: Ulid, updated_at_ms: u64) -> MetadataEventRecord {
        let realm_id = RealmId::from_bytes([1; 32]);
        let group_id = Ulid::from_parts(1, 1);
        let document_id = Ulid::from_parts(1, 2);
        let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
        MetadataEventRecord {
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
            payload: MetadataEventPayload::Scaffold {
                name: "raw".to_string(),
                description: "raw state".to_string(),
                date_published: "2026-07-24".to_string(),
                license: None,
            },
            occurred_at_ms: updated_at_ms,
        }
    }

    fn crate_payload(name: &str) -> MetadataEventPayload {
        MetadataEventPayload::RoCrate {
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
        update.payload = MetadataEventPayload::UpsertDataEntity {
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
            MetadataEventPayload::RoCrate { jsonld } => {
                MetadataEventPayload::ReplaceRoCrate { jsonld }
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
        update.payload = MetadataEventPayload::UpsertContextualEntity {
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
            Err(RawReadError::LimitExceeded(_))
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
        let key = event_log_key(event.record.document_id, Ulid::from_parts(2, 10));

        assert!(matches!(
            validate_raw_entry(event.record.document_id, &key, &event),
            Err(RawReadError::InconsistentLog(_))
        ));
    }

    #[tokio::test]
    async fn rebuilds_out_order() {
        let mut base = test_event(Ulid::from_parts(1, 10), 1);
        base.payload = crate_payload("base");
        let mut late = test_event(Ulid::from_parts(2, 10), 2);
        late.payload = MetadataEventPayload::UpsertDataEntity {
            jsonld: r#"{"@id":"data/file.txt","@type":"File","name":"late"}"#.to_string(),
        };
        let mut latest = test_event(Ulid::from_parts(3, 10), 3);
        latest.payload = MetadataEventPayload::UpsertDataEntity {
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let mut writes = [&base, &late, &latest]
            .into_iter()
            .map(create_event_entry)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        writes.push((
            RAW_REVISION_KEYSPACE.to_string(),
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
