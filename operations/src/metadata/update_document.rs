use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    CREATE_ACCEPTANCE_KEYSPACE, EVENT_LOG_KEYSPACE, RAW_BUDGET_KEYSPACE,
    REALM_CONFIG_KEYSPACE,
};
use aruna_core::metadata::{
    RAW_BYTES_LIMIT, EVENT_LIMIT, MetadataBatch, MetadataBatchSource,
    MetadataEffect, MetadataError, MetadataEvent, MetadataEventPayload, MetadataEventRecord,
    MetadataLifecycleRecord, ProfileValidationStatus, RawOriginBudget,
    deterministic_materialization_actor, raw_quotas,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    create_acceptance_key, document_lifecycle_entry, event_log_key, event_log_prefix,
    profile_validation_entry, raw_budget_entry, raw_budget_key, sync_revision_entry,
};
use aruna_core::structs::storage::metadata_registry::{MetadataAuditRecord, MetadataRegistryRecord};
use aruna_core::structs::placement::placement_record::PlacementRef;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId, TxnId};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::metadata::materialization_queue::{
    new_materialization_job, new_pending_status, schedule_materialization,
};
use crate::metadata::profile_validation::{
    not_profiled_status, stale_status, submission_profile_tag, validate_submission,
};
use crate::metadata::projector::{create_outbox_record, registry_outbox_record};
use crate::metadata::repository::{
    StorageReadError, event_projection_entries, parse_registry_read, read_registry_effect,
};
use crate::sync::document_outbox::{outbox_write_entry, schedule_drain_effect};
use crate::sync::shard_placement::sort_node_ids;

const RAW_EVENT_LIMIT: usize = EVENT_LIMIT as usize;

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateDocumentConfig {
    pub actor: aruna_core::structs::identity::auth::Actor,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub public: bool,
    pub mutation: UpdateDocumentMutation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UpdateDocumentMutation {
    ReplaceRoCrate {
        jsonld: String,
    },
    UpsertDataEntity {
        jsonld: String,
    },
    UpsertContextualEntity {
        jsonld: String,
    },
    /// A batch a device planned against its own replica. It is appended
    /// verbatim, under the device's actor, so both replicas converge on it.
    ApplyBatch {
        batch: Box<MetadataBatch>,
        authored: MetadataBatchSource,
    },
}

/// Validates a metadata update and persists the event plus projection work.
/// Success means acceptance into the durable event/projection pipeline, not
/// completed graph materialization or replica convergence.
#[derive(Debug, PartialEq)]
pub struct UpdateDocumentOperation {
    config: UpdateDocumentConfig,
    /// Minted before the batch is planned: it is the batch actor as well as the
    /// event id, so a plan can never be attributed to another event.
    event_id: Ulid,
    txn_id: Option<TxnId>,
    record: Option<MetadataRegistryRecord>,
    update_event: Option<MetadataEventRecord>,
    planned_batch: Option<MetadataBatch>,
    raw_budget: Option<RawOriginBudget>,
    next_raw_budget: Option<RawOriginBudget>,
    accepted_create: Option<MetadataEventRecord>,
    realm_config: Option<RealmConfigDocument>,
    /// Buckets this update publishes onto and the activation generation each
    /// resolved at, read as a fence inside the write transaction.
    fenced: Vec<(PlacementRef, u64)>,
    route_profile_status: Option<ProfileValidationStatus>,
    /// Phase-time and identity sampling; production keeps the defaults.
    phase_source: crate::metadata::MetadataPhaseSource,
    state: UpdateDocumentState,
    output: Option<Result<MetadataRegistryRecord, UpdateDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum UpdateDocumentState {
    Init,
    ReadCurrent,
    ReadRealmConfig,
    PlanBatch,
    StartTransaction,
    ReadFence,
    ReadRawFence,
    ReadRawEvents,
    WriteUpdateBatch,
    CommitTransaction,
    ScheduleMaterializationDrain,
    ScheduleOutboxDrain,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum UpdateDocumentError {
    #[error(transparent)]
    StorageError(#[from] aruna_core::errors::StorageError),
    #[error(transparent)]
    ConversionError(#[from] aruna_core::errors::ConversionError),
    #[error(transparent)]
    MetadataError(#[from] MetadataError),
    #[error("document not found")]
    DocumentNotFound,
    #[error("missing active transaction")]
    MissingTransaction,
    #[error("operation did not finish")]
    NotFinished,
    #[error("metadata raw update budget exceeded")]
    RawLimit,
    #[error("the document's bucket cut over to a new holder set; retry the update")]
    PlacementFenced,
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl UpdateDocumentOperation {
    pub fn new(config: UpdateDocumentConfig) -> Self {
        let route_profile_status = match &config.mutation {
            UpdateDocumentMutation::ReplaceRoCrate { jsonld }
                if !submission_profile_tag(jsonld) =>
            {
                Some(not_profiled_status(config.document_id))
            }
            UpdateDocumentMutation::ReplaceRoCrate { .. } => None,
            UpdateDocumentMutation::UpsertDataEntity { .. }
            | UpdateDocumentMutation::UpsertContextualEntity { .. }
            | UpdateDocumentMutation::ApplyBatch { .. } => {
                Some(stale_status(config.document_id, "dataset_revision_changed"))
            }
        };
        let planned_batch = match &config.mutation {
            UpdateDocumentMutation::ApplyBatch { batch, .. } => Some((**batch).clone()),
            _ => None,
        };
        let phase_source = crate::metadata::MetadataPhaseSource::default();
        Self {
            config,
            // The event identity is minted from the phase source on the first
            // real transition, so replacing the source before `start` is not
            // shadowed by an incidental constructor sample.
            event_id: Ulid::nil(),
            txn_id: None,
            record: None,
            update_event: None,
            planned_batch,
            raw_budget: None,
            next_raw_budget: None,
            accepted_create: None,
            realm_config: None,
            fenced: Vec::new(),
            route_profile_status,
            phase_source,
            state: UpdateDocumentState::Init,
            output: None,
        }
    }

    /// Replaces the phase-time and identity source and re-mints the event
    /// identity from it. Set before `start`; production never calls this.
    #[cfg(test)]
    pub(crate) fn with_phase_source(
        mut self,
        phase_source: crate::metadata::MetadataPhaseSource,
    ) -> Self {
        self.phase_source = phase_source;
        self.event_id = phase_source.next_id();
        self
    }

    fn updated_record(&self, mut record: MetadataRegistryRecord) -> MetadataRegistryRecord {
        record.public = self.config.public;
        record.updated_at_ms = self.phase_source.now_ms();
        record
    }

    fn batch_source(&self) -> MetadataBatchSource {
        match &self.config.mutation {
            UpdateDocumentMutation::ReplaceRoCrate { jsonld } => {
                MetadataBatchSource::ReplaceRoCrate {
                    jsonld: jsonld.clone(),
                }
            }
            UpdateDocumentMutation::UpsertDataEntity { jsonld } => {
                MetadataBatchSource::UpsertDataEntity {
                    jsonld: jsonld.clone(),
                }
            }
            UpdateDocumentMutation::UpsertContextualEntity { jsonld } => {
                MetadataBatchSource::UpsertContextualEntity {
                    jsonld: jsonld.clone(),
                }
            }
            UpdateDocumentMutation::ApplyBatch { authored, .. } => authored.clone(),
        }
    }

    fn update_event_payload(&self) -> Result<MetadataEventPayload, UpdateDocumentError> {
        let Some(batch) = self.planned_batch.clone() else {
            return Err(MetadataError::Backend(
                "metadata batch is missing before update commit".to_string(),
            )
            .into());
        };
        Ok(MetadataEventPayload::ApplyBatch {
            batch,
            authored: self.batch_source(),
        })
    }

    fn update_event_record(
        &self,
        record: &MetadataRegistryRecord,
    ) -> Result<MetadataEventRecord, UpdateDocumentError> {
        let mut record = record.clone();
        record.last_event_id = self.event_id;
        let occurred_at_ms = record.updated_at_ms;
        Ok(MetadataEventRecord {
            event_id: self.event_id,
            record,
            user_id: self.config.actor.user_id,
            node_id: self.config.actor.node_id,
            payload: self.update_event_payload()?,
            occurred_at_ms,
        })
    }

    fn audit_record(&self, event: &MetadataEventRecord) -> MetadataAuditRecord {
        MetadataAuditRecord {
            realm_id: event.record.realm_id,
            group_id: event.record.group_id,
            document_id: event.record.document_id,
            graph_iri: event.record.graph_iri.clone(),
            user_id: self.config.actor.user_id,
            node_id: self.config.actor.node_id,
            operation: event.payload.audit_operation(),
            occurred_at_ms: event.occurred_at_ms,
            details: Some(event.payload.materialization_kind().to_string()),
        }
    }

    /// Plans the change set against the local graph. The planner runs the same
    /// structural validation the applying call would, so this replaces it.
    fn plan_batch_effect(
        &self,
        record: &MetadataRegistryRecord,
    ) -> Result<Option<Effect>, MetadataError> {
        match &self.config.mutation {
            // A device already planned its batch, so there is nothing to plan.
            UpdateDocumentMutation::ApplyBatch { .. } => return Ok(None),
            UpdateDocumentMutation::ReplaceRoCrate { .. } => {}
            UpdateDocumentMutation::UpsertDataEntity { jsonld }
            | UpdateDocumentMutation::UpsertContextualEntity { jsonld } => {
                validate_entity_jsonld(jsonld)?;
            }
        }
        Ok(Some(Effect::Metadata(MetadataEffect::PlanBatch {
            graph_iri: record.graph_iri.clone(),
            actor: deterministic_materialization_actor(self.event_id),
            source: self.batch_source(),
        })))
    }

    fn begin_transaction_effect(&mut self) -> Effects {
        self.state = UpdateDocumentState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    /// The buckets this update publishes onto and the generation each resolves
    /// at. Empty before the realm's first candidate map, where no activation
    /// exists and no transition can be in flight.
    fn fenced_buckets(&self, record: &MetadataRegistryRecord) -> Vec<(PlacementRef, u64)> {
        let Some(config) = self.realm_config.as_ref() else {
            return Vec::new();
        };
        [
            record.placement,
            crate::placement::registry_placement(config, record),
        ]
        .into_iter()
        .filter_map(|placement| {
            let generation = crate::placement::fence::write_generation(config, &placement)?;
            Some((placement, generation))
        })
        .collect()
    }

    fn generation_of(&self, placement: &PlacementRef) -> u64 {
        self.fenced
            .iter()
            .find(|(bucket, _)| bucket == placement)
            .map_or(0, |(_, generation)| *generation)
    }

    fn write_batch_effect(&self, txn_id: TxnId) -> Result<Effect, UpdateDocumentError> {
        let Some(event) = self.update_event.as_ref() else {
            return Err(UpdateDocumentError::MissingTransaction);
        };
        let now = self.phase_source.now_ms();
        let audit = self.audit_record(event);
        // Updating an existing document is a mutation, not an origin write, so it
        // never mints the lifecycle sync topic genesis.
        let lifecycle_outbox = create_outbox_record(event, self.realm_config.as_ref(), false)
            .fenced_at(self.generation_of(&event.record.placement));
        let outbox = (!event.record.holder_node_ids.is_empty()).then_some(&lifecycle_outbox);
        let status = new_pending_status(event, now);
        let job = new_materialization_job(event, now);
        let mut writes = event_projection_entries(event, &audit, outbox, &status, &job)?;
        // Refresh the everywhere-bound registry row so non-holders see the new
        // revision, not just the bucket's holders.
        if let Some(registry_outbox) =
            registry_outbox_record(event, self.realm_config.as_ref(), false).map(|record| {
                let generation = self.generation_of(&record.placement);
                record.fenced_at(generation)
            })
        {
            writes.push(
                outbox_write_entry(&registry_outbox)
                    .map_err(aruna_core::errors::ConversionError::from)?,
            );
        }
        let lifecycle = MetadataLifecycleRecord::Upsert {
            event: Box::new(event.clone()),
        };
        writes.push(document_lifecycle_entry(&lifecycle)?);
        if outbox.is_none() {
            let aruna_core::document::DocumentOutboxEvent::Upsert { change, .. } =
                lifecycle_outbox.event
            else {
                unreachable!("metadata lifecycle update outbox must be an upsert");
            };
            writes.push(sync_revision_entry(&lifecycle_outbox.target, &change)?);
        }
        let Some(raw_budget) = self.next_raw_budget.as_ref() else {
            return Err(UpdateDocumentError::RawLimit);
        };
        writes.push(raw_budget_entry(raw_budget)?);
        let Some(mut profile_status) = self.route_profile_status.clone() else {
            return Err(MetadataError::Backend(
                "profile validation status is missing before update commit".to_string(),
            )
            .into());
        };
        profile_status.document_id = event.record.document_id;
        profile_status.dataset_revision = event.event_id;
        // The merged render is only known once the batch materializes, so the
        // accepted status carries no digest to be fresh against yet.
        profile_status.dataset_digest = None;
        writes.push(profile_validation_entry(&profile_status)?);
        Ok(Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        }))
    }

    fn fail(&mut self, error: UpdateDocumentError) -> Effects {
        let cleanup = self.abort();
        self.state = UpdateDocumentState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(UpdateDocumentError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }

    fn origin_quota(
        &self,
        event: &MetadataEventRecord,
    ) -> Result<RawOriginBudget, UpdateDocumentError> {
        if event.record.document_id != self.config.document_id
            || event.record.establishing_event_id != event.event_id
            || event.record.last_event_id != event.event_id
            || !matches!(
                event.payload,
                MetadataEventPayload::Scaffold { .. } | MetadataEventPayload::RoCrate { .. }
            )
        {
            return Err(UpdateDocumentError::RawLimit);
        }
        let mut origins = event.record.holder_node_ids.clone();
        if origins.is_empty() {
            return Err(UpdateDocumentError::RawLimit);
        }
        let original = origins.clone();
        sort_node_ids(&mut origins);
        if origins != original {
            return Err(UpdateDocumentError::RawLimit);
        }
        let encoded_bytes = postcard::experimental::serialized_size(event)
            .map_err(|_| UpdateDocumentError::RawLimit)
            .and_then(|size| u64::try_from(size).map_err(|_| UpdateDocumentError::RawLimit))?;
        raw_quotas(
            event.record.document_id,
            &origins,
            event.node_id,
            encoded_bytes,
        )
        .and_then(|budgets| {
            budgets
                .into_iter()
                .find(|budget| budget.node_id == self.config.actor.node_id)
        })
        .ok_or(UpdateDocumentError::RawLimit)
    }

    fn valid_budget(&self, budget: &RawOriginBudget, quota: &RawOriginBudget) -> bool {
        budget.document_id == self.config.document_id
            && budget.node_id == self.config.actor.node_id
            && budget.event_limit == quota.event_limit
            && budget.byte_limit == quota.byte_limit
            && budget.events >= quota.events
            && budget.encoded_bytes >= quota.encoded_bytes
            && budget.events <= budget.event_limit
            && budget.encoded_bytes <= budget.byte_limit
    }

    fn history_budget(
        &self,
        values: &[(ByteView, ByteView)],
        next_start_after: Option<&ByteView>,
    ) -> Result<(RawOriginBudget, usize, u64), UpdateDocumentError> {
        if values.len() > RAW_EVENT_LIMIT || next_start_after.is_some() {
            return Err(UpdateDocumentError::RawLimit);
        }
        let Some(create) = self.accepted_create.as_ref() else {
            return Err(UpdateDocumentError::RawLimit);
        };
        let quota = self.origin_quota(create)?;
        let mut events = 0u32;
        let mut encoded_bytes = 0u64;
        let mut total_bytes = 0u64;
        let mut saw_create = false;
        for (key, value) in values {
            let event: MetadataEventRecord =
                postcard::from_bytes(value).map_err(|_| UpdateDocumentError::RawLimit)?;
            if key != &event_log_key(self.config.document_id, event.event_id)
                || event.record.document_id != self.config.document_id
            {
                return Err(UpdateDocumentError::RawLimit);
            }
            let value_len =
                u64::try_from(value.len()).map_err(|_| UpdateDocumentError::RawLimit)?;
            total_bytes = total_bytes
                .checked_add(value_len)
                .ok_or(UpdateDocumentError::RawLimit)?;
            if total_bytes > RAW_BYTES_LIMIT {
                return Err(UpdateDocumentError::RawLimit);
            }
            if &event == create {
                saw_create = true;
            }
            if event.node_id == self.config.actor.node_id {
                events = events.checked_add(1).ok_or(UpdateDocumentError::RawLimit)?;
                encoded_bytes = encoded_bytes
                    .checked_add(value_len)
                    .ok_or(UpdateDocumentError::RawLimit)?;
            }
        }
        if !saw_create || events > quota.event_limit || encoded_bytes > quota.byte_limit {
            return Err(UpdateDocumentError::RawLimit);
        }
        Ok((
            RawOriginBudget {
                document_id: quota.document_id,
                node_id: quota.node_id,
                event_limit: quota.event_limit,
                byte_limit: quota.byte_limit,
                events,
                encoded_bytes,
            },
            values.len(),
            total_bytes,
        ))
    }

    fn check_raw_budget(
        &self,
        history_events: usize,
        history_bytes: u64,
    ) -> Result<RawOriginBudget, UpdateDocumentError> {
        let Some(event) = self.update_event.as_ref() else {
            return Err(UpdateDocumentError::MissingTransaction);
        };
        if history_events >= RAW_EVENT_LIMIT {
            return Err(UpdateDocumentError::RawLimit);
        }
        let Some(budget) = self.raw_budget.as_ref() else {
            return Err(UpdateDocumentError::RawLimit);
        };
        let Some(create) = self.accepted_create.as_ref() else {
            return Err(UpdateDocumentError::RawLimit);
        };
        let quota = self.origin_quota(create)?;
        if !self.valid_budget(budget, &quota) || budget.events >= budget.event_limit {
            return Err(UpdateDocumentError::RawLimit);
        }
        let event_bytes = postcard::experimental::serialized_size(event)
            .map_err(aruna_core::errors::ConversionError::from)?;
        let event_bytes = u64::try_from(event_bytes).map_err(|_| UpdateDocumentError::RawLimit)?;
        if history_bytes
            .checked_add(event_bytes)
            .is_none_or(|bytes| bytes > RAW_BYTES_LIMIT)
        {
            return Err(UpdateDocumentError::RawLimit);
        }
        let encoded_bytes = budget
            .encoded_bytes
            .checked_add(event_bytes)
            .ok_or(UpdateDocumentError::RawLimit)?;
        if encoded_bytes > budget.byte_limit {
            return Err(UpdateDocumentError::RawLimit);
        }
        Ok(RawOriginBudget {
            document_id: budget.document_id,
            node_id: budget.node_id,
            event_limit: budget.event_limit,
            byte_limit: budget.byte_limit,
            events: budget
                .events
                .checked_add(1)
                .ok_or(UpdateDocumentError::RawLimit)?,
            encoded_bytes,
        })
    }
    fn read_current(&mut self, event: Event) -> Effects {
        match parse_registry_read(event) {
            Ok(Some(record)) => {
                self.record = Some(record.clone());
                self.state = UpdateDocumentState::ReadRealmConfig;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: REALM_CONFIG_KEYSPACE.to_string(),
                    key: ByteView::from(*record.realm_id.as_bytes()),
                    txn_id: None,
                })]
            }
            Ok(None) => self.fail(UpdateDocumentError::DocumentNotFound),
            Err(StorageReadError::Storage(error)) => self.fail(error.into()),
            Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
        }
    }

    fn read_realm_config(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                if let Some(bytes) = value {
                    match RealmConfigDocument::from_bytes(&bytes) {
                        Ok(config) => self.realm_config = Some(config),
                        Err(error) => return self.fail(error.into()),
                    }
                }
                let Some(record) = self.record.clone() else {
                    return self.fail(UpdateDocumentError::DocumentNotFound);
                };
                match self.plan_batch_effect(&record) {
                    Ok(Some(effect)) => {
                        self.state = UpdateDocumentState::PlanBatch;
                        smallvec![effect]
                    }
                    Ok(None) => self.begin_transaction_effect(),
                    Err(error) => self.fail(error.into()),
                }
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            other => self.unexpected_event("realm config read result", format!("{other:?}")),
        }
    }

    fn plan_batch(&mut self, event: Event) -> Effects {
        match event {
            Event::Metadata(MetadataEvent::BatchPlanned { batch, .. }) => {
                self.planned_batch = Some(batch);
                self.begin_transaction_effect()
            }
            Event::Metadata(MetadataEvent::Error { error, .. }) => self.fail(error.into()),
            other => self.unexpected_event("metadata batch plan result", format!("{other:?}")),
        }
    }

    fn start_transaction(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                self.txn_id = Some(txn_id);
                self.state = UpdateDocumentState::ReadFence;
                smallvec![read_registry_effect(
                    self.config.group_id,
                    self.config.document_id,
                    Some(txn_id),
                )]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            other => self.unexpected_event("transaction start result", format!("{other:?}")),
        }
    }

    fn read_fence(&mut self, event: Event) -> Effects {
        match parse_registry_read(event) {
            Ok(Some(record)) => {
                let record = self.updated_record(record);
                let update_event = match self.update_event_record(&record) {
                    Ok(update_event) => update_event,
                    Err(error) => return self.fail(error),
                };
                self.record = Some(update_event.record.clone());
                self.update_event = Some(update_event);
                let Some(txn_id) = self.txn_id else {
                    return self.fail(UpdateDocumentError::MissingTransaction);
                };
                self.state = UpdateDocumentState::ReadRawFence;
                self.fenced = self.fenced_buckets(&record);
                let mut reads = vec![
                    (
                        RAW_BUDGET_KEYSPACE.to_string(),
                        raw_budget_key(self.config.document_id, self.config.actor.node_id),
                    ),
                    (
                        CREATE_ACCEPTANCE_KEYSPACE.to_string(),
                        create_acceptance_key(self.config.document_id),
                    ),
                ];
                // The fence joins this transaction's read set, so a
                // departing holder's close conflicts an uncommitted write.
                reads.extend(self.fenced.iter().map(|(placement, _)| {
                    crate::placement::fence::fence_read(&record.realm_id, placement)
                }));
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads,
                    txn_id: Some(txn_id),
                })]
            }
            Ok(None) => self.fail(UpdateDocumentError::DocumentNotFound),
            Err(StorageReadError::Storage(error)) => self.fail(error.into()),
            Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
        }
    }

    fn read_raw_fence(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchReadResult { values }) => {
                let [(_, raw_budget), (_, accepted_create), fences @ ..] = values.as_slice() else {
                    return self.unexpected_event(
                        "metadata raw sidecar read",
                        format!("batch read with {} values", values.len()),
                    );
                };
                if fences.len() != self.fenced.len() {
                    return self.unexpected_event(
                        "one fence value per fenced bucket",
                        format!("batch read with {} values", values.len()),
                    );
                }
                let admitted =
                    self.fenced
                        .iter()
                        .zip(fences)
                        .all(|((_, generation), (_, value))| {
                            crate::placement::fence::admits(value.as_ref(), *generation)
                        });
                if !admitted {
                    return self.fail(UpdateDocumentError::PlacementFenced);
                }
                let Some(value) = accepted_create.clone() else {
                    return self.fail(UpdateDocumentError::RawLimit);
                };
                let create: MetadataEventRecord = match postcard::from_bytes(&value) {
                    Ok(create) => create,
                    Err(_) => return self.fail(UpdateDocumentError::RawLimit),
                };
                let quota = match self.origin_quota(&create) {
                    Ok(quota) => quota,
                    Err(error) => return self.fail(error),
                };
                self.accepted_create = Some(create);
                let budget = match raw_budget.clone() {
                    Some(value) => {
                        let budget: RawOriginBudget = match postcard::from_bytes(&value) {
                            Ok(budget) => budget,
                            Err(_) => {
                                return self.fail(UpdateDocumentError::RawLimit);
                            }
                        };
                        if !self.valid_budget(&budget, &quota) {
                            return self.fail(UpdateDocumentError::RawLimit);
                        }
                        Some(budget)
                    }
                    None => None,
                };
                self.raw_budget = budget;
                let Some(txn_id) = self.txn_id else {
                    return self.fail(UpdateDocumentError::MissingTransaction);
                };
                self.state = UpdateDocumentState::ReadRawEvents;
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: EVENT_LOG_KEYSPACE.to_string(),
                    prefix: Some(event_log_prefix(self.config.document_id)),
                    start: None,
                    limit: RAW_EVENT_LIMIT,
                    txn_id: Some(txn_id),
                })]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            other => self.unexpected_event("raw sidecar read result", format!("{other:?}")),
        }
    }

    fn read_raw_events(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                let (reconstructed, history_events, history_bytes) =
                    match self.history_budget(&values, next_start_after.as_ref()) {
                        Ok(history) => history,
                        Err(error) => return self.fail(error),
                    };
                if self
                    .raw_budget
                    .as_ref()
                    .is_some_and(|budget| &reconstructed != budget)
                {
                    return self.fail(UpdateDocumentError::RawLimit);
                }
                self.raw_budget = Some(reconstructed);
                self.next_raw_budget = match self.check_raw_budget(history_events, history_bytes) {
                    Ok(budget) => Some(budget),
                    Err(error) => return self.fail(error),
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(UpdateDocumentError::MissingTransaction);
                };
                self.state = UpdateDocumentState::WriteUpdateBatch;
                match self.write_batch_effect(txn_id) {
                    Ok(effect) => smallvec![effect],
                    Err(error) => self.fail(error),
                }
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            other => self.unexpected_event("raw event iteration result", format!("{other:?}")),
        }
    }

    fn write_update_batch(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(UpdateDocumentError::MissingTransaction);
                };
                self.state = UpdateDocumentState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            other => self.unexpected_event("metadata update batch write", format!("{other:?}")),
        }
    }

    fn commit_transaction(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;
                self.state = UpdateDocumentState::ScheduleMaterializationDrain;
                smallvec![schedule_materialization()]
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.txn_id = None;
                self.fail(error.into())
            }
            other => self.unexpected_event("transaction commit result", format!("{other:?}")),
        }
    }

    fn finish_materialization_drain(&mut self, event: Event) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. }) => {
                self.state = UpdateDocumentState::ScheduleOutboxDrain;
                smallvec![schedule_drain_effect()]
            }
            Event::Task(TaskEvent::Error { message, .. }) => {
                warn!(message = %message, "Failed to schedule metadata materialization drain after committed update");
                self.state = UpdateDocumentState::ScheduleOutboxDrain;
                smallvec![schedule_drain_effect()]
            }
            other => self.unexpected_event(
                "metadata materialization drain schedule",
                format!("{other:?}"),
            ),
        }
    }

    fn schedule_outbox_drain(&mut self, event: Event) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. }) => {
                let Some(record) = self.record.clone() else {
                    return self.fail(UpdateDocumentError::MissingTransaction);
                };
                self.state = UpdateDocumentState::Finish;
                self.output = Some(Ok(record));
                smallvec![]
            }
            Event::Task(TaskEvent::Error { message, .. }) => {
                warn!(message = %message, "Failed to schedule metadata document outbox drain after committed update");
                let Some(record) = self.record.clone() else {
                    return self.fail(UpdateDocumentError::MissingTransaction);
                };
                self.state = UpdateDocumentState::Finish;
                self.output = Some(Ok(record));
                smallvec![]
            }
            other => self.unexpected_event(
                "metadata document outbox drain schedule",
                format!("{other:?}"),
            ),
        }
    }
}

pub async fn update_metadata_document(
    mut operation: UpdateDocumentOperation,
    context: &DriverContext,
) -> Result<MetadataRegistryRecord, UpdateDocumentError> {
    operation.route_profile_status = Some(match &operation.config.mutation {
        UpdateDocumentMutation::ReplaceRoCrate { jsonld } => {
            validate_submission(
                context,
                operation.config.document_id,
                operation.config.group_id,
                jsonld,
            )
            .await?
        }
        UpdateDocumentMutation::UpsertDataEntity { .. }
        | UpdateDocumentMutation::UpsertContextualEntity { .. }
        | UpdateDocumentMutation::ApplyBatch { .. } => {
            stale_status(operation.config.document_id, "dataset_revision_changed")
        }
    });
    let cache_generation = context
        .metadata_handle
        .as_ref()
        .map(|metadata_handle| metadata_handle.visibility_generation());
    let updated = drive(operation, context).await?;
    if let (Some(metadata_handle), Some(cache_generation)) =
        (context.metadata_handle.as_ref(), cache_generation)
    {
        metadata_handle.upsert_cached_at(updated.clone(), cache_generation);
    }
    Ok(updated)
}

fn validate_entity_jsonld(jsonld: &str) -> Result<(), MetadataError> {
    let value: serde_json::Value = serde_json::from_str(jsonld)
        .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
    let object = value.as_object().ok_or_else(|| {
        MetadataError::InvalidInput("entity payload must be a JSON object".to_string())
    })?;
    if object.contains_key("@graph") || object.contains_key("graph") {
        return Err(MetadataError::InvalidInput(
            "entity payload must not contain `@graph`; send a single JSON-LD entity object"
                .to_string(),
        ));
    }
    let has_id = object
        .get("@id")
        .or_else(|| object.get("id"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| !value.trim().is_empty());
    if !has_id {
        return Err(MetadataError::InvalidInput(
            "entity payload must define string `@id`".to_string(),
        ));
    }
    let entity_type = object
        .get("@type")
        .or_else(|| object.get("type"))
        .ok_or_else(|| {
            MetadataError::InvalidInput("entity payload must define `@type`".to_string())
        })?;
    let has_type = match entity_type {
        serde_json::Value::String(value) => !value.trim().is_empty(),
        serde_json::Value::Array(values) => {
            !values.is_empty()
                && values
                    .iter()
                    .all(|value| value.as_str().is_some_and(|value| !value.trim().is_empty()))
        }
        _ => false,
    };
    if !has_type {
        return Err(MetadataError::InvalidInput(
            "entity `@type` must be a string or non-empty string array".to_string(),
        ));
    }
    let has_name = object
        .get("name")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| !value.trim().is_empty());
    if !has_name {
        return Err(MetadataError::InvalidInput(
            "entity payload must define string `name`".to_string(),
        ));
    }
    Ok(())
}

impl Operation for UpdateDocumentOperation {
    type Output = MetadataRegistryRecord;
    type Error = UpdateDocumentError;

    fn start(&mut self) -> Effects {
        if self.event_id.is_nil() {
            self.event_id = self.phase_source.next_id();
        }
        self.state = UpdateDocumentState::ReadCurrent;
        smallvec![read_registry_effect(
            self.config.group_id,
            self.config.document_id,
            None
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            UpdateDocumentState::ReadCurrent => self.read_current(event),
            UpdateDocumentState::ReadRealmConfig => self.read_realm_config(event),
            UpdateDocumentState::PlanBatch => self.plan_batch(event),
            UpdateDocumentState::StartTransaction => self.start_transaction(event),
            UpdateDocumentState::ReadFence => self.read_fence(event),
            UpdateDocumentState::ReadRawFence => self.read_raw_fence(event),
            UpdateDocumentState::ReadRawEvents => self.read_raw_events(event),
            UpdateDocumentState::WriteUpdateBatch => self.write_update_batch(event),
            UpdateDocumentState::CommitTransaction => self.commit_transaction(event),
            UpdateDocumentState::ScheduleMaterializationDrain => {
                self.finish_materialization_drain(event)
            }
            UpdateDocumentState::ScheduleOutboxDrain => self.schedule_outbox_drain(event),
            UpdateDocumentState::Finish
            | UpdateDocumentState::Error
            | UpdateDocumentState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            UpdateDocumentState::Finish | UpdateDocumentState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(UpdateDocumentError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::document::{
        DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentOutboxRecord,
    };
    use aruna_core::keyspaces::{
        SYNC_OUTBOX_KEYSPACE, SYNC_REVISION_KEYSPACE, METADATA_AUDIT_KEYSPACE,
        DOCUMENT_INDEX_KEYSPACE, DOCUMENT_LIFECYCLE_KEYSPACE,
        EVENT_LOG_KEYSPACE, METADATA_INDEX_KEYSPACE,
        DOCUMENT_JOB_KEYSPACE, MATERIALIZATION_JOB_KEYSPACE,
        MATERIALIZATION_STATUS_KEYSPACE, RAW_BUDGET_KEYSPACE,
    };
    use aruna_core::storage_entries::{
        create_acceptance_key, event_log_key, metadata_registry_key, raw_budget_key,
        sync_revision_key,
    };
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::placement::placement_record::PlacementRef;
    use aruna_core::structs::identity::realm::RealmId;

    fn actor() -> Actor {
        let realm_id = RealmId::from_bytes([9u8; 32]);
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::from_parts(1, 1), realm_id),
            realm_id,
        }
    }

    fn record(actor: &Actor) -> MetadataRegistryRecord {
        let group_id = Ulid::from_parts(2, 2);
        let document_id = Ulid::from_parts(3, 3);
        let document_path = "datasets/update-atomicity";
        MetadataRegistryRecord {
            realm_id: actor.realm_id,
            group_id,
            document_id,
            document_path: document_path.to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: false,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &actor.realm_id,
                group_id,
                document_path,
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: vec![actor.node_id],
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from_parts(1, 1),
            last_event_id: Ulid::from_parts(1, 1),
        }
    }

    fn replace_jsonld(document_id: Ulid, name: &str) -> String {
        format!(
            r#"{{
  "@context": "https://w3id.org/ro/crate/1.2/context",
  "@graph": [
    {{
      "@id": "ro-crate-metadata.json",
      "@type": "CreativeWork",
      "conformsTo": {{"@id": "https://w3id.org/ro/crate/1.2"}},
      "about": {{"@id": "https://w3id.org/aruna/{document_id}"}}
    }},
    {{
      "@id": "https://w3id.org/aruna/{document_id}",
      "@type": "Dataset",
      "name": "{name}",
      "description": "Updated atomically",
      "datePublished": "2026-01-01",
      "license": {{"@id": "https://creativecommons.org/licenses/by/4.0/"}}
    }}
  ]
}}"#
        )
    }

    fn config(
        actor: Actor,
        record: &MetadataRegistryRecord,
        mutation: UpdateDocumentMutation,
    ) -> UpdateDocumentConfig {
        UpdateDocumentConfig {
            actor,
            group_id: record.group_id,
            document_id: record.document_id,
            public: true,
            mutation,
        }
    }

    fn registry_read(record: &MetadataRegistryRecord) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: metadata_registry_key(record.group_id, record.document_id),
            value: Some(postcard::to_allocvec(record).unwrap().into()),
        })
    }

    fn realm_config_read(record: &MetadataRegistryRecord) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(*record.realm_id.as_bytes()),
            value: None,
        })
    }

    fn create_event(record: &MetadataRegistryRecord) -> MetadataEventRecord {
        let node_id = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        MetadataEventRecord {
            event_id: record.establishing_event_id,
            record: record.clone(),
            user_id: aruna_core::UserId::local(Ulid::from_parts(2, 2), record.realm_id),
            node_id,
            payload: MetadataEventPayload::Scaffold {
                name: "name".to_string(),
                description: "description".to_string(),
                date_published: "date".to_string(),
                license: None,
            },
            occurred_at_ms: record.created_at_ms,
        }
    }

    fn budget(record: &MetadataRegistryRecord, events: u32, encoded_bytes: u64) -> RawOriginBudget {
        let create = create_event(record);
        let create_bytes = postcard::experimental::serialized_size(&create).unwrap() as u64;
        let mut budget = raw_quotas(
            record.document_id,
            &record.holder_node_ids,
            actor().node_id,
            create_bytes,
        )
        .unwrap()
        .into_iter()
        .find(|budget| budget.node_id == actor().node_id)
        .unwrap();
        budget.events = events;
        budget.encoded_bytes = encoded_bytes;
        budget
    }

    fn raw_read_for(
        record: &MetadataRegistryRecord,
        node_id: aruna_core::NodeId,
        budget: Option<RawOriginBudget>,
    ) -> Event {
        let create = create_event(record);
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    raw_budget_key(record.document_id, node_id),
                    budget.map(|budget| postcard::to_allocvec(&budget).unwrap().into()),
                ),
                (
                    create_acceptance_key(record.document_id),
                    Some(postcard::to_allocvec(&create).unwrap().into()),
                ),
            ],
        })
    }

    fn raw_read(record: &MetadataRegistryRecord, budget: Option<RawOriginBudget>) -> Event {
        raw_read_for(record, actor().node_id, budget)
    }

    fn raw_budget_read(record: &MetadataRegistryRecord, events: u32, encoded_bytes: u64) -> Event {
        raw_read(record, Some(budget(record, events, encoded_bytes)))
    }

    fn raw_missing_budget(record: &MetadataRegistryRecord) -> Event {
        raw_read(record, None)
    }

    fn raw_missing_for(record: &MetadataRegistryRecord, node_id: aruna_core::NodeId) -> Event {
        raw_read_for(record, node_id, None)
    }

    fn raw_events(record: &MetadataRegistryRecord) -> Event {
        let create = create_event(record);
        Event::Storage(StorageEvent::IterResult {
            values: vec![(
                event_log_key(record.document_id, create.event_id),
                postcard::to_allocvec(&create).unwrap().into(),
            )],
            next_start_after: None,
        })
    }

    fn assert_no_mutation(effects: &[Effect]) {
        for effect in effects {
            match effect {
                Effect::Metadata(MetadataEffect::ApplyRoCrate { .. })
                | Effect::Metadata(MetadataEffect::UpsertDataEntity { .. })
                | Effect::Metadata(MetadataEffect::UpsertContextualEntity { .. })
                | Effect::Metadata(MetadataEffect::MergeBatch { .. })
                | Effect::Metadata(MetadataEffect::SyncBestEffort { .. }) => {
                    panic!("unexpected graph mutation or sync effect: {effect:?}");
                }
                _ => {}
            }
        }
    }

    fn batch_planned(record: &MetadataRegistryRecord) -> Event {
        Event::Metadata(MetadataEvent::BatchPlanned {
            graph_iri: record.graph_iri.clone(),
            batch: MetadataBatch {
                graph_iri: record.graph_iri.clone(),
                actor: [7u8; 32],
                counter: 1,
                base_clock: craqle::VectorClock::default(),
                ops: Vec::new(),
                timestamp_millis: 1,
            },
        })
    }

    fn assert_plan_batch(effects: &[Effect]) {
        let [Effect::Metadata(MetadataEffect::PlanBatch { .. })] = effects else {
            panic!("expected batch planning before transaction, got {effects:?}");
        };
    }

    fn is_replace(payload: &MetadataEventPayload) -> bool {
        matches!(
            payload,
            MetadataEventPayload::ApplyBatch {
                authored: MetadataBatchSource::ReplaceRoCrate { .. },
                ..
            }
        )
    }

    fn is_data_upsert(payload: &MetadataEventPayload) -> bool {
        matches!(
            payload,
            MetadataEventPayload::ApplyBatch {
                authored: MetadataBatchSource::UpsertDataEntity { .. },
                ..
            }
        )
    }

    fn assert_start_transaction(effects: &[Effect]) {
        let [Effect::Storage(StorageEffect::StartTransaction { read: false })] = effects else {
            panic!("expected write transaction start, got {effects:?}");
        };
    }

    fn assert_update_batch(
        effects: &[Effect],
        txn_id: TxnId,
        expected_payload: impl FnOnce(&MetadataEventPayload) -> bool,
    ) -> MetadataEventRecord {
        let [
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(write_txn_id),
            }),
        ] = effects
        else {
            panic!("expected update batch write, got {effects:?}");
        };
        assert_eq!(*write_txn_id, txn_id);
        for keyspace in [
            EVENT_LOG_KEYSPACE,
            METADATA_INDEX_KEYSPACE,
            DOCUMENT_INDEX_KEYSPACE,
            METADATA_AUDIT_KEYSPACE,
            SYNC_OUTBOX_KEYSPACE,
            SYNC_REVISION_KEYSPACE,
            DOCUMENT_LIFECYCLE_KEYSPACE,
            MATERIALIZATION_STATUS_KEYSPACE,
            MATERIALIZATION_JOB_KEYSPACE,
            DOCUMENT_JOB_KEYSPACE,
            RAW_BUDGET_KEYSPACE,
        ] {
            assert!(
                writes
                    .iter()
                    .any(|(entry_keyspace, _, _)| entry_keyspace == keyspace),
                "missing keyspace {keyspace} in update batch: {writes:?}"
            );
        }
        let event = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == EVENT_LOG_KEYSPACE)
            .map(|(_, _, value)| {
                postcard::from_bytes::<MetadataEventRecord>(value).expect("update event decodes")
            })
            .expect("event log write exists");
        assert!(expected_payload(&event.payload));
        let outbox = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == SYNC_OUTBOX_KEYSPACE)
            .map(|(_, _, value)| {
                postcard::from_bytes::<DocumentOutboxRecord>(value).expect("outbox record decodes")
            })
            .expect("outbox write exists");
        assert_eq!(outbox.outbox_id, event.event_id);
        assert!(matches!(outbox.event, DocumentOutboxEvent::Upsert { .. }));
        let (revision_key, revision): (_, DocumentChange) = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == SYNC_REVISION_KEYSPACE)
            .map(|(_, key, value)| {
                (
                    key,
                    postcard::from_bytes(value).expect("revision sidecar decodes"),
                )
            })
            .expect("revision sidecar write exists");
        assert_eq!(revision_key, &sync_revision_key(&outbox.target));
        assert_eq!(revision.current.event_id, event.event_id);
        assert_eq!(revision.current.actor, event.node_id);
        assert_eq!(revision.current.generation, event.record.updated_at_ms);
        assert_eq!(revision.kind, DocumentChangeKind::Upsert);
        let lifecycle = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == DOCUMENT_LIFECYCLE_KEYSPACE)
            .map(|(_, _, value)| {
                postcard::from_bytes::<MetadataLifecycleRecord>(value)
                    .expect("lifecycle source decodes")
            })
            .expect("lifecycle source write exists");
        assert_eq!(
            lifecycle,
            MetadataLifecycleRecord::Upsert {
                event: Box::new(event.clone())
            }
        );
        event
    }

    // The bucket is chosen once, by the create-receiving node; re-choosing it on
    // an update under a changed config would fork the document across topics.
    #[test]
    fn update_keeps_placement() {
        let actor = actor();
        let mut record = record(&actor);
        record.placement = PlacementRef {
            strategy_id: Ulid::from_bytes([5u8; 16]),
            shard: 11,
        };
        let txn_id = Ulid::from_parts(4, 4);
        let mut operation = UpdateDocumentOperation::new(config(
            actor.clone(),
            &record,
            UpdateDocumentMutation::ReplaceRoCrate {
                jsonld: replace_jsonld(record.document_id, "Placement Preserved"),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        operation.step(realm_config_read(&record));
        operation.step(batch_planned(&record));
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read {
                txn_id: Some(read_txn),
                ..
            })] if *read_txn == txn_id
        ));
        operation.step(registry_read(&record));
        operation.step(raw_budget_read(
            &record,
            1,
            postcard::experimental::serialized_size(&create_event(&record)).unwrap() as u64,
        ));
        let effects = operation.step(raw_events(&record));

        let event = assert_update_batch(effects.as_slice(), txn_id, is_replace);
        assert_eq!(event.record.placement, record.placement);
    }

    #[test]
    fn update_uses_fence() {
        let actor = actor();
        let record = record(&actor);
        let mut fenced = record.clone();
        fenced.placement = PlacementRef {
            strategy_id: Ulid::from_bytes([6u8; 16]),
            shard: 12,
        };
        let txn_id = Ulid::from_parts(5, 5);
        let mut operation = UpdateDocumentOperation::new(config(
            actor,
            &record,
            UpdateDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        assert_plan_batch(operation.step(realm_config_read(&record)).as_slice());
        assert_start_transaction(operation.step(batch_planned(&record)).as_slice());
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read {
                txn_id: Some(read_txn),
                ..
            })] if *read_txn == txn_id
        ));

        operation.step(registry_read(&fenced));
        operation.step(raw_budget_read(
            &record,
            1,
            postcard::experimental::serialized_size(&create_event(&record)).unwrap() as u64,
        ));
        let event = assert_update_batch(
            operation.step(raw_events(&record)).as_slice(),
            txn_id,
            is_data_upsert,
        );
        assert_eq!(event.record.placement, fenced.placement);
    }

    /// A realm whose buckets are activated at generation one, so an update
    /// resolves a generation and takes the bucket's fence.
    fn activated_config(record: &mut MetadataRegistryRecord) -> Event {
        let mut config = RealmConfigDocument::new(record.realm_id, Vec::new(), 3);
        config.ensure_node(actor().node_id, aruna_core::structs::identity::realm::RealmNodeKind::Server);
        let strategy_id = Ulid::from_bytes([5u8; 16]);
        config
            .strategies
            .push(aruna_core::structs::placement::placement_record::PlacementStrategy {
                strategy_id,
                name: "default".to_string(),
                replica_count: Some(1),
                distinct_locations: false,
                affinity: Vec::new(),
                shard_count: 16,
            });
        config.default_strategy_id = Some(strategy_id);
        config.snapshot_candidate_map();
        record.placement = PlacementRef {
            strategy_id,
            shard: 11,
        };
        Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(*record.realm_id.as_bytes()),
            value: Some(config.to_bytes(&actor()).unwrap().into()),
        })
    }

    fn fenced_raw_read(
        record: &MetadataRegistryRecord,
        budget: Option<RawOriginBudget>,
        closed: &[Option<u64>],
    ) -> Event {
        let Event::Storage(StorageEvent::BatchReadResult { mut values }) =
            raw_read_for(record, actor().node_id, budget)
        else {
            unreachable!("the raw sidecar read is a batch read");
        };
        for generation in closed {
            values.push((
                ByteView::from(b"fence".to_vec()),
                generation.map(|generation| generation.to_be_bytes().to_vec().into()),
            ));
        }
        Event::Storage(StorageEvent::BatchReadResult { values })
    }

    fn outbox_rows(effects: &[Effect]) -> Vec<aruna_core::document::DocumentOutboxRecord> {
        effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Storage(StorageEffect::BatchWrite { writes, .. }) => Some(writes),
                _ => None,
            })
            .flatten()
            .filter(|(key_space, _, _)| {
                key_space == aruna_core::keyspaces::SYNC_OUTBOX_KEYSPACE
            })
            .map(|(_, _, value)| postcard::from_bytes(value.as_ref()).expect("outbox row decodes"))
            .collect()
    }

    /// Steps an update to the fence read and answers it with `closed`.
    fn step_to_fence(
        operation: &mut UpdateDocumentOperation,
        record: &MetadataRegistryRecord,
        config: Event,
        txn_id: Ulid,
        closed: &[Option<u64>],
    ) -> Effects {
        operation.start();
        operation.step(registry_read(record));
        assert_plan_batch(operation.step(config).as_slice());
        assert_start_transaction(operation.step(batch_planned(record)).as_slice());
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let reads = operation.step(registry_read(record));
        let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = reads.as_slice() else {
            panic!("the transaction batch-reads the raw sidecar and the fences");
        };
        assert_eq!(reads.len(), 2 + closed.len(), "one read per fenced bucket");
        let budget = budget(
            record,
            1,
            postcard::experimental::serialized_size(&create_event(record)).unwrap() as u64,
        );
        operation.step(fenced_raw_read(record, Some(budget), closed))
    }

    #[test]
    fn update_takes_bucket() {
        // An admitted update stamps the generation it resolved at onto every
        // outbox row it commits, so the drain can bound the predecessor set.
        let actor = actor();
        let mut record = record(&actor);
        let realm_config = activated_config(&mut record);
        let txn_id = Ulid::from_parts(6, 6);
        let mut operation = UpdateDocumentOperation::new(config(
            actor,
            &record,
            UpdateDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        let effects = step_to_fence(&mut operation, &record, realm_config, txn_id, &[None, None]);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { .. })]
        ));
        let rows = outbox_rows(operation.step(raw_events(&record)).as_slice());
        assert!(!rows.is_empty(), "an admitted update publishes");
        for row in rows {
            assert_eq!(row.generation, 1, "row for {:?}", row.placement);
        }
    }

    #[test]
    fn closed_fence_rejects() {
        // The departing holder closed generation one: the write must not commit
        // an old-placement row after that.
        let actor = actor();
        let mut record = record(&actor);
        let realm_config = activated_config(&mut record);
        let txn_id = Ulid::from_parts(7, 7);
        let mut operation = UpdateDocumentOperation::new(config(
            actor,
            &record,
            UpdateDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        let effects = step_to_fence(
            &mut operation,
            &record,
            realm_config,
            txn_id,
            &[Some(1), None],
        );
        assert!(outbox_rows(effects.as_slice()).is_empty());
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Storage(StorageEffect::BatchWrite { .. })))
        );
        assert_eq!(
            operation.finalize().unwrap_err(),
            UpdateDocumentError::PlacementFenced
        );
    }

    #[test]
    fn rejects_raw_limit() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::from_parts(8, 8);
        let mut operation = UpdateDocumentOperation::new(config(
            actor,
            &record,
            UpdateDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        operation.step(realm_config_read(&record));
        operation.step(batch_planned(&record));
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(registry_read(&record));
        operation.step(raw_budget_read(
            &record,
            1,
            postcard::experimental::serialized_size(&create_event(&record)).unwrap() as u64,
        ));
        let values = (0..RAW_EVENT_LIMIT)
            .map(|index| (ByteView::from(vec![index as u8]), ByteView::from(vec![0])))
            .collect();
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: Some(ByteView::from(vec![1])),
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));
        assert_eq!(operation.finalize(), Err(UpdateDocumentError::RawLimit));
    }

    #[test]
    fn accepts_raw_history() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::from_parts(9, 9);
        let mut operation = UpdateDocumentOperation::new(config(
            actor,
            &record,
            UpdateDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        operation.step(realm_config_read(&record));
        operation.step(batch_planned(&record));
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(registry_read(&record));
        operation.step(raw_budget_read(
            &record,
            1,
            postcard::experimental::serialized_size(&create_event(&record)).unwrap() as u64,
        ));
        let effects = operation.step(raw_events(&record));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchWrite {
                txn_id: Some(write_txn),
                ..
            })] if *write_txn == txn_id
        ));
    }

    #[test]
    fn rebuilds_missing_budget() {
        let actor = actor();
        let record = record(&actor);
        let create = create_event(&record);
        let txn_id = Ulid::from_parts(10, 10);
        let mut operation = UpdateDocumentOperation::new(config(
            actor.clone(),
            &record,
            UpdateDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        operation.step(realm_config_read(&record));
        operation.step(batch_planned(&record));
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(registry_read(&record));
        let effects = operation.step(raw_missing_budget(&record));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter {
                limit: RAW_EVENT_LIMIT,
                start: None,
                ..
            })]
        ));
        let create_value = postcard::to_allocvec(&create).unwrap();
        let create_len = create_value.len() as u64;
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(
                event_log_key(record.document_id, create.event_id),
                create_value.clone().into(),
            )],
            next_start_after: None,
        }));
        let event = assert_update_batch(effects.as_slice(), txn_id, is_data_upsert);
        let budget_value = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::Storage(StorageEffect::BatchWrite { writes, .. }) => writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == RAW_BUDGET_KEYSPACE)
                    .map(|(_, _, value)| value),
                _ => None,
            })
            .expect("rebuilt budget write exists");
        let budget: RawOriginBudget = postcard::from_bytes(budget_value).unwrap();
        assert_eq!(budget.events, 2);
        assert_eq!(
            budget.encoded_bytes,
            create_len + postcard::experimental::serialized_size(&event).unwrap() as u64
        );
        assert_eq!(budget.node_id, actor.node_id);
        assert_eq!(event.record.document_id, record.document_id);
    }

    #[test]
    fn rejects_new_origin() {
        let creator = actor();
        let original = record(&creator);
        let mut current = original.clone();
        let mut outsider = creator.clone();
        outsider.node_id = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        current.holder_node_ids = vec![outsider.node_id];
        let txn_id = Ulid::from_parts(11, 11);
        let mut operation = UpdateDocumentOperation::new(config(
            outsider.clone(),
            &current,
            UpdateDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&current));
        operation.step(realm_config_read(&current));
        operation.step(batch_planned(&current));
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(registry_read(&current));
        let effects = operation.step(raw_missing_for(&original, outsider.node_id));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));
        assert_eq!(operation.finalize(), Err(UpdateDocumentError::RawLimit));
    }

    #[test]
    fn rejects_origin_budget() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::from_parts(12, 12);
        let mut operation = UpdateDocumentOperation::new(config(
            actor,
            &record,
            UpdateDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        operation.step(realm_config_read(&record));
        operation.step(batch_planned(&record));
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(registry_read(&record));
        // The exhausted budget is rejected at the sidecar fence read itself.
        let effects = operation.step(raw_budget_read(&record, EVENT_LIMIT, 0));

        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                    if *abort_txn == txn_id
            ),
            "expected raw-limit abort, got {effects:?}"
        );
        assert_eq!(operation.finalize(), Err(UpdateDocumentError::RawLimit));
    }

    #[test]
    fn update_fence_missing() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::from_parts(13, 13);
        let mut operation = UpdateDocumentOperation::new(config(
            actor,
            &record,
            UpdateDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        assert_plan_batch(operation.step(realm_config_read(&record)).as_slice());
        assert_start_transaction(operation.step(batch_planned(&record)).as_slice());
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: metadata_registry_key(record.group_id, record.document_id),
            value: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));
        assert_eq!(
            operation.finalize(),
            Err(UpdateDocumentError::DocumentNotFound)
        );
    }

    #[test]
    fn replace_validates_commits() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::from_parts(14, 14);
        let mut operation = UpdateDocumentOperation::new(config(
            actor,
            &record,
            UpdateDocumentMutation::ReplaceRoCrate {
                jsonld: replace_jsonld(record.document_id, "Atomic Replace"),
            },
        ));

        assert_no_mutation(operation.start().as_slice());
        let effects = operation.step(registry_read(&record));
        assert_no_mutation(effects.as_slice());
        let effects = operation.step(realm_config_read(&record));
        let [Effect::Metadata(MetadataEffect::PlanBatch { graph_iri, .. })] = effects.as_slice()
        else {
            panic!("expected batch planning before transaction, got {effects:?}");
        };
        assert_eq!(*graph_iri, record.graph_iri);

        let effects = operation.step(batch_planned(&record));
        assert_start_transaction(effects.as_slice());

        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_no_mutation(effects.as_slice());
        let effects = operation.step(registry_read(&record));
        assert_no_mutation(effects.as_slice());
        let effects = operation.step(raw_budget_read(
            &record,
            1,
            postcard::experimental::serialized_size(&create_event(&record)).unwrap() as u64,
        ));
        assert_no_mutation(effects.as_slice());
        let effects = operation.step(raw_events(&record));
        assert_no_mutation(effects.as_slice());
        assert_update_batch(effects.as_slice(), txn_id, is_replace);
    }

    #[test]
    fn entity_upsert_appends() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::from_parts(15, 15);
        let mut operation = UpdateDocumentOperation::new(config(
            actor,
            &record,
            UpdateDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        let effects = operation.step(registry_read(&record));
        assert_no_mutation(effects.as_slice());
        let effects = operation.step(realm_config_read(&record));
        assert_no_mutation(effects.as_slice());
        assert_plan_batch(effects.as_slice());
        assert_start_transaction(operation.step(batch_planned(&record)).as_slice());

        let _effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(registry_read(&record));
        operation.step(raw_budget_read(
            &record,
            1,
            postcard::experimental::serialized_size(&create_event(&record)).unwrap() as u64,
        ));
        let effects = operation.step(raw_events(&record));
        let event = assert_update_batch(effects.as_slice(), txn_id, is_data_upsert);
        assert_eq!(event.record.last_event_id, event.event_id);
    }

    #[test]
    fn rejects_no_holders() {
        let actor = actor();
        let mut record = record(&actor);
        record.holder_node_ids.clear();
        let txn_id = Ulid::from_parts(16, 16);
        let mut operation = UpdateDocumentOperation::new(config(
            actor,
            &record,
            UpdateDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        assert_plan_batch(operation.step(realm_config_read(&record)).as_slice());
        assert_start_transaction(operation.step(batch_planned(&record)).as_slice());

        let _effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(registry_read(&record));
        let effects = operation.step(raw_missing_budget(&record));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));
        assert_eq!(operation.finalize(), Err(UpdateDocumentError::RawLimit));
    }

    #[test]
    fn denial_aborts() {
        // The realm-config phase is where an invalid entity payload is denied
        // before the transaction opens: no forbidden mutation effect may escape.
        let actor = actor();
        let record = record(&actor);
        let mut operation = UpdateDocumentOperation::new(config(
            actor,
            &record,
            UpdateDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"name":"missing identity"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        let effects = operation.read_realm_config(realm_config_read(&record));
        assert_no_mutation(effects.as_slice());
        assert_eq!(
            operation.finalize(),
            Err(UpdateDocumentError::MetadataError(
                MetadataError::InvalidInput("entity payload must define string `@id`".to_string())
            ))
        );
    }

    #[test]
    fn commit_preserves_graph() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::from_parts(17, 17);
        let mut operation = UpdateDocumentOperation::new(config(
            actor,
            &record,
            UpdateDocumentMutation::ReplaceRoCrate {
                jsonld: replace_jsonld(record.document_id, "Commit Failure"),
            },
        ));

        assert_no_mutation(operation.start().as_slice());
        let effects = operation.step(registry_read(&record));
        assert_no_mutation(effects.as_slice());
        let effects = operation.step(realm_config_read(&record));
        assert_no_mutation(effects.as_slice());
        let effects = operation.step(batch_planned(&record));
        assert_no_mutation(effects.as_slice());
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_no_mutation(effects.as_slice());
        let effects = operation.step(registry_read(&record));
        assert_no_mutation(effects.as_slice());
        let effects = operation.step(raw_budget_read(
            &record,
            1,
            postcard::experimental::serialized_size(&create_event(&record)).unwrap() as u64,
        ));
        assert_no_mutation(effects.as_slice());
        let effects = operation.step(raw_events(&record));
        assert_no_mutation(effects.as_slice());
        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert_no_mutation(effects.as_slice());

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: aruna_core::errors::StorageError::WriteError("boom".to_string()),
        }));

        assert_no_mutation(effects.as_slice());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(UpdateDocumentError::StorageError(
                aruna_core::errors::StorageError::WriteError("boom".to_string())
            ))
        );
    }
    const FIXED_PHASE_MS: u64 = 1_700_000_000_000;
    const FIXED_RECORD_ID: [u8; 16] = [0x7b; 16];

    fn fixed_phase_ms() -> u64 {
        FIXED_PHASE_MS
    }

    fn fixed_phase_id() -> Ulid {
        Ulid::from_bytes(FIXED_RECORD_ID)
    }

    fn fixed_phase_source() -> crate::metadata::MetadataPhaseSource {
        crate::metadata::MetadataPhaseSource::fixed(fixed_phase_ms, fixed_phase_id)
    }

    fn fixed_mutation(record: &MetadataRegistryRecord) -> UpdateDocumentMutation {
        UpdateDocumentMutation::UpsertDataEntity {
            jsonld: replace_jsonld(record.document_id, "fixed"),
        }
    }

    /// The update trace takes its event identity and phase time from the
    /// injected source, so identical inputs produce identical records.
    #[test]
    fn update_source_pinned() {
        let mut record = record(&actor());
        record.document_id = Ulid::from_bytes([0x31; 16]);
        record.graph_iri = MetadataRegistryRecord::graph_iri_for(record.document_id);
        record.permission_path = MetadataRegistryRecord::permission_path_for(
            &record.realm_id,
            record.group_id,
            &record.document_path,
            record.document_id,
        );
        let first_config = config(actor(), &record, fixed_mutation(&record));

        let operation =
            UpdateDocumentOperation::new(first_config).with_phase_source(fixed_phase_source());

        assert_eq!(operation.event_id, fixed_phase_id());
        let updated = operation.updated_record(record.clone());
        assert_eq!(updated.updated_at_ms, FIXED_PHASE_MS);

        let replay =
            UpdateDocumentOperation::new(config(actor(), &record, fixed_mutation(&record)))
                .with_phase_source(fixed_phase_source());
        assert_eq!(replay.updated_record(record), updated);
    }

    /// A wrong event is an explicit failure, not a silent state change.
    #[test]
    fn rejects_wrong_event() {
        let record = record(&actor());
        let mut operation =
            UpdateDocumentOperation::new(config(actor(), &record, fixed_mutation(&record)))
                .with_phase_source(fixed_phase_source());
        operation.state = UpdateDocumentState::WriteUpdateBatch;

        let effects = operation.step(Event::Storage(StorageEvent::SyncAllFinished));

        assert!(effects.is_empty());
        assert!(matches!(
            operation.output,
            Some(Err(UpdateDocumentError::UnexpectedEvent { .. }))
        ));
        assert_eq!(operation.state, UpdateDocumentState::Error);
    }
}
