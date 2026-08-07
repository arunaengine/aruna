use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    METADATA_CREATE_ACCEPTANCE_KEYSPACE, METADATA_EVENT_LOG_KEYSPACE, METADATA_RAW_BUDGET_KEYSPACE,
    REALM_CONFIG_KEYSPACE,
};
use aruna_core::metadata::{
    METADATA_RAW_BYTES_LIMIT, METADATA_RAW_EVENT_LIMIT, MetadataApplyRoCrateRequest,
    MetadataCreateEventPayload, MetadataCreateEventRecord, MetadataDocumentLifecycleRecord,
    MetadataEffect, MetadataError, MetadataEvent, MetadataGraphPolicy, MetadataRawOriginBudget,
    MetadataRequestDurability, raw_quotas,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{
    document_sync_revision_write_entry, metadata_create_acceptance_key,
    metadata_document_lifecycle_write_entry, metadata_event_log_key, metadata_event_log_prefix,
    raw_budget_entry, raw_budget_key,
};
use aruna_core::structs::{MetadataAuditRecord, MetadataRegistryRecord, RealmConfigDocument};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId, TxnId};
use byteview::ByteView;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::document_sync_outbox::{outbox_write_entry, schedule_outbox_drain_effect};
use crate::driver::{DriverContext, drive};
use crate::metadata::materialization_queue::{
    new_materialization_job, new_pending_materialization_status,
    schedule_metadata_materialization_drain_effect,
};
use crate::metadata::projector::{create_event_outbox_record, registry_outbox_record};
use crate::metadata::repository::{
    StorageReadError, metadata_event_projection_write_entries, parse_registry_read,
    read_registry_effect,
};
use crate::sync_placement::sort_node_ids;

const RAW_EVENT_LIMIT: usize = METADATA_RAW_EVENT_LIMIT as usize;

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateMetadataDocumentConfig {
    pub actor: aruna_core::structs::Actor,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub public: bool,
    pub mutation: UpdateMetadataDocumentMutation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UpdateMetadataDocumentMutation {
    ReplaceRoCrate { jsonld: String },
    UpsertDataEntity { jsonld: String },
    UpsertContextualEntity { jsonld: String },
}

/// Validates a metadata update and persists the event plus projection work.
///
/// A successful operation means the update has been accepted into the durable
/// event/projection pipeline. Graph materialization and replica convergence may
/// still be pending.
#[derive(Debug, PartialEq)]
pub struct UpdateMetadataDocumentOperation {
    config: UpdateMetadataDocumentConfig,
    txn_id: Option<TxnId>,
    record: Option<MetadataRegistryRecord>,
    update_event: Option<MetadataCreateEventRecord>,
    raw_budget: Option<MetadataRawOriginBudget>,
    next_raw_budget: Option<MetadataRawOriginBudget>,
    accepted_create: Option<MetadataCreateEventRecord>,
    realm_config: Option<RealmConfigDocument>,
    state: UpdateMetadataDocumentState,
    output: Option<Result<MetadataRegistryRecord, UpdateMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum UpdateMetadataDocumentState {
    Init,
    ReadCurrent,
    ReadRealmConfig,
    ValidateMutation,
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
pub enum UpdateMetadataDocumentError {
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
    #[error("metadata raw update budget exceeded")]
    RawLimit,
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl UpdateMetadataDocumentOperation {
    pub fn new(config: UpdateMetadataDocumentConfig) -> Self {
        Self {
            config,
            txn_id: None,
            record: None,
            update_event: None,
            raw_budget: None,
            next_raw_budget: None,
            accepted_create: None,
            realm_config: None,
            state: UpdateMetadataDocumentState::Init,
            output: None,
        }
    }

    fn current_timestamp_ms() -> u64 {
        u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default()
    }

    fn graph_policy(&self, record: &MetadataRegistryRecord) -> MetadataGraphPolicy {
        MetadataGraphPolicy {
            public: self.config.public,
            permission_paths: vec![record.permission_path.clone()],
        }
        .normalized()
    }

    fn updated_record(&self, mut record: MetadataRegistryRecord) -> MetadataRegistryRecord {
        record.public = self.config.public;
        record.updated_at_ms = Self::current_timestamp_ms();
        record
    }

    fn update_event_payload(&self) -> MetadataCreateEventPayload {
        match &self.config.mutation {
            UpdateMetadataDocumentMutation::ReplaceRoCrate { jsonld } => {
                MetadataCreateEventPayload::ReplaceRoCrate {
                    jsonld: jsonld.clone(),
                }
            }
            UpdateMetadataDocumentMutation::UpsertDataEntity { jsonld } => {
                MetadataCreateEventPayload::UpsertDataEntity {
                    jsonld: jsonld.clone(),
                }
            }
            UpdateMetadataDocumentMutation::UpsertContextualEntity { jsonld } => {
                MetadataCreateEventPayload::UpsertContextualEntity {
                    jsonld: jsonld.clone(),
                }
            }
        }
    }

    fn update_event_record(&self, record: &MetadataRegistryRecord) -> MetadataCreateEventRecord {
        let event_id = Ulid::generate();
        let mut record = record.clone();
        record.last_event_id = event_id;
        let occurred_at_ms = record.updated_at_ms;
        MetadataCreateEventRecord {
            event_id,
            record,
            user_id: self.config.actor.user_id,
            node_id: self.config.actor.node_id,
            payload: self.update_event_payload(),
            occurred_at_ms,
        }
    }

    fn audit_record(&self, event: &MetadataCreateEventRecord) -> MetadataAuditRecord {
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

    fn validation_effect(
        &self,
        record: &MetadataRegistryRecord,
    ) -> Result<Option<Effect>, MetadataError> {
        match &self.config.mutation {
            UpdateMetadataDocumentMutation::ReplaceRoCrate { jsonld } => {
                Ok(Some(Effect::Metadata(MetadataEffect::ValidateRoCrate {
                    request: MetadataApplyRoCrateRequest {
                        graph_iri: record.graph_iri.clone(),
                        jsonld: jsonld.clone(),
                        policy: self.graph_policy(record),
                        durability: MetadataRequestDurability::WalAlreadyDurable,
                        deterministic_actor: None,
                    },
                })))
            }
            UpdateMetadataDocumentMutation::UpsertDataEntity { jsonld }
            | UpdateMetadataDocumentMutation::UpsertContextualEntity { jsonld } => {
                validate_entity_jsonld(jsonld)?;
                Ok(None)
            }
        }
    }

    fn begin_transaction_effect(&mut self) -> Effects {
        self.state = UpdateMetadataDocumentState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn write_update_batch_effect(
        &self,
        txn_id: TxnId,
    ) -> Result<Effect, UpdateMetadataDocumentError> {
        let Some(event) = self.update_event.as_ref() else {
            return Err(UpdateMetadataDocumentError::MissingTransaction);
        };
        let now = Self::current_timestamp_ms();
        let audit = self.audit_record(event);
        // Updating an existing document is a mutation, not an origin write, so it
        // never mints the lifecycle sync topic genesis.
        let lifecycle_outbox = create_event_outbox_record(event, self.realm_config.as_ref(), false);
        let outbox = (!event.record.holder_node_ids.is_empty()).then_some(&lifecycle_outbox);
        let status = new_pending_materialization_status(event, now);
        let job = new_materialization_job(event, now);
        let mut writes =
            metadata_event_projection_write_entries(event, &audit, outbox, &status, &job)?;
        // Refresh the everywhere-bound registry row so non-holders see the new
        // revision, not just the bucket's holders.
        if let Some(registry_outbox) =
            registry_outbox_record(event, self.realm_config.as_ref(), false)
        {
            writes.push(
                outbox_write_entry(&registry_outbox)
                    .map_err(aruna_core::errors::ConversionError::from)?,
            );
        }
        let lifecycle = MetadataDocumentLifecycleRecord::Upsert {
            event: Box::new(event.clone()),
        };
        writes.push(metadata_document_lifecycle_write_entry(&lifecycle)?);
        if outbox.is_none() {
            let aruna_core::document::DocumentSyncOutboxEvent::Upsert { change, .. } =
                lifecycle_outbox.event
            else {
                unreachable!("metadata lifecycle update outbox must be an upsert");
            };
            writes.push(document_sync_revision_write_entry(
                &lifecycle_outbox.target,
                &change,
            )?);
        }
        let Some(raw_budget) = self.next_raw_budget.as_ref() else {
            return Err(UpdateMetadataDocumentError::RawLimit);
        };
        writes.push(raw_budget_entry(raw_budget)?);
        Ok(Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        }))
    }

    fn fail(&mut self, error: UpdateMetadataDocumentError) -> Effects {
        let cleanup = self.abort();
        self.state = UpdateMetadataDocumentState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(UpdateMetadataDocumentError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }

    fn origin_quota(
        &self,
        event: &MetadataCreateEventRecord,
    ) -> Result<MetadataRawOriginBudget, UpdateMetadataDocumentError> {
        if event.record.document_id != self.config.document_id
            || event.record.establishing_event_id != event.event_id
            || event.record.last_event_id != event.event_id
            || !matches!(
                event.payload,
                MetadataCreateEventPayload::Scaffold { .. }
                    | MetadataCreateEventPayload::RoCrate { .. }
            )
        {
            return Err(UpdateMetadataDocumentError::RawLimit);
        }
        let mut origins = event.record.holder_node_ids.clone();
        if origins.is_empty() {
            return Err(UpdateMetadataDocumentError::RawLimit);
        }
        let original = origins.clone();
        sort_node_ids(&mut origins);
        if origins != original {
            return Err(UpdateMetadataDocumentError::RawLimit);
        }
        let encoded_bytes = postcard::serialized_size(event)
            .map_err(|_| UpdateMetadataDocumentError::RawLimit)
            .and_then(|size| {
                u64::try_from(size).map_err(|_| UpdateMetadataDocumentError::RawLimit)
            })?;
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
        .ok_or(UpdateMetadataDocumentError::RawLimit)
    }

    fn valid_budget(
        &self,
        budget: &MetadataRawOriginBudget,
        quota: &MetadataRawOriginBudget,
    ) -> bool {
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
    ) -> Result<(MetadataRawOriginBudget, usize, u64), UpdateMetadataDocumentError> {
        if values.len() > RAW_EVENT_LIMIT || next_start_after.is_some() {
            return Err(UpdateMetadataDocumentError::RawLimit);
        }
        let Some(create) = self.accepted_create.as_ref() else {
            return Err(UpdateMetadataDocumentError::RawLimit);
        };
        let quota = self.origin_quota(create)?;
        let mut events = 0u32;
        let mut encoded_bytes = 0u64;
        let mut total_bytes = 0u64;
        let mut saw_create = false;
        for (key, value) in values {
            let event: MetadataCreateEventRecord =
                postcard::from_bytes(value).map_err(|_| UpdateMetadataDocumentError::RawLimit)?;
            if key != &metadata_event_log_key(self.config.document_id, event.event_id)
                || event.record.document_id != self.config.document_id
            {
                return Err(UpdateMetadataDocumentError::RawLimit);
            }
            let value_len =
                u64::try_from(value.len()).map_err(|_| UpdateMetadataDocumentError::RawLimit)?;
            total_bytes = total_bytes
                .checked_add(value_len)
                .ok_or(UpdateMetadataDocumentError::RawLimit)?;
            if total_bytes > METADATA_RAW_BYTES_LIMIT {
                return Err(UpdateMetadataDocumentError::RawLimit);
            }
            if &event == create {
                saw_create = true;
            }
            if event.node_id == self.config.actor.node_id {
                events = events
                    .checked_add(1)
                    .ok_or(UpdateMetadataDocumentError::RawLimit)?;
                encoded_bytes = encoded_bytes
                    .checked_add(value_len)
                    .ok_or(UpdateMetadataDocumentError::RawLimit)?;
            }
        }
        if !saw_create || events > quota.event_limit || encoded_bytes > quota.byte_limit {
            return Err(UpdateMetadataDocumentError::RawLimit);
        }
        Ok((
            MetadataRawOriginBudget {
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
    ) -> Result<MetadataRawOriginBudget, UpdateMetadataDocumentError> {
        let Some(event) = self.update_event.as_ref() else {
            return Err(UpdateMetadataDocumentError::MissingTransaction);
        };
        if history_events >= RAW_EVENT_LIMIT {
            return Err(UpdateMetadataDocumentError::RawLimit);
        }
        let Some(budget) = self.raw_budget.as_ref() else {
            return Err(UpdateMetadataDocumentError::RawLimit);
        };
        let Some(create) = self.accepted_create.as_ref() else {
            return Err(UpdateMetadataDocumentError::RawLimit);
        };
        let quota = self.origin_quota(create)?;
        if !self.valid_budget(budget, &quota) || budget.events >= budget.event_limit {
            return Err(UpdateMetadataDocumentError::RawLimit);
        }
        let event_bytes =
            postcard::serialized_size(event).map_err(aruna_core::errors::ConversionError::from)?;
        let event_bytes =
            u64::try_from(event_bytes).map_err(|_| UpdateMetadataDocumentError::RawLimit)?;
        if history_bytes
            .checked_add(event_bytes)
            .is_none_or(|bytes| bytes > METADATA_RAW_BYTES_LIMIT)
        {
            return Err(UpdateMetadataDocumentError::RawLimit);
        }
        let encoded_bytes = budget
            .encoded_bytes
            .checked_add(event_bytes)
            .ok_or(UpdateMetadataDocumentError::RawLimit)?;
        if encoded_bytes > budget.byte_limit {
            return Err(UpdateMetadataDocumentError::RawLimit);
        }
        Ok(MetadataRawOriginBudget {
            document_id: budget.document_id,
            node_id: budget.node_id,
            event_limit: budget.event_limit,
            byte_limit: budget.byte_limit,
            events: budget
                .events
                .checked_add(1)
                .ok_or(UpdateMetadataDocumentError::RawLimit)?,
            encoded_bytes,
        })
    }
}

pub async fn update_metadata_document(
    operation: UpdateMetadataDocumentOperation,
    context: &DriverContext,
) -> Result<MetadataRegistryRecord, UpdateMetadataDocumentError> {
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

impl Operation for UpdateMetadataDocumentOperation {
    type Output = MetadataRegistryRecord;
    type Error = UpdateMetadataDocumentError;

    fn start(&mut self) -> Effects {
        self.state = UpdateMetadataDocumentState::ReadCurrent;
        smallvec![read_registry_effect(
            self.config.group_id,
            self.config.document_id,
            None
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            UpdateMetadataDocumentState::ReadCurrent => match parse_registry_read(event) {
                Ok(Some(record)) => {
                    self.record = Some(record.clone());
                    self.state = UpdateMetadataDocumentState::ReadRealmConfig;
                    smallvec![Effect::Storage(StorageEffect::Read {
                        key_space: REALM_CONFIG_KEYSPACE.to_string(),
                        key: ByteView::from(*record.realm_id.as_bytes()),
                        txn_id: None,
                    })]
                }
                Ok(None) => self.fail(UpdateMetadataDocumentError::DocumentNotFound),
                Err(StorageReadError::Storage(error)) => self.fail(error.into()),
                Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
            },
            UpdateMetadataDocumentState::ReadRealmConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    if let Some(bytes) = value {
                        match RealmConfigDocument::from_bytes(&bytes) {
                            Ok(config) => self.realm_config = Some(config),
                            Err(error) => return self.fail(error.into()),
                        }
                    }
                    let Some(record) = self.record.clone() else {
                        return self.fail(UpdateMetadataDocumentError::DocumentNotFound);
                    };
                    match self.validation_effect(&record) {
                        Ok(Some(effect)) => {
                            self.state = UpdateMetadataDocumentState::ValidateMutation;
                            smallvec![effect]
                        }
                        Ok(None) => self.begin_transaction_effect(),
                        Err(error) => self.fail(error.into()),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("realm config read result", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::ValidateMutation => match event {
                Event::Metadata(MetadataEvent::ValidationResult { .. }) => {
                    self.begin_transaction_effect()
                }
                Event::Metadata(MetadataEvent::Error { error, .. }) => self.fail(error.into()),
                other => self.unexpected_event("metadata validation result", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::StartTransaction => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.txn_id = Some(txn_id);
                    self.state = UpdateMetadataDocumentState::ReadFence;
                    smallvec![read_registry_effect(
                        self.config.group_id,
                        self.config.document_id,
                        Some(txn_id),
                    )]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::ReadFence => match parse_registry_read(event) {
                Ok(Some(record)) => {
                    let record = self.updated_record(record);
                    let update_event = self.update_event_record(&record);
                    self.record = Some(update_event.record.clone());
                    self.update_event = Some(update_event);
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = UpdateMetadataDocumentState::ReadRawFence;
                    smallvec![Effect::Storage(StorageEffect::BatchRead {
                        reads: vec![
                            (
                                METADATA_RAW_BUDGET_KEYSPACE.to_string(),
                                raw_budget_key(self.config.document_id, self.config.actor.node_id,),
                            ),
                            (
                                METADATA_CREATE_ACCEPTANCE_KEYSPACE.to_string(),
                                metadata_create_acceptance_key(self.config.document_id),
                            ),
                        ],
                        txn_id: Some(txn_id),
                    })]
                }
                Ok(None) => self.fail(UpdateMetadataDocumentError::DocumentNotFound),
                Err(StorageReadError::Storage(error)) => self.fail(error.into()),
                Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
            },
            UpdateMetadataDocumentState::ReadRawFence => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let [(_, raw_budget), (_, accepted_create)] = values.as_slice() else {
                        return self.unexpected_event(
                            "metadata raw sidecar read",
                            format!("batch read with {} values", values.len()),
                        );
                    };
                    let Some(value) = accepted_create.clone() else {
                        return self.fail(UpdateMetadataDocumentError::RawLimit);
                    };
                    let create: MetadataCreateEventRecord = match postcard::from_bytes(&value) {
                        Ok(create) => create,
                        Err(_) => return self.fail(UpdateMetadataDocumentError::RawLimit),
                    };
                    let quota = match self.origin_quota(&create) {
                        Ok(quota) => quota,
                        Err(error) => return self.fail(error),
                    };
                    self.accepted_create = Some(create);
                    let budget = match raw_budget.clone() {
                        Some(value) => {
                            let budget: MetadataRawOriginBudget = match postcard::from_bytes(&value)
                            {
                                Ok(budget) => budget,
                                Err(_) => {
                                    return self.fail(UpdateMetadataDocumentError::RawLimit);
                                }
                            };
                            if !self.valid_budget(&budget, &quota) {
                                return self.fail(UpdateMetadataDocumentError::RawLimit);
                            }
                            Some(budget)
                        }
                        None => None,
                    };
                    self.raw_budget = budget;
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = UpdateMetadataDocumentState::ReadRawEvents;
                    smallvec![Effect::Storage(StorageEffect::Iter {
                        key_space: METADATA_EVENT_LOG_KEYSPACE.to_string(),
                        prefix: Some(metadata_event_log_prefix(self.config.document_id)),
                        start: None,
                        limit: RAW_EVENT_LIMIT,
                        txn_id: Some(txn_id),
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("raw sidecar read result", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::ReadRawEvents => match event {
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
                        return self.fail(UpdateMetadataDocumentError::RawLimit);
                    }
                    self.raw_budget = Some(reconstructed);
                    self.next_raw_budget =
                        match self.check_raw_budget(history_events, history_bytes) {
                            Ok(budget) => Some(budget),
                            Err(error) => return self.fail(error),
                        };
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = UpdateMetadataDocumentState::WriteUpdateBatch;
                    match self.write_update_batch_effect(txn_id) {
                        Ok(effect) => smallvec![effect],
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("raw event iteration result", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::WriteUpdateBatch => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    let Some(txn_id) = self.txn_id else {
                        return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = UpdateMetadataDocumentState::CommitTransaction;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("metadata update batch write", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::CommitTransaction => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                    self.txn_id = None;
                    self.state = UpdateMetadataDocumentState::ScheduleMaterializationDrain;
                    smallvec![schedule_metadata_materialization_drain_effect()]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.txn_id = None;
                    self.fail(error.into())
                }
                other => self.unexpected_event("transaction commit result", format!("{other:?}")),
            },
            UpdateMetadataDocumentState::ScheduleMaterializationDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    self.state = UpdateMetadataDocumentState::ScheduleOutboxDrain;
                    smallvec![schedule_outbox_drain_effect()]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(message = %message, "Failed to schedule metadata materialization drain after committed update");
                    self.state = UpdateMetadataDocumentState::ScheduleOutboxDrain;
                    smallvec![schedule_outbox_drain_effect()]
                }
                other => self.unexpected_event(
                    "metadata materialization drain schedule",
                    format!("{other:?}"),
                ),
            },
            UpdateMetadataDocumentState::ScheduleOutboxDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => {
                    let Some(record) = self.record.clone() else {
                        return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = UpdateMetadataDocumentState::Finish;
                    self.output = Some(Ok(record));
                    smallvec![]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    warn!(message = %message, "Failed to schedule metadata document outbox drain after committed update");
                    let Some(record) = self.record.clone() else {
                        return self.fail(UpdateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = UpdateMetadataDocumentState::Finish;
                    self.output = Some(Ok(record));
                    smallvec![]
                }
                other => self.unexpected_event(
                    "metadata document outbox drain schedule",
                    format!("{other:?}"),
                ),
            },
            UpdateMetadataDocumentState::Finish
            | UpdateMetadataDocumentState::Error
            | UpdateMetadataDocumentState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            UpdateMetadataDocumentState::Finish | UpdateMetadataDocumentState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .expect("metadata update operation must set output")
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::{
        DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent,
        DocumentSyncOutboxRecord,
    };
    use aruna_core::keyspaces::{
        DOCUMENT_SYNC_OUTBOX_KEYSPACE, DOCUMENT_SYNC_REVISION_KEYSPACE, METADATA_AUDIT_KEYSPACE,
        METADATA_DOCUMENT_INDEX_KEYSPACE, METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
        METADATA_EVENT_LOG_KEYSPACE, METADATA_INDEX_KEYSPACE,
        METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE, METADATA_MATERIALIZATION_JOB_KEYSPACE,
        METADATA_MATERIALIZATION_STATUS_KEYSPACE, METADATA_RAW_BUDGET_KEYSPACE,
    };
    use aruna_core::storage_entries::{
        document_sync_revision_key, metadata_create_acceptance_key, metadata_event_log_key,
        metadata_registry_key, raw_budget_key,
    };
    use aruna_core::structs::{Actor, PlacementRef, RealmId};

    fn actor() -> Actor {
        let realm_id = RealmId::from_bytes([9u8; 32]);
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
            realm_id,
        }
    }

    fn record(actor: &Actor) -> MetadataRegistryRecord {
        let group_id = Ulid::generate();
        let document_id = Ulid::generate();
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
        mutation: UpdateMetadataDocumentMutation,
    ) -> UpdateMetadataDocumentConfig {
        UpdateMetadataDocumentConfig {
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

    fn create_event(record: &MetadataRegistryRecord) -> MetadataCreateEventRecord {
        let node_id = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        MetadataCreateEventRecord {
            event_id: record.establishing_event_id,
            record: record.clone(),
            user_id: aruna_core::UserId::local(Ulid::from_parts(2, 2), record.realm_id),
            node_id,
            payload: MetadataCreateEventPayload::Scaffold {
                name: "name".to_string(),
                description: "description".to_string(),
                date_published: "date".to_string(),
                license: None,
            },
            occurred_at_ms: record.created_at_ms,
        }
    }

    fn budget(
        record: &MetadataRegistryRecord,
        events: u32,
        encoded_bytes: u64,
    ) -> MetadataRawOriginBudget {
        let create = create_event(record);
        let create_bytes = postcard::serialized_size(&create).unwrap() as u64;
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
        budget: Option<MetadataRawOriginBudget>,
    ) -> Event {
        let create = create_event(record);
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    raw_budget_key(record.document_id, node_id),
                    budget.map(|budget| postcard::to_allocvec(&budget).unwrap().into()),
                ),
                (
                    metadata_create_acceptance_key(record.document_id),
                    Some(postcard::to_allocvec(&create).unwrap().into()),
                ),
            ],
        })
    }

    fn raw_read(record: &MetadataRegistryRecord, budget: Option<MetadataRawOriginBudget>) -> Event {
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
                metadata_event_log_key(record.document_id, create.event_id),
                postcard::to_allocvec(&create).unwrap().into(),
            )],
            next_start_after: None,
        })
    }

    fn assert_no_graph_mutation_or_sync(effects: &[Effect]) {
        for effect in effects {
            match effect {
                Effect::Metadata(MetadataEffect::ApplyRoCrate { .. })
                | Effect::Metadata(MetadataEffect::UpsertDataEntity { .. })
                | Effect::Metadata(MetadataEffect::UpsertContextualEntity { .. })
                | Effect::Metadata(MetadataEffect::SyncGraphBestEffort { .. }) => {
                    panic!("unexpected graph mutation or sync effect: {effect:?}");
                }
                _ => {}
            }
        }
    }

    fn assert_start_transaction(effects: &[Effect]) {
        let [Effect::Storage(StorageEffect::StartTransaction { read: false })] = effects else {
            panic!("expected write transaction start, got {effects:?}");
        };
    }

    fn assert_update_batch(
        effects: &[Effect],
        txn_id: TxnId,
        expected_payload: impl FnOnce(&MetadataCreateEventPayload) -> bool,
    ) -> MetadataCreateEventRecord {
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
            METADATA_EVENT_LOG_KEYSPACE,
            METADATA_INDEX_KEYSPACE,
            METADATA_DOCUMENT_INDEX_KEYSPACE,
            METADATA_AUDIT_KEYSPACE,
            DOCUMENT_SYNC_OUTBOX_KEYSPACE,
            DOCUMENT_SYNC_REVISION_KEYSPACE,
            METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
            METADATA_MATERIALIZATION_STATUS_KEYSPACE,
            METADATA_MATERIALIZATION_JOB_KEYSPACE,
            METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE,
            METADATA_RAW_BUDGET_KEYSPACE,
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
            .find(|(keyspace, _, _)| keyspace == METADATA_EVENT_LOG_KEYSPACE)
            .map(|(_, _, value)| {
                postcard::from_bytes::<MetadataCreateEventRecord>(value)
                    .expect("update event decodes")
            })
            .expect("event log write exists");
        assert!(expected_payload(&event.payload));
        let outbox = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == DOCUMENT_SYNC_OUTBOX_KEYSPACE)
            .map(|(_, _, value)| {
                postcard::from_bytes::<DocumentSyncOutboxRecord>(value)
                    .expect("outbox record decodes")
            })
            .expect("outbox write exists");
        assert_eq!(outbox.outbox_id, event.event_id);
        assert!(matches!(
            outbox.event,
            DocumentSyncOutboxEvent::Upsert { .. }
        ));
        let (revision_key, revision): (_, DocumentSyncChange) = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == DOCUMENT_SYNC_REVISION_KEYSPACE)
            .map(|(_, key, value)| {
                (
                    key,
                    postcard::from_bytes(value).expect("revision sidecar decodes"),
                )
            })
            .expect("revision sidecar write exists");
        assert_eq!(revision_key, &document_sync_revision_key(&outbox.target));
        assert_eq!(revision.current.event_id, event.event_id);
        assert_eq!(revision.current.actor, event.node_id);
        assert_eq!(revision.current.generation, event.record.updated_at_ms);
        assert_eq!(revision.kind, DocumentSyncChangeKind::Upsert);
        let lifecycle = writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == METADATA_DOCUMENT_LIFECYCLE_KEYSPACE)
            .map(|(_, _, value)| {
                postcard::from_bytes::<MetadataDocumentLifecycleRecord>(value)
                    .expect("lifecycle source decodes")
            })
            .expect("lifecycle source write exists");
        assert_eq!(
            lifecycle,
            MetadataDocumentLifecycleRecord::Upsert {
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
            epoch: 0,
            shard: 11,
        };
        let txn_id = Ulid::generate();
        let mut operation = UpdateMetadataDocumentOperation::new(config(
            actor.clone(),
            &record,
            UpdateMetadataDocumentMutation::ReplaceRoCrate {
                jsonld: replace_jsonld(record.document_id, "Placement Preserved"),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        operation.step(realm_config_read(&record));
        operation.step(Event::Metadata(MetadataEvent::ValidationResult {
            graph_iri: record.graph_iri.clone(),
        }));
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
            postcard::serialized_size(&create_event(&record)).unwrap() as u64,
        ));
        let effects = operation.step(raw_events(&record));

        let event = assert_update_batch(effects.as_slice(), txn_id, |payload| {
            matches!(payload, MetadataCreateEventPayload::ReplaceRoCrate { .. })
        });
        assert_eq!(event.record.placement, record.placement);
    }

    #[test]
    fn update_uses_fence() {
        let actor = actor();
        let record = record(&actor);
        let mut fenced = record.clone();
        fenced.placement = PlacementRef {
            strategy_id: Ulid::from_bytes([6u8; 16]),
            epoch: 1,
            shard: 12,
        };
        let txn_id = Ulid::generate();
        let mut operation = UpdateMetadataDocumentOperation::new(config(
            actor,
            &record,
            UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        operation.step(realm_config_read(&record));
        assert_start_transaction(
            operation
                .step(Event::Metadata(MetadataEvent::ValidationResult {
                    graph_iri: record.graph_iri.clone(),
                }))
                .as_slice(),
        );
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
            postcard::serialized_size(&create_event(&record)).unwrap() as u64,
        ));
        let event = assert_update_batch(
            operation.step(raw_events(&record)).as_slice(),
            txn_id,
            |payload| matches!(payload, MetadataCreateEventPayload::UpsertDataEntity { .. }),
        );
        assert_eq!(event.record.placement, fenced.placement);
    }

    #[test]
    fn rejects_raw_limit() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::generate();
        let mut operation = UpdateMetadataDocumentOperation::new(config(
            actor,
            &record,
            UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        operation.step(realm_config_read(&record));
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(registry_read(&record));
        operation.step(raw_budget_read(
            &record,
            1,
            postcard::serialized_size(&create_event(&record)).unwrap() as u64,
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
        assert_eq!(
            operation.finalize(),
            Err(UpdateMetadataDocumentError::RawLimit)
        );
    }

    #[test]
    fn accepts_raw_history() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::generate();
        let mut operation = UpdateMetadataDocumentOperation::new(config(
            actor,
            &record,
            UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        operation.step(realm_config_read(&record));
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(registry_read(&record));
        operation.step(raw_budget_read(
            &record,
            1,
            postcard::serialized_size(&create_event(&record)).unwrap() as u64,
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
        let txn_id = Ulid::generate();
        let mut operation = UpdateMetadataDocumentOperation::new(config(
            actor.clone(),
            &record,
            UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        operation.step(realm_config_read(&record));
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
                metadata_event_log_key(record.document_id, create.event_id),
                create_value.clone().into(),
            )],
            next_start_after: None,
        }));
        let event = assert_update_batch(effects.as_slice(), txn_id, |payload| {
            matches!(payload, MetadataCreateEventPayload::UpsertDataEntity { .. })
        });
        let budget_value = effects
            .iter()
            .find_map(|effect| match effect {
                Effect::Storage(StorageEffect::BatchWrite { writes, .. }) => writes
                    .iter()
                    .find(|(keyspace, _, _)| keyspace == METADATA_RAW_BUDGET_KEYSPACE)
                    .map(|(_, _, value)| value),
                _ => None,
            })
            .expect("rebuilt budget write exists");
        let budget: MetadataRawOriginBudget = postcard::from_bytes(budget_value).unwrap();
        assert_eq!(budget.events, 2);
        assert_eq!(
            budget.encoded_bytes,
            create_len + postcard::serialized_size(&event).unwrap() as u64
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
        let txn_id = Ulid::generate();
        let mut operation = UpdateMetadataDocumentOperation::new(config(
            outsider.clone(),
            &current,
            UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&current));
        operation.step(realm_config_read(&current));
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(registry_read(&current));
        let effects = operation.step(raw_missing_for(&original, outsider.node_id));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));
        assert_eq!(
            operation.finalize(),
            Err(UpdateMetadataDocumentError::RawLimit)
        );
    }

    #[test]
    fn rejects_origin_budget() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::generate();
        let mut operation = UpdateMetadataDocumentOperation::new(config(
            actor,
            &record,
            UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        operation.step(realm_config_read(&record));
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(registry_read(&record));
        operation.step(raw_budget_read(&record, METADATA_RAW_EVENT_LIMIT, 0));
        let effects = operation.step(raw_events(&record));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));
        assert_eq!(
            operation.finalize(),
            Err(UpdateMetadataDocumentError::RawLimit)
        );
    }

    #[test]
    fn update_fence_missing() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::generate();
        let mut operation = UpdateMetadataDocumentOperation::new(config(
            actor,
            &record,
            UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        operation.step(realm_config_read(&record));
        assert_start_transaction(
            operation
                .step(Event::Storage(StorageEvent::TransactionStarted { txn_id }))
                .as_slice(),
        );
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
            Err(UpdateMetadataDocumentError::DocumentNotFound)
        );
    }

    #[test]
    fn replace_rocrate_validates_and_commits_update_intent_before_craqle_mutation() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::generate();
        let mut operation = UpdateMetadataDocumentOperation::new(config(
            actor,
            &record,
            UpdateMetadataDocumentMutation::ReplaceRoCrate {
                jsonld: replace_jsonld(record.document_id, "Atomic Replace"),
            },
        ));

        assert_no_graph_mutation_or_sync(operation.start().as_slice());
        let effects = operation.step(registry_read(&record));
        assert_no_graph_mutation_or_sync(effects.as_slice());
        let effects = operation.step(realm_config_read(&record));
        let [Effect::Metadata(MetadataEffect::ValidateRoCrate { request })] = effects.as_slice()
        else {
            panic!("expected RO-Crate validation before transaction, got {effects:?}");
        };
        assert_eq!(request.graph_iri, record.graph_iri);

        let effects = operation.step(Event::Metadata(MetadataEvent::ValidationResult {
            graph_iri: record.graph_iri.clone(),
        }));
        assert_start_transaction(effects.as_slice());

        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_no_graph_mutation_or_sync(effects.as_slice());
        let effects = operation.step(registry_read(&record));
        assert_no_graph_mutation_or_sync(effects.as_slice());
        let effects = operation.step(raw_budget_read(
            &record,
            1,
            postcard::serialized_size(&create_event(&record)).unwrap() as u64,
        ));
        assert_no_graph_mutation_or_sync(effects.as_slice());
        let effects = operation.step(raw_events(&record));
        assert_no_graph_mutation_or_sync(effects.as_slice());
        assert_update_batch(effects.as_slice(), txn_id, |payload| {
            matches!(payload, MetadataCreateEventPayload::ReplaceRoCrate { .. })
        });
    }

    #[test]
    fn entity_upsert_appends_durable_update_event_before_materialization() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::generate();
        let mut operation = UpdateMetadataDocumentOperation::new(config(
            actor,
            &record,
            UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        let effects = operation.step(registry_read(&record));
        assert_no_graph_mutation_or_sync(effects.as_slice());
        let effects = operation.step(realm_config_read(&record));
        assert_no_graph_mutation_or_sync(effects.as_slice());
        assert_start_transaction(effects.as_slice());

        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(registry_read(&record));
        operation.step(raw_budget_read(
            &record,
            1,
            postcard::serialized_size(&create_event(&record)).unwrap() as u64,
        ));
        let effects = operation.step(raw_events(&record));
        let event = assert_update_batch(effects.as_slice(), txn_id, |payload| {
            matches!(payload, MetadataCreateEventPayload::UpsertDataEntity { .. })
        });
        assert_eq!(event.record.last_event_id, event.event_id);
    }

    #[test]
    fn rejects_no_holders() {
        let actor = actor();
        let mut record = record(&actor);
        record.holder_node_ids.clear();
        let txn_id = Ulid::generate();
        let mut operation = UpdateMetadataDocumentOperation::new(config(
            actor,
            &record,
            UpdateMetadataDocumentMutation::UpsertDataEntity {
                jsonld: r#"{"@id":"./data/file.txt","@type":"File","name":"file.txt"}"#.to_string(),
            },
        ));

        operation.start();
        operation.step(registry_read(&record));
        let effects = operation.step(realm_config_read(&record));
        assert_start_transaction(effects.as_slice());

        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(registry_read(&record));
        let effects = operation.step(raw_missing_budget(&record));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));
        assert_eq!(
            operation.finalize(),
            Err(UpdateMetadataDocumentError::RawLimit)
        );
    }

    #[test]
    fn commit_failure_does_not_mutate_or_sync_graph() {
        let actor = actor();
        let record = record(&actor);
        let txn_id = Ulid::generate();
        let mut operation = UpdateMetadataDocumentOperation::new(config(
            actor,
            &record,
            UpdateMetadataDocumentMutation::ReplaceRoCrate {
                jsonld: replace_jsonld(record.document_id, "Commit Failure"),
            },
        ));

        assert_no_graph_mutation_or_sync(operation.start().as_slice());
        let effects = operation.step(registry_read(&record));
        assert_no_graph_mutation_or_sync(effects.as_slice());
        let effects = operation.step(realm_config_read(&record));
        assert_no_graph_mutation_or_sync(effects.as_slice());
        let effects = operation.step(Event::Metadata(MetadataEvent::ValidationResult {
            graph_iri: record.graph_iri.clone(),
        }));
        assert_no_graph_mutation_or_sync(effects.as_slice());
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_no_graph_mutation_or_sync(effects.as_slice());
        let effects = operation.step(registry_read(&record));
        assert_no_graph_mutation_or_sync(effects.as_slice());
        let effects = operation.step(raw_budget_read(
            &record,
            1,
            postcard::serialized_size(&create_event(&record)).unwrap() as u64,
        ));
        assert_no_graph_mutation_or_sync(effects.as_slice());
        let effects = operation.step(raw_events(&record));
        assert_no_graph_mutation_or_sync(effects.as_slice());
        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert_no_graph_mutation_or_sync(effects.as_slice());

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: aruna_core::errors::StorageError::WriteError,
        }));

        assert_no_graph_mutation_or_sync(effects.as_slice());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(UpdateMetadataDocumentError::StorageError(
                aruna_core::errors::StorageError::WriteError
            ))
        );
    }
}
