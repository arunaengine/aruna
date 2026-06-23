use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::metadata::{
    MetadataApplyRoCrateRequest, MetadataCreateEventPayload, MetadataCreateEventRecord,
    MetadataEffect, MetadataError, MetadataEvent, MetadataGraphPolicy, MetadataRequestDurability,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{MetadataAuditRecord, MetadataRegistryRecord};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId, TxnId};
use chrono::Utc;
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::document_sync_outbox::schedule_outbox_drain_effect;
use crate::metadata::materialization_queue::{
    new_materialization_job, new_pending_materialization_status,
    schedule_metadata_materialization_drain_effect,
};
use crate::metadata::projector::create_event_outbox_record;
use crate::metadata::repository::{
    StorageReadError, metadata_event_projection_write_entries, parse_registry_read,
    read_registry_effect,
};

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateMetadataDocumentConfig {
    pub actor: aruna_core::structs::Actor,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub public: bool,
    pub mutation: UpdateMetadataDocumentMutation,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateMetadataDocumentMutation {
    ReplaceRoCrate { jsonld: String },
    UpsertDataEntity { jsonld: String },
    UpsertContextualEntity { jsonld: String },
}

#[derive(Debug, PartialEq)]
pub struct UpdateMetadataDocumentOperation {
    config: UpdateMetadataDocumentConfig,
    txn_id: Option<TxnId>,
    record: Option<MetadataRegistryRecord>,
    update_event: Option<MetadataCreateEventRecord>,
    state: UpdateMetadataDocumentState,
    output: Option<Result<MetadataRegistryRecord, UpdateMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum UpdateMetadataDocumentState {
    Init,
    ReadCurrent,
    ValidateMutation,
    StartTransaction,
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
        let event_id = Ulid::new();
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
        let outbox = create_event_outbox_record(event);
        let status = new_pending_materialization_status(event, now);
        let job = new_materialization_job(event, now);
        let writes =
            metadata_event_projection_write_entries(event, &audit, Some(&outbox), &status, &job)?;
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
                    let record = self.updated_record(record);
                    let update_event = self.update_event_record(&record);
                    self.update_event = Some(update_event);
                    self.record = Some(record.clone());
                    match self.validation_effect(&record) {
                        Ok(Some(effect)) => {
                            self.state = UpdateMetadataDocumentState::ValidateMutation;
                            smallvec![effect]
                        }
                        Ok(None) => self.begin_transaction_effect(),
                        Err(error) => self.fail(error.into()),
                    }
                }
                Ok(None) => self.fail(UpdateMetadataDocumentError::DocumentNotFound),
                Err(StorageReadError::Storage(error)) => self.fail(error.into()),
                Err(StorageReadError::Conversion(error)) => self.fail(error.into()),
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
                    self.state = UpdateMetadataDocumentState::WriteUpdateBatch;
                    match self.write_update_batch_effect(txn_id) {
                        Ok(effect) => smallvec![effect],
                        Err(error) => self.fail(error),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected_event("transaction start result", format!("{other:?}")),
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

