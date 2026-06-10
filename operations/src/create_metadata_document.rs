use aruna_core::NodeId;
use aruna_core::effects::Effect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::metadata::{
    MetadataCreateCrateRequest, MetadataCreateEventPayload, MetadataCreateEventRecord,
    MetadataEffect, MetadataError, MetadataEvent, MetadataGraphPolicy, MetadataRequestDurability,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{Actor, MetadataRegistryRecord};
use aruna_core::types::{Effects, GroupId};
use chrono::Utc;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::repository::{read_registry_by_document_effect, write_create_event_effect};

#[derive(Debug, Clone, PartialEq)]
pub struct CreateMetadataDocumentConfig {
    pub actor: Actor,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub document_path: String,
    pub public: bool,
    pub payload: CreateMetadataDocumentPayload,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateMetadataDocumentPayload {
    Scaffold {
        name: String,
        description: String,
        date_published: String,
        license: String,
    },
    RoCrate {
        jsonld: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateMetadataDocumentResult {
    pub record: MetadataRegistryRecord,
    pub event_id: Ulid,
}

#[derive(Debug, PartialEq)]
pub struct CreateMetadataDocumentOperation {
    config: CreateMetadataDocumentConfig,
    skip_existing_check: bool,
    state: CreateMetadataDocumentState,
    record: Option<MetadataRegistryRecord>,
    create_event: Option<MetadataCreateEventRecord>,
    output: Option<Result<CreateMetadataDocumentResult, CreateMetadataDocumentError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum CreateMetadataDocumentState {
    Init,
    ValidateGraph,
    CheckExisting,
    AppendCreateEvent,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateMetadataDocumentError {
    #[error(transparent)]
    StorageError(#[from] aruna_core::errors::StorageError),
    #[error(transparent)]
    ConversionError(#[from] aruna_core::errors::ConversionError),
    #[error(transparent)]
    MetadataError(#[from] MetadataError),
    #[error("document already exists")]
    DocumentAlreadyExists,
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

impl CreateMetadataDocumentOperation {
    pub fn new(config: CreateMetadataDocumentConfig) -> Self {
        Self {
            config,
            skip_existing_check: false,
            state: CreateMetadataDocumentState::Init,
            record: None,
            create_event: None,
            output: None,
        }
    }

    pub fn new_for_generated_document_id(config: CreateMetadataDocumentConfig) -> Self {
        let mut operation = Self::new(config);
        operation.skip_existing_check = true;
        operation
    }

    fn graph_iri(&self) -> String {
        MetadataRegistryRecord::graph_iri_for(self.config.document_id)
    }

    fn permission_path(&self) -> String {
        MetadataRegistryRecord::permission_path_for(
            &self.config.actor.realm_id,
            self.config.group_id,
            &self.config.document_path,
            self.config.document_id,
        )
    }

    fn current_timestamp_ms() -> u64 {
        u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default()
    }

    fn holder_node_ids(&self) -> Vec<NodeId> {
        vec![self.config.actor.node_id]
    }

    fn build_record(&self, holder_node_ids: Vec<NodeId>) -> MetadataRegistryRecord {
        let now = Self::current_timestamp_ms();
        MetadataRegistryRecord {
            realm_id: self.config.actor.realm_id,
            group_id: self.config.group_id,
            document_id: self.config.document_id,
            document_path: MetadataRegistryRecord::normalize_document_path(
                &self.config.document_path,
            ),
            graph_iri: self.graph_iri(),
            public: self.config.public,
            permission_path: self.permission_path(),
            holder_node_ids,
            created_at_ms: now,
            updated_at_ms: now,
            last_event_id: Ulid::nil(),
        }
    }

    fn create_event_payload(&self) -> MetadataCreateEventPayload {
        match &self.config.payload {
            CreateMetadataDocumentPayload::Scaffold {
                name,
                description,
                date_published,
                license,
            } => MetadataCreateEventPayload::Scaffold {
                name: name.clone(),
                description: description.clone(),
                date_published: date_published.clone(),
                license: license.clone(),
            },
            CreateMetadataDocumentPayload::RoCrate { jsonld } => {
                MetadataCreateEventPayload::RoCrate {
                    jsonld: jsonld.clone(),
                }
            }
        }
    }

    fn create_event_record(&self, record: &MetadataRegistryRecord) -> MetadataCreateEventRecord {
        let event_id = Ulid::new();
        let mut record = record.clone();
        record.last_event_id = event_id;
        let occurred_at_ms = record.created_at_ms;
        MetadataCreateEventRecord {
            event_id,
            record,
            user_id: self.config.actor.user_id,
            node_id: self.config.actor.node_id,
            payload: self.create_event_payload(),
            occurred_at_ms,
        }
    }

    fn graph_policy(&self) -> MetadataGraphPolicy {
        MetadataGraphPolicy {
            public: self.config.public,
            permission_paths: vec![self.permission_path()],
        }
        .normalized()
    }

    fn graph_validation_effect(&self) -> Effect {
        let graph_iri = self.graph_iri();
        let policy = self.graph_policy();
        match &self.config.payload {
            CreateMetadataDocumentPayload::Scaffold {
                name,
                description,
                date_published,
                license,
            } => Effect::Metadata(MetadataEffect::ValidateCreateCrate {
                request: MetadataCreateCrateRequest {
                    graph_iri,
                    name: name.clone(),
                    description: description.clone(),
                    date_published: date_published.clone(),
                    license: license.clone(),
                    policy,
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor: None,
                },
            }),
            CreateMetadataDocumentPayload::RoCrate { jsonld } => {
                Effect::Metadata(MetadataEffect::ValidateRoCrate {
                    request: aruna_core::metadata::MetadataApplyRoCrateRequest {
                        graph_iri,
                        jsonld: jsonld.clone(),
                        policy,
                        durability: MetadataRequestDurability::WalAlreadyDurable,
                        deterministic_actor: None,
                    },
                })
            }
        }
    }

    fn validation_effect(&mut self) -> Effects {
        self.state = CreateMetadataDocumentState::ValidateGraph;
        smallvec![self.graph_validation_effect()]
    }

    fn append_create_event_effect(&mut self) -> Effects {
        let record = self.build_record(self.holder_node_ids());
        let create_event = self.create_event_record(&record);
        self.create_event = Some(create_event.clone());
        self.record = Some(create_event.record.clone());
        self.state = CreateMetadataDocumentState::AppendCreateEvent;
        match write_create_event_effect(&create_event) {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(CreateMetadataDocumentError::ConversionError(error)),
        }
    }

    fn fail(&mut self, error: CreateMetadataDocumentError) -> Effects {
        self.state = CreateMetadataDocumentState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn fail_without_cleanup(&mut self, error: CreateMetadataDocumentError) -> Effects {
        self.state = CreateMetadataDocumentState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(CreateMetadataDocumentError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for CreateMetadataDocumentOperation {
    type Output = CreateMetadataDocumentResult;
    type Error = CreateMetadataDocumentError;

    fn start(&mut self) -> Effects {
        self.validation_effect()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            CreateMetadataDocumentState::ValidateGraph => match event {
                Event::Metadata(MetadataEvent::ValidationResult { .. }) => {
                    if self.skip_existing_check {
                        return self.append_create_event_effect();
                    }
                    self.state = CreateMetadataDocumentState::CheckExisting;
                    smallvec![read_registry_by_document_effect(
                        self.config.document_id,
                        None
                    )]
                }
                Event::Metadata(MetadataEvent::Error { error, .. }) => {
                    self.fail_without_cleanup(error.into())
                }
                other => self.unexpected_event("metadata validation result", format!("{other:?}")),
            },
            CreateMetadataDocumentState::CheckExisting => {
                match crate::metadata::repository::parse_registry_read(event) {
                    Ok(Some(_)) => self
                        .fail_without_cleanup(CreateMetadataDocumentError::DocumentAlreadyExists),
                    Ok(None) => self.append_create_event_effect(),
                    Err(crate::metadata::repository::StorageReadError::Storage(error)) => {
                        self.fail_without_cleanup(error.into())
                    }
                    Err(crate::metadata::repository::StorageReadError::Conversion(error)) => {
                        self.fail_without_cleanup(error.into())
                    }
                }
            }
            CreateMetadataDocumentState::AppendCreateEvent => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let Some(record) = self.record.clone() else {
                        return self
                            .fail_without_cleanup(CreateMetadataDocumentError::MissingTransaction);
                    };
                    let Some(create_event) = self.create_event.as_ref() else {
                        return self
                            .fail_without_cleanup(CreateMetadataDocumentError::MissingTransaction);
                    };
                    self.state = CreateMetadataDocumentState::Finish;
                    self.output = Some(Ok(CreateMetadataDocumentResult {
                        record,
                        event_id: create_event.event_id,
                    }));
                    smallvec![]
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.fail_without_cleanup(error.into())
                }
                other => {
                    self.unexpected_event("metadata create event append", format!("{other:?}"))
                }
            },
            CreateMetadataDocumentState::Finish
            | CreateMetadataDocumentState::Error
            | CreateMetadataDocumentState::Init => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateMetadataDocumentState::Finish | CreateMetadataDocumentState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .expect("metadata create operation must set output")
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
        CreateMetadataDocumentPayload,
    };

    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{METADATA_DOCUMENT_INDEX_KEYSPACE, METADATA_EVENT_LOG_KEYSPACE};
    use aruna_core::metadata::{
        MetadataCreateEventPayload, MetadataCreateEventRecord, MetadataEffect, MetadataError,
        MetadataEvent, MetadataRequestDurability,
    };
    use aruna_core::operation::Operation;
    use aruna_core::storage_entries::metadata_event_log_prefix;
    use aruna_core::structs::{Actor, RealmId};
    use aruna_core::types::{GroupId, Key};
    use ulid::Ulid;

    fn actor(realm_id: RealmId, key_byte: u8) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[key_byte; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
            realm_id,
        }
    }

    fn config(actor: Actor, group_id: GroupId, document_id: Ulid) -> CreateMetadataDocumentConfig {
        CreateMetadataDocumentConfig {
            actor,
            group_id,
            document_id,
            document_path: "datasets/fast-create".to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Fast Create".to_string(),
                description: "Validate then append only".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }
    }

    fn validation_result(document_id: Ulid) -> Event {
        Event::Metadata(MetadataEvent::ValidationResult {
            graph_iri: format!("https://w3id.org/aruna/{document_id}"),
        })
    }

    fn assert_validation_effect(effects: &[Effect], document_id: Ulid) {
        let [Effect::Metadata(MetadataEffect::ValidateCreateCrate { request })] = effects else {
            panic!("expected metadata validation effect");
        };
        assert_eq!(
            request.graph_iri,
            format!("https://w3id.org/aruna/{document_id}")
        );
        assert_eq!(
            request.durability,
            MetadataRequestDurability::WalAlreadyDurable
        );
    }

    fn assert_existing_read(effects: &[Effect]) {
        let [
            Effect::Storage(StorageEffect::Read {
                key_space, txn_id, ..
            }),
        ] = effects
        else {
            panic!("expected metadata document index read");
        };
        assert_eq!(key_space, METADATA_DOCUMENT_INDEX_KEYSPACE);
        assert_eq!(txn_id, &None);
    }

    fn assert_create_event_append(effects: &[Effect], document_id: Ulid, actor: &Actor) -> Key {
        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id,
            }),
        ] = effects
        else {
            panic!("expected metadata create event append");
        };
        assert_eq!(key_space, METADATA_EVENT_LOG_KEYSPACE);
        assert_eq!(txn_id, &None);
        assert!(
            key.as_ref()
                .starts_with(metadata_event_log_prefix(document_id).as_ref())
        );

        let event: MetadataCreateEventRecord =
            postcard::from_bytes(value.as_ref()).expect("create event decodes");
        assert_eq!(event.record.document_id, document_id);
        assert_eq!(event.record.holder_node_ids, vec![actor.node_id]);
        assert_eq!(event.user_id, actor.user_id);
        assert_eq!(event.node_id, actor.node_id);
        assert!(matches!(
            event.payload,
            MetadataCreateEventPayload::Scaffold { .. }
        ));

        key.clone()
    }

    #[test]
    fn generated_document_id_validates_then_appends_without_existing_read() {
        let realm_id = RealmId([11u8; 32]);
        let actor = actor(realm_id, 6);
        let group_id = GroupId::new();
        let document_id = Ulid::new();
        let mut operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor.clone(),
            group_id,
            document_id,
        ));

        let effects = operation.start();
        assert_validation_effect(effects.as_slice(), document_id);
        let effects = operation.step(validation_result(document_id));
        assert_create_event_append(effects.as_slice(), document_id, &actor);
    }

    #[test]
    fn create_checks_existing_after_validation_and_uses_local_holder() {
        let realm_id = RealmId([8u8; 32]);
        let actor = actor(realm_id, 1);
        let group_id = GroupId::new();
        let document_id = Ulid::new();
        let mut operation =
            CreateMetadataDocumentOperation::new(config(actor.clone(), group_id, document_id));

        let effects = operation.start();
        assert_validation_effect(effects.as_slice(), document_id);
        let effects = operation.step(validation_result(document_id));
        assert_existing_read(effects.as_slice());
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: document_id.to_bytes().to_vec().into(),
            value: None,
        }));
        let create_event_key = assert_create_event_append(effects.as_slice(), document_id, &actor);
        assert_eq!(
            operation
                .record
                .as_ref()
                .map(|record| &record.holder_node_ids),
            Some(&vec![actor.node_id])
        );

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: create_event_key,
        }));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(
            operation
                .finalize()
                .expect("operation succeeds")
                .record
                .document_id,
            document_id
        );
    }

    #[test]
    fn create_returns_after_event_append_without_persistent_effects() {
        let realm_id = RealmId([12u8; 32]);
        let actor = actor(realm_id, 7);
        let group_id = GroupId::new();
        let document_id = Ulid::new();
        let mut operation = CreateMetadataDocumentOperation::new_for_generated_document_id(config(
            actor.clone(),
            group_id,
            document_id,
        ));

        assert_validation_effect(operation.start().as_slice(), document_id);
        let effects = operation.step(validation_result(document_id));
        let create_event_key = assert_create_event_append(effects.as_slice(), document_id, &actor);
        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: create_event_key,
        }));

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(
            operation
                .finalize()
                .expect("operation succeeds")
                .record
                .document_id,
            document_id
        );
    }

    #[test]
    fn validation_failure_does_not_append_event() {
        let realm_id = RealmId([13u8; 32]);
        let actor = actor(realm_id, 8);
        let group_id = GroupId::new();
        let document_id = Ulid::new();
        let mut operation =
            CreateMetadataDocumentOperation::new(config(actor, group_id, document_id));

        assert_validation_effect(operation.start().as_slice(), document_id);
        let effects = operation.step(Event::Metadata(MetadataEvent::Error {
            graph_iri: Some(format!("https://w3id.org/aruna/{document_id}")),
            error: MetadataError::InvalidInput("invalid RO-Crate".to_string()),
        }));

        assert!(effects.is_empty());
        assert!(operation.create_event.is_none());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(CreateMetadataDocumentError::MetadataError(
                MetadataError::InvalidInput("invalid RO-Crate".to_string())
            ))
        );
    }

    #[test]
    fn create_event_append_failure_fails_without_projection_cleanup() {
        let realm_id = RealmId([10u8; 32]);
        let actor = actor(realm_id, 5);
        let group_id = GroupId::new();
        let document_id = Ulid::new();
        let mut operation =
            CreateMetadataDocumentOperation::new(config(actor.clone(), group_id, document_id));

        assert_validation_effect(operation.start().as_slice(), document_id);
        assert_existing_read(operation.step(validation_result(document_id)).as_slice());
        let append = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: document_id.to_bytes().to_vec().into(),
            value: None,
        }));
        assert_create_event_append(append.as_slice(), document_id, &actor);

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: aruna_core::errors::StorageError::WriteError,
        }));
        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(CreateMetadataDocumentError::StorageError(
                aruna_core::errors::StorageError::WriteError,
            ))
        );
    }
}
