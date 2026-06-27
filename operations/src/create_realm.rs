use aruna_core::admin_document_reducer::{AdminDocumentReducerError, AdminDocumentReducerState};
use aruna_core::admin_documents::{
    AdminDocumentOperation, AdminDocumentRoleDefinition, AdminDocumentTarget,
};
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE, REALM_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::admin_document_reducer_state_write_entry;
use aruna_core::structs::{
    Actor, OidcProviderConfig, Realm, RealmAuthorizationDocument, RealmConfigDocument,
    RealmNodeKind,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::document_sync_outbox::{
    new_outbox_record_with_id, outbox_write_entry, schedule_outbox_drain_effect,
};

#[derive(Clone, Debug, PartialEq)]
pub struct CreateRealmConfig {
    pub actor: Actor,
    pub realm_description: String,
    pub oidc_providers: Vec<OidcProviderConfig>,
}

#[derive(PartialEq)]
pub struct CreateRealmOperation {
    config: CreateRealmConfig,
    txn_id: Option<Ulid>,
    auth_doc: Option<RealmAuthorizationDocument>,
    config_doc: Option<RealmConfigDocument>,
    realm: Option<Realm>,
    state: CreateRealmState,
    output: Option<Result<(Realm, RealmAuthorizationDocument), CreateRealmError>>,
}

impl std::fmt::Debug for CreateRealmOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateRealmOperation")
            .field("config", &self.config)
            .field("auth_doc", &self.auth_doc)
            .field("config_doc", &self.config_doc)
            .field("state", &self.state)
            .field("txn_id", &self.txn_id)
            .field("output", &self.output)
            .finish()
    }
}

impl CreateRealmOperation {
    pub fn new(config: CreateRealmConfig) -> Self {
        CreateRealmOperation {
            config,
            auth_doc: None,
            config_doc: None,
            realm: None,
            state: CreateRealmState::Init,
            txn_id: None,
            output: None,
        }
    }
    fn emit_create_realm(&mut self) -> Result<Effects, CreateRealmError> {
        let realm = Realm {
            realm_id: self.config.actor.realm_id,
            description: self.config.realm_description.clone(),
        };

        self.realm = Some(realm.clone());

        let key = (*self.config.actor.realm_id.as_bytes()).into();

        let value = realm.to_bytes(&self.config.actor)?.into();

        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: REALM_KEYSPACE.to_string(),
            key,
            value,
            txn_id: self.txn_id,
        })])
    }

    fn emit_create_auth_doc(&mut self) -> Result<Effects, CreateRealmError> {
        self.txn_id
            .ok_or_else(|| CreateRealmError::NoTransactionFound)?;

        let realm_id = self.config.actor.realm_id;

        let auth_doc =
            RealmAuthorizationDocument::new_default_realm_doc(self.config.actor.realm_id);

        self.auth_doc = Some(auth_doc.clone());

        let key = (*realm_id.as_bytes()).into();
        let value = auth_doc.to_bytes(&self.config.actor)?.into();
        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: AUTH_KEYSPACE.to_string(),
            key,
            value,
            txn_id: self.txn_id,
        })])
    }

    fn emit_create_config_doc(&mut self) -> Result<Effects, CreateRealmError> {
        self.txn_id
            .ok_or_else(|| CreateRealmError::NoTransactionFound)?;

        let realm_id = self.config.actor.realm_id;
        let mut config_doc =
            RealmConfigDocument::default_for_realm(realm_id, self.config.oidc_providers.clone());
        config_doc.ensure_node(self.config.actor.node_id, RealmNodeKind::Management);
        self.config_doc = Some(config_doc.clone());

        let key = (*realm_id.as_bytes()).into();
        let value = config_doc.to_bytes(&self.config.actor)?.into();
        let mut writes = vec![(REALM_CONFIG_KEYSPACE.to_string(), key, value)];
        writes.extend(self.admin_reducer_seed_writes()?);

        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })])
    }

    fn admin_reducer_seed_writes(&self) -> Result<Vec<(String, Key, Value)>, CreateRealmError> {
        let realm_id = self.config.actor.realm_id;
        let config_doc = self
            .config_doc
            .as_ref()
            .ok_or(CreateRealmError::RealmConfigDocNotFound)?;
        let auth_doc = self
            .auth_doc
            .as_ref()
            .ok_or(CreateRealmError::AuthDocNotFound)?;
        let realm_admin_role = auth_doc
            .roles
            .values()
            .find(|role| role.name == "realm_admin")
            .ok_or(CreateRealmError::RealmAdminRoleNotFound)?;

        let realm_target = AdminDocumentTarget::Realm { realm_id };
        let mut realm_state = AdminDocumentReducerState::new(realm_target);
        let realm_role_event = realm_state.apply_operation(
            &self.config.actor,
            AdminDocumentOperation::RealmRoleCreated {
                role: AdminDocumentRoleDefinition::from(realm_admin_role),
            },
        )?;

        let config_target = AdminDocumentTarget::RealmConfig { realm_id };
        let mut config_state = AdminDocumentReducerState::new(config_target);
        let config_node_event = config_state.apply_operation(
            &self.config.actor,
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: self.config.actor.node_id,
                kind: RealmNodeKind::Management,
            },
        )?;
        let mut config_events = vec![config_node_event];
        let mut oidc_providers = self.config.oidc_providers.clone();
        oidc_providers.sort_by(|left, right| left.id.cmp(&right.id));
        for provider in oidc_providers {
            config_events.push(config_state.apply_operation(
                &self.config.actor,
                AdminDocumentOperation::RealmConfigOidcProviderUpserted { provider },
            )?);
        }
        config_events.push(config_state.apply_operation(
            &self.config.actor,
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: config_doc.metadata_replication.clone(),
                discovery: config_doc.discovery.clone(),
            },
        )?);

        let realm_auth_target = DocumentSyncTarget::RealmAuthorization { realm_id };
        let realm_config_target = DocumentSyncTarget::RealmConfig { realm_id };
        let realm_auth_record = new_outbox_record_with_id(
            realm_role_event.event_id,
            self.config.actor.node_id,
            realm_auth_target,
            Vec::new(),
            DocumentSyncOutboxEvent::AdminOperation {
                event: Box::new(realm_role_event),
            },
        );
        let mut writes = vec![
            admin_document_reducer_state_write_entry(&realm_state)?,
            admin_document_reducer_state_write_entry(&config_state)?,
            outbox_write_entry(&realm_auth_record).map_err(ConversionError::from)?,
        ];
        for event in config_events {
            let record = new_outbox_record_with_id(
                event.event_id,
                self.config.actor.node_id,
                realm_config_target.clone(),
                Vec::new(),
                DocumentSyncOutboxEvent::AdminOperation {
                    event: Box::new(event),
                },
            );
            writes.push(outbox_write_entry(&record).map_err(ConversionError::from)?);
        }

        Ok(writes)
    }

    fn finish_after_outbox_schedule(&mut self) -> Effects {
        if let Some(realm) = &self.realm
            && let Some(auth) = &self.auth_doc
            && self.config_doc.is_some()
        {
            self.state = CreateRealmState::Finish;
            self.output = Some(Ok((realm.clone(), auth.clone())));
            smallvec![]
        } else {
            self.fail(CreateRealmError::RealmNotFound)
        }
    }

    fn fail(&mut self, err: CreateRealmError) -> Effects {
        self.state = CreateRealmState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn fail_with_cleanup(&mut self, err: CreateRealmError, cleanup_effects: Effects) -> Effects {
        self.state = CreateRealmState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: CreateRealmState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            CreateRealmError::UnexpectedEvent {
                state,
                expected,
                got,
            },
            cleanup_effects,
        )
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                CreateRealmState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };

        self.state = CreateRealmState::CreateRealm;
        self.txn_id = Some(txn_id);
        match self.emit_create_realm() {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn handle_create_realm(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                CreateRealmState::CreateRealm,
                "Event::Storage(StorageEvent::WriteResult)",
                got,
            );
        };

        self.state = CreateRealmState::CreateAuthDoc;
        match self.emit_create_auth_doc() {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn handle_create_auth_doc(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                CreateRealmState::CreateAuthDoc,
                "Event::Storage(StorageEvent::WriteResult)",
                got,
            );
        };

        self.state = CreateRealmState::CreateConfigDoc;
        match self.emit_create_config_doc() {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn handle_create_config_doc(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected_event(
                CreateRealmState::CreateConfigDoc,
                "Event::Storage(StorageEvent::BatchWriteResult)",
                got,
            );
        };

        self.state = CreateRealmState::CommitTransaction;
        if let Some(txn_id) = self.txn_id {
            smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
        } else {
            self.fail(CreateRealmError::NoTransactionFound)
        }
    }

    fn handle_commit_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                CreateRealmState::CommitTransaction,
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        };

        if self.realm.is_some() && self.auth_doc.is_some() && self.config_doc.is_some() {
            self.state = CreateRealmState::ScheduleDocumentSyncOutboxDrain;
            smallvec![schedule_outbox_drain_effect()]
        } else {
            self.fail(CreateRealmError::RealmNotFound)
        }
    }

    fn handle_schedule_document_sync_outbox_drain(&mut self, event: Event) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. }) => self.finish_after_outbox_schedule(),
            Event::Task(TaskEvent::Error { .. }) => self.finish_after_outbox_schedule(),
            other => self.unexpected_event(
                CreateRealmState::ScheduleDocumentSyncOutboxDrain,
                "Event::Task(TaskEvent::TimerScheduled)",
                format!("{other:?}"),
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CreateRealmState {
    Init,
    StartTransaction,
    CreateRealm,
    CreateAuthDoc,
    CreateConfigDoc,
    CommitTransaction,
    ScheduleDocumentSyncOutboxDrain,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateRealmError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AdminDocumentReducerError(#[from] AdminDocumentReducerError),
    #[error("authorization document not found")]
    AuthDocNotFound,
    #[error("realm config document not found")]
    RealmConfigDocNotFound,
    #[error("realm_admin role not found")]
    RealmAdminRoleNotFound,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("No group found")]
    RealmNotFound,
    #[error("Creating Group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: CreateRealmState,
        expected: &'static str,
        got: String,
    },
}

impl Operation for CreateRealmOperation {
    type Output = (Realm, RealmAuthorizationDocument);

    type Error = CreateRealmError;

    fn start(&mut self) -> Effects {
        self.state = CreateRealmState::StartTransaction;

        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            CreateRealmState::StartTransaction => self.handle_start_transaction(event),
            CreateRealmState::CreateRealm => self.handle_create_realm(event),
            CreateRealmState::CreateAuthDoc => self.handle_create_auth_doc(event),
            CreateRealmState::CreateConfigDoc => self.handle_create_config_doc(event),
            CreateRealmState::CommitTransaction => self.handle_commit_transaction(event),
            CreateRealmState::ScheduleDocumentSyncOutboxDrain => {
                self.handle_schedule_document_sync_outbox_drain(event)
            }
            CreateRealmState::Init | CreateRealmState::Finish | CreateRealmState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateRealmState::Finish | CreateRealmState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or_else(|| CreateRealmError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use aruna_core::UserId;
    use aruna_core::admin_document_reducer::AdminDocumentReducerState;
    use aruna_core::admin_documents::{
        AdminDocumentOperation, AdminDocumentRoleDefinition, AdminDocumentTarget,
    };
    use aruna_core::document::{
        DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        ADMIN_DOCUMENT_STATE_KEYSPACE, DOCUMENT_SYNC_OUTBOX_KEYSPACE, REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        Actor, OidcProviderConfig, Realm, RealmAuthorizationDocument, RealmConfigDocument, RealmId,
        RealmNodeKind,
    };
    use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
    use aruna_core::types::{Key, KeySpace, TxnId, Value};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use ed25519_dalek::SigningKey;
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};

    fn actor(realm_id: RealmId, node_seed: u8, user_seed: u8) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[node_seed; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([user_seed; 16]), realm_id),
            realm_id,
        }
    }

    fn config(actor: Actor) -> CreateRealmConfig {
        CreateRealmConfig {
            actor,
            realm_description: "A realm description".to_string(),
            oidc_providers: Vec::new(),
        }
    }

    fn oidc_provider(id: &str) -> OidcProviderConfig {
        OidcProviderConfig {
            id: id.to_string(),
            issuer: format!("https://issuer.example/{id}"),
            audience: format!("audience-{id}"),
            discovery_url: format!("https://issuer.example/{id}/.well-known/openid-configuration"),
        }
    }

    fn batch_writes(effects: &[Effect], txn_id: TxnId) -> &Vec<(KeySpace, Key, Value)> {
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: effect_txn_id,
            }) => {
                assert_eq!(*effect_txn_id, Some(txn_id));
                writes
            }
            other => panic!("unexpected effect: {other:?}"),
        }
    }

    fn write_values<'a>(writes: &'a [(KeySpace, Key, Value)], keyspace: &str) -> Vec<&'a Value> {
        writes
            .iter()
            .filter(|(candidate, _, _)| candidate == keyspace)
            .map(|(_, _, value)| value)
            .collect()
    }

    fn operation_ready_to_schedule(actor: Actor, txn_id: TxnId) -> CreateRealmOperation {
        let mut config_doc = RealmConfigDocument::default_for_realm(actor.realm_id, Vec::new());
        config_doc.ensure_node(actor.node_id, RealmNodeKind::Management);
        let mut operation = CreateRealmOperation::new(config(actor.clone()));
        operation.txn_id = Some(txn_id);
        operation.realm = Some(Realm {
            realm_id: actor.realm_id,
            description: "A realm description".to_string(),
        });
        operation.auth_doc = Some(RealmAuthorizationDocument::new_default_realm_doc(
            actor.realm_id,
        ));
        operation.config_doc = Some(config_doc);
        operation.state = super::CreateRealmState::CommitTransaction;
        operation
    }

    #[test]
    fn seeds_reducer_state_and_admin_outbox() {
        let realm_id = RealmId::from_bytes([2; 32]);
        let actor = actor(realm_id, 3, 4);
        let txn_id = TxnId::new();
        let auth_doc = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let realm_admin_role = auth_doc
            .roles
            .values()
            .find(|role| role.name == "realm_admin")
            .unwrap()
            .clone();
        let alpha_provider = oidc_provider("alpha");
        let beta_provider = oidc_provider("beta");
        let mut realm_config = config(actor.clone());
        realm_config.oidc_providers = vec![beta_provider.clone(), alpha_provider.clone()];
        let mut operation = CreateRealmOperation::new(realm_config);
        operation.txn_id = Some(txn_id);
        operation.auth_doc = Some(auth_doc);

        let effects = operation.emit_create_config_doc().unwrap();
        let writes = batch_writes(&effects, txn_id);

        let config_doc = RealmConfigDocument::from_bytes(
            write_values(writes, REALM_CONFIG_KEYSPACE)
                .first()
                .unwrap()
                .as_ref(),
        )
        .unwrap();
        assert!(config_doc.has_node(actor.node_id));

        let states = write_values(writes, ADMIN_DOCUMENT_STATE_KEYSPACE)
            .into_iter()
            .map(|value| postcard::from_bytes::<AdminDocumentReducerState>(value.as_ref()).unwrap())
            .collect::<Vec<_>>();
        let realm_target = AdminDocumentTarget::Realm { realm_id };
        let config_target = AdminDocumentTarget::RealmConfig { realm_id };
        let realm_state = states
            .iter()
            .find(|state| state.target == realm_target)
            .unwrap();
        let config_state = states
            .iter()
            .find(|state| state.target == config_target)
            .unwrap();
        assert!(
            realm_state
                .materialized_realm_roles()
                .contains(&realm_admin_role.role_id)
        );
        assert!(
            realm_state
                .materialized_realm_role_user_assignments()
                .is_empty()
        );
        assert_eq!(
            config_state.materialized_realm_config_nodes()[&actor.node_id],
            RealmNodeKind::Management
        );
        let materialized_providers = config_state.materialized_realm_config_oidc_providers();
        assert_eq!(materialized_providers.len(), 2);
        assert_eq!(materialized_providers.get("alpha"), Some(&alpha_provider));
        assert_eq!(materialized_providers.get("beta"), Some(&beta_provider));
        assert_eq!(
            config_state.materialized_realm_config_metadata_replication(),
            Some(config_doc.metadata_replication.clone())
        );
        assert_eq!(
            config_state.materialized_realm_config_discovery(),
            Some(config_doc.discovery.clone())
        );

        let outbox_records = write_values(writes, DOCUMENT_SYNC_OUTBOX_KEYSPACE)
            .into_iter()
            .map(|value| postcard::from_bytes::<DocumentSyncOutboxRecord>(value.as_ref()).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(outbox_records.len(), 5);
        assert!(outbox_records.iter().any(|record| {
            record.target == DocumentSyncTarget::RealmAuthorization { realm_id }
                && matches!(
                    &record.event,
                    DocumentSyncOutboxEvent::AdminOperation { event }
                        if event.target == realm_target
                            && matches!(
                                &event.op,
                                AdminDocumentOperation::RealmRoleCreated { role }
                                    if role == &AdminDocumentRoleDefinition::from(&realm_admin_role)
                            )
                )
        }));
        let config_events = outbox_records
            .iter()
            .filter_map(|record| {
                if record.target != (DocumentSyncTarget::RealmConfig { realm_id }) {
                    return None;
                }

                let DocumentSyncOutboxEvent::AdminOperation { event } = &record.event else {
                    return None;
                };

                if event.target != config_target {
                    return None;
                }

                Some((event.origin_seq, event.op.clone()))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            config_events,
            vec![
                (
                    1,
                    AdminDocumentOperation::RealmConfigNodeEnsured {
                        node_id: actor.node_id,
                        kind: RealmNodeKind::Management,
                    },
                ),
                (
                    2,
                    AdminDocumentOperation::RealmConfigOidcProviderUpserted {
                        provider: alpha_provider,
                    },
                ),
                (
                    3,
                    AdminDocumentOperation::RealmConfigOidcProviderUpserted {
                        provider: beta_provider,
                    },
                ),
                (
                    4,
                    AdminDocumentOperation::RealmConfigSettingsSet {
                        metadata_replication: config_doc.metadata_replication,
                        discovery: config_doc.discovery,
                    },
                ),
            ]
        );
    }

    #[test]
    fn schedules_outbox_drain_and_finishes_without_direct_replication() {
        let realm_id = RealmId::from_bytes([5; 32]);
        let actor = actor(realm_id, 6, 7);
        let txn_id = TxnId::new();
        let mut operation = operation_ready_to_schedule(actor, txn_id);

        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert_eq!(
            operation.state,
            super::CreateRealmState::ScheduleDocumentSyncOutboxDrain
        );
        assert_eq!(
            effects.first(),
            Some(&Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::DrainDocumentSyncOutbox,
                after: Duration::ZERO,
            }))
        );

        let effects = operation.step(Event::Task(TaskEvent::Error {
            key: Some(TaskKey::DrainDocumentSyncOutbox),
            message: "schedule failed".to_string(),
        }));
        assert_eq!(operation.state, super::CreateRealmState::Finish);
        assert!(effects.is_empty());
        assert!(operation.output.as_ref().is_some_and(Result::is_ok));
    }

    #[tokio::test]
    pub async fn test_realm_creation() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();

        let context = DriverContext {
            storage_handle,
            blob_handle: None,
            net_handle: Some(net_handle.clone()),
            metadata_handle: None,
            task_handle: Some(task_handle),
        };

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key: SigningKey = SigningKey::generate(&mut csprng);
        let pubkey = realm_signing_key.verifying_key().to_bytes();
        let realm_id = RealmId::from_bytes(pubkey);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let realm_admin = UserId::local(Ulid::new(), realm_id);

        let realm_config = CreateRealmConfig {
            actor: Actor {
                node_id,
                user_id: realm_admin,
                realm_id,
            },
            realm_description: "A realm description".to_string(),
            oidc_providers: Vec::new(),
        };
        let realm_operation = CreateRealmOperation::new(realm_config.clone());
        let result = drive(realm_operation, &context).await.unwrap();
        assert_eq!(result.0.description, realm_config.realm_description);
        assert_eq!(result.0.realm_id, realm_config.actor.realm_id);
        assert!(
            result.1.roles.iter().any(|(_id, role)| {
                role.name == "realm_admin" && role.assigned_users.is_empty()
            })
        );
        assert!(result.1.operation_restrictions.is_empty());

        net_handle.shutdown().await;
    }
}
