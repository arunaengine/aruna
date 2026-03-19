use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::consts::{AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE, REALM_KEYSPACE};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, Realm, RealmAuthorizationDocument, RealmConfigDocument};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::automerge_announce::AnnounceAutomergeDocumentOperation;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateRealmConfig {
    pub actor: Actor,
    pub realm_description: String,
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
    fn emit_create_realm(&mut self) -> Result<aruna_core::types::Effects, CreateRealmError> {
        let realm = Realm {
            realm_id: self.config.actor.realm_id.clone(),
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

    fn emit_create_auth_doc(&mut self) -> Result<aruna_core::types::Effects, CreateRealmError> {
        self.txn_id
            .ok_or_else(|| CreateRealmError::NoTransactionFound)?;

        let realm_id = self.config.actor.realm_id.clone();

        let auth_doc = RealmAuthorizationDocument::new_default_realm_doc(
            self.config.actor.user_id,
            self.config.actor.realm_id.clone(),
        );

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

    fn emit_create_config_doc(&mut self) -> Result<aruna_core::types::Effects, CreateRealmError> {
        self.txn_id
            .ok_or_else(|| CreateRealmError::NoTransactionFound)?;

        let realm_id = self.config.actor.realm_id.clone();
        let config_doc = RealmConfigDocument::default_for_realm(realm_id.clone());
        self.config_doc = Some(config_doc.clone());

        let key = (*realm_id.as_bytes()).into();
        let value = config_doc.to_bytes(&self.config.actor)?.into();
        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key,
            value,
            txn_id: self.txn_id,
        })])
    }

    fn fail(&mut self, err: CreateRealmError) -> aruna_core::types::Effects {
        self.state = CreateRealmState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn fail_with_cleanup(
        &mut self,
        err: CreateRealmError,
        cleanup_effects: aruna_core::types::Effects,
    ) -> aruna_core::types::Effects {
        self.state = CreateRealmState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: CreateRealmState,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
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

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, aruna_core::types::Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }

    fn handle_start_transaction(&mut self, event: Event) -> aruna_core::types::Effects {
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

    fn handle_create_realm(&mut self, event: Event) -> aruna_core::types::Effects {
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

    fn handle_create_auth_doc(&mut self, event: Event) -> aruna_core::types::Effects {
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

    fn handle_create_config_doc(&mut self, event: Event) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                CreateRealmState::CreateConfigDoc,
                "Event::Storage(StorageEvent::WriteResult)",
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

    fn handle_commit_transaction(&mut self, event: Event) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                CreateRealmState::CommitTransaction,
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        };

        if let Some(realm) = &self.realm
            && self.auth_doc.is_some()
            && self.config_doc.is_some()
        {
            self.state = CreateRealmState::AnnounceAuthDoc;
            smallvec![Effect::SubOperation(boxed_suboperation(
                AnnounceAutomergeDocumentOperation::new(
                    AutomergeDocumentVariant::RealmAuthorization {
                        realm_id: realm.realm_id.clone(),
                    }
                ),
                |result| Event::SubOperation(SubOperationEvent::AutomergeStateResult {
                    result: result.map_err(|error| error.to_string()),
                }),
            ))]
        } else {
            self.fail(CreateRealmError::RealmNotFound)
        }
    }

    fn handle_announce_auth_doc(&mut self, event: Event) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AutomergeStateResult { result }) = event else {
            return self.unexpected_event(
                CreateRealmState::AnnounceAuthDoc,
                "Event::SubOperation(SubOperationEvent::AutomergeStateResult)",
                got,
            );
        };

        if let Err(error) = result {
            return self.fail(CreateRealmError::AutomergeState(error));
        }

        if let Some(realm) = &self.realm {
            self.state = CreateRealmState::AnnounceConfigDoc;
            smallvec![Effect::SubOperation(boxed_suboperation(
                AnnounceAutomergeDocumentOperation::new(AutomergeDocumentVariant::RealmConfig {
                    realm_id: realm.realm_id.clone(),
                }),
                |result| Event::SubOperation(SubOperationEvent::AutomergeStateResult {
                    result: result.map_err(|error| error.to_string()),
                }),
            ))]
        } else {
            self.fail(CreateRealmError::RealmNotFound)
        }
    }

    fn handle_announce_config_doc(&mut self, event: Event) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AutomergeStateResult { result }) = event else {
            return self.unexpected_event(
                CreateRealmState::AnnounceConfigDoc,
                "Event::SubOperation(SubOperationEvent::AutomergeStateResult)",
                got,
            );
        };

        if let Err(error) = result {
            return self.fail(CreateRealmError::AutomergeState(error));
        }

        if let Some(realm) = &self.realm
            && let Some(auth) = &self.auth_doc
        {
            self.state = CreateRealmState::Finish;
            self.output = Some(Ok((realm.clone(), auth.clone())));
            smallvec![]
        } else {
            self.fail(CreateRealmError::RealmNotFound)
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
    AnnounceAuthDoc,
    AnnounceConfigDoc,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateRealmError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("automerge announcement failed: {0}")]
    AutomergeState(String),
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

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = CreateRealmState::StartTransaction;

        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
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
            CreateRealmState::AnnounceAuthDoc => self.handle_announce_auth_doc(event),
            CreateRealmState::AnnounceConfigDoc => self.handle_announce_config_doc(event),
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

    fn abort(&mut self) -> aruna_core::types::Effects {
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod test {
    use aruna_core::structs::{Actor, RealmId};
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use ed25519_dalek::SigningKey;
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};

    #[tokio::test]
    pub async fn test_realm_creation() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(&random_path.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                use_dns_discovery: false,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();

        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle.clone()),
            automerge_handle: None,
            task_handle: Some(task_handle),
        };

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key: SigningKey = SigningKey::generate(&mut csprng);
        let pubkey = realm_signing_key.verifying_key().to_bytes();
        let realm_id = RealmId::from_bytes(pubkey);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let realm_admin = Ulid::new();

        let realm_config = CreateRealmConfig {
            actor: Actor {
                node_id,
                user_id: realm_admin,
                realm_id,
            },
            realm_description: format!("A realm description"),
        };
        let realm_operation = CreateRealmOperation::new(realm_config.clone());
        let result = drive(realm_operation, &context).await.unwrap();
        assert_eq!(result.0.description, realm_config.realm_description);
        assert_eq!(result.0.realm_id, realm_config.actor.realm_id);
        assert!(result.1.roles.iter().any(|(_id, role)| {
            role.name == "admin"
                && role
                    .assigned_users
                    .iter()
                    .any(|user| user == &realm_config.actor.user_id)
        }));
        assert!(result.1.operation_restrictions.is_empty());

        net_handle.shutdown().await;
    }
}
