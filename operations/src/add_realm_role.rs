use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    Actor, AuthContext, Permission, RealmAuthorizationDocument, RealmId, Role,
};
use aruna_core::types::TxnId;
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use crate::automerge_announce::AnnounceAutomergeDocumentOperation;
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_core::types::Effects;

#[derive(Clone, Debug, PartialEq)]
pub struct AddRealmRoleConfig {
    pub actor: Actor,
    pub realm_id: RealmId,
    pub role: Role,
}

#[derive(PartialEq)]
pub struct AddRealmRoleOperation {
    input: AddRealmRoleConfig,
    state: AddRealmRoleState,
    output: Option<Result<RealmAuthorizationDocument, AddRealmRoleError>>,
}

impl std::fmt::Debug for AddRealmRoleOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddRealmRoleOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AddRealmRoleState {
    Init,
    Auth,
    StartTransaction,
    GetAuthDoc {
        txn_id: TxnId,
    },
    CreateRole {
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
    },
    CommitTransaction {
        auth_doc: RealmAuthorizationDocument,
    },
    AnnounceAuthDoc {
        auth_doc: RealmAuthorizationDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum AddRealmRoleError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("automerge announcement failed: {0}")]
    AutomergeState(String),
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("No realm authorization document found")]
    RealmAuthDocNotFound,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Adding role to realm did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: AddRealmRoleState,
        expected: &'static str,
        got: String,
    },
}

impl AddRealmRoleOperation {
    pub fn new(input: AddRealmRoleConfig) -> Self {
        AddRealmRoleOperation {
            input,
            state: AddRealmRoleState::Init,
            output: None,
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                AddRealmRoleState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };
        match self.emit_get_auth_doc(txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn auth_context(&self) -> AuthContext {
        AuthContext {
            user_id: self.input.actor.user_id,
            realm_id: self.input.actor.realm_id.clone(),
            path_restrictions: None,
        }
    }

    fn handle_authorization(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected_event(
                AddRealmRoleState::Auth,
                "Event::SubOperation(SubOperationEvent::AuthorizationResult)",
                got,
            );
        };

        match self.emit_start_transaction(allowed) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_start_transaction(
        &mut self,
        auth_result: Result<bool, AuthorizationError>,
    ) -> Result<Effects, AddRealmRoleError> {
        if auth_result? {
            self.state = AddRealmRoleState::StartTransaction;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(AddRealmRoleError::Unauthorized)
        }
    }

    fn emit_get_auth_doc(&mut self, txn_id: TxnId) -> Result<Effects, AddRealmRoleError> {
        self.state = AddRealmRoleState::GetAuthDoc { txn_id };
        let key = (*self.input.realm_id.as_bytes()).into();
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: AUTH_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_get_auth_doc(&mut self, event: Event, txn_id: TxnId) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::ReadResult)",
                got,
            );
        };

        match self.emit_create_role(txn_id, value) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_create_role(
        &mut self,
        txn_id: TxnId,
        auth_doc: Option<ByteView>,
    ) -> Result<Effects, AddRealmRoleError> {
        let mut auth_doc = RealmAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| AddRealmRoleError::RealmAuthDocNotFound)?,
        )?;
        auth_doc
            .roles
            .insert(self.input.role.role_id, self.input.role.clone());

        let key = (*auth_doc.realm_id.as_bytes()).into();
        let value = auth_doc.to_bytes(&self.input.actor)?.into();

        self.state = AddRealmRoleState::CreateRole { txn_id, auth_doc };

        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: AUTH_KEYSPACE.to_string(),
            key,
            value,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_create_role(
        &mut self,
        event: Event,
        txn_id: TxnId,
        auth_doc: RealmAuthorizationDocument,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::WriteResult)",
                got,
            );
        };

        self.state = AddRealmRoleState::CommitTransaction { auth_doc };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_transaction(
        &mut self,
        event: Event,
        auth_doc: RealmAuthorizationDocument,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        };
        self.state = AddRealmRoleState::AnnounceAuthDoc {
            auth_doc: auth_doc.clone(),
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            AnnounceAutomergeDocumentOperation::new(
                AutomergeDocumentVariant::RealmAuthorization {
                    realm_id: auth_doc.realm_id.clone(),
                },
                self.input.actor.node_id,
            ),
            |result| Event::SubOperation(SubOperationEvent::AutomergeStateResult {
                result: result.map_err(|error| error.to_string()),
            }),
        ))]
    }

    fn handle_announce_auth_doc(
        &mut self,
        event: Event,
        auth_doc: RealmAuthorizationDocument,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AutomergeStateResult { result }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::SubOperation(SubOperationEvent::AutomergeStateResult)",
                got,
            );
        };
        if let Err(error) = result {
            return self.fail(AddRealmRoleError::AutomergeState(error));
        }
        self.state = AddRealmRoleState::Finish;
        self.output = Some(Ok(auth_doc));
        smallvec![]
    }

    fn fail(&mut self, err: AddRealmRoleError) -> Effects {
        self.state = AddRealmRoleState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn fail_with_cleanup(&mut self, err: AddRealmRoleError, cleanup_effects: Effects) -> Effects {
        self.state = AddRealmRoleState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: AddRealmRoleState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            AddRealmRoleError::UnexpectedEvent {
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
}

impl Operation for AddRealmRoleOperation {
    type Output = RealmAuthorizationDocument;

    type Error = AddRealmRoleError;

    fn start(&mut self) -> Effects {
        self.state = AddRealmRoleState::Auth;

        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.auth_context(),
                path: format!(
                    "/{}/admin/roles/{}",
                    self.input.realm_id, self.input.role.role_id
                ),
                required_permission: Permission::WRITE,
            }),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: result
            }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state.clone() {
            AddRealmRoleState::Auth => self.handle_authorization(event),
            AddRealmRoleState::StartTransaction => self.handle_start_transaction(event),
            AddRealmRoleState::GetAuthDoc { txn_id } => self.handle_get_auth_doc(event, txn_id),
            AddRealmRoleState::CreateRole { txn_id, auth_doc } => {
                self.handle_create_role(event, txn_id, auth_doc)
            }
            AddRealmRoleState::CommitTransaction { auth_doc } => {
                self.handle_commit_transaction(event, auth_doc)
            }
            AddRealmRoleState::AnnounceAuthDoc { auth_doc } => {
                self.handle_announce_auth_doc(event, auth_doc)
            }
            AddRealmRoleState::Init | AddRealmRoleState::Finish | AddRealmRoleState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AddRealmRoleState::Finish | AddRealmRoleState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or_else(|| AddRealmRoleError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            AddRealmRoleState::GetAuthDoc { txn_id, .. }
            | AddRealmRoleState::CreateRole { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::{HashMap, HashSet};

    use crate::add_realm_role::{AddRealmRoleConfig, AddRealmRoleOperation};
    use crate::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};
    use aruna_core::structs::{Actor, Permission, Role};
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_add_role() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
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
            blob_handle: None,
        };

        let user_id = Ulid::new();
        let realm_id = aruna_core::structs::RealmId([0u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let realm_config = CreateRealmConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id: realm_id.clone(),
            },
            realm_description: "A realm description".to_string(),
        };
        let realm_operation = CreateRealmOperation::new(realm_config.clone());
        let (_realm, _realm_auth_doc) = drive(realm_operation, &context).await.unwrap();
        drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: realm_config.actor.clone(),
            }),
            &context,
        )
        .await
        .unwrap();

        let add_role_input = AddRealmRoleConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id: realm_id.clone(),
            },
            realm_id: realm_id.clone(),
            role: Role {
                role_id: Ulid::new(),
                name: "test_role".to_string(),
                permissions: HashMap::from([(
                    format!("{}/admin/create_group/*", realm_id),
                    Permission::WRITE,
                )]),
                assigned_users: HashSet::from([user_id]),
            },
        };

        let add_role_operation = AddRealmRoleOperation::new(add_role_input.clone());
        let auth_doc = drive(add_role_operation, &context).await.unwrap();

        assert_eq!(
            auth_doc.roles.get(&add_role_input.role.role_id).unwrap(),
            &add_role_input.role
        );

        net_handle.shutdown().await;
    }
}
