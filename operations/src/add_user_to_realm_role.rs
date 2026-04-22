use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, AuthContext, Permission, RealmAuthorizationDocument, RealmId};
use aruna_core::types::{RoleId, TxnId, UserId};
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;

use crate::announce::AnnounceTopicOperation;
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_core::types::Effects;

#[derive(Clone, Debug, PartialEq)]
pub struct AddUserToRealmRolesInput {
    pub actor: Actor,
    pub realm_id: RealmId,
    pub user_id: UserId,
    pub role_ids: HashSet<RoleId>,
}

#[derive(PartialEq)]
pub struct AddUserToRealmRolesOperation {
    input: AddUserToRealmRolesInput,
    state: AddUserToRealmRolesState,
    output: Option<Result<RealmAuthorizationDocument, AddUserToRealmRolesError>>,
}

impl std::fmt::Debug for AddUserToRealmRolesOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddUserToRealmRolesOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AddUserToRealmRolesState {
    Init,
    Auth,
    StartTransaction,
    GetAuthDoc {
        txn_id: TxnId,
    },
    UpdateAuthDoc {
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
pub enum AddUserToRealmRolesError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Role not found")]
    RoleNotFound,
    #[error("Authorization document not found")]
    AuthDocNotFound,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Adding user to realm  did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: AddUserToRealmRolesState,
        expected: &'static str,
        got: String,
    },
}

impl AddUserToRealmRolesOperation {
    pub fn new(input: AddUserToRealmRolesInput) -> Self {
        AddUserToRealmRolesOperation {
            input,
            state: AddUserToRealmRolesState::Init,
            output: None,
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                AddUserToRealmRolesState::StartTransaction,
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
            realm_id: self.input.actor.realm_id,
            path_restrictions: None,
        }
    }

    fn handle_authorization(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected_event(
                AddUserToRealmRolesState::Auth,
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
    ) -> Result<Effects, AddUserToRealmRolesError> {
        if auth_result? {
            self.state = AddUserToRealmRolesState::StartTransaction;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(AddUserToRealmRolesError::Unauthorized)
        }
    }

    fn emit_get_auth_doc(&mut self, txn_id: TxnId) -> Result<Effects, AddUserToRealmRolesError> {
        self.state = AddUserToRealmRolesState::GetAuthDoc { txn_id };
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

        match self.emit_update_auth_doc(txn_id, value) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_update_auth_doc(
        &mut self,
        txn_id: TxnId,
        auth_doc: Option<ByteView>,
    ) -> Result<Effects, AddUserToRealmRolesError> {
        let mut auth_doc = RealmAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| AddUserToRealmRolesError::AuthDocNotFound)?,
        )?;
        for role in &self.input.role_ids {
            let role = auth_doc
                .roles
                .get_mut(role)
                .ok_or_else(|| AddUserToRealmRolesError::RoleNotFound)?;
            role.assigned_users.insert(self.input.user_id);
        }

        let key = (*auth_doc.realm_id.as_bytes()).into();
        let value = auth_doc.to_bytes(&self.input.actor)?.into();

        self.state = AddUserToRealmRolesState::UpdateAuthDoc { txn_id, auth_doc };

        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: AUTH_KEYSPACE.to_string(),
            key,
            value,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_update_auth_doc(
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

        self.state = AddUserToRealmRolesState::CommitTransaction { auth_doc };
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
        self.state = AddUserToRealmRolesState::AnnounceAuthDoc {
            auth_doc: auth_doc.clone(),
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            AnnounceTopicOperation::new(
                AutomergeDocumentVariant::RealmAuthorization {
                    realm_id: auth_doc.realm_id,
                }
                .topic_id(),
                self.input.actor.node_id,
            ),
            |result| Event::SubOperation(SubOperationEvent::TopicAnnouncementResult {
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
        let Event::SubOperation(SubOperationEvent::TopicAnnouncementResult { result }) = event
        else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::SubOperation(SubOperationEvent::TopicAnnouncementResult)",
                got,
            );
        };
        if let Err(error) = result {
            return self.fail(AddUserToRealmRolesError::TopicAnnouncement(error));
        }
        self.state = AddUserToRealmRolesState::Finish;
        self.output = Some(Ok(auth_doc));
        smallvec![]
    }

    fn fail(&mut self, err: AddUserToRealmRolesError) -> Effects {
        self.state = AddUserToRealmRolesState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn fail_with_cleanup(
        &mut self,
        err: AddUserToRealmRolesError,
        cleanup_effects: Effects,
    ) -> Effects {
        self.state = AddUserToRealmRolesState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: AddUserToRealmRolesState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            AddUserToRealmRolesError::UnexpectedEvent {
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

impl Operation for AddUserToRealmRolesOperation {
    type Output = RealmAuthorizationDocument;

    type Error = AddUserToRealmRolesError;

    fn start(&mut self) -> Effects {
        self.state = AddUserToRealmRolesState::Auth;

        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.auth_context(),
                path: format!(
                    "/{}/admin/roles/{}",
                    self.input.realm_id, self.input.user_id
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
            AddUserToRealmRolesState::Auth => self.handle_authorization(event),
            AddUserToRealmRolesState::StartTransaction => self.handle_start_transaction(event),
            AddUserToRealmRolesState::GetAuthDoc { txn_id } => {
                self.handle_get_auth_doc(event, txn_id)
            }
            AddUserToRealmRolesState::UpdateAuthDoc { txn_id, auth_doc } => {
                self.handle_update_auth_doc(event, txn_id, auth_doc)
            }
            AddUserToRealmRolesState::CommitTransaction { auth_doc } => {
                self.handle_commit_transaction(event, auth_doc)
            }
            AddUserToRealmRolesState::AnnounceAuthDoc { auth_doc } => {
                self.handle_announce_auth_doc(event, auth_doc)
            }
            AddUserToRealmRolesState::Init
            | AddUserToRealmRolesState::Finish
            | AddUserToRealmRolesState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AddUserToRealmRolesState::Finish | AddUserToRealmRolesState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or_else(|| AddUserToRealmRolesError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            AddUserToRealmRolesState::GetAuthDoc { txn_id }
            | AddUserToRealmRolesState::UpdateAuthDoc { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }

            _ => smallvec![],
        }
    }
}

#[cfg(test)]
pub mod test {
    use crate::add_user_to_realm_role::{AddUserToRealmRolesInput, AddUserToRealmRolesOperation};
    use crate::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::structs::Actor;
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_add_user() {
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
            metadata_handle: None,
            task_handle: Some(task_handle),
            blob_handle: None,
        };

        let realm_id = aruna_core::structs::RealmId([0u8; 32]);
        let user_id = UserId::local(Ulid::new(), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let realm_config = CreateRealmConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            realm_description: "A realm description".to_string(),
        };
        let realm_operation = CreateRealmOperation::new(realm_config.clone());
        let (_realm, realm_auth_doc) = drive(realm_operation, &context).await.unwrap();
        drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: realm_config.actor.clone(),
            }),
            &context,
        )
        .await
        .unwrap();

        let admin_role = realm_auth_doc
            .roles
            .iter()
            .filter_map(|(id, r)| {
                if r.name == "realm_admin" {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();

        let add_user_input = AddUserToRealmRolesInput {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            realm_id,
            user_id: UserId::local(Ulid::new(), realm_id),
            role_ids: admin_role,
        };

        let add_user_operation = AddUserToRealmRolesOperation::new(add_user_input.clone());
        let auth_doc = drive(add_user_operation, &context).await.unwrap();

        assert!(
            auth_doc
                .roles
                .iter()
                .any(|(_, role)| role.assigned_users.contains(&add_user_input.user_id))
        );

        assert!(
            auth_doc
                .roles
                .iter()
                .find(|(_, v)| v.name == "realm_admin")
                .unwrap()
                .1
                .assigned_users
                .contains(&add_user_input.user_id)
        );

        net_handle.shutdown().await;
    }
}
