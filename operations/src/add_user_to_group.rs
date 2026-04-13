use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, AuthContext, GroupAuthorizationDocument, Permission};
use aruna_core::types::{GroupId, RoleId, TxnId, UserId};
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;

use crate::automerge_announce::AnnounceTopicOperation;
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_core::types::Effects;

#[derive(Clone, Debug, PartialEq)]
pub struct AddUserToGroupInput {
    pub actor: Actor,
    pub group_id: GroupId,
    pub user_id: UserId,
    pub role_ids: HashSet<RoleId>,
}

#[derive(PartialEq)]
pub struct AddUserToGroupOperation {
    input: AddUserToGroupInput,
    state: AddUserToGroupState,
    output: Option<Result<GroupAuthorizationDocument, AddUserToGroupError>>,
}

impl std::fmt::Debug for AddUserToGroupOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddUserToGroupOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AddUserToGroupState {
    Init,
    Auth,
    StartTransaction,
    GetAuthDoc {
        txn_id: TxnId,
    },
    UpdateAuthDoc {
        txn_id: TxnId,
        auth_doc: GroupAuthorizationDocument,
    },
    CommitTransaction {
        auth_doc: GroupAuthorizationDocument,
    },
    AnnounceAuthDoc {
        auth_doc: GroupAuthorizationDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum AddUserToGroupError {
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
    #[error("Adding user to group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: AddUserToGroupState,
        expected: &'static str,
        got: String,
    },
}

impl AddUserToGroupOperation {
    pub fn new(input: AddUserToGroupInput) -> Self {
        AddUserToGroupOperation {
            input,
            state: AddUserToGroupState::Init,
            output: None,
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                AddUserToGroupState::StartTransaction,
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
                AddUserToGroupState::Auth,
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
    ) -> Result<Effects, AddUserToGroupError> {
        if auth_result? {
            self.state = AddUserToGroupState::StartTransaction;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(AddUserToGroupError::Unauthorized)
        }
    }

    fn emit_get_auth_doc(&mut self, txn_id: TxnId) -> Result<Effects, AddUserToGroupError> {
        self.state = AddUserToGroupState::GetAuthDoc { txn_id };
        let key = self.input.group_id.to_bytes().into();
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
    ) -> Result<Effects, AddUserToGroupError> {
        let mut auth_doc = GroupAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| AddUserToGroupError::AuthDocNotFound)?,
        )?;
        for role in &self.input.role_ids {
            let role = auth_doc
                .roles
                .get_mut(role)
                .ok_or_else(|| AddUserToGroupError::RoleNotFound)?;
            role.assigned_users.insert(self.input.user_id);
        }

        let key = auth_doc.group_id.to_bytes().into();
        let value = auth_doc.to_bytes(&self.input.actor)?.into();

        self.state = AddUserToGroupState::UpdateAuthDoc { txn_id, auth_doc };

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
        auth_doc: GroupAuthorizationDocument,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::WriteResult)",
                got,
            );
        };

        self.state = AddUserToGroupState::CommitTransaction { auth_doc };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_transaction(
        &mut self,
        event: Event,
        auth_doc: GroupAuthorizationDocument,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        };
        self.state = AddUserToGroupState::AnnounceAuthDoc {
            auth_doc: auth_doc.clone(),
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            AnnounceTopicOperation::new(
                AutomergeDocumentVariant::GroupAuthorization {
                    group_id: auth_doc.group_id,
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
        auth_doc: GroupAuthorizationDocument,
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
            return self.fail(AddUserToGroupError::TopicAnnouncement(error));
        }
        self.state = AddUserToGroupState::Finish;
        self.output = Some(Ok(auth_doc));
        smallvec![]
    }

    fn fail(&mut self, err: AddUserToGroupError) -> Effects {
        self.state = AddUserToGroupState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn fail_with_cleanup(&mut self, err: AddUserToGroupError, cleanup_effects: Effects) -> Effects {
        self.state = AddUserToGroupState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: AddUserToGroupState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            AddUserToGroupError::UnexpectedEvent {
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

impl Operation for AddUserToGroupOperation {
    type Output = GroupAuthorizationDocument;

    type Error = AddUserToGroupError;

    fn start(&mut self) -> Effects {
        self.state = AddUserToGroupState::Auth;

        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.auth_context(),
                path: format!(
                    "/{}/g/{}/admin/users/{}",
                    self.input.actor.realm_id, self.input.group_id, self.input.user_id
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
            AddUserToGroupState::Auth => self.handle_authorization(event),
            AddUserToGroupState::StartTransaction => self.handle_start_transaction(event),
            AddUserToGroupState::GetAuthDoc { txn_id } => self.handle_get_auth_doc(event, txn_id),
            AddUserToGroupState::UpdateAuthDoc { txn_id, auth_doc } => {
                self.handle_update_auth_doc(event, txn_id, auth_doc)
            }
            AddUserToGroupState::CommitTransaction { auth_doc } => {
                self.handle_commit_transaction(event, auth_doc)
            }
            AddUserToGroupState::AnnounceAuthDoc { auth_doc } => {
                self.handle_announce_auth_doc(event, auth_doc)
            }
            AddUserToGroupState::Init
            | AddUserToGroupState::Finish
            | AddUserToGroupState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AddUserToGroupState::Finish | AddUserToGroupState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or_else(|| AddUserToGroupError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            AddUserToGroupState::GetAuthDoc { txn_id }
            | AddUserToGroupState::UpdateAuthDoc { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }

            _ => smallvec![],
        }
    }
}

#[cfg(test)]
pub mod test {
    use aruna_core::structs::Actor;
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::add_user_to_group::{AddUserToGroupInput, AddUserToGroupOperation};
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};

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

        let user_id = Ulid::new();
        let realm_id = aruna_core::structs::RealmId([0u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();

        let realm_config = CreateRealmConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id: realm_id.clone(),
            },
            realm_description: "Test realm".to_string(),
        };
        let realm_operation = CreateRealmOperation::new(realm_config);
        let _ = drive(realm_operation, &context).await.unwrap();

        let group_config = CreateGroupConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id: realm_id.clone(),
            },
            display_name: "Test group".to_string(),
        };
        let group_operation = CreateGroupOperation::new(group_config.clone());
        let (group, auth_doc) = drive(group_operation, &context).await.unwrap();

        let reader_writer_roles = auth_doc
            .roles
            .iter()
            .filter_map(|(k, v)| if v.name == "user" { Some(*k) } else { None })
            .collect();

        let add_user_input = AddUserToGroupInput {
            actor: Actor {
                node_id,
                user_id,
                realm_id: realm_id.clone(),
            },
            group_id: group.group_id,
            user_id: Ulid::new(),
            role_ids: reader_writer_roles,
        };

        let add_user_operation = AddUserToGroupOperation::new(add_user_input.clone());
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
                .find(|(_, v)| v.name == "user")
                .unwrap()
                .1
                .assigned_users
                .contains(&add_user_input.user_id)
        );

        net_handle.shutdown().await;
    }
}
