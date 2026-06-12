use aruna_core::document::DocumentSyncTarget;
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

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::replicate_documents::replicate_documents_effect;
use aruna_core::types::Effects;

#[derive(Clone, Debug, PartialEq)]
pub struct RemoveUserFromGroupInput {
    pub actor: Actor,
    pub group_id: GroupId,
    pub user_id: UserId,
    /// None removes the user from every role of the group.
    pub role_ids: Option<HashSet<RoleId>>,
}

#[derive(PartialEq)]
pub struct RemoveUserFromGroupOperation {
    input: RemoveUserFromGroupInput,
    state: RemoveUserFromGroupState,
    output: Option<Result<GroupAuthorizationDocument, RemoveUserFromGroupError>>,
}

impl std::fmt::Debug for RemoveUserFromGroupOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoveUserFromGroupOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RemoveUserFromGroupState {
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
pub enum RemoveUserFromGroupError {
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
    #[error("cannot remove the last admin of a group")]
    LastAdmin,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Removing user from group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: RemoveUserFromGroupState,
        expected: &'static str,
        got: String,
    },
}

impl RemoveUserFromGroupOperation {
    pub fn new(input: RemoveUserFromGroupInput) -> Self {
        RemoveUserFromGroupOperation {
            input,
            state: RemoveUserFromGroupState::Init,
            output: None,
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                RemoveUserFromGroupState::StartTransaction,
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
                RemoveUserFromGroupState::Auth,
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
    ) -> Result<Effects, RemoveUserFromGroupError> {
        if auth_result? {
            self.state = RemoveUserFromGroupState::StartTransaction;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(RemoveUserFromGroupError::Unauthorized)
        }
    }

    fn emit_get_auth_doc(&mut self, txn_id: TxnId) -> Result<Effects, RemoveUserFromGroupError> {
        self.state = RemoveUserFromGroupState::GetAuthDoc { txn_id };
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
    ) -> Result<Effects, RemoveUserFromGroupError> {
        let mut auth_doc = GroupAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| RemoveUserFromGroupError::AuthDocNotFound)?,
        )?;

        let guarded_admin_roles: Vec<RoleId> = auth_doc
            .roles
            .iter()
            .filter_map(|(role_id, role)| {
                (role.name == "admin" && role.assigned_users.contains(&self.input.user_id))
                    .then_some(*role_id)
            })
            .collect();

        match &self.input.role_ids {
            Some(role_ids) => {
                for role_id in role_ids {
                    let role = auth_doc
                        .roles
                        .get_mut(role_id)
                        .ok_or_else(|| RemoveUserFromGroupError::RoleNotFound)?;
                    role.assigned_users.remove(&self.input.user_id);
                }
            }
            None => {
                for role in auth_doc.roles.values_mut() {
                    role.assigned_users.remove(&self.input.user_id);
                }
            }
        }

        // A group must always retain at least one admin, otherwise nobody
        // could manage it anymore. This also applies to self-leave.
        if guarded_admin_roles.iter().any(|role_id| {
            auth_doc
                .roles
                .get(role_id)
                .is_some_and(|role| role.assigned_users.is_empty())
        }) {
            return Err(RemoveUserFromGroupError::LastAdmin);
        }

        let key = auth_doc.group_id.to_bytes().into();
        let value = auth_doc.to_bytes(&self.input.actor)?.into();

        self.state = RemoveUserFromGroupState::UpdateAuthDoc { txn_id, auth_doc };

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

        self.state = RemoveUserFromGroupState::CommitTransaction { auth_doc };
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
        self.state = RemoveUserFromGroupState::AnnounceAuthDoc {
            auth_doc: auth_doc.clone(),
        };
        let document = DocumentSyncTarget::GroupAuthorization {
            group_id: auth_doc.group_id,
        };
        smallvec![replicate_documents_effect(
            self.input.actor.realm_id,
            self.input.actor.node_id,
            vec![document],
        )]
    }

    fn handle_announce_auth_doc(
        &mut self,
        event: Event,
        auth_doc: GroupAuthorizationDocument,
    ) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::SubOperation(SubOperationEvent::DocumentSyncResult)",
                got,
            );
        };
        if let Err(error) = result {
            return self.fail(RemoveUserFromGroupError::TopicAnnouncement(error));
        }
        self.state = RemoveUserFromGroupState::Finish;
        self.output = Some(Ok(auth_doc));
        smallvec![]
    }

    fn fail(&mut self, err: RemoveUserFromGroupError) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(err, cleanup_effects)
    }

    fn fail_with_cleanup(
        &mut self,
        err: RemoveUserFromGroupError,
        cleanup_effects: Effects,
    ) -> Effects {
        self.state = RemoveUserFromGroupState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: RemoveUserFromGroupState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            RemoveUserFromGroupError::UnexpectedEvent {
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

impl Operation for RemoveUserFromGroupOperation {
    type Output = GroupAuthorizationDocument;

    type Error = RemoveUserFromGroupError;

    fn start(&mut self) -> Effects {
        // Self-leave needs no admin permission; the last-admin guard still applies.
        if self.input.actor.user_id == self.input.user_id {
            self.state = RemoveUserFromGroupState::StartTransaction;
            return smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })];
        }

        self.state = RemoveUserFromGroupState::Auth;

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
            RemoveUserFromGroupState::Auth => self.handle_authorization(event),
            RemoveUserFromGroupState::StartTransaction => self.handle_start_transaction(event),
            RemoveUserFromGroupState::GetAuthDoc { txn_id } => {
                self.handle_get_auth_doc(event, txn_id)
            }
            RemoveUserFromGroupState::UpdateAuthDoc { txn_id, auth_doc } => {
                self.handle_update_auth_doc(event, txn_id, auth_doc)
            }
            RemoveUserFromGroupState::CommitTransaction { auth_doc } => {
                self.handle_commit_transaction(event, auth_doc)
            }
            RemoveUserFromGroupState::AnnounceAuthDoc { auth_doc } => {
                self.handle_announce_auth_doc(event, auth_doc)
            }
            RemoveUserFromGroupState::Init
            | RemoveUserFromGroupState::Finish
            | RemoveUserFromGroupState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RemoveUserFromGroupState::Finish | RemoveUserFromGroupState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or_else(|| RemoveUserFromGroupError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            RemoveUserFromGroupState::GetAuthDoc { txn_id }
            | RemoveUserFromGroupState::UpdateAuthDoc { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }

            _ => smallvec![],
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::HashSet;

    use aruna_core::UserId;
    use aruna_core::structs::{Actor, Group, GroupAuthorizationDocument, RealmId};
    use aruna_core::types::RoleId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use crate::add_user_to_group::{AddUserToGroupInput, AddUserToGroupOperation};
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};
    use crate::get_group::{GetGroupConfig, GetGroupOperation};
    use crate::remove_user_from_group::{
        RemoveUserFromGroupError, RemoveUserFromGroupInput, RemoveUserFromGroupOperation,
    };

    async fn test_context() -> (DriverContext, NetHandle, TempDir) {
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
            net_handle: Some(net_handle.clone()),
            metadata_handle: None,
            task_handle: Some(task_handle),
            blob_handle: None,
        };
        (context, net_handle, random_path)
    }

    async fn setup_group(context: &DriverContext) -> (Actor, Group, GroupAuthorizationDocument) {
        let realm_id = RealmId([0u8; 32]);
        let user_id = UserId::local(Ulid::new(), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let actor = Actor {
            node_id,
            user_id,
            realm_id,
        };

        let realm_operation = CreateRealmOperation::new(CreateRealmConfig {
            actor: actor.clone(),
            realm_description: "Test realm".to_string(),
            oidc_providers: Vec::new(),
        });
        drive(realm_operation, context).await.unwrap();

        let group_operation = CreateGroupOperation::new(CreateGroupConfig {
            actor: actor.clone(),
            display_name: "Test group".to_string(),
        });
        let (group, auth_doc) = drive(group_operation, context).await.unwrap();
        (actor, group, auth_doc)
    }

    fn role_ids_by_name(auth_doc: &GroupAuthorizationDocument, name: &str) -> HashSet<RoleId> {
        auth_doc
            .roles
            .iter()
            .filter_map(|(k, v)| (v.name == name).then_some(*k))
            .collect()
    }

    #[tokio::test]
    pub async fn test_remove_user_from_all_roles() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;

        let member_id = UserId::local(Ulid::new(), actor.realm_id);
        let add_input = AddUserToGroupInput {
            actor: actor.clone(),
            group_id: group.group_id,
            user_id: member_id,
            role_ids: role_ids_by_name(&auth_doc, "user"),
        };
        let auth_doc = drive(AddUserToGroupOperation::new(add_input), &context)
            .await
            .unwrap();
        assert!(
            auth_doc
                .roles
                .values()
                .any(|role| role.assigned_users.contains(&member_id))
        );

        let remove_input = RemoveUserFromGroupInput {
            actor,
            group_id: group.group_id,
            user_id: member_id,
            role_ids: None,
        };
        let auth_doc = drive(RemoveUserFromGroupOperation::new(remove_input), &context)
            .await
            .unwrap();
        assert!(
            auth_doc
                .roles
                .values()
                .all(|role| !role.assigned_users.contains(&member_id))
        );

        net_handle.shutdown().await;
    }

    #[tokio::test]
    pub async fn test_last_admin_guard() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, _auth_doc) = setup_group(&context).await;

        let remove_input = RemoveUserFromGroupInput {
            actor: actor.clone(),
            group_id: group.group_id,
            user_id: actor.user_id,
            role_ids: None,
        };
        let result = drive(RemoveUserFromGroupOperation::new(remove_input), &context).await;
        assert_eq!(result.unwrap_err(), RemoveUserFromGroupError::LastAdmin);

        let (_, auth_doc) = drive(
            GetGroupOperation::new(GetGroupConfig {
                group_id: group.group_id,
            }),
            &context,
        )
        .await
        .unwrap();
        assert!(
            auth_doc
                .roles
                .values()
                .filter(|role| role.name == "admin")
                .all(|role| role.assigned_users.contains(&actor.user_id))
        );

        net_handle.shutdown().await;
    }

    #[tokio::test]
    pub async fn test_self_leave() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;

        let member_id = UserId::local(Ulid::new(), actor.realm_id);
        let add_input = AddUserToGroupInput {
            actor: actor.clone(),
            group_id: group.group_id,
            user_id: member_id,
            role_ids: role_ids_by_name(&auth_doc, "user"),
        };
        drive(AddUserToGroupOperation::new(add_input), &context)
            .await
            .unwrap();

        let member_actor = Actor {
            node_id: actor.node_id,
            user_id: member_id,
            realm_id: actor.realm_id,
        };
        let remove_input = RemoveUserFromGroupInput {
            actor: member_actor,
            group_id: group.group_id,
            user_id: member_id,
            role_ids: None,
        };
        let auth_doc = drive(RemoveUserFromGroupOperation::new(remove_input), &context)
            .await
            .unwrap();
        assert!(
            auth_doc
                .roles
                .values()
                .all(|role| !role.assigned_users.contains(&member_id))
        );

        net_handle.shutdown().await;
    }
}
