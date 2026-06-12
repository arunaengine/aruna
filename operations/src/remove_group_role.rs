use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, AuthContext, Group, GroupAuthorizationDocument, RealmId};
use aruna_core::types::{GroupId, RoleId, TxnId};
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::replicate_documents::replicate_documents_effect;
use aruna_core::structs::Permission;
use aruna_core::types::Effects;

#[derive(Clone, Debug, PartialEq)]
pub struct RemoveGroupRoleConfig {
    pub auth_context: AuthContext,
    pub actor: Actor,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub role_id: RoleId,
}

#[derive(PartialEq)]
pub struct RemoveGroupRoleOperation {
    input: RemoveGroupRoleConfig,
    state: RemoveGroupRoleState,
    output: Option<Result<(Group, GroupAuthorizationDocument), RemoveGroupRoleError>>,
}

impl std::fmt::Debug for RemoveGroupRoleOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoveGroupRoleOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RemoveGroupRoleState {
    Init,
    Auth,
    StartTransaction,
    GetGroup {
        txn_id: TxnId,
    },
    GetAuthDoc {
        txn_id: TxnId,
        group: Group,
    },
    RemoveRole {
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    },
    UpdateGroup {
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    },
    CommitTransaction {
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    },
    AnnounceGroupDoc {
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    },
    AnnounceAuthDoc {
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RemoveGroupRoleError {
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
    #[error("No group found")]
    GroupNotFound,
    #[error("Authorization document not found")]
    AuthDocNotFound,
    #[error("Role not found")]
    RoleNotFound,
    #[error("the admin role of a group cannot be deleted")]
    AdminRoleUndeletable,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Removing role from group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: RemoveGroupRoleState,
        expected: &'static str,
        got: String,
    },
}

impl RemoveGroupRoleOperation {
    pub fn new(input: RemoveGroupRoleConfig) -> Self {
        RemoveGroupRoleOperation {
            input,
            state: RemoveGroupRoleState::Init,
            output: None,
        }
    }

    fn handle_authorization(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected_event(
                RemoveGroupRoleState::Auth,
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
    ) -> Result<Effects, RemoveGroupRoleError> {
        if auth_result? {
            self.state = RemoveGroupRoleState::StartTransaction;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(RemoveGroupRoleError::Unauthorized)
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                RemoveGroupRoleState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };
        match self.emit_get_group(txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_get_group(&mut self, txn_id: TxnId) -> Result<Effects, RemoveGroupRoleError> {
        self.state = RemoveGroupRoleState::GetGroup { txn_id };
        let key = self.input.group_id.to_bytes().into();
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: GROUP_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_get_group(&mut self, event: Event, txn_id: TxnId) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::ReadResult)",
                got,
            );
        };

        match self.emit_get_auth_doc(value, txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_get_auth_doc(
        &mut self,
        group: Option<ByteView>,
        txn_id: TxnId,
    ) -> Result<Effects, RemoveGroupRoleError> {
        let group = Group::from_bytes(&group.ok_or_else(|| RemoveGroupRoleError::GroupNotFound)?)?;

        self.state = RemoveGroupRoleState::GetAuthDoc { txn_id, group };

        let key = self.input.group_id.to_bytes().into();
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: AUTH_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_get_auth_doc(&mut self, event: Event, txn_id: TxnId, group: Group) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::ReadResult)",
                got,
            );
        };

        match self.emit_remove_role(txn_id, group, value) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_remove_role(
        &mut self,
        txn_id: TxnId,
        mut group: Group,
        auth_doc: Option<ByteView>,
    ) -> Result<Effects, RemoveGroupRoleError> {
        let mut auth_doc = GroupAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| RemoveGroupRoleError::AuthDocNotFound)?,
        )?;
        let role = auth_doc
            .roles
            .get(&self.input.role_id)
            .ok_or_else(|| RemoveGroupRoleError::RoleNotFound)?;
        // The admin role is the only guaranteed management entry point of a group.
        if role.name == "admin" {
            return Err(RemoveGroupRoleError::AdminRoleUndeletable);
        }
        auth_doc.roles.remove(&self.input.role_id);
        group.roles.remove(&self.input.role_id);

        let key = group.group_id.to_bytes().into();
        let value = auth_doc.to_bytes(&self.input.actor)?.into();

        self.state = RemoveGroupRoleState::RemoveRole {
            txn_id,
            group,
            auth_doc,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: AUTH_KEYSPACE.to_string(),
            key,
            value,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_remove_role(
        &mut self,
        event: Event,
        txn_id: TxnId,
        group: Group,
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

        match self.emit_update_group(txn_id, group, auth_doc) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_update_group(
        &mut self,
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    ) -> Result<Effects, RemoveGroupRoleError> {
        let key = group.group_id.to_bytes().into();
        let value = group.to_bytes(&self.input.actor)?.into();

        self.state = RemoveGroupRoleState::UpdateGroup {
            txn_id,
            group,
            auth_doc,
        };

        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: GROUP_KEYSPACE.to_string(),
            key,
            value,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_update_group(
        &mut self,
        event: Event,
        txn_id: TxnId,
        group: Group,
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
        self.state = RemoveGroupRoleState::CommitTransaction { group, auth_doc };

        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_transaction(
        &mut self,
        event: Event,
        group: Group,
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
        self.state = RemoveGroupRoleState::AnnounceGroupDoc {
            group: group.clone(),
            auth_doc: auth_doc.clone(),
        };
        let document = DocumentSyncTarget::Group {
            group_id: group.group_id,
        };
        smallvec![replicate_documents_effect(
            self.input.actor.realm_id,
            self.input.actor.node_id,
            vec![document],
        )]
    }

    fn handle_announce_group_doc(
        &mut self,
        event: Event,
        group: Group,
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
            return self.fail(RemoveGroupRoleError::TopicAnnouncement(error));
        }
        self.state = RemoveGroupRoleState::AnnounceAuthDoc {
            group: group.clone(),
            auth_doc: auth_doc.clone(),
        };
        let document = DocumentSyncTarget::GroupAuthorization {
            group_id: group.group_id,
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
        group: Group,
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
            return self.fail(RemoveGroupRoleError::TopicAnnouncement(error));
        }
        self.state = RemoveGroupRoleState::Finish;
        self.output = Some(Ok((group, auth_doc)));
        smallvec![]
    }

    fn fail(&mut self, err: RemoveGroupRoleError) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(err, cleanup_effects)
    }

    fn fail_with_cleanup(
        &mut self,
        err: RemoveGroupRoleError,
        cleanup_effects: Effects,
    ) -> Effects {
        self.state = RemoveGroupRoleState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: RemoveGroupRoleState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            RemoveGroupRoleError::UnexpectedEvent {
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

impl Operation for RemoveGroupRoleOperation {
    type Output = (Group, GroupAuthorizationDocument);

    type Error = RemoveGroupRoleError;

    fn start(&mut self) -> Effects {
        self.state = RemoveGroupRoleState::Auth;

        let auth_config = CheckPermissionsConfig {
            auth_context: self.input.auth_context.clone(),
            path: format!("/{}/g/{}/admin", self.input.realm_id, self.input.group_id),
            required_permission: Permission::WRITE,
        };
        let auth_operation =
            boxed_suboperation(CheckPermissionsOperation::new(auth_config), |result| {
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed: result })
            });

        smallvec![Effect::SubOperation(auth_operation)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state.clone() {
            RemoveGroupRoleState::Auth => self.handle_authorization(event),
            RemoveGroupRoleState::StartTransaction => self.handle_start_transaction(event),
            RemoveGroupRoleState::GetGroup { txn_id } => self.handle_get_group(event, txn_id),
            RemoveGroupRoleState::GetAuthDoc { txn_id, group } => {
                self.handle_get_auth_doc(event, txn_id, group)
            }
            RemoveGroupRoleState::RemoveRole {
                txn_id,
                group,
                auth_doc,
            } => self.handle_remove_role(event, txn_id, group, auth_doc),
            RemoveGroupRoleState::UpdateGroup {
                txn_id,
                group,
                auth_doc,
            } => self.handle_update_group(event, txn_id, group, auth_doc),
            RemoveGroupRoleState::CommitTransaction { group, auth_doc } => {
                self.handle_commit_transaction(event, group, auth_doc)
            }
            RemoveGroupRoleState::AnnounceGroupDoc { group, auth_doc } => {
                self.handle_announce_group_doc(event, group, auth_doc)
            }
            RemoveGroupRoleState::AnnounceAuthDoc { group, auth_doc } => {
                self.handle_announce_auth_doc(event, group, auth_doc)
            }
            RemoveGroupRoleState::Init
            | RemoveGroupRoleState::Finish
            | RemoveGroupRoleState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RemoveGroupRoleState::Finish | RemoveGroupRoleState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .ok_or_else(|| RemoveGroupRoleError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            RemoveGroupRoleState::GetGroup { txn_id }
            | RemoveGroupRoleState::GetAuthDoc { txn_id, .. }
            | RemoveGroupRoleState::RemoveRole { txn_id, .. }
            | RemoveGroupRoleState::UpdateGroup { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }

            _ => smallvec![],
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::{HashMap, HashSet};

    use aruna_core::UserId;
    use aruna_core::structs::{
        Actor, AuthContext, Group, GroupAuthorizationDocument, Permission, RealmId, Role,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use crate::add_group_role::{AddGroupRoleConfig, AddGroupRoleOperation};
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};
    use crate::remove_group_role::{
        RemoveGroupRoleConfig, RemoveGroupRoleError, RemoveGroupRoleOperation,
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

    fn auth_context(actor: &Actor) -> AuthContext {
        AuthContext {
            user_id: actor.user_id,
            realm_id: actor.realm_id,
            path_restrictions: None,
        }
    }

    #[tokio::test]
    pub async fn test_remove_role() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, _auth_doc) = setup_group(&context).await;

        let role = Role {
            role_id: Ulid::new(),
            name: "custom_role".to_string(),
            permissions: HashMap::from([(
                format!("/{}/g/{}/meta/**", actor.realm_id, group.group_id),
                Permission::READ,
            )]),
            assigned_users: HashSet::new(),
        };
        let add_config = AddGroupRoleConfig {
            auth_context: auth_context(&actor),
            actor: actor.clone(),
            realm_id: actor.realm_id,
            group_id: group.group_id,
            role: role.clone(),
        };
        let (group, auth_doc) = drive(AddGroupRoleOperation::new(add_config), &context)
            .await
            .unwrap();
        assert!(group.roles.contains(&role.role_id));
        assert!(auth_doc.roles.contains_key(&role.role_id));

        let remove_config = RemoveGroupRoleConfig {
            auth_context: auth_context(&actor),
            actor: actor.clone(),
            realm_id: actor.realm_id,
            group_id: group.group_id,
            role_id: role.role_id,
        };
        let (group, auth_doc) = drive(RemoveGroupRoleOperation::new(remove_config), &context)
            .await
            .unwrap();
        assert!(!group.roles.contains(&role.role_id));
        assert!(!auth_doc.roles.contains_key(&role.role_id));

        net_handle.shutdown().await;
    }

    #[tokio::test]
    pub async fn test_admin_role_undeletable() {
        let (context, net_handle, _tmp) = test_context().await;
        let (actor, group, auth_doc) = setup_group(&context).await;
        let admin_role_id = *auth_doc
            .roles
            .iter()
            .find(|(_, role)| role.name == "admin")
            .unwrap()
            .0;

        let remove_config = RemoveGroupRoleConfig {
            auth_context: auth_context(&actor),
            actor: actor.clone(),
            realm_id: actor.realm_id,
            group_id: group.group_id,
            role_id: admin_role_id,
        };
        let result = drive(RemoveGroupRoleOperation::new(remove_config), &context).await;
        assert_eq!(
            result.unwrap_err(),
            RemoveGroupRoleError::AdminRoleUndeletable
        );

        net_handle.shutdown().await;
    }
}
