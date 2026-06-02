use crate::replicate_documents::replicate_documents_effect;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{Actor, Group, GroupAuthorizationDocument};
use aruna_core::types::Effects;
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;
use tracing::trace;
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateGroupConfig {
    pub actor: Actor,
    pub display_name: String,
}

#[derive(PartialEq)]
pub struct CreateGroupOperation {
    config: CreateGroupConfig,
    group: Option<Group>,
    auth_doc: Option<GroupAuthorizationDocument>,
    state: CreateGroupState,
    txn_id: Option<Ulid>,
    output: Option<Result<(Group, GroupAuthorizationDocument), CreateGroupError>>,
}

impl std::fmt::Debug for CreateGroupOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateGroupOperation")
            .field("config", &self.config)
            .field("group", &self.group)
            .field("auth_doc", &self.auth_doc)
            .field("state", &self.state)
            .field("txn_id", &self.txn_id)
            .field("output", &self.output)
            .finish()
    }
}

impl CreateGroupOperation {
    pub fn new(config: CreateGroupConfig) -> Self {
        CreateGroupOperation {
            config,
            group: None,
            auth_doc: None,
            state: CreateGroupState::Init,
            txn_id: None,
            output: None,
        }
    }
    #[tracing::instrument(name = "group.create.emit_group", level = "debug", skip(self), fields(state = ?self.state, group_name = %self.config.display_name))]
    fn emit_create_group(&mut self) -> Result<Effects, CreateGroupError> {
        let group_id = Ulid::new();
        let group = Group {
            roles: HashSet::new(),
            display_name: self.config.display_name.clone(),
            group_id,
            realm_id: self.config.actor.realm_id,
        };

        self.group = Some(group.clone());

        trace!(
            event = "group.create.started",
            group_id = %group.group_id,
            realm_id = %group.realm_id,
            user_id = %self.config.actor.user_id,
            "Creating group"
        );

        let key = group_id.to_bytes().into();

        let value = group.to_bytes(&self.config.actor)?.into();

        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: GROUP_KEYSPACE.to_string(),
            key,
            value,
            txn_id: self.txn_id,
        })])
    }

    #[tracing::instrument(name = "group.create.emit_auth_doc", level = "debug", skip(self), fields(state = ?self.state))]
    fn emit_create_auth_doc(&mut self) -> Result<Effects, CreateGroupError> {
        self.txn_id.ok_or(CreateGroupError::NoTransactionFound)?;

        let group_id = self
            .group
            .as_ref()
            .ok_or(CreateGroupError::GroupNotFound)?
            .group_id;

        let auth_doc = GroupAuthorizationDocument::new_default_group_doc(
            self.config.actor.user_id,
            self.config.actor.realm_id,
            group_id,
        );

        self.auth_doc = Some(auth_doc.clone());

        let key = group_id.to_bytes().into();
        let value = auth_doc.to_bytes(&self.config.actor)?.into();
        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: AUTH_KEYSPACE.to_string(),
            key,
            value,
            txn_id: self.txn_id,
        })])
    }

    #[tracing::instrument(name = "group.create.fail", level = "debug", skip(self), fields(state = ?self.state, error = %err))]
    fn fail(&mut self, err: CreateGroupError) -> Effects {
        self.state = CreateGroupState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    #[tracing::instrument(name = "group.create.fail_with_cleanup", level = "debug", skip(self, cleanup_effects), fields(state = ?self.state, error = %err))]
    fn fail_with_cleanup(&mut self, err: CreateGroupError, cleanup_effects: Effects) -> Effects {
        self.state = CreateGroupState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    #[tracing::instrument(name = "group.create.unexpected_event", level = "debug", skip(self, got), fields(current_state = ?self.state, expected, got = %got))]
    fn unexpected_event(
        &mut self,
        state: CreateGroupState,
        expected: &'static str,
        got: String,
    ) -> Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            CreateGroupError::UnexpectedEvent {
                state,
                expected,
                got,
            },
            cleanup_effects,
        )
    }

    #[tracing::instrument(name = "group.create.fail_on_storage_error", level = "trace", skip(self, event), fields(state = ?self.state, event = ?event))]
    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }

    #[tracing::instrument(name = "group.create.handle_start_transaction", level = "debug", skip(self, event), fields(state = ?self.state, event = ?event))]
    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                CreateGroupState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };

        self.state = CreateGroupState::CreateGroup;
        self.txn_id = Some(txn_id);
        match self.emit_create_group() {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    #[tracing::instrument(name = "group.create.handle_group_write", level = "debug", skip(self, event), fields(state = ?self.state, event = ?event))]
    fn handle_create_group(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                CreateGroupState::CreateGroup,
                "Event::Storage(StorageEvent::WriteResult)",
                got,
            );
        };

        self.state = CreateGroupState::CreateRoles;
        match self.emit_create_auth_doc() {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    #[tracing::instrument(name = "group.create.handle_auth_write", level = "debug", skip(self, event), fields(state = ?self.state, event = ?event))]
    fn handle_create_roles(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                CreateGroupState::CreateRoles,
                "Event::Storage(StorageEvent::WriteResult)",
                got,
            );
        };

        self.state = CreateGroupState::CommitTransaction;
        if let Some(txn_id) = self.txn_id {
            smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
        } else {
            self.fail(CreateGroupError::NoTransactionFound)
        }
    }

    #[tracing::instrument(name = "group.create.handle_commit", level = "debug", skip(self, event), fields(state = ?self.state, event = ?event))]
    fn handle_commit_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                CreateGroupState::CommitTransaction,
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        };

        if let Some(group) = &self.group
            && self.auth_doc.is_some()
        {
            self.state = CreateGroupState::ReplicateDocuments;
            trace!(
                event = "group.create.announce_group",
                group_id = %group.group_id,
                realm_id = %group.realm_id,
                user_id = %self.config.actor.user_id,
                "Announcing group"
            );
            smallvec![replicate_documents_effect(
                self.config.actor.realm_id,
                self.config.actor.node_id,
                vec![
                    DocumentSyncTarget::Group {
                        group_id: group.group_id,
                    },
                    DocumentSyncTarget::GroupAuthorization {
                        group_id: group.group_id,
                    },
                ],
            )]
        } else {
            self.fail(CreateGroupError::GroupNotFound)
        }
    }

    #[tracing::instrument(name = "group.create.handle_replicate", level = "debug", skip(self, event), fields(state = ?self.state, event = ?event))]
    fn handle_replicate_documents(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) = event else {
            return self.unexpected_event(
                CreateGroupState::ReplicateDocuments,
                "Event::SubOperation(SubOperationEvent::DocumentSyncResult)",
                got,
            );
        };

        if let Err(error) = result {
            return self.fail(CreateGroupError::DocumentSync(error));
        }

        if let Some(group) = &self.group
            && let Some(auth) = &self.auth_doc
        {
            trace!(
                event = "group.create.completed",
                group_id = %group.group_id,
                realm_id = %group.realm_id,
                "Created and replicated group"
            );
            self.state = CreateGroupState::Finish;
            self.output = Some(Ok((group.clone(), auth.clone())));
            smallvec![]
        } else {
            self.fail(CreateGroupError::GroupNotFound)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CreateGroupState {
    Init,
    StartTransaction,
    CreateGroup,
    CreateRoles,
    CommitTransaction,
    ReplicateDocuments,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateGroupError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("document sync failed: {0}")]
    DocumentSync(String),
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("No group found")]
    GroupNotFound,
    #[error("Creating Group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: CreateGroupState,
        expected: &'static str,
        got: String,
    },
}

impl Operation for CreateGroupOperation {
    type Output = (Group, GroupAuthorizationDocument);

    type Error = CreateGroupError;

    #[tracing::instrument(name = "group.create.start", level = "debug", skip(self), fields(group_name = %self.config.display_name))]
    fn start(&mut self) -> Effects {
        self.state = CreateGroupState::StartTransaction;

        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    #[tracing::instrument(name = "group.create.step", level = "debug", skip(self, event), fields(state = ?self.state, event = ?event))]
    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            CreateGroupState::StartTransaction => self.handle_start_transaction(event),
            CreateGroupState::CreateGroup => self.handle_create_group(event),
            CreateGroupState::CreateRoles => self.handle_create_roles(event),
            CreateGroupState::CommitTransaction => self.handle_commit_transaction(event),
            CreateGroupState::ReplicateDocuments => self.handle_replicate_documents(event),
            CreateGroupState::Init | CreateGroupState::Finish | CreateGroupState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateGroupState::Finish | CreateGroupState::Error
        )
    }

    #[tracing::instrument(name = "group.create.finalize", level = "debug", skip(self), fields(state = ?self.state))]
    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(CreateGroupError::NotFinished)?
    }

    #[tracing::instrument(name = "group.create.abort", level = "debug", skip(self), fields(state = ?self.state, txn_id = ?self.txn_id))]
    fn abort(&mut self) -> Effects {
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod test {
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::structs::Actor;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_group_creation() {
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

        let realm_id = aruna_core::structs::RealmId([0u8; 32]);
        let user_id = UserId::local(Ulid::new(), realm_id);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let group_config = CreateGroupConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            display_name: "Test group".to_string(),
        };
        let group_operation = CreateGroupOperation::new(group_config.clone());
        let result = drive(group_operation, &context).await.unwrap();
        assert_eq!(result.0.display_name, group_config.display_name);
        assert_eq!(result.0.realm_id, group_config.actor.realm_id);
        assert!(
            result
                .0
                .roles
                .iter()
                .all(|r| result.1.roles.iter().any(|(id, _)| id == r))
        );
        assert!(result.1.roles.iter().any(|(_id, role)| {
            role.name == "admin"
                && role
                    .assigned_users
                    .iter()
                    .any(|user| user == &group_config.actor.user_id)
        }));
        assert!(
            result
                .1
                .roles
                .iter()
                .any(|(_id, role)| { role.name == "user" })
        );
        assert!(
            result
                .1
                .roles
                .iter()
                .any(|(_id, role)| { role.name == "viewer" })
        );

        net_handle.shutdown().await;
    }
}
