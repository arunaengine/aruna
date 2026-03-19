use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::consts::{AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, Group, GroupAuthorizationDocument};
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;
use ulid::Ulid;

use crate::automerge_announce::AnnounceAutomergeDocumentOperation;

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
    fn emit_create_group(&mut self) -> Result<aruna_core::types::Effects, CreateGroupError> {
        let group_id = Ulid::new();
        let group = Group {
            roles: HashSet::new(),
            display_name: self.config.display_name.clone(),
            group_id,
            realm_id: self.config.actor.realm_id.clone(),
        };

        self.group = Some(group.clone());

        let key = group_id.to_bytes().into();

        let value = group.to_bytes(&self.config.actor)?.into();

        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: GROUP_KEYSPACE.to_string(),
            key,
            value,
            txn_id: self.txn_id,
        })])
    }

    fn emit_create_auth_doc(&mut self) -> Result<aruna_core::types::Effects, CreateGroupError> {
        self.txn_id.ok_or(CreateGroupError::NoTransactionFound)?;

        let group_id = self
            .group
            .as_ref()
            .ok_or(CreateGroupError::GroupNotFound)?
            .group_id;

        let auth_doc = GroupAuthorizationDocument::new_default_group_doc(
            self.config.actor.user_id,
            self.config.actor.realm_id.clone(),
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

    fn fail(&mut self, err: CreateGroupError) -> aruna_core::types::Effects {
        self.state = CreateGroupState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn fail_with_cleanup(
        &mut self,
        err: CreateGroupError,
        cleanup_effects: aruna_core::types::Effects,
    ) -> aruna_core::types::Effects {
        self.state = CreateGroupState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: CreateGroupState,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
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

    fn handle_create_group(&mut self, event: Event) -> aruna_core::types::Effects {
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

    fn handle_create_roles(&mut self, event: Event) -> aruna_core::types::Effects {
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

    fn handle_commit_transaction(&mut self, event: Event) -> aruna_core::types::Effects {
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
            self.state = CreateGroupState::AnnounceAuthDoc;
            smallvec![Effect::SubOperation(boxed_suboperation(
                AnnounceAutomergeDocumentOperation::new(
                    AutomergeDocumentVariant::GroupAuthorization {
                        group_id: group.group_id,
                    }
                ),
                |result| Event::SubOperation(SubOperationEvent::AutomergeStateResult {
                    result: result.map_err(|error| error.to_string()),
                }),
            ))]
        } else {
            self.fail(CreateGroupError::GroupNotFound)
        }
    }

    fn handle_announce_auth_doc(&mut self, event: Event) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AutomergeStateResult { result }) = event else {
            return self.unexpected_event(
                CreateGroupState::AnnounceAuthDoc,
                "Event::SubOperation(SubOperationEvent::AutomergeStateResult)",
                got,
            );
        };

        if let Err(error) = result {
            return self.fail(CreateGroupError::AutomergeState(error));
        }

        if let Some(group) = &self.group
            && let Some(auth) = &self.auth_doc
        {
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
    AnnounceAuthDoc,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateGroupError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("automerge announcement failed: {0}")]
    AutomergeState(String),
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

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = CreateGroupState::StartTransaction;

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
            CreateGroupState::StartTransaction => self.handle_start_transaction(event),
            CreateGroupState::CreateGroup => self.handle_create_group(event),
            CreateGroupState::CreateRoles => self.handle_create_roles(event),
            CreateGroupState::CommitTransaction => self.handle_commit_transaction(event),
            CreateGroupState::AnnounceAuthDoc => self.handle_announce_auth_doc(event),
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

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(CreateGroupError::NotFinished)?
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
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::driver::{DriverContext, drive};
    use aruna_core::structs::Actor;
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_group_creation() {
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
            blob_handle: None,
            net_handle: Some(net_handle.clone()),
            automerge_handle: None,
            task_handle: Some(task_handle),
        };

        let user_id = Ulid::new();
        let realm_id = aruna_core::structs::RealmId([0u8; 32]);
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
