use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::StorageEvent;
use aruna_core::operation::Operation;
use aruna_core::structs::{AuthorizationDocument, Group, RealmId};
use aruna_core::types::UserId;
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug)]
pub struct CreateGroupConfig {
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub display_name: String,
}

pub struct CreateGroupOperation {
    config: CreateGroupConfig,
    group: Option<Group>,
    auth_doc: Option<AuthorizationDocument>,
    state: CreateGroupState,
    txn_id: Option<Ulid>,
    output: Option<Result<(Group, AuthorizationDocument), CreateGroupError>>,
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
            group_id: group_id.clone(),
            realm_id: self.config.realm_id.clone(),
        };

        self.group = Some(group.clone());

        let key = group_id.to_bytes().into();

        let value = group.to_bytes()?.into();

        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: "groups".to_string(),
            key,
            value,
            txn_id: self.txn_id,
        })])
    }

    fn emit_create_auth_doc(&mut self) -> Result<aruna_core::types::Effects, CreateGroupError> {
        self.txn_id
            .ok_or_else(|| CreateGroupError::NoTransactionFound)?;

        let group_id = self
            .group
            .as_ref()
            .ok_or_else(|| CreateGroupError::GroupNotFound)?
            .group_id;

        let auth_doc = AuthorizationDocument::new_with_default(
            self.config.user_id,
            self.config.realm_id.clone(),
            group_id,
        );

        self.auth_doc = Some(auth_doc.clone());

        let key = group_id.to_bytes().into();
        let value = auth_doc.to_bytes()?.into();
        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: "auth".to_string(),
            key,
            value,
            txn_id: self.txn_id,
        })])
    }
}

#[derive(Debug)]
pub enum CreateGroupState {
    Init,
    StartTransaction,
    CreateGroup,
    CreateRoles,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error)]
pub enum CreateGroupError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("No group found")]
    GroupNotFound,
    #[error("Creating Group did not finish")]
    NotFinished,
}

impl Operation for CreateGroupOperation {
    type Output = (Group, AuthorizationDocument);

    type Error = CreateGroupError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = CreateGroupState::StartTransaction;

        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, events: aruna_core::events::Event) -> aruna_core::types::Effects {
        match (events, &self.state) {
            (
                aruna_core::events::Event::Storage(StorageEvent::TransactionStarted { txn_id }),
                CreateGroupState::StartTransaction,
            ) => {
                self.state = CreateGroupState::CreateGroup;
                self.txn_id = Some(txn_id);
                match self.emit_create_group() {
                    Ok(effects) => effects,
                    Err(err) => {
                        self.state = CreateGroupState::Error;
                        self.output = Some(Err(err));
                        smallvec![]
                    }
                }
            }
            (
                aruna_core::events::Event::Storage(StorageEvent::WriteResult { .. }),
                CreateGroupState::CreateGroup,
            ) => {
                self.state = CreateGroupState::CreateRoles;

                match self.emit_create_auth_doc() {
                    Ok(effects) => effects,
                    Err(err) => {
                        self.state = CreateGroupState::Error;
                        self.output = Some(Err(err));
                        smallvec![]
                    }
                }
            }
            (
                aruna_core::events::Event::Storage(StorageEvent::WriteResult { .. }),
                CreateGroupState::CreateRoles,
            ) => {
                self.state = CreateGroupState::CommitTransaction;
                if let Some(txn_id) = self.txn_id {
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                } else {
                    self.state = CreateGroupState::Error;
                    self.output = Some(Err(CreateGroupError::NoTransactionFound));
                    smallvec![]
                }
            }
            (
                aruna_core::events::Event::Storage(StorageEvent::TransactionCommitted { .. }),
                CreateGroupState::CommitTransaction,
            ) => {
                if let Some(group) = &self.group
                    && let Some(auth) = &self.auth_doc
                {
                    self.state = CreateGroupState::Finish;
                    self.output = Some(Ok((group.clone(), auth.clone())));
                } else {
                    self.state = CreateGroupState::Error;
                    self.output = Some(Err(CreateGroupError::GroupNotFound));
                }
                smallvec![]
            }
            (aruna_core::events::Event::Storage(StorageEvent::Error { error }), _) => {
                self.state = CreateGroupState::Error;
                self.output = Some(Err(error.into()));
                smallvec![]
            }
            _ => {
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
        self.output.ok_or_else(|| CreateGroupError::NotFinished)?
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
    use aruna_storage::storage;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_group_creation() {
        let random_path = format!("/dev/shm/{}", Ulid::new().to_string());
        let storage_handle = storage::FjallStorage::open(&random_path).unwrap();

        let context = DriverContext { storage_handle };

        let group_config = CreateGroupConfig {
            user_id: Ulid::new(),
            realm_id: aruna_core::structs::RealmId([0u8; 32]),
            display_name: "Test group".to_string(),
        };
        let group_operation = CreateGroupOperation::new(group_config.clone());
        let result = drive(group_operation, &context).await.unwrap();
        assert_eq!(result.0.display_name, group_config.display_name);
        assert_eq!(result.0.realm_id, group_config.realm_id);
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
                    .any(|user| user == &group_config.user_id)
        }));
        assert!(result.1.roles.iter().any(|(_id, role)| {
            role.name == "user"
        }));
        assert!(result.1.roles.iter().any(|(_id, role)| {
            role.name == "viewer"
        }));
    }
}
