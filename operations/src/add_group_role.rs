use aruna_core::consts::{AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{Actor, Group, GroupAuthorizationDocument, Role};
use aruna_core::types::{GroupId, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct AddRoleConfig {
    pub actor: Actor,
    pub group_id: GroupId,
    pub role: Role,
}

pub struct AddRoleOperation {
    input: AddRoleConfig,
    state: AddRoleState,
    output: Option<Result<(Group, GroupAuthorizationDocument), AddRoleError>>,
}

impl std::fmt::Debug for AddRoleOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddRoleOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub enum AddRoleState {
    Init,
    StartTransaction,
    GetGroup {
        txn_id: TxnId,
    },
    GetAuthDoc {
        txn_id: TxnId,
        group: Group,
    },
    CreateRole {
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
    Finish,
    Error,
}

#[derive(Debug, Error)]
pub enum AddRoleError {
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
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: AddRoleState,
        expected: &'static str,
        got: String,
    },
}

impl AddRoleOperation {
    pub fn new(input: AddRoleConfig) -> Self {
        AddRoleOperation {
            input,
            state: AddRoleState::Init,
            output: None,
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                AddRoleState::StartTransaction,
                "Event::Storage(StorageEvent::TransactionStarted)",
                got,
            );
        };
        match self.emit_get_group(txn_id) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_get_group(
        &mut self,
        txn_id: TxnId,
    ) -> Result<aruna_core::types::Effects, AddRoleError> {
        self.state = AddRoleState::GetGroup { txn_id };
        let key = self.input.group_id.to_bytes().into();
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: GROUP_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_get_group(&mut self, event: Event, txn_id: TxnId) -> aruna_core::types::Effects {
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
    ) -> Result<aruna_core::types::Effects, AddRoleError> {
        let group = Group::from_bytes(&group.ok_or_else(|| AddRoleError::GroupNotFound)?)?;

        self.state = AddRoleState::GetAuthDoc { txn_id, group };

        let key = self.input.group_id.to_bytes().into();
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: AUTH_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_get_auth_doc(
        &mut self,
        event: Event,
        txn_id: TxnId,
        group: Group,
    ) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::ReadResult)",
                got,
            );
        };

        match self.emit_create_role(txn_id, group, value) {
            Ok(effects) => effects,
            Err(err) => self.fail(err),
        }
    }

    fn emit_create_role(
        &mut self,
        txn_id: TxnId,
        mut group: Group,
        auth_doc: Option<ByteView>,
    ) -> Result<aruna_core::types::Effects, AddRoleError> {
        let mut auth_doc = GroupAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| AddRoleError::GroupNotFound)?,
        )?;
        auth_doc
            .roles
            .insert(self.input.role.role_id, self.input.role.clone());

        group.roles.insert(self.input.role.role_id);

        let key = group.group_id.to_bytes().into();
        let value = auth_doc.to_bytes(&self.input.actor)?.into();

        self.state = AddRoleState::CreateRole {
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

    fn handle_create_role(
        &mut self,
        event: Event,
        txn_id: TxnId,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    ) -> aruna_core::types::Effects {
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
    ) -> Result<aruna_core::types::Effects, AddRoleError> {
        let key = group.group_id.to_bytes().into();
        let value = group.to_bytes(&self.input.actor)?.into();

        self.state = AddRoleState::UpdateGroup {
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
    ) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::WriteResult)",
                got,
            );
        };
        self.state = AddRoleState::CommitTransaction { group, auth_doc };

        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_transaction(
        &mut self,
        event: Event,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    ) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        };
        self.state = AddRoleState::Finish;
        self.output = Some(Ok((group, auth_doc)));

        smallvec![]
    }

    fn fail(&mut self, err: AddRoleError) -> aruna_core::types::Effects {
        self.state = AddRoleState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn fail_with_cleanup(
        &mut self,
        err: AddRoleError,
        cleanup_effects: aruna_core::types::Effects,
    ) -> aruna_core::types::Effects {
        self.state = AddRoleState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: AddRoleState,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            AddRoleError::UnexpectedEvent {
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
}

impl Operation for AddRoleOperation {
    type Output = (Group, GroupAuthorizationDocument);

    type Error = AddRoleError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = AddRoleState::StartTransaction;

        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state.clone() {
            AddRoleState::StartTransaction => self.handle_start_transaction(event),
            AddRoleState::GetGroup { txn_id } => self.handle_get_group(event, txn_id),
            AddRoleState::GetAuthDoc { txn_id, group } => {
                self.handle_get_auth_doc(event, txn_id, group)
            }
            AddRoleState::CreateRole {
                txn_id,
                group,
                auth_doc,
            } => self.handle_create_role(event, txn_id, group, auth_doc),
            AddRoleState::UpdateGroup {
                txn_id,
                group,
                auth_doc,
            } => self.handle_update_group(event, txn_id, group, auth_doc),
            AddRoleState::CommitTransaction { group, auth_doc } => {
                self.handle_commit_transaction(event, group, auth_doc)
            }
            AddRoleState::Init | AddRoleState::Finish | AddRoleState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, AddRoleState::Finish | AddRoleState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or_else(|| AddRoleError::NotFinished)?
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        match self.state {
            AddRoleState::GetGroup { txn_id }
            | AddRoleState::GetAuthDoc { txn_id, .. }
            | AddRoleState::CreateRole { txn_id, .. }
            | AddRoleState::UpdateGroup { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }

            _ => smallvec![],
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::{HashMap, HashSet};

    use aruna_core::structs::{Actor, Permission, Role};
    use aruna_storage::storage;
    use iroh::PublicKey;
    use tempfile::tempdir;
    use ulid::Ulid;

    use crate::add_group_role::{AddRoleConfig, AddRoleOperation};
    use crate::create_group::{CreateGroupConfig, CreateGroupOperation};
    use crate::driver::{DriverContext, drive};

    #[tokio::test]
    pub async fn test_add_role() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(&random_path.path().to_str().unwrap()).unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: None,
        };

        let user_id = Ulid::new();
        let realm_id = aruna_core::structs::RealmId([0u8; 32]);
        let node_id = PublicKey::from_bytes(&[0u8; 32]).unwrap();
        let group_config = CreateGroupConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id: realm_id.clone(),
            },
            display_name: "Test group".to_string(),
        };
        let group_operation = CreateGroupOperation::new(group_config.clone());
        let result = drive(group_operation, &context).await.unwrap();

        let add_role_input = AddRoleConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id: realm_id.clone(),
            },

            group_id: result.0.group_id,
            role: Role {
                role_id: Ulid::new(),
                name: "test_role".to_string(),
                permissions: HashMap::from([(
                    format!(
                        "{}/g/{}/meta/{}",
                        realm_id.to_string(),
                        result.0.group_id.to_string(),
                        Ulid::new(),
                    ),
                    Permission::READ,
                )]),
                assigned_users: HashSet::from([user_id]),
            },
        };

        let add_role_operation = AddRoleOperation::new(add_role_input.clone());
        let (group, auth_doc) = drive(add_role_operation, &context).await.unwrap();

        assert!(group.roles.contains(&add_role_input.role.role_id));
        assert_eq!(
            auth_doc.roles.get(&add_role_input.role.role_id).unwrap(),
            &add_role_input.role
        );
    }
}
