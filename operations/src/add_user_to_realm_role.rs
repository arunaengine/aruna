use aruna_core::consts::AUTH_KEYSPACE;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{Actor, RealmAuthorizationDocument, RealmId};
use aruna_core::types::{RoleId, TxnId, UserId};
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::HashSet;
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct AddUserToRealmRolesInput {
    pub actor: Actor,
    pub realm_id: RealmId,
    pub user_id: UserId,
    pub role_ids: HashSet<RoleId>,
}

pub struct AddUserToRealmRolesOperation {
    input: AddUserToRealmRolesInput,
    state: AddUserToRealmRolesState,
    output: Option<Result<RealmAuthorizationDocument, AddUserToRealmRolesError>>,
}

impl std::fmt::Debug for AddUserToRealmRolesOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddRoleOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub enum AddUserToRealmRolesState {
    Init,
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
    Finish,
    Error,
}

#[derive(Debug, Error)]
pub enum AddUserToRealmRolesError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Role not found")]
    RoleNotFound,
    #[error("Authorization document not found")]
    AuthDocNotFound,
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

    fn handle_start_transaction(&mut self, event: Event) -> aruna_core::types::Effects {
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

    fn emit_get_auth_doc(
        &mut self,
        txn_id: TxnId,
    ) -> Result<aruna_core::types::Effects, AddUserToRealmRolesError> {
        self.state = AddUserToRealmRolesState::GetAuthDoc { txn_id };
        let key = (*self.input.realm_id.as_bytes()).into();
        Ok(smallvec![Effect::Storage(StorageEffect::Read {
            key_space: AUTH_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })])
    }

    fn handle_get_auth_doc(&mut self, event: Event, txn_id: TxnId) -> aruna_core::types::Effects {
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
    ) -> Result<aruna_core::types::Effects, AddUserToRealmRolesError> {
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
    ) -> aruna_core::types::Effects {
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
    ) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::Storage(StorageEvent::TransactionCommitted)",
                got,
            );
        };
        self.state = AddUserToRealmRolesState::Finish;
        self.output = Some(Ok(auth_doc));

        smallvec![]
    }

    fn fail(&mut self, err: AddUserToRealmRolesError) -> aruna_core::types::Effects {
        self.state = AddUserToRealmRolesState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn fail_with_cleanup(
        &mut self,
        err: AddUserToRealmRolesError,
        cleanup_effects: aruna_core::types::Effects,
    ) -> aruna_core::types::Effects {
        self.state = AddUserToRealmRolesState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: AddUserToRealmRolesState,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
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

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, aruna_core::types::Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }

        Ok(event)
    }
}

impl Operation for AddUserToRealmRolesOperation {
    type Output = RealmAuthorizationDocument;

    type Error = AddUserToRealmRolesError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = AddUserToRealmRolesState::StartTransaction;

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

    fn abort(&mut self) -> aruna_core::types::Effects {
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
    use aruna_core::structs::Actor;
    use aruna_storage::storage;
    use iroh::PublicKey;
    use tempfile::tempdir;
    use ulid::Ulid;
    use crate::add_user_to_realm_role::{AddUserToRealmRolesInput, AddUserToRealmRolesOperation};
    use crate::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use crate::driver::{DriverContext, drive};

    #[tokio::test]
    pub async fn test_add_user() {
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
        let realm_config = CreateRealmConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id: realm_id.clone(),
            },
            realm_description: "A realm description".to_string(),
        };
        let realm_operation = CreateRealmOperation::new(realm_config.clone());
        let (_realm, realm_auth_doc) = drive(realm_operation, &context).await.unwrap();

        let admin_role = realm_auth_doc
            .roles
            .iter()
            .filter_map(|(id, r)| if r.name == "admin" { Some(*id) } else { None })
            .collect();

        let add_user_input = AddUserToRealmRolesInput {
            actor: Actor {
                node_id,
                user_id,
                realm_id: realm_id.clone(),
            },
            realm_id: realm_id.clone(),
            user_id: Ulid::new(),
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
                .find(|(_, v)| v.name == "admin")
                .unwrap()
                .1
                .assigned_users
                .get(&add_user_input.user_id)
                .is_some()
        );
    }
}
