use aruna_core::consts::{AUTH_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, AuthContext, Group, GroupAuthorizationDocument, RealmId, Role};
use aruna_core::types::{GroupId, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::automerge_announce::AnnounceAutomergeDocumentOperation;

#[derive(Clone, Debug, PartialEq)]
pub struct AddGroupRoleConfig {
    pub auth_context: AuthContext,
    pub actor: Actor,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub role: Role,
}

#[derive(PartialEq)]
pub struct AddGroupRoleOperation {
    input: AddGroupRoleConfig,
    state: AddGroupRoleState,
    output: Option<Result<(Group, GroupAuthorizationDocument), AddGroupRoleError>>,
}

impl std::fmt::Debug for AddGroupRoleOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddRoleOperation")
            .field("input", &self.input)
            .field("state", &self.state)
            .field("output", &self.output)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AddGroupRoleState {
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
    AnnounceAuthDoc {
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum AddGroupRoleError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("No group found")]
    GroupNotFound,
    #[error(transparent)]
    CheckPermissionsError(#[from] AuthorizationError),
    #[error("Adding role to group did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: AddGroupRoleState,
        expected: &'static str,
        got: String,
    },
}

impl AddGroupRoleOperation {
    pub fn new(input: AddGroupRoleConfig) -> Self {
        AddGroupRoleOperation {
            input,
            state: AddGroupRoleState::Init,
            output: None,
        }
    }

    fn handle_authorization(&mut self, event: Event) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(aruna_core::events::SubOperationEvent::AuthorizationResult {
            allowed,
        }) = event
        else {
            return self.unexpected_event(
                AddGroupRoleState::Auth,
                "Event::SubOperation(SuboperationEvent::AuthorizationResult)",
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
    ) -> Result<aruna_core::types::Effects, AddGroupRoleError> {
        if auth_result? {
            self.state = AddGroupRoleState::StartTransaction;
            Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )])
        } else {
            Err(AddGroupRoleError::Unauthorized)
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                AddGroupRoleState::StartTransaction,
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
    ) -> Result<aruna_core::types::Effects, AddGroupRoleError> {
        self.state = AddGroupRoleState::GetGroup { txn_id };
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
    ) -> Result<aruna_core::types::Effects, AddGroupRoleError> {
        let group = Group::from_bytes(&group.ok_or_else(|| AddGroupRoleError::GroupNotFound)?)?;

        self.state = AddGroupRoleState::GetAuthDoc { txn_id, group };

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
    ) -> Result<aruna_core::types::Effects, AddGroupRoleError> {
        let mut auth_doc = GroupAuthorizationDocument::from_bytes(
            &auth_doc.ok_or_else(|| AddGroupRoleError::GroupNotFound)?,
        )?;
        auth_doc
            .roles
            .insert(self.input.role.role_id, self.input.role.clone());

        group.roles.insert(self.input.role.role_id);

        let key = group.group_id.to_bytes().into();
        let value = auth_doc.to_bytes(&self.input.actor)?.into();

        self.state = AddGroupRoleState::CreateRole {
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
    ) -> Result<aruna_core::types::Effects, AddGroupRoleError> {
        let key = group.group_id.to_bytes().into();
        let value = group.to_bytes(&self.input.actor)?.into();

        self.state = AddGroupRoleState::UpdateGroup {
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
        self.state = AddGroupRoleState::CommitTransaction { group, auth_doc };

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
        self.state = AddGroupRoleState::AnnounceAuthDoc {
            group: group.clone(),
            auth_doc: auth_doc.clone(),
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            AnnounceAutomergeDocumentOperation::new(AutomergeDocumentVariant::GroupAuthorization {
                group_id: group.group_id,
            }),
            |result| Event::SubOperation(SubOperationEvent::AutomergeStateResult {
                result: result.map_err(|error| error.to_string()),
            }),
        ))]
    }

    fn handle_announce_auth_doc(
        &mut self,
        event: Event,
        group: Group,
        auth_doc: GroupAuthorizationDocument,
    ) -> aruna_core::types::Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AutomergeStateResult { .. }) = event else {
            return self.unexpected_event(
                self.state.clone(),
                "Event::SubOperation(SubOperationEvent::AutomergeStateResult)",
                got,
            );
        };
        self.state = AddGroupRoleState::Finish;
        self.output = Some(Ok((group, auth_doc)));
        smallvec![]
    }

    fn fail(&mut self, err: AddGroupRoleError) -> aruna_core::types::Effects {
        self.state = AddGroupRoleState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn fail_with_cleanup(
        &mut self,
        err: AddGroupRoleError,
        cleanup_effects: aruna_core::types::Effects,
    ) -> aruna_core::types::Effects {
        self.state = AddGroupRoleState::Error;
        self.output = Some(Err(err));
        cleanup_effects
    }

    fn unexpected_event(
        &mut self,
        state: AddGroupRoleState,
        expected: &'static str,
        got: String,
    ) -> aruna_core::types::Effects {
        let cleanup_effects = self.abort();
        self.fail_with_cleanup(
            AddGroupRoleError::UnexpectedEvent {
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

impl Operation for AddGroupRoleOperation {
    type Output = (Group, GroupAuthorizationDocument);

    type Error = AddGroupRoleError;

    fn start(&mut self) -> aruna_core::types::Effects {
        self.state = AddGroupRoleState::Auth;

        let auth_config = CheckPermissionsConfig {
            auth_context: self.input.auth_context.clone(),
            path: format!(
                "/{}/g/{}/admin",
                self.input.realm_id.to_string(),
                self.input.group_id.to_string()
            ),
            required_permission: aruna_core::structs::Permission::WRITE,
        };
        let auth_operation =
            boxed_suboperation(CheckPermissionsOperation::new(auth_config), |result| {
                Event::SubOperation(aruna_core::events::SubOperationEvent::AuthorizationResult {
                    allowed: result,
                })
            });

        smallvec![Effect::SubOperation(auth_operation)]
    }

    fn step(&mut self, event: Event) -> aruna_core::types::Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state.clone() {
            AddGroupRoleState::Auth => self.handle_authorization(event),
            AddGroupRoleState::StartTransaction => self.handle_start_transaction(event),
            AddGroupRoleState::GetGroup { txn_id } => self.handle_get_group(event, txn_id),
            AddGroupRoleState::GetAuthDoc { txn_id, group } => {
                self.handle_get_auth_doc(event, txn_id, group)
            }
            AddGroupRoleState::CreateRole {
                txn_id,
                group,
                auth_doc,
            } => self.handle_create_role(event, txn_id, group, auth_doc),
            AddGroupRoleState::UpdateGroup {
                txn_id,
                group,
                auth_doc,
            } => self.handle_update_group(event, txn_id, group, auth_doc),
            AddGroupRoleState::CommitTransaction { group, auth_doc } => {
                self.handle_commit_transaction(event, group, auth_doc)
            }
            AddGroupRoleState::AnnounceAuthDoc { group, auth_doc } => {
                self.handle_announce_auth_doc(event, group, auth_doc)
            }
            AddGroupRoleState::Init | AddGroupRoleState::Finish | AddGroupRoleState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AddGroupRoleState::Finish | AddGroupRoleState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or_else(|| AddGroupRoleError::NotFinished)?
    }

    fn abort(&mut self) -> aruna_core::types::Effects {
        match self.state {
            AddGroupRoleState::GetGroup { txn_id }
            | AddGroupRoleState::GetAuthDoc { txn_id, .. }
            | AddGroupRoleState::CreateRole { txn_id, .. }
            | AddGroupRoleState::UpdateGroup { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }

            _ => smallvec![],
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::{HashMap, HashSet};

    use crate::add_group_role::{AddGroupRoleConfig, AddGroupRoleOperation, AddGroupRoleState};
    use aruna_core::consts::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::effects::Effect;
    use aruna_core::events::Event;
    use aruna_core::operation::Operation;
    use aruna_core::structs::{Actor, Group, GroupAuthorizationDocument, Permission, Role};
    use aruna_core::types::TxnId;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_add_role() {
        //
        // Inputs
        //
        let user_id = Ulid::new();
        let realm_id = aruna_core::structs::RealmId([0u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let group_id = Ulid::new();
        let auth_doc =
            GroupAuthorizationDocument::new_default_group_doc(user_id, realm_id.clone(), group_id);
        let group = Group {
            display_name: "test".to_string(),
            group_id,
            realm_id: realm_id.clone(),
            roles: auth_doc.roles.iter().map(|(r, _)| *r).collect(),
        };
        let auth_context = aruna_core::structs::AuthContext {
            user_id,
            realm_id: realm_id.clone(),
            path_restrictions: None,
        };
        let actor = Actor {
            node_id,
            user_id,
            realm_id: realm_id.clone(),
        };
        let add_role_input = AddGroupRoleConfig {
            auth_context: auth_context.clone(),
            actor: actor.clone(),
            realm_id: realm_id.clone(),
            group_id,
            role: Role {
                role_id: Ulid::new(),
                name: "test_role".to_string(),
                permissions: HashMap::from([(
                    format!(
                        "{}/g/{}/meta/{}",
                        realm_id.to_string(),
                        group_id.to_string(),
                        Ulid::new(),
                    ),
                    Permission::READ,
                )]),
                assigned_users: HashSet::from([user_id]),
            },
        };

        //
        // Steps
        //
        let mut add_role_operation = AddGroupRoleOperation::new(add_role_input.clone());
        assert_eq!(add_role_operation.state, AddGroupRoleState::Init);

        let effects = add_role_operation.start();
        let auth_effect = effects.first().unwrap();
        assert!(matches!(auth_effect, Effect::SubOperation(_)));
        assert_eq!(add_role_operation.state, AddGroupRoleState::Auth);
        let effects = add_role_operation.step(Event::SubOperation(
            aruna_core::events::SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));

        let txn_effect = effects.first().unwrap();
        assert_eq!(
            txn_effect,
            &Effect::Storage(aruna_core::effects::StorageEffect::StartTransaction { read: false })
        );
        assert_eq!(
            add_role_operation.state,
            AddGroupRoleState::StartTransaction
        );

        let txn_id = TxnId::new();
        let effects = add_role_operation.step(Event::Storage(
            aruna_core::events::StorageEvent::TransactionStarted { txn_id },
        ));
        let get_group_effect = effects.first().unwrap();
        assert_eq!(
            get_group_effect,
            &Effect::Storage(aruna_core::effects::StorageEffect::Read {
                key_space: GROUP_KEYSPACE.to_string(),
                key: group_id.to_bytes().into(),
                txn_id: Some(txn_id)
            })
        );
        assert_eq!(
            add_role_operation.state,
            AddGroupRoleState::GetGroup { txn_id }
        );

        let effects = add_role_operation.step(Event::Storage(
            aruna_core::events::StorageEvent::ReadResult {
                key: group_id.to_bytes().into(),
                value: Some(group.to_bytes(&actor).unwrap().into()),
            },
        ));
        let get_auth_doc_effect = effects.first().unwrap();
        assert_eq!(
            get_auth_doc_effect,
            &Effect::Storage(aruna_core::effects::StorageEffect::Read {
                key_space: AUTH_KEYSPACE.to_string(),
                key: group_id.to_bytes().into(),
                txn_id: Some(txn_id)
            })
        );
        assert_eq!(
            add_role_operation.state,
            AddGroupRoleState::GetAuthDoc {
                txn_id,
                group: group.clone()
            }
        );

        let effects = add_role_operation.step(Event::Storage(
            aruna_core::events::StorageEvent::ReadResult {
                key: group_id.to_bytes().into(),
                value: Some(auth_doc.to_bytes(&actor).unwrap().into()),
            },
        ));
        let create_role_effect = effects.first().unwrap();
        let mut mutated_group = group.clone();
        mutated_group.roles.insert(add_role_input.role.role_id);
        let mut mutated_auth_doc = auth_doc.clone();
        mutated_auth_doc
            .roles
            .insert(add_role_input.role.role_id, add_role_input.role.clone());
        match create_role_effect {
            Effect::Storage(aruna_core::effects::StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: effect_txn_id,
            }) => {
                assert_eq!(key_space, AUTH_KEYSPACE);
                assert_eq!(key, &group_id.to_bytes().into());
                assert_eq!(effect_txn_id, &Some(txn_id));
                let stored_auth_doc = GroupAuthorizationDocument::from_bytes(value).unwrap();
                assert_eq!(stored_auth_doc, mutated_auth_doc);
            }
            other => panic!("unexpected create role effect: {other:?}"),
        }
        assert_eq!(
            add_role_operation.state,
            AddGroupRoleState::CreateRole {
                txn_id,
                group: mutated_group.clone(),
                auth_doc: mutated_auth_doc.clone(),
            }
        );

        assert!(mutated_group.roles.contains(&add_role_input.role.role_id));
        assert_eq!(
            mutated_auth_doc.roles.get(&add_role_input.role.role_id).unwrap(),
            &add_role_input.role
        );
    }
}
