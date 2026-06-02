use aruna_core::USER_KEYSPACE;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{Actor, AuthContext, Permission, RealmId, User};
use aruna_core::types::{Effects, TxnId, UserId};
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::replicate_documents_to_realm::replicate_documents_to_realm_effect;

const MAX_USER_NAME_LEN: usize = 256;
const MAX_USER_ATTRIBUTES: usize = 128;
const MAX_ATTRIBUTE_KEY_LEN: usize = 128;
const MAX_ATTRIBUTE_VALUE_LEN: usize = 4096;

#[derive(Clone, Debug, PartialEq)]
pub struct UpdateUserInput {
    pub actor: Actor,
    pub auth_context: AuthContext,
    pub self_realm_id: RealmId,
    pub user_id: String,
    pub name: Option<String>,
    pub set_attributes: HashMap<String, String>,
    pub remove_attributes: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub struct UpdateUserOperation {
    input: UpdateUserInput,
    target_user_id: Option<UserId>,
    state: UpdateUserState,
    output: Option<Result<User, UpdateUserError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum UpdateUserState {
    Init,
    Auth,
    StartTransaction,
    ReadUser { txn_id: TxnId },
    WriteUser { txn_id: TxnId, user: User },
    CommitTransaction { txn_id: TxnId, user: User },
    AnnounceUser { user: User },
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum UpdateUserError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error("User not found")]
    UserNotFound,
    #[error("stored user id does not match requested user id")]
    UserIdMismatch,
    #[error("user name must be non-empty and at most {MAX_USER_NAME_LEN} bytes")]
    InvalidUserName,
    #[error("invalid user attribute key: {0}")]
    InvalidAttributeKey(String),
    #[error("invalid user attribute value for key: {0}")]
    InvalidAttributeValue(String),
    #[error("too many user attributes")]
    TooManyAttributes,
    #[error(transparent)]
    AuthorizationError(#[from] AuthorizationError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("topic announcement failed: {0}")]
    TopicAnnouncement(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("update user did not finish")]
    NotFinished,
}

impl UpdateUserOperation {
    pub fn new(input: UpdateUserInput) -> Self {
        Self {
            input,
            target_user_id: None,
            state: UpdateUserState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: UpdateUserError) -> Effects {
        let cleanup = self.abort();
        self.state = UpdateUserState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }
        Ok(event)
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(UpdateUserError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn start_auth(&mut self) -> Result<Effects, UpdateUserError> {
        let target_user_id = UserId::from_string(&self.input.user_id)?;
        if target_user_id.realm_id != self.input.self_realm_id
            || self.input.auth_context.realm_id != self.input.self_realm_id
            || self.input.actor.realm_id != self.input.self_realm_id
            || self.input.actor.user_id != self.input.auth_context.user_id
        {
            return Err(UpdateUserError::Unauthorized);
        }
        self.target_user_id = Some(target_user_id);

        if self.input.auth_context.user_id == target_user_id {
            if self.input.auth_context.path_restrictions.is_some() {
                return Err(UpdateUserError::Unauthorized);
            }
            self.state = UpdateUserState::StartTransaction;
            return Ok(smallvec![Effect::Storage(
                StorageEffect::StartTransaction { read: false }
            )]);
        }

        self.state = UpdateUserState::Auth;
        Ok(smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.input.auth_context.clone(),
                path: format!("/{}/admin/u/{}", self.input.self_realm_id, target_user_id),
                required_permission: Permission::WRITE,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))])
    }

    fn handle_auth_result(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected_event(
                "Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed })",
                got,
            );
        };

        match allowed {
            Ok(true) => {
                self.state = UpdateUserState::StartTransaction;
                smallvec![Effect::Storage(StorageEffect::StartTransaction {
                    read: false,
                })]
            }
            Ok(false) => self.fail(UpdateUserError::Unauthorized),
            Err(error) => self.fail(error.into()),
        }
    }

    fn handle_start_transaction(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::TransactionStarted { txn_id })",
                got,
            );
        };
        let Some(target_user_id) = self.target_user_id else {
            return self.fail(UpdateUserError::UserNotFound);
        };
        self.state = UpdateUserState::ReadUser { txn_id };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_KEYSPACE.to_string(),
            key: ByteView::from(target_user_id.to_bytes()),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_read_user(&mut self, event: Event, txn_id: TxnId) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::ReadResult)", got);
        };

        match self.emit_write_user(txn_id, value) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn emit_write_user(
        &mut self,
        txn_id: TxnId,
        value: Option<ByteView>,
    ) -> Result<Effects, UpdateUserError> {
        let current = value.ok_or(UpdateUserError::UserNotFound)?;
        let mut user = User::from_bytes(&current)?;
        if Some(user.user_id) != self.target_user_id {
            return Err(UpdateUserError::UserIdMismatch);
        }

        apply_updates(&mut user, &self.input)?;
        let bytes = user.reconcile_bytes(Some(&current), &self.input.actor)?;
        self.state = UpdateUserState::WriteUser {
            txn_id,
            user: user.clone(),
        };
        Ok(smallvec![Effect::Storage(StorageEffect::Write {
            key_space: USER_KEYSPACE.to_string(),
            key: ByteView::from(user.user_id.to_bytes()),
            value: ByteView::from(bytes),
            txn_id: Some(txn_id),
        })])
    }

    fn handle_write_user(&mut self, event: Event, txn_id: TxnId, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::WriteResult)", got);
        };
        self.state = UpdateUserState::CommitTransaction { txn_id, user };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_transaction(&mut self, event: Event, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::TransactionCommitted { .. })",
                got,
            );
        };
        let user_id = user.user_id;
        self.state = UpdateUserState::AnnounceUser { user };
        let document = DocumentSyncTarget::User { user_id };
        smallvec![replicate_documents_to_realm_effect(
            self.input.actor.realm_id,
            self.input.actor.node_id,
            vec![document],
        )]
    }

    fn handle_announce_user(&mut self, event: Event, user: User) -> Effects {
        let got = format!("{event:?}");
        let Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }) = event else {
            return self.unexpected_event(
                "Event::SubOperation(SubOperationEvent::DocumentSyncResult { result })",
                got,
            );
        };

        match result {
            Ok(()) => {
                self.state = UpdateUserState::Finish;
                self.output = Some(Ok(user));
                smallvec![]
            }
            Err(error) => self.fail(UpdateUserError::TopicAnnouncement(error)),
        }
    }
}

impl Operation for UpdateUserOperation {
    type Output = User;
    type Error = UpdateUserError;

    fn start(&mut self) -> Effects {
        match self.start_auth() {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state.clone() {
            UpdateUserState::Auth => self.handle_auth_result(event),
            UpdateUserState::StartTransaction => self.handle_start_transaction(event),
            UpdateUserState::ReadUser { txn_id } => self.handle_read_user(event, txn_id),
            UpdateUserState::WriteUser { txn_id, user } => {
                self.handle_write_user(event, txn_id, user)
            }
            UpdateUserState::CommitTransaction { user, .. } => {
                self.handle_commit_transaction(event, user)
            }
            UpdateUserState::AnnounceUser { user } => self.handle_announce_user(event, user),
            UpdateUserState::Init | UpdateUserState::Finish | UpdateUserState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, UpdateUserState::Finish | UpdateUserState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(UpdateUserError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            UpdateUserState::ReadUser { txn_id }
            | UpdateUserState::WriteUser { txn_id, .. }
            | UpdateUserState::CommitTransaction { txn_id, .. } => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

fn apply_updates(user: &mut User, input: &UpdateUserInput) -> Result<(), UpdateUserError> {
    if let Some(name) = input.name.as_ref() {
        let trimmed = name.trim();
        if trimmed.is_empty() || trimmed.len() > MAX_USER_NAME_LEN {
            return Err(UpdateUserError::InvalidUserName);
        }
        user.name = trimmed.to_string();
    }

    let mut removals = HashSet::new();
    for key in &input.remove_attributes {
        validate_attribute_key(key)?;
        removals.insert(key.clone());
    }
    for key in removals {
        user.attributes.remove(&key);
    }

    for (key, value) in &input.set_attributes {
        validate_attribute_key(key)?;
        validate_attribute_value(key, value)?;
        user.attributes.insert(key.clone(), value.clone());
    }

    if user.attributes.len() > MAX_USER_ATTRIBUTES {
        return Err(UpdateUserError::TooManyAttributes);
    }

    Ok(())
}

fn validate_attribute_key(key: &str) -> Result<(), UpdateUserError> {
    if key.is_empty()
        || key.len() > MAX_ATTRIBUTE_KEY_LEN
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
    {
        return Err(UpdateUserError::InvalidAttributeKey(key.to_string()));
    }
    Ok(())
}

fn validate_attribute_value(key: &str, value: &str) -> Result<(), UpdateUserError> {
    if value.len() > MAX_ATTRIBUTE_VALUE_LEN || value.chars().any(char::is_control) {
        return Err(UpdateUserError::InvalidAttributeValue(key.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{UpdateUserInput, UpdateUserOperation};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{Actor, AuthContext, RealmId, User};
    use aruna_core::types::{TxnId, UserId};
    use std::collections::HashMap;
    use ulid::Ulid;

    fn actor(realm_id: RealmId, user_id: UserId) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            user_id,
            realm_id,
        }
    }

    fn auth_context(realm_id: RealmId, user_id: UserId) -> AuthContext {
        AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        }
    }

    fn input(realm_id: RealmId, caller_id: UserId, user_id: UserId) -> UpdateUserInput {
        UpdateUserInput {
            actor: actor(realm_id, caller_id),
            auth_context: auth_context(realm_id, caller_id),
            self_realm_id: realm_id,
            user_id: user_id.to_string(),
            name: Some("Alice Updated".to_string()),
            set_attributes: HashMap::from([
                ("orcid".to_string(), "0000-0002-1825-0097".to_string()),
                ("department".to_string(), "biology".to_string()),
            ]),
            remove_attributes: vec!["old".to_string()],
        }
    }

    fn stored_user(user_id: UserId) -> User {
        User {
            user_id,
            name: "Alice".to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: HashMap::from([
                ("old".to_string(), "remove-me".to_string()),
                ("department".to_string(), "physics".to_string()),
            ]),
        }
    }

    #[test]
    fn updates_user_attributes_and_announces() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([3u8; 16]), realm_id);
        let original = stored_user(user_id);
        let mut operation = UpdateUserOperation::new(input(realm_id, user_id, user_id));

        assert!(matches!(
            operation.start().first(),
            Some(Effect::Storage(StorageEffect::StartTransaction {
                read: false
            }))
        ));

        let txn_id = TxnId::new();
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::Read { .. }))
        ));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: user_id.to_bytes().into(),
            value: Some(original.to_bytes(&actor(realm_id, user_id)).unwrap().into()),
        }));
        let updated = match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Write { value, .. }) => User::from_bytes(value).unwrap(),
            other => panic!("unexpected update effect: {other:?}"),
        };
        assert_eq!(updated.name, "Alice Updated");
        assert_eq!(
            updated.attributes.get("orcid").map(String::as_str),
            Some("0000-0002-1825-0097")
        );
        assert_eq!(
            updated.attributes.get("department").map(String::as_str),
            Some("biology")
        );
        assert!(!updated.attributes.contains_key("old"));

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: user_id.to_bytes().into(),
        }));
        assert!(matches!(
            effects.first(),
            Some(Effect::Storage(StorageEffect::CommitTransaction { .. }))
        ));

        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(matches!(effects.first(), Some(Effect::SubOperation(_))));

        let effects = operation.step(Event::SubOperation(SubOperationEvent::DocumentSyncResult {
            result: Ok(()),
        }));
        assert!(effects.is_empty());
        assert_eq!(operation.finalize().unwrap(), updated);
    }

    #[test]
    fn unauthorized_update_fails() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let caller_id = UserId::local(Ulid::from_bytes([4u8; 16]), realm_id);
        let user_id = UserId::local(Ulid::from_bytes([5u8; 16]), realm_id);
        let mut operation = UpdateUserOperation::new(input(realm_id, caller_id, user_id));
        operation.start();

        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
        ));

        assert!(effects.is_empty());
        assert!(operation.finalize().is_err());
    }

    #[test]
    fn scoped_self_update_fails() {
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let user_id = UserId::local(Ulid::from_bytes([7u8; 16]), realm_id);
        let mut input = input(realm_id, user_id, user_id);
        input.auth_context.path_restrictions = Some(Vec::new());
        let mut operation = UpdateUserOperation::new(input);

        let effects = operation.start();

        assert!(effects.is_empty());
        assert!(operation.finalize().is_err());
    }
}
