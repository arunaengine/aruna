use aruna_core::USER_KEYSPACE;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{AuthContext, Permission, RealmId, User};
use aruna_core::types::{Effects, Key, UserId, Value};
use smallvec::smallvec;
use thiserror::Error;

use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};

#[derive(Clone, Debug, PartialEq)]
pub struct ListUsersInput {
    pub auth_context: AuthContext,
    pub self_realm_id: RealmId,
    pub limit: usize,
    pub start_after: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListUsersOutput {
    pub users: Vec<User>,
    pub next_start_after: Option<String>,
}

#[derive(Debug, PartialEq)]
pub struct ListUsersOperation {
    input: ListUsersInput,
    state: ListUsersState,
    users: Vec<User>,
    next_storage_start_after: Option<Key>,
    output: Option<Result<ListUsersOutput, ListUsersError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ListUsersState {
    Init,
    Auth,
    ListUsers,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListUsersError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error(transparent)]
    AuthorizationError(#[from] AuthorizationError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
    #[error("list users did not finish")]
    NotFinished,
}

impl ListUsersOperation {
    pub fn new(mut input: ListUsersInput) -> Self {
        input.limit = input.limit.max(1);
        Self {
            input,
            state: ListUsersState::Init,
            users: Vec::new(),
            next_storage_start_after: None,
            output: None,
        }
    }

    fn fail(&mut self, error: ListUsersError) -> Effects {
        self.state = ListUsersState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn fail_on_storage_error(&mut self, event: Event) -> Result<Event, Effects> {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return Err(self.fail(error.into()));
        }
        Ok(event)
    }

    fn unexpected_event(&mut self, expected: &'static str, got: String) -> Effects {
        self.fail(ListUsersError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn start_after_key(&self) -> Result<Option<Key>, ListUsersError> {
        match (&self.next_storage_start_after, &self.input.start_after) {
            (Some(key), _) => Ok(Some(key.clone())),
            (None, Some(user_id)) => {
                Ok(Some(UserId::from_string(user_id)?.to_storage_key().into()))
            }
            (None, None) => Ok(None),
        }
    }

    fn emit_list_users(&mut self) -> Result<Effects, ListUsersError> {
        self.state = ListUsersState::ListUsers;
        Ok(smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: USER_KEYSPACE.to_string(),
            prefix: None,
            start_after: self.start_after_key()?,
            limit: self.input.limit.saturating_add(1),
            txn_id: None,
        })])
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
            Ok(true) => match self.emit_list_users() {
                Ok(effects) => effects,
                Err(error) => self.fail(error),
            },
            Ok(false) => self.fail(ListUsersError::Unauthorized),
            Err(error) => self.fail(error.into()),
        }
    }

    fn handle_list_users(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.unexpected_event("Event::Storage(StorageEvent::IterResult)", got);
        };

        match self.collect_users(values, next_start_after) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn collect_users(
        &mut self,
        values: Vec<(Key, Value)>,
        next_start_after: Option<Key>,
    ) -> Result<Effects, ListUsersError> {
        for (key, value) in values {
            let key_user_id = UserId::from_storage_key(&key)?;
            if key_user_id.realm_id != self.input.self_realm_id {
                continue;
            }

            let user = User::from_bytes(&value)?;
            if user.user_id.realm_id == self.input.self_realm_id {
                self.users.push(user);
            }
        }

        if self.users.len() > self.input.limit {
            self.users.truncate(self.input.limit);
            let next_start_after = self.users.last().map(|user| user.user_id.to_string());
            return Ok(self.emit_finish(next_start_after));
        }

        if let Some(next_start_after) = next_start_after {
            self.next_storage_start_after = Some(next_start_after);
            return self.emit_list_users();
        }

        Ok(self.emit_finish(None))
    }

    fn emit_finish(&mut self, next_start_after: Option<String>) -> Effects {
        self.state = ListUsersState::Finish;
        self.output = Some(Ok(ListUsersOutput {
            users: std::mem::take(&mut self.users),
            next_start_after,
        }));
        smallvec![]
    }
}

impl Operation for ListUsersOperation {
    type Output = ListUsersOutput;
    type Error = ListUsersError;

    fn start(&mut self) -> Effects {
        self.state = ListUsersState::Auth;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.input.auth_context.clone(),
                path: format!("/{}/admin/u/**", self.input.self_realm_id),
                required_permission: Permission::READ,
            }),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            ListUsersState::Auth => self.handle_auth_result(event),
            ListUsersState::ListUsers => self.handle_list_users(event),
            ListUsersState::Init | ListUsersState::Finish | ListUsersState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListUsersState::Finish | ListUsersState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ListUsersError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{ListUsersInput, ListUsersOperation, ListUsersOutput};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{Actor, AuthContext, RealmId, User};
    use aruna_core::types::UserId;
    use ulid::Ulid;

    fn actor(realm_id: RealmId, user_id: UserId) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
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

    fn user(realm_id: RealmId, seed: u8, name: &str) -> User {
        User {
            user_id: UserId::local(Ulid::from_bytes([seed; 16]), realm_id),
            name: name.to_string(),
            subject_ids: Vec::new(),
            attributes: Default::default(),
        }
    }

    fn input(realm_id: RealmId, limit: usize, start_after: Option<String>) -> ListUsersInput {
        let caller = UserId::local(Ulid::from_bytes([1u8; 16]), realm_id);
        ListUsersInput {
            auth_context: auth_context(realm_id, caller),
            self_realm_id: realm_id,
            limit,
            start_after,
        }
    }

    fn authorize(operation: &mut ListUsersOperation) -> aruna_core::types::Effects {
        operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ))
    }

    #[test]
    fn authorized_user_list_emits_iter() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let mut operation = ListUsersOperation::new(input(realm_id, 10, None));

        assert!(matches!(
            operation.start().first(),
            Some(Effect::SubOperation(_))
        ));
        let effects = authorize(&mut operation);
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Iter {
                key_space,
                prefix,
                start_after,
                limit,
                txn_id,
            }) => {
                assert_eq!(key_space, aruna_core::USER_KEYSPACE);
                assert_eq!(prefix, &None);
                assert_eq!(start_after, &None);
                assert_eq!(*limit, 11);
                assert_eq!(txn_id, &None);
            }
            other => panic!("unexpected effect: {other:?}"),
        }
    }

    #[test]
    fn filters_users_by_realm_and_returns_cursor() {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let foreign_realm_id = RealmId::from_bytes([4u8; 32]);
        let alice = user(realm_id, 2, "alice");
        let bob = user(realm_id, 3, "bob");
        let carol = user(realm_id, 4, "carol");
        let foreign = user(foreign_realm_id, 5, "foreign");
        let mut operation = ListUsersOperation::new(input(realm_id, 2, None));
        operation.start();
        authorize(&mut operation);

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                (
                    alice.user_id.to_storage_key().into(),
                    alice
                        .to_bytes(&actor(realm_id, alice.user_id))
                        .unwrap()
                        .into(),
                ),
                (
                    foreign.user_id.to_storage_key().into(),
                    foreign
                        .to_bytes(&actor(foreign_realm_id, foreign.user_id))
                        .unwrap()
                        .into(),
                ),
                (
                    bob.user_id.to_storage_key().into(),
                    bob.to_bytes(&actor(realm_id, bob.user_id)).unwrap().into(),
                ),
                (
                    carol.user_id.to_storage_key().into(),
                    carol
                        .to_bytes(&actor(realm_id, carol.user_id))
                        .unwrap()
                        .into(),
                ),
            ],
            next_start_after: None,
        }));

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize().unwrap(),
            ListUsersOutput {
                users: vec![alice.clone(), bob.clone()],
                next_start_after: Some(bob.user_id.to_string()),
            }
        );
    }

    #[test]
    fn uses_start_after_cursor() {
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let start_after = user(realm_id, 6, "cursor").user_id;
        let mut operation =
            ListUsersOperation::new(input(realm_id, 10, Some(start_after.to_string())));
        operation.start();

        let effects = authorize(&mut operation);
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Iter {
                start_after: Some(key),
                ..
            }) => {
                assert_eq!(key.as_ref(), start_after.to_storage_key().as_slice());
            }
            other => panic!("unexpected effect: {other:?}"),
        }
    }

    #[test]
    fn unauthorized_user_list_fails() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let mut operation = ListUsersOperation::new(input(realm_id, 10, None));
        operation.start();

        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
        ));

        assert!(effects.is_empty());
        assert!(operation.finalize().is_err());
    }
}
