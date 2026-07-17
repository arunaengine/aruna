use aruna_core::USER_KEYSPACE;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{RealmId, User};
use aruna_core::types::{Effects, Key, UserId, Value};
use aruna_core::user_update_validation::SAFE_USER_ATTRIBUTE_KEYS;
use smallvec::smallvec;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct ResolveUsersInput {
    pub realm_id: RealmId,
    pub user_ids: Vec<UserId>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedUser {
    pub user_id: UserId,
    pub name: String,
    pub attributes: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolveUsersOutput {
    pub users: Vec<ResolvedUser>,
}

#[derive(Debug, PartialEq)]
pub struct ResolveUsersOperation {
    realm_id: RealmId,
    ids: Vec<UserId>,
    state: ResolveUsersState,
    output: Option<Result<ResolveUsersOutput, ResolveUsersError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum ResolveUsersState {
    Init,
    ResolveUsers,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ResolveUsersError {
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
    #[error("resolve users did not finish")]
    NotFinished,
}

fn safe_attributes(attributes: HashMap<String, String>) -> HashMap<String, String> {
    attributes
        .into_iter()
        .filter(|(key, _)| SAFE_USER_ATTRIBUTE_KEYS.contains(&key.as_str()))
        .collect()
}

impl ResolveUsersOperation {
    pub fn new(input: ResolveUsersInput) -> Self {
        let mut seen = HashSet::new();
        let ids = input
            .user_ids
            .into_iter()
            .filter(|id| seen.insert(*id))
            .collect();
        Self {
            realm_id: input.realm_id,
            ids,
            state: ResolveUsersState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ResolveUsersError) -> Effects {
        self.state = ResolveUsersState::Error;
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
        self.fail(ResolveUsersError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn emit_resolve(&mut self) -> Effects {
        if self.ids.is_empty() {
            return self.emit_finish(Vec::new());
        }
        self.state = ResolveUsersState::ResolveUsers;
        let reads = self
            .ids
            .iter()
            .map(|id| (USER_KEYSPACE.to_string(), Key::from(id.to_bytes())))
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn handle_resolve(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected_event("Event::Storage(StorageEvent::BatchReadResult)", got);
        };
        if values.len() != self.ids.len() {
            return self.unexpected_event(
                "Event::Storage(StorageEvent::BatchReadResult) matching requested ids",
                got,
            );
        }

        match self.collect_users(values) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn collect_users(
        &mut self,
        values: Vec<(Key, Option<Value>)>,
    ) -> Result<Effects, ResolveUsersError> {
        let mut users = Vec::new();
        for (_, value) in values {
            let Some(value) = value else {
                continue;
            };
            let user = User::from_bytes(&value)?;
            if user.user_id.realm_id != self.realm_id {
                continue;
            }
            users.push(ResolvedUser {
                user_id: user.user_id,
                name: user.name,
                attributes: safe_attributes(user.attributes),
            });
        }
        Ok(self.emit_finish(users))
    }

    fn emit_finish(&mut self, users: Vec<ResolvedUser>) -> Effects {
        self.state = ResolveUsersState::Finish;
        self.output = Some(Ok(ResolveUsersOutput { users }));
        smallvec![]
    }
}

impl Operation for ResolveUsersOperation {
    type Output = ResolveUsersOutput;
    type Error = ResolveUsersError;

    fn start(&mut self) -> Effects {
        self.emit_resolve()
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            ResolveUsersState::ResolveUsers => self.handle_resolve(event),
            ResolveUsersState::Init | ResolveUsersState::Finish | ResolveUsersState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ResolveUsersState::Finish | ResolveUsersState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(ResolveUsersError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{ResolveUsersInput, ResolveUsersOperation, ResolvedUser};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{Actor, RealmId, User};
    use aruna_core::types::{Key, UserId, Value};
    use std::collections::HashMap;
    use ulid::Ulid;

    fn actor(user_id: UserId) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            user_id,
            realm_id: user_id.realm_id,
        }
    }

    fn user(realm_id: RealmId, seed: u8, name: &str) -> User {
        User {
            user_id: UserId::local(Ulid::from_bytes([seed; 16]), realm_id),
            name: name.to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: Default::default(),
        }
    }

    fn value(user: &User) -> Value {
        user.to_bytes(&actor(user.user_id)).unwrap().into()
    }

    fn input(realm_id: RealmId, ids: Vec<UserId>) -> ResolveUsersInput {
        ResolveUsersInput {
            realm_id,
            user_ids: ids,
        }
    }

    #[test]
    fn omits_foreign_realm_user() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let foreign_id = RealmId::from_bytes([3u8; 32]);
        let local = user(realm_id, 2, "Local");
        let foreign = user(foreign_id, 3, "Foreign");
        let mut operation =
            ResolveUsersOperation::new(input(realm_id, vec![local.user_id, foreign.user_id]));

        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (Key::from(local.user_id.to_bytes()), Some(value(&local))),
                (Key::from(foreign.user_id.to_bytes()), Some(value(&foreign))),
            ],
        }));

        assert!(effects.is_empty());
        let output = operation.finalize().unwrap();
        assert_eq!(
            output.users,
            vec![ResolvedUser {
                user_id: local.user_id,
                name: "Local".to_string(),
                attributes: HashMap::new(),
            }]
        );
    }

    #[test]
    fn omits_unknown_user() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let known = user(realm_id, 2, "Known");
        let missing = UserId::local(Ulid::from_bytes([5u8; 16]), realm_id);
        let mut operation =
            ResolveUsersOperation::new(input(realm_id, vec![known.user_id, missing]));

        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (Key::from(known.user_id.to_bytes()), Some(value(&known))),
                (Key::from(missing.to_bytes()), None),
            ],
        }));

        assert!(effects.is_empty());
        let output = operation.finalize().unwrap();
        assert_eq!(output.users.len(), 1);
        assert_eq!(output.users[0].user_id, known.user_id);
    }

    #[test]
    fn dedups_repeated_ids() {
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let alice = user(realm_id, 2, "Alice");
        let bob = user(realm_id, 3, "Bob");
        let mut operation = ResolveUsersOperation::new(input(
            realm_id,
            vec![alice.user_id, alice.user_id, bob.user_id],
        ));

        let effects = operation.start();
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::BatchRead { reads, txn_id }) => {
                assert_eq!(reads.len(), 2);
                assert_eq!(txn_id, &None);
            }
            other => panic!("unexpected effect: {other:?}"),
        }
    }

    #[test]
    fn exposes_only_safe_attributes() {
        // email and unlisted keys must never reach the output.
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let mut alice = user(realm_id, 2, "Alice");
        alice.attributes = HashMap::from([
            ("email".into(), "alice@example.org".into()),
            ("orcid".into(), "0000-0002".into()),
            ("affiliation".into(), "Lab".into()),
            ("department".into(), "Bio".into()),
            ("ui.theme".into(), "dark".into()),
        ]);
        let mut operation = ResolveUsersOperation::new(input(realm_id, vec![alice.user_id]));

        operation.start();
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(Key::from(alice.user_id.to_bytes()), Some(value(&alice)))],
        }));

        let output = operation.finalize().unwrap();
        assert_eq!(
            output.users[0].attributes,
            HashMap::from([
                ("orcid".to_string(), "0000-0002".to_string()),
                ("affiliation".to_string(), "Lab".to_string()),
                ("department".to_string(), "Bio".to_string()),
            ])
        );
    }

    #[test]
    fn rejects_unexpected_event() {
        // The sans-I/O contract fails on an event invalid for the state.
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let alice = user(realm_id, 2, "Alice");
        let mut operation = ResolveUsersOperation::new(input(realm_id, vec![alice.user_id]));

        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: Key::from(alice.user_id.to_bytes()),
        }));

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(operation.finalize().is_err());
    }
}
