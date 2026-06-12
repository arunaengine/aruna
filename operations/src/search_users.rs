use aruna_core::USER_KEYSPACE;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{RealmId, User};
use aruna_core::types::{Effects, Key, UserId, Value};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct SearchUsersInput {
    pub realm_id: RealmId,
    pub query: String,
    pub limit: usize,
    pub start_after: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchUsersMatch {
    pub user_id: UserId,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchUsersOutput {
    pub users: Vec<SearchUsersMatch>,
    pub next_start_after: Option<String>,
}

#[derive(Debug, PartialEq)]
pub struct SearchUsersOperation {
    input: SearchUsersInput,
    state: SearchUsersState,
    matches: Vec<SearchUsersMatch>,
    next_storage_start_after: Option<Key>,
    output: Option<Result<SearchUsersOutput, SearchUsersError>>,
}

#[derive(Debug, Clone, PartialEq)]
enum SearchUsersState {
    Init,
    SearchUsers,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum SearchUsersError {
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
    #[error("search users did not finish")]
    NotFinished,
}

impl SearchUsersOperation {
    pub fn new(mut input: SearchUsersInput) -> Self {
        input.limit = input.limit.max(1);
        Self {
            input,
            state: SearchUsersState::Init,
            matches: Vec::new(),
            next_storage_start_after: None,
            output: None,
        }
    }

    fn fail(&mut self, error: SearchUsersError) -> Effects {
        self.state = SearchUsersState::Error;
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
        self.fail(SearchUsersError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got,
        })
    }

    fn start_after_key(&self) -> Result<Option<Key>, SearchUsersError> {
        match (&self.next_storage_start_after, &self.input.start_after) {
            (Some(key), _) => Ok(Some(key.clone())),
            (None, Some(user_id)) => {
                Ok(Some(UserId::from_string(user_id)?.to_storage_key().into()))
            }
            (None, None) => Ok(None),
        }
    }

    fn emit_search_users(&mut self) -> Result<Effects, SearchUsersError> {
        self.state = SearchUsersState::SearchUsers;
        Ok(smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: USER_KEYSPACE.to_string(),
            prefix: Some(UserId::storage_prefix(self.input.realm_id)),
            start: self.start_after_key()?.map(IterStart::After),
            limit: self.input.limit.saturating_add(1).max(64),
            txn_id: None,
        })])
    }

    fn handle_search_users(&mut self, event: Event) -> Effects {
        let got = format!("{event:?}");
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.unexpected_event("Event::Storage(StorageEvent::IterResult)", got);
        };

        match self.collect_matches(values, next_start_after) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn matches_query(user: &User, query: &str) -> bool {
        user.name.to_lowercase().contains(query)
            || user
                .attributes
                .get("email")
                .is_some_and(|email| email.to_lowercase().contains(query))
    }

    fn collect_matches(
        &mut self,
        values: Vec<(Key, Value)>,
        next_start_after: Option<Key>,
    ) -> Result<Effects, SearchUsersError> {
        let query = self.input.query.to_lowercase();
        for (key, value) in values {
            let key_user_id = UserId::from_storage_key(&key)?;
            if key_user_id.realm_id != self.input.realm_id {
                continue;
            }

            let user = User::from_bytes(&value)?;
            if user.user_id.realm_id != self.input.realm_id || !Self::matches_query(&user, &query) {
                continue;
            }

            self.matches.push(SearchUsersMatch {
                user_id: user.user_id,
                name: user.name,
            });
        }

        if self.matches.len() > self.input.limit {
            self.matches.truncate(self.input.limit);
            let next_start_after = self.matches.last().map(|user| user.user_id.to_string());
            return Ok(self.emit_finish(next_start_after));
        }

        if let Some(next_start_after) = next_start_after {
            self.next_storage_start_after = Some(next_start_after);
            return self.emit_search_users();
        }

        Ok(self.emit_finish(None))
    }

    fn emit_finish(&mut self, next_start_after: Option<String>) -> Effects {
        self.state = SearchUsersState::Finish;
        self.output = Some(Ok(SearchUsersOutput {
            users: std::mem::take(&mut self.matches),
            next_start_after,
        }));
        smallvec![]
    }
}

impl Operation for SearchUsersOperation {
    type Output = SearchUsersOutput;
    type Error = SearchUsersError;

    fn start(&mut self) -> Effects {
        match self.emit_search_users() {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match self.fail_on_storage_error(event) {
            Ok(event) => event,
            Err(effects) => return effects,
        };

        match self.state {
            SearchUsersState::SearchUsers => self.handle_search_users(event),
            SearchUsersState::Init | SearchUsersState::Finish | SearchUsersState::Error => {
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            SearchUsersState::Finish | SearchUsersState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(SearchUsersError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{SearchUsersInput, SearchUsersMatch, SearchUsersOperation, SearchUsersOutput};
    use aruna_core::effects::{Effect, IterStart, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{Actor, RealmId, User};
    use aruna_core::types::{Key, UserId, Value};
    use std::collections::HashMap;
    use ulid::Ulid;

    fn actor(realm_id: RealmId, user_id: UserId) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            user_id,
            realm_id,
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

    fn entry(user: &User) -> (Key, Value) {
        (
            user.user_id.to_storage_key().into(),
            user.to_bytes(&actor(user.user_id.realm_id, user.user_id))
                .unwrap()
                .into(),
        )
    }

    fn matched(user: &User) -> SearchUsersMatch {
        SearchUsersMatch {
            user_id: user.user_id,
            name: user.name.clone(),
        }
    }

    fn input(realm_id: RealmId, query: &str, limit: usize) -> SearchUsersInput {
        SearchUsersInput {
            realm_id,
            query: query.to_string(),
            limit,
            start_after: None,
        }
    }

    #[test]
    fn matches_name_case_insensitively() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let alice = user(realm_id, 2, "Alice");
        let bob = user(realm_id, 3, "bob");
        let mut operation = SearchUsersOperation::new(input(realm_id, "ALi", 10));

        let effects = operation.start();
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Iter {
                key_space,
                prefix,
                start,
                limit,
                txn_id,
            }) => {
                assert_eq!(key_space, aruna_core::USER_KEYSPACE);
                assert_eq!(prefix.as_ref(), Some(&UserId::storage_prefix(realm_id)));
                assert_eq!(start, &None);
                assert_eq!(*limit, 64);
                assert_eq!(txn_id, &None);
            }
            other => panic!("unexpected effect: {other:?}"),
        }

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![entry(&alice), entry(&bob)],
            next_start_after: None,
        }));

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize().unwrap(),
            SearchUsersOutput {
                users: vec![matched(&alice)],
                next_start_after: None,
            }
        );
    }

    #[test]
    fn matches_email_attribute() {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let mut bob = user(realm_id, 2, "bob");
        bob.attributes = HashMap::from([("email".into(), "Alice@Example.org".into())]);
        let carol = user(realm_id, 3, "carol");
        let mut operation = SearchUsersOperation::new(input(realm_id, "example", 10));
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![entry(&bob), entry(&carol)],
            next_start_after: None,
        }));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize().unwrap(),
            SearchUsersOutput {
                users: vec![matched(&bob)],
                next_start_after: None,
            }
        );
    }

    #[test]
    fn returns_cursor_when_more_matches_than_limit() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let alice = user(realm_id, 2, "match-alice");
        let bob = user(realm_id, 3, "match-bob");
        let mut operation = SearchUsersOperation::new(input(realm_id, "match", 1));
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![entry(&alice), entry(&bob)],
            next_start_after: None,
        }));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize().unwrap(),
            SearchUsersOutput {
                users: vec![matched(&alice)],
                next_start_after: Some(alice.user_id.to_string()),
            }
        );
    }

    #[test]
    fn continues_iteration_until_enough_matches() {
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let alice = user(realm_id, 2, "match-alice");
        let skipped = user(realm_id, 3, "other");
        let bob = user(realm_id, 4, "match-bob");
        let mut operation = SearchUsersOperation::new(input(realm_id, "match", 2));
        operation.start();

        let continuation_key: Key = skipped.user_id.to_storage_key().into();
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![entry(&alice), entry(&skipped)],
            next_start_after: Some(continuation_key.clone()),
        }));
        match effects.first().unwrap() {
            Effect::Storage(StorageEffect::Iter {
                start: Some(IterStart::After(key)),
                ..
            }) => {
                assert_eq!(key, &continuation_key);
            }
            other => panic!("unexpected effect: {other:?}"),
        }
        assert!(!operation.is_complete());

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![entry(&bob)],
            next_start_after: None,
        }));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize().unwrap(),
            SearchUsersOutput {
                users: vec![matched(&alice), matched(&bob)],
                next_start_after: None,
            }
        );
    }
}
