//! Searches a realm's users by name and returns the matches in pages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::USER_KEYSPACE;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::User;
use aruna_core::types::Key;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct SearchUsersInput {
    pub realm_id: RealmId,
    pub query: String,
    pub limit: usize,
    pub start_after: Option<String>,
    /// Matches only active, non-service accounts whose public name is the whole query.
    pub exact_name: bool,
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

/// Scans the realm's users page by page until one more match than the limit or the end.
#[derive(Debug, PartialEq)]
pub struct SearchUsersOperation {
    input: SearchUsersInput,
    matches: Vec<SearchUsersMatch>,
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
        got: &'static str,
    },
    #[error("search users did not finish")]
    NotFinished,
}

impl From<IoError> for SearchUsersError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "SearchUsers".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

impl SearchUsersOperation {
    pub fn new(mut input: SearchUsersInput) -> Self {
        input.limit = input.limit.max(1);
        Self {
            input,
            matches: Vec::new(),
        }
    }

    fn page(&self, after: Option<Key>) -> IoCall {
        IoCall::Page {
            space: USER_KEYSPACE.to_string(),
            prefix: UserId::storage_prefix(self.input.realm_id),
            after,
            limit: self.input.limit.saturating_add(1).max(64),
        }
    }

    fn finish(&mut self, next_start_after: Option<String>) -> Next<SearchUsersOutput> {
        Next::Done(SearchUsersOutput {
            users: std::mem::take(&mut self.matches),
            next_start_after,
        })
    }
}

impl Operation for SearchUsersOperation {
    type Output = SearchUsersOutput;
    type Error = SearchUsersError;

    fn first(&mut self) -> Result<Next<SearchUsersOutput>, SearchUsersError> {
        let after = (self.input.start_after.as_deref())
            .map(UserId::from_string)
            .transpose()?
            .map(|user_id| Key::from(user_id.to_storage_key()));
        Ok(Next::Call(self.page(after)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SearchUsersOutput>, SearchUsersError> {
        let IoResult::Page { rows, next } = result else {
            return Err(SearchUsersError::UnexpectedEvent {
                state: "SearchUsers".to_string(),
                expected: "IoResult::Page",
                got: result.label(),
            });
        };
        let query = self.input.query.to_lowercase();
        for (key, value) in rows {
            if UserId::from_storage_key(&key)?.realm_id != self.input.realm_id {
                continue;
            }
            let user = User::from_bytes(&value)?;
            let matched = match self.input.exact_name {
                true => user.public_name_is(&query),
                false => user.public_matches(&query),
            };
            if user.user_id.realm_id != self.input.realm_id || !matched {
                continue;
            }
            self.matches.push(SearchUsersMatch {
                user_id: user.user_id,
                name: user.public_name(),
            });
        }
        if self.matches.len() > self.input.limit {
            self.matches.truncate(self.input.limit);
            let next_start_after = self.matches.last().map(|user| user.user_id.to_string());
            return Ok(self.finish(next_start_after));
        }
        match next {
            Some(after) => Ok(Next::Call(self.page(Some(after)))),
            None => Ok(self.finish(None)),
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{SearchUsersInput, SearchUsersMatch, SearchUsersOperation, SearchUsersOutput};
    use aruna_core::UserId;
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::operation::Operation;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::identity::user::User;
    use aruna_core::types::{Key, Value};
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

    fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
        IoResult::Page { rows, next }
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
            exact_name: false,
        }
    }

    #[test]
    fn matches_case_insensitively() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let alice = user(realm_id, 2, "Alice");
        let bob = user(realm_id, 3, "bob");
        let mut operation = SearchUsersOperation::new(input(realm_id, "ALi", 10));

        let scan = IoCall::Page {
            space: aruna_core::USER_KEYSPACE.to_string(),
            prefix: UserId::storage_prefix(realm_id),
            after: None,
            limit: 64,
        };
        assert_eq!(operation.first(), Ok(Next::Call(scan)));

        let output = SearchUsersOutput {
            users: vec![matched(&alice)],
            next_start_after: None,
        };
        assert_eq!(
            operation.next(page(vec![entry(&alice), entry(&bob)], None)),
            Ok(Next::Done(output))
        );
    }

    #[test]
    fn matches_email_attribute() {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let mut bob = user(realm_id, 2, "bob");
        bob.attributes = HashMap::from([
            ("email".into(), "Alice@Example.org".into()),
            ("profile.visibility.email".into(), "public".into()),
        ]);
        let carol = user(realm_id, 3, "carol");
        let mut operation = SearchUsersOperation::new(input(realm_id, "example", 10));
        operation.first().unwrap();

        let output = SearchUsersOutput {
            users: vec![matched(&bob)],
            next_start_after: None,
        };
        assert_eq!(
            operation.next(page(vec![entry(&bob), entry(&carol)], None)),
            Ok(Next::Done(output))
        );
    }

    #[test]
    fn returns_match_cursor() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let alice = user(realm_id, 2, "match-alice");
        let bob = user(realm_id, 3, "match-bob");
        let mut operation = SearchUsersOperation::new(input(realm_id, "match", 1));
        operation.first().unwrap();

        let output = SearchUsersOutput {
            users: vec![matched(&alice)],
            next_start_after: Some(alice.user_id.to_string()),
        };
        assert_eq!(
            operation.next(page(vec![entry(&alice), entry(&bob)], None)),
            Ok(Next::Done(output))
        );
    }

    #[test]
    fn continues_until_limit() {
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let alice = user(realm_id, 2, "match-alice");
        let skipped = user(realm_id, 3, "other");
        let bob = user(realm_id, 4, "match-bob");
        let mut operation = SearchUsersOperation::new(input(realm_id, "match", 2));
        operation.first().unwrap();

        let continuation_key: Key = skipped.user_id.to_storage_key().into();
        let first = page(
            vec![entry(&alice), entry(&skipped)],
            Some(continuation_key.clone()),
        );
        assert!(matches!(
            operation.next(first),
            Ok(Next::Call(IoCall::Page { after: Some(key), .. })) if key == continuation_key
        ));

        let output = SearchUsersOutput {
            users: vec![matched(&alice), matched(&bob)],
            next_start_after: None,
        };
        assert_eq!(
            operation.next(page(vec![entry(&bob)], None)),
            Ok(Next::Done(output))
        );
    }
}
