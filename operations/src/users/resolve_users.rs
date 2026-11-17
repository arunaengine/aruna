//! Resolves a list of user ids into their names and attributes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::USER_KEYSPACE;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::User;
use aruna_core::types::{Key, Value};
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
    /// The primary a merged account resolves to; name and attributes are the primary's.
    pub merged_into: Option<UserId>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolveUsersOutput {
    pub users: Vec<ResolvedUser>,
}

/// Reads the users, then the primaries of merged ones, whose names and attributes they show.
#[derive(Debug, PartialEq)]
pub struct ResolveUsersOperation {
    realm_id: RealmId,
    ids: Vec<UserId>,
    /// The resolved users once read, while their primaries are read.
    users: Option<Vec<ResolvedUser>>,
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
        got: &'static str,
    },
    #[error("resolve users did not finish")]
    NotFinished,
}

impl From<IoError> for ResolveUsersError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ResolveUsers".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished | IoError::Codec(_) => {
                Self::NotFinished
            }
        }
    }
}

fn user_reads(ids: &[UserId]) -> IoCall {
    let reads = ids
        .iter()
        .map(|id| (USER_KEYSPACE.to_string(), Key::from(id.to_bytes())))
        .collect();
    IoCall::Read(reads)
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
            users: None,
        }
    }

    fn collect_users(
        &mut self,
        values: Vec<Option<Value>>,
    ) -> Result<Next<ResolveUsersOutput>, ResolveUsersError> {
        let mut users = Vec::new();
        for value in values.into_iter().flatten() {
            let user = User::from_bytes(&value)?;
            if user.user_id.realm_id != self.realm_id {
                continue;
            }
            users.push(ResolvedUser {
                user_id: user.user_id,
                name: user.public_name(),
                attributes: user.public_attributes(),
                merged_into: user.merged_into(),
            });
        }
        let mut primaries: Vec<UserId> = users.iter().filter_map(|user| user.merged_into).collect();
        primaries.sort();
        primaries.dedup();
        if primaries.is_empty() {
            return Ok(Next::Done(ResolveUsersOutput { users }));
        }
        self.users = Some(users);
        Ok(Next::Call(user_reads(&primaries)))
    }

    /// Shows each merged account as its primary, which always is the root of the merge.
    fn show_primaries(
        &mut self,
        values: Vec<Option<Value>>,
    ) -> Result<Next<ResolveUsersOutput>, ResolveUsersError> {
        let mut primaries = HashMap::new();
        for value in values.into_iter().flatten() {
            let user = User::from_bytes(&value)?;
            primaries.insert(user.user_id, user);
        }
        let mut users = self.users.take().unwrap_or_default();
        for resolved in &mut users {
            if let Some(primary) = resolved.merged_into.and_then(|id| primaries.get(&id)) {
                resolved.name = primary.public_name();
                resolved.attributes = primary.public_attributes();
            }
        }
        Ok(Next::Done(ResolveUsersOutput { users }))
    }
}

impl Operation for ResolveUsersOperation {
    type Output = ResolveUsersOutput;
    type Error = ResolveUsersError;

    /// No ids is an empty read, which the runner answers without storage.
    fn first(&mut self) -> Result<Next<ResolveUsersOutput>, ResolveUsersError> {
        Ok(Next::Call(user_reads(&self.ids)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ResolveUsersOutput>, ResolveUsersError> {
        let IoResult::Values(values) = result else {
            return Err(ResolveUsersError::UnexpectedEvent {
                state: "ResolveUsers".to_string(),
                expected: "IoResult::Values",
                got: result.label(),
            });
        };
        if self.users.is_some() {
            return self.show_primaries(values);
        }
        if values.len() != self.ids.len() {
            return Err(ResolveUsersError::UnexpectedEvent {
                state: "ResolveUsers".to_string(),
                expected: "IoResult::Values matching requested ids",
                got: "IoResult::Values",
            });
        }
        self.collect_users(values)
    }
}

#[cfg(test)]
mod pure_tests {
    use super::ResolveUsersOutput;
    use super::{ResolveUsersInput, ResolveUsersOperation, ResolvedUser};
    use aruna_core::UserId;
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::operation::Operation;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::identity::user::User;
    use aruna_core::types::Value;
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

    fn read(values: Vec<Option<Value>>) -> IoResult {
        IoResult::Values(values)
    }

    fn done(
        next: Result<Next<ResolveUsersOutput>, super::ResolveUsersError>,
    ) -> ResolveUsersOutput {
        match next {
            Ok(Next::Done(output)) => output,
            other => panic!("expected the users, got {other:?}"),
        }
    }

    fn input(realm_id: RealmId, ids: Vec<UserId>) -> ResolveUsersInput {
        ResolveUsersInput {
            realm_id,
            user_ids: ids,
        }
    }

    #[test]
    fn omits_foreign_user() {
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let foreign_id = RealmId::from_bytes([3u8; 32]);
        let local = user(realm_id, 2, "Local");
        let foreign = user(foreign_id, 3, "Foreign");
        let mut operation =
            ResolveUsersOperation::new(input(realm_id, vec![local.user_id, foreign.user_id]));

        operation.first().unwrap();
        let rows = vec![Some(value(&local)), Some(value(&foreign))];
        let output = done(operation.next(read(rows)));
        assert_eq!(
            output.users,
            vec![ResolvedUser {
                user_id: local.user_id,
                name: "Local".to_string(),
                attributes: HashMap::new(),
                merged_into: None,
            }]
        );
    }

    #[test]
    fn secondary_shows_primary() {
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let primary = user(realm_id, 7, "Primary");
        let mut secondary = user(realm_id, 8, "Secondary");
        secondary.attributes.insert(
            aruna_core::user::validation::MERGED_INTO_ATTRIBUTE.to_string(),
            primary.user_id.to_string(),
        );
        let mut operation = ResolveUsersOperation::new(input(realm_id, vec![secondary.user_id]));

        operation.first().unwrap();
        let primaries = operation.next(read(vec![Some(value(&secondary))]));
        assert!(matches!(primaries, Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 1));
        let output = done(operation.next(read(vec![Some(value(&primary))])));
        assert_eq!(output.users.len(), 1);
        assert_eq!(output.users[0].user_id, secondary.user_id);
        assert_eq!(output.users[0].name, "Primary");
        assert_eq!(output.users[0].merged_into, Some(primary.user_id));
    }

    #[test]
    fn omits_unknown_user() {
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let known = user(realm_id, 2, "Known");
        let missing = UserId::local(Ulid::from_bytes([5u8; 16]), realm_id);
        let mut operation =
            ResolveUsersOperation::new(input(realm_id, vec![known.user_id, missing]));

        operation.first().unwrap();
        let output = done(operation.next(read(vec![Some(value(&known)), None])));
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

        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Read(reads))) if reads.len() == 2
        ));
    }

    #[test]
    fn exposes_public_attributes() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let mut alice = user(realm_id, 2, "Alice");
        alice.attributes = HashMap::from([
            ("email".into(), "alice@example.org".into()),
            ("orcid".into(), "0000-0002".into()),
            ("affiliation".into(), "Lab".into()),
            ("department".into(), "Bio".into()),
            ("ui.theme".into(), "dark".into()),
            ("profile.visibility.orcid".into(), "public".into()),
            ("profile.visibility.affiliation".into(), "public".into()),
            ("profile.visibility.department".into(), "public".into()),
        ]);
        let mut operation = ResolveUsersOperation::new(input(realm_id, vec![alice.user_id]));

        operation.first().unwrap();
        let output = done(operation.next(read(vec![Some(value(&alice))])));
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
        // A result of another kind, or a read of another length, fails the lookup.
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let alice = user(realm_id, 2, "Alice");
        let mut operation = ResolveUsersOperation::new(input(realm_id, vec![alice.user_id]));

        operation.first().unwrap();
        assert!(operation.next(IoResult::Done).is_err());
        assert!(operation.next(read(Vec::new())).is_err());
    }
}
