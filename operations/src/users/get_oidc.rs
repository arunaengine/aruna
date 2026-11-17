//! Looks up the user behind an OIDC issuer and subject through the subject index.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::values;
use aruna_core::structs::identity::auth::oidc_subject_key;
use aruna_core::structs::identity::user::User;
use aruna_core::{SUBJECT_INDEX_KEYSPACE, USER_KEYSPACE};
use byteview::ByteView;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct GetOidcInput {
    pub issuer: String,
    pub subject_id: String,
}

/// What the lookup reads next.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Stage {
    Subject,
    User,
    /// Follows a merged account's pointers to the root of its merges.
    Merge,
}

/// Reads the subject index and the user in one snapshot, following merges to their root.
#[derive(Debug, PartialEq)]
pub struct GetOidcOperation {
    input: GetOidcInput,
    stage: Stage,
    start: Option<User>,
    current: Option<User>,
    seen: Vec<UserId>,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetOidcError {
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
        got: &'static str,
    },
    #[error("registration did not finish")]
    NotFinished,
    #[error("User not found")]
    UserNotFound,
}

impl From<IoError> for GetOidcError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "GetOidc".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

type OidcTurn = Result<Next<User>, GetOidcError>;

fn user_read(user_id: UserId) -> IoCall {
    let key = ByteView::from(user_id.to_storage_key());
    IoCall::Read(vec![(USER_KEYSPACE.to_string(), key)])
}

impl GetOidcOperation {
    pub fn new(input: GetOidcInput) -> Self {
        Self {
            input,
            stage: Stage::Subject,
            start: None,
            current: None,
            seen: Vec::new(),
        }
    }

    fn subject_key(&self) -> Result<String, GetOidcError> {
        Ok(oidc_subject_key(
            &self.input.issuer,
            &self.input.subject_id,
        )?)
    }

    /// A sign-in that still names a merged account opens the root of its merges, once that root
    /// lists the account; otherwise the account itself, which then refuses as deactivated.
    fn follow(&mut self) -> OidcTurn {
        let start = self.start.clone().ok_or(GetOidcError::NotFinished)?;
        let current = self.current.clone().ok_or(GetOidcError::NotFinished)?;
        if let Some(next) = current.merged_into()
            && next.realm_id == start.user_id.realm_id
            && next != start.user_id
            && !self.seen.contains(&next)
        {
            self.seen.push(next);
            self.stage = Stage::Merge;
            return Ok(Next::Call(user_read(next)));
        }
        let root =
            current.merged_into().is_none() && current.merged_user_ids().contains(&start.user_id);
        Ok(Next::Done(if root { current } else { start }))
    }
}

impl Operation for GetOidcOperation {
    type Output = User;
    type Error = GetOidcError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }

    fn first(&mut self) -> OidcTurn {
        let key = ByteView::from(self.subject_key()?.into_bytes());
        Ok(Next::Call(IoCall::Read(vec![(
            SUBJECT_INDEX_KEYSPACE.to_string(),
            key,
        )])))
    }

    fn next(&mut self, result: IoResult) -> OidcTurn {
        let [value] = values::<1>(result)?;
        match self.stage {
            Stage::Subject => {
                let key = value.ok_or(GetOidcError::UserNotFound)?;
                self.stage = Stage::User;
                Ok(Next::Call(user_read(UserId::from_storage_key(&key)?)))
            }
            Stage::User => {
                let user = User::from_bytes(&value.ok_or(GetOidcError::UserNotFound)?)?;
                self.start = Some(user.clone());
                self.current = Some(user);
                self.follow()
            }
            Stage::Merge => {
                let expected = self.current.as_ref().and_then(User::merged_into);
                let next = value.map(|bytes| User::from_bytes(&bytes)).transpose()?;
                self.current = match next {
                    Some(next) if Some(next.user_id) == expected => Some(next),
                    _ => self.start.clone(),
                };
                self.follow()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{GetOidcInput, GetOidcOperation};
    use aruna_core::UserId;
    use aruna_core::calls::{IoResult, Next};
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::identity::user::User;
    use aruna_core::user::validation::{MERGED_INTO_ATTRIBUTE, MERGED_PREFIX};
    use byteview::ByteView;
    use ulid::Ulid;

    fn user(seed: u8, attributes: Vec<(String, String)>) -> User {
        User {
            user_id: UserId::local(Ulid::from_bytes([seed; 16]), RealmId([8u8; 32])),
            name: format!("user {seed}"),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: attributes.into_iter().collect(),
        }
    }

    fn read(user: &User) -> IoResult {
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            user_id: user.user_id,
            realm_id: user.user_id.realm_id,
        };
        IoResult::Values(vec![Some(user.to_bytes(&actor).unwrap().into())])
    }

    /// Signs in with a subject indexed to `merged` and returns the account the login opens.
    fn sign_in(merged: &User, primary: &User) -> UserId {
        let mut operation = GetOidcOperation::new(GetOidcInput {
            issuer: "https://issuer.example".to_string(),
            subject_id: "subject".to_string(),
        });
        assert_eq!(operation.transaction(), TransactionMode::ReadSnapshot);
        operation.first().unwrap();
        let index = ByteView::from(merged.user_id.to_storage_key());
        operation.next(IoResult::Values(vec![Some(index)])).unwrap();
        let mut turn = operation.next(read(merged));
        if matches!(turn, Ok(Next::Call(_))) {
            turn = operation.next(read(primary));
        }
        match turn {
            Ok(Next::Done(user)) => user.user_id,
            _ => panic!("expected the account the login opens"),
        }
    }

    #[test]
    fn merged_opens_root() {
        let pointer = |to: &User| vec![(MERGED_INTO_ATTRIBUTE.to_string(), to.user_id.to_string())];
        let unlisted = user(1, Vec::new());
        let merged = user(2, pointer(&unlisted));
        assert_eq!(sign_in(&merged, &unlisted), merged.user_id);

        let marker = (
            format!("{MERGED_PREFIX}{}", merged.user_id.user_ulid),
            merged.user_id.to_string(),
        );
        let primary = user(1, vec![marker]);
        assert_eq!(sign_in(&merged, &primary), primary.user_id);
    }
}
