//! Turns email on for a user's verified address as a new register write of the setting.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SecretBytes;
use aruna_core::document::DocumentTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, values};
use aruna_core::structs::execution::email::{EMAIL_CATEGORIES, EmailPreference};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::user::User;
use aruna_core::user::validation::{EMAIL_HASH_ATTRIBUTE, email_hash};
use thiserror::Error;
use ulid::Ulid;

use crate::document_repository::{GetConfigError, read_effect};
use crate::notifications::setting_rows::{
    SettingError, SettingValue, SettingWrite, WriteSettingOperation,
};
use crate::notifications::{unexpected, unexpected_event};

#[derive(Debug, Error, PartialEq)]
pub enum EmailError {
    #[error(transparent)]
    Setting(#[from] SettingError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("choose at least one of these categories: {}", EMAIL_CATEGORIES.join(", "))]
    InvalidCategories,
    #[error("the address is not the verified email address of this account")]
    NotVerified,
    #[error("email is not configured in this realm")]
    NotConfigured,
}

impl From<IoError> for EmailError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            error => Self::Setting(SettingError::Io(error)),
        }
    }
}

/// One email opt-in with its ids, time and address format check.
#[derive(Debug, PartialEq)]
pub struct EmailOptIn {
    pub owner: UserId,
    pub local: NodeId,
    /// The trimmed address.
    pub address: String,
    pub address_valid: bool,
    pub categories: BTreeSet<String>,
    pub write_id: Ulid,
    pub event_id: Ulid,
    pub now_ms: u64,
}

impl EmailOptIn {
    /// An opt-in of the trimmed `address` with new ids.
    pub fn new(
        owner: UserId,
        local: NodeId,
        address: String,
        categories: BTreeSet<String>,
        now_ms: u64,
    ) -> Self {
        let address = address.trim().to_string();
        Self {
            owner,
            local,
            address_valid: aruna_outbound::valid_address(&address),
            address,
            categories,
            write_id: Ulid::generate(),
            event_id: Ulid::generate(),
            now_ms,
        }
    }
}

#[derive(Debug, Default, PartialEq)]
enum OptInState {
    #[default]
    Config,
    User,
    Write(Box<WriteSettingOperation>),
}

type Turn = Result<Next<EmailPreference>, EmailError>;

/// Turns on email for known categories to the account's verified address, in a realm with an
/// email profile. The address is sealed to the realm key, so only management nodes open it.
#[derive(Debug, PartialEq)]
pub struct SetEmailOperation {
    opt_in: Option<EmailOptIn>,
    state: OptInState,
}

impl SetEmailOperation {
    pub fn new(opt_in: EmailOptIn) -> Self {
        Self {
            opt_in: Some(opt_in),
            state: OptInState::Config,
        }
    }

    /// Checks the realm's email profile and the categories, then reads the account.
    fn configured(&mut self, value: Option<&[u8]>) -> Turn {
        let config = value
            .ok_or(GetConfigError::DocumentNotFound)
            .and_then(|value| Ok(RealmConfigDocument::from_bytes(value)?))
            .map_err(|error| SettingError::Config(error.to_string()))?;
        if config.email.is_none() {
            return Err(EmailError::NotConfigured);
        }
        let opt_in = self.opt_in.as_ref().ok_or(EmailError::NotVerified)?;
        let categories = &opt_in.categories;
        let known =
            (categories.iter()).all(|category| EMAIL_CATEGORIES.contains(&category.as_str()));
        if categories.is_empty() || !known {
            return Err(EmailError::InvalidCategories);
        }
        self.state = OptInState::User;
        Ok(Next::Call(read_row(USER_KEYSPACE, opt_in.owner.to_bytes())))
    }

    /// Writes the opt-in when `user` holds the address its sign-in provider verified.
    fn checked(&mut self, user: Option<User>) -> Turn {
        let opt_in = self.opt_in.take().ok_or(EmailError::NotVerified)?;
        let hash = user.and_then(|user| user.attributes.get(EMAIL_HASH_ATTRIBUTE).cloned());
        let verified = hash.is_some_and(|hash| hash == email_hash(&opt_in.address));
        if !verified || !opt_in.address_valid {
            return Err(EmailError::NotVerified);
        }
        let write = SettingWrite {
            owner: opt_in.owner,
            local: opt_in.local,
            write_id: opt_in.write_id,
            event_id: opt_in.event_id,
            now_ms: opt_in.now_ms,
            value: SettingValue::Email {
                categories: opt_in.categories,
                address: Some(SecretBytes::new(opt_in.address.into_bytes())),
            },
        };
        let mut write = Box::new(WriteSettingOperation::new(write));
        let turn = write.first();
        self.state = OptInState::Write(write);
        written(turn)
    }
}

/// The preference a finished setting write stored, or its next call.
fn written(turn: Result<Next<Vec<u8>>, SettingError>) -> Turn {
    match turn? {
        Next::Call(call) => Ok(Next::Call(call)),
        Next::Done(bytes) => Ok(Next::Done(EmailPreference::from_bytes(&bytes)?)),
        Next::Wait => Ok(Next::Wait),
        Next::Then { output, calls } => Ok(Next::Then {
            output: EmailPreference::from_bytes(&output)?,
            calls,
        }),
    }
}

impl Operation for SetEmailOperation {
    type Output = EmailPreference;
    type Error = EmailError;

    fn first(&mut self) -> Turn {
        let opt_in = self.opt_in.as_ref().ok_or(EmailError::NotVerified)?;
        let realm_id = opt_in.owner.realm_id;
        let read = read_effect(&DocumentTarget::RealmConfig { realm_id }, None);
        Ok(Next::Call(IoCall::from(read)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match (&mut self.state, result) {
            // A failed config read means the realm cannot be checked now, not a storage fault.
            (OptInState::Config, result) => match result.event()? {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    self.configured(value.as_deref())
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    Err(SettingError::Config(GetConfigError::from(error).to_string()).into())
                }
                other => Err(unexpected_event("the realm config read", &other).into()),
            },
            (OptInState::User, result @ IoResult::Values(_)) => {
                let [value] = values::<1>(result)?;
                let user = value.as_deref().map(User::from_bytes).transpose()?;
                self.checked(user)
            }
            (OptInState::Write(write), result) => written(write.next(result)),
            (_, other) => Err(unexpected("a config, account or setting result", &other).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::locations::node_id;
    use crate::tests::notifications::user;

    fn opt_in(categories: &[&str]) -> EmailOptIn {
        EmailOptIn {
            owner: user(1, 2),
            local: node_id(1),
            address: "ada@example.org".to_string(),
            address_valid: true,
            categories: categories
                .iter()
                .map(|category| category.to_string())
                .collect(),
            write_id: Ulid::from_parts(1, 1),
            event_id: Ulid::from_parts(2, 2),
            now_ms: 1_000,
        }
    }

    fn at_user(categories: &[&str]) -> SetEmailOperation {
        let mut set = SetEmailOperation::new(opt_in(categories));
        set.state = OptInState::User;
        set
    }

    #[test]
    fn reads_config_first() {
        let mut set = SetEmailOperation::new(opt_in(&["share.upload"]));
        let target = DocumentTarget::RealmConfig {
            realm_id: user(1, 2).realm_id,
        };
        assert_eq!(
            set.first(),
            Ok(Next::Call(IoCall::from(read_effect(&target, None))))
        );
    }

    #[test]
    fn config_failure_unavailable() {
        let mut set = SetEmailOperation::new(opt_in(&["share.upload"]));
        let missing = Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        });
        assert!(matches!(
            set.next(IoResult::Event(Box::new(missing))),
            Err(EmailError::Setting(SettingError::Config(_)))
        ));
        let mut set = SetEmailOperation::new(opt_in(&["share.upload"]));
        let failed = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert!(matches!(
            set.next(IoResult::Event(Box::new(failed))),
            Err(EmailError::Setting(SettingError::Config(_)))
        ));
    }

    #[test]
    fn missing_user_unverified() {
        let mut set = at_user(&["share.upload"]);
        assert_eq!(
            set.next(IoResult::Values(vec![None])),
            Err(EmailError::NotVerified)
        );
    }

    #[test]
    fn unexpected_results_fail() {
        let mut set = at_user(&["share.upload"]);
        assert!(matches!(
            set.next(IoResult::Done),
            Err(EmailError::Setting(SettingError::Io(
                IoError::Unexpected { .. }
            )))
        ));
        assert_eq!(
            EmailError::from(IoError::Storage(StorageError::Timeout)),
            EmailError::Storage(StorageError::Timeout)
        );
    }
}
