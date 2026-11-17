//! Turns email on for a user's verified address as a new register write of the setting.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::compute::SecretBytes;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::email::{EMAIL_CATEGORIES, EmailPreference};
use aruna_core::structs::identity::user::User;
use aruna_core::types::Effects;
use aruna_core::user::validation::{EMAIL_HASH_ATTRIBUTE, email_hash};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::document_repository::GetConfigOperation;
use crate::notifications::setting_rows::{
    SettingError, SettingValue, SettingWrite, WriteSettingOperation,
};

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

/// One email opt-in; ids and time come from the caller, as does the address format check.
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

#[derive(Debug, Default, PartialEq)]
enum OptInState {
    #[default]
    Init,
    Config(Box<GetConfigOperation>),
    User,
    Write(Box<WriteSettingOperation>),
    Done(Box<EmailPreference>),
    Failed(EmailError),
}

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
            state: OptInState::Init,
        }
    }

    fn fail(&mut self, error: EmailError) -> Effects {
        self.state = OptInState::Failed(error);
        smallvec![]
    }

    fn configured(&mut self, config: GetConfigOperation) -> Effects {
        let Some(opt_in) = self.opt_in.as_ref() else {
            return self.fail(EmailError::NotVerified);
        };
        let config = match config.finalize() {
            Ok(config) => config,
            Err(error) => return self.fail(SettingError::Config(error.to_string()).into()),
        };
        if config.email.is_none() {
            return self.fail(EmailError::NotConfigured);
        }
        let categories = &opt_in.categories;
        let known =
            (categories.iter()).all(|category| EMAIL_CATEGORIES.contains(&category.as_str()));
        if categories.is_empty() || !known {
            return self.fail(EmailError::InvalidCategories);
        }
        self.state = OptInState::User;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_KEYSPACE.to_string(),
            key: opt_in.owner.to_bytes().into(),
            txn_id: None,
        })]
    }

    /// Writes the opt-in when `user` holds the address its sign-in provider verified.
    fn checked(&mut self, user: Option<User>) -> Effects {
        let Some(opt_in) = self.opt_in.take() else {
            return self.fail(EmailError::NotVerified);
        };
        let hash = user.and_then(|user| user.attributes.get(EMAIL_HASH_ATTRIBUTE).cloned());
        let verified = hash.is_some_and(|hash| hash == email_hash(&opt_in.address));
        if !verified || !opt_in.address_valid {
            return self.fail(EmailError::NotVerified);
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
        let mut write = WriteSettingOperation::new(write);
        let effects = write.start();
        self.state = OptInState::Write(Box::new(write));
        effects
    }

    fn written(&mut self, write: WriteSettingOperation) -> Effects {
        let preference = write
            .finalize()
            .map_err(EmailError::from)
            .and_then(|bytes| Ok(EmailPreference::from_bytes(&bytes)?));
        match preference {
            Ok(preference) => {
                self.state = OptInState::Done(Box::new(preference));
                smallvec![]
            }
            Err(error) => self.fail(error),
        }
    }
}

impl Operation for SetEmailOperation {
    type Output = EmailPreference;
    type Error = EmailError;

    fn start(&mut self) -> Effects {
        let Some(opt_in) = self.opt_in.as_ref() else {
            return self.fail(EmailError::NotVerified);
        };
        let mut config = GetConfigOperation::new(opt_in.owner.realm_id);
        let effects = config.start();
        if config.is_complete() {
            return self.configured(config);
        }
        self.state = OptInState::Config(Box::new(config));
        effects
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::take(&mut self.state), event) {
            (OptInState::Config(mut config), event) => {
                let effects = config.step(event);
                if config.is_complete() {
                    return self.configured(*config);
                }
                self.state = OptInState::Config(config);
                effects
            }
            (OptInState::User, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                match value.as_deref().map(User::from_bytes).transpose() {
                    Ok(user) => self.checked(user),
                    Err(error) => self.fail(error.into()),
                }
            }
            (OptInState::User, Event::Storage(StorageEvent::Error { error })) => {
                self.fail(error.into())
            }
            (OptInState::Write(mut write), event) => {
                let effects = write.step(event);
                if write.is_complete() {
                    return self.written(*write);
                }
                self.state = OptInState::Write(write);
                effects
            }
            (_, other) => {
                let error = SettingError::UnexpectedEvent(format!("{other:?}"));
                self.fail(error.into())
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, OptInState::Done(_) | OptInState::Failed(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            OptInState::Done(preference) => Ok(*preference),
            OptInState::Failed(error) => Err(error),
            _ => Err(SettingError::NotFinished.into()),
        }
    }

    fn abort(&mut self) -> Effects {
        match &mut self.state {
            OptInState::Config(config) => config.abort(),
            OptInState::Write(write) => write.abort(),
            _ => smallvec![],
        }
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::tests::locations::node_id;
    use crate::tests::notifications::user;
    use aruna_core::operation_check::{
        check_abort, rejects_events, rejects_finalize, sample_events,
    };

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

    #[test]
    fn reads_config_first() {
        let mut set = SetEmailOperation::new(opt_in(&["share.upload"]));
        let mut config = GetConfigOperation::new(user(1, 2).realm_id);
        assert_eq!(set.start(), config.start());
    }

    #[test]
    fn missing_user_unverified() {
        let mut set = SetEmailOperation::new(opt_in(&["share.upload"]));
        set.state = OptInState::User;
        set.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }));
        assert_eq!(set.finalize(), Err(EmailError::NotVerified));
    }

    #[test]
    fn user_read_error() {
        let mut set = SetEmailOperation::new(opt_in(&["share.upload"]));
        set.state = OptInState::User;
        set.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(
            set.finalize(),
            Err(EmailError::Storage(StorageError::Timeout))
        );
    }

    #[test]
    fn unexpected_events_fail() {
        let make = || {
            let mut set = SetEmailOperation::new(opt_in(&["share.upload"]));
            set.state = OptInState::User;
            set
        };
        let expected = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
            )
        };
        assert!(!rejects_events(make, sample_events, expected).is_empty());
    }

    #[test]
    fn abort_and_finalize() {
        let mut set = SetEmailOperation::new(opt_in(&["share.upload"]));
        set.start();
        set.abort();
        rejects_finalize(set);
        let mut idle = SetEmailOperation::new(opt_in(&[]));
        check_abort(&mut idle, &[]);
    }
}
