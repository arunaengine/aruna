//! Share link use cases: create a link after checking the creator, open a link with its
//! password, and revoke a link of its owner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::keyspaces::SHARE_LINK_KEYSPACE;
use aruna_core::operation::{Operation, boxed_retry, boxed_suboperation};
use aruna_core::rows;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::share::{ShareAction, ShareEvent, ShareLink, ShareToken};
use aruna_core::types::Value;
use thiserror::Error;
use ulid::Ulid;

use aruna_core::document::DocumentTarget;
use aruna_core::structs::identity::user::User;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::error::{AuthorizeError, check_error};
use crate::shares::rules::{ShareRuleError, required_permission, validate_link, visit_expiry};
use crate::shares::store::{LogShareOperation, ShareStoreError, save_rows};

#[derive(Debug, Error, PartialEq)]
pub enum ShareError {
    #[error(transparent)]
    Rule(#[from] ShareRuleError),
    #[error(transparent)]
    Store(#[from] ShareStoreError),
    #[error("permission denied")]
    Denied,
    #[error("authorization check failed: {0}")]
    Check(String),
    /// Unknown, ended, foreign or mistyped links share this one answer.
    #[error("the link does not work")]
    Unavailable,
    #[error("the link needs a password")]
    PasswordRequired,
    #[error("the password is not correct")]
    WrongPassword,
    #[error("the access log was not written: {0}")]
    Log(String),
    #[error("unexpected event in share state {0}")]
    UnexpectedEvent(&'static str),
    #[error("share operation did not finish")]
    NotFinished,
}

impl From<aruna_core::errors::ConversionError> for ShareError {
    fn from(error: aruna_core::errors::ConversionError) -> Self {
        Self::Store(error.into())
    }
}

impl From<StorageError> for ShareError {
    fn from(error: StorageError) -> Self {
        Self::Store(error.into())
    }
}

impl From<IoError> for ShareError {
    fn from(error: IoError) -> Self {
        Self::Store(error.into())
    }
}

fn read_link(id: Ulid) -> IoCall {
    rows::read_row(SHARE_LINK_KEYSPACE, id.to_bytes().to_vec())
}

fn parse_link(value: Option<Value>, id: Ulid) -> Result<Option<ShareLink>, ShareError> {
    let Some(bytes) = value else {
        return Ok(None);
    };
    let link = ShareLink::from_bytes(&bytes).map_err(ShareStoreError::from)?;
    match link.id == id {
        true => Ok(Some(link)),
        false => Err(ShareError::UnexpectedEvent("read")),
    }
}

fn logged(event: Event) -> Result<(), ShareError> {
    match event {
        Event::SubOperation(SubOperationEvent::ShareLogged { result }) => {
            result.map_err(ShareError::Log)
        }
        _ => Err(ShareError::UnexpectedEvent("log")),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateShareConfig {
    pub auth: AuthContext,
    /// The new link; ids, secrets and the password hash come from the caller.
    pub link: ShareLink,
    /// The permission path of the target the creator must reach.
    pub path: String,
    /// Unix seconds.
    pub now: u64,
}

/// Checks a new link and the creator's permission on its target, then stores it.
#[derive(Debug, PartialEq)]
pub struct CreateShareOperation {
    config: CreateShareConfig,
    saving: bool,
}

impl CreateShareOperation {
    pub fn new(config: CreateShareConfig) -> Self {
        Self {
            config,
            saving: false,
        }
    }
}

impl Operation for CreateShareOperation {
    type Output = ShareLink;
    type Error = ShareError;

    fn first(&mut self) -> Result<Next<ShareLink>, ShareError> {
        let config = &self.config;
        if config.link.created_by != config.auth.user_id {
            return Err(ShareError::Denied);
        }
        validate_link(&config.link, config.now)?;
        let check = CheckPermissionsConfig {
            auth_context: config.auth.clone(),
            path: config.path.clone(),
            required_permission: required_permission(config.link.mode),
        };
        Ok(Next::Call(IoCall::Child(boxed_suboperation(
            CheckPermissionsOperation::new(check),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ShareLink>, ShareError> {
        if self.saving {
            rows::done(result)?;
            return Ok(Next::Done(self.config.link.clone()));
        }
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) =
            rows::event(result)?
        else {
            return Err(ShareError::UnexpectedEvent("create"));
        };
        match allowed {
            Ok(true) => {
                self.saving = true;
                Ok(Next::Call(IoCall::Write(save_rows(&self.config.link)?)))
            }
            Ok(false) => Err(ShareError::Denied),
            Err(error) => match check_error(error) {
                AuthorizeError::PermissionDenied => Err(ShareError::Denied),
                error => Err(ShareError::Check(error.to_string())),
            },
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct OpenShareConfig {
    pub token: ShareToken,
    pub password: Option<String>,
    /// Unix seconds.
    pub now: u64,
    /// Orders the access log entry this open writes.
    pub entry_id: Ulid,
    /// The client network the log keeps.
    pub client: String,
}

impl std::fmt::Debug for OpenShareConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenShareConfig")
            .field("share_id", &self.token.share_id())
            .finish_non_exhaustive()
    }
}

/// A link opened for one visit.
#[derive(Clone, Debug, PartialEq)]
pub struct OpenedShare {
    pub link: ShareLink,
    pub visit_token: String,
    /// Unix seconds.
    pub expires_at: u64,
}

/// The call an open waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenState {
    Load,
    /// Reads the creator's account; a deactivated creator ends the link before any password.
    CheckCreator,
    LogOpen,
    LogWrong,
}

/// Opens a link with its URL token and, when it has one, its password; logs the outcome.
#[derive(Debug, PartialEq)]
pub struct OpenShareOperation {
    config: OpenShareConfig,
    state: OpenState,
    link: Option<ShareLink>,
    opened: Option<OpenedShare>,
}

impl OpenShareOperation {
    pub fn new(config: OpenShareConfig) -> Self {
        Self {
            config,
            state: OpenState::Load,
            link: None,
            opened: None,
        }
    }

    fn log(&mut self, action: ShareAction, state: OpenState) -> Next<OpenedShare> {
        let event = ShareEvent {
            at: self.config.now.saturating_mul(1000),
            action,
            key: None,
            bytes: None,
            client: Some(self.config.client.clone()),
        };
        let share_id = self.config.token.share_id();
        let entry_id = self.config.entry_id;
        self.state = state;
        let build = move || LogShareOperation::new(share_id, entry_id, event.clone());
        Next::Call(IoCall::Child(boxed_retry(
            build,
            &entry_id.to_bytes(),
            |result| {
                let result = result.map_err(|error| error.to_string());
                Event::SubOperation(SubOperationEvent::ShareLogged { result })
            },
        )))
    }

    fn loaded(&mut self, link: Option<ShareLink>) -> Result<Next<OpenedShare>, ShareError> {
        let now = self.config.now;
        let link = match link {
            Some(link)
                if matches!(self.config.token, ShareToken::Link { .. })
                    && link.active(now)
                    && link.admits(&self.config.token, now) =>
            {
                link
            }
            _ => return Err(ShareError::Unavailable),
        };
        let creator = DocumentTarget::User {
            user_id: link.created_by,
        };
        self.link = Some(link);
        self.state = OpenState::CheckCreator;
        Ok(Next::Call(rows::read_row(
            creator.storage_keyspace(),
            creator.storage_key(),
        )))
    }

    fn creator_read(&mut self, value: Option<Value>) -> Result<Next<OpenedShare>, ShareError> {
        // An account without a user document counts as active, as everywhere else.
        let user = value.map(|bytes| User::from_bytes(&bytes)).transpose()?;
        if user.is_some_and(|user| user.is_deactivated()) {
            return Err(ShareError::Unavailable);
        }
        let link = self
            .link
            .take()
            .ok_or(ShareError::UnexpectedEvent("creator"))?;
        self.admit(link)
    }

    fn admit(&mut self, link: ShareLink) -> Result<Next<OpenedShare>, ShareError> {
        let now = self.config.now;
        if let Some(hash) = &link.password {
            let password = (self.config.password.as_deref()).ok_or(ShareError::PasswordRequired)?;
            if !hash.verify(password) {
                return Ok(self.log(ShareAction::WrongPassword, OpenState::LogWrong));
            }
        }
        let expires_at = visit_expiry(&link, now);
        self.opened = Some(OpenedShare {
            visit_token: link.visit_token(expires_at),
            expires_at,
            link,
        });
        Ok(self.log(ShareAction::Open, OpenState::LogOpen))
    }
}

impl Operation for OpenShareOperation {
    type Output = OpenedShare;
    type Error = ShareError;

    fn first(&mut self) -> Result<Next<OpenedShare>, ShareError> {
        Ok(Next::Call(read_link(self.config.token.share_id())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<OpenedShare>, ShareError> {
        match self.state {
            OpenState::Load => {
                let link = parse_link(rows::value(result)?, self.config.token.share_id())?;
                self.loaded(link)
            }
            OpenState::CheckCreator => self.creator_read(rows::value(result)?),
            OpenState::LogOpen => {
                logged(rows::event(result)?)?;
                (self.opened.take().map(Next::Done)).ok_or(ShareError::NotFinished)
            }
            OpenState::LogWrong => {
                logged(rows::event(result)?)?;
                Err(ShareError::WrongPassword)
            }
        }
    }
}

/// Revokes one link of `owner`; another user's link is `Unavailable` like an unknown one.
#[derive(Debug, PartialEq)]
pub struct RevokeShareOperation {
    owner: UserId,
    id: Ulid,
    now: u64,
    saving: bool,
}

impl RevokeShareOperation {
    pub fn new(owner: UserId, id: Ulid, now: u64) -> Self {
        Self {
            owner,
            id,
            now,
            saving: false,
        }
    }
}

impl Operation for RevokeShareOperation {
    type Output = ();
    type Error = ShareError;

    fn first(&mut self) -> Result<Next<()>, ShareError> {
        Ok(Next::Call(read_link(self.id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ShareError> {
        if self.saving {
            rows::done(result)?;
            return Ok(Next::Done(()));
        }
        let link = parse_link(rows::value(result)?, self.id)?;
        let link = link.filter(|link| link.created_by == self.owner);
        let link = link.ok_or(ShareError::Unavailable)?;
        if link.revoked_at.is_some() {
            return Ok(Next::Done(()));
        }
        let revoked = ShareLink {
            revoked_at: Some(self.now),
            ..link
        };
        self.saving = true;
        Ok(Next::Call(IoCall::Write(save_rows(&revoked)?)))
    }
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
