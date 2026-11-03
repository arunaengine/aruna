//! Share link use cases: create a link after checking the creator, open a link with its
//! password, and revoke a link of its owner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::SHARE_LINK_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::share::{ShareAction, ShareEvent, ShareLink, ShareToken};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use aruna_core::document::DocumentTarget;
use aruna_core::structs::identity::user::User;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::request_authorization::{AuthorizeError, check_error};
use crate::document_repository::read_effect;
use crate::shares::rules::{ShareRuleError, required_permission, validate_link, visit_expiry};
use crate::shares::store::{LogShareOperation, ShareStoreError, save_effect};

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

fn read_link(id: Ulid) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: SHARE_LINK_KEYSPACE.to_string(),
        key: id.to_bytes().to_vec().into(),
        txn_id: None,
    })
}

fn parse_link(event: Event, id: Ulid) -> Result<Option<ShareLink>, ShareError> {
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => match value {
            Some(bytes) => {
                let link = ShareLink::from_bytes(&bytes).map_err(ShareStoreError::from)?;
                match link.id == id {
                    true => Ok(Some(link)),
                    false => Err(ShareError::UnexpectedEvent("read")),
                }
            }
            None => Ok(None),
        },
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(ShareError::UnexpectedEvent("read")),
    }
}

fn written(event: Event, state: &'static str) -> Result<(), ShareError> {
    match event {
        Event::Storage(
            StorageEvent::BatchWriteResult { .. } | StorageEvent::WriteResult { .. },
        ) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(ShareError::UnexpectedEvent(state)),
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CreateState {
    Init,
    Authorize,
    Save,
    Done,
    Failed,
}

/// Checks a new link and the creator's permission on its target, then stores it.
#[derive(Debug, PartialEq)]
pub struct CreateShareOperation {
    config: CreateShareConfig,
    state: CreateState,
    error: Option<ShareError>,
}

impl CreateShareOperation {
    pub fn new(config: CreateShareConfig) -> Self {
        Self {
            config,
            state: CreateState::Init,
            error: None,
        }
    }

    fn fail(&mut self, error: ShareError) -> Effects {
        self.state = CreateState::Failed;
        self.error = Some(error);
        smallvec![]
    }
}

impl Operation for CreateShareOperation {
    type Output = ShareLink;
    type Error = ShareError;

    fn start(&mut self) -> Effects {
        let config = &self.config;
        if config.link.created_by != config.auth.user_id {
            return self.fail(ShareError::Denied);
        }
        if let Err(error) = validate_link(&config.link, config.now) {
            return self.fail(error.into());
        }
        self.state = CreateState::Authorize;
        let check = CheckPermissionsConfig {
            auth_context: config.auth.clone(),
            path: config.path.clone(),
            required_permission: required_permission(config.link.mode),
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(check),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (
                CreateState::Authorize,
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
            ) => match allowed {
                Ok(true) => match save_effect(&self.config.link) {
                    Ok(effect) => {
                        self.state = CreateState::Save;
                        smallvec![effect]
                    }
                    Err(error) => self.fail(error.into()),
                },
                Ok(false) => self.fail(ShareError::Denied),
                Err(error) => match check_error(error) {
                    AuthorizeError::PermissionDenied => self.fail(ShareError::Denied),
                    error => self.fail(ShareError::Check(error.to_string())),
                },
            },
            (CreateState::Save, event) => match written(event, "save") {
                Ok(()) => {
                    self.state = CreateState::Done;
                    smallvec![]
                }
                Err(error) => self.fail(error),
            },
            _ => self.fail(ShareError::UnexpectedEvent("create")),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, CreateState::Done | CreateState::Failed)
    }

    fn finalize(self) -> Result<ShareLink, ShareError> {
        match (self.state, self.error) {
            (CreateState::Done, _) => Ok(self.config.link),
            (_, Some(error)) => Err(error),
            _ => Err(ShareError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenState {
    Init,
    Load,
    /// Reads the creator's account; a deactivated creator ends the link before any password.
    CheckCreator,
    LogOpen,
    LogWrong,
    Done,
    Failed,
}

/// Opens a link with its URL token and, when it has one, its password; logs the outcome.
#[derive(Debug, PartialEq)]
pub struct OpenShareOperation {
    config: OpenShareConfig,
    state: OpenState,
    link: Option<ShareLink>,
    opened: Option<OpenedShare>,
    error: Option<ShareError>,
}

impl OpenShareOperation {
    pub fn new(config: OpenShareConfig) -> Self {
        Self {
            config,
            state: OpenState::Init,
            link: None,
            opened: None,
            error: None,
        }
    }

    fn fail(&mut self, error: ShareError) -> Effects {
        self.state = OpenState::Failed;
        self.error = Some(error);
        smallvec![]
    }

    fn log(&mut self, action: ShareAction, state: OpenState) -> Effects {
        let event = ShareEvent {
            at: self.config.now.saturating_mul(1000),
            action,
            key: None,
            bytes: None,
            client: Some(self.config.client.clone()),
        };
        let share_id = self.config.token.share_id();
        self.state = state;
        let log = LogShareOperation::new(share_id, self.config.entry_id, event);
        smallvec![Effect::SubOperation(boxed_suboperation(log, |result| {
            let result = result.map_err(|error| error.to_string());
            Event::SubOperation(SubOperationEvent::ShareLogged { result })
        }))]
    }

    fn loaded(&mut self, link: Option<ShareLink>) -> Effects {
        let now = self.config.now;
        let link = match link {
            Some(link)
                if matches!(self.config.token, ShareToken::Link { .. })
                    && link.active(now)
                    && link.admits(&self.config.token, now) =>
            {
                link
            }
            _ => return self.fail(ShareError::Unavailable),
        };
        let creator = DocumentTarget::User {
            user_id: link.created_by,
        };
        self.link = Some(link);
        self.state = OpenState::CheckCreator;
        smallvec![read_effect(&creator, None)]
    }

    fn creator_read(&mut self, event: Event) -> Effects {
        let active = match event {
            // An account without a user document counts as active, as everywhere else.
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value
                .map(|bytes| User::from_bytes(&bytes).map(|user| !user.is_deactivated()))
                .transpose()
                .map(|active| active.unwrap_or(true)),
            Event::Storage(StorageEvent::Error { error }) => return self.fail(error.into()),
            _ => return self.fail(ShareError::UnexpectedEvent("creator")),
        };
        match (active, self.link.take()) {
            (Ok(true), Some(link)) => self.admit(link),
            (Ok(false), _) => self.fail(ShareError::Unavailable),
            (Err(error), _) => self.fail(error.into()),
            (Ok(true), None) => self.fail(ShareError::UnexpectedEvent("creator")),
        }
    }

    fn admit(&mut self, link: ShareLink) -> Effects {
        let now = self.config.now;
        if let Some(hash) = &link.password {
            let Some(password) = self.config.password.as_deref() else {
                return self.fail(ShareError::PasswordRequired);
            };
            if !hash.verify(password) {
                return self.log(ShareAction::WrongPassword, OpenState::LogWrong);
            }
        }
        let expires_at = visit_expiry(&link, now);
        self.opened = Some(OpenedShare {
            visit_token: link.visit_token(expires_at),
            expires_at,
            link,
        });
        self.log(ShareAction::Open, OpenState::LogOpen)
    }
}

impl Operation for OpenShareOperation {
    type Output = OpenedShare;
    type Error = ShareError;

    fn start(&mut self) -> Effects {
        self.state = OpenState::Load;
        smallvec![read_link(self.config.token.share_id())]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            OpenState::Load => match parse_link(event, self.config.token.share_id()) {
                Ok(link) => self.loaded(link),
                Err(error) => self.fail(error),
            },
            OpenState::CheckCreator => self.creator_read(event),
            OpenState::LogOpen => match logged(event) {
                Ok(()) => {
                    self.state = OpenState::Done;
                    smallvec![]
                }
                Err(error) => self.fail(error),
            },
            OpenState::LogWrong => match logged(event) {
                Ok(()) => self.fail(ShareError::WrongPassword),
                Err(error) => self.fail(error),
            },
            OpenState::Init | OpenState::Done | OpenState::Failed => {
                self.fail(ShareError::UnexpectedEvent("open"))
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, OpenState::Done | OpenState::Failed)
    }

    fn finalize(self) -> Result<OpenedShare, ShareError> {
        match (self.state, self.opened, self.error) {
            (OpenState::Done, Some(opened), _) => Ok(opened),
            (_, _, Some(error)) => Err(error),
            _ => Err(ShareError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RevokeState {
    Init,
    Load,
    Save,
    Done,
    Failed,
}

/// Revokes one link of `owner`; another user's link is `Unavailable` like an unknown one.
#[derive(Debug, PartialEq)]
pub struct RevokeShareOperation {
    owner: UserId,
    id: Ulid,
    now: u64,
    state: RevokeState,
    error: Option<ShareError>,
}

impl RevokeShareOperation {
    pub fn new(owner: UserId, id: Ulid, now: u64) -> Self {
        Self {
            owner,
            id,
            now,
            state: RevokeState::Init,
            error: None,
        }
    }

    fn fail(&mut self, error: ShareError) -> Effects {
        self.state = RevokeState::Failed;
        self.error = Some(error);
        smallvec![]
    }
}

impl Operation for RevokeShareOperation {
    type Output = ();
    type Error = ShareError;

    fn start(&mut self) -> Effects {
        self.state = RevokeState::Load;
        smallvec![read_link(self.id)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            RevokeState::Load => match parse_link(event, self.id) {
                Ok(Some(link)) if link.created_by == self.owner => {
                    if link.revoked_at.is_some() {
                        self.state = RevokeState::Done;
                        return smallvec![];
                    }
                    let revoked = ShareLink {
                        revoked_at: Some(self.now),
                        ..link
                    };
                    match save_effect(&revoked) {
                        Ok(effect) => {
                            self.state = RevokeState::Save;
                            smallvec![effect]
                        }
                        Err(error) => self.fail(error.into()),
                    }
                }
                Ok(_) => self.fail(ShareError::Unavailable),
                Err(error) => self.fail(error),
            },
            RevokeState::Save => match written(event, "save") {
                Ok(()) => {
                    self.state = RevokeState::Done;
                    smallvec![]
                }
                Err(error) => self.fail(error),
            },
            RevokeState::Init | RevokeState::Done | RevokeState::Failed => {
                self.fail(ShareError::UnexpectedEvent("revoke"))
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RevokeState::Done | RevokeState::Failed)
    }

    fn finalize(self) -> Result<(), ShareError> {
        match (self.state, self.error) {
            (RevokeState::Done, _) => Ok(()),
            (_, Some(error)) => Err(error),
            _ => Err(ShareError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
