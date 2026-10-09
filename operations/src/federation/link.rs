//! Linked logins: the confirmation that binds a fresh local login and a fresh login of another
//! realm, the link it allows, and the unlink that cuts the linked login off first.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::federation::{FederationError, Signed};
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::link::{LinkAction, LinkConfirmation, LinkError, MAX_LINK_SECS};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, NodeCapabilities, SessionKind};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::user::User;
use aruna_core::types::Effects;
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::auth::revoke_token::RevokeTokenError;
use crate::users::update_user::UpdateUserError;

#[derive(Debug, Error, PartialEq)]
pub enum LinkLoginError {
    #[error("a fresh unrestricted portal login of an active local account is required")]
    LocalRefused,
    #[error("a fresh federated login of an admitted realm is required")]
    ForeignRefused,
    #[error("the linked login was cut off after the confirmation")]
    CutOff,
    #[error("the account has no such linked login")]
    NotLinked,
    #[error("caller may not unlink this login")]
    Unauthorized,
    #[error(transparent)]
    Confirmation(#[from] LinkError),
    #[error(transparent)]
    Sign(#[from] FederationError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Authorization(#[from] AuthorizationError),
    #[error(transparent)]
    Update(#[from] UpdateUserError),
    #[error(transparent)]
    Revoke(#[from] RevokeTokenError),
    #[error("link operation did not finish")]
    NotFinished,
}

fn user_read(user_id: &UserId) -> (String, ByteView) {
    (
        USER_KEYSPACE.to_string(),
        ByteView::from(user_id.to_bytes()),
    )
}

fn config_read(auth: &AuthContext) -> (String, ByteView) {
    let target = DocumentTarget::RealmConfig {
        realm_id: auth.realm_id,
    };
    (target.storage_keyspace().to_string(), target.storage_key())
}

/// An unrestricted portal session of a local account, issued at most `MAX_LINK_SECS` ago.
fn fresh_local(auth: &AuthContext, issued_at: u64, now: u64) -> bool {
    let portal = auth.session.as_ref().map(|session| session.kind) == Some(SessionKind::Portal);
    portal
        && auth.path_restrictions.is_none()
        && auth.user_id.realm_id == auth.realm_id
        && !auth.user_id.is_nil()
        && issued_at.saturating_add(MAX_LINK_SECS) >= now
}

/// An active account that is not a service account.
fn usable_account(value: Option<&ByteView>) -> Result<User, LinkLoginError> {
    let user = value
        .map(|bytes| User::from_bytes(bytes))
        .transpose()?
        .ok_or(LinkLoginError::LocalRefused)?;
    if user.is_deactivated() || user.service_group().is_some() {
        return Err(LinkLoginError::LocalRefused);
    }
    Ok(user)
}

fn read_config(value: Option<&ByteView>) -> Result<RealmConfigDocument, LinkLoginError> {
    value
        .map(|bytes| RealmConfigDocument::from_bytes(bytes))
        .transpose()?
        .ok_or(LinkLoginError::ForeignRefused)
}

/// Whether the realm still admits logins of `foreign`'s realm.
fn admitted(config: &RealmConfigDocument, foreign: &UserId) -> bool {
    config
        .federation
        .as_ref()
        .is_some_and(|settings| settings.accepted_realms.admits(&foreign.realm_id))
}

fn batch_values(event: Event) -> Result<Vec<Option<ByteView>>, LinkLoginError> {
    match event {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(LinkLoginError::NotFinished),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConfirmLinkConfig {
    /// The caller's local portal login.
    pub auth_context: AuthContext,
    pub local_issued_at: u64,
    /// The verified federated login this realm issued for the other realm's user.
    pub foreign: AuthContext,
    pub foreign_issued_at: u64,
    pub node_capabilities: NodeCapabilities,
    pub now: u64,
}

/// Signs a short-lived confirmation for linking the foreign login to the caller's account.
#[derive(Debug, PartialEq)]
pub struct ConfirmLinkOperation {
    config: ConfirmLinkConfig,
    output: Option<Result<Signed<LinkConfirmation>, LinkLoginError>>,
}

impl ConfirmLinkOperation {
    pub fn new(config: ConfirmLinkConfig) -> Self {
        Self {
            config,
            output: None,
        }
    }

    fn foreign_user(&self) -> Option<UserId> {
        let (foreign, local) = (&self.config.foreign, &self.config.auth_context);
        let session = foreign.session.as_ref()?;
        let fresh = self.config.foreign_issued_at.saturating_add(MAX_LINK_SECS) >= self.config.now;
        (session.kind == SessionKind::Federated
            && session.via.is_none()
            && foreign.realm_id == local.realm_id
            && foreign.user_id.realm_id != local.realm_id
            && !foreign.user_id.is_nil()
            && foreign.path_restrictions.is_none()
            && fresh)
            .then_some(foreign.user_id)
    }

    fn sign(&self, event: Event) -> Result<Signed<LinkConfirmation>, LinkLoginError> {
        let values = batch_values(event)?;
        let [user, config] = values.as_slice() else {
            return Err(LinkLoginError::NotFinished);
        };
        usable_account(user.as_ref())?;
        let foreign = self.foreign_user().ok_or(LinkLoginError::ForeignRefused)?;
        if !admitted(&read_config(config.as_ref())?, &foreign) {
            return Err(LinkLoginError::ForeignRefused);
        }
        let config = &self.config;
        let confirmation = LinkConfirmation {
            realm_id: config.auth_context.realm_id,
            action: LinkAction::Link,
            local_user: config.auth_context.user_id,
            foreign_user: foreign,
            foreign_issued_at: config.foreign_issued_at,
            issued_at: config.now,
            expires_at: config.now.saturating_add(MAX_LINK_SECS),
            confirmation_id: Ulid::generate(),
        };
        Ok(Signed::sign(confirmation, &config.node_capabilities)?)
    }
}

impl Operation for ConfirmLinkOperation {
    type Output = Signed<LinkConfirmation>;
    type Error = LinkLoginError;

    fn start(&mut self) -> Effects {
        let config = &self.config;
        if !fresh_local(&config.auth_context, config.local_issued_at, config.now) {
            self.output = Some(Err(LinkLoginError::LocalRefused));
            return smallvec![];
        }
        if self.foreign_user().is_none() {
            self.output = Some(Err(LinkLoginError::ForeignRefused));
            return smallvec![];
        }
        let auth = &config.auth_context;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![user_read(&auth.user_id), config_read(auth)],
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.output = Some(self.sign(event));
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(LinkLoginError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
