//! Linked logins: the confirmation that binds a fresh local login and a fresh login of another
//! realm, the link it allows, and the unlink that cuts the linked login off first.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::auth::{REVOCATION_GRACE_SECS, user_cutoff_expiry, user_cutoff_hash};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::federation::{FederationError, Signed};
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::link::{
    LinkAction, LinkConfirmation, LinkError, MAX_LINK_SECS, check_confirmation,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::{
    Actor, AuthContext, NodeCapabilities, Permission, SessionKind,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::user::User;
use aruna_core::types::Effects;
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::revoke_token::{
    RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenError, RevokeTokenOperation,
};
use crate::users::update_user::{
    AliasChange, UpdateUserError, UpdateUserInput, UpdateUserOperation,
};

/// Commit conflicts a link or unlink rereads ownership for before it gives up.
const LINK_RETRIES: u8 = 3;

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

/// Runs the replicated alias change, restarting it on a commit conflict so ownership is reread.
#[derive(Debug, PartialEq)]
struct AliasUpdate {
    input: UpdateUserInput,
    update: UpdateUserOperation,
    retries: u8,
}

impl AliasUpdate {
    fn new(input: UpdateUserInput) -> Self {
        let update = UpdateUserOperation::new(input.clone());
        Self {
            input,
            update,
            retries: 0,
        }
    }

    /// The effects of this event, and the result once the update finished.
    fn step(&mut self, event: Event) -> (Effects, Option<Result<User, LinkLoginError>>) {
        let effects = self.update.step(event);
        if !self.update.is_complete() {
            return (effects, None);
        }
        let update = std::mem::replace(
            &mut self.update,
            UpdateUserOperation::new(self.input.clone()),
        );
        match update.finalize() {
            Err(UpdateUserError::StorageError(StorageError::TransactionConflict))
                if self.retries < LINK_RETRIES =>
            {
                self.retries += 1;
                let mut restarted = effects;
                restarted.extend(self.update.start());
                (restarted, None)
            }
            result => (effects, Some(result.map_err(Into::into))),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LinkLoginConfig {
    pub actor: Actor,
    pub auth_context: AuthContext,
    pub local_issued_at: u64,
    pub confirmation: Signed<LinkConfirmation>,
    pub now: u64,
}

#[derive(Debug, PartialEq)]
enum LinkState {
    Read,
    Update(Box<AliasUpdate>),
    Done,
}

/// Links the confirmed foreign login to the caller's account through the replicated user
/// document, after the confirmation, the realm's admission and the foreign cutoff are rechecked.
#[derive(Debug, PartialEq)]
pub struct LinkLoginOperation {
    config: LinkLoginConfig,
    state: LinkState,
    output: Option<Result<User, LinkLoginError>>,
}

impl LinkLoginOperation {
    pub fn new(config: LinkLoginConfig) -> Self {
        Self {
            config,
            state: LinkState::Read,
            output: None,
        }
    }

    fn finish(&mut self, result: Result<User, LinkLoginError>) -> Effects {
        self.state = LinkState::Done;
        self.output = Some(result);
        smallvec![]
    }

    fn admit(&self, event: Event) -> Result<UpdateUserInput, LinkLoginError> {
        let values = batch_values(event)?;
        let [user, config] = values.as_slice() else {
            return Err(LinkLoginError::NotFinished);
        };
        usable_account(user.as_ref())?;
        let realm = read_config(config.as_ref())?;
        let payload = &self.config.confirmation.payload;
        if !admitted(&realm, &payload.foreign_user) {
            return Err(LinkLoginError::ForeignRefused);
        }
        // A cutoff of the foreign login after its confirmed session voids the confirmation.
        if realm
            .user_cutoff(&payload.foreign_user, self.config.now)
            .is_some_and(|cutoff| payload.foreign_issued_at < cutoff)
        {
            return Err(LinkLoginError::CutOff);
        }
        let config = &self.config;
        Ok(UpdateUserInput {
            actor: config.actor.clone(),
            auth_context: config.auth_context.clone(),
            self_realm_id: config.auth_context.realm_id,
            user_id: config.auth_context.user_id.to_string(),
            name: None,
            set_attributes: Default::default(),
            remove_attributes: Vec::new(),
            system: false,
            alias: Some(AliasChange::Add(payload.foreign_user)),
        })
    }
}

impl Operation for LinkLoginOperation {
    type Output = User;
    type Error = LinkLoginError;

    fn start(&mut self) -> Effects {
        let config = &self.config;
        let auth = &config.auth_context;
        if !fresh_local(auth, config.local_issued_at, config.now) {
            return self.finish(Err(LinkLoginError::LocalRefused));
        }
        let checked = check_confirmation(
            &config.confirmation,
            &auth.realm_id,
            &auth.user_id,
            config.now,
        );
        if let Err(error) = checked {
            return self.finish(Err(error.into()));
        }
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![user_read(&auth.user_id), config_read(auth)],
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match std::mem::replace(&mut self.state, LinkState::Done) {
            LinkState::Read => match self.admit(event) {
                Ok(input) => {
                    let mut update = Box::new(AliasUpdate::new(input));
                    let effects = update.update.start();
                    self.state = LinkState::Update(update);
                    effects
                }
                Err(error) => self.finish(Err(error)),
            },
            LinkState::Update(mut update) => match update.step(event) {
                (effects, None) => {
                    self.state = LinkState::Update(update);
                    effects
                }
                (effects, Some(result)) => {
                    self.finish(result);
                    effects
                }
            },
            LinkState::Done => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(LinkLoginError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match &mut self.state {
            LinkState::Update(update) => update.update.abort(),
            _ => smallvec![],
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnlinkLoginConfig {
    pub actor: Actor,
    pub auth_context: AuthContext,
    pub user_id: UserId,
    pub alias: UserId,
    pub now: u64,
}

#[derive(Debug, PartialEq)]
enum UnlinkState {
    Read,
    Authorize,
    Cutoff(Box<RevokeTokenOperation>),
    Update(Box<AliasUpdate>),
    Done,
}

/// Removes a linked login: the owner or a realm administrator first records the credential
/// cutoff of the foreign login, then removes the alias through the replicated user document.
#[derive(Debug, PartialEq)]
pub struct UnlinkLoginOperation {
    config: UnlinkLoginConfig,
    state: UnlinkState,
    output: Option<Result<User, LinkLoginError>>,
}

impl UnlinkLoginOperation {
    pub fn new(config: UnlinkLoginConfig) -> Self {
        Self {
            config,
            state: UnlinkState::Read,
            output: None,
        }
    }

    fn finish(&mut self, result: Result<User, LinkLoginError>) -> Effects {
        self.state = UnlinkState::Done;
        self.output = Some(result);
        smallvec![]
    }

    fn linked(&self, event: Event) -> Result<(), LinkLoginError> {
        let values = batch_values(event)?;
        let user = values
            .first()
            .and_then(Option::as_ref)
            .map(|bytes| User::from_bytes(bytes))
            .transpose()?;
        let alias = self.config.alias;
        if !user.is_some_and(|user| user.alias_user_ids.contains(&alias))
            || alias.realm_id == self.config.user_id.realm_id
        {
            return Err(LinkLoginError::NotLinked);
        }
        Ok(())
    }

    fn cutoff(&mut self) -> Effects {
        let config = &self.config;
        // The grace also cuts off sessions another node mints before the cutoff reaches it.
        let cutoff = config.now.saturating_add(REVOCATION_GRACE_SECS);
        let mut revoke = Box::new(RevokeTokenOperation::new(RevokeTokenConfig {
            actor: config.actor.clone(),
            token_hash: user_cutoff_hash(&config.alias),
            expires_at: user_cutoff_expiry(cutoff),
            token_owner: config.alias,
            admission: RevokeTokenAdmission::Privileged,
            now: config.now,
        }));
        let effects = revoke.start();
        self.state = UnlinkState::Cutoff(revoke);
        effects
    }

    fn update(&mut self) -> Effects {
        let config = &self.config;
        let mut update = Box::new(AliasUpdate::new(UpdateUserInput {
            actor: config.actor.clone(),
            auth_context: config.auth_context.clone(),
            self_realm_id: config.actor.realm_id,
            user_id: config.user_id.to_string(),
            name: None,
            set_attributes: Default::default(),
            remove_attributes: Vec::new(),
            system: true,
            alias: Some(AliasChange::Remove(config.alias)),
        }));
        let effects = update.update.start();
        self.state = UnlinkState::Update(update);
        effects
    }
}

impl Operation for UnlinkLoginOperation {
    type Output = User;
    type Error = LinkLoginError;

    fn start(&mut self) -> Effects {
        let config = &self.config;
        let caller = config.auth_context.user_id;
        if caller.realm_id != config.actor.realm_id
            || config.user_id.realm_id != config.actor.realm_id
            || config.auth_context.path_restrictions.is_some()
        {
            return self.finish(Err(LinkLoginError::Unauthorized));
        }
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![user_read(&config.user_id)],
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match std::mem::replace(&mut self.state, UnlinkState::Done) {
            UnlinkState::Read => {
                if let Err(error) = self.linked(event) {
                    return self.finish(Err(error));
                }
                if self.config.auth_context.user_id == self.config.user_id {
                    return self.cutoff();
                }
                let realm_id = self.config.actor.realm_id;
                self.state = UnlinkState::Authorize;
                smallvec![Effect::SubOperation(boxed_suboperation(
                    CheckPermissionsOperation::new(CheckPermissionsConfig {
                        auth_context: self.config.auth_context.clone(),
                        path: format!("/{realm_id}/admin/u/{}", self.config.user_id),
                        required_permission: Permission::WRITE,
                    }),
                    |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                        allowed
                    }),
                ))]
            }
            UnlinkState::Authorize => match event {
                Event::SubOperation(SubOperationEvent::AuthorizationResult {
                    allowed: Ok(true),
                }) => self.cutoff(),
                Event::SubOperation(SubOperationEvent::AuthorizationResult {
                    allowed: Err(error),
                }) => self.finish(Err(error.into())),
                _ => self.finish(Err(LinkLoginError::Unauthorized)),
            },
            UnlinkState::Cutoff(mut revoke) => {
                let effects = revoke.step(event);
                if !revoke.is_complete() {
                    self.state = UnlinkState::Cutoff(revoke);
                    return effects;
                }
                match revoke.finalize() {
                    Ok(_) => {
                        let mut next = effects;
                        next.extend(self.update());
                        next
                    }
                    Err(error) => {
                        self.finish(Err(error.into()));
                        effects
                    }
                }
            }
            UnlinkState::Update(mut update) => match update.step(event) {
                (effects, None) => {
                    self.state = UnlinkState::Update(update);
                    effects
                }
                (effects, Some(result)) => {
                    self.finish(result);
                    effects
                }
            },
            UnlinkState::Done => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(LinkLoginError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match &mut self.state {
            UnlinkState::Cutoff(revoke) => revoke.abort(),
            UnlinkState::Update(update) => update.update.abort(),
            _ => smallvec![],
        }
    }
}
