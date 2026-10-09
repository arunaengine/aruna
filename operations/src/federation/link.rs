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
use aruna_core::handoff::{HandoffError, LoginHandoff, check_handoff};
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::link::{
    LinkAction, LinkConfirmation, LinkError, MAX_LINK_SECS, check_confirmation, link_secret,
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
    Handoff(#[from] HandoffError),
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

/// An unrestricted portal session of a local account whose primary login was at most
/// `MAX_LINK_SECS` ago; renewals keep that time and child sessions have none.
fn fresh_local(auth: &AuthContext, auth_time: Option<u64>, now: u64) -> bool {
    let portal = auth.session.as_ref().map(|session| session.kind) == Some(SessionKind::Portal);
    portal
        && auth.path_restrictions.is_none()
        && auth.user_id.realm_id == auth.realm_id
        && !auth.user_id.is_nil()
        && auth_time.is_some_and(|login| login.saturating_add(MAX_LINK_SECS) >= now)
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
    /// Primary login time from the caller's token claims.
    pub auth_time: Option<u64>,
    /// A login handoff of the other realm made for this link attempt.
    pub handoff: Signed<LoginHandoff>,
    /// The browser secret whose `link_secret` with the caller's session the nonce hashes.
    pub secret: Vec<u8>,
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

    fn sign(&self, event: Event) -> Result<Signed<LinkConfirmation>, LinkLoginError> {
        let values = batch_values(event)?;
        let [user, realm] = values.as_slice() else {
            return Err(LinkLoginError::NotFinished);
        };
        usable_account(user.as_ref())?;
        let settings = read_config(realm.as_ref())?
            .federation
            .ok_or(LinkLoginError::ForeignRefused)?;
        let config = &self.config;
        let sid = config
            .auth_context
            .session
            .as_ref()
            .map(|session| session.sid.as_str());
        let secret = link_secret(sid.unwrap_or_default(), &config.secret);
        let realm_id = config.auth_context.realm_id;
        check_handoff(&config.handoff, &realm_id, &settings, &secret, config.now)?;
        let handoff = &config.handoff.payload;
        let confirmation = LinkConfirmation {
            realm_id,
            action: LinkAction::Link,
            local_user: config.auth_context.user_id,
            foreign_user: handoff.user,
            foreign_issued_at: handoff.issued_at,
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
        if !fresh_local(&config.auth_context, config.auth_time, config.now) {
            self.output = Some(Err(LinkLoginError::LocalRefused));
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
    /// Primary login time from the caller's token claims.
    pub auth_time: Option<u64>,
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
        if !fresh_local(auth, config.auth_time, config.now) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::federation::{
        AcceptedRealms, FederationSettings, RealmDescriptor, RegistrationMode,
    };
    use aruna_core::handoff::secret_nonce;
    use aruna_core::structs::identity::auth::SessionRef;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::time::unix_timestamp_secs;
    use ed25519_dalek::SigningKey;
    use tempfile::{TempDir, tempdir};
    use url::Url;

    fn key() -> SigningKey {
        SigningKey::from_bytes(&[21; 32])
    }

    fn realm() -> RealmId {
        RealmId::from_bytes(key().verifying_key().to_bytes())
    }

    fn capabilities() -> NodeCapabilities {
        NodeCapabilities::management_node(key()).unwrap()
    }

    fn local(seed: u8) -> UserId {
        UserId::local(Ulid::from_bytes([seed; 16]), realm())
    }

    const SECRET: &[u8] = b"link attempt secret";

    fn home_key() -> SigningKey {
        SigningKey::from_bytes(&[22; 32])
    }

    fn foreign() -> UserId {
        let home = RealmId::from_bytes(home_key().verifying_key().to_bytes());
        UserId::new(Ulid::from_bytes([1; 16]), home)
    }

    fn sid() -> String {
        Ulid::from_bytes([9; 16]).to_string()
    }

    fn descriptor() -> Signed<RealmDescriptor> {
        let descriptor = RealmDescriptor {
            realm_id: realm(),
            name: "B".to_string(),
            description: String::new(),
            api_url: Url::parse("https://b.example.org/api/v1").unwrap(),
            portal_url: Url::parse("https://b.example.org/").unwrap(),
            issued_at: 1,
        };
        Signed::sign(descriptor, &capabilities()).unwrap()
    }

    /// A handoff of the home realm whose nonce hashes `secret`.
    fn handoff(secret: &[u8], now: u64) -> Signed<LoginHandoff> {
        let nonce = secret_nonce(secret);
        let payload = LoginHandoff::new(
            foreign().realm_id,
            foreign(),
            &descriptor(),
            nonce,
            None,
            now,
        );
        let home = NodeCapabilities::management_node(home_key()).unwrap();
        Signed::sign(payload.unwrap(), &home).unwrap()
    }

    fn session(user_id: UserId, kind: SessionKind) -> AuthContext {
        AuthContext {
            user_id,
            realm_id: realm(),
            path_restrictions: None,
            session: Some(SessionRef {
                sid: sid(),
                kind,
                name: None,
                via: None,
            }),
        }
    }

    fn actor(user_id: UserId) -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[23; 32]).public(),
            user_id,
            realm_id: realm(),
        }
    }

    async fn put(context: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let effect = StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;
    }

    /// Two active local accounts and a realm that admits logins of every realm.
    async fn context() -> (TempDir, DriverContext) {
        let dir = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        for seed in [2, 3] {
            let user = User {
                user_id: local(seed),
                name: format!("user {seed}"),
                subject_ids: Vec::new(),
                alias_user_ids: Default::default(),
                attributes: Default::default(),
            };
            let bytes = user.to_bytes(&actor(local(seed))).unwrap();
            put(&context, USER_KEYSPACE, local(seed).to_bytes(), bytes).await;
        }
        let descriptor = descriptor();
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 3);
        config.federation = Some(FederationSettings {
            name: "B".to_string(),
            api_url: descriptor.payload.api_url.clone(),
            portal_url: descriptor.payload.portal_url.clone(),
            registry_url: None,
            registration: RegistrationMode::Enabled,
            accepted_realms: AcceptedRealms::Any,
            descriptor,
        });
        let target = DocumentTarget::RealmConfig { realm_id: realm() };
        let bytes = config.to_bytes(&actor(local(2))).unwrap();
        put(
            &context,
            target.storage_keyspace(),
            target.storage_key().to_vec(),
            bytes,
        )
        .await;
        (dir, context)
    }

    fn confirm(user: UserId, auth_time: Option<u64>, now: u64) -> ConfirmLinkOperation {
        ConfirmLinkOperation::new(ConfirmLinkConfig {
            auth_context: session(user, SessionKind::Portal),
            auth_time,
            handoff: handoff(&link_secret(&sid(), SECRET), now),
            secret: SECRET.to_vec(),
            node_capabilities: capabilities(),
            now,
        })
    }

    fn link(user: UserId, confirmation: Signed<LinkConfirmation>, now: u64) -> LinkLoginOperation {
        LinkLoginOperation::new(LinkLoginConfig {
            actor: actor(user),
            auth_context: session(user, SessionKind::Portal),
            auth_time: Some(now),
            confirmation,
            now,
        })
    }

    #[test]
    fn stale_logins_refused() {
        // An old primary login, a child token without one, or another kind never confirms.
        let now = 10_000;
        for auth_time in [Some(now - MAX_LINK_SECS - 1), None] {
            let mut stale = confirm(local(2), auth_time, now);
            assert!(stale.start().is_empty());
            assert_eq!(stale.finalize(), Err(LinkLoginError::LocalRefused));
            let mut linking = link(local(2), confirmation(now), now);
            linking.config.auth_time = auth_time;
            assert!(linking.start().is_empty());
            assert_eq!(linking.finalize(), Err(LinkLoginError::LocalRefused));
        }
        let mut api = confirm(local(2), Some(now), now);
        api.config.auth_context = session(local(2), SessionKind::Api);
        assert!(api.start().is_empty());
        assert_eq!(api.finalize(), Err(LinkLoginError::LocalRefused));
    }

    fn confirmation(now: u64) -> Signed<LinkConfirmation> {
        let payload = LinkConfirmation {
            realm_id: realm(),
            action: LinkAction::Link,
            local_user: local(2),
            foreign_user: foreign(),
            foreign_issued_at: now,
            issued_at: now,
            expires_at: now + MAX_LINK_SECS,
            confirmation_id: Ulid::from_bytes([8; 16]),
        };
        Signed::sign(payload, &capabilities()).unwrap()
    }

    #[tokio::test]
    async fn handoff_bound_attempt() {
        // Only a fresh handoff bound to this session's attempt confirms the foreign side.
        let (_dir, context) = context().await;
        let now = unix_timestamp_secs();
        let bound = drive(confirm(local(2), Some(now), now), &context).await;
        assert_eq!(bound.unwrap().payload.foreign_user, foreign());
        let other_sid = Ulid::from_bytes([10; 16]).to_string();
        for secret in [SECRET.to_vec(), link_secret(&other_sid, SECRET)] {
            let mut unbound = confirm(local(2), Some(now), now);
            unbound.config.handoff = handoff(&secret, now);
            let refused = drive(unbound, &context).await;
            assert_eq!(refused, Err(HandoffError::WrongSecret.into()));
        }
        let mut aged = confirm(local(2), Some(now), now);
        aged.config.handoff = handoff(&link_secret(&sid(), SECRET), now - MAX_LINK_SECS);
        let refused = drive(aged, &context).await;
        assert_eq!(refused, Err(HandoffError::BadLifetime.into()));
    }

    #[tokio::test]
    async fn link_owned_once() {
        // The confirmed login links once; another account and another caller are refused.
        let (_dir, context) = context().await;
        let now = unix_timestamp_secs();
        let confirmation = drive(confirm(local(2), Some(now), now), &context)
            .await
            .unwrap();
        // A confirmation is only good for the account it names.
        let stolen = drive(link(local(3), confirmation.clone(), now), &context).await;
        assert_eq!(
            stolen,
            Err(LinkLoginError::Confirmation(LinkError::Unbound))
        );
        let user = drive(link(local(2), confirmation.clone(), now), &context)
            .await
            .unwrap();
        assert!(user.alias_user_ids.contains(&foreign()));
        // Replaying the confirmation while linked changes nothing.
        let replay = drive(link(local(2), confirmation, now), &context).await;
        assert!(replay.is_ok());
        let other = drive(confirm(local(3), Some(now), now), &context)
            .await
            .unwrap();
        let claimed = drive(link(local(3), other, now), &context).await;
        assert_eq!(
            claimed,
            Err(LinkLoginError::Update(UpdateUserError::AliasClaimed))
        );
    }

    #[tokio::test]
    async fn cutoff_voids_confirmation() {
        // A cutoff of the foreign login after its session voids an unused or replayed confirmation.
        let (_dir, context) = context().await;
        let now = unix_timestamp_secs();
        let confirmation = drive(confirm(local(2), Some(now), now), &context)
            .await
            .unwrap();
        let target = DocumentTarget::RealmConfig { realm_id: realm() };
        let mut config = RealmConfigDocument::from_bytes(
            &crate::jobs::key_wake::read_row(
                &context.storage_handle,
                target.storage_keyspace(),
                target.storage_key().to_vec(),
            )
            .await
            .unwrap()
            .unwrap(),
        )
        .unwrap();
        config
            .revoked_tokens
            .push(aruna_core::structs::identity::realm::TokenRevocation {
                token_hash: user_cutoff_hash(&foreign()),
                expires_at: user_cutoff_expiry(now + 1),
            });
        let bytes = config.to_bytes(&actor(local(2))).unwrap();
        put(
            &context,
            target.storage_keyspace(),
            target.storage_key().to_vec(),
            bytes,
        )
        .await;
        let linked = drive(link(local(2), confirmation, now), &context).await;
        assert_eq!(linked, Err(LinkLoginError::CutOff));
    }

    #[tokio::test]
    async fn unlink_cuts_off_first() {
        // Only the owner unlinks here; the cutoff lands first and voids older confirmations.
        let (_dir, context) = context().await;
        let now = unix_timestamp_secs();
        let confirmation = drive(confirm(local(2), Some(now), now), &context)
            .await
            .unwrap();
        drive(link(local(2), confirmation.clone(), now), &context)
            .await
            .unwrap();
        let unlink = |caller: UserId| {
            UnlinkLoginOperation::new(UnlinkLoginConfig {
                actor: actor(caller),
                auth_context: session(caller, SessionKind::Portal),
                user_id: local(2),
                alias: foreign(),
                now,
            })
        };
        let stranger = drive(unlink(local(3)), &context).await;
        assert!(stranger.is_err());
        let user = drive(unlink(local(2)), &context).await.unwrap();
        assert!(!user.alias_user_ids.contains(&foreign()));
        let target = DocumentTarget::RealmConfig { realm_id: realm() };
        let row = crate::jobs::key_wake::read_row(
            &context.storage_handle,
            target.storage_keyspace(),
            target.storage_key().to_vec(),
        )
        .await
        .unwrap()
        .unwrap();
        let config = RealmConfigDocument::from_bytes(&row).unwrap();
        assert!(config.user_cutoff(&foreign(), now).is_some());
        let replay = drive(link(local(2), confirmation, now), &context).await;
        assert_eq!(replay, Err(LinkLoginError::CutOff));
    }
}
