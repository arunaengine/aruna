//! Linked logins: the confirmation that binds a fresh local login and a fresh login of another
//! realm, the link it allows, and the unlink that cuts the linked login off first.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::auth::{REVOCATION_GRACE_SECS, user_cutoff_expiry, user_cutoff_hash};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::federation::{FederationError, Signed};
use aruna_core::handoff::{HandoffError, LoginHandoff, check_handoff};
use aruna_core::keyspaces::USER_KEYSPACE;
use aruna_core::link::{
    LinkAction, LinkConfirmation, LinkError, MAX_LINK_SECS, check_confirmation, fresh_local,
    link_secret,
};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::user::User;
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::revoke_token::{
    RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenError, RevokeTokenOperation,
};
use crate::users::update_user::{
    AliasChange, UpdateUserError, UpdateUserInput, UpdateUserOperation,
};

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
    #[error("unexpected event for the link operation state")]
    UnexpectedEvent,
}

impl From<IoError> for LinkLoginError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { .. } | IoError::Codec(_) => Self::UnexpectedEvent,
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished => Self::NotFinished,
        }
    }
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
    /// The id of the confirmation, minted by the caller.
    pub confirmation_id: Ulid,
}

/// Signs a short-lived confirmation for linking the foreign login to the caller's account.
#[derive(Debug, PartialEq)]
pub struct ConfirmLinkOperation {
    config: ConfirmLinkConfig,
}

impl ConfirmLinkOperation {
    pub fn new(config: ConfirmLinkConfig) -> Self {
        Self { config }
    }

    fn sign(&self, result: IoResult) -> Result<Signed<LinkConfirmation>, LinkLoginError> {
        let [user, realm] = values::<2>(result)?;
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
            confirmation_id: config.confirmation_id,
        };
        Ok(Signed::sign(confirmation, &config.node_capabilities)?)
    }
}

impl Operation for ConfirmLinkOperation {
    type Output = Signed<LinkConfirmation>;
    type Error = LinkLoginError;

    fn first(&mut self) -> Result<Next<Self::Output>, LinkLoginError> {
        let config = &self.config;
        if !fresh_local(&config.auth_context, config.auth_time, config.now) {
            return Err(LinkLoginError::LocalRefused);
        }
        let auth = &config.auth_context;
        let reads = vec![user_read(&auth.user_id), config_read(auth)];
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, LinkLoginError> {
        self.sign(result).map(Next::Done)
    }
}

/// The link refusal a failed alias change stands for.
fn link_error(error: UpdateUserError) -> LinkLoginError {
    match error {
        UpdateUserError::AliasInactive => LinkLoginError::LocalRefused,
        UpdateUserError::AliasNotAdmitted => LinkLoginError::ForeignRefused,
        UpdateUserError::AliasCutOff => LinkLoginError::CutOff,
        error => error.into(),
    }
}

/// Whether a link change lost a transaction conflict that a fresh run may avoid.
fn lost_conflict(error: &LinkLoginError) -> bool {
    matches!(
        error,
        LinkLoginError::Update(UpdateUserError::StorageError(
            StorageError::TransactionConflict
        )) | LinkLoginError::Storage(StorageError::TransactionConflict)
    )
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

/// Links the confirmed foreign login to the caller's account through the replicated user
/// document; the account, the realm's admission and the foreign cutoff are rechecked inside
/// every transaction attempt.
#[derive(Debug, PartialEq)]
pub struct LinkLoginOperation {
    config: LinkLoginConfig,
}

impl LinkLoginOperation {
    pub fn new(config: LinkLoginConfig) -> Self {
        Self { config }
    }

    fn input(&self) -> UpdateUserInput {
        let config = &self.config;
        let payload = &config.confirmation.payload;
        UpdateUserInput {
            actor: config.actor.clone(),
            auth_context: config.auth_context.clone(),
            self_realm_id: config.auth_context.realm_id,
            user_id: config.auth_context.user_id.to_string(),
            name: None,
            set_attributes: Default::default(),
            remove_attributes: Vec::new(),
            system: false,
            alias: Some(AliasChange::Add {
                alias: payload.foreign_user,
                issued_at: payload.foreign_issued_at,
                now: config.now,
            }),
            subject: None,
        }
    }
}

type LinkTurn = Result<Next<User>, LinkLoginError>;

impl Operation for LinkLoginOperation {
    type Output = User;
    type Error = LinkLoginError;

    fn first(&mut self) -> LinkTurn {
        let config = &self.config;
        let auth = &config.auth_context;
        if !fresh_local(auth, config.auth_time, config.now) {
            return Err(LinkLoginError::LocalRefused);
        }
        check_confirmation(
            &config.confirmation,
            &auth.realm_id,
            &auth.user_id,
            config.now,
        )?;
        let update = UpdateUserOperation::new(self.input());
        Ok(Next::Call(IoCall::child(update)))
    }

    fn next(&mut self, result: IoResult) -> LinkTurn {
        // A lost commit conflict ends the run; the runner reruns it so ownership is reread.
        let user = result.child::<User, UpdateUserError>()?.map_err(link_error)?;
        Ok(Next::Done(user))
    }

    fn is_conflict(error: &LinkLoginError) -> bool {
        lost_conflict(error)
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
enum UnlinkPhase {
    Read,
    Authorize,
    Cutoff,
    Update,
}

/// Removes a linked login: the owner or a realm administrator first records the credential
/// cutoff of the foreign login, then removes the alias through the replicated user document.
#[derive(Debug, PartialEq)]
pub struct UnlinkLoginOperation {
    config: UnlinkLoginConfig,
    phase: UnlinkPhase,
}

impl UnlinkLoginOperation {
    pub fn new(config: UnlinkLoginConfig) -> Self {
        Self {
            config,
            phase: UnlinkPhase::Read,
        }
    }

    fn linked(&self, result: IoResult) -> Result<(), LinkLoginError> {
        let [user] = values::<1>(result)?;
        let user = user.map(|bytes| User::from_bytes(&bytes)).transpose()?;
        let alias = self.config.alias;
        if !user.is_some_and(|user| user.alias_user_ids.contains(&alias))
            || alias.realm_id == self.config.user_id.realm_id
        {
            return Err(LinkLoginError::NotLinked);
        }
        Ok(())
    }

    fn cutoff(&mut self) -> LinkTurn {
        let config = &self.config;
        // The grace also cuts off sessions another node mints before the cutoff reaches it.
        let cutoff = config.now.saturating_add(REVOCATION_GRACE_SECS);
        let revoke = RevokeTokenOperation::new(RevokeTokenConfig {
            actor: config.actor.clone(),
            token_hash: user_cutoff_hash(&config.alias),
            expires_at: user_cutoff_expiry(cutoff),
            token_owner: config.alias,
            admission: RevokeTokenAdmission::Privileged,
            now: config.now,
        });
        self.phase = UnlinkPhase::Cutoff;
        Ok(Next::Call(IoCall::child(revoke)))
    }

    fn update(&mut self) -> LinkTurn {
        let config = &self.config;
        let update = UpdateUserOperation::new(UpdateUserInput {
            actor: config.actor.clone(),
            auth_context: config.auth_context.clone(),
            self_realm_id: config.actor.realm_id,
            user_id: config.user_id.to_string(),
            name: None,
            set_attributes: Default::default(),
            remove_attributes: Vec::new(),
            system: true,
            alias: Some(AliasChange::Remove(config.alias)),
            subject: None,
        });
        self.phase = UnlinkPhase::Update;
        Ok(Next::Call(IoCall::child(update)))
    }
}

impl Operation for UnlinkLoginOperation {
    type Output = User;
    type Error = LinkLoginError;

    fn first(&mut self) -> LinkTurn {
        let config = &self.config;
        let caller = config.auth_context.user_id;
        if caller.realm_id != config.actor.realm_id
            || config.user_id.realm_id != config.actor.realm_id
            || config.auth_context.path_restrictions.is_some()
        {
            return Err(LinkLoginError::Unauthorized);
        }
        Ok(Next::Call(IoCall::Read(vec![user_read(&config.user_id)])))
    }

    fn next(&mut self, result: IoResult) -> LinkTurn {
        match self.phase {
            UnlinkPhase::Read => {
                self.linked(result)?;
                if self.config.auth_context.user_id == self.config.user_id {
                    return self.cutoff();
                }
                let realm_id = self.config.actor.realm_id;
                self.phase = UnlinkPhase::Authorize;
                let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
                    auth_context: self.config.auth_context.clone(),
                    path: format!("/{realm_id}/admin/u/{}", self.config.user_id),
                    required_permission: Permission::WRITE,
                });
                Ok(Next::Call(IoCall::child(check)))
            }
            UnlinkPhase::Authorize => match result.child::<bool, AuthorizationError>()? {
                Ok(true) => self.cutoff(),
                Ok(false) => Err(LinkLoginError::Unauthorized),
                Err(error) => Err(error.into()),
            },
            UnlinkPhase::Cutoff => {
                result.child::<RealmConfigDocument, RevokeTokenError>()??;
                self.update()
            }
            UnlinkPhase::Update => {
                // A lost commit conflict ends the run; the runner reruns it so ownership is reread.
                let user = result.child::<User, UpdateUserError>()?.map_err(link_error)?;
                Ok(Next::Done(user))
            }
        }
    }

    fn is_conflict(error: &LinkLoginError) -> bool {
        lost_conflict(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::EffectOperation;
    use aruna_core::types::Effects;
    use smallvec::smallvec;
    use aruna_core::federation::{
        AcceptedRealms, FederationSettings, RealmDescriptor, RegistrationMode,
    };
    use aruna_core::handoff::secret_nonce;
    use aruna_core::structs::identity::auth::{SessionKind, SessionRef};
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
            contact: None,
            login_providers: Vec::new(),
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
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
            description: String::new(),
            contact: None,
            login_providers: Vec::new(),
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
            confirmation_id: Ulid::from_parts(now, 1),
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
            assert_eq!(stale.first(), Err(LinkLoginError::LocalRefused));
            let mut linking = link(local(2), confirmation(now), now);
            linking.config.auth_time = auth_time;
            assert_eq!(linking.first(), Err(LinkLoginError::LocalRefused));
        }
        let mut api = confirm(local(2), Some(now), now);
        api.config.auth_context = session(local(2), SessionKind::Api);
        assert_eq!(api.first(), Err(LinkLoginError::LocalRefused));
    }

    #[test]
    fn unlink_needs_admin() {
        // Another caller unlinks only with write access to the account; a refusal ends the run.
        let now = 10_000;
        let unlink = || {
            UnlinkLoginOperation::new(UnlinkLoginConfig {
                actor: actor(local(3)),
                auth_context: session(local(3), SessionKind::Portal),
                user_id: local(2),
                alias: foreign(),
                now,
            })
        };
        let answer = |allowed: Result<bool, AuthorizationError>| {
            let mut operation = unlink();
            operation.phase = UnlinkPhase::Authorize;
            operation.next(IoResult::Child(ChildOutcome::new(allowed)))
        };
        assert_eq!(answer(Ok(false)), Err(LinkLoginError::Unauthorized));
        let allowed = answer(Ok(true));
        assert!(matches!(allowed, Ok(Next::Call(IoCall::Child(_)))));
        let mut unread = unlink();
        assert_eq!(
            unread.first(),
            Ok(Next::Call(IoCall::Read(vec![user_read(&local(2))])))
        );
        assert_eq!(
            unread.next(IoResult::Values(vec![None])),
            Err(LinkLoginError::NotLinked)
        );
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

    /// The realm config with a cutoff of the foreign login after `now`.
    async fn cutoff_config(context: &DriverContext, now: u64) -> (String, Vec<u8>, Vec<u8>) {
        let target = DocumentTarget::RealmConfig { realm_id: realm() };
        let key = target.storage_key().to_vec();
        let keyspace = target.storage_keyspace().to_string();
        let row = crate::jobs::key_wake::read_row(&context.storage_handle, &keyspace, key.clone());
        let mut config = RealmConfigDocument::from_bytes(&row.await.unwrap().unwrap()).unwrap();
        config
            .revoked_tokens
            .push(aruna_core::structs::identity::realm::TokenRevocation {
                token_hash: user_cutoff_hash(&foreign()),
                expires_at: user_cutoff_expiry(now + 1),
            });
        (keyspace, key, config.to_bytes(&actor(local(2))).unwrap())
    }

    #[tokio::test]
    async fn cutoff_voids_confirmation() {
        // A cutoff of the foreign login after its session voids an unused or replayed confirmation.
        let (_dir, context) = context().await;
        let now = unix_timestamp_secs();
        let confirmation = drive(confirm(local(2), Some(now), now), &context)
            .await
            .unwrap();
        let (keyspace, key, bytes) = cutoff_config(&context, now).await;
        put(&context, &keyspace, key, bytes).await;
        let linked = drive(link(local(2), confirmation, now), &context).await;
        assert_eq!(linked, Err(LinkLoginError::CutOff));
    }

    /// Commits `write` outside the link's user update right after its first read inside it.
    #[derive(Debug, PartialEq)]
    struct Racing {
        update: UpdateUserOperation,
        write: Option<StorageEffect>,
        held: Option<Event>,
    }

    impl EffectOperation for Racing {
        type Output = User;
        type Error = UpdateUserError;

        fn start(&mut self) -> Effects {
            self.update.start()
        }

        fn step(&mut self, event: Event) -> Effects {
            if let Some(held) = self.held.take() {
                return self.update.step(held);
            }
            if matches!(event, Event::Storage(StorageEvent::BatchReadResult { .. }))
                && let Some(write) = self.write.take()
            {
                self.held = Some(event);
                return smallvec![Effect::Storage(write)];
            }
            self.update.step(event)
        }

        fn is_complete(&self) -> bool {
            self.update.is_complete()
        }

        fn finalize(self) -> Result<User, UpdateUserError> {
            self.update.finalize()
        }

        fn abort(&mut self) -> Effects {
            self.update.abort()
        }
    }

    #[tokio::test]
    async fn racing_cutoff_wins() {
        // A cutoff committed while the link's transaction is open is seen on its retry.
        let (_dir, context) = context().await;
        let now = unix_timestamp_secs();
        let confirmation = drive(confirm(local(2), Some(now), now), &context)
            .await
            .unwrap();
        let (key_space, key, value) = cutoff_config(&context, now).await;
        let racing = Racing {
            update: UpdateUserOperation::new(link(local(2), confirmation, now).input()),
            write: Some(StorageEffect::Write {
                key_space,
                key: key.into(),
                value: value.into(),
                txn_id: None,
            }),
            held: None,
        };
        let linked = drive(racing, &context).await.map_err(link_error);
        assert_eq!(linked, Err(LinkLoginError::CutOff));
        let row = crate::jobs::key_wake::read_row(
            &context.storage_handle,
            USER_KEYSPACE,
            local(2).to_bytes(),
        );
        let user = User::from_bytes(&row.await.unwrap().unwrap()).unwrap();
        assert!(!user.alias_user_ids.contains(&foreign()));
    }

    #[tokio::test]
    async fn unlink_cuts_first() {
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
