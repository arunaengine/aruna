//! Native federated login: signs a handoff for a local portal user, and turns an admitted
//! handoff of another realm into a federated session here.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::federation::{FederationError, RealmDescriptor, Signed};
use aruna_core::handoff::{FEDERATED_SESSION_SECS, HandoffError, LoginHandoff, check_handoff};
use aruna_core::keyspaces::{FEDERATION_KEYSPACE, USER_KEYSPACE};
use aruna_core::link::{alias_claims_key, alias_owner};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, NodeCapabilities, SessionKind};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::user::User;
use aruna_core::types::Effects;
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use crate::realm::get_config::{GetConfigError, GetConfigOperation};
use crate::session::{
    CreateSessionConfig, CreateSessionError, CreateSessionOperation, CreatedSession,
};
use crate::users::read_document::{ReadUserError, ReadUserOperation};

#[derive(Clone, Debug, PartialEq)]
pub struct IssueHandoffConfig {
    pub auth_context: AuthContext,
    /// The audience realm's signed descriptor.
    pub descriptor: Signed<RealmDescriptor>,
    /// Lowercase hex SHA-256 of the audience portal's browser secret.
    pub nonce: String,
    pub node_capabilities: NodeCapabilities,
    pub now: u64,
}

#[derive(Debug, Error, PartialEq)]
pub enum IssueHandoffError {
    #[error("only an unrestricted portal session of a local user may log in elsewhere")]
    Refused,
    #[error("the user is deactivated")]
    Deactivated,
    #[error(transparent)]
    Invalid(#[from] HandoffError),
    #[error(transparent)]
    Read(#[from] ReadUserError),
    #[error(transparent)]
    Sign(#[from] FederationError),
    #[error("handoff operation did not finish")]
    NotFinished,
    #[error("unexpected event for the handoff operation state")]
    UnexpectedEvent,
}

/// Signs a login handoff for the caller after the local user, session and status checks.
#[derive(Debug, PartialEq)]
pub struct IssueHandoffOperation {
    config: IssueHandoffConfig,
    read: Option<ReadUserOperation>,
    output: Option<Result<Signed<LoginHandoff>, IssueHandoffError>>,
}

impl IssueHandoffOperation {
    pub fn new(config: IssueHandoffConfig) -> Self {
        Self {
            config,
            read: None,
            output: None,
        }
    }

    fn sign(
        &self,
        read: Result<aruna_core::structs::identity::user::User, ReadUserError>,
    ) -> Result<Signed<LoginHandoff>, IssueHandoffError> {
        let name = match read {
            Ok(user) if user.is_deactivated() => return Err(IssueHandoffError::Deactivated),
            Ok(user) => Some(user.name).filter(|name| !name.is_empty()),
            Err(ReadUserError::NotFound) => None,
            Err(error) => return Err(error.into()),
        };
        let config = &self.config;
        let handoff = LoginHandoff::new(
            config.auth_context.realm_id,
            config.auth_context.user_id,
            &config.descriptor,
            config.nonce.clone(),
            name,
            config.now,
        )?;
        Ok(Signed::sign(handoff, &config.node_capabilities)?)
    }
}

impl Operation for IssueHandoffOperation {
    type Output = Signed<LoginHandoff>;
    type Error = IssueHandoffError;

    fn start(&mut self) -> Effects {
        let auth = &self.config.auth_context;
        let portal = auth.session.as_ref().map(|session| session.kind) == Some(SessionKind::Portal);
        if auth.user_id.realm_id != auth.realm_id || auth.path_restrictions.is_some() || !portal {
            self.output = Some(Err(IssueHandoffError::Refused));
            return smallvec![];
        }
        let mut read = ReadUserOperation::new(auth.user_id);
        let effects = read.start();
        self.read = Some(read);
        effects
    }

    fn step(&mut self, event: Event) -> Effects {
        // Only the user read of `start` answers; before it and once decided, events are refused.
        let Some(read) = self.read.as_mut() else {
            self.output = Some(Err(IssueHandoffError::UnexpectedEvent));
            return smallvec![];
        };
        let effects = read.step(event);
        if read.is_complete()
            && let Some(read) = self.read.take()
        {
            self.output = Some(self.sign(read.finalize()));
        }
        effects
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(IssueHandoffError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        self.read.as_mut().map(Operation::abort).unwrap_or_default()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FederatedLoginConfig {
    pub realm_id: RealmId,
    pub handoff: Signed<LoginHandoff>,
    /// The 32 byte browser secret whose SHA-256 is the handoff nonce.
    pub secret: Vec<u8>,
    pub node_capabilities: NodeCapabilities,
    pub now: u64,
}

#[derive(Debug, Error, PartialEq)]
pub enum FederatedLoginError {
    #[error(transparent)]
    Config(#[from] GetConfigError),
    #[error("this realm has no federation settings")]
    Disabled,
    #[error(transparent)]
    Rejected(#[from] HandoffError),
    #[error("this login was cut off here a moment ago; try again in a few minutes")]
    CutOff,
    #[error(transparent)]
    Session(#[from] CreateSessionError),
    #[error(transparent)]
    Storage(#[from] aruna_core::errors::StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("federated login did not finish")]
    NotFinished,
    #[error("unexpected event for the federated login state")]
    UnexpectedEvent,
}

#[derive(Debug, PartialEq)]
enum FederatedLoginState {
    Start,
    ReadConfig(GetConfigOperation),
    ReadClaims,
    ReadOwner(UserId),
    Session(CreateSessionOperation),
    Done,
}

/// Admits a login handoff against the current settings and opens an 8 hour federated session,
/// for the local account the login is linked to when that link resolves unambiguously.
#[derive(Debug, PartialEq)]
pub struct FederatedLoginOperation {
    config: FederatedLoginConfig,
    state: FederatedLoginState,
    /// The realm config read at admission, for the cutoff checks of the session ids.
    realm: Option<RealmConfigDocument>,
    output: Option<Result<CreatedSession, FederatedLoginError>>,
}

impl FederatedLoginOperation {
    pub fn new(config: FederatedLoginConfig) -> Self {
        Self {
            config,
            state: FederatedLoginState::Start,
            realm: None,
            output: None,
        }
    }

    fn admit(
        &mut self,
        read: Result<RealmConfigDocument, GetConfigError>,
    ) -> Result<(), FederatedLoginError> {
        let realm = read?;
        let settings = realm
            .federation
            .as_ref()
            .ok_or(FederatedLoginError::Disabled)?;
        let config = &self.config;
        check_handoff(
            &config.handoff,
            &config.realm_id,
            settings,
            &config.secret,
            config.now,
        )?;
        self.realm = Some(realm);
        if self.cut_off(&self.config.handoff.payload.user) {
            return Err(FederatedLoginError::CutOff);
        }
        Ok(())
    }

    /// Whether token validation would refuse a session of `user` issued now.
    fn cut_off(&self, user: &UserId) -> bool {
        let now = self.config.now;
        self.realm
            .as_ref()
            .and_then(|realm| realm.user_cutoff(user, now))
            .is_some_and(|cutoff| now < cutoff)
    }

    fn session(&mut self, user_id: UserId, via: Option<UserId>) -> Effects {
        let config = &self.config;
        let handoff = &config.handoff.payload;
        let mut session = CreateSessionOperation::new(CreateSessionConfig {
            time: config.now,
            expiry: config.now.saturating_add(FEDERATED_SESSION_SECS),
            user_id,
            realm_id: config.realm_id,
            node_capabilities: config.node_capabilities.clone(),
            kind: SessionKind::Federated,
            label: None,
            name: handoff.name.clone(),
            restrictions: None,
            via,
            auth_time: None,
        });
        let effects = session.start();
        self.state = FederatedLoginState::Session(session);
        effects
    }

    fn read(&mut self, key_space: &str, key: Vec<u8>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    /// The linked account and its stored bytes from a storage read, or the error it carries.
    fn read_value(event: Event) -> Result<Option<ByteView>, FederatedLoginError> {
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            _ => Err(FederatedLoginError::NotFinished),
        }
    }

    fn claims_read(&mut self, event: Event) -> Effects {
        let foreign = self.config.handoff.payload.user;
        let owner = Self::read_value(event).and_then(|value| {
            let claims = value
                .map(|bytes| postcard::from_bytes::<BTreeSet<UserId>>(&bytes))
                .transpose()
                .map_err(ConversionError::from)?
                .unwrap_or_default();
            Ok(alias_owner(&claims))
        });
        match owner {
            Ok(Some(owner)) => {
                self.state = FederatedLoginState::ReadOwner(owner);
                self.read(USER_KEYSPACE, owner.to_bytes())
            }
            Ok(None) => self.session(foreign, None),
            Err(error) => self.finish(Err(error)),
        }
    }

    /// A linked login opens a session of an active, non-service account that still links it.
    fn owner_read(&mut self, event: Event, owner: UserId) -> Effects {
        let foreign = self.config.handoff.payload.user;
        let user = Self::read_value(event).and_then(|value| {
            value
                .map(|bytes| User::from_bytes(&bytes).map_err(FederatedLoginError::from))
                .transpose()
        });
        match user {
            Ok(Some(user))
                if !user.is_deactivated()
                    && user.service_group().is_none()
                    && user.linked_login(&foreign) =>
            {
                if self.cut_off(&owner) {
                    return self.finish(Err(FederatedLoginError::CutOff));
                }
                self.session(owner, Some(foreign))
            }
            Ok(_) => self.session(foreign, None),
            Err(error) => self.finish(Err(error)),
        }
    }

    fn finish(&mut self, result: Result<CreatedSession, FederatedLoginError>) -> Effects {
        self.state = FederatedLoginState::Done;
        self.output = Some(result);
        smallvec![]
    }
}

impl Operation for FederatedLoginOperation {
    type Output = CreatedSession;
    type Error = FederatedLoginError;

    fn start(&mut self) -> Effects {
        if self.state != FederatedLoginState::Start {
            return smallvec![];
        }
        let mut read = GetConfigOperation::new(self.config.realm_id);
        let effects = read.start();
        self.state = FederatedLoginState::ReadConfig(read);
        effects
    }

    fn step(&mut self, event: Event) -> Effects {
        match std::mem::replace(&mut self.state, FederatedLoginState::Done) {
            FederatedLoginState::ReadConfig(mut read) => {
                let effects = read.step(event);
                if !read.is_complete() {
                    self.state = FederatedLoginState::ReadConfig(read);
                    return effects;
                }
                match self.admit(read.finalize()) {
                    Ok(()) => {
                        self.state = FederatedLoginState::ReadClaims;
                        let key = alias_claims_key(&self.config.handoff.payload.user);
                        self.read(FEDERATION_KEYSPACE, key)
                    }
                    Err(error) => self.finish(Err(error)),
                }
            }
            FederatedLoginState::ReadClaims => self.claims_read(event),
            FederatedLoginState::ReadOwner(owner) => self.owner_read(event, owner),
            FederatedLoginState::Session(mut session) => {
                let effects = session.step(event);
                if !session.is_complete() {
                    self.state = FederatedLoginState::Session(session);
                    return effects;
                }
                self.finish(session.finalize().map_err(Into::into));
                effects
            }
            FederatedLoginState::Start | FederatedLoginState::Done => {
                self.finish(Err(FederatedLoginError::UnexpectedEvent))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(FederatedLoginError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match &mut self.state {
            FederatedLoginState::ReadConfig(read) => read.abort(),
            FederatedLoginState::Session(session) => session.abort(),
            _ => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::events::StorageEvent;
    use aruna_core::structs::identity::auth::{Actor, SessionRef};
    use aruna_core::structs::identity::realm::RealmConfigDocument;
    use aruna_core::structs::identity::user::User;
    use aruna_core::user::validation::DEACTIVATED_ATTRIBUTE;
    use byteview::ByteView;
    use ed25519_dalek::SigningKey;
    use ulid::Ulid;
    use url::Url;

    fn capabilities() -> NodeCapabilities {
        NodeCapabilities::management_node(SigningKey::from_bytes(&[4; 32])).unwrap()
    }

    fn realm_id() -> RealmId {
        RealmId::from_bytes(SigningKey::from_bytes(&[4; 32]).verifying_key().to_bytes())
    }

    fn actor() -> Actor {
        Actor {
            node_id: iroh::SecretKey::from_bytes(&[5; 32]).public(),
            user_id: UserId::local(Ulid::from_bytes([1; 16]), realm_id()),
            realm_id: realm_id(),
        }
    }

    fn read_result(value: Vec<u8>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(Vec::new()),
            value: Some(ByteView::from(value)),
        })
    }

    fn issue(kind: SessionKind) -> IssueHandoffOperation {
        let descriptor = RealmDescriptor {
            realm_id: RealmId::from_bytes([9; 32]),
            name: "B".to_string(),
            description: String::new(),
            api_url: Url::parse("https://b.example.org").unwrap(),
            portal_url: Url::parse("https://b.example.org").unwrap(),
            issued_at: 1,
        };
        IssueHandoffOperation::new(IssueHandoffConfig {
            auth_context: AuthContext {
                user_id: actor().user_id,
                realm_id: realm_id(),
                path_restrictions: None,
                session: Some(SessionRef {
                    sid: "s".to_string(),
                    kind,
                    name: None,
                    via: None,
                }),
            },
            descriptor: Signed::sign(descriptor, &capabilities()).unwrap(),
            nonce: "00".repeat(32),
            node_capabilities: capabilities(),
            now: 10,
        })
    }

    #[test]
    fn federated_session_refused() {
        // A federated session cannot chain a login to a third realm; nothing is read.
        let mut operation = issue(SessionKind::Federated);
        assert!(operation.start().is_empty());
        assert_eq!(operation.finalize(), Err(IssueHandoffError::Refused));
    }

    #[test]
    fn deactivated_user_refused() {
        let mut operation = issue(SessionKind::Portal);
        assert_eq!(operation.start().len(), 1);
        let user = User {
            user_id: actor().user_id,
            name: "Ada".to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: [(DEACTIVATED_ATTRIBUTE.to_string(), "true".to_string())].into(),
        };
        operation.step(read_result(user.to_bytes(&actor()).unwrap()));
        assert_eq!(operation.finalize(), Err(IssueHandoffError::Deactivated));
    }

    fn login() -> FederatedLoginOperation {
        let handoff_user = UserId::new(Ulid::from_bytes([2; 16]), RealmId::from_bytes([9; 32]));
        let handoff = LoginHandoff {
            issuer: handoff_user.realm_id,
            user: handoff_user,
            audience: realm_id(),
            descriptor_digest: String::new(),
            nonce: String::new(),
            name: None,
            issued_at: 10,
            expires_at: 70,
            handoff_id: Ulid::from_bytes([3; 16]),
        };
        FederatedLoginOperation::new(FederatedLoginConfig {
            realm_id: realm_id(),
            handoff: Signed::sign(handoff, &capabilities()).unwrap(),
            secret: vec![4; 32],
            node_capabilities: capabilities(),
            now: 10,
        })
    }

    #[test]
    fn missing_settings_refused() {
        // Without federation settings no session is written.
        let mut operation = login();
        assert_eq!(operation.start().len(), 1);
        let config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        let effects = operation.step(read_result(config.to_bytes(&actor()).unwrap()));
        assert!(effects.is_empty());
        assert_eq!(operation.finalize(), Err(FederatedLoginError::Disabled));
    }

    #[test]
    fn stray_events_rejected() {
        // Events before `start` and after the decision are refused, never read as answers.
        let config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        let answer = || read_result(config.to_bytes(&actor()).unwrap());
        let mut early = login();
        assert!(early.step(answer()).is_empty());
        assert_eq!(early.finalize(), Err(FederatedLoginError::UnexpectedEvent));
        let mut late = login();
        late.start();
        late.step(answer());
        assert!(late.step(answer()).is_empty());
        assert_eq!(late.finalize(), Err(FederatedLoginError::UnexpectedEvent));

        let mut early = issue(SessionKind::Portal);
        early.step(answer());
        assert_eq!(early.finalize(), Err(IssueHandoffError::UnexpectedEvent));
        let mut late = issue(SessionKind::Federated);
        late.start();
        late.step(answer());
        assert_eq!(late.finalize(), Err(IssueHandoffError::UnexpectedEvent));
    }
}
