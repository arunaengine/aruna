//! Native federated login: signs a handoff for a local portal user, and turns an admitted
//! handoff of another realm into a federated session here.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::federation::{FederationError, RealmDescriptor, Signed};
use aruna_core::handoff::{FEDERATED_SESSION_SECS, HandoffError, LoginHandoff, check_handoff};
use aruna_core::keyspaces::{FEDERATION_KEYSPACE, USER_KEYSPACE};
use aruna_core::link::{alias_claims_key, alias_owner};
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, values};
use aruna_core::structs::identity::auth::{AuthContext, NodeCapabilities, SessionKind};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::user::User;
use thiserror::Error;

use crate::document_repository::GetConfigError;
use crate::session::{
    CreateSessionConfig, CreateSessionError, CreateSessionOperation, CreatedSession,
};
use crate::users::read_document::ReadUserError;

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

impl From<IoError> for IssueHandoffError {
    fn from(error: IoError) -> Self {
        let read = match error {
            IoError::Conflict => ReadUserError::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => ReadUserError::StorageError(error),
            IoError::Unexpected { .. } | IoError::Codec(_) => return Self::UnexpectedEvent,
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished => {
                return Self::NotFinished;
            }
        };
        Self::Read(read)
    }
}

/// Signs a login handoff for the caller after the local user, session and status checks.
#[derive(Debug, PartialEq)]
pub struct IssueHandoffOperation {
    config: IssueHandoffConfig,
}

impl IssueHandoffOperation {
    pub fn new(config: IssueHandoffConfig) -> Self {
        Self { config }
    }

    /// Signs the handoff with the name of the read user; a missing user signs without one.
    fn sign(&self, user: Option<User>) -> Result<Signed<LoginHandoff>, IssueHandoffError> {
        let name = match user {
            Some(user) if user.is_deactivated() => return Err(IssueHandoffError::Deactivated),
            Some(user) => Some(user.name).filter(|name| !name.is_empty()),
            None => None,
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

    fn first(&mut self) -> Result<Next<Self::Output>, IssueHandoffError> {
        let auth = &self.config.auth_context;
        let portal = auth.session.as_ref().map(|session| session.kind) == Some(SessionKind::Portal);
        if auth.user_id.realm_id != auth.realm_id || auth.path_restrictions.is_some() || !portal {
            return Err(IssueHandoffError::Refused);
        }
        let user = DocumentTarget::User {
            user_id: auth.user_id,
        };
        Ok(Next::Call(read_row(
            user.storage_keyspace(),
            user.storage_key(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IssueHandoffError> {
        let [user] = values::<1>(result)?;
        let user = user
            .map(|bytes| User::from_bytes(&bytes))
            .transpose()
            .map_err(|error| IssueHandoffError::Read(error.into()))?;
        self.sign(user).map(Next::Done)
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

impl From<IoError> for FederatedLoginError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { .. } | IoError::Codec(_) => Self::UnexpectedEvent,
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished => Self::NotFinished,
        }
    }
}

#[derive(Debug, PartialEq)]
enum LoginPhase {
    Config,
    Claims,
    Owner(UserId),
    Session,
}

type LoginTurn = Result<Next<CreatedSession>, FederatedLoginError>;

/// Admits a login handoff against the current settings and opens an 8 hour federated session,
/// for the local account the login is linked to when that link resolves unambiguously.
#[derive(Debug, PartialEq)]
pub struct FederatedLoginOperation {
    config: FederatedLoginConfig,
    phase: LoginPhase,
    /// The realm config read at admission, for the cutoff checks of the session ids.
    realm: Option<RealmConfigDocument>,
}

impl FederatedLoginOperation {
    pub fn new(config: FederatedLoginConfig) -> Self {
        Self {
            config,
            phase: LoginPhase::Config,
            realm: None,
        }
    }

    fn admit(&mut self, realm: RealmConfigDocument) -> Result<(), FederatedLoginError> {
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

    fn session(&mut self, user_id: UserId, via: Option<UserId>) -> LoginTurn {
        let config = &self.config;
        let handoff = &config.handoff.payload;
        let session = CreateSessionOperation::new(CreateSessionConfig {
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
            cutoff: None,
        });
        self.phase = LoginPhase::Session;
        Ok(Next::Call(IoCall::child(session)))
    }

    /// The linked account named by the alias claims, or a session of the foreign login.
    fn claims_read(&mut self, result: IoResult) -> LoginTurn {
        let [value] = values::<1>(result)?;
        let claims = value
            .map(|bytes| postcard::from_bytes::<BTreeSet<UserId>>(&bytes))
            .transpose()
            .map_err(ConversionError::from)?
            .unwrap_or_default();
        match alias_owner(&claims) {
            Some(owner) => {
                self.phase = LoginPhase::Owner(owner);
                Ok(Next::Call(read_row(USER_KEYSPACE, owner.to_bytes())))
            }
            None => self.session(self.config.handoff.payload.user, None),
        }
    }

    /// A linked login opens a session of an active, non-service account that still links it.
    fn owner_read(&mut self, result: IoResult, owner: UserId) -> LoginTurn {
        let foreign = self.config.handoff.payload.user;
        let [value] = values::<1>(result)?;
        let user = value.map(|bytes| User::from_bytes(&bytes)).transpose()?;
        match user {
            Some(user)
                if !user.is_deactivated()
                    && user.service_group().is_none()
                    && user.linked_login(&foreign) =>
            {
                if self.cut_off(&owner) {
                    return Err(FederatedLoginError::CutOff);
                }
                self.session(owner, Some(foreign))
            }
            _ => self.session(foreign, None),
        }
    }
}

impl Operation for FederatedLoginOperation {
    type Output = CreatedSession;
    type Error = FederatedLoginError;

    fn first(&mut self) -> LoginTurn {
        let target = DocumentTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        Ok(Next::Call(read_row(
            target.storage_keyspace(),
            target.storage_key(),
        )))
    }

    fn next(&mut self, result: IoResult) -> LoginTurn {
        match std::mem::replace(&mut self.phase, LoginPhase::Session) {
            LoginPhase::Config => {
                let [value] = values::<1>(result)?;
                let bytes = value.ok_or(GetConfigError::DocumentNotFound)?;
                let realm =
                    RealmConfigDocument::from_bytes(&bytes).map_err(GetConfigError::from)?;
                self.admit(realm)?;
                self.phase = LoginPhase::Claims;
                let key = alias_claims_key(&self.config.handoff.payload.user);
                Ok(Next::Call(read_row(FEDERATION_KEYSPACE, key)))
            }
            LoginPhase::Claims => self.claims_read(result),
            LoginPhase::Owner(owner) => self.owner_read(result, owner),
            LoginPhase::Session => {
                let created = result.child::<CreatedSession, CreateSessionError>()??;
                Ok(Next::Done(created))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::auth::{Actor, SessionRef};
    use aruna_core::structs::identity::realm::RealmConfigDocument;
    use aruna_core::structs::identity::user::User;
    use aruna_core::user::validation::DEACTIVATED_ATTRIBUTE;
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

    fn read_result(value: Vec<u8>) -> IoResult {
        IoResult::Values(vec![Some(value.into())])
    }

    fn issue(kind: SessionKind) -> IssueHandoffOperation {
        let descriptor = RealmDescriptor {
            realm_id: RealmId::from_bytes([9; 32]),
            name: "B".to_string(),
            description: String::new(),
            api_url: Url::parse("https://b.example.org").unwrap(),
            portal_url: Url::parse("https://b.example.org").unwrap(),
            contact: None,
            login_providers: Vec::new(),
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
        assert_eq!(operation.first(), Err(IssueHandoffError::Refused));
    }

    #[test]
    fn deactivated_user_refused() {
        let mut operation = issue(SessionKind::Portal);
        assert!(operation.first().is_ok());
        let user = User {
            user_id: actor().user_id,
            name: "Ada".to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: [(DEACTIVATED_ATTRIBUTE.to_string(), "true".to_string())].into(),
        };
        let read = IoResult::Values(vec![Some(user.to_bytes(&actor()).unwrap().into())]);
        assert_eq!(operation.next(read), Err(IssueHandoffError::Deactivated));
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
        let target = DocumentTarget::RealmConfig {
            realm_id: realm_id(),
        };
        let read = read_row(target.storage_keyspace(), target.storage_key());
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        let config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        let answer = operation.next(read_result(config.to_bytes(&actor()).unwrap()));
        assert_eq!(answer, Err(FederatedLoginError::Disabled));
        let mut missing = login();
        let answer = missing.next(IoResult::Values(vec![None]));
        let not_found = FederatedLoginError::Config(GetConfigError::DocumentNotFound);
        assert_eq!(answer, Err(not_found));
    }

    #[test]
    fn unlinked_login_session() {
        // Without one owning account the foreign login gets its own session.
        let mut operation = login();
        operation.phase = LoginPhase::Claims;
        let answer = operation.next(IoResult::Values(vec![None]));
        assert!(matches!(answer, Ok(Next::Call(IoCall::Child(_)))));
        let refused = || CreateSessionError::LimitReached { oldest: 1 };
        let outcome = ChildOutcome::new(Err::<CreatedSession, _>(refused()));
        let answer = operation.next(IoResult::Child(outcome));
        assert_eq!(answer, Err(FederatedLoginError::Session(refused())));
    }
}
