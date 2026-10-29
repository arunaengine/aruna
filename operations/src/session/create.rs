//! Creates a user session with a fresh token and drops stale sessions past the user limit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::index::{MAX_USER_SESSIONS, decode_index, encode_index, owner_key};
use crate::auth::create_token::{CreateTokenConfig, CreateTokenError, mint_login_token};
use aruna_core::UserId;
use aruna_core::auth::bearer_token_hash;
use aruna_core::compute::Secret;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{USER_OWNER_KEYSPACE, USER_SESSION_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{
    NodeCapabilities, PathRestriction, SessionKind, SessionRef,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::session::UserSession;
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;
use std::collections::BTreeSet;
use thiserror::Error;
use ulid::Ulid;

pub const MAX_SESSION_TTL: u64 = 24 * 60 * 60;

#[derive(Clone, Debug, PartialEq)]
pub struct CreateSessionConfig {
    pub time: u64,
    pub expiry: u64,
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub node_capabilities: NodeCapabilities,
    pub kind: SessionKind,
    pub label: Option<String>,
    /// Display name a federated session carries in its token.
    pub name: Option<String>,
    pub restrictions: Option<Vec<PathRestriction>>,
    /// The linked login of another realm a federated session of a local account came through.
    pub via: Option<UserId>,
    /// Time of the primary login this session descends from; `None` for child sessions.
    pub auth_time: Option<u64>,
    /// The user's token cutoff: sessions issued before it no longer work and count as stale.
    pub cutoff: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedSession {
    pub token: Secret,
    pub session: UserSession,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CreateSessionState {
    Init,
    StartTransaction,
    ReadOwnerIndex,
    ReadSessions { index: BTreeSet<String> },
    DeleteStale { index: BTreeSet<String> },
    WriteSession,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateSessionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Token(#[from] CreateTokenError),
    #[error("session expiry is invalid")]
    InvalidExpiry,
    #[error("session id collision")]
    IdCollision,
    #[error("session owner index is inconsistent")]
    IndexInconsistent,
    /// `oldest` is when the oldest active session was issued.
    #[error("active session limit reached")]
    LimitReached { oldest: u64 },
    #[error("making room for the session failed: {0}")]
    Retire(String),
    #[error("session operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Debug, PartialEq)]
pub struct CreateSessionOperation {
    config: CreateSessionConfig,
    sid: String,
    session: Option<UserSession>,
    token: Option<Secret>,
    txn_id: Option<TxnId>,
    state: CreateSessionState,
    output: Option<Result<CreatedSession, CreateSessionError>>,
}

pub fn bound_session_expiry(
    now: u64,
    requested: Option<u64>,
    parent_expiry: u64,
) -> Result<u64, CreateSessionError> {
    let lifetime = requested.unwrap_or(MAX_SESSION_TTL).min(MAX_SESSION_TTL);
    let expires_at = now
        .checked_add(lifetime)
        .ok_or(CreateSessionError::InvalidExpiry)?
        .min(parent_expiry);
    if lifetime == 0 || expires_at <= now {
        return Err(CreateSessionError::InvalidExpiry);
    }
    Ok(expires_at)
}

impl CreateSessionOperation {
    pub fn new(config: CreateSessionConfig) -> Self {
        Self::new_with_sid(config, Ulid::generate().to_string())
    }

    pub fn new_with_sid(config: CreateSessionConfig, sid: String) -> Self {
        Self {
            config,
            sid,
            session: None,
            token: None,
            txn_id: None,
            state: CreateSessionState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: CreateSessionError) -> Effects {
        let cleanup = self.abort();
        self.state = CreateSessionState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        self.emit_error(CreateSessionError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got: format!("{event:?}"),
        })
    }

    fn prepare_session(&mut self) -> Result<(), CreateSessionError> {
        Ulid::from_string(&self.sid).map_err(ConversionError::from)?;
        if self.config.expiry <= self.config.time {
            return Err(CreateSessionError::InvalidExpiry);
        }
        let session_ref = SessionRef {
            sid: self.sid.clone(),
            kind: self.config.kind,
            name: self.config.name.clone(),
            via: self.config.via,
        };
        let token = mint_login_token(
            &CreateTokenConfig {
                time: self.config.time,
                expiry: Some(self.config.expiry),
                user_id: self.config.user_id,
                realm_id: self.config.realm_id,
                node_capabilities: self.config.node_capabilities.clone(),
                session: Some(session_ref),
                restrictions: self.config.restrictions.clone(),
            },
            self.config.auth_time,
        )?;
        self.session = Some(UserSession {
            sid: self.sid.clone(),
            user_id: self.config.user_id,
            kind: self.config.kind,
            label: self.config.label.clone(),
            created_at: self.config.time,
            expires_at: self.config.expiry,
            token_hash: bearer_token_hash(&token),
            revoked: false,
            via: self.config.via,
        });
        self.token = Some(Secret::new(token));
        Ok(())
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected("transaction started", event);
        };
        self.txn_id = Some(txn_id);
        self.state = CreateSessionState::ReadOwnerIndex;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USER_OWNER_KEYSPACE.to_string(),
            key: owner_key(self.config.user_id),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_index(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected("owner index read", event);
        };
        let index = match decode_index(value.as_ref()) {
            Ok(index) => index,
            Err(error) => return self.emit_error(error.into()),
        };
        if index.contains(&self.sid) {
            return self.emit_error(CreateSessionError::IdCollision);
        }
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(StorageError::TransactionNotFound.into());
        };
        let mut reads = index
            .iter()
            .map(|sid| (USER_SESSION_KEYSPACE.to_string(), sid.as_bytes().into()))
            .collect::<Vec<_>>();
        reads.push((
            USER_SESSION_KEYSPACE.to_string(),
            self.sid.as_bytes().into(),
        ));
        self.state = CreateSessionState::ReadSessions {
            index: index.clone(),
        };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    fn handle_sessions(&mut self, event: Event, index: BTreeSet<String>) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected("session records read", event);
        };
        if values.len() != index.len() + 1 {
            return self.emit_error(CreateSessionError::IndexInconsistent);
        }
        let mut active = BTreeSet::new();
        let mut stale = BTreeSet::new();
        let mut oldest = u64::MAX;
        let mut pending_seen = false;
        for (key, value) in values {
            if key.as_ref() == self.sid.as_bytes() {
                if pending_seen || value.is_some() {
                    return self.emit_error(CreateSessionError::IdCollision);
                }
                pending_seen = true;
                continue;
            }
            let Some(value) = value else {
                return self.emit_error(CreateSessionError::IndexInconsistent);
            };
            let session = match UserSession::from_bytes(value.as_ref()) {
                Ok(session) => session,
                Err(error) => return self.emit_error(error.into()),
            };
            if session.user_id != self.config.user_id
                || session.sid.as_bytes() != key.as_ref()
                || !index.contains(&session.sid)
            {
                return self.emit_error(CreateSessionError::IndexInconsistent);
            }
            let cut = self
                .config
                .cutoff
                .is_some_and(|cutoff| session.created_at < cutoff);
            if session.revoked || cut || session.expires_at <= self.config.time {
                stale.insert(session.sid);
            } else {
                oldest = oldest.min(session.created_at);
                active.insert(session.sid);
            }
        }
        if !pending_seen || active.len() + stale.len() != index.len() {
            return self.emit_error(CreateSessionError::IndexInconsistent);
        }
        if active.len() >= MAX_USER_SESSIONS {
            return self.emit_error(CreateSessionError::LimitReached { oldest });
        }
        active.insert(self.sid.clone());
        if stale.is_empty() {
            return self.write_session(active);
        }
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(StorageError::TransactionNotFound.into());
        };
        self.state = CreateSessionState::DeleteStale { index: active };
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: stale
                .into_iter()
                .map(|sid| (USER_SESSION_KEYSPACE.to_string(), sid.as_bytes().into()))
                .collect(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_deleted(&mut self, event: Event, index: BTreeSet<String>) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.unexpected("stale sessions deleted", event);
        };
        self.write_session(index)
    }

    fn write_session(&mut self, index: BTreeSet<String>) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(StorageError::TransactionNotFound.into());
        };
        let Some(session) = self.session.as_ref() else {
            return self.emit_error(CreateSessionError::NotFinished);
        };
        let session_bytes = match session.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return self.emit_error(error.into()),
        };
        let index_bytes = match encode_index(&index) {
            Ok(bytes) => bytes,
            Err(error) => return self.emit_error(error.into()),
        };
        self.state = CreateSessionState::WriteSession;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (
                    USER_SESSION_KEYSPACE.to_string(),
                    self.sid.as_bytes().into(),
                    session_bytes.into(),
                ),
                (
                    USER_OWNER_KEYSPACE.to_string(),
                    owner_key(self.config.user_id),
                    index_bytes,
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected("session write", event);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(StorageError::TransactionNotFound.into());
        };
        let Some(session) = self.session.clone() else {
            return self.emit_error(CreateSessionError::NotFinished);
        };
        let Some(token) = self.token.take() else {
            return self.emit_error(CreateSessionError::NotFinished);
        };
        self.output = Some(Ok(CreatedSession { token, session }));
        self.state = CreateSessionState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected("transaction committed", event);
        };
        self.txn_id = None;
        self.state = CreateSessionState::Finish;
        smallvec![]
    }
}

impl Operation for CreateSessionOperation {
    type Output = CreatedSession;
    type Error = CreateSessionError;

    fn start(&mut self) -> Effects {
        if let Err(error) = self.prepare_session() {
            return self.emit_error(error);
        }
        self.state = CreateSessionState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(error.into());
        }
        match self.state.clone() {
            CreateSessionState::Init => self.start(),
            CreateSessionState::StartTransaction => self.handle_started(event),
            CreateSessionState::ReadOwnerIndex => self.handle_index(event),
            CreateSessionState::ReadSessions { index } => self.handle_sessions(event, index),
            CreateSessionState::DeleteStale { index } => self.handle_deleted(event, index),
            CreateSessionState::WriteSession => self.handle_written(event),
            CreateSessionState::CommitTransaction => self.handle_committed(event),
            CreateSessionState::Finish | CreateSessionState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateSessionState::Finish | CreateSessionState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(CreateSessionError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

/// Creates a session for the user. At the session limit the user's cutoff moves past the
/// oldest active sessions, which signs them out, and creation runs again. A service account
/// keeps the plain limit, so renewing never ends the sessions its automation uses.
pub async fn create_making_room(
    context: &crate::driver::DriverContext,
    node_id: aruna_core::id::NodeId,
    mut config: CreateSessionConfig,
) -> Result<CreatedSession, CreateSessionError> {
    let read_cutoff = |realm_id, user_id| async move {
        crate::auth::bearer_token::realm_user_cutoff(&context.storage_handle, realm_id, &user_id)
            .await
            .map_err(|error| CreateSessionError::Retire(error.to_string()))
    };
    let now = config.time;
    config.cutoff = read_cutoff(config.realm_id, config.user_id).await?;
    // A token issued before the cutoff would be refused at once, so it is issued at it.
    let at_cutoff = |config: &mut CreateSessionConfig| {
        config.time = config.time.max(config.cutoff.unwrap_or(0));
    };
    at_cutoff(&mut config);
    loop {
        let oldest = match crate::driver::drive(
            CreateSessionOperation::new(config.clone()),
            context,
        )
        .await
        {
            Err(CreateSessionError::LimitReached { oldest }) => oldest,
            other => return other,
        };
        let read = crate::users::read_document::ReadUserOperation::new(config.user_id);
        let service = match crate::driver::drive(read, context).await {
            Ok(user) => user.service_group().is_some(),
            // An unknown account may be a service account not yet replicated here.
            Err(crate::users::read_document::ReadUserError::NotFound) => true,
            Err(error) => return Err(CreateSessionError::Retire(error.to_string())),
        };
        if service {
            return Err(CreateSessionError::LimitReached { oldest });
        }
        let actor = aruna_core::structs::identity::auth::Actor {
            node_id,
            user_id: config.user_id,
            realm_id: config.realm_id,
        };
        let retire = super::revoke::cutoff_revocation(actor, oldest, now);
        crate::driver::drive(
            crate::auth::revoke_token::RevokeTokenOperation::new(retire),
            context,
        )
        .await
        .map_err(|error| CreateSessionError::Retire(error.to_string()))?;
        let cutoff = read_cutoff(config.realm_id, config.user_id).await?;
        // Without progress the limit stands rather than looping.
        if cutoff.is_none_or(|cutoff| cutoff <= oldest) {
            return Err(CreateSessionError::LimitReached { oldest });
        }
        config.cutoff = cutoff;
        at_cutoff(&mut config);
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::keys::generate_signing_key;

    fn session_config() -> CreateSessionConfig {
        let signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
        CreateSessionConfig {
            time: 100,
            expiry: 200,
            user_id: UserId::local(Ulid::from_bytes([4; 16]), realm_id),
            realm_id,
            node_capabilities: NodeCapabilities::management_node(signing_key).unwrap(),
            kind: SessionKind::Portal,
            label: None,
            name: None,
            restrictions: None,
            via: None,
            auth_time: None,
            cutoff: None,
        }
    }

    fn session_record(
        config: &CreateSessionConfig,
        sid: String,
        expires_at: u64,
        revoked: bool,
    ) -> UserSession {
        UserSession {
            sid,
            user_id: config.user_id,
            kind: SessionKind::Portal,
            label: None,
            created_at: 1,
            expires_at,
            token_hash: "a".repeat(64),
            revoked,
            via: None,
        }
    }

    fn begin_read(operation: &mut CreateSessionOperation, index: &BTreeSet<String>, txn_id: TxnId) {
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: owner_key(operation.config.user_id),
            value: Some(encode_index(index).unwrap()),
        }));
    }

    #[test]
    fn bounds_parent_expiry() {
        assert_eq!(bound_session_expiry(100, Some(600), 500).unwrap(), 500);
        assert_eq!(bound_session_expiry(100, None, 100_000).unwrap(), 86_500);
        assert_eq!(
            bound_session_expiry(100, Some(0), 500),
            Err(CreateSessionError::InvalidExpiry)
        );
    }

    #[test]
    fn prunes_stale_sessions() {
        let config = session_config();
        let expired_sid = Ulid::from_parts(1, 1).to_string();
        let revoked_sid = Ulid::from_parts(2, 2).to_string();
        let pending_sid = Ulid::from_parts(3, 3).to_string();
        let index = BTreeSet::from([expired_sid.clone(), revoked_sid.clone()]);
        let mut operation =
            CreateSessionOperation::new_with_sid(config.clone(), pending_sid.clone());
        let txn_id = Ulid::from_bytes([6; 16]);
        begin_read(&mut operation, &index, txn_id);

        let deleted = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    expired_sid.as_bytes().into(),
                    Some(
                        session_record(&config, expired_sid.clone(), 99, false)
                            .to_bytes()
                            .unwrap()
                            .into(),
                    ),
                ),
                (
                    revoked_sid.as_bytes().into(),
                    Some(
                        session_record(&config, revoked_sid.clone(), 200, true)
                            .to_bytes()
                            .unwrap()
                            .into(),
                    ),
                ),
                (pending_sid.as_bytes().into(), None),
            ],
        }));
        let [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })] = deleted.as_slice()
        else {
            panic!("stale session records must be deleted");
        };
        assert_eq!(
            deletes
                .iter()
                .map(|(_, key)| key.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([expired_sid.as_bytes().into(), revoked_sid.as_bytes().into()])
        );

        let written = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = written.as_slice() else {
            panic!("the new session and bounded index must be written together");
        };
        let stored_index = writes
            .iter()
            .find(|(key_space, _, _)| key_space == USER_OWNER_KEYSPACE)
            .map(|(_, _, value)| value)
            .unwrap();
        assert_eq!(
            decode_index(Some(stored_index)).unwrap(),
            BTreeSet::from([pending_sid])
        );
    }

    #[test]
    fn rejects_session_limit() {
        // At the limit the oldest active session is named; past the user's cutoff the same
        // sessions are stale and the new session is created.
        let config = session_config();
        let pending_sid = Ulid::from_parts(1_000, 1_000).to_string();
        let mut index = BTreeSet::new();
        let mut values = Vec::new();
        for value in 0..MAX_USER_SESSIONS {
            let sid = Ulid::from_parts(value as u64, value as u128).to_string();
            index.insert(sid.clone());
            values.push((
                sid.as_bytes().into(),
                Some(
                    session_record(&config, sid, 200, false)
                        .to_bytes()
                        .unwrap()
                        .into(),
                ),
            ));
        }
        values.push((pending_sid.as_bytes().into(), None));
        let mut operation =
            CreateSessionOperation::new_with_sid(config.clone(), pending_sid.clone());
        begin_read(&mut operation, &index, Ulid::from_bytes([6; 16]));
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: values.clone(),
        }));

        assert_eq!(
            operation.finalize(),
            Err(CreateSessionError::LimitReached { oldest: 1 })
        );

        let cut = CreateSessionConfig {
            cutoff: Some(2),
            ..config
        };
        let mut operation = CreateSessionOperation::new_with_sid(cut, pending_sid);
        begin_read(&mut operation, &index, Ulid::from_bytes([6; 16]));
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })]
                if deletes.len() == MAX_USER_SESSIONS
        ));
    }

    /// A realm whose user `session_config` creates sessions for, on real storage.
    async fn session_realm() -> (
        tempfile::TempDir,
        crate::driver::DriverContext,
        CreateSessionConfig,
        aruna_core::id::NodeId,
    ) {
        use crate::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
        let dir = tempfile::tempdir().unwrap();
        let storage =
            aruna_storage::storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::driver::DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(aruna_tasks::TaskHandle::new()),
            compute_handle: None,
        };
        let config = session_config();
        let node_id = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        let actor = aruna_core::structs::identity::auth::Actor {
            node_id,
            user_id: config.user_id,
            realm_id: config.realm_id,
        };
        let realm = CreateRealmConfig {
            actor,
            realm_description: "session realm".to_string(),
            oidc_providers: Vec::new(),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        };
        crate::driver::drive(CreateRealmOperation::new(realm), &context)
            .await
            .unwrap();
        (dir, context, config, node_id)
    }

    /// Stores the user document of `config`'s user with the given attributes.
    async fn store_user(
        context: &crate::driver::DriverContext,
        config: &CreateSessionConfig,
        node_id: aruna_core::id::NodeId,
        attributes: std::collections::HashMap<String, String>,
    ) {
        use aruna_core::document::DocumentTarget;
        let user = aruna_core::structs::identity::user::User {
            user_id: config.user_id,
            name: "user".to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes,
        };
        let target = DocumentTarget::User {
            user_id: config.user_id,
        };
        let actor = aruna_core::structs::identity::auth::Actor {
            node_id,
            user_id: config.user_id,
            realm_id: config.realm_id,
        };
        let write = StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: user.to_bytes(&actor).unwrap().into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(write).await;
    }

    // Signing out works past the 64 self-service revocations a user may hold: the rest move
    // the user's cutoff, and the next sign-in still works.
    #[tokio::test]
    async fn revokes_past_capacity() {
        use crate::driver::drive;
        use crate::session::revoke::RevokeSessionOperation;
        let (_dir, context, config, node_id) = session_realm().await;
        let now = aruna_core::time::unix_timestamp_secs();
        let base = now - 1_000;
        let mut sessions = Vec::new();
        for offset in 0..100 {
            let session = CreateSessionConfig {
                time: base + offset,
                expiry: now + 3_600,
                ..config.clone()
            };
            let created = drive(CreateSessionOperation::new(session), &context)
                .await
                .unwrap();
            sessions.push(created.session);
        }
        let actor = aruna_core::structs::identity::auth::Actor {
            node_id,
            user_id: config.user_id,
            realm_id: config.realm_id,
        };
        for session in &sessions {
            let revoke = RevokeSessionOperation::new(actor.clone(), session.sid.clone(), now);
            let revoked = drive(revoke, &context).await.unwrap();
            assert!(revoked.is_some_and(|revoked| revoked.revoked));
        }
        let cutoff = crate::auth::bearer_token::realm_user_cutoff(
            &context.storage_handle,
            config.realm_id,
            &config.user_id,
        )
        .await
        .unwrap();
        // The last revoked session and every older one are cut off.
        assert_eq!(cutoff, Some(base + 100));
        let next = CreateSessionConfig {
            time: now,
            expiry: now + 3_600,
            ..config
        };
        create_making_room(&context, node_id, next).await.unwrap();
    }

    // A service account, or one whose document is not here yet, at the limit is refused as
    // before; none of its sessions are ended.
    #[tokio::test]
    async fn service_keeps_limit() {
        use crate::driver::drive;
        for known in [true, false] {
            let (_dir, context, config, node_id) = session_realm().await;
            if known {
                let service = std::collections::HashMap::from([(
                    aruna_core::user::validation::SERVICE_GROUP_ATTRIBUTE.to_string(),
                    Ulid::from_bytes([8; 16]).to_string(),
                )]);
                store_user(&context, &config, node_id, service).await;
            }
            let now = aruna_core::time::unix_timestamp_secs();
            let base = now - 1_000;
            for offset in 0..MAX_USER_SESSIONS as u64 {
                let session = CreateSessionConfig {
                    time: base + offset,
                    expiry: now + 3_600,
                    ..config.clone()
                };
                drive(CreateSessionOperation::new(session), &context)
                    .await
                    .unwrap();
            }
            let next = CreateSessionConfig {
                time: now,
                expiry: now + 3_600,
                ..config.clone()
            };
            let refused = create_making_room(&context, node_id, next).await;
            assert_eq!(
                refused,
                Err(CreateSessionError::LimitReached { oldest: base })
            );
            let cutoff = crate::auth::bearer_token::realm_user_cutoff(
                &context.storage_handle,
                config.realm_id,
                &config.user_id,
            )
            .await
            .unwrap();
            assert_eq!(cutoff, None);
        }
    }

    // A sign-in right after a sign-out moved the cutoff into the next second is issued at the
    // cutoff, so its token is not refused at once.
    #[tokio::test]
    async fn issues_at_cutoff() {
        use crate::driver::drive;
        let (_dir, context, config, node_id) = session_realm().await;
        let now = aruna_core::time::unix_timestamp_secs();
        let actor = aruna_core::structs::identity::auth::Actor {
            node_id,
            user_id: config.user_id,
            realm_id: config.realm_id,
        };
        let raise = super::super::revoke::cutoff_revocation(actor, now, now);
        drive(
            crate::auth::revoke_token::RevokeTokenOperation::new(raise),
            &context,
        )
        .await
        .unwrap();
        let next = CreateSessionConfig {
            time: now,
            expiry: now + 3_600,
            ..config
        };
        let created = create_making_room(&context, node_id, next).await.unwrap();
        assert_eq!(created.session.created_at, now + 1);
        assert_eq!(created.session.expires_at, now + 3_600);
    }

    // A user at the session limit still signs in: the oldest session is signed out instead.
    #[tokio::test]
    async fn full_user_signs_in() {
        use crate::driver::drive;
        let (_dir, context, config, node_id) = session_realm().await;
        store_user(&context, &config, node_id, Default::default()).await;
        // The cutoff is read against the wall clock, so the sessions are recent.
        let now = aruna_core::time::unix_timestamp_secs();
        let base = now - 1_000;
        let mut first = None;
        for offset in 0..MAX_USER_SESSIONS as u64 {
            let session = CreateSessionConfig {
                time: base + offset,
                expiry: now + 3_600,
                ..config.clone()
            };
            let created = drive(CreateSessionOperation::new(session), &context)
                .await
                .unwrap();
            first.get_or_insert(created.session);
        }
        let next = CreateSessionConfig {
            time: now,
            expiry: now + 3_600,
            ..config.clone()
        };
        let refused = drive(CreateSessionOperation::new(next.clone()), &context).await;
        assert_eq!(
            refused,
            Err(CreateSessionError::LimitReached { oldest: base })
        );

        create_making_room(&context, node_id, next).await.unwrap();

        let cutoff = crate::auth::bearer_token::realm_user_cutoff(
            &context.storage_handle,
            config.realm_id,
            &config.user_id,
        )
        .await
        .unwrap();
        assert_eq!(cutoff, Some(base + 1));
        assert!(first.unwrap().created_at < base + 1);
    }
}
