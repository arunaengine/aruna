//! Creates an S3 session credential and drops the oldest ones past the per group limit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    MAX_GROUP_SESSIONS, S3SessionCredentials, S3SessionError, SESSION_SECRETS_LEN, build_session,
    decode_index, encode_index, expiry_key, owner_key, session_age,
};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::{Secret, draw_secret, drawn_secret};
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{S3_SESSION_KEYSPACE, SESSION_EXPIRY_KEYSPACE, SESSION_OWNER_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::identity::group_delete::{GroupWriteError, check_group_write};
use aruna_core::structs::identity::s3_session::{S3Session, SESSION_MAX_TTL};
use aruna_core::types::{GroupId, Key, Value};
use std::collections::BTreeSet;
use std::time::SystemTime;

use crate::groups::fence::group_fence_key;

#[derive(Debug, PartialEq)]
pub struct CreateS3Config {
    pub user_identity: UserId,
    pub group_id: GroupId,
    pub now: SystemTime,
    pub expiry: SystemTime,
    pub path_restrictions: Option<Vec<PathRestriction>>,
    pub issued_by: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CreatePhase {
    Secret,
    Index,
    Sessions,
    Delete,
    Write,
}

#[derive(Debug, PartialEq)]
pub struct CreateS3Operation {
    /// The key id is minted when the run starts.
    mint_key: bool,
    now_ms: u64,
    ids: IdMint,
    config: CreateS3Config,
    key_id: String,
    encryption_key: CredentialEncryptionKey,
    pending: Option<S3SessionCredentials>,
    /// The owner index as it is read, then as it is written.
    index: BTreeSet<String>,
    phase: CreatePhase,
}

impl CreateS3Operation {
    pub fn new(config: CreateS3Config, encryption_key: CredentialEncryptionKey) -> Self {
        Self {
            mint_key: true,
            ..Self::with_key(config, String::new(), encryption_key)
        }
    }

    pub fn with_key(
        config: CreateS3Config,
        key_id: String,
        encryption_key: CredentialEncryptionKey,
    ) -> Self {
        Self {
            config,
            key_id,
            mint_key: false,
            now_ms: 0,
            ids: IdMint::default(),
            encryption_key,
            pending: None,
            index: BTreeSet::new(),
            phase: CreatePhase::Secret,
        }
    }

    /// Builds the session from the drawn secrets, then reads the group's sessions.
    fn secret_drawn(
        &mut self,
        drawn: Secret,
    ) -> Result<Next<S3SessionCredentials>, S3SessionError> {
        let access_key = S3Session::build_access_key(&self.key_id)?;
        self.pending = Some(build_session(
            access_key,
            self.config.user_identity,
            self.config.group_id,
            self.config.expiry,
            self.config.path_restrictions.clone(),
            self.config.issued_by,
            &self.encryption_key,
            drawn,
            aruna_core::id_mint::id_nonce(self.ids.mint(self.now_ms)),
        )?);
        let owner = owner_key(self.config.user_identity, self.config.group_id);
        self.phase = CreatePhase::Index;
        Ok(Next::Call(IoCall::Read(vec![
            (SESSION_OWNER_KEYSPACE.to_string(), owner),
            group_fence_key(self.config.group_id),
        ])))
    }

    fn pending_key(&self) -> Result<String, S3SessionError> {
        (self.pending.as_ref())
            .map(|pending| pending.access_key_id.clone())
            .ok_or(S3SessionError::Failed)
    }

    /// Reads the group's sessions and the new key, which must not exist yet.
    fn index_read(
        &mut self,
        values: Vec<Option<Value>>,
    ) -> Result<Next<S3SessionCredentials>, S3SessionError> {
        let [index, fence] = <[Option<Value>; 2]>::try_from(values)
            .map_err(|_| S3SessionError::GroupWrite(GroupWriteError::Unexpected))?;
        check_group_write(fence.as_deref())?;
        self.index = decode_index(index.as_ref())?;
        let pending_key = self.pending_key()?;
        if self.index.contains(&pending_key) {
            return Err(S3SessionError::IndexInconsistent);
        }
        let mut reads: Vec<(String, Key)> = (self.index.iter())
            .map(|key| (S3_SESSION_KEYSPACE.to_string(), key.as_bytes().into()))
            .collect();
        reads.push((
            S3_SESSION_KEYSPACE.to_string(),
            pending_key.as_bytes().into(),
        ));
        self.phase = CreatePhase::Sessions;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// Drops expired sessions and the oldest ones above the bound, so the new one fits.
    fn sessions_read(
        &mut self,
        values: Vec<Option<Value>>,
    ) -> Result<Next<S3SessionCredentials>, S3SessionError> {
        if values.len() != self.index.len() + 1 {
            return Err(S3SessionError::IndexInconsistent);
        }
        let pending_key = self.pending_key()?;
        let keys: Vec<String> = (self.index.iter().cloned())
            .chain([pending_key.clone()])
            .collect();
        let mut active = Vec::new();
        let mut removed = Vec::new();
        for (key, value) in keys.iter().zip(values) {
            if *key == pending_key {
                if value.is_some() {
                    return Err(S3SessionError::IndexInconsistent);
                }
                continue;
            }
            let value = value.ok_or(S3SessionError::IndexInconsistent)?;
            let session = S3Session::from_bytes(value.as_ref())?;
            if session.access_key != *key
                || !self.index.contains(&session.access_key)
                || session.user_identity != self.config.user_identity
                || session.group_id != self.config.group_id
            {
                return Err(S3SessionError::IndexInconsistent);
            }
            match session.is_expired(self.config.now) {
                true => removed.push(session),
                false => active.push(session),
            }
        }
        // The new session must fit the bound, so the oldest ones above it are evicted.
        active.sort_by_key(session_age);
        let evicted = active.len().saturating_sub(MAX_GROUP_SESSIONS - 1);
        removed.extend(active.drain(..evicted));
        self.index = active
            .into_iter()
            .map(|session| session.access_key)
            .collect();
        self.index.insert(pending_key);
        if removed.is_empty() {
            return self.write();
        }
        let mut deletes = Vec::with_capacity(removed.len() * 2);
        for session in removed {
            let expiry = expiry_key(session.expiry, &session.access_key)?;
            deletes.push((
                S3_SESSION_KEYSPACE.to_string(),
                session.access_key.as_bytes().into(),
            ));
            deletes.push((SESSION_EXPIRY_KEYSPACE.to_string(), expiry));
        }
        self.phase = CreatePhase::Delete;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    /// The new session, the owner index with it and its expiry row.
    fn write(&mut self) -> Result<Next<S3SessionCredentials>, S3SessionError> {
        let pending = self.pending.as_ref().ok_or(S3SessionError::Failed)?;
        let session_bytes = pending.session.to_bytes()?;
        let index_bytes = encode_index(&self.index)?;
        let expiry = expiry_key(pending.session.expiry, &pending.access_key_id)?;
        let owner = owner_key(self.config.user_identity, self.config.group_id);
        self.phase = CreatePhase::Write;
        Ok(Next::Call(IoCall::Write(vec![
            (
                S3_SESSION_KEYSPACE.to_string(),
                pending.access_key_id.as_bytes().into(),
                session_bytes.into(),
            ),
            (
                SESSION_OWNER_KEYSPACE.to_string(),
                owner.clone(),
                index_bytes,
            ),
            (SESSION_EXPIRY_KEYSPACE.to_string(), expiry, owner),
        ])))
    }
}

impl Operation for CreateS3Operation {
    type Output = S3SessionCredentials;
    type Error = S3SessionError;

    fn first(&mut self) -> Result<Next<S3SessionCredentials>, S3SessionError> {
        // Minted once, so a rerun after a conflict keeps the key id.
        if std::mem::take(&mut self.mint_key) {
            self.key_id = self.ids.mint(self.now_ms).to_string();
        }
        let ttl = (self.config.expiry.duration_since(self.config.now))
            .map_err(|_| S3SessionError::InvalidExpiry)?;
        if ttl.is_zero() || ttl > SESSION_MAX_TTL {
            return Err(S3SessionError::InvalidExpiry);
        }
        self.phase = CreatePhase::Secret;
        Ok(Next::Call(draw_secret(SESSION_SECRETS_LEN)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<S3SessionCredentials>, S3SessionError> {
        match self.phase {
            CreatePhase::Secret => self.secret_drawn(drawn_secret(result)?),
            CreatePhase::Index => self.index_read(result.values()?),
            CreatePhase::Sessions => self.sessions_read(result.values()?),
            CreatePhase::Delete => {
                result.done()?;
                self.write()
            }
            CreatePhase::Write => {
                result.done()?;
                let pending = self.pending.take().ok_or(S3SessionError::Failed)?;
                Ok(Next::Done(pending))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn expected_error(error: &Self::Error) -> bool {
        error.expected()
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::{Effect, NetEffect};
    use aruna_core::events::{Event, NetEvent};
    use aruna_core::structs::identity::realm::RealmId;
    use std::time::Duration;
    use ulid::Ulid;

    fn operation() -> CreateS3Operation {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let config = CreateS3Config {
            user_identity: UserId::local(Ulid::from_parts(2, 2), RealmId::from_bytes([1; 32])),
            group_id: Ulid::from_parts(3, 3),
            now,
            expiry: now + Duration::from_secs(60),
            path_restrictions: None,
            issued_by: [1; 32],
        };
        let key_id = Ulid::from_parts(4, 4).to_string();
        CreateS3Operation::with_key(config, key_id, CredentialEncryptionKey::derive(&[7; 32]))
    }

    /// Fixed drawn secrets: 30 signing letters, then 64 token letters.
    fn drawn() -> IoResult {
        let secret = Secret::new(format!("{}{}", "s".repeat(30), "t".repeat(64)));
        IoResult::Event(Box::new(Event::Net(NetEvent::SecretDrawn(secret))))
    }

    #[test]
    fn reads_index_fence() {
        let mut create = operation();
        let Ok(Next::Call(IoCall::Effect(draw))) = create.first() else {
            panic!("expected the secret draw");
        };
        assert_eq!(
            *draw,
            Effect::Net(NetEffect::DrawSecret(SESSION_SECRETS_LEN))
        );
        let Ok(Next::Call(IoCall::Read(reads))) = create.next(drawn()) else {
            panic!("expected the owner index and group fence read");
        };
        assert_eq!(reads[1], group_fence_key(Ulid::from_parts(3, 3)));
        assert_eq!(
            create.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        let Ok(Next::Call(IoCall::Read(sessions))) =
            create.next(IoResult::Values(vec![None, None]))
        else {
            panic!("expected the session reads");
        };
        assert_eq!(sessions.len(), 1);
        let write = create.next(IoResult::Values(vec![None]));
        assert!(matches!(write, Ok(Next::Call(IoCall::Write(rows))) if rows.len() == 3));
        let Ok(Next::Done(credentials)) = create.next(IoResult::Done) else {
            panic!("expected the new credentials");
        };
        assert_eq!(credentials.secret_access_key.expose(), "s".repeat(30));
        assert_eq!(credentials.session_token.expose(), "t".repeat(64));
    }

    #[test]
    fn key_from_run() {
        // The key id comes from the run's time and entropy, so two runs never share it.
        let mut create = operation();
        create.mint_key = true;
        create.stamp(5_000);
        create.seed(9);
        create.first().unwrap();
        assert_eq!(create.key_id, IdMint::new(9).mint(5_000).to_string());
    }

    #[test]
    fn refuses_taken_key() {
        let mut create = operation();
        create.first().unwrap();
        create.next(drawn()).unwrap();
        create.next(IoResult::Values(vec![None, None])).unwrap();
        let taken = IoResult::Values(vec![Some(vec![1].into())]);
        assert_eq!(create.next(taken), Err(S3SessionError::IndexInconsistent));
        let mut short = operation();
        short.first().unwrap();
        short.next(drawn()).unwrap();
        let answer = short.next(IoResult::Values(vec![None]));
        assert_eq!(
            answer,
            Err(S3SessionError::GroupWrite(GroupWriteError::Unexpected))
        );
    }
}
