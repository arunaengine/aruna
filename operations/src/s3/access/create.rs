//! Creates an S3 access key and encrypted secret for a user, with path limits and an expiry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::compute::Secret;
use aruna_core::compute::SharedSecret;
use aruna_core::credential_encryption::{
    CredentialEncryptionKey, EncryptedS3Secret, EncryptionError,
};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    ACCESS_OWNER_KEYSPACE, AUTH_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, BUCKET_HOLDER_KEYSPACE,
    KEY_COPY_KEYSPACE, S3_BUCKET_KEYSPACE, TOKEN_INDEX_KEYSPACE, USER_ACCESS_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::permission_path::{RestrictionLimitError, validate_restriction_limits};
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketHolder, BucketKeyError, BucketKeyRef, HolderOrigin, TokenCopy,
};
use aruna_core::structs::storage::key_audit::{
    AuditAction, AuditOutcome, BucketAuditRecord, next_event_id,
};
use aruna_core::types::{Effects, GroupId, Key, Value};
use rand::distr::Alphanumeric;
use rand::{RngExt, rng};
use smallvec::smallvec;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use ulid::Ulid;

use super::index::{
    MAX_ACTIVE_CREDENTIALS, decode_index, encode_index, owner_key, token_deletes, token_scan,
};
use crate::s3::bucket::key::rows::{Row, SettingsError, audit_row, parse_authority};

pub const DEFAULT_CREDENTIAL_TTL: Duration = Duration::from_secs(24 * 60 * 60 * 365);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateUserState {
    Init,
    StartTransaction,
    ReadOwnerIndex,
    ReadCredentials {
        index: std::collections::BTreeSet<String>,
        replace: bool,
    },
    DeleteStale {
        index: std::collections::BTreeSet<String>,
    },
    WriteCredentials,
    CommitTransaction,
    Finish,
    Error,
    ReadTokenBuckets {
        index: std::collections::BTreeSet<String>,
    },
    ReadTokenAuthority {
        index: std::collections::BTreeSet<String>,
    },
    SealTokens {
        index: std::collections::BTreeSet<String>,
    },
    ScanStaleTokens {
        index: std::collections::BTreeSet<String>,
    },
    DeleteStaleTokens {
        index: std::collections::BTreeSet<String>,
    },
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateUserError {
    #[error(transparent)]
    GroupWrite(#[from] aruna_core::structs::identity::group_delete::GroupWriteError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    RestrictionLimit(#[from] RestrictionLimitError),
    #[error(transparent)]
    Encryption(#[from] EncryptionError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: CreateUserState,
        expected: &'static str,
        received: Event,
    },
    #[error("No user access found")]
    NotFound,
    #[error("active credential limit reached")]
    LimitReached,
    #[error("credential owner index is inconsistent")]
    IndexInconsistent,
    #[error("User access creation not finished")]
    NotFinished,
    #[error("User access creation failed")]
    CreateAccessFailed,
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("bucket {0} does not exist")]
    NoSuchBucket(String),
    #[error("bucket {0} is not encrypted")]
    NotEncrypted(String),
    /// The caller is neither creator, current admin nor an explicit holder of the bucket (D30).
    #[error("the caller holds no key of bucket {0}")]
    NotHolder(String),
    #[error("bucket {0} is locked")]
    BucketLocked(String),
}

#[derive(Debug, PartialEq)]
pub struct CreateUserConfig {
    pub user_identity: UserId,
    pub group_id: GroupId,
    pub expiry: SystemTime,
    pub path_restrictions: Option<Vec<PathRestriction>>,
    pub issued_by: [u8; 32],
}

#[derive(Debug, PartialEq)]
pub struct CreateUserOperation {
    config: CreateUserConfig,
    key_id: String,
    encryption_key: CredentialEncryptionKey,
    pending_secret: Option<Secret>,
    access: Option<UserAccess>,
    txn_id: Option<ulid::Ulid>,
    state: CreateUserState,
    output: Result<(String, Secret, UserAccess), CreateUserError>,
    tokens: Option<TokenPlan>,
    /// Deleted credentials whose token copies still go, and where the current one's scan stands.
    stale_tokens: (Vec<String>, Option<Key>),
}

/// The encrypted buckets a new credential gets token copies of, and what sealing needs.
#[derive(Debug, Default, PartialEq)]
struct TokenPlan {
    buckets: Vec<String>,
    origin: Option<(RealmId, NodeId)>,
    now_ms: u64,
    /// Each bucket's stored record and settings rows, for the authority check.
    rows: Vec<[(Key, Option<Value>); 2]>,
    /// Each bucket's record and active key generation.
    keys: Vec<(BucketInfo, BucketKeyRef)>,
    written: Vec<Row>,
    token: Option<SharedSecret>,
}

impl TokenPlan {
    /// Reads of the realm document, then each bucket's group document and the caller's grant.
    fn check_buckets(
        &mut self,
        values: Vec<(Key, Option<Value>)>,
        caller: UserId,
    ) -> Result<Vec<(String, Key)>, CreateUserError> {
        let (realm_id, _) = self.origin.ok_or(CreateUserError::CreateAccessFailed)?;
        let mut reads = vec![(
            AUTH_KEYSPACE.to_string(),
            realm_id.as_bytes().to_vec().into(),
        )];
        let mut values = values.into_iter();
        for bucket in &self.buckets {
            let (Some(info), Some(settings)) = (values.next(), values.next()) else {
                return Err(CreateUserError::CreateAccessFailed);
            };
            let Some(record) = info.1.as_deref() else {
                return Err(CreateUserError::NoSuchBucket(bucket.clone()));
            };
            let record = BucketInfo::from_bytes(record)?;
            let active = BucketEncryption::from_row(settings.1.as_deref())?.active_key();
            let active = active.ok_or_else(|| CreateUserError::NotEncrypted(bucket.clone()))?;
            let grant = [&active.bucket_id.to_bytes()[..], &caller.to_storage_key()].concat();
            reads.push((
                AUTH_KEYSPACE.to_string(),
                record.group_id.to_bytes().to_vec().into(),
            ));
            reads.push((BUCKET_HOLDER_KEYSPACE.to_string(), grant.into()));
            self.rows.push([info, settings]);
            self.keys.push((record, active));
        }
        Ok(reads)
    }

    /// The active key of each bucket the caller holds now: as creator, admin or explicit holder.
    fn check_holders(
        &mut self,
        values: Vec<(Key, Option<Value>)>,
        caller: UserId,
    ) -> Result<(Vec<BucketKeyRef>, RealmId, NodeId), CreateUserError> {
        let (realm_id, node_id) = self.origin.ok_or(CreateUserError::CreateAccessFailed)?;
        let mut values = values.into_iter();
        let realm = values.next().ok_or(CreateUserError::CreateAccessFailed)?;
        let rows = std::mem::take(&mut self.rows);
        let buckets = self.buckets.iter().zip(rows).zip(&self.keys);
        for ((bucket, [info, settings]), (record, _)) in buckets {
            let (Some(group), Some((_, grant))) = (values.next(), values.next()) else {
                return Err(CreateUserError::CreateAccessFailed);
            };
            let authority = vec![info, settings, realm.clone(), group];
            let state = parse_authority(authority, realm_id, record.group_id)?;
            let grant = grant
                .map(|value| BucketHolder::from_bytes(&value))
                .transpose()?;
            let explicit = grant.is_some_and(|grant| grant.origin == HolderOrigin::Explicit);
            if !(explicit || state.info.created_by == caller || state.admins.contains(&caller)) {
                return Err(CreateUserError::NotHolder(bucket.clone()));
            }
        }
        let keys = self.keys.iter().map(|(_, key)| *key).collect();
        Ok((keys, realm_id, node_id))
    }

    /// The copy, index and audit rows of each sealed copy; the token waits for the output.
    fn sealed(
        &mut self,
        copies: Vec<TokenCopy>,
        token: SharedSecret,
        caller: UserId,
    ) -> Result<(), CreateUserError> {
        let (_, node_id) = self.origin.ok_or(CreateUserError::CreateAccessFailed)?;
        let matches = self.keys.len() == copies.len()
            && (self.keys.iter().zip(&copies)).all(|((_, key), copy)| copy.key == *key);
        if !matches {
            return Err(CreateUserError::CreateAccessFailed);
        }
        for copy in &copies {
            let audit = BucketAuditRecord {
                event_id: next_event_id(self.now_ms),
                bucket_id: copy.key.bucket_id,
                at_ms: self.now_ms,
                action: AuditAction::TokenCreated,
                actor: Some(caller),
                node_id,
                generation: Some(copy.key.generation),
                session_id: None,
                intent_id: None,
                sequence: None,
                deadline_ms: None,
                reason: Some(format!("token for access key {}", copy.access_key)),
                outcome: AuditOutcome::Applied,
            };
            let value = copy.to_bytes()?;
            self.written.extend([
                (
                    KEY_COPY_KEYSPACE.to_string(),
                    copy.key().into(),
                    value.into(),
                ),
                (
                    TOKEN_INDEX_KEYSPACE.to_string(),
                    copy.index_key().into(),
                    Vec::new().into(),
                ),
                audit_row(&audit)?,
            ]);
        }
        self.token = Some(token);
        Ok(())
    }

    /// The requested name of the bucket with stable id `bucket_id`.
    fn bucket_name(&self, bucket_id: Ulid) -> String {
        let position = self
            .keys
            .iter()
            .position(|(_, key)| key.bucket_id == bucket_id);
        position
            .and_then(|position| self.buckets.get(position).cloned())
            .unwrap_or_else(|| bucket_id.to_string())
    }
}

impl CreateUserOperation {
    pub fn new(config: CreateUserConfig, encryption_key: CredentialEncryptionKey) -> Self {
        Self::new_with_key(config, Ulid::generate().to_string(), encryption_key)
    }

    pub fn new_with_key(
        config: CreateUserConfig,
        key_id: String,
        encryption_key: CredentialEncryptionKey,
    ) -> Self {
        Self {
            config,
            key_id,
            encryption_key,
            pending_secret: None,
            access: None,
            txn_id: None,
            state: CreateUserState::Init,
            output: Err(CreateUserError::NotFinished),
            tokens: None,
            stale_tokens: (Vec::new(), None),
        }
    }

    /// Seals the unlocked key of each of `buckets` with one new token in the credential's
    /// transaction. The caller must hold each key; any locked bucket fails the whole credential.
    pub fn with_tokens(
        mut self,
        buckets: Vec<String>,
        origin: (RealmId, NodeId),
        now_ms: u64,
    ) -> Self {
        self.tokens = Some(TokenPlan {
            buckets,
            origin: Some(origin),
            now_ms,
            ..TokenPlan::default()
        });
        self
    }

    /// Reads each token bucket's record and settings in the credential's transaction.
    fn read_token_buckets(&mut self, index: std::collections::BTreeSet<String>) -> Effects {
        let Some(plan) = self.tokens.as_ref() else {
            return self.write_credentials(index);
        };
        let reads = plan
            .buckets
            .iter()
            .flat_map(|bucket| {
                let key: Key = bucket.as_bytes().to_vec().into();
                [
                    (S3_BUCKET_KEYSPACE.to_string(), key.clone()),
                    (BUCKET_ENCRYPTION_KEYSPACE.to_string(), key),
                ]
            })
            .collect();
        self.state = CreateUserState::ReadTokenBuckets { index };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })]
    }

    /// Every bucket must exist and encrypt; then the authority of each is read.
    fn token_buckets_read(
        &mut self,
        event: Event,
        index: std::collections::BTreeSet<String>,
    ) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.handle_error(CreateUserError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchReadResult)",
                received: event,
            });
        };
        let caller = self.config.user_identity;
        let reads = match self.tokens.as_mut() {
            Some(plan) => plan.check_buckets(values, caller),
            None => Err(CreateUserError::CreateAccessFailed),
        };
        let reads = match reads {
            Ok(reads) => reads,
            Err(error) => return self.handle_error(error),
        };
        self.state = CreateUserState::ReadTokenAuthority { index };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })]
    }

    /// The caller must hold each key now; then the adapter seals them with one new token.
    fn token_authority_read(
        &mut self,
        event: Event,
        index: std::collections::BTreeSet<String>,
    ) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.handle_error(CreateUserError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchReadResult)",
                received: event,
            });
        };
        let caller = self.config.user_identity;
        let checked = match self.tokens.as_mut() {
            Some(plan) => plan.check_holders(values, caller),
            None => Err(CreateUserError::CreateAccessFailed),
        };
        let (keys, realm_id, node_id) = match checked {
            Ok(checked) => checked,
            Err(error) => return self.handle_error(error),
        };
        let Some(access_key) = self.access.as_ref().map(|access| access.access_key.clone()) else {
            return self.handle_error(CreateUserError::CreateAccessFailed);
        };
        self.state = CreateUserState::SealTokens { index };
        smallvec![Effect::Blob(BlobEffect::SealToken {
            keys,
            realm_id,
            node_id,
            access_key,
            created_by: caller,
        })]
    }

    /// Keeps the token for the output and writes the copies with the credential.
    fn tokens_sealed(
        &mut self,
        event: Event,
        index: std::collections::BTreeSet<String>,
    ) -> Effects {
        let caller = self.config.user_identity;
        let sealed = match (event, self.tokens.as_mut()) {
            (Event::Blob(BlobEvent::TokenSealed { copies, token }), Some(plan)) => {
                plan.sealed(copies, token, caller)
            }
            (Event::Blob(BlobEvent::Error(BlobError::BucketKey(error))), Some(plan)) => match error
            {
                BucketKeyError::Locked(id) => {
                    Err(CreateUserError::BucketLocked(plan.bucket_name(id)))
                }
                error => Err(BlobError::BucketKey(error).into()),
            },
            (Event::Blob(BlobEvent::Error(error)), _) => Err(error.into()),
            (received, _) => Err(CreateUserError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Blob(BlobEvent::TokenSealed)",
                received,
            }),
        };
        match sealed {
            Ok(()) => self.write_credentials(index),
            Err(error) => self.handle_error(error),
        }
    }

    fn handle_init(&mut self) -> Effects {
        if !matches!(self.state, CreateUserState::Init) {
            return self.abort();
        }
        if let Some(restrictions) = self.config.path_restrictions.as_deref()
            && let Err(err) = validate_restriction_limits(restrictions)
        {
            return self.handle_error(err.into());
        }
        let access_key = match UserAccess::build_access_key(&self.key_id) {
            Ok(access_key) => access_key,
            Err(err) => return self.handle_error(err.into()),
        };
        let plaintext = rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect::<String>();
        let mut access = UserAccess {
            access_key,
            user_identity: self.config.user_identity,
            group_id: self.config.group_id,
            secret: EncryptedS3Secret::empty(),
            expiry: self.config.expiry,
            path_restrictions: self.config.path_restrictions.clone(),
            issued_by: self.config.issued_by,
            revoked_at: None,
        };
        if let Err(err) = access.encrypt_secret(&self.encryption_key, &plaintext) {
            return self.handle_error(err.into());
        }

        self.pending_secret = Some(Secret::new(plaintext));
        self.access = Some(access);
        self.state = CreateUserState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.handle_error(CreateUserError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };
        self.txn_id = Some(txn_id);
        self.state = CreateUserState::ReadOwnerIndex;
        smallvec![crate::groups::fence::read_group_record(
            self.config.group_id,
            ACCESS_OWNER_KEYSPACE,
            owner_key(self.config.user_identity),
            txn_id,
        )]
    }

    fn handle_index(&mut self, event: Event) -> Effects {
        let value = match crate::groups::fence::parse_group_record(event) {
            Ok(value) => value,
            Err(error) => return self.handle_error(error.into()),
        };
        let index = match decode_index(value.as_ref()) {
            Ok(index) => index,
            Err(error) => return self.handle_error(error.into()),
        };
        let Some(txn_id) = self.txn_id else {
            return self.handle_error(CreateUserError::CreateAccessFailed);
        };
        let Some(new_access) = self.access.as_ref() else {
            return self.handle_error(CreateUserError::CreateAccessFailed);
        };
        let replace = index.contains(&new_access.access_key);
        let mut reads: Vec<_> = index
            .iter()
            .map(|access_key| {
                (
                    USER_ACCESS_KEYSPACE.to_string(),
                    access_key.as_bytes().into(),
                )
            })
            .collect();
        if !replace {
            reads.push((
                USER_ACCESS_KEYSPACE.to_string(),
                new_access.access_key.as_bytes().into(),
            ));
        }
        self.state = CreateUserState::ReadCredentials { index, replace };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    fn handle_credentials(
        &mut self,
        event: Event,
        index: std::collections::BTreeSet<String>,
        replace: bool,
    ) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.handle_error(CreateUserError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchReadResult)",
                received: event,
            });
        };
        if values.len() != index.len() + usize::from(!replace) {
            return self.handle_error(CreateUserError::IndexInconsistent);
        }

        let Some(new_access) = self.access.as_ref() else {
            return self.handle_error(CreateUserError::CreateAccessFailed);
        };
        let now = SystemTime::now();
        let mut active = std::collections::BTreeSet::new();
        let mut stale = Vec::new();
        for (key, value) in values {
            if !replace && key.as_ref() == new_access.access_key.as_bytes() {
                if value.is_some() {
                    return self.handle_error(CreateUserError::IndexInconsistent);
                }
                continue;
            }
            let Some(value) = value else {
                return self.handle_error(CreateUserError::IndexInconsistent);
            };
            let access = match UserAccess::from_bytes(value.as_ref()) {
                Ok(access) => access,
                Err(error) => return self.handle_error(error.into()),
            };
            if access.user_identity != self.config.user_identity
                || access.access_key.as_bytes() != key.as_ref()
                || !index.contains(&access.access_key)
            {
                return self.handle_error(CreateUserError::IndexInconsistent);
            }
            let stale_record = access.is_revoked() || access.is_expired(now);
            if replace && access.access_key == new_access.access_key && !stale_record {
                return self.handle_error(CreateUserError::IndexInconsistent);
            }
            if !stale_record {
                active.insert(access.access_key);
            } else {
                stale.push(access.access_key);
            }
        }
        if active.len() >= MAX_ACTIVE_CREDENTIALS {
            return self.handle_error(CreateUserError::LimitReached);
        }
        active.insert(new_access.access_key.clone());
        if !stale.is_empty() {
            let Some(txn_id) = self.txn_id else {
                return self.handle_error(CreateUserError::CreateAccessFailed);
            };
            self.state = CreateUserState::DeleteStale { index: active };
            self.stale_tokens = (stale.clone(), None);
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: stale
                    .into_iter()
                    .map(|access_key| {
                        (
                            USER_ACCESS_KEYSPACE.to_string(),
                            access_key.as_bytes().into(),
                        )
                    })
                    .collect(),
                txn_id: Some(txn_id),
            })];
        }
        self.read_token_buckets(active)
    }

    fn handle_stale_deleted(
        &mut self,
        event: Event,
        index: std::collections::BTreeSet<String>,
    ) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.handle_error(CreateUserError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                received: event,
            });
        };
        self.scan_stale_tokens(index)
    }

    /// The token copies of each deleted credential go in the same transaction, a page at a time.
    fn scan_stale_tokens(&mut self, index: std::collections::BTreeSet<String>) -> Effects {
        let (Some(txn_id), Some(access_key)) = (self.txn_id, self.stale_tokens.0.last()) else {
            return self.read_token_buckets(index);
        };
        let scan = token_scan(access_key, self.stale_tokens.1.take(), txn_id);
        self.state = CreateUserState::ScanStaleTokens { index };
        smallvec![scan]
    }

    fn stale_tokens_scanned(
        &mut self,
        event: Event,
        index: std::collections::BTreeSet<String>,
    ) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.handle_error(CreateUserError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };
        let (Some(txn_id), Some(access_key)) = (self.txn_id, self.stale_tokens.0.last()) else {
            return self.handle_error(CreateUserError::CreateAccessFailed);
        };
        if values.is_empty() {
            self.stale_tokens.0.pop();
            return self.scan_stale_tokens(index);
        }
        let deletes = match token_deletes(access_key, values) {
            Ok(deletes) => deletes,
            Err(error) => return self.handle_error(error.into()),
        };
        self.stale_tokens.1 = next_start_after;
        self.state = CreateUserState::DeleteStaleTokens { index };
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        })]
    }

    fn stale_tokens_deleted(
        &mut self,
        event: Event,
        index: std::collections::BTreeSet<String>,
    ) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.handle_error(CreateUserError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                received: event,
            });
        };
        // The last page of this credential leaves no cursor; the next credential follows.
        if self.stale_tokens.1.is_none() {
            self.stale_tokens.0.pop();
        }
        self.scan_stale_tokens(index)
    }

    fn write_credentials(&mut self, index: std::collections::BTreeSet<String>) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.handle_error(CreateUserError::CreateAccessFailed);
        };
        let Some(access) = self.access.as_ref() else {
            return self.handle_error(CreateUserError::CreateAccessFailed);
        };
        let bytes = match access.to_bytes() {
            Ok(bytes) => bytes,
            Err(err) => return self.handle_error(err.into()),
        };
        let index_value = match encode_index(&index) {
            Ok(value) => value,
            Err(err) => return self.handle_error(err.into()),
        };
        let mut writes = vec![
            (
                USER_ACCESS_KEYSPACE.to_string(),
                access.access_key.as_bytes().into(),
                bytes.into(),
            ),
            (
                ACCESS_OWNER_KEYSPACE.to_string(),
                owner_key(self.config.user_identity),
                index_value,
            ),
        ];
        if let Some(plan) = self.tokens.as_mut() {
            writes.append(&mut plan.written);
        }
        self.state = CreateUserState::WriteCredentials;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.handle_error(CreateUserError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchWriteResult)",
                received: event,
            });
        };

        let Some(access) = self.access.clone() else {
            return self.handle_error(CreateUserError::CreateAccessFailed);
        };
        let Some(secret) = self.pending_secret.take() else {
            return self.handle_error(CreateUserError::CreateAccessFailed);
        };
        self.output = Ok((access.access_key.clone(), secret, access));
        let Some(txn_id) = self.txn_id else {
            return self.handle_error(CreateUserError::CreateAccessFailed);
        };
        self.state = CreateUserState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;
                self.state = CreateUserState::Finish;
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => {
                if !matches!(&error, &StorageError::QueueFull) {
                    self.txn_id = None;
                }
                self.handle_error(error.into())
            }
            other => self.handle_error(CreateUserError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: other,
            }),
        }
    }

    pub fn handle_error(&mut self, error: CreateUserError) -> Effects {
        self.state = CreateUserState::Error;
        self.output = Err(error);
        self.abort()
    }
}

impl Operation for CreateUserOperation {
    type Output = (String, Secret, UserAccess);
    type Error = CreateUserError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            CreateUserState::Init => self.handle_init(),
            CreateUserState::StartTransaction => self.handle_started(event),
            CreateUserState::ReadOwnerIndex => self.handle_index(event),
            CreateUserState::ReadCredentials { ref index, replace } => {
                self.handle_credentials(event, index.clone(), replace)
            }
            CreateUserState::DeleteStale { ref index } => {
                self.handle_stale_deleted(event, index.clone())
            }
            CreateUserState::WriteCredentials => self.handle_written(event),
            CreateUserState::CommitTransaction => self.handle_committed(event),
            CreateUserState::Finish => smallvec![],
            CreateUserState::Error => self.abort(),
            CreateUserState::ReadTokenBuckets { ref index } => {
                self.token_buckets_read(event, index.clone())
            }
            CreateUserState::ReadTokenAuthority { ref index } => {
                self.token_authority_read(event, index.clone())
            }
            CreateUserState::SealTokens { ref index } => self.tokens_sealed(event, index.clone()),
            CreateUserState::ScanStaleTokens { ref index } => {
                self.stale_tokens_scanned(event, index.clone())
            }
            CreateUserState::DeleteStaleTokens { ref index } => {
                self.stale_tokens_deleted(event, index.clone())
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, CreateUserState::Finish | CreateUserState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        // A finished operation carries its success, a failed one its initiating
        // error; any other state is an explicit premature-finalization failure.
        match self.state {
            CreateUserState::Finish | CreateUserState::Error => self.output,
            _ => Err(CreateUserError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

/// Creates a credential with token copies; the output carries the token, shown only once.
#[derive(Debug, PartialEq)]
pub struct CreateTokenOperation(CreateUserOperation);

impl CreateTokenOperation {
    /// `operation` gets token copies of each of `buckets`; see `with_tokens`.
    pub fn new(
        operation: CreateUserOperation,
        buckets: Vec<String>,
        origin: (RealmId, NodeId),
        now_ms: u64,
    ) -> Self {
        Self(operation.with_tokens(buckets, origin, now_ms))
    }
}

impl Operation for CreateTokenOperation {
    type Output = (String, Secret, UserAccess, SharedSecret);
    type Error = CreateUserError;

    fn start(&mut self) -> Effects {
        self.0.start()
    }

    fn step(&mut self, event: Event) -> Effects {
        self.0.step(event)
    }

    fn is_complete(&self) -> bool {
        self.0.is_complete()
    }

    fn finalize(mut self) -> Result<Self::Output, Self::Error> {
        let token = self.0.tokens.as_mut().and_then(|plan| plan.token.take());
        let (access_key, secret, access) = self.0.finalize()?;
        let token = token.ok_or(CreateUserError::CreateAccessFailed)?;
        Ok((access_key, secret, access, token))
    }

    fn abort(&mut self) -> Effects {
        self.0.abort()
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use crate::s3::access::index::owner_key;
    use aruna_core::effects::IterStart;

    fn owner_read(op: &CreateUserOperation, value: Option<aruna_core::types::Value>) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (owner_key(op.config.user_identity), value),
                (op.config.group_id.to_bytes().into(), None),
            ],
        })
    }

    fn test_issuer() -> [u8; 32] {
        *iroh::SecretKey::from_bytes(&[9u8; 32]).public().as_bytes()
    }

    fn test_key() -> CredentialEncryptionKey {
        CredentialEncryptionKey::derive(&[9u8; 32])
    }

    /// A fixed far-future expiry, so an "active" fixture never depends on the
    /// wall clock that the production decision reads.
    fn active_expiry() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(4_000_000_000)
    }

    fn make_config(user_identity: UserId, group_id: GroupId) -> CreateUserConfig {
        CreateUserConfig {
            user_identity,
            group_id,
            expiry: active_expiry(),
            path_restrictions: None,
            issued_by: test_issuer(),
        }
    }

    fn make_user_identity() -> UserId {
        UserId::default()
    }

    #[test]
    fn creates_user_access() {
        let user_identity = make_user_identity();
        let group_id = Ulid::from_parts(1, 1);
        let mut op = CreateUserOperation::new(make_config(user_identity, group_id), test_key());

        // Start opens the transaction before the owner index is checked.
        let effects = op.start();
        assert_eq!(effects.len(), 1);
        assert_eq!(op.state, CreateUserState::StartTransaction);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::StartTransaction { read: false })
        ));

        let txn_id = Ulid::from_parts(2, 2);
        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(read_txn),
            ..
        }) = &effects[0]
        else {
            panic!("Expected owner index read");
        };
        assert_eq!(reads[0].0, aruna_core::keyspaces::ACCESS_OWNER_KEYSPACE);
        assert_eq!(reads[1].0, aruna_core::keyspaces::GROUP_DELETE_KEYSPACE);
        assert_eq!(*read_txn, txn_id);

        let Some(access) = op.access.as_ref() else {
            panic!("Expected generated access");
        };
        assert_eq!(access.user_identity, user_identity);
        assert_eq!(access.group_id, group_id);
        assert_eq!(access.open_secret(&test_key()).unwrap().len(), 30);
        assert_eq!(access.path_restrictions, None);
        assert_eq!(access.issued_by, test_issuer());
        assert_eq!(access.revoked_at, None);

        // An empty index still probes the fresh key for a collision in the txn.
        let access_key = access.access_key.clone();
        let effects = op.step(owner_read(&op, None));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchRead { txn_id: Some(id), .. })] if *id == txn_id
        ));
        let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(access_key.as_bytes().into(), None)],
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchWrite { txn_id: Some(id), .. })] if *id == txn_id
        ));
        let effects = op.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: id })] if *id == txn_id
        ));
        let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(effects.is_empty());
        assert_eq!(op.state, CreateUserState::Finish);
        assert!(op.is_complete());

        // 4. Finalize returns the tuple directly, never a nested result.
        let (access_key, plaintext, returned_access) =
            op.finalize().expect("finished operation finalizes");
        assert_eq!(returned_access.user_identity, user_identity);
        assert_eq!(returned_access.group_id, group_id);
        assert_eq!(returned_access.access_key, access_key);
        // The one-time plaintext opens the stored ciphertext on the issuing key.
        assert_eq!(
            returned_access.open_secret(&test_key()).unwrap(),
            plaintext.expose()
        );
    }

    #[test]
    fn replaces_stale() {
        let user_identity = make_user_identity();
        let stale_key = "newkey".to_string();
        let mut op = CreateUserOperation::new_with_key(
            make_config(user_identity, Ulid::from_parts(3, 3)),
            "newkey".to_string(),
            test_key(),
        );
        op.start();
        let txn_id = Ulid::from_parts(4, 4);
        op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        op.step(owner_read(
            &op,
            Some(encode_index(&std::collections::BTreeSet::from([stale_key.clone()])).unwrap()),
        ));
        let stale = UserAccess {
            access_key: stale_key.clone(),
            user_identity,
            group_id: Ulid::from_parts(5, 5),
            secret: EncryptedS3Secret::empty(),
            expiry: SystemTime::UNIX_EPOCH,
            path_restrictions: None,
            issued_by: test_issuer(),
            revoked_at: None,
        };
        let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(
                stale_key.clone().into(),
                Some(stale.to_bytes().unwrap().into()),
            )],
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchDelete { deletes, txn_id: Some(id) })]
                if *id == txn_id && deletes.len() == 1
        ));
        let effects = op.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        // The stale credential's token copies go in the same transaction: one full page, then
        // the rest after its cursor.
        let [
            Effect::Storage(StorageEffect::Iter {
                key_space,
                prefix,
                start: None,
                txn_id: Some(id),
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the token index scan, got {effects:?}");
        };
        assert_eq!((key_space.as_str(), *id), (TOKEN_INDEX_KEYSPACE, txn_id));
        let prefix = prefix.clone().unwrap();
        assert_eq!(
            prefix.as_ref(),
            TokenCopy::index_prefix(&stale_key).as_slice()
        );
        let copy = |bucket: u8| TokenCopy {
            key: BucketKeyRef::new(Ulid::from_bytes([bucket; 16]), 1),
            access_key: stale_key.clone(),
            created_by: user_identity,
            nonce: [0; 12],
            ciphertext: vec![0; 48],
            created_at_ms: 1,
        };
        let page = |bucket: u8, next: bool| {
            let key = Key::from(copy(bucket).index_key());
            Event::Storage(StorageEvent::IterResult {
                values: vec![(key.clone(), Value::from(Vec::new()))],
                next_start_after: next.then_some(key),
            })
        };
        let effects = op.step(page(1, true));
        let [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })] = effects.as_slice()
        else {
            panic!("expected the token deletes, got {effects:?}");
        };
        let deleted: Vec<_> = deletes
            .iter()
            .map(|(space, key)| (space.as_str(), key.to_vec()))
            .collect();
        assert_eq!(
            deleted,
            [
                (KEY_COPY_KEYSPACE, copy(1).key()),
                (TOKEN_INDEX_KEYSPACE, copy(1).index_key()),
            ]
        );
        let effects = op.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { start: Some(IterStart::After(after)), .. })]
                if after.as_ref() == copy(1).index_key().as_slice()
        ));
        op.step(page(2, false));
        let effects = op.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchWrite { txn_id: Some(id), .. })]
                if *id == txn_id
        ));
    }

    #[test]
    fn rejects_early_finalize() {
        let mut op = CreateUserOperation::new(
            make_config(make_user_identity(), Ulid::from_parts(20, 20)),
            test_key(),
        );
        op.start();

        assert_eq!(
            op.finalize(),
            Err(CreateUserError::NotFinished),
            "premature finalization must be an explicit failure"
        );
    }

    #[test]
    fn rejects_active_collision() {
        let user_identity = make_user_identity();
        let mut op = CreateUserOperation::new_with_key(
            make_config(user_identity, Ulid::from_parts(6, 6)),
            "newkey".to_string(),
            test_key(),
        );
        op.start();
        let txn_id = Ulid::from_parts(7, 7);
        op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        op.step(owner_read(
            &op,
            Some(encode_index(&std::collections::BTreeSet::from(["newkey".to_string()])).unwrap()),
        ));
        let access = UserAccess {
            access_key: "newkey".to_string(),
            user_identity,
            group_id: Ulid::from_parts(8, 8),
            secret: EncryptedS3Secret::empty(),
            expiry: active_expiry(),
            path_restrictions: None,
            issued_by: test_issuer(),
            revoked_at: None,
        };
        let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(
                "newkey".to_string().into(),
                Some(access.to_bytes().unwrap().into()),
            )],
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: id })]
                if *id == txn_id
        ));
        assert!(matches!(
            op.finalize().unwrap_err(),
            CreateUserError::IndexInconsistent
        ));
    }

    #[test]
    fn rejects_full_index() {
        let user_identity = make_user_identity();
        let keys = (0..MAX_ACTIVE_CREDENTIALS)
            .map(|index| format!("key{index}"))
            .collect::<std::collections::BTreeSet<_>>();
        let mut op = CreateUserOperation::new_with_key(
            make_config(user_identity, Ulid::from_parts(9, 9)),
            "newkey".to_string(),
            test_key(),
        );
        op.start();
        let txn_id = Ulid::from_parts(10, 10);
        op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        op.step(owner_read(&op, Some(encode_index(&keys).unwrap())));
        let mut values = keys
            .iter()
            .map(|key| {
                let access = UserAccess {
                    access_key: key.clone(),
                    user_identity,
                    group_id: Ulid::from_parts(11, 11),
                    secret: EncryptedS3Secret::empty(),
                    expiry: active_expiry(),
                    path_restrictions: None,
                    issued_by: test_issuer(),
                    revoked_at: None,
                };
                (key.clone().into(), Some(access.to_bytes().unwrap().into()))
            })
            .collect::<Vec<_>>();
        values.push(("newkey".to_string().into(), None));
        let effects = op.step(Event::Storage(StorageEvent::BatchReadResult { values }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: id })] if *id == txn_id
        ));
        assert!(matches!(
            op.finalize().unwrap_err(),
            CreateUserError::LimitReached
        ));
    }

    #[test]
    fn rejects_oversized_restrictions() {
        // Over the cap the operation fails with no storage write emitted.
        use aruna_core::permission_path::MAX_TOKEN_RESTRICTIONS;
        use aruna_core::structs::identity::auth::Permission;
        let restrictions = (0..=MAX_TOKEN_RESTRICTIONS)
            .map(|index| PathRestriction {
                pattern: format!("/r/{index}/**"),
                permission: Permission::READ,
            })
            .collect::<Vec<_>>();
        let mut config = make_config(make_user_identity(), Ulid::from_parts(12, 12));
        config.path_restrictions = Some(restrictions);
        let mut op = CreateUserOperation::new(config, test_key());

        let effects = op.start();
        assert!(effects.is_empty());
        assert_eq!(op.state, CreateUserState::Error);
        assert!(matches!(
            op.finalize().unwrap_err(),
            CreateUserError::RestrictionLimit(_)
        ));
    }

    #[test]
    fn rejects_invalid_steps() {
        let user_identity = make_user_identity();
        let group_id = Ulid::from_parts(13, 13);

        // Starting twice does not bypass the transaction state.
        let mut op = CreateUserOperation::new(make_config(user_identity, group_id), test_key());
        op.start();
        let effects = op.start();
        assert!(effects.is_empty());
        assert_eq!(op.state, CreateUserState::StartTransaction);

        // A wrong event aborts the open transaction and fails closed.
        let mut op = CreateUserOperation::new(make_config(user_identity, group_id), test_key());
        op.start();
        let key = Ulid::from_parts(14, 14).to_bytes().into();
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key,
            value: None,
        }));
        assert!(effects.is_empty());
        assert_eq!(op.state, CreateUserState::Error);
        assert!(matches!(
            op.finalize().unwrap_err(),
            CreateUserError::InvalidStateEvent { .. }
        ));
    }

    mod tokens {
        use super::*;
        use crate::s3::bucket::key::rows::authority_rows;
        use aruna_core::compute::SecretBytes;
        use aruna_core::structs::identity::realm::RealmId;
        use aruna_core::structs::storage::encryption::EncryptionMode;
        use aruna_core::structs::storage::format::Compression;

        const BUCKET_ID: Ulid = Ulid::from_bytes([4; 16]);

        fn user(seed: u8) -> UserId {
            UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
        }

        fn info() -> BucketInfo {
            BucketInfo {
                group_id: Ulid::from_bytes([3; 16]),
                created_at: SystemTime::UNIX_EPOCH,
                created_by: user(1),
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
            }
        }

        fn sealed() -> BucketEncryption {
            BucketEncryption {
                mode: EncryptionMode::VaultLocked,
                bucket_id: Some(BUCKET_ID),
                key_generation: 2,
                ..Default::default()
            }
        }

        fn rows(values: Vec<Option<Vec<u8>>>) -> Event {
            let values = values
                .into_iter()
                .map(|value| (Key::from(Vec::new()), value.map(Value::from)))
                .collect();
            Event::Storage(StorageEvent::BatchReadResult { values })
        }

        /// Runs a token credential of `caller` for bucket `sealed` up to its bucket read.
        fn started(caller: UserId) -> CreateTokenOperation {
            let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
            let config = make_config(caller, Ulid::from_bytes([3; 16]));
            let user_op = CreateUserOperation::new(config, test_key());
            let buckets = vec!["sealed".to_string()];
            let mut operation =
                CreateTokenOperation::new(user_op, buckets, (caller.realm_id, node), 9);
            operation.start();
            let txn_id = Ulid::from_bytes([9; 16]);
            operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
            operation.step(owner_read(&operation.0, None));
            let access_key = operation.0.access.as_ref().unwrap().access_key.clone();
            let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
                values: vec![(access_key.into(), None)],
            }));
            let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = effects.as_slice()
            else {
                panic!("expected the bucket read, got {effects:?}");
            };
            let spaces: Vec<_> = reads.iter().map(|(space, _)| space.as_str()).collect();
            assert_eq!(spaces, [S3_BUCKET_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE]);
            operation
        }

        /// Answers the bucket and authority reads; `grant` is the caller's stored grant.
        fn checked(
            caller: UserId,
            admins: &[UserId],
            grant: Option<Vec<u8>>,
        ) -> (CreateTokenOperation, Effects) {
            let mut operation = started(caller);
            let row = |value: Vec<u8>| Some(value);
            operation.step(rows(vec![
                row(info().to_bytes().unwrap()),
                row(sealed().to_bytes().unwrap()),
            ]));
            let authority = authority_rows(&info(), Some(&sealed()), admins);
            let documents = authority[2..]
                .iter()
                .map(|(_, value)| value.as_ref().map(|value| value.to_vec()));
            let mut values: Vec<_> = documents.collect();
            values.push(grant);
            let effects = operation.step(rows(values));
            (operation, effects)
        }

        fn failed(operation: CreateTokenOperation) -> CreateUserError {
            match operation.finalize() {
                Err(error) => error,
                Ok(_) => panic!("the credential was created"),
            }
        }

        #[test]
        fn creator_gets_token() {
            let (mut operation, effects) = checked(user(1), &[], None);
            let key = BucketKeyRef::new(BUCKET_ID, 2);
            let access_key = operation.0.access.as_ref().unwrap().access_key.clone();
            assert!(matches!(
                effects.as_slice(),
                [Effect::Blob(BlobEffect::SealToken { keys, access_key: named, created_by, .. })]
                    if *keys == [key] && *named == access_key && *created_by == user(1)
            ));
            let copy = TokenCopy {
                key,
                access_key: access_key.clone(),
                created_by: user(1),
                nonce: [0; 12],
                ciphertext: vec![0; 48],
                created_at_ms: 9,
            };
            let token = SharedSecret::new(SecretBytes::new(vec![7; 32]));
            let sealed = BlobEvent::TokenSealed {
                copies: vec![copy.clone()],
                token: token.clone(),
            };
            let effects = operation.step(Event::Blob(sealed));
            // Credential, owner index, copy, its index and one audit entry commit together.
            let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice()
            else {
                panic!("expected the credential writes, got {effects:?}");
            };
            let spaces: Vec<_> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
            assert_eq!(
                spaces,
                [
                    USER_ACCESS_KEYSPACE,
                    ACCESS_OWNER_KEYSPACE,
                    KEY_COPY_KEYSPACE,
                    TOKEN_INDEX_KEYSPACE,
                    aruna_core::keyspaces::BUCKET_AUDIT_KEYSPACE,
                ]
            );
            assert_eq!(writes[2].1.as_ref(), copy.key().as_slice());
            assert_eq!(TokenCopy::from_bytes(&writes[2].2).unwrap(), copy);
            assert_eq!(writes[3].1.as_ref(), copy.index_key().as_slice());
            let audit = BucketAuditRecord::from_bytes(&writes[4].2).unwrap();
            assert_eq!(
                (audit.action, audit.actor, audit.bucket_id, audit.generation),
                (AuditAction::TokenCreated, Some(user(1)), BUCKET_ID, Some(2))
            );
            operation.step(Event::Storage(StorageEvent::BatchWriteResult {
                entries: Vec::new(),
            }));
            let txn_id = Ulid::from_bytes([9; 16]);
            operation.step(Event::Storage(StorageEvent::TransactionCommitted {
                txn_id,
            }));
            let (created, _, _, returned) = operation.finalize().unwrap();
            assert_eq!((created, returned), (access_key, token));
        }

        #[test]
        fn holders_only() {
            // A former admin without a grant gets no token; an explicit grant or a current
            // admin role is enough.
            let (operation, effects) = checked(user(2), &[], None);
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::AbortTransaction { .. })]
            ));
            assert_eq!(
                failed(operation),
                CreateUserError::NotHolder("sealed".to_string())
            );
            let (_, effects) = checked(user(2), &[user(2)], None);
            assert!(matches!(
                effects.as_slice(),
                [Effect::Blob(BlobEffect::SealToken { .. })]
            ));
            let grant = BucketHolder {
                bucket_id: BUCKET_ID,
                user_id: user(2),
                origin: HolderOrigin::Explicit,
                state: aruna_core::structs::storage::encryption::GrantState::Ready,
                granted_by: user(1),
                granted_at_ms: 1,
            };
            let (_, effects) = checked(user(2), &[], Some(grant.to_bytes().unwrap()));
            assert!(matches!(
                effects.as_slice(),
                [Effect::Blob(BlobEffect::SealToken { .. })]
            ));
        }

        #[test]
        fn unusable_buckets_refused() {
            let mut operation = started(user(1));
            operation.step(rows(vec![None, None]));
            assert_eq!(
                failed(operation),
                CreateUserError::NoSuchBucket("sealed".to_string())
            );

            let mut operation = started(user(1));
            operation.step(rows(vec![Some(info().to_bytes().unwrap()), None]));
            assert_eq!(
                failed(operation),
                CreateUserError::NotEncrypted("sealed".to_string())
            );

            // A locked bucket fails the whole credential; nothing is written.
            let (mut operation, _) = checked(user(1), &[], None);
            let locked = BlobError::BucketKey(BucketKeyError::Locked(BUCKET_ID));
            let effects = operation.step(Event::Blob(BlobEvent::Error(locked)));
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::AbortTransaction { .. })]
            ));
            assert_eq!(
                failed(operation),
                CreateUserError::BucketLocked("sealed".to_string())
            );
        }
    }
}
