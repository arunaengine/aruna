//! Creates an S3 access key and encrypted secret for a user, with path limits and an expiry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::Secret;
use aruna_core::credential_encryption::{
    CredentialEncryptionKey, EncryptedS3Secret, EncryptionError,
};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{
    ACCESS_OWNER_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, S3_BUCKET_KEYSPACE, USER_ACCESS_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::permission_path::{RestrictionLimitError, validate_restriction_limits};
use aruna_core::rows::{PrefixPages, values};
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::identity::group_delete::check_group_write;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::key_audit::{
    AuditAction, AuditOutcome, BucketAuditRecord, next_event_id,
};
use aruna_core::types::{GroupId, Key, Value};
use rand::distr::Alphanumeric;
use rand::{RngExt, rng};
use std::collections::BTreeSet;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use ulid::Ulid;

use super::index::{
    MAX_ACTIVE_CREDENTIALS, decode_index, encode_index, owner_key, token_deletes, token_pages,
};
use crate::groups::fence::group_fence_key;
use crate::s3::bucket::key::rows::{Row, audit_row};

pub const DEFAULT_CREDENTIAL_TTL: Duration = Duration::from_secs(24 * 60 * 60 * 365);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreateStep {
    OwnerIndex,
    Credentials,
    DeleteStale,
    TokenPage,
    TokenDeletes,
    TokenBuckets,
    WriteCredentials,
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
    #[error("active credential limit reached")]
    LimitReached,
    #[error("credential owner index is inconsistent")]
    IndexInconsistent,
    #[error("User access creation failed")]
    CreateAccessFailed,
    #[error("bucket {0} does not exist")]
    NoSuchBucket(String),
    #[error("bucket {0} is not encrypted")]
    NotEncrypted(String),
    #[error("bucket {0} is not in the credential's group")]
    OtherGroup(String),
}

impl From<IoError> for CreateUserError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

type CreateTurn = Result<Next<(String, Secret, UserAccess)>, CreateUserError>;

#[derive(Debug, PartialEq)]
pub struct CreateUserConfig {
    pub user_identity: UserId,
    pub group_id: GroupId,
    pub expiry: SystemTime,
    pub path_restrictions: Option<Vec<PathRestriction>>,
    pub issued_by: [u8; 32],
}

/// Creates the credential, its owner index entry and audit rows in one transaction.
#[derive(Debug, PartialEq)]
pub struct CreateUserOperation {
    config: CreateUserConfig,
    key_id: String,
    encryption_key: CredentialEncryptionKey,
    pending_secret: Option<Secret>,
    access: Option<UserAccess>,
    step: CreateStep,
    /// The owner index as read, then the active credentials it will hold.
    index: BTreeSet<String>,
    replace: bool,
    tokens: Option<TokenPlan>,
    /// Deleted credentials whose token grants still go, and the pages of the current one.
    stale: Vec<String>,
    pages: Option<PrefixPages>,
    /// The administrator who issues this credential for another account, audited with it.
    issuer: Option<UserId>,
}

/// The encrypted buckets a new credential reads with its token, and the audit rows of each.
#[derive(Debug, Default, PartialEq)]
struct TokenPlan {
    buckets: Vec<String>,
    origin: Option<(RealmId, NodeId)>,
    now_ms: u64,
    written: Vec<Row>,
}

impl TokenPlan {
    /// Every bucket must exist in `group` and encrypt; each gets one audit entry.
    fn check_buckets(
        &mut self,
        values: Vec<Option<Value>>,
        caller: UserId,
        access_key: &str,
        group: GroupId,
    ) -> Result<(), CreateUserError> {
        let (_, node_id) = self.origin.ok_or(CreateUserError::CreateAccessFailed)?;
        let mut values = values.into_iter();
        for bucket in &self.buckets {
            let (Some(info), Some(settings)) = (values.next(), values.next()) else {
                return Err(CreateUserError::CreateAccessFailed);
            };
            let Some(info) = info else {
                return Err(CreateUserError::NoSuchBucket(bucket.clone()));
            };
            if BucketInfo::from_bytes(&info)?.group_id != group {
                return Err(CreateUserError::OtherGroup(bucket.clone()));
            }
            let active = BucketEncryption::from_row(settings.as_deref())?.active_key();
            let key = active.ok_or_else(|| CreateUserError::NotEncrypted(bucket.clone()))?;
            let audit = BucketAuditRecord {
                event_id: next_event_id(self.now_ms),
                bucket_id: key.bucket_id,
                at_ms: self.now_ms,
                action: AuditAction::TokenCreated,
                actor: Some(caller),
                node_id,
                generation: Some(key.generation),
                session_id: None,
                intent_id: None,
                sequence: None,
                deadline_ms: None,
                reason: Some(format!("token for access key {access_key}")),
                outcome: AuditOutcome::Applied,
            };
            self.written.push(audit_row(&audit)?);
        }
        Ok(())
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
            step: CreateStep::OwnerIndex,
            index: BTreeSet::new(),
            replace: false,
            tokens: None,
            stale: Vec::new(),
            pages: None,
            issuer: None,
        }
    }

    /// Records the credential in the admin audit as issued by `actor`, in its transaction.
    pub fn with_issuer(mut self, actor: UserId) -> Self {
        self.issuer = Some(actor);
        self
    }

    /// Checks that each of `buckets` exists and encrypts, in the credential's transaction. The
    /// caller then opens key requests for the credential's token key.
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

    fn call(&mut self, step: CreateStep, call: IoCall) -> CreateTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn new_access(&self) -> Result<&UserAccess, CreateUserError> {
        (self.access.as_ref()).ok_or(CreateUserError::CreateAccessFailed)
    }

    /// Builds the credential and its one-time secret before any storage call.
    fn prepare(&mut self) -> Result<(), CreateUserError> {
        if let Some(restrictions) = self.config.path_restrictions.as_deref() {
            validate_restriction_limits(restrictions)?;
        }
        let access_key = UserAccess::build_access_key(&self.key_id)?;
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
        access.encrypt_secret(&self.encryption_key, &plaintext)?;
        self.pending_secret = Some(Secret::new(plaintext));
        self.access = Some(access);
        Ok(())
    }

    /// Reads every indexed credential, and the new key unless the index already names it.
    fn index_read(&mut self, [value, fence]: [Option<Value>; 2]) -> CreateTurn {
        check_group_write(fence.as_deref())?;
        self.index = decode_index(value.as_ref())?;
        let new_key = self.new_access()?.access_key.clone();
        self.replace = self.index.contains(&new_key);
        let mut reads: Vec<_> = (self.index.iter())
            .map(|key| (USER_ACCESS_KEYSPACE.to_string(), key.as_bytes().into()))
            .collect();
        if !self.replace {
            reads.push((USER_ACCESS_KEYSPACE.to_string(), new_key.as_bytes().into()));
        }
        self.call(CreateStep::Credentials, IoCall::Read(reads))
    }

    fn credentials_read(&mut self, read: Vec<Option<Value>>) -> CreateTurn {
        let replace = self.replace;
        if read.len() != self.index.len() + usize::from(!replace) {
            return Err(CreateUserError::IndexInconsistent);
        }
        let new_key = self.new_access()?.access_key.clone();
        let index = std::mem::take(&mut self.index);
        let mut read = read.into_iter();
        let now = SystemTime::now();
        let mut active = BTreeSet::new();
        let mut stale = Vec::new();
        for key in &index {
            let value = read.next().flatten();
            let value = value.ok_or(CreateUserError::IndexInconsistent)?;
            let access = UserAccess::from_bytes(value.as_ref())?;
            if access.user_identity != self.config.user_identity || access.access_key != *key {
                return Err(CreateUserError::IndexInconsistent);
            }
            let stale_record = access.is_revoked() || access.is_expired(now);
            if replace && access.access_key == new_key && !stale_record {
                return Err(CreateUserError::IndexInconsistent);
            }
            match stale_record {
                true => stale.push(access.access_key),
                false => {
                    active.insert(access.access_key);
                }
            }
        }
        // A fresh key that already has a record collides with another credential.
        if read.next().flatten().is_some() {
            return Err(CreateUserError::IndexInconsistent);
        }
        if active.len() >= MAX_ACTIVE_CREDENTIALS {
            return Err(CreateUserError::LimitReached);
        }
        active.insert(new_key);
        self.index = active;
        if stale.is_empty() {
            return self.read_token_buckets();
        }
        let deletes = (stale.iter())
            .map(|key| (USER_ACCESS_KEYSPACE.to_string(), key.as_bytes().into()))
            .collect();
        self.stale = stale;
        self.call(CreateStep::DeleteStale, IoCall::Delete(deletes))
    }

    /// The token grants of each deleted credential go in the same transaction, a page at a time.
    fn scan_tokens(&mut self) -> CreateTurn {
        let Some(access_key) = self.stale.last() else {
            return self.read_token_buckets();
        };
        let pages = token_pages(access_key);
        let page = pages.call();
        self.pages = Some(pages);
        self.call(CreateStep::TokenPage, page)
    }

    fn token_page(&mut self, result: IoResult) -> CreateTurn {
        let pages = self
            .pages
            .as_mut()
            .ok_or(CreateUserError::CreateAccessFailed)?;
        let rows = pages.take(result)?;
        if rows.is_empty() {
            self.stale.pop();
            return self.scan_tokens();
        }
        let access_key = self
            .stale
            .last()
            .ok_or(CreateUserError::CreateAccessFailed)?;
        let deletes = token_deletes(access_key, rows)?;
        self.call(CreateStep::TokenDeletes, IoCall::Delete(deletes))
    }

    /// The last page of this credential leaves no cursor; the next credential follows.
    fn tokens_deleted(&mut self) -> CreateTurn {
        let pages = self
            .pages
            .as_ref()
            .ok_or(CreateUserError::CreateAccessFailed)?;
        if pages.done() {
            self.stale.pop();
            return self.scan_tokens();
        }
        let page = pages.call();
        self.call(CreateStep::TokenPage, page)
    }

    /// Reads each token bucket's record and settings in the credential's transaction.
    fn read_token_buckets(&mut self) -> CreateTurn {
        let Some(plan) = self.tokens.as_ref() else {
            return self.write_credentials();
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
        if let Some(actor) = self.issuer {
            let action = AdminAuditAction::ServiceCredentialIssued;
            let mut audit = AdminAuditRecord::new(action, actor, self.config.user_identity);
            audit.group_id = Some(self.config.group_id);
            audit.session_id = Some(access.access_key.clone());
            match audit.entries() {
                Ok(entries) => writes.extend(entries),
                Err(err) => return self.handle_error(err.into()),
            }
        }
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
                received: (event).label(),
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
                received: (other).label(),
            }),
        }
    }

    pub fn handle_error(&mut self, error: CreateUserError) -> Effects {
        self.state = CreateUserState::Error;
        self.output = Err(error);
        self.abort()
    }
}

impl EffectOperation for CreateUserOperation {
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
        abort_effects(&mut self.txn_id)
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use crate::s3::access::index::owner_key;
    use aruna_core::effects::IterStart;
    use aruna_core::keyspaces::{
        ABE_GRANT_KEYSPACE, ABE_REQUEST_KEYSPACE, ADMIN_AUDIT_KEYSPACE, TOKEN_GRANT_KEYSPACE,
    };
    use aruna_core::structs::storage::abe_access::token_prefix;

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
    fn issuer_is_audited() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let (owner, admin) = (
            UserId::new(Ulid::from_parts(1, 1), realm_id),
            UserId::default(),
        );
        let group_id = Ulid::from_parts(1, 2);
        let written = |op: CreateUserOperation| {
            let mut op = op;
            op.start();
            op.step(Event::Storage(StorageEvent::TransactionStarted {
                txn_id: Ulid::from_parts(2, 2),
            }));
            op.step(owner_read(&op, None));
            let access_key = op.access.as_ref().unwrap().access_key.clone();
            let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
                values: vec![(access_key.as_bytes().into(), None)],
            }));
            let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice()
            else {
                panic!("expected the credential writes, got {effects:?}");
            };
            (access_key, writes.clone())
        };
        let plain = CreateUserOperation::new(make_config(owner, group_id), test_key());
        let (_, writes) = written(plain);
        assert!(
            writes
                .iter()
                .all(|(space, _, _)| space != ADMIN_AUDIT_KEYSPACE)
        );

        let issued =
            CreateUserOperation::new(make_config(owner, group_id), test_key()).with_issuer(admin);
        let (access_key, writes) = written(issued);
        let audit: Vec<_> = writes
            .iter()
            .filter(|(space, _, _)| space == ADMIN_AUDIT_KEYSPACE)
            .collect();
        assert_eq!(audit.len(), 2, "one realm row and one group row");
        let record = AdminAuditRecord::from_bytes(&audit[1].2).unwrap();
        assert_eq!(record.action, AdminAuditAction::ServiceCredentialIssued);
        assert_eq!((record.actor, record.target), (admin, owner));
        assert_eq!(record.group_id, Some(group_id));
        assert_eq!(record.session_id, Some(access_key));
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
        // The stale credential's token grants go in the same transaction: one full page, then
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
        assert_eq!((key_space.as_str(), *id), (TOKEN_GRANT_KEYSPACE, txn_id));
        let prefix = prefix.clone().unwrap();
        assert_eq!(prefix.as_ref(), token_prefix(&stale_key).as_slice());
        let request = |bucket: u8| vec![bucket; 80];
        let row = |bucket: u8| [token_prefix(&stale_key), request(bucket)].concat();
        let page = |bucket: u8, next: bool| {
            let key = Key::from(row(bucket));
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
                (ABE_REQUEST_KEYSPACE, request(1)),
                (ABE_GRANT_KEYSPACE, request(1)),
                (TOKEN_GRANT_KEYSPACE, row(1)),
            ]
        );
        let effects = op.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { start: Some(IterStart::After(after)), .. })]
                if after.as_ref() == row(1).as_slice()
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
        use aruna_core::structs::identity::realm::RealmId;
        use aruna_core::structs::storage::blob::BucketInfo;
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
                distribution: None,
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
        fn started(caller: UserId) -> CreateUserOperation {
            let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
            let config = make_config(caller, Ulid::from_bytes([3; 16]));
            let buckets = vec!["sealed".to_string()];
            let mut operation = CreateUserOperation::new(config, test_key()).with_tokens(
                buckets,
                (caller.realm_id, node),
                9,
            );
            operation.start();
            let txn_id = Ulid::from_bytes([9; 16]);
            operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
            operation.step(owner_read(&operation, None));
            let access_key = operation.access.as_ref().unwrap().access_key.clone();
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

        fn failed(operation: CreateUserOperation) -> CreateUserError {
            match operation.finalize() {
                Err(error) => error,
                Ok(_) => panic!("the credential was created"),
            }
        }

        #[test]
        fn token_bucket_audited() {
            // Any caller may name an encrypted bucket, locked or not; key holders issue later.
            let mut operation = started(user(2));
            let effects = operation.step(rows(vec![
                Some(info().to_bytes().unwrap()),
                Some(sealed().to_bytes().unwrap()),
            ]));
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
                    aruna_core::keyspaces::BUCKET_AUDIT_KEYSPACE,
                ]
            );
            let audit = BucketAuditRecord::from_bytes(&writes[2].2).unwrap();
            assert_eq!(
                (audit.action, audit.actor, audit.bucket_id, audit.generation),
                (AuditAction::TokenCreated, Some(user(2)), BUCKET_ID, Some(2))
            );
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

            // A bucket of another group gets no token keys from this credential.
            let mut operation = started(user(1));
            let other = BucketInfo {
                group_id: Ulid::from_bytes([4; 16]),
                ..info()
            };
            operation.step(rows(vec![
                Some(other.to_bytes().unwrap()),
                Some(sealed().to_bytes().unwrap()),
            ]));
            assert_eq!(
                failed(operation),
                CreateUserError::OtherGroup("sealed".to_string())
            );
        }
    }
}
