//! Node-local key requests and grant admission for scoped encrypted reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod copies;
mod enumerate;
pub mod envelope;
mod epoch;
mod member;
mod reissue;
pub mod rekey;
mod requests;
mod snapshot;

use crate::users::vault_read::{ReadVaultConfig, ReadVaultOperation};
use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect, VaultQuery};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::*;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::identity::user::vault::{UserKeyRecord, VaultRecords};
use aruna_core::structs::storage::abe::{AbeEffect, AbeError, AbeEvent, AbeParameters};
use aruna_core::structs::storage::abe_access::*;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyError, BucketKeyRecord, EncryptionMode,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
pub use epoch::{EpochDueOperation, mark_due, marked};
pub use member::MemberKeysOperation;
pub use reissue::ReissueOperation;
use smallvec::smallvec;
use snapshot::Snapshot;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq)]
pub enum KeyAction {
    Request(KeyScope),
    Open(Option<Vec<u8>>),
    Grants(Option<Vec<u8>>),
    Publish(KeyGrant),
    /// Opens requests for a member's direct read scopes after a role grant by the caller.
    Member(aruna_core::UserId),
    /// Opens requests of the caller's new credential, sealed to its token key and narrowed by
    /// its path restrictions.
    Token {
        access_key: String,
        public_key: [u8; 32],
        restrictions: Option<Vec<PathRestriction>>,
    },
    /// Raises the bucket epoch: always when instant, else only when a raise is due and the caller
    /// holds the bucket key or the node manages it.
    Epoch {
        instant: bool,
    },
    /// Refuses all but unrestricted key holders, then raises a due epoch, before a re-key.
    Rekey,
    /// Opens enumerated grants for the current writes under a prefix that the caller may read.
    Writes(String),
}
#[derive(Debug, PartialEq)]
pub enum KeyResult {
    Request(KeyRequest),
    Requests(Vec<KeyRequest>, Option<Vec<u8>>),
    Grants(Vec<KeyGrant>, Option<Vec<u8>>),
    Grant(KeyGrant),
    Opened(Vec<Ulid>),
    Epoch(u64),
}
#[derive(Debug, Error, PartialEq)]
pub enum KeyError {
    #[error(transparent)]
    Abe(#[from] AbeError),
    #[error("the bucket or key request was not found")]
    Missing,
    #[error("access to the encryption record is denied")]
    Denied,
    #[error("users of another realm cannot hold this realm's encryption keys")]
    Foreign,
    #[error("encryption storage is unavailable")]
    Storage,
    #[error("a re-key of another prefix is unfinished in this bucket")]
    Busy,
    #[error("the bucket key is locked on this node")]
    Locked,
    #[error("the prefix holds at least {0} more readable files than one enumerated grant names")]
    Bound(usize),
    #[error("a file in this folder is still being uploaded or converted; try again shortly")]
    Unfinished,
    #[error(
        "a read policy depends on the session, so ask for this grant while the bucket is unlocked"
    )]
    Session,
}
#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
    Init,
    Start,
    Bucket,
    Settings,
    Snapshot,
    Unlock,
    Keys,
    Credential,
    Records,
    Existing,
    Reuse,
    Issue,
    Count,
    Holders,
    Cleanup,
    Write,
    Commit,
    Drain,
    Done,
    Reissue,
    Heads,
    Writes,
    Envelopes,
}
#[derive(Debug, PartialEq)]
pub struct KeyOperation {
    deletes: Vec<(String, Key)>,
    writes: Vec<(String, Key, Value)>,
    bucket: String,
    auth: AuthContext,
    node: NodeId,
    action: KeyAction,
    now: u64,
    txn: Option<TxnId>,
    state: State,
    info: Option<BucketInfo>,
    snapshot: Option<Snapshot>,
    recipient_keys: Vec<UserKeyRecord>,
    keys: Option<ReadVaultOperation>,
    request: Option<KeyRequest>,
    queue_full: bool,
    scopes: Vec<KeyScope>,
    /// The epoch of each enumerated scope in `scopes`, and of the one being requested.
    groups: Vec<u64>,
    group_epoch: Option<u64>,
    listed: Vec<(String, Ulid)>,
    opened: Vec<Ulid>,
    reused: bool,
    more: bool,
    fresh: bool,
    notify: bool,
    quiet: bool,
    managed: bool,
    result: Option<KeyResult>,
    output: Option<Result<KeyResult, KeyError>>,
}
impl KeyOperation {
    pub fn new(
        bucket: String,
        auth: AuthContext,
        node: NodeId,
        action: KeyAction,
        now: u64,
    ) -> Self {
        Self {
            deletes: Vec::new(),
            writes: Vec::new(),
            bucket,
            auth,
            node,
            action,
            now,
            txn: None,
            state: State::Init,
            info: None,
            snapshot: None,
            recipient_keys: Vec::new(),
            keys: None,
            request: None,
            queue_full: false,
            scopes: Vec::new(),
            groups: Vec::new(),
            group_epoch: None,
            listed: Vec::new(),
            opened: Vec::new(),
            reused: false,
            more: false,
            fresh: false,
            notify: false,
            quiet: false,
            managed: false,
            result: None,
            output: None,
        }
    }
    /// Sends no holder notifications and walks no reissue pages, for reissue's own runs.
    pub fn quiet(mut self) -> Self {
        self.quiet = true;
        self
    }
    fn fail(&mut self, error: impl Into<KeyError>) -> Effects {
        self.state = State::Done;
        self.output = Some(Err(error.into()));
        self.abort()
    }
    fn read(&self, space: &str, key: Vec<u8>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: space.to_string(),
            key: key.into(),
            txn_id: self.txn
        })]
    }
    fn fenced(&self, mut effects: Effects) -> Effects {
        for effect in &mut effects {
            if let Effect::Storage(
                StorageEffect::Read { txn_id, .. } | StorageEffect::Iter { txn_id, .. },
            ) = effect
            {
                *txn_id = self.txn;
            }
        }
        effects
    }
    fn newest(&self) -> Option<&UserKeyRecord> {
        self.recipient_keys
            .iter()
            .max_by_key(|k| (k.created_at_ms, k.record_id))
    }
    fn settings_read(&mut self, value: Option<Value>) -> Effects {
        let settings = BucketEncryption::from_row(value.as_deref()).ok();
        let Some(key) = settings.as_ref().and_then(|s| s.active_key()) else {
            return self.fail(KeyError::Missing);
        };
        self.managed = settings.is_some_and(|s| s.mode == EncryptionMode::NodeManaged);
        if let KeyAction::Publish(grant) = &self.action
            && grant.context.request.parameters.key != key
        {
            return self.fail(AbeError::Stale);
        }
        let Some(info) = &self.info else {
            return self.fail(KeyError::Missing);
        };
        let realm = self.auth.realm_id.as_bytes().to_vec();
        let holder = [
            key.bucket_id.to_bytes().to_vec(),
            self.auth.user_id.to_storage_key(),
        ];
        self.state = State::Snapshot;
        let bucket_id = key.bucket_id.to_bytes().to_vec();
        // Raises and node issuance read the due row; listing and grant reads never conflict.
        let reads_due = !matches!(
            self.action,
            KeyAction::Open(_) | KeyAction::Grants(_) | KeyAction::Publish(_)
        );
        let due = reads_due.then(|| (ABE_DUE_KEYSPACE.to_string(), bucket_id.into()));
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (AUTH_KEYSPACE.to_string(), realm.clone().into()),
                (
                    AUTH_KEYSPACE.to_string(),
                    info.group_id.to_bytes().to_vec().into()
                ),
                (REALM_CONFIG_KEYSPACE.to_string(), realm.into()),
                (ABE_PARAMETERS_KEYSPACE.to_string(), key.key().into()),
                (
                    ABE_EPOCH_KEYSPACE.to_string(),
                    key.bucket_id.to_bytes().to_vec().into()
                ),
                (BUCKET_KEY_KEYSPACE.to_string(), key.key().into()),
                (BUCKET_HOLDER_KEYSPACE.to_string(), holder.concat().into()),
                (
                    USER_KEYSPACE.to_string(),
                    self.recipient().to_bytes().into()
                )
            ]
            .into_iter()
            .chain(due)
            .collect(),
            txn_id: self.txn
        })]
    }
    /// Opens or issues the requests of a request, member or token action under the snapshot epoch.
    fn prepare(&mut self) -> Effects {
        if matches!(self.action, KeyAction::Member(_) | KeyAction::Token { .. }) {
            match self.member_scopes() {
                Ok(scopes) if scopes.is_empty() => {
                    self.result = Some(KeyResult::Opened(Vec::new()));
                    return self.flush();
                }
                Ok(scopes) => self.scopes = scopes,
                Err(error) => return self.fail(error),
            }
        }
        if let KeyAction::Writes(prefix) = &self.action {
            let prefix = prefix.clone();
            return self.list_heads(&prefix);
        }
        if let Some(access_key) = self.token_credential() {
            self.state = State::Credential;
            return self.read(USER_ACCESS_KEYSPACE, access_key.into_bytes());
        }
        self.read_keys()
    }
    /// Reads the recipient's user keys from their vault.
    fn read_keys(&mut self) -> Effects {
        let mut keys = ReadVaultOperation::new(ReadVaultConfig {
            node_id: self.node,
            user_id: self.recipient(),
            query: VaultQuery::Keys,
            deadline: std::time::Duration::from_secs(10),
        });
        self.state = State::Keys;
        let effects = self.fenced(keys.start());
        self.keys = Some(keys);
        effects
    }
    fn keys_read(&mut self, event: Event) -> Effects {
        let Some(keys) = self.keys.as_mut() else {
            return self.fail(KeyError::Storage);
        };
        let effects = keys.step(event);
        if !keys.is_complete() {
            return self.fenced(effects);
        }
        let records = match self.keys.take().map(|k| k.finalize()) {
            Some(Ok(VaultRecords::Keys(records))) => records,
            _ => return self.fail(KeyError::Storage),
        };
        if records
            .iter()
            .any(|r| r.user_id != self.recipient() || r.validate().is_err())
        {
            return self.fail(AbeError::Context);
        }
        self.recipient_keys = records;
        self.records()
    }
    fn records(&mut self) -> Effects {
        let Some(snapshot) = &self.snapshot else {
            return self.fail(KeyError::Missing);
        };
        let bucket = snapshot.parameters.key.bucket_id.to_bytes().to_vec();
        let own = [bucket.clone(), self.recipient().to_storage_key()].concat();
        let (space, prefix, cursor) = match &self.action {
            KeyAction::Publish(grant) => {
                self.state = State::Records;
                return self.read(ABE_REQUEST_KEYSPACE, grant.context.request.key());
            }
            KeyAction::Open(_) if !snapshot.holder => return self.fail(KeyError::Denied),
            KeyAction::Open(cursor) => (ABE_REQUEST_KEYSPACE, bucket, cursor.clone()),
            KeyAction::Request(_)
            | KeyAction::Member(_)
            | KeyAction::Token { .. }
            | KeyAction::Writes(_) => (ABE_REQUEST_KEYSPACE, own, None),
            KeyAction::Grants(cursor) => (ABE_GRANT_KEYSPACE, own, cursor.clone()),
            KeyAction::Epoch { .. } | KeyAction::Rekey => return self.fail(AbeError::Context),
        };
        self.state = State::Records;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: space.to_string(),
            prefix: Some(prefix.into()),
            start: cursor.map(|v| IterStart::After(v.into())),
            limit: MAX_REQUESTS + 1,
            txn_id: self.txn
        })]
    }
    fn existing_read(&mut self, value: Option<Value>) -> Effects {
        let KeyAction::Publish(submitted) = &self.action else {
            return self.fail(AbeError::Context);
        };
        match value.map(|v| KeyGrant::from_bytes(&v)) {
            Some(Ok(grant)) if grant.context.request == submitted.context.request => {
                if let Err(error) = self.grant_allowed(&grant.context.request) {
                    return self.fail(error);
                }
                self.result = Some(KeyResult::Grant(grant));
                self.flush()
            }
            Some(_) => self.fail(AbeError::Stale),
            None => self.fail(KeyError::Missing),
        }
    }
    fn flush(&mut self) -> Effects {
        if !self.deletes.is_empty() {
            self.state = State::Cleanup;
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: std::mem::take(&mut self.deletes),
                txn_id: self.txn
            })];
        }
        if !self.writes.is_empty() {
            self.state = State::Write;
            return smallvec![Effect::Storage(StorageEffect::BatchWrite {
                writes: std::mem::take(&mut self.writes),
                txn_id: self.txn
            })];
        }
        // Older epochs left uncovered get the next bounded batch for the same scope.
        if self.more
            && let Some(KeyResult::Grant(grant)) = &self.result
        {
            self.more = false;
            return self.next_batch(grant.context.request.clone());
        }
        // A member's own new request notifies holders once; reused requests send nothing.
        if matches!(self.action, KeyAction::Request(_))
            && matches!(self.result, Some(KeyResult::Request(_)))
            && !self.reused
            && !self.quiet
            && !std::mem::replace(&mut self.fresh, true)
            && let Some(effects) = self.holders()
        {
            return effects;
        }
        if matches!(
            self.action,
            KeyAction::Member(_) | KeyAction::Token { .. } | KeyAction::Writes(_)
        ) && !matches!(self.result, Some(KeyResult::Opened(_)))
        {
            return self.next_scope();
        }
        let Some(txn_id) = self.txn else {
            return self.fail(KeyError::Storage);
        };
        self.state = State::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}
impl Operation for KeyOperation {
    type Output = KeyResult;
    type Error = KeyError;
    fn start(&mut self) -> Effects {
        if self.auth.federated() {
            return self.fail(KeyError::Foreign);
        }
        if self.auth.user_id.is_nil() {
            return self.fail(KeyError::Denied);
        }
        self.state = State::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }
    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn = Some(txn_id);
                self.state = State::Bucket;
                self.read(S3_BUCKET_KEYSPACE, self.bucket.as_bytes().to_vec())
            }
            (State::Bucket, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let Some(info) = value.as_ref().and_then(|v| BucketInfo::from_bytes(v).ok()) else {
                    return self.fail(KeyError::Missing);
                };
                if info.created_by.realm_id != self.auth.realm_id {
                    return self.fail(KeyError::Denied);
                }
                self.info = Some(info);
                self.state = State::Settings;
                self.read(BUCKET_ENCRYPTION_KEYSPACE, self.bucket.as_bytes().to_vec())
            }
            (State::Settings, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.settings_read(value)
            }
            (State::Snapshot, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                if let Err(error) = self.snapshot_read(values) {
                    return self.fail(error);
                }
                if matches!(self.action, KeyAction::Open(_)) {
                    return self.records();
                }
                match self.action {
                    KeyAction::Epoch { instant } => return self.raise(instant),
                    KeyAction::Rekey => return self.raise(false),
                    _ => {}
                }
                if self.snapshot.as_ref().is_some_and(|s| s.due) {
                    return self.due_check();
                }
                self.prepare()
            }
            (State::Unlock, Event::Blob(BlobEvent::KeyStatus { generations })) => {
                let key = self.snapshot.as_ref().map(|s| s.parameters.key);
                if generations.iter().any(|g| g.active && Some(g.key) == key) {
                    return self.raise_first();
                }
                self.prepare()
            }
            (State::Keys, event) => self.keys_read(event),
            (State::Credential, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.credential_read(value)
            }
            (State::Records, event) => self.records_read(event),
            (State::Existing, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.existing_read(value)
            }
            (State::Reuse, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.reuse_read(values)
            }
            (State::Count, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.count_read(values)
            }
            (State::Holders, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.notify_holders(values)
            }
            // A key unlocked after the locked status read cannot issue before the due raise: retry.
            (State::Issue, Event::Blob(BlobEvent::Abe(_)))
                if self.snapshot.as_ref().is_some_and(|s| s.due) =>
            {
                self.fail(KeyError::Storage)
            }
            (State::Issue, Event::Blob(BlobEvent::Abe(event))) => match *event {
                AbeEvent::Grant(grant) => self.publish_grant(grant),
                _ => self.fail(AbeError::Context),
            },
            (
                State::Issue,
                Event::Blob(BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(_)))),
            ) => match self.request.take() {
                Some(request) => {
                    self.result = Some(KeyResult::Request(request.clone()));
                    self.write_request(request)
                }
                None => self.fail(KeyError::Missing),
            },
            (State::Issue, Event::Blob(BlobEvent::Error(BlobError::Abe(error)))) => {
                self.fail(error)
            }
            (State::Cleanup, Event::Storage(StorageEvent::BatchDeleteResult { .. }))
            | (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => self.flush(),
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { txn_id }))
                if self.txn == Some(txn_id) =>
            {
                self.txn = None;
                if self.notify {
                    self.state = State::Drain;
                    return smallvec![crate::notifications::outbox::schedule_drain_effect()];
                }
                self.walk()
            }
            // Delivery is retried from the stored outbox, so a lost drain timer only delays it.
            (State::Drain, Event::Task(_)) => self.walk(),
            // One page per run bounds the request; saved progress resumes on the next issuance.
            (State::Reissue, Event::SubOperation(SubOperationEvent::ReissuePaged { .. })) => {
                self.state = State::Done;
                self.output = self.result.take().map(Ok);
                smallvec![]
            }
            (State::Heads, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.heads_read(values)
            }
            (State::Writes, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.writes_read(values)
            }
            (State::Envelopes, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.envelopes_read(values)
            }
            (_, Event::Storage(StorageEvent::Error { .. })) => self.fail(KeyError::Storage),
            _ => self.fail(AbeError::Context),
        }
    }
    fn is_complete(&self) -> bool {
        self.state == State::Done
    }
    fn finalize(self) -> Result<KeyResult, KeyError> {
        self.output.unwrap_or(Err(AbeError::Context.into()))
    }
    fn abort(&mut self) -> Effects {
        self.txn.take().map_or_else(Effects::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::permission_rules::{CollectedRole, PermissionRules};
    use aruna_core::structs::identity::auth::{Permission, Role};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::encryption::BucketKeyRef;

    #[test]
    fn refuses_foreign_user() {
        // A federated user served by realm 1 cannot request keys there.
        let foreign = aruna_core::UserId::new(Ulid::from_bytes([5; 16]), RealmId([9; 32]));
        let auth = AuthContext {
            user_id: foreign,
            realm_id: RealmId([1; 32]),
            path_restrictions: None,
            session: None,
        };
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let action = KeyAction::Request(KeyScope::Subtree("foo/".into()));
        let mut operation = KeyOperation::new("bucket".into(), auth, node, action, 1);
        assert!(operation.start().is_empty());
        assert_eq!(operation.finalize(), Err(KeyError::Foreign));
    }

    #[test]
    fn due_refuses_grant() {
        let realm_id = RealmId([1; 32]);
        let user = aruna_core::UserId::new(Ulid::from_bytes([5; 16]), realm_id);
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let info = BucketInfo {
            group_id: Ulid::from_bytes([8; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: user,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
        };
        let parameters = AbeParameters {
            realm_id,
            node_id: node,
            key: BucketKeyRef::new(Ulid::from_bytes([4; 16]), 1),
            fingerprint: [7; 32],
            parameters: vec![9; 3],
        };
        let scope = KeyScope::Subtree("foo/".into());
        let request = KeyRequest {
            request_id: Ulid::from_bytes([1; 16]),
            requesting_user: user,
            recipient_user: user,
            recipient_record: None,
            recipient_public: None,
            recipient_fingerprint: None,
            bucket: "bucket".into(),
            parameters: parameters.clone(),
            scope: scope.clone(),
            epochs: vec![1],
            credential_id: None,
            restrictions: None,
            revisions: Vec::new(),
            created_at_ms: 1,
        };
        let auth = AuthContext {
            user_id: user,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let action = KeyAction::Request(scope);
        let mut operation = KeyOperation::new("bucket".into(), auth, node, action, 1);
        let root = aruna_core::structs::storage::blob::bucket_permission_path(
            realm_id,
            info.group_id,
            node,
            "bucket",
        );
        let role = CollectedRole {
            role: Role {
                role_id: Ulid::from_bytes([2; 16]),
                name: "reader".into(),
                permissions: [(format!("{root}/**"), Permission::READ)].into(),
                assigned_users: [user].into(),
            },
            direct: true,
            public: false,
        };
        let txn_id = TxnId::generate();
        operation.info = Some(info);
        operation.txn = Some(txn_id);
        operation.state = State::Issue;
        operation.request = Some(request.clone());
        // The status read saw a locked key and left the raise due; an unlock then let Issue succeed.
        operation.snapshot = Some(Snapshot {
            parameters,
            epoch: 1,
            revisions: Vec::new(),
            rules: PermissionRules::from_roles(vec![role], None).unwrap(),
            holder: false,
            holders: Default::default(),
            policies: false,
            due: true,
            checks: Default::default(),
        });
        let issuer = KeyIssuer::Node(node);
        let grant = KeyGrant {
            context: GrantContext { request, issuer },
            enc: [0; 32],
            ciphertext: Vec::new(),
        };
        let event = Event::Blob(BlobEvent::Abe(Box::new(AbeEvent::Grant(grant))));
        let effects = operation.step(event);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: id })] if *id == txn_id
        ));
        assert_eq!(operation.finalize(), Err(KeyError::Storage));
    }

    /// A token run at epoch 3 whose snapshot admits READ on `pattern`, and its foo/ request.
    fn merge_fixture(pattern: &str) -> (KeyOperation, KeyRequest) {
        let realm_id = RealmId([1; 32]);
        let user = aruna_core::UserId::new(Ulid::from_bytes([5; 16]), realm_id);
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let group_id = Ulid::from_bytes([8; 16]);
        let key = BucketKeyRef::new(Ulid::from_bytes([4; 16]), 1);
        let secret = aruna_core::compute::SecretBytes::new(vec![9; 32]);
        let parameters =
            aruna_core::structs::storage::abe::create_parameters(&secret, realm_id, node, key)
                .unwrap();
        let access_key = Ulid::from_bytes([3; 16]).to_string();
        let (record, public, fingerprint) = token_recipient(&access_key, [1; 32]).unwrap();
        let request = KeyRequest {
            request_id: Ulid::from_bytes([1; 16]),
            requesting_user: user,
            recipient_user: user,
            recipient_record: Some(record),
            recipient_public: Some(public),
            recipient_fingerprint: Some(fingerprint),
            bucket: "bucket".into(),
            parameters: parameters.clone(),
            scope: KeyScope::Subtree("foo/".into()),
            epochs: vec![3],
            credential_id: Some(access_key.clone()),
            restrictions: None,
            revisions: Vec::new(),
            created_at_ms: 1,
        };
        let auth = AuthContext {
            user_id: user,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let action = KeyAction::Token {
            access_key,
            public_key: [1; 32],
            restrictions: None,
        };
        let mut operation = KeyOperation::new("bucket".into(), auth, node, action, 1);
        let root = aruna_core::structs::storage::blob::bucket_permission_path(
            realm_id, group_id, node, "bucket",
        );
        let role = CollectedRole {
            role: Role {
                role_id: Ulid::from_bytes([2; 16]),
                name: "reader".into(),
                permissions: [(format!("{root}/{pattern}"), Permission::READ)].into(),
                assigned_users: [user].into(),
            },
            direct: true,
            public: false,
        };
        operation.info = Some(BucketInfo {
            group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: user,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
        });
        operation.txn = Some(TxnId::generate());
        operation.snapshot = Some(Snapshot {
            parameters,
            epoch: 3,
            revisions: Vec::new(),
            rules: PermissionRules::from_roles(vec![role], None).unwrap(),
            holder: false,
            holders: Default::default(),
            policies: false,
            due: false,
            checks: Default::default(),
        });
        (operation, request)
    }

    /// The stored grant row of `request` under request id `id` with `epochs`.
    fn held(request: &KeyRequest, id: u8, epochs: &[u64]) -> (Key, Value) {
        let mut request = request.clone();
        request.request_id = Ulid::from_bytes([id; 16]);
        request.epochs = epochs.to_vec();
        let grant = KeyGrant {
            context: GrantContext {
                issuer: KeyIssuer::Node(request.parameters.node_id),
                request,
            },
            enc: [0; 32],
            ciphertext: vec![0; 16],
        };
        let key = grant.context.request.key().into();
        (key, grant.to_bytes().unwrap().into())
    }

    fn issued(request: &KeyRequest, epochs: &[u64]) -> Event {
        let mut request = request.clone();
        request.request_id = Ulid::from_bytes([20; 16]);
        request.epochs = epochs.to_vec();
        let grant = KeyGrant {
            context: GrantContext {
                issuer: KeyIssuer::Node(request.parameters.node_id),
                request,
            },
            enc: [0; 32],
            ciphertext: vec![0; 16],
        };
        Event::Blob(BlobEvent::Abe(Box::new(AbeEvent::Grant(grant))))
    }

    #[test]
    fn merges_same_context() {
        let (mut operation, request) = merge_fixture("**");
        let mut other = request.clone();
        other.scope = KeyScope::Subtree("bar/".into());
        let mut narrowed = request.clone();
        narrowed.restrictions = Some(vec![PathRestriction {
            pattern: "foo/**".into(),
            permission: Permission::READ,
        }]);
        let mut revised = request.clone();
        revised.revisions = vec![[1; 32]];
        let values = vec![
            held(&request, 11, &[1]),
            held(&request, 12, &[2]),
            held(&other, 13, &[1]),
            held(&narrowed, 14, &[1, 2]),
            held(&revised, 15, &[1]),
        ];
        operation.request = Some(request.clone());
        // Both foo/ grants join the new epoch; other scopes, restrictions and revisions stay apart.
        let effects = operation.reuse_read(values.clone());
        let [Effect::Blob(BlobEffect::Abe(effect))] = effects.as_slice() else {
            panic!("one issuance: {effects:?}");
        };
        let AbeEffect::Issue(context) = effect.as_ref() else {
            panic!("an issue effect");
        };
        assert_eq!(context.request.epochs, vec![1, 2, 3]);

        // Publication deletes exactly the merged parts and their token rows in its transaction.
        assert!(matches!(
            operation.step(issued(&request, &[1, 2, 3])).as_slice(),
            [Effect::Storage(StorageEffect::Iter { .. })]
        ));
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: values.clone(),
            next_start_after: None,
        }));
        let [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })] = effects.as_slice()
        else {
            panic!("one delete batch: {effects:?}");
        };
        let grants: Vec<&Key> = deletes
            .iter()
            .filter(|(space, _)| space == ABE_GRANT_KEYSPACE)
            .map(|(_, key)| key)
            .collect();
        assert_eq!(grants, vec![&values[0].0, &values[1].0]);
        let rows = deletes.iter().filter(|(s, _)| s == TOKEN_GRANT_KEYSPACE);
        assert_eq!(rows.count(), 2);
    }

    #[test]
    fn refused_merge_keeps() {
        // READ moved off foo/ before publication: the merge is refused and no grant is deleted.
        let (mut operation, request) = merge_fixture("bar/**");
        operation.state = State::Issue;
        let effects = operation.step(issued(&request, &[1, 2, 3]));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert!(operation.deletes.is_empty() && operation.writes.is_empty());
        assert_eq!(operation.finalize(), Err(KeyError::Denied));
    }

    #[test]
    fn fences_inactive_recipients() {
        // A deactivated user or a credential issued before the user cutoff gets no new grant.
        use aruna_core::structs::identity::group::GroupAuthorizationDocument;
        use aruna_core::structs::identity::realm::{
            RealmAuthorizationDocument, RealmConfigDocument, TokenRevocation,
        };
        use aruna_core::structs::identity::user::User;
        let realm_id = RealmId([1; 32]);
        let user = aruna_core::UserId::new(Ulid::from_bytes([5; 16]), realm_id);
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let actor = aruna_core::structs::identity::auth::Actor {
            node_id: node,
            user_id: user,
            realm_id,
        };
        let group_id = Ulid::from_bytes([8; 16]);
        let key = BucketKeyRef::new(Ulid::from_bytes([4; 16]), 1);
        let secret = aruna_core::compute::SecretBytes::new(vec![9; 32]);
        let parameters =
            aruna_core::structs::storage::abe::create_parameters(&secret, realm_id, node, key)
                .unwrap();
        let values = |account: &User, config: &RealmConfigDocument| -> Vec<(Key, Option<Value>)> {
            let rows: [Option<Vec<u8>>; 8] = [
                Some(
                    RealmAuthorizationDocument::default_realm_doc(realm_id)
                        .to_bytes(&actor)
                        .unwrap(),
                ),
                Some(
                    GroupAuthorizationDocument::default_group_doc(user, realm_id, group_id)
                        .to_bytes(&actor)
                        .unwrap(),
                ),
                Some(config.to_bytes(&actor).unwrap()),
                Some(parameters.to_bytes().unwrap()),
                Some(1u64.to_be_bytes().to_vec()),
                Some(
                    BucketKeyRecord::new(key, Ulid::from_bytes([3; 16]), [0; 32], 1)
                        .to_bytes()
                        .unwrap(),
                ),
                None,
                Some(account.to_bytes(&actor).unwrap()),
            ];
            rows.into_iter()
                .map(|v| (Key::from(Vec::new()), v.map(Value::from)))
                .collect()
        };
        let snapshot = |action: KeyAction, account: &User, config: &RealmConfigDocument| {
            let auth = AuthContext {
                user_id: user,
                realm_id,
                path_restrictions: None,
                session: None,
            };
            let mut operation = KeyOperation::new("bucket".into(), auth, node, action, 3_000_000);
            operation.info = Some(BucketInfo {
                group_id,
                created_at: std::time::SystemTime::UNIX_EPOCH,
                created_by: user,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Default::default(),
            });
            operation.snapshot_read(values(account, config))
        };
        let mut account = User {
            user_id: user,
            name: "reader".into(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: Default::default(),
        };
        let token = KeyAction::Token {
            access_key: Ulid::from_parts(1_000_000, 1).to_string(),
            public_key: [1; 32],
            restrictions: None,
        };
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        assert_eq!(snapshot(token.clone(), &account, &config), Ok(()));

        config.revoked_tokens.push(TokenRevocation {
            token_hash: aruna_core::auth::user_cutoff_hash(&user),
            expires_at: aruna_core::auth::user_cutoff_expiry(2_000),
        });
        assert_eq!(snapshot(token, &account, &config), Err(KeyError::Denied));

        let request = KeyAction::Request(KeyScope::Subtree("foo/".into()));
        let config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        assert_eq!(snapshot(request.clone(), &account, &config), Ok(()));
        account.attributes.insert(
            aruna_core::user::validation::DEACTIVATED_ATTRIBUTE.to_string(),
            "true".into(),
        );
        assert_eq!(snapshot(request, &account, &config), Err(KeyError::Denied));
    }
}
