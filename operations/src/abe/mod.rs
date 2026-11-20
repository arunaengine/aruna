//! Node-local key requests and grant admission for scoped encrypted reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod enumerate;
pub mod envelope;
mod epoch;
pub mod member;
pub mod reissue;
mod requests;
mod snapshot;

use crate::users::vault::read::{ReadVaultConfig, ReadVaultError, ReadVaultOperation};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, VaultQuery};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::*;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{StagedCall, outside_answer, values};
use aruna_core::structs::execution::notification;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::identity::user::vault::{UserKeyRecord, VaultRecords};
use aruna_core::structs::storage::abe::{AbeEffect, AbeError, AbeEvent, AbeParameters};
use aruna_core::structs::storage::abe_access::*;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyError, BucketKeyRecord, EncryptionMode,
};
use aruna_core::types::{Key, Value};
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
    Member(aruna_core::user::id::UserId),
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
    /// A record the request needs is not on this node yet; unlike `Missing`, the bucket exists.
    #[error("a record the key request needs is not on this node yet")]
    Unprepared,
    #[error("access to the encryption record is denied")]
    Denied,
    #[error("encryption storage is unavailable")]
    Storage,
    #[error("a concurrent change conflicted with this key request")]
    Conflict,
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
impl From<IoError> for KeyError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Conflict,
            _ => Self::Storage,
        }
    }
}
type KeyTurn = Result<Next<KeyResult>, KeyError>;
#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
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
    HolderWrite,
    Cleanup,
    Write,
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
    /// The holders told of this request, and the holder page that follows the one written.
    notified: std::collections::BTreeSet<aruna_core::user::id::UserId>,
    holder_next: Option<Key>,
    /// The role holders still to tell, in bounded chunks before the explicit holder pages.
    role_holders: std::collections::VecDeque<aruna_core::user::id::UserId>,
    explicit_started: bool,
    quiet: bool,
    managed: bool,
    result: Option<KeyResult>,
    ids: IdMint,
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
            state: State::Bucket,
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
            notified: Default::default(),
            holder_next: None,
            role_holders: Default::default(),
            explicit_started: false,
            quiet: false,
            managed: false,
            result: None,
            ids: IdMint::default(),
        }
    }
    /// Sends no holder notifications and walks no reissue pages, for reissue's own runs.
    pub fn quiet(mut self) -> Self {
        self.quiet = true;
        self
    }
    fn fail(&mut self, error: impl Into<KeyError>) -> KeyTurn {
        Err(error.into())
    }
    fn read(&self, space: &str, key: Vec<u8>) -> KeyTurn {
        Ok(Next::Call(IoCall::Read(vec![(
            space.to_string(),
            key.into(),
        )])))
    }
    fn newest(&self) -> Option<&UserKeyRecord> {
        self.recipient_keys
            .iter()
            .max_by_key(|k| (k.created_at_ms, k.record_id))
    }
    fn settings_read(&mut self, value: Option<Value>) -> KeyTurn {
        // Settings that do not decode or validate fail; only decoded ones without a key need none.
        let Ok(settings) = BucketEncryption::from_row(value.as_deref()) else {
            return self.fail(AbeError::Context);
        };
        let Some(key) = settings.active_key() else {
            return self.fail(KeyError::Missing);
        };
        self.managed = settings.mode == EncryptionMode::NodeManaged;
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
        Ok(Next::Call(IoCall::Read(
            vec![
                (AUTH_KEYSPACE.to_string(), realm.clone().into()),
                (
                    AUTH_KEYSPACE.to_string(),
                    info.group_id.to_bytes().to_vec().into(),
                ),
                (REALM_CONFIG_KEYSPACE.to_string(), realm.into()),
                (ABE_PARAMETERS_KEYSPACE.to_string(), key.key().into()),
                (
                    ABE_EPOCH_KEYSPACE.to_string(),
                    key.bucket_id.to_bytes().to_vec().into(),
                ),
                (BUCKET_KEY_KEYSPACE.to_string(), key.key().into()),
                (BUCKET_HOLDER_KEYSPACE.to_string(), holder.concat().into()),
                (
                    USER_KEYSPACE.to_string(),
                    self.recipient().to_bytes().into(),
                ),
            ]
            .into_iter()
            .chain(due)
            .collect(),
        )))
    }
    /// Opens or issues the requests of a request, member or token action under the snapshot epoch.
    fn prepare(&mut self) -> KeyTurn {
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
    fn read_keys(&mut self) -> KeyTurn {
        let mut keys = ReadVaultOperation::new(ReadVaultConfig {
            node_id: self.node,
            realm_id: self.auth.realm_id,
            user_id: self.recipient(),
            query: VaultQuery::Keys,
            deadline: std::time::Duration::from_secs(10),
        });
        self.state = State::Keys;
        let next = keys.first();
        self.keys = Some(keys);
        self.keys_next(next)
    }
    fn keys_read(&mut self, result: IoResult) -> KeyTurn {
        let Some(keys) = self.keys.as_mut() else {
            return self.fail(KeyError::Storage);
        };
        let next = keys.next(result);
        self.keys_next(next)
    }
    /// Makes the vault read's calls this run's own, so its local reads join the transaction.
    fn keys_next(&mut self, next: Result<Next<VaultRecords>, ReadVaultError>) -> KeyTurn {
        let records = match next {
            Ok(Next::Call(call)) => return Ok(Next::Call(call)),
            Ok(Next::Done(VaultRecords::Keys(records))) => records,
            _ => return self.fail(KeyError::Storage),
        };
        self.keys = None;
        if records
            .iter()
            .any(|r| r.user_id != self.recipient() || r.validate().is_err())
        {
            return self.fail(AbeError::Context);
        }
        self.recipient_keys = records;
        self.records()
    }
    fn records(&mut self) -> KeyTurn {
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
        Ok(Next::Call(IoCall::Page {
            space: space.to_string(),
            prefix: prefix.into(),
            after: cursor.map(Key::from),
            limit: MAX_REQUESTS + 1,
        }))
    }
    fn existing_read(&mut self, value: Option<Value>) -> KeyTurn {
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
    fn flush(&mut self) -> KeyTurn {
        if !self.deletes.is_empty() {
            self.state = State::Cleanup;
            let deletes = std::mem::take(&mut self.deletes);
            return Ok(Next::Call(IoCall::Delete(deletes)));
        }
        if !self.writes.is_empty() {
            self.state = State::Write;
            let writes = std::mem::take(&mut self.writes);
            return Ok(Next::Call(IoCall::Write(writes)));
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
        let output = self.result.take().ok_or(AbeError::Context)?;
        let drain = notification::schedule_drain_effect();
        let drain = self.notify.then(|| IoCall::Effect(Box::new(drain)));
        let calls: Vec<IoCall> = drain.into_iter().chain(self.walk()).collect();
        if calls.is_empty() {
            return Ok(Next::Done(output));
        }
        Ok(Next::Then { output, calls })
    }
    /// Answers the issuance: a key unlocked after the locked status read cannot issue before the
    /// due raise, so that run retries.
    fn issue_read(&mut self, event: Event) -> KeyTurn {
        match event {
            Event::Blob(BlobEvent::Abe(_)) if self.snapshot.as_ref().is_some_and(|s| s.due) => {
                self.fail(KeyError::Storage)
            }
            Event::Blob(BlobEvent::Abe(event)) => match *event {
                AbeEvent::Grant(grant) => self.publish_grant(grant),
                _ => self.fail(AbeError::Context),
            },
            Event::Blob(BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(_)))) => {
                match self.request.take() {
                    Some(request) => {
                        self.result = Some(KeyResult::Request(request.clone()));
                        self.write_request(request)
                    }
                    None => self.fail(KeyError::Missing),
                }
            }
            Event::Blob(BlobEvent::Error(BlobError::Abe(error))) => self.fail(error),
            _ => self.fail(AbeError::Context),
        }
    }
}
impl Operation for KeyOperation {
    type Output = KeyResult;
    type Error = KeyError;
    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }
    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
    fn first(&mut self) -> KeyTurn {
        if self.auth.user_id.is_nil() {
            return self.fail(KeyError::Denied);
        }
        self.state = State::Bucket;
        self.read(S3_BUCKET_KEYSPACE, self.bucket.as_bytes().to_vec())
    }
    fn next(&mut self, result: IoResult) -> KeyTurn {
        match self.state {
            State::Bucket => {
                let [value] = values::<1>(result)?;
                // Only an absent bucket is `Missing`; one that does not decode fails.
                let Some(value) = value else {
                    return self.fail(KeyError::Missing);
                };
                let Ok(info) = BucketInfo::from_bytes(&value) else {
                    return self.fail(AbeError::Context);
                };
                self.info = Some(info);
                self.state = State::Settings;
                self.read(BUCKET_ENCRYPTION_KEYSPACE, self.bucket.as_bytes().to_vec())
            }
            State::Settings => {
                let [value] = values::<1>(result)?;
                self.settings_read(value)
            }
            State::Snapshot => {
                self.snapshot_read(result.values()?)?;
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
            State::Unlock => {
                let Event::Blob(BlobEvent::KeyStatus { generations }) = result.event()? else {
                    return self.fail(AbeError::Context);
                };
                let key = self.snapshot.as_ref().map(|s| s.parameters.key);
                if generations.iter().any(|g| g.active && Some(g.key) == key) {
                    return self.raise_first();
                }
                self.prepare()
            }
            State::Keys => self.keys_read(result),
            State::Credential => {
                let [value] = values::<1>(result)?;
                self.credential_read(value)
            }
            State::Records => self.records_read(result),
            State::Existing => {
                let [value] = values::<1>(result)?;
                self.existing_read(value)
            }
            State::Reuse => self.reuse_read(result.page()?.0),
            State::Count => self.count_read(result.page()?.0),
            State::Holders => self.holders_read(result.page()?),
            State::HolderWrite => {
                result.done()?;
                self.holders_written()
            }
            State::Issue => self.issue_read(result.event()?),
            State::Cleanup | State::Write => {
                result.done()?;
                self.flush()
            }
            State::Heads => self.heads_read(outside_answer(result)?.page()?.0),
            State::Writes => self.writes_read(outside_answer(result)?.values()?),
            State::Envelopes => self.envelopes_read(outside_answer(result)?.values()?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::permission_rules::{CollectedRole, PermissionRules};
    use aruna_core::errors::StorageError;
    use aruna_core::structs::identity::auth::{Permission, Role};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::encryption::BucketKeyRef;

    #[test]
    fn foreign_user_starts() {
        // A federated user served by realm 1 requests keys there under the ordinary checks.
        let foreign =
            aruna_core::user::id::UserId::new(Ulid::from_bytes([5; 16]), RealmId([9; 32]));
        let auth = AuthContext {
            user_id: foreign,
            realm_id: RealmId([1; 32]),
            path_restrictions: None,
            session: None,
        };
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let action = KeyAction::Request(KeyScope::Subtree("foo/".into()));
        let mut operation = KeyOperation::new("bucket".into(), auth, node, action, 1);
        assert_eq!(operation.transaction(), TransactionMode::Write);
        assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
    }

    #[test]
    fn unreadable_rows_fail() {
        // A stored bucket or settings row that does not decode is an error, never a skip.
        let realm_id = RealmId([1; 32]);
        let user_id = aruna_core::user::id::UserId::new(Ulid::from_bytes([5; 16]), realm_id);
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let action = KeyAction::Member(user_id);
        let mut operation = KeyOperation::new("bucket".into(), auth, node, action, 1);
        operation.first().unwrap();
        let garbage = || IoResult::Values(vec![Some(Value::from(vec![0xff; 3]))]);
        assert_eq!(operation.next(garbage()), Err(KeyError::Abe(AbeError::Context)));
        operation.state = State::Settings;
        let unreadable = operation.settings_read(Some(Value::from(vec![0xff; 3])));
        assert_eq!(unreadable, Err(KeyError::Abe(AbeError::Context)));
        let absent = IoResult::Values(vec![None]);
        operation.state = State::Bucket;
        assert_eq!(operation.next(absent), Err(KeyError::Missing));
    }

    #[test]
    fn missing_records_unprepared() {
        // An absent authorization, parameter or epoch row is no vanished bucket.
        let realm_id = RealmId([1; 32]);
        let user_id = aruna_core::user::id::UserId::new(Ulid::from_bytes([5; 16]), realm_id);
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let action = KeyAction::Member(user_id);
        let mut operation = KeyOperation::new("bucket".into(), auth, node, action, 1);
        let mut values = vec![None; 8];
        values[7] = Some(Value::from(b"user".to_vec()));
        assert_eq!(operation.snapshot_read(values), Err(KeyError::Unprepared));
    }

    /// A request of user 5 in realm 1 for a bucket whose snapshot names `holders`.
    fn holder_fixture(
        holders: std::collections::BTreeSet<aruna_core::user::id::UserId>,
    ) -> KeyOperation {
        let realm_id = RealmId([1; 32]);
        let user = aruna_core::user::id::UserId::new(Ulid::from_bytes([5; 16]), realm_id);
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let auth = AuthContext {
            user_id: user,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let action = KeyAction::Request(KeyScope::Subtree("foo/".into()));
        let mut operation = KeyOperation::new("bucket".into(), auth, node, action, 1);
        operation.info = Some(BucketInfo {
            group_id: Ulid::from_bytes([8; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: user,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: None,
        });
        operation.snapshot = Some(Snapshot {
            parameters: AbeParameters {
                realm_id,
                node_id: node,
                key: BucketKeyRef::new(Ulid::from_bytes([4; 16]), 1),
                fingerprint: [7; 32],
                parameters: vec![9; 3],
            },
            epoch: 1,
            revisions: Vec::new(),
            rules: PermissionRules::from_roles(Vec::new(), None).unwrap(),
            holder: false,
            holders,
            policies: false,
            due: false,
            checks: Default::default(),
        });
        operation
    }

    #[test]
    fn role_holders_chunked() {
        // Many role holders are told in bounded writes before the explicit holder pages.
        let realm_id = RealmId([1; 32]);
        let holders = (0..300u128)
            .map(|n| aruna_core::user::id::UserId::new(Ulid::from_parts(1, n), realm_id))
            .collect();
        let mut operation = holder_fixture(holders);
        let Some(Ok(Next::Call(IoCall::Write(first)))) = operation.holders() else {
            panic!("expected the first chunk");
        };
        assert_eq!(first.len(), requests::HOLDER_PAGE);
        let Ok(Next::Call(IoCall::Write(rest))) = operation.next(IoResult::Done) else {
            panic!("expected the rest of the role holders");
        };
        assert_eq!(rest.len(), 300 - requests::HOLDER_PAGE);
        let turn = operation.next(IoResult::Done);
        assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))), "then the explicit pages");
    }

    #[test]
    fn holders_told_paged() {
        // A holder page writes its own notices before the next page is read.
        let realm_id = RealmId([1; 32]);
        let user = aruna_core::user::id::UserId::new(Ulid::from_bytes([5; 16]), realm_id);
        let holder = aruna_core::user::id::UserId::new(Ulid::from_bytes([6; 16]), realm_id);
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let auth = AuthContext {
            user_id: user,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let action = KeyAction::Request(KeyScope::Subtree("foo/".into()));
        let mut operation = KeyOperation::new("bucket".into(), auth, node, action, 1);
        let bucket_id = Ulid::from_bytes([4; 16]);
        operation.info = Some(BucketInfo {
            group_id: Ulid::from_bytes([8; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: user,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: None,
        });
        operation.snapshot = Some(Snapshot {
            parameters: AbeParameters {
                realm_id,
                node_id: node,
                key: BucketKeyRef::new(bucket_id, 1),
                fingerprint: [7; 32],
                parameters: vec![9; 3],
            },
            epoch: 1,
            revisions: Vec::new(),
            rules: PermissionRules::from_roles(Vec::new(), None).unwrap(),
            holder: false,
            holders: Default::default(),
            policies: false,
            due: false,
            checks: Default::default(),
        });
        let row = aruna_core::structs::storage::encryption::BucketHolder {
            bucket_id,
            user_id: holder,
            origin: aruna_core::structs::storage::encryption::HolderOrigin::Explicit,
            state: aruna_core::structs::storage::encryption::GrantState::Ready,
            granted_by: user,
            granted_at_ms: 1,
        };
        let page = vec![(Key::from(row.key()), Value::from(row.to_bytes().unwrap()))];
        let next = Some(Key::from(row.key()));
        let Ok(Next::Call(IoCall::Write(notices))) = operation.holders_read((page, next)) else {
            panic!("expected the page's notices");
        };
        assert_eq!(notices.len(), 1);
        let turn = operation.next(IoResult::Done);
        assert!(
            matches!(turn, Ok(Next::Call(IoCall::Page { .. }))),
            "then the next page"
        );
    }

    #[test]
    fn reports_conflicts() {
        // Only a transaction conflict is rerun by `drive_key`; other storage faults stay final.
        for (error, expected) in [
            (IoError::Conflict, KeyError::Conflict),
            (IoError::Storage(StorageError::Timeout), KeyError::Storage),
        ] {
            let error = KeyError::from(error);
            assert_eq!(error, expected);
            let conflict = <KeyOperation as Operation>::is_conflict(&error);
            assert_eq!(conflict, expected == KeyError::Conflict);
        }
    }

    #[test]
    fn due_refuses_grant() {
        let realm_id = RealmId([1; 32]);
        let user = aruna_core::user::id::UserId::new(Ulid::from_bytes([5; 16]), realm_id);
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
            distribution: None,
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
        operation.info = Some(info);
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
        assert_eq!(
            operation.next(IoResult::Event(Box::new(event))),
            Err(KeyError::Storage)
        );
    }

    /// A token run at epoch 3 whose snapshot admits READ on `pattern`, and its foo/ request.
    fn merge_fixture(pattern: &str) -> (KeyOperation, KeyRequest) {
        let realm_id = RealmId([1; 32]);
        let user = aruna_core::user::id::UserId::new(Ulid::from_bytes([5; 16]), realm_id);
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
            distribution: None,
        });
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
        let turn = operation.reuse_read(values.clone());
        let Ok(Next::Call(IoCall::Effect(effect))) = &turn else {
            panic!("one issuance: {turn:?}");
        };
        let Effect::Blob(BlobEffect::Abe(effect)) = effect.as_ref() else {
            panic!("an issuance effect");
        };
        let AbeEffect::Issue(context) = effect.as_ref() else {
            panic!("an issue effect");
        };
        assert_eq!(context.request.epochs, vec![1, 2, 3]);

        // Publication deletes exactly the merged parts and their token rows in its transaction.
        let grant = IoResult::Event(Box::new(issued(&request, &[1, 2, 3])));
        assert!(matches!(
            operation.next(grant),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let turn = operation.next(IoResult::Page {
            rows: values.clone(),
            next: None,
        });
        let Ok(Next::Call(IoCall::Delete(deletes))) = turn else {
            panic!("one delete batch: {turn:?}");
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
        let grant = IoResult::Event(Box::new(issued(&request, &[1, 2, 3])));
        assert_eq!(operation.next(grant), Err(KeyError::Denied));
        assert!(operation.deletes.is_empty() && operation.writes.is_empty());
    }

    #[test]
    fn fences_inactive_recipients() {
        // A deactivated user or a credential issued before the user cutoff gets no new grant.
        // Only a user of another realm may lack a local user row.
        use aruna_core::structs::identity::group::GroupAuthorizationDocument;
        use aruna_core::structs::identity::realm::{
            RealmAuthorizationDocument, RealmConfigDocument, TokenRevocation,
        };
        use aruna_core::structs::identity::user::User;
        let realm_id = RealmId([1; 32]);
        let user = aruna_core::user::id::UserId::new(Ulid::from_bytes([5; 16]), realm_id);
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
        let values = |account: Option<&User>, config: &RealmConfigDocument| -> Vec<Option<Value>> {
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
                account.map(|account| account.to_bytes(&actor).unwrap()),
            ];
            rows.into_iter().map(|v| v.map(Value::from)).collect()
        };
        let snapshot = |action: KeyAction,
                        caller: aruna_core::user::id::UserId,
                        account: Option<&User>,
                        config: &RealmConfigDocument| {
            let auth = AuthContext {
                user_id: caller,
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
                distribution: None,
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
        assert_eq!(
            snapshot(token.clone(), user, Some(&account), &config),
            Ok(())
        );

        config.revoked_tokens.push(TokenRevocation {
            token_hash: aruna_core::auth::user_cutoff_hash(&user),
            expires_at: aruna_core::auth::user_cutoff_expiry(2_000),
        });
        assert_eq!(
            snapshot(token, user, Some(&account), &config),
            Err(KeyError::Denied)
        );

        let request = KeyAction::Request(KeyScope::Subtree("foo/".into()));
        let config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        assert_eq!(
            snapshot(request.clone(), user, Some(&account), &config),
            Ok(())
        );
        account.attributes.insert(
            aruna_core::user::validation::DEACTIVATED_ATTRIBUTE.to_string(),
            "true".into(),
        );
        assert_eq!(
            snapshot(request.clone(), user, Some(&account), &config),
            Err(KeyError::Denied)
        );

        let foreign =
            aruna_core::user::id::UserId::new(Ulid::from_bytes([5; 16]), RealmId([9; 32]));
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        assert_eq!(snapshot(request.clone(), foreign, None, &config), Ok(()));
        let missing = snapshot(request.clone(), user, None, &config);
        assert_eq!(missing, Err(KeyError::Storage));
        config.revoked_tokens.push(TokenRevocation {
            token_hash: aruna_core::auth::user_cutoff_hash(&foreign),
            expires_at: aruna_core::auth::user_cutoff_expiry(2_000),
        });
        let token = KeyAction::Token {
            access_key: Ulid::from_parts(1_000_000, 1).to_string(),
            public_key: [1; 32],
            restrictions: None,
        };
        assert_eq!(
            snapshot(token, foreign, None, &config),
            Err(KeyError::Denied)
        );
    }
}
