//! Node-local key requests and grant admission for scoped encrypted reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod envelope;
mod requests;
mod snapshot;

use crate::users::vault_read::{ReadVaultConfig, ReadVaultOperation};
use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect, VaultQuery};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::*;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::user::vault::{UserKeyRecord, VaultRecords};
use aruna_core::structs::storage::abe::{AbeEffect, AbeError, AbeEvent, AbeParameters};
use aruna_core::structs::storage::abe_access::*;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyError, BucketKeyRecord};
use aruna_core::types::{Effects, Key, TxnId, Value};
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
}
#[derive(Debug, PartialEq)]
pub enum KeyResult {
    Request(KeyRequest),
    Requests(Vec<KeyRequest>, Option<Vec<u8>>),
    Grants(Vec<KeyGrant>, Option<Vec<u8>>),
    Grant(KeyGrant),
}
#[derive(Debug, Error, PartialEq)]
pub enum KeyError {
    #[error(transparent)]
    Abe(#[from] AbeError),
    #[error("the bucket or key request was not found")]
    Missing,
    #[error("access to the encryption record is denied")]
    Denied,
    #[error("encryption storage is unavailable")]
    Storage,
}
#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
    Init,
    Start,
    Bucket,
    Settings,
    Snapshot,
    Keys,
    Records,
    Existing,
    Reuse,
    Issue,
    Count,
    Cleanup,
    Write,
    Commit,
    Done,
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
            result: None,
            output: None,
        }
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
        let Some(key) = BucketEncryption::from_row(value.as_deref())
            .ok()
            .and_then(|s| s.active_key())
        else {
            return self.fail(KeyError::Missing);
        };
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
                (BUCKET_HOLDER_KEYSPACE.to_string(), holder.concat().into())
            ],
            txn_id: self.txn
        })]
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
        let own = [bucket.clone(), self.auth.user_id.to_storage_key()].concat();
        let (space, prefix, cursor) = match &self.action {
            KeyAction::Publish(grant) => {
                self.state = State::Records;
                return self.read(ABE_REQUEST_KEYSPACE, grant.context.request.key());
            }
            KeyAction::Open(_) if !snapshot.holder => return self.fail(KeyError::Denied),
            KeyAction::Open(cursor) => (ABE_REQUEST_KEYSPACE, bucket, cursor.clone()),
            KeyAction::Request(_) => (ABE_REQUEST_KEYSPACE, own, None),
            KeyAction::Grants(cursor) => (ABE_GRANT_KEYSPACE, own, cursor.clone()),
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
        if self.auth.user_id.realm_id != self.auth.realm_id || self.auth.user_id.is_nil() {
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
            (State::Keys, event) => self.keys_read(event),
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
                self.state = State::Done;
                self.output = self.result.take().map(Ok);
                smallvec![]
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
