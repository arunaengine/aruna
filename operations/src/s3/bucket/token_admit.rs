//! Admits a plaintext read with a token credential while the bucket key is locked. A token opens
//! only while its creator still holds the bucket key, by the same rule as an unlock (D30).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key::rows::{SettingsError, authority_read, parse_authority};
use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_HOLDER_KEYSPACE, BUCKET_KEY_KEYSPACE, KEY_COPY_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::ArchiveKey;
use aruna_core::structs::storage::encryption::{
    BucketHolder, BucketKeyError, BucketKeyRecord, BucketKeyRef, HolderOrigin, KeyState, ReadLease,
    TokenCopy, TokenCredential,
};
use aruna_core::types::{Effects, GroupId, Key, Value};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum TokenAdmitError {
    /// `Locked` when no usable copy exists or its creator holds no key any more.
    #[error(transparent)]
    Key(#[from] BucketKeyError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error(transparent)]
    Blob(BlobError),
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
    #[error("the token admission did not finish")]
    NotFinished,
}

/// One read of `archive` of `bucket`, sealed with `key`, admitted with `credential`.
#[derive(Debug, PartialEq)]
pub struct TokenAdmitInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub key: BucketKeyRef,
    pub archive: ArchiveKey,
    pub credential: TokenCredential,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Step {
    ReadRows,
    ReadGrant,
    Admit,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct AdmitTokenOperation {
    input: TokenAdmitInput,
    step: Step,
    copy: Option<(TokenCopy, [u8; 32])>,
    output: Option<Result<ReadLease, TokenAdmitError>>,
}

impl AdmitTokenOperation {
    pub fn new(input: TokenAdmitInput) -> Self {
        Self {
            input,
            step: Step::ReadRows,
            copy: None,
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<TokenAdmitError>) -> Effects {
        self.step = Step::Error;
        self.output = Some(Err(error.into()));
        smallvec![]
    }

    fn locked(&mut self) -> Effects {
        self.fail(BucketKeyError::Locked(self.input.key.bucket_id))
    }

    /// The bucket, its authority, the token copy and the key record of this generation.
    fn rows_read(&mut self, mut values: Vec<(Key, Option<Value>)>) -> Effects {
        if values.len() != 6 {
            return self.fail(TokenAdmitError::NotFinished);
        }
        let rows: Vec<_> = values
            .split_off(4)
            .into_iter()
            .map(|(_, row)| row)
            .collect();
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let state = match parse_authority(values, realm_id, group_id) {
            Ok(state) => state,
            Err(error) => return self.fail(error),
        };
        let key = self.input.key;
        let (Some(copy), Some(record)) = (&rows[0], &rows[1]) else {
            return self.locked();
        };
        let copy = match TokenCopy::from_bytes(copy) {
            Ok(copy) => copy,
            Err(error) => return self.fail(error),
        };
        let record = match BucketKeyRecord::from_bytes(record) {
            Ok(record) => record,
            Err(error) => return self.fail(error),
        };
        let same = state.settings.bucket_id == Some(key.bucket_id)
            && copy.key == key
            && copy.access_key == self.input.credential.access_key
            && record.key == key
            && record.state != KeyState::Retired;
        if !same {
            return self.locked();
        }
        let creator = copy.created_by;
        self.copy = Some((copy, record.public_key));
        if state.info.created_by == creator || state.admins.contains(&creator) {
            return self.admit();
        }
        let grant = [&key.bucket_id.to_bytes()[..], &creator.to_storage_key()].concat();
        self.step = Step::ReadGrant;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BUCKET_HOLDER_KEYSPACE.to_string(),
            key: grant.into(),
            txn_id: None,
        })]
    }

    /// A creator who is neither creator of the bucket nor admin needs an explicit grant now.
    fn grant_read(&mut self, value: Option<Value>) -> Effects {
        let grant = value
            .map(|value| BucketHolder::from_bytes(&value))
            .transpose();
        match grant {
            Ok(Some(grant)) if grant.origin == HolderOrigin::Explicit => self.admit(),
            Ok(_) => self.locked(),
            Err(error) => self.fail(error),
        }
    }

    fn admit(&mut self) -> Effects {
        let Some((copy, public_key)) = self.copy.take() else {
            return self.fail(TokenAdmitError::NotFinished);
        };
        self.step = Step::Admit;
        smallvec![Effect::Blob(BlobEffect::AdmitToken {
            key: self.input.key,
            archive: self.input.archive.clone(),
            copy: Box::new(copy),
            public_key,
            token: self.input.credential.token.clone(),
            realm_id: self.input.realm_id,
            node_id: self.input.node_id,
        })]
    }

    fn admitted(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::ReadAdmitted { lease })
                if lease.key == self.input.key && lease.archive == self.input.archive =>
            {
                self.step = Step::Finish;
                self.output = Some(Ok(lease));
                smallvec![]
            }
            Event::Blob(BlobEvent::Error(BlobError::BucketKey(error))) => self.fail(error),
            Event::Blob(BlobEvent::Error(error)) => self.fail(TokenAdmitError::Blob(error)),
            received => self.fail(TokenAdmitError::InvalidStateEvent {
                state: "Admit",
                expected: "ReadAdmitted",
                received,
            }),
        }
    }
}

impl Operation for AdmitTokenOperation {
    type Output = ReadLease;
    type Error = TokenAdmitError;

    fn start(&mut self) -> Effects {
        let (bucket, key) = (&self.input.bucket, self.input.key);
        let authority = authority_read(bucket, self.input.realm_id, self.input.group_id, None);
        let Effect::Storage(StorageEffect::BatchRead { mut reads, .. }) = authority else {
            return self.fail(TokenAdmitError::NotFinished);
        };
        let access_key = &self.input.credential.access_key;
        reads.push((
            KEY_COPY_KEYSPACE.to_string(),
            TokenCopy::copy_key(key, access_key).into(),
        ));
        reads.push((BUCKET_KEY_KEYSPACE.to_string(), key.key().into()));
        self.step = Step::ReadRows;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (Step::Finish | Step::Error, _) => smallvec![],
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error),
            (Step::ReadRows, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.rows_read(values)
            }
            (Step::ReadGrant, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.grant_read(value)
            }
            (Step::Admit, event) => self.admitted(event),
            (_, received) => self.fail(TokenAdmitError::InvalidStateEvent {
                state: "ReadRows",
                expected: "the read of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, Step::Finish | Step::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(TokenAdmitError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
