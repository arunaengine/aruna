//! Grants a user explicit access to a bucket key. While the active generation is unlocked the
//! user's copies are sealed at once; otherwise the grant stays pending until the next unlock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key_rows::{SettingsError, authority_read, parse_authority};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_HOLDER_KEYSPACE, KEY_COPY_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{
    BucketHolder, BucketKeyError, BucketKeyRef, CopyTarget, GrantState, HolderOrigin, SealedCopy,
};
use aruna_core::structs::storage::holders::{HolderState, KeyLookup};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GrantStep {
    Init,
    StartTransaction,
    ReadBucket,
    ReadGrant,
    SealCopies,
    WriteGrant,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GrantError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("the bucket does not encrypt")]
    NotEncrypted,
    /// The granting user lost the group admin role before the grant committed.
    #[error("the caller is no group admin")]
    NotAdmin,
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("the grant did not finish")]
    NotFinished,
}

/// A grant by an authorized group admin; `lookup` is the grantee's key directory answer.
#[derive(Debug, PartialEq)]
pub struct GrantInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub user_id: UserId,
    pub granted_by: UserId,
    pub lookup: KeyLookup,
    pub now_ms: u64,
}

#[derive(Debug, PartialEq)]
pub struct GrantResult {
    pub grant: BucketHolder,
    pub state: HolderState,
}

#[derive(Debug, PartialEq)]
pub struct GrantHolderOperation {
    input: GrantInput,
    step: GrantStep,
    txn_id: Option<TxnId>,
    key: Option<BucketKeyRef>,
    grant: Option<BucketHolder>,
    output: Option<Result<GrantResult, GrantError>>,
}

impl GrantHolderOperation {
    pub fn new(input: GrantInput) -> Self {
        Self {
            input,
            step: GrantStep::Init,
            txn_id: None,
            key: None,
            grant: None,
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<GrantError>) -> Effects {
        self.output = Some(Err(error.into()));
        let effects = self.abort();
        self.step = GrantStep::Error;
        effects
    }

    /// The granting admin is checked again against the authorization read in this transaction.
    fn read_grant(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let settings = match parse_authority(values, realm_id, group_id) {
            Ok(state) if state.admins.contains(&self.input.granted_by) => state.settings,
            Ok(_) => return self.fail(GrantError::NotAdmin),
            Err(error) => return self.fail(error),
        };
        let Some(key) = settings.active_key() else {
            return self.fail(GrantError::NotEncrypted);
        };
        self.key = Some(key);
        let grant = BucketHolder {
            bucket_id: key.bucket_id,
            user_id: self.input.user_id,
            origin: HolderOrigin::Explicit,
            state: GrantState::Pending,
            granted_by: self.input.granted_by,
            granted_at_ms: self.input.now_ms,
        };
        let read = StorageEffect::Read {
            key_space: BUCKET_HOLDER_KEYSPACE.to_string(),
            key: grant.key().into(),
            txn_id: self.txn_id,
        };
        self.grant = Some(grant);
        self.step = GrantStep::ReadGrant;
        smallvec![Effect::Storage(read)]
    }

    /// An earlier grant keeps who granted it and when; the user's keys get copies if they can.
    fn seal(&mut self, existing: Option<Value>) -> Effects {
        if let Some(existing) = existing {
            match BucketHolder::from_bytes(&existing) {
                Ok(existing) if existing.origin == HolderOrigin::Explicit => {
                    self.grant = Some(existing)
                }
                Ok(_) => {}
                Err(error) => return self.fail(error),
            }
        }
        let (Some(key), KeyLookup::Keys(keys)) = (self.key, &self.input.lookup) else {
            return self.write(Vec::new());
        };
        let holders: Vec<_> = keys
            .iter()
            .filter(|record| record.user_id == self.input.user_id)
            .map(|record| CopyTarget {
                user_id: record.user_id,
                key_record: record.record_id,
                key_id: record.key_id.clone(),
                public_key: record.public_key,
            })
            .collect();
        if holders.is_empty() {
            return self.write(Vec::new());
        }
        self.step = GrantStep::SealCopies;
        smallvec![Effect::Blob(BlobEffect::SealUnlocked {
            key,
            realm_id: self.input.realm_id,
            node_id: self.input.node_id,
            holders,
        })]
    }

    fn write(&mut self, copies: Vec<SealedCopy>) -> Effects {
        let (Some(key), Some(mut grant)) = (self.key, self.grant.take()) else {
            return self.fail(GrantError::NotFinished);
        };
        let foreign = |copy: &SealedCopy| copy.key != key || copy.user_id != grant.user_id;
        if copies.iter().any(foreign) {
            return self.fail(GrantError::NotFinished);
        }
        grant.state = match copies.is_empty() {
            true => GrantState::Pending,
            false => GrantState::Ready,
        };
        let mut writes = Vec::new();
        for copy in &copies {
            match copy.to_bytes() {
                Ok(value) => writes.push((
                    KEY_COPY_KEYSPACE.to_string(),
                    copy.key().into(),
                    value.into(),
                )),
                Err(error) => return self.fail(error),
            }
        }
        match grant.to_bytes() {
            Ok(value) => writes.push((
                BUCKET_HOLDER_KEYSPACE.to_string(),
                grant.key().into(),
                value.into(),
            )),
            Err(error) => return self.fail(error),
        }
        let state = match (&self.input.lookup, copies.is_empty()) {
            (_, false) => HolderState::Ready,
            (KeyLookup::Keys(_), true) => HolderState::Pending,
            (KeyLookup::Missing, true) => HolderState::MissingKey,
            (KeyLookup::Unavailable, true) => HolderState::Unavailable,
        };
        self.output = Some(Ok(GrantResult { grant, state }));
        self.step = GrantStep::WriteGrant;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }
}

impl Operation for GrantHolderOperation {
    type Output = GrantResult;
    type Error = GrantError;

    fn start(&mut self) -> Effects {
        self.step = GrantStep::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.fail(error.clone());
        }
        match (self.step, event) {
            (
                GrantStep::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.step = GrantStep::ReadBucket;
                let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
                smallvec![authority_read(
                    &self.input.bucket,
                    realm_id,
                    group_id,
                    Some(txn_id)
                )]
            }
            (GrantStep::ReadBucket, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.read_grant(values)
            }
            (GrantStep::ReadGrant, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.seal(value)
            }
            (GrantStep::SealCopies, Event::Blob(BlobEvent::CopiesSealed { copies })) => {
                self.write(copies)
            }
            // A locked generation cannot seal now; the next unlock seals the pending grant.
            (
                GrantStep::SealCopies,
                Event::Blob(BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(_)))),
            ) => self.write(Vec::new()),
            (GrantStep::WriteGrant, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(GrantError::NotFinished);
                };
                self.step = GrantStep::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (
                GrantStep::CommitTransaction,
                Event::Storage(StorageEvent::TransactionCommitted { txn_id }),
            ) if Some(txn_id) == self.txn_id => {
                self.txn_id = None;
                self.step = GrantStep::Finish;
                smallvec![]
            }
            (GrantStep::Finish | GrantStep::Error, _) => smallvec![],
            (_, Event::Blob(BlobEvent::Error(error))) => self.fail(error),
            (state, received) => self.fail(GrantError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, GrantStep::Finish | GrantStep::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.step {
            GrantStep::Finish | GrantStep::Error => {
                self.output.unwrap_or(Err(GrantError::NotFinished))
            }
            _ => Err(GrantError::NotFinished),
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

#[cfg(test)]
#[path = "key_grant_tests.rs"]
mod tests;
