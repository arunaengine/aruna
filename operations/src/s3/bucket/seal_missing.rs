//! Seals copies of an unlocked key generation for current holders who have none yet: pending
//! explicit grants and implicit holders who became admin or published keys since the last seal.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key::rows::{SettingsError, authority_read, copy_targets, parse_authority};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_HOLDER_KEYSPACE, KEY_COPY_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{
    BucketHolder, BucketKeyError, BucketKeyRef, GrantState, SealedCopy,
};
use aruna_core::structs::storage::holders::{HolderState, KeyLookup, resolve_holders};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SealStep {
    Init,
    StartTransaction,
    ReadBucket,
    ReadGrants,
    ReadCopies,
    SealCopies,
    WriteCopies,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum SealMissingError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error(transparent)]
    Key(#[from] BucketKeyError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("sealing the missing copies did not finish")]
    NotFinished,
}

/// Runs after an unlock of `key`; `lookups` are the key directory answers of the holders.
#[derive(Debug, PartialEq)]
pub struct SealMissingInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub key: BucketKeyRef,
    pub lookups: BTreeMap<UserId, KeyLookup>,
}

#[derive(Debug, PartialEq)]
pub struct SealMissingOperation {
    input: SealMissingInput,
    step: SealStep,
    txn_id: Option<TxnId>,
    creator: Option<UserId>,
    admins: BTreeSet<UserId>,
    grants: Vec<BucketHolder>,
    sealed: Vec<UserId>,
    output: Option<Result<Vec<UserId>, SealMissingError>>,
}

fn decode<T>(
    values: &[(Key, Value)],
    parse: impl Fn(&[u8]) -> Result<T, ConversionError>,
) -> Result<Vec<T>, ConversionError> {
    values
        .iter()
        .map(|(_, value)| parse(value.as_ref()))
        .collect()
}

impl SealMissingOperation {
    pub fn new(input: SealMissingInput) -> Self {
        Self {
            input,
            step: SealStep::Init,
            txn_id: None,
            creator: None,
            admins: BTreeSet::new(),
            grants: Vec::new(),
            sealed: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<SealMissingError>) -> Effects {
        self.output = Some(Err(error.into()));
        let effects = self.abort();
        self.step = SealStep::Error;
        effects
    }

    fn scan(&mut self, step: SealStep, key_space: &str, prefix: Vec<u8>) -> Effects {
        self.step = step;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: Some(prefix.into()),
            start: None,
            limit: u64::MAX as usize,
            txn_id: self.txn_id,
        })]
    }

    fn read_grants(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let state = match parse_authority(values, realm_id, group_id) {
            Ok(state) => state,
            Err(error) => return self.fail(error),
        };
        let key = self.input.key;
        if state.settings.bucket_id != Some(key.bucket_id) {
            return self.fail(BucketKeyError::StaleGeneration {
                requested: key.generation,
                current: state.settings.key_generation,
            });
        }
        self.creator = Some(state.info.created_by);
        self.admins = state.admins;
        let prefix = key.bucket_id.to_bytes().to_vec();
        self.scan(SealStep::ReadGrants, BUCKET_HOLDER_KEYSPACE, prefix)
    }

    /// Seals to every key of the holders that have no copy of this generation yet.
    fn seal(&mut self, copies: Vec<SealedCopy>) -> Effects {
        let creator = self.creator.unwrap_or_default();
        let (admins, lookups) = (&self.admins, &self.input.lookups);
        let mut report = resolve_holders(creator, admins, &self.grants, lookups, &copies);
        report
            .holders
            .retain(|holder| holder.state == HolderState::Pending);
        let holders = copy_targets(&report, lookups);
        if holders.is_empty() {
            return self.write(Vec::new());
        }
        self.step = SealStep::SealCopies;
        smallvec![Effect::Blob(BlobEffect::SealUnlocked {
            key: self.input.key,
            realm_id: self.input.realm_id,
            node_id: self.input.node_id,
            holders,
        })]
    }

    /// Stores the copies and marks the explicit grants they made ready.
    fn write(&mut self, copies: Vec<SealedCopy>) -> Effects {
        if copies.iter().any(|copy| copy.key != self.input.key) {
            return self.fail(SealMissingError::NotFinished);
        }
        let users: BTreeSet<_> = copies.iter().map(|copy| copy.user_id).collect();
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
        let mut ready = Vec::new();
        for grant in &mut self.grants {
            if users.contains(&grant.user_id) && grant.state == GrantState::Pending {
                grant.state = GrantState::Ready;
                ready.push(grant.clone());
            }
        }
        for grant in &ready {
            match grant.to_bytes() {
                Ok(value) => writes.push((
                    BUCKET_HOLDER_KEYSPACE.to_string(),
                    grant.key().into(),
                    value.into(),
                )),
                Err(error) => return self.fail(error),
            }
        }
        self.sealed = users.into_iter().collect();
        let Some(txn_id) = self.txn_id else {
            return self.fail(SealMissingError::NotFinished);
        };
        if writes.is_empty() {
            self.step = SealStep::CommitTransaction;
            return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
        }
        self.step = SealStep::WriteCopies;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })]
    }
}

impl Operation for SealMissingOperation {
    type Output = Vec<UserId>;
    type Error = SealMissingError;

    fn start(&mut self) -> Effects {
        self.step = SealStep::StartTransaction;
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
                SealStep::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.step = SealStep::ReadBucket;
                let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
                smallvec![authority_read(
                    &self.input.bucket,
                    realm_id,
                    group_id,
                    Some(txn_id)
                )]
            }
            (SealStep::ReadBucket, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.read_grants(values)
            }
            (SealStep::ReadGrants, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                match decode(&values, BucketHolder::from_bytes) {
                    Ok(grants) => {
                        self.grants = grants;
                        let prefix = self.input.key.key();
                        self.scan(SealStep::ReadCopies, KEY_COPY_KEYSPACE, prefix)
                    }
                    Err(error) => self.fail(error),
                }
            }
            (SealStep::ReadCopies, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                match decode(&values, SealedCopy::from_bytes) {
                    Ok(copies) => self.seal(copies),
                    Err(error) => self.fail(error),
                }
            }
            (SealStep::SealCopies, Event::Blob(BlobEvent::CopiesSealed { copies })) => {
                self.write(copies)
            }
            (SealStep::WriteCopies, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(SealMissingError::NotFinished);
                };
                self.step = SealStep::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (
                SealStep::CommitTransaction,
                Event::Storage(StorageEvent::TransactionCommitted { txn_id }),
            ) if Some(txn_id) == self.txn_id => {
                self.txn_id = None;
                self.output = Some(Ok(std::mem::take(&mut self.sealed)));
                self.step = SealStep::Finish;
                smallvec![]
            }
            (SealStep::Finish | SealStep::Error, _) => smallvec![],
            (_, Event::Blob(BlobEvent::Error(error))) => self.fail(error),
            (state, received) => self.fail(SealMissingError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, SealStep::Finish | SealStep::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(SealMissingError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
#[path = "seal_missing_tests.rs"]
mod tests;
