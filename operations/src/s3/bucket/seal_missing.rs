//! Seals copies of an unlocked key generation for current holders who have none yet: pending
//! explicit grants and implicit holders who became admin or published keys since the last seal.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key::rows::{
    SettingsError, authority_keys, copy_targets, keyless, parse_authority, scan_call,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{BUCKET_HOLDER_KEYSPACE, KEY_COPY_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{
    BucketHolder, BucketKeyError, BucketKeyRef, GrantState, SealedCopy,
};
use aruna_core::structs::storage::holders::{HolderState, KeyLookup, resolve_holders};
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SealStep {
    ReadBucket,
    ReadGrants,
    ReadCopies,
    SealCopies,
    WriteCopies,
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
    #[error("sealed copies name another key generation")]
    OtherGeneration,
    #[error("sealing the missing copies failed: {0}")]
    Io(IoError),
}

impl From<IoError> for SealMissingError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

type Step = Result<Next<Vec<UserId>>, SealMissingError>;

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
    creator: Option<UserId>,
    admins: BTreeSet<UserId>,
    grants: Vec<BucketHolder>,
    sealed: Vec<UserId>,
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
            step: SealStep::ReadBucket,
            creator: None,
            admins: BTreeSet::new(),
            grants: Vec::new(),
            sealed: Vec::new(),
        }
    }

    fn scan(&mut self, step: SealStep, key_space: &str, prefix: Vec<u8>) -> Step {
        self.step = step;
        Ok(Next::Call(scan_call(key_space, prefix.into())))
    }

    fn read_grants(&mut self, values: Vec<(Key, Option<Value>)>) -> Step {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let state = parse_authority(values, realm_id, group_id)?;
        let key = self.input.key;
        if state.settings.bucket_id != Some(key.bucket_id) {
            return Err(BucketKeyError::StaleGeneration {
                requested: key.generation,
                current: state.settings.key_generation,
            }
            .into());
        }
        self.creator = Some(state.info.created_by);
        self.admins = state.admins;
        let prefix = key.bucket_id.to_bytes().to_vec();
        self.scan(SealStep::ReadGrants, BUCKET_HOLDER_KEYSPACE, prefix)
    }

    /// Seals to every key of the holders that have no copy of this generation yet.
    fn seal(&mut self, copies: Vec<SealedCopy>) -> Step {
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
        let seal = Effect::Blob(BlobEffect::SealUnlocked {
            key: self.input.key,
            realm_id: self.input.realm_id,
            node_id: self.input.node_id,
            holders,
        });
        Ok(Next::Call(IoCall::Effect(Box::new(seal))))
    }

    /// Stores the copies and marks the explicit grants they made ready.
    fn write(&mut self, copies: Vec<SealedCopy>) -> Step {
        if copies.iter().any(|copy| copy.key != self.input.key) {
            return Err(SealMissingError::OtherGeneration);
        }
        let users: BTreeSet<_> = copies.iter().map(|copy| copy.user_id).collect();
        let mut writes = Vec::new();
        for copy in &copies {
            let value = copy.to_bytes()?;
            writes.push((
                KEY_COPY_KEYSPACE.to_string(),
                copy.key().into(),
                value.into(),
            ));
        }
        for grant in &mut self.grants {
            if users.contains(&grant.user_id) && grant.state == GrantState::Pending {
                grant.state = GrantState::Ready;
                let value = grant.to_bytes()?;
                writes.push((
                    BUCKET_HOLDER_KEYSPACE.to_string(),
                    grant.key().into(),
                    value.into(),
                ));
            }
        }
        self.sealed = users.into_iter().collect();
        self.step = SealStep::WriteCopies;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for SealMissingOperation {
    type Output = Vec<UserId>;
    type Error = SealMissingError;

    fn first(&mut self) -> Step {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let reads = authority_keys(&self.input.bucket, realm_id, group_id);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.step {
            SealStep::ReadBucket => self.read_grants(keyless(result.values()?)),
            SealStep::ReadGrants => {
                let (values, _) = result.page()?;
                self.grants = decode(&values, BucketHolder::from_bytes)?;
                let prefix = self.input.key.key();
                self.scan(SealStep::ReadCopies, KEY_COPY_KEYSPACE, prefix)
            }
            SealStep::ReadCopies => {
                let (values, _) = result.page()?;
                let copies = decode(&SealedCopy::user_rows(values), SealedCopy::from_bytes)?;
                self.seal(copies)
            }
            SealStep::SealCopies => match result.event()? {
                Event::Blob(BlobEvent::CopiesSealed { copies }) => self.write(copies),
                Event::Blob(BlobEvent::Error(error)) => Err(error.into()),
                other => Err(IoError::Unexpected {
                    expected: "BlobEvent::CopiesSealed",
                    got: other.label(),
                }
                .into()),
            },
            SealStep::WriteCopies => {
                result.done()?;
                Ok(Next::Done(std::mem::take(&mut self.sealed)))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[path = "seal_missing_tests.rs"]
mod tests;
