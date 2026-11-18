//! Grants a user explicit access to a bucket key. While the active generation is unlocked the
//! user's copies are sealed at once; otherwise the grant stays pending until the next unlock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key::rows::{
    SettingsError, audit_row, authority_keys, keyless, parse_authority,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{BUCKET_HOLDER_KEYSPACE, KEY_COPY_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{
    BucketHolder, BucketKeyError, BucketKeyRef, CopyTarget, GrantState, HolderOrigin, SealedCopy,
};
use aruna_core::structs::storage::holders::{HolderState, KeyLookup};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GrantStep {
    ReadBucket,
    ReadGrant,
    SealCopies,
    WriteGrant,
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
    #[error("sealed copies name another key or user")]
    ForeignCopy,
    #[error("the grant failed: {0}")]
    Io(IoError),
}

impl From<IoError> for GrantError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

type Step = Result<Next<GrantResult>, GrantError>;

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
    key: Option<BucketKeyRef>,
    grant: Option<BucketHolder>,
    output: Option<GrantResult>,
}

impl GrantHolderOperation {
    pub fn new(input: GrantInput) -> Self {
        Self {
            input,
            step: GrantStep::ReadBucket,
            key: None,
            grant: None,
            output: None,
        }
    }

    /// The granting admin is checked again against the authorization read in this transaction.
    fn read_grant(&mut self, values: Vec<(Key, Option<Value>)>) -> Step {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let state = parse_authority(values, realm_id, group_id)?;
        if !state.admins.contains(&self.input.granted_by) {
            return Err(GrantError::NotAdmin);
        }
        let key = state
            .settings
            .active_key()
            .ok_or(GrantError::NotEncrypted)?;
        self.key = Some(key);
        let grant = BucketHolder {
            bucket_id: key.bucket_id,
            user_id: self.input.user_id,
            origin: HolderOrigin::Explicit,
            state: GrantState::Pending,
            granted_by: self.input.granted_by,
            granted_at_ms: self.input.now_ms,
        };
        let read = read_row(BUCKET_HOLDER_KEYSPACE, grant.key());
        self.grant = Some(grant);
        self.step = GrantStep::ReadGrant;
        Ok(Next::Call(read))
    }

    /// An earlier grant keeps who granted it and when; the user's keys get copies if they can.
    fn seal(&mut self, existing: Option<Value>) -> Step {
        if let Some(existing) = existing {
            let existing = BucketHolder::from_bytes(&existing)?;
            if existing.origin == HolderOrigin::Explicit {
                self.grant = Some(existing);
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
        let seal = Effect::Blob(BlobEffect::SealUnlocked {
            key,
            realm_id: self.input.realm_id,
            node_id: self.input.node_id,
            holders,
        });
        Ok(Next::Call(IoCall::Effect(Box::new(seal))))
    }

    fn write(&mut self, copies: Vec<SealedCopy>) -> Step {
        let (Some(key), Some(mut grant)) = (self.key, self.grant.take()) else {
            return Err(GrantError::ForeignCopy);
        };
        let foreign = |copy: &SealedCopy| copy.key != key || copy.user_id != grant.user_id;
        if copies.iter().any(foreign) {
            return Err(GrantError::ForeignCopy);
        }
        grant.state = match copies.is_empty() {
            true => GrantState::Pending,
            false => GrantState::Ready,
        };
        let mut writes = Vec::new();
        for copy in &copies {
            let value = copy.to_bytes()?;
            writes.push((
                KEY_COPY_KEYSPACE.to_string(),
                copy.key().into(),
                value.into(),
            ));
        }
        // The grant and its audit record commit together.
        let record = BucketAuditRecord {
            event_id: Ulid::generate(),
            bucket_id: key.bucket_id,
            at_ms: self.input.now_ms,
            action: AuditAction::HolderGrant,
            actor: Some(self.input.granted_by),
            node_id: self.input.node_id,
            generation: Some(key.generation),
            session_id: None,
            intent_id: None,
            sequence: None,
            deadline_ms: None,
            reason: Some(format!("granted to {}", grant.user_id)),
            outcome: AuditOutcome::Applied,
        };
        writes.push(audit_row(&record)?);
        let value = grant.to_bytes()?;
        writes.push((
            BUCKET_HOLDER_KEYSPACE.to_string(),
            grant.key().into(),
            value.into(),
        ));
        let state = match (&self.input.lookup, copies.is_empty()) {
            (_, false) => HolderState::Ready,
            (KeyLookup::Keys(_), true) => HolderState::Pending,
            (KeyLookup::Missing, true) => HolderState::MissingKey,
            (KeyLookup::Unavailable, true) => HolderState::Unavailable,
        };
        self.output = Some(GrantResult { grant, state });
        self.step = GrantStep::WriteGrant;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for GrantHolderOperation {
    type Output = GrantResult;
    type Error = GrantError;

    fn first(&mut self) -> Step {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let reads = authority_keys(&self.input.bucket, realm_id, group_id);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.step {
            GrantStep::ReadBucket => self.read_grant(keyless(result.values()?)),
            GrantStep::ReadGrant => self.seal(result.value()?),
            GrantStep::SealCopies => match result.event()? {
                Event::Blob(BlobEvent::CopiesSealed { copies }) => self.write(copies),
                // A locked generation cannot seal now; the next unlock seals the pending grant.
                Event::Blob(BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(_)))) => {
                    self.write(Vec::new())
                }
                Event::Blob(BlobEvent::Error(error)) => Err(error.into()),
                other => Err(IoError::Unexpected {
                    expected: "BlobEvent::CopiesSealed",
                    got: other.label(),
                }
                .into()),
            },
            GrantStep::WriteGrant => {
                result.done()?;
                let output = self
                    .output
                    .take()
                    .ok_or(GrantError::Io(IoError::Unfinished))?;
                Ok(Next::Done(output))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
#[path = "grant_tests.rs"]
mod tests;
