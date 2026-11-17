//! Removes an explicit key holder of a bucket. It rereads holders and copies in its transaction,
//! refuses a stale holder revision and asks for confirmation before it weakens recovery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key::rows::{
    SettingsError, authority_keys, keyless, parse_authority, scan_call,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{
    BUCKET_AUDIT_KEYSPACE, BUCKET_HOLDER_KEYSPACE, BUCKET_KEY_KEYSPACE, KEY_COPY_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketHolder, BucketKeyRecord, HolderOrigin, KeyState, SealedCopy,
};
use aruna_core::structs::storage::holders::{
    KeyLookup, Recovery, RecoveryState, holder_revision, resolve_holders, revision_with_facts,
};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RemovalStep {
    ReadBucket,
    ReadGrants,
    ReadCopies,
    ReadKeys,
    DeleteRows,
    WriteAudit,
}

#[derive(Debug, Error, PartialEq)]
pub enum RemovalError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error("the bucket has no keys")]
    NotEncrypted,
    #[error("the user holds no explicit grant")]
    NoSuchGrant,
    #[error("the holders changed since they were read")]
    StaleHolders,
    /// The removing user lost the group admin role before the removal committed.
    #[error("the caller is no group admin")]
    NotAdmin,
    #[error("removing this holder breaks the recovery rule")]
    RecoveryConfirmationRequired,
    #[error("the removal failed: {0}")]
    Io(IoError),
}

impl From<IoError> for RemovalError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

type Step = Result<Next<RemovalResult>, RemovalError>;

/// A removal by an authorized group admin, decided on the holder `revision` it observed.
#[derive(Debug, PartialEq)]
pub struct RemovalInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub user_id: UserId,
    pub removed_by: UserId,
    pub revision: [u8; 32],
    pub confirm_recovery: bool,
    pub lookups: BTreeMap<UserId, KeyLookup>,
    pub now_ms: u64,
}

#[derive(Debug, PartialEq)]
pub struct RemovalResult {
    /// Recovery after the removal, per retained generation without a node copy.
    pub recovery: BTreeMap<u64, Recovery>,
    /// Copies deleted with the grant; a user who stays creator or admin keeps theirs.
    pub deleted_copies: usize,
}

#[derive(Debug, PartialEq)]
pub struct RemoveHolderOperation {
    input: RemovalInput,
    step: RemovalStep,
    creator: Option<UserId>,
    admins: BTreeSet<UserId>,
    settings: BucketEncryption,
    grants: Vec<BucketHolder>,
    copies: Vec<SealedCopy>,
    audit: Option<BucketAuditRecord>,
    output: Option<RemovalResult>,
}

impl RemoveHolderOperation {
    pub fn new(input: RemovalInput) -> Self {
        Self {
            input,
            step: RemovalStep::ReadBucket,
            creator: None,
            admins: BTreeSet::new(),
            settings: BucketEncryption::default(),
            grants: Vec::new(),
            copies: Vec::new(),
            audit: None,
            output: None,
        }
    }

    fn scan(&mut self, step: RemovalStep, key_space: &str) -> Step {
        let bucket_id = self.settings.bucket_id.ok_or(RemovalError::NotEncrypted)?;
        self.step = step;
        let prefix = bucket_id.to_bytes().to_vec().into();
        Ok(Next::Call(scan_call(key_space, prefix)))
    }

    /// The revision of the holder list as the caller saw it: rows and facts of the active key.
    fn revision(&self, copies: &[SealedCopy]) -> [u8; 32] {
        let active = self.settings.active_key();
        let in_active: Vec<_> = copies
            .iter()
            .filter(|copy| Some(copy.key) == active)
            .cloned()
            .collect();
        let creator = self.creator.unwrap_or_default();
        let (admins, lookups) = (&self.admins, &self.input.lookups);
        let report = resolve_holders(creator, admins, &self.grants, lookups, &in_active);
        let rows = holder_revision(&self.grants, copies);
        revision_with_facts(rows, creator, admins, &report)
    }

    /// Every retained generation without a node copy keeps its recovery path unless confirmed;
    /// a decrypting change keeps source generations while new writes use mode off.
    fn remove(&mut self, keys: Vec<BucketKeyRecord>) -> Step {
        let copies = std::mem::take(&mut self.copies);
        if self.revision(&copies) != self.input.revision {
            return Err(RemovalError::StaleHolders);
        }
        let target = self.input.user_id;
        let is_target = |grant: &BucketHolder| grant.user_id == target;
        let Some(grant) = self.grants.iter().find(|grant| is_target(grant)).cloned() else {
            return Err(RemovalError::NoSuchGrant);
        };
        if grant.origin != HolderOrigin::Explicit {
            return Err(RemovalError::NoSuchGrant);
        }
        let creator = self.creator.unwrap_or_default();
        // A user who stays creator or admin keeps the copies those roles entitle them to.
        let implicit = creator == target || self.admins.contains(&target);
        let remaining: Vec<_> = self
            .grants
            .iter()
            .filter(|g| !is_target(g))
            .cloned()
            .collect();
        let (removed, kept): (Vec<_>, Vec<_>) = copies
            .into_iter()
            .partition(|copy| copy.user_id == target && !implicit);
        let report = |grants: &[BucketHolder], copies: Vec<SealedCopy>| {
            let (admins, lookups) = (&self.admins, &self.input.lookups);
            resolve_holders(creator, admins, grants, lookups, &copies).recovery
        };
        let retained = keys
            .iter()
            .filter(|record| record.state != KeyState::Retired && record.vault_entry.is_none());
        let mut recovery = BTreeMap::new();
        let mut breaks = false;
        for record in retained {
            let of_key = |copy: &&SealedCopy| copy.key == record.key;
            let all = removed
                .iter()
                .chain(&kept)
                .filter(of_key)
                .cloned()
                .collect();
            let before = report(&self.grants, all);
            let after = report(&remaining, kept.iter().filter(of_key).cloned().collect());
            let weakens = after.ready_holders < before.ready_holders
                || after.ready_with_recovery < before.ready_with_recovery;
            // A degraded or unknown recovery path is protected against any further loss.
            breaks |= match before.state {
                RecoveryState::Met => after.state != RecoveryState::Met,
                RecoveryState::Degraded | RecoveryState::Unknown => weakens,
            };
            recovery.insert(record.key.generation, after);
        }
        if breaks && !self.input.confirm_recovery {
            return Err(RemovalError::RecoveryConfirmationRequired);
        }
        let mut deletes: Vec<(String, Key)> =
            vec![(BUCKET_HOLDER_KEYSPACE.to_string(), grant.key().into())];
        deletes.extend(
            removed
                .iter()
                .map(|copy| (KEY_COPY_KEYSPACE.to_string(), copy.key().into())),
        );
        let active = self.settings.active_key();
        self.output = Some(RemovalResult {
            recovery,
            deleted_copies: removed.len(),
        });
        let reason = breaks.then(|| "recovery weakened with confirmation".to_string());
        let record = BucketAuditRecord {
            event_id: Ulid::generate(),
            bucket_id: grant.bucket_id,
            at_ms: self.input.now_ms,
            action: AuditAction::HolderRemoval,
            actor: Some(self.input.removed_by),
            node_id: self.input.node_id,
            generation: active.map(|key| key.generation),
            session_id: None,
            intent_id: None,
            sequence: None,
            deadline_ms: None,
            reason,
            outcome: AuditOutcome::Applied,
        };
        self.audit = Some(record);
        self.step = RemovalStep::DeleteRows;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }
}

impl Operation for RemoveHolderOperation {
    type Output = RemovalResult;
    type Error = RemovalError;

    fn first(&mut self) -> Step {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let reads = authority_keys(&self.input.bucket, realm_id, group_id);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.step {
            RemovalStep::ReadBucket => {
                let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
                let state = parse_authority(keyless(result.values()?), realm_id, group_id)?;
                if !state.admins.contains(&self.input.removed_by) {
                    return Err(RemovalError::NotAdmin);
                }
                self.creator = Some(state.info.created_by);
                self.admins = state.admins;
                self.settings = state.settings;
                self.scan(RemovalStep::ReadGrants, BUCKET_HOLDER_KEYSPACE)
            }
            RemovalStep::ReadGrants => {
                let (values, _) = result.page()?;
                self.grants = decode(&values, BucketHolder::from_bytes)?;
                self.scan(RemovalStep::ReadCopies, KEY_COPY_KEYSPACE)
            }
            RemovalStep::ReadCopies => {
                let (values, _) = result.page()?;
                self.copies = decode(&SealedCopy::user_rows(values), SealedCopy::from_bytes)?;
                self.scan(RemovalStep::ReadKeys, BUCKET_KEY_KEYSPACE)
            }
            RemovalStep::ReadKeys => {
                let (values, _) = result.page()?;
                let keys = decode(&values, BucketKeyRecord::from_bytes)?;
                self.remove(keys)
            }
            RemovalStep::DeleteRows => {
                result.done()?;
                let record = self.audit.take().ok_or(IoError::Unfinished)?;
                let value = record.to_bytes()?;
                self.step = RemovalStep::WriteAudit;
                let row = (
                    BUCKET_AUDIT_KEYSPACE.to_string(),
                    record.key().into(),
                    value.into(),
                );
                Ok(Next::Call(IoCall::Write(vec![row])))
            }
            RemovalStep::WriteAudit => {
                result.done()?;
                let output = self.output.take().ok_or(IoError::Unfinished)?;
                Ok(Next::Done(output))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}

/// Decodes every row; an unreadable row fails the removal rather than being skipped.
fn decode<T>(
    values: &[(Key, Value)],
    parse: impl Fn(&[u8]) -> Result<T, ConversionError>,
) -> Result<Vec<T>, ConversionError> {
    values
        .iter()
        .map(|(_, value)| parse(value.as_ref()))
        .collect()
}

#[cfg(test)]
#[path = "removal_tests.rs"]
mod tests;
