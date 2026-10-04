//! Removes an explicit key holder of a bucket. It rereads holders and copies in its transaction,
//! refuses a stale holder revision and asks for confirmation before it breaks recovery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key_rows::{SettingsError, authority_read, parse_authority};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_AUDIT_KEYSPACE, BUCKET_HOLDER_KEYSPACE, KEY_COPY_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketHolder, EncryptionMode, HolderOrigin, SealedCopy,
};
use aruna_core::structs::storage::holders::{
    KeyLookup, Recovery, RecoveryState, holder_revision, resolve_holders,
};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RemovalStep {
    Init,
    StartTransaction,
    ReadBucket,
    ReadGrants,
    ReadCopies,
    DeleteRows,
    WriteAudit,
    CommitTransaction,
    Finish,
    Error,
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
    #[error("removing this holder breaks the recovery rule")]
    RecoveryConfirmationRequired,
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("the removal did not finish")]
    NotFinished,
}

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
    pub recovery: Recovery,
    /// Copies deleted with the grant; a user who stays creator or admin keeps theirs.
    pub deleted_copies: usize,
}

#[derive(Debug, PartialEq)]
pub struct RemoveHolderOperation {
    input: RemovalInput,
    step: RemovalStep,
    txn_id: Option<TxnId>,
    creator: Option<UserId>,
    admins: BTreeSet<UserId>,
    settings: BucketEncryption,
    grants: Vec<BucketHolder>,
    audit: Option<BucketAuditRecord>,
    output: Option<Result<RemovalResult, RemovalError>>,
}

impl RemoveHolderOperation {
    pub fn new(input: RemovalInput) -> Self {
        Self {
            input,
            step: RemovalStep::Init,
            txn_id: None,
            creator: None,
            admins: BTreeSet::new(),
            settings: BucketEncryption::default(),
            grants: Vec::new(),
            audit: None,
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<RemovalError>) -> Effects {
        self.output = Some(Err(error.into()));
        let effects = self.abort();
        self.step = RemovalStep::Error;
        effects
    }

    fn scan(&mut self, step: RemovalStep, key_space: &str) -> Effects {
        let Some(bucket_id) = self.settings.bucket_id else {
            return self.fail(RemovalError::NotEncrypted);
        };
        self.step = step;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: Some(bucket_id.to_bytes().to_vec().into()),
            start: None,
            limit: u64::MAX as usize,
            txn_id: self.txn_id,
        })]
    }

    fn remove(&mut self, copies: Vec<SealedCopy>) -> Effects {
        if holder_revision(&self.grants, &copies) != self.input.revision {
            return self.fail(RemovalError::StaleHolders);
        }
        let target = self.input.user_id;
        let is_target = |grant: &BucketHolder| grant.user_id == target;
        let Some(grant) = self.grants.iter().find(|grant| is_target(grant)).cloned() else {
            return self.fail(RemovalError::NoSuchGrant);
        };
        if grant.origin != HolderOrigin::Explicit {
            return self.fail(RemovalError::NoSuchGrant);
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
        let active = self.settings.active_key();
        let in_active = |copy: &&SealedCopy| Some(copy.key) == active;
        let report = |grants: &[BucketHolder], copies: Vec<SealedCopy>| {
            let (admins, lookups) = (&self.admins, &self.input.lookups);
            resolve_holders(creator, admins, grants, lookups, &copies).recovery
        };
        let before = report(
            &self.grants,
            removed
                .iter()
                .chain(&kept)
                .filter(in_active)
                .cloned()
                .collect(),
        );
        let after = report(&remaining, kept.iter().filter(in_active).cloned().collect());
        let breaks = self.settings.mode == EncryptionMode::VaultLocked
            && before.state == RecoveryState::Met
            && after.state != RecoveryState::Met;
        if breaks && !self.input.confirm_recovery {
            return self.fail(RemovalError::RecoveryConfirmationRequired);
        }
        let mut deletes: Vec<(String, Key)> =
            vec![(BUCKET_HOLDER_KEYSPACE.to_string(), grant.key().into())];
        deletes.extend(
            removed
                .iter()
                .map(|copy| (KEY_COPY_KEYSPACE.to_string(), copy.key().into())),
        );
        self.output = Some(Ok(RemovalResult {
            recovery: after,
            deleted_copies: removed.len(),
        }));
        let reason = breaks.then(|| "recovery rule broken with confirmation".to_string());
        let record = BucketAuditRecord {
            event_id: Ulid::generate(),
            bucket_id: grant.bucket_id,
            at_ms: self.input.now_ms,
            action: AuditAction::HolderRemoval,
            actor: Some(self.input.removed_by),
            node_id: self.input.node_id,
            generation: active.map(|key| key.generation),
            deadline_ms: None,
            reason,
            outcome: AuditOutcome::Applied,
        };
        self.audit = Some(record);
        self.step = RemovalStep::DeleteRows;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: self.txn_id,
        })]
    }
}

impl Operation for RemoveHolderOperation {
    type Output = RemovalResult;
    type Error = RemovalError;

    fn start(&mut self) -> Effects {
        self.step = RemovalStep::StartTransaction;
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
                RemovalStep::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.step = RemovalStep::ReadBucket;
                let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
                smallvec![authority_read(
                    &self.input.bucket,
                    realm_id,
                    group_id,
                    Some(txn_id)
                )]
            }
            (RemovalStep::ReadBucket, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
                match parse_authority(values, realm_id, group_id) {
                    Ok(state) => {
                        self.creator = Some(state.info.created_by);
                        self.admins = state.admins;
                        self.settings = state.settings;
                        self.scan(RemovalStep::ReadGrants, BUCKET_HOLDER_KEYSPACE)
                    }
                    Err(error) => self.fail(error),
                }
            }
            (RemovalStep::ReadGrants, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                match decode(&values, BucketHolder::from_bytes) {
                    Ok(grants) => {
                        self.grants = grants;
                        self.scan(RemovalStep::ReadCopies, KEY_COPY_KEYSPACE)
                    }
                    Err(error) => self.fail(error),
                }
            }
            (RemovalStep::ReadCopies, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                match decode(&values, SealedCopy::from_bytes) {
                    Ok(copies) => self.remove(copies),
                    Err(error) => self.fail(error),
                }
            }
            (RemovalStep::DeleteRows, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                let Some(record) = self.audit.take() else {
                    return self.fail(RemovalError::NotFinished);
                };
                let value = match record.to_bytes() {
                    Ok(value) => value,
                    Err(error) => return self.fail(error),
                };
                self.step = RemovalStep::WriteAudit;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: BUCKET_AUDIT_KEYSPACE.to_string(),
                    key: record.key().into(),
                    value: value.into(),
                    txn_id: self.txn_id,
                })]
            }
            (RemovalStep::WriteAudit, Event::Storage(StorageEvent::WriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(RemovalError::NotFinished);
                };
                self.step = RemovalStep::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (
                RemovalStep::CommitTransaction,
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.txn_id = None;
                self.step = RemovalStep::Finish;
                smallvec![]
            }
            (RemovalStep::Finish | RemovalStep::Error, _) => smallvec![],
            (state, received) => self.fail(RemovalError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, RemovalStep::Finish | RemovalStep::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.step {
            RemovalStep::Finish | RemovalStep::Error => {
                self.output.unwrap_or(Err(RemovalError::NotFinished))
            }
            _ => Err(RemovalError::NotFinished),
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
#[path = "key_removal_tests.rs"]
mod tests;
