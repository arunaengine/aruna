//! Tells the current key holders of a vault-locked bucket when its recovery path weakens, for
//! example after an admin lost the group admin role (D30). Each weakening is told once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::notifications::outbox_record::{new_outbox_record, schedule_drain_effect};
use crate::s3::bucket::key::rows::{
    SettingsError, authority_keys, keyless, parse_authority, scan_call,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{BUCKET_HOLDER_KEYSPACE, BUCKET_RECOVERY_KEYSPACE, KEY_COPY_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::storage_entries::outbox_write_entry;
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{
    BucketHolder, BucketKeyRef, EncryptionMode, SealedCopy,
};
use aruna_core::structs::storage::holders::{KeyLookup, RecoveryState, resolve_holders};
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveryStep {
    ReadBucket,
    ReadGrants,
    ReadCopies,
    ReadMarker,
    WriteRows,
}

#[derive(Debug, Error, PartialEq)]
pub enum RecoveryNoticeError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error("the recovery check did not finish")]
    NotFinished,
    #[error("the recovery check failed: {0}")]
    Io(IoError),
}

impl From<IoError> for RecoveryNoticeError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

type Step = Result<Next<Vec<UserId>>, RecoveryNoticeError>;

/// One bucket to check; `lookups` are the key directory answers of its holders.
#[derive(Debug, PartialEq)]
pub struct RecoveryInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub lookups: BTreeMap<UserId, KeyLookup>,
    pub now_ms: u64,
}

/// The ready holders and ready holders with a recovery code the holders were last told about.
fn marker(ready: (usize, usize)) -> Vec<u8> {
    [
        (ready.0 as u64).to_be_bytes(),
        (ready.1 as u64).to_be_bytes(),
    ]
    .concat()
}

fn parse_marker(value: &[u8]) -> Option<(usize, usize)> {
    let (holders, recovery) = value.split_at_checked(8)?;
    let holders = u64::from_be_bytes(holders.try_into().ok()?);
    let recovery = u64::from_be_bytes(recovery.try_into().ok()?);
    Some((holders as usize, recovery as usize))
}

/// Checks one vault-locked bucket in one transaction and wakes the outbox drain after the
/// commit when it told a holder.
#[derive(Debug, PartialEq)]
pub struct RecoveryNoticeOperation {
    input: RecoveryInput,
    step: RecoveryStep,
    creator: Option<UserId>,
    admins: BTreeSet<UserId>,
    key: Option<BucketKeyRef>,
    grants: Vec<BucketHolder>,
    copies: Vec<SealedCopy>,
    told: Vec<UserId>,
}

impl RecoveryNoticeOperation {
    pub fn new(input: RecoveryInput) -> Self {
        Self {
            input,
            step: RecoveryStep::ReadBucket,
            creator: None,
            admins: BTreeSet::new(),
            key: None,
            grants: Vec::new(),
            copies: Vec::new(),
            told: Vec::new(),
        }
    }

    fn scan(&mut self, step: RecoveryStep, key_space: &str, prefix: Vec<u8>) -> Step {
        self.step = step;
        Ok(Next::Call(scan_call(key_space, prefix.into())))
    }

    /// Only a vault-locked bucket depends on its holders to recover the key.
    fn read_grants(&mut self, values: Vec<(Key, Option<Value>)>) -> Step {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let state = parse_authority(values, realm_id, group_id)?;
        let key = state.settings.active_key();
        let (EncryptionMode::VaultLocked, Some(key)) = (state.settings.mode, key) else {
            return Ok(Next::Done(Vec::new()));
        };
        self.creator = Some(state.info.created_by);
        self.admins = state.admins;
        self.key = Some(key);
        let prefix = key.bucket_id.to_bytes().to_vec();
        self.scan(RecoveryStep::ReadGrants, BUCKET_HOLDER_KEYSPACE, prefix)
    }

    /// Tells every current holder once when the ready holders or recovery codes went down while
    /// the rule is broken; an unknown state is not told, and a met rule forgets the marker.
    fn decide(&mut self, marker_row: Option<Value>) -> Step {
        let (Some(creator), Some(key)) = (self.creator, self.key) else {
            return Err(RecoveryNoticeError::NotFinished);
        };
        let (admins, lookups) = (&self.admins, &self.input.lookups);
        let report = resolve_holders(creator, admins, &self.grants, lookups, &self.copies);
        let recovery = report.recovery;
        let ready = (recovery.ready_holders, recovery.ready_with_recovery);
        let marker_key: Key = key.bucket_id.to_bytes().to_vec().into();
        let told = marker_row.as_deref().and_then(parse_marker);
        let call = match recovery.state {
            RecoveryState::Unknown => return Ok(Next::Done(Vec::new())),
            RecoveryState::Met if told.is_none() => return Ok(Next::Done(Vec::new())),
            RecoveryState::Met => {
                IoCall::Delete(vec![(BUCKET_RECOVERY_KEYSPACE.to_string(), marker_key)])
            }
            RecoveryState::Degraded => {
                let weakened = told.is_none_or(|told| ready.0 < told.0 || ready.1 < told.1);
                let mut writes = vec![(
                    BUCKET_RECOVERY_KEYSPACE.to_string(),
                    marker_key,
                    marker(ready).into(),
                )];
                // Users of another realm have no inbox here.
                let realm_id = self.input.realm_id;
                let told: Vec<_> = match weakened {
                    true => report
                        .holders
                        .iter()
                        .map(|holder| holder.user_id)
                        .filter(|user| user.realm_id == realm_id)
                        .collect(),
                    false => Vec::new(),
                };
                for holder in &told {
                    let kind = NotificationKind::BucketRecoveryDegraded {
                        bucket: self.input.bucket.clone(),
                        node_id: self.input.node_id,
                        group_id: self.input.group_id,
                    };
                    let class = NotificationClass::Direct;
                    let record = NotificationRecord::new(*holder, class, kind, self.input.now_ms);
                    writes.push(outbox_write_entry(&new_outbox_record(record))?);
                }
                self.told = told;
                IoCall::Write(writes)
            }
        };
        self.step = RecoveryStep::WriteRows;
        Ok(Next::Call(call))
    }

    /// Delivery is retried from the stored outbox, so a lost drain timer only delays it.
    fn finish(&mut self) -> Next<Vec<UserId>> {
        let told = std::mem::take(&mut self.told);
        if told.is_empty() {
            return Next::Done(told);
        }
        let drain = IoCall::Effect(Box::new(schedule_drain_effect()));
        Next::Then {
            output: told,
            calls: vec![drain],
        }
    }
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

impl Operation for RecoveryNoticeOperation {
    type Output = Vec<UserId>;
    type Error = RecoveryNoticeError;

    fn first(&mut self) -> Step {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let reads = authority_keys(&self.input.bucket, realm_id, group_id);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.step {
            RecoveryStep::ReadBucket => self.read_grants(keyless(result.values()?)),
            RecoveryStep::ReadGrants => {
                let key = self.key.ok_or(RecoveryNoticeError::NotFinished)?;
                let (values, _) = result.page()?;
                self.grants = decode(&values, BucketHolder::from_bytes)?;
                self.scan(RecoveryStep::ReadCopies, KEY_COPY_KEYSPACE, key.key())
            }
            RecoveryStep::ReadCopies => {
                let key = self.key.ok_or(RecoveryNoticeError::NotFinished)?;
                let (values, _) = result.page()?;
                self.copies = decode(&SealedCopy::user_rows(values), SealedCopy::from_bytes)?;
                self.step = RecoveryStep::ReadMarker;
                let marker = key.bucket_id.to_bytes().to_vec();
                Ok(Next::Call(read_row(BUCKET_RECOVERY_KEYSPACE, marker)))
            }
            RecoveryStep::ReadMarker => self.decide(result.value()?),
            RecoveryStep::WriteRows => {
                result.done()?;
                Ok(self.finish())
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;
