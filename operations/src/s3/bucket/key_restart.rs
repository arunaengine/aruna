//! Finds the vault-locked buckets whose last recorded session was unlocked before this start,
//! with the generations that were unlocked and the users who hold a grant or copy of them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::restart_notice::RestartedBucket;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_AUDIT_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, BUCKET_HOLDER_KEYSPACE, KEY_COPY_KEYSPACE,
    S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketHolder, EncryptionMode, SealedCopy,
};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;
use ulid::Ulid;

const SCAN_LIMIT: usize = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanStep {
    Init,
    ScanSettings,
    ScanAudit,
    ReadBucket,
    ScanGrants,
    ScanCopies,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RestartScanError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("the restart scan did not finish")]
    NotFinished,
}

#[derive(Debug, PartialEq)]
pub struct RestartScanOperation {
    step: ScanStep,
    /// Vault-locked buckets still to scan, by name and stable id.
    pending: Vec<(String, Ulid)>,
    current: Option<RestartedBucket>,
    /// Applied unlock state per generation of the current bucket, in audit order.
    unlocked: BTreeMap<u64, bool>,
    holders: BTreeSet<aruna_core::UserId>,
    found: Vec<RestartedBucket>,
    output: Option<Result<Vec<RestartedBucket>, RestartScanError>>,
}

impl Default for RestartScanOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl RestartScanOperation {
    pub fn new() -> Self {
        Self {
            step: ScanStep::Init,
            pending: Vec::new(),
            current: None,
            unlocked: BTreeMap::new(),
            holders: BTreeSet::new(),
            found: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<RestartScanError>) -> Effects {
        self.output = Some(Err(error.into()));
        self.step = ScanStep::Error;
        smallvec![]
    }

    fn scan(&mut self, step: ScanStep, key_space: &str, prefix: Option<Vec<u8>>) -> Effects {
        self.step = step;
        iter(key_space, prefix, None, SCAN_LIMIT)
    }

    /// Grants and copies of one bucket are read whole.
    fn scan_rows(&mut self, step: ScanStep, key_space: &str, bucket_id: Ulid) -> Effects {
        self.step = step;
        iter(
            key_space,
            Some(bucket_id.to_bytes().to_vec()),
            None,
            usize::MAX,
        )
    }

    fn next_bucket(&mut self) -> Effects {
        let Some((bucket, bucket_id)) = self.pending.pop() else {
            self.step = ScanStep::Finish;
            self.output = Some(Ok(std::mem::take(&mut self.found)));
            return smallvec![];
        };
        self.unlocked.clear();
        self.holders.clear();
        self.current = Some(RestartedBucket {
            bucket,
            bucket_id,
            group_id: Ulid::nil(),
            generations: Vec::new(),
            holders: Vec::new(),
        });
        let prefix = bucket_id.to_bytes().to_vec();
        self.scan(ScanStep::ScanAudit, BUCKET_AUDIT_KEYSPACE, Some(prefix))
    }

    fn settings_scanned(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        for (key, value) in values {
            let Ok(settings) = BucketEncryption::from_bytes(value.as_ref()) else {
                continue;
            };
            let bucket = String::from_utf8_lossy(key.as_ref()).into_owned();
            if let (EncryptionMode::VaultLocked, Some(bucket_id)) =
                (settings.mode, settings.bucket_id)
            {
                self.pending.push((bucket, bucket_id));
            }
        }
        match next {
            Some(start) => iter(BUCKET_ENCRYPTION_KEYSPACE, None, Some(start), SCAN_LIMIT),
            None => self.next_bucket(),
        }
    }

    /// An applied unlock or extension leaves a generation unlocked until a later applied lock.
    fn audit_scanned(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        for (_, value) in values {
            let Ok(record) = BucketAuditRecord::from_bytes(value.as_ref()) else {
                continue;
            };
            let (Some(generation), AuditOutcome::Applied) = (record.generation, record.outcome)
            else {
                continue;
            };
            match record.action {
                AuditAction::Unlock | AuditAction::Extend => {
                    self.unlocked.insert(generation, true);
                }
                AuditAction::Lock | AuditAction::TimedLock | AuditAction::RestartLock => {
                    self.unlocked.insert(generation, false);
                }
                _ => {}
            }
        }
        let Some(current) = self.current.as_mut() else {
            return self.fail(RestartScanError::NotFinished);
        };
        if let Some(start) = next {
            let prefix = current.bucket_id.to_bytes().to_vec();
            return iter(BUCKET_AUDIT_KEYSPACE, Some(prefix), Some(start), SCAN_LIMIT);
        }
        current.generations = self
            .unlocked
            .iter()
            .filter(|(_, unlocked)| **unlocked)
            .map(|(generation, _)| *generation)
            .collect();
        if current.generations.is_empty() {
            return self.next_bucket();
        }
        self.step = ScanStep::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: current.bucket.as_bytes().into(),
            txn_id: None,
        })]
    }

    fn bucket_read(&mut self, value: Option<Value>) -> Effects {
        // A bucket deleted meanwhile has nobody to tell.
        let Some(value) = value else {
            return self.next_bucket();
        };
        let info = match BucketInfo::from_bytes(value.as_ref()) {
            Ok(info) => info,
            Err(error) => return self.fail(error),
        };
        self.holders.insert(info.created_by);
        let Some(current) = self.current.as_mut() else {
            return self.fail(RestartScanError::NotFinished);
        };
        current.group_id = info.group_id;
        let bucket_id = current.bucket_id;
        self.scan_rows(ScanStep::ScanGrants, BUCKET_HOLDER_KEYSPACE, bucket_id)
    }

    fn rows_scanned(&mut self, values: Vec<(Key, Value)>) -> Effects {
        let Some(current) = self.current.as_mut() else {
            return self.fail(RestartScanError::NotFinished);
        };
        for (_, value) in values {
            let user = match self.step {
                ScanStep::ScanGrants => BucketHolder::from_bytes(value.as_ref())
                    .ok()
                    .map(|grant| grant.user_id),
                _ => SealedCopy::from_bytes(value.as_ref())
                    .ok()
                    .filter(|copy| current.generations.contains(&copy.key.generation))
                    .map(|copy| copy.user_id),
            };
            self.holders.extend(user);
        }
        if self.step == ScanStep::ScanGrants {
            let bucket_id = current.bucket_id;
            return self.scan_rows(ScanStep::ScanCopies, KEY_COPY_KEYSPACE, bucket_id);
        }
        let mut bucket = self.current.take();
        if let Some(bucket) = bucket.as_mut() {
            bucket.holders = std::mem::take(&mut self.holders).into_iter().collect();
        }
        self.found.extend(bucket);
        self.next_bucket()
    }
}

fn iter(key_space: &str, prefix: Option<Vec<u8>>, start: Option<Key>, limit: usize) -> Effects {
    smallvec![Effect::Storage(StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix: prefix.map(Into::into),
        start: start.map(IterStart::After),
        limit,
        txn_id: None,
    })]
}

impl Operation for RestartScanOperation {
    type Output = Vec<RestartedBucket>;
    type Error = RestartScanError;

    fn start(&mut self) -> Effects {
        self.scan(ScanStep::ScanSettings, BUCKET_ENCRYPTION_KEYSPACE, None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error),
            (
                ScanStep::ScanSettings,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.settings_scanned(values, next_start_after),
            (
                ScanStep::ScanAudit,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.audit_scanned(values, next_start_after),
            (ScanStep::ReadBucket, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.bucket_read(value)
            }
            (
                ScanStep::ScanGrants | ScanStep::ScanCopies,
                Event::Storage(StorageEvent::IterResult { values, .. }),
            ) => self.rows_scanned(values),
            (ScanStep::Finish | ScanStep::Error, _) => smallvec![],
            (state, received) => self.fail(RestartScanError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, ScanStep::Finish | ScanStep::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(RestartScanError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "key_restart_tests.rs"]
mod tests;
