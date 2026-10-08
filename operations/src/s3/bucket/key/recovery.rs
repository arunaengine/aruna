//! Tells the current key holders of a vault-locked bucket when its recovery path weakens, for
//! example after an admin lost the group admin role (D30). Each weakening is told once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::{DriverContext, drive};
use crate::notifications::outbox::{new_outbox_record, schedule_drain_effect};
use crate::s3::bucket::holders::lookup_keys;
use crate::s3::bucket::key::rows::{SettingsError, authority_read, parse_authority};
use crate::s3::key_status::KeyStatusOperation;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_ENCRYPTION_KEYSPACE, BUCKET_HOLDER_KEYSPACE, BUCKET_RECOVERY_KEYSPACE,
    KEY_COPY_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::shutdown::Shutdown;
use aruna_core::storage_entries::outbox_write_entry;
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketHolder, BucketKeyRef, EncryptionMode, SealedCopy,
};
use aruna_core::structs::storage::holders::{KeyLookup, RecoveryState, resolve_holders};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use thiserror::Error;
use tracing::warn;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveryStep {
    Init,
    StartTransaction,
    ReadBucket,
    ReadGrants,
    ReadCopies,
    ReadMarker,
    WriteRows,
    Commit,
    ScheduleDrain,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RecoveryNoticeError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("the recovery check did not finish")]
    NotFinished,
}

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

#[derive(Debug, PartialEq)]
pub struct RecoveryNoticeOperation {
    input: RecoveryInput,
    step: RecoveryStep,
    txn_id: Option<TxnId>,
    creator: Option<UserId>,
    admins: BTreeSet<UserId>,
    key: Option<BucketKeyRef>,
    grants: Vec<BucketHolder>,
    copies: Vec<SealedCopy>,
    told: Vec<UserId>,
    output: Option<Result<Vec<UserId>, RecoveryNoticeError>>,
}

impl RecoveryNoticeOperation {
    pub fn new(input: RecoveryInput) -> Self {
        Self {
            input,
            step: RecoveryStep::Init,
            txn_id: None,
            creator: None,
            admins: BTreeSet::new(),
            key: None,
            grants: Vec::new(),
            copies: Vec::new(),
            told: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<RecoveryNoticeError>) -> Effects {
        self.output = Some(Err(error.into()));
        let effects = self.abort();
        self.step = RecoveryStep::Error;
        effects
    }

    fn scan(&mut self, step: RecoveryStep, key_space: &str, prefix: Vec<u8>) -> Effects {
        self.step = step;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: Some(prefix.into()),
            start: None,
            limit: u64::MAX as usize,
            txn_id: self.txn_id,
        })]
    }

    /// Only a vault-locked bucket depends on its holders to recover the key.
    fn read_grants(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let state = match parse_authority(values, realm_id, group_id) {
            Ok(state) => state,
            Err(error) => return self.fail(error),
        };
        let key = state.settings.active_key();
        let (EncryptionMode::VaultLocked, Some(key)) = (state.settings.mode, key) else {
            return self.commit();
        };
        self.creator = Some(state.info.created_by);
        self.admins = state.admins;
        self.key = Some(key);
        let prefix = key.bucket_id.to_bytes().to_vec();
        self.scan(RecoveryStep::ReadGrants, BUCKET_HOLDER_KEYSPACE, prefix)
    }

    /// Tells every current holder once when the ready holders or recovery codes went down while
    /// the rule is broken; an unknown state is not told, and a met rule forgets the marker.
    fn decide(&mut self, marker_row: Option<Value>) -> Effects {
        let (Some(creator), Some(key)) = (self.creator, self.key) else {
            return self.fail(RecoveryNoticeError::NotFinished);
        };
        let (admins, lookups) = (&self.admins, &self.input.lookups);
        let report = resolve_holders(creator, admins, &self.grants, lookups, &self.copies);
        let recovery = report.recovery;
        let ready = (recovery.ready_holders, recovery.ready_with_recovery);
        let marker_key: Key = key.bucket_id.to_bytes().to_vec().into();
        let told = marker_row.as_deref().and_then(parse_marker);
        let effect = match recovery.state {
            RecoveryState::Unknown => return self.commit(),
            RecoveryState::Met if told.is_none() => return self.commit(),
            RecoveryState::Met => StorageEffect::Delete {
                key_space: BUCKET_RECOVERY_KEYSPACE.to_string(),
                key: marker_key,
                txn_id: self.txn_id,
            },
            RecoveryState::Degraded => {
                let weakened = told.is_none_or(|told| ready.0 < told.0 || ready.1 < told.1);
                let mut writes = vec![(
                    BUCKET_RECOVERY_KEYSPACE.to_string(),
                    marker_key,
                    marker(ready).into(),
                )];
                let told: Vec<_> = match weakened {
                    true => report.holders.iter().map(|holder| holder.user_id).collect(),
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
                    match outbox_write_entry(&new_outbox_record(record)) {
                        Ok(entry) => writes.push(entry),
                        Err(error) => return self.fail(error),
                    }
                }
                self.told = told;
                StorageEffect::BatchWrite {
                    writes,
                    txn_id: self.txn_id,
                }
            }
        };
        self.step = RecoveryStep::WriteRows;
        smallvec![Effect::Storage(effect)]
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(RecoveryNoticeError::NotFinished);
        };
        self.step = RecoveryStep::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
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

    fn start(&mut self) -> Effects {
        self.step = RecoveryStep::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (RecoveryStep::Finish | RecoveryStep::Error, _) => smallvec![],
            // Delivery is retried from the stored outbox, so a lost drain timer only delays it.
            (RecoveryStep::ScheduleDrain, Event::Task(_)) => {
                self.output = Some(Ok(std::mem::take(&mut self.told)));
                self.step = RecoveryStep::Finish;
                smallvec![]
            }
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error),
            (
                RecoveryStep::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.step = RecoveryStep::ReadBucket;
                let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
                smallvec![authority_read(
                    &self.input.bucket,
                    realm_id,
                    group_id,
                    Some(txn_id)
                )]
            }
            (
                RecoveryStep::ReadBucket,
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => self.read_grants(values),
            (RecoveryStep::ReadGrants, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                let Some(key) = self.key else {
                    return self.fail(RecoveryNoticeError::NotFinished);
                };
                match decode(&values, BucketHolder::from_bytes) {
                    Ok(grants) => self.grants = grants,
                    Err(error) => return self.fail(error),
                }
                self.scan(RecoveryStep::ReadCopies, KEY_COPY_KEYSPACE, key.key())
            }
            (RecoveryStep::ReadCopies, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                let Some(key) = self.key else {
                    return self.fail(RecoveryNoticeError::NotFinished);
                };
                match decode(&SealedCopy::user_rows(values), SealedCopy::from_bytes) {
                    Ok(copies) => self.copies = copies,
                    Err(error) => return self.fail(error),
                }
                self.step = RecoveryStep::ReadMarker;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: BUCKET_RECOVERY_KEYSPACE.to_string(),
                    key: key.bucket_id.to_bytes().to_vec().into(),
                    txn_id: self.txn_id,
                })]
            }
            (RecoveryStep::ReadMarker, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.decide(value)
            }
            (
                RecoveryStep::WriteRows,
                Event::Storage(
                    StorageEvent::BatchWriteResult { .. } | StorageEvent::DeleteResult { .. },
                ),
            ) => self.commit(),
            (
                RecoveryStep::Commit,
                Event::Storage(StorageEvent::TransactionCommitted { txn_id }),
            ) if Some(txn_id) == self.txn_id => {
                self.txn_id = None;
                if self.told.is_empty() {
                    self.output = Some(Ok(Vec::new()));
                    self.step = RecoveryStep::Finish;
                    return smallvec![];
                }
                self.step = RecoveryStep::ScheduleDrain;
                smallvec![schedule_drain_effect()]
            }
            (state, received) => self.fail(RecoveryNoticeError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, RecoveryStep::Finish | RecoveryStep::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(RecoveryNoticeError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Checks one bucket: reads its holders, asks the key directory and tells the holders if the
/// recovery path weakened. Call it after a group or realm role change and at startup.
pub async fn check_recovery(
    context: &DriverContext,
    (realm_id, node_id): (RealmId, NodeId),
    bucket: &str,
    group_id: GroupId,
) -> Result<Vec<UserId>, RecoveryNoticeError> {
    let status = KeyStatusOperation::new(bucket.to_string(), realm_id, group_id);
    let snapshot = drive(status, context)
        .await
        .map_err(|error| StorageError::ReadError(error.to_string()))?;
    let Some(info) = snapshot.info else {
        return Ok(Vec::new());
    };
    let users: BTreeSet<_> = std::iter::once(info.created_by)
        .chain(snapshot.admins.iter().copied())
        .chain(snapshot.grants.iter().map(|grant| grant.user_id))
        .collect();
    let lookups = lookup_keys(context, node_id, users).await;
    let input = RecoveryInput {
        bucket: bucket.to_string(),
        group_id,
        realm_id,
        node_id,
        lookups,
        now_ms: aruna_core::time::unix_timestamp_millis(),
    };
    drive(RecoveryNoticeOperation::new(input), context).await
}

/// How often the node checks its vault-locked buckets for weakened recovery; role changes reach
/// it through document sync, so no local event marks them.
const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// Checks every vault-locked bucket of this node once.
pub async fn check_buckets(context: &DriverContext, origin: (RealmId, NodeId)) {
    let mut start = None;
    loop {
        let scan = StorageEffect::Iter {
            key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            prefix: None,
            start: start.take().map(IterStart::After),
            limit: 1_000,
            txn_id: None,
        };
        let event = context.storage_handle.send_storage_effect(scan).await;
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            warn!("Bucket recovery sweep could not list buckets");
            return;
        };
        for (key, value) in values {
            let locked = BucketEncryption::from_bytes(value.as_ref())
                .is_ok_and(|settings| settings.mode == EncryptionMode::VaultLocked);
            let bucket = String::from_utf8_lossy(key.as_ref()).into_owned();
            let group_id = match locked {
                true => bucket_group(context, &bucket).await,
                false => None,
            };
            let Some(group_id) = group_id else {
                continue;
            };
            if let Err(error) = check_recovery(context, origin, &bucket, group_id).await {
                warn!(%bucket, error = %error, "Bucket recovery check failed");
            }
        }
        match next_start_after {
            Some(next) => start = Some(next),
            None => return,
        }
    }
}

async fn bucket_group(context: &DriverContext, bucket: &str) -> Option<GroupId> {
    let read = StorageEffect::Read {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => BucketInfo::from_bytes(value.as_ref())
            .ok()
            .map(|info| info.group_id),
        _ => None,
    }
}

/// Checks the vault-locked buckets right after startup and then at a fixed interval.
pub fn spawn_recovery_sweep(
    context: Arc<DriverContext>,
    origin: (RealmId, NodeId),
    shutdown: &Shutdown,
) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = check_buckets(context.as_ref(), origin) => {}
            }
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(SWEEP_INTERVAL) => {}
            }
        }
    });
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;
