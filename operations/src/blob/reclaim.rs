use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    GROUP_STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BackendLocation, BackendRef, BlobCleanupWork, BlobLocationKey, BlobVersion, CleanupStrategy,
    GroupStorageBackend, HashPathIndexKey, ReclaimCandidate, ReclaimCandidateKey, VersionKey,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Effects, Key, TxnId};
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use smallvec::smallvec;
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use crate::blob::blob_keyspace_helper::{blob_location_read, iter_hash_path_index_effect};
use crate::blob::cleanup::schedule_blob_cleanup_effect;
use crate::driver::{DriverContext, drive, node_routing};
use crate::group_backends::{RecordReadError, backend_key, parse_read};
use crate::jobs::store::iter_prefix_page;
use crate::task_persistence::persist_task_effect;
use crate::usage_stats::{StoredDelta, UsageCounterUpdate, UsageUpdateError};

pub const RECLAIM_SWEEP_AFTER: Duration = Duration::from_secs(15 * 60);
pub const RECLAIM_SWEEP_RETRY: Duration = Duration::from_secs(60);
const RECLAIM_PAGE_SIZE: usize = 128;

pub fn schedule_blob_reclaim_effect() -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::DrainBlobReclaimQueue,
        after: Duration::ZERO,
    })
}

pub async fn restore_reclaim_sweep(storage: &StorageHandle, task_handle: &TaskHandle) {
    let effect = TaskEffect::ShortenTimer {
        key: TaskKey::DrainBlobReclaimQueue,
        after: Duration::ZERO,
    };
    if let Err(message) = persist_task_effect(storage, &effect).await {
        warn!(message = %message, "Failed to persist blob reclaim timer");
        return;
    }
    match task_handle.send_effect(Effect::Task(effect)).await {
        Event::Task(TaskEvent::TimerScheduled { .. }) => {}
        Event::Task(TaskEvent::Error { message, .. }) => {
            warn!(message = %message, "Failed to schedule blob reclaim sweep");
        }
        other => warn!(event = ?other, "Unexpected blob reclaim timer result"),
    }
}

/// What one candidate resolved to. `Dropped` covers every reason the queue row
/// is stale: retain, a vanished backend, or a location that is already gone.
/// `NotDue` keeps the row: the grace grew after the sweep read it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReclaimVerdict {
    Freed { bytes: u64 },
    Pinned,
    Dropped,
    NotDue,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReclaimOutcome {
    pub freed: usize,
    pub freed_bytes: u64,
    pub pinned: usize,
    pub dropped: usize,
    pub not_due: usize,
    pub failed: usize,
}

/// Pages the whole queue so a candidate that keeps conflicting cannot starve
/// the rows behind it.
pub async fn process_reclaim_batch(context: &DriverContext) -> Result<ReclaimOutcome, String> {
    sweep_at(context, SystemTime::now()).await
}

async fn sweep_at(context: &DriverContext, now: SystemTime) -> Result<ReclaimOutcome, String> {
    let catalog = node_routing(context).catalog;
    let mut records: HashMap<Ulid, Option<GroupStorageBackend>> = HashMap::new();
    let mut outcome = ReclaimOutcome::default();
    let mut start_after = None;

    loop {
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            BLOB_RECLAIM_KEYSPACE,
            None,
            start_after,
            RECLAIM_PAGE_SIZE,
            None,
        )
        .await?;

        let mut stale = Vec::new();
        for (key, value) in values {
            let Some(candidate) = decode_candidate(&key, &value) else {
                stale.push(key);
                outcome.dropped = outcome.dropped.saturating_add(1);
                continue;
            };
            let backend = candidate.0.backend.clone();
            let strategy = match &candidate.0.backend {
                BackendRef::Node(name) => catalog.cleanup_of(name).unwrap_or_default(),
                BackendRef::Group(id) => group_strategy(context, &mut records, *id)
                    .await?
                    .unwrap_or_default(),
            };
            let Some(after) = strategy.grace() else {
                stale.push(key);
                outcome.dropped = outcome.dropped.saturating_add(1);
                continue;
            };
            if candidate
                .1
                .enqueued_at
                .checked_add(after)
                .is_none_or(|due| due > now)
            {
                outcome.not_due = outcome.not_due.saturating_add(1);
                continue;
            }
            let operation = ReclaimBlobOperation::new(candidate.0, candidate.1.enqueued_at, now);
            match drive(operation, context).await {
                Ok(ReclaimVerdict::Freed { bytes }) => {
                    outcome.freed = outcome.freed.saturating_add(1);
                    outcome.freed_bytes = outcome.freed_bytes.saturating_add(bytes);
                }
                Ok(ReclaimVerdict::Pinned) => outcome.pinned = outcome.pinned.saturating_add(1),
                Ok(ReclaimVerdict::Dropped) => outcome.dropped = outcome.dropped.saturating_add(1),
                Ok(ReclaimVerdict::NotDue) => outcome.not_due = outcome.not_due.saturating_add(1),
                Err(error) => {
                    warn!(backend = %backend, error = %error, "Blob reclaim candidate failed");
                    outcome.failed = outcome.failed.saturating_add(1);
                }
            }
        }

        delete_candidates(context, stale).await?;

        match next {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }

    info!(
        freed = outcome.freed,
        freed_bytes = outcome.freed_bytes,
        pinned = outcome.pinned,
        dropped = outcome.dropped,
        not_due = outcome.not_due,
        failed = outcome.failed,
        "Blob reclaim sweep finished"
    );
    Ok(outcome)
}

fn decode_candidate(key: &Key, value: &[u8]) -> Option<(ReclaimCandidateKey, ReclaimCandidate)> {
    match (
        ReclaimCandidateKey::from_bytes(key.as_ref()),
        ReclaimCandidate::from_bytes(value),
    ) {
        (Ok(key), Ok(candidate)) => Some((key, candidate)),
        _ => {
            warn!("Dropping undecodable reclaim candidate");
            None
        }
    }
}

/// Reads a tenant record once per drain. A missing record resolves to retain,
/// which is also forced: without the record there are no credentials to delete
/// the bytes with.
async fn group_strategy(
    context: &DriverContext,
    records: &mut HashMap<Ulid, Option<GroupStorageBackend>>,
    backend_id: Ulid,
) -> Result<Option<CleanupStrategy>, String> {
    if let std::collections::hash_map::Entry::Vacant(slot) = records.entry(backend_id) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
                key: backend_key(backend_id),
                txn_id: None,
            })
            .await;
        slot.insert(
            parse_read(event, GroupStorageBackend::from_bytes)
                .map_err(|error| error.to_string())?,
        );
    }
    Ok(records
        .get(&backend_id)
        .and_then(|record| record.as_ref())
        .map(|record| record.cleanup))
}

async fn delete_candidates(context: &DriverContext, keys: Vec<Key>) -> Result<(), String> {
    if keys.is_empty() {
        return Ok(());
    }
    let deletes = keys
        .into_iter()
        .map(|key| (BLOB_RECLAIM_KEYSPACE.to_string(), key))
        .collect();
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected candidate delete event: {other:?}")),
    }
}

/// Queue depth for one backend, computed from the queues themselves so it can
/// never drift. `truncated` reports that a scan hit its cap.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReclaimStatus {
    pub pending_candidates: usize,
    pub failing_cleanups: usize,
    pub oldest_enqueued_at: Option<SystemTime>,
    pub truncated: bool,
}

const STATUS_SCAN_LIMIT: usize = 10_000;

/// Counts one backend's queued candidates and the physical deletes still owed
/// to it. The candidate side is a bounded prefix scan; the cleanup queue has no
/// backend order, so that side is a capped filtered scan.
pub async fn backend_status(
    context: &DriverContext,
    backend: &BackendRef,
) -> Result<ReclaimStatus, String> {
    let mut status = ReclaimStatus::default();
    let (candidates, next) = iter_prefix_page(
        &context.storage_handle,
        BLOB_RECLAIM_KEYSPACE,
        Some(ReclaimCandidateKey::prefix(backend).into()),
        None,
        STATUS_SCAN_LIMIT,
        None,
    )
    .await?;
    status.pending_candidates = candidates.len();
    status.truncated = next.is_some();
    for (_, value) in candidates {
        if let Ok(candidate) = ReclaimCandidate::from_bytes(value.as_ref()) {
            status.oldest_enqueued_at = Some(match status.oldest_enqueued_at {
                Some(oldest) => oldest.min(candidate.enqueued_at),
                None => candidate.enqueued_at,
            });
        }
    }

    let (cleanups, next) = iter_prefix_page(
        &context.storage_handle,
        BLOB_CLEANUP_KEYSPACE,
        None,
        None,
        STATUS_SCAN_LIMIT,
        None,
    )
    .await?;
    status.truncated = status.truncated || next.is_some();
    status.failing_cleanups = cleanups
        .iter()
        .filter(
            |(_, value)| match BlobCleanupWork::from_bytes(value.as_ref()) {
                Ok(BlobCleanupWork::DeleteBlob { location }) => &location.backend == backend,
                _ => false,
            },
        )
        .count();

    Ok(status)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReclaimState {
    Init,
    StartTransaction,
    FenceBackend,
    ReadLocation,
    ScanAliases,
    ReadVersions,
    DeleteRows,
    QueueCleanup,
    UpdateUsage,
    CommitTransaction,
    AbortTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReclaimBlobError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
    #[error(transparent)]
    Read(#[from] RecordReadError),
    #[error("reclaim failed")]
    Failed,
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

/// Deletes one unreferenced copy. Everything that decides the outcome happens
/// inside a single transaction: the tenant record fence, the location read, and
/// the full alias scan, so a concurrent write either loses its commit or pins
/// the hash before this one commits.
#[derive(Debug, PartialEq)]
pub struct ReclaimBlobOperation {
    key: ReclaimCandidateKey,
    enqueued_at: SystemTime,
    sweep_time: SystemTime,
    state: ReclaimState,
    txn_id: Option<TxnId>,
    location: Option<BackendLocation>,
    next_alias_page: Option<Key>,
    usage_update: Option<UsageCounterUpdate>,
    output: Option<Result<ReclaimVerdict, ReclaimBlobError>>,
}

impl ReclaimBlobOperation {
    pub fn new(key: ReclaimCandidateKey, enqueued_at: SystemTime, sweep_time: SystemTime) -> Self {
        Self {
            key,
            enqueued_at,
            sweep_time,
            state: ReclaimState::Init,
            txn_id: None,
            location: None,
            next_alias_page: None,
            usage_update: None,
            output: None,
        }
    }

    fn location_key(&self) -> BlobLocationKey {
        BlobLocationKey::new(self.key.blake3, self.key.backend.clone())
    }

    fn fail(&mut self, error: ReclaimBlobError) -> Effects {
        self.output = Some(Err(error));
        if let Some(txn_id) = self.txn_id.take() {
            self.state = ReclaimState::AbortTransaction;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        self.state = ReclaimState::Error;
        smallvec![]
    }

    fn unexpected(&mut self, state: &'static str, expected: &'static str, event: Event) -> Effects {
        self.fail(ReclaimBlobError::InvalidStateEvent {
            state,
            expected,
            received: event,
        })
    }

    /// Ends the candidate without touching the bytes: the queue row goes, the
    /// location row stays.
    fn drop_candidate(&mut self, verdict: ReclaimVerdict) -> Effects {
        self.output = Some(Ok(verdict));
        self.state = ReclaimState::DeleteRows;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: vec![(
                BLOB_RECLAIM_KEYSPACE.to_string(),
                self.key.to_bytes().into()
            )],
            txn_id: self.txn_id,
        })]
    }

    fn handle_txn_started(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                self.txn_id = Some(txn_id);
                match self.key.backend {
                    BackendRef::Group(backend_id) => {
                        self.state = ReclaimState::FenceBackend;
                        smallvec![Effect::Storage(StorageEffect::Read {
                            key_space: GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
                            key: backend_key(backend_id),
                            txn_id: self.txn_id,
                        })]
                    }
                    BackendRef::Node(_) => self.read_location(),
                }
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.unexpected(
                "StartTransaction",
                "Event::Storage(StorageEvent::TransactionStarted)",
                received,
            ),
        }
    }

    /// Re-checks the tenant's strategy under the transaction, so a flip to
    /// retain conflicts with this sweep instead of racing it. A grace the tenant
    /// lengthened since the sweep read it makes the candidate not due again.
    fn handle_fence(&mut self, event: Event) -> Effects {
        let record = match parse_read(event, GroupStorageBackend::from_bytes) {
            Ok(Some(record)) => record,
            Ok(None) => return self.drop_candidate(ReclaimVerdict::Dropped),
            Err(error) => return self.fail(error.into()),
        };
        let Some(after) = record.cleanup.grace() else {
            return self.drop_candidate(ReclaimVerdict::Dropped);
        };
        match self.enqueued_at.checked_add(after) {
            Some(due) if due <= self.sweep_time => self.read_location(),
            _ => self.finish_not_due(),
        }
    }

    /// Leaves the queue row in place: the candidate is simply not due yet.
    fn finish_not_due(&mut self) -> Effects {
        self.output = Some(Ok(ReclaimVerdict::NotDue));
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = ReclaimState::AbortTransaction;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => {
                self.state = ReclaimState::Finish;
                smallvec![]
            }
        }
    }

    fn read_location(&mut self) -> Effects {
        self.state = ReclaimState::ReadLocation;
        smallvec![blob_location_read(&self.location_key(), self.txn_id)]
    }

    fn handle_location(&mut self, event: Event) -> Effects {
        let location = match parse_read(event, BackendLocation::from_bytes) {
            Ok(Some(location)) => location,
            Ok(None) => return self.drop_candidate(ReclaimVerdict::Dropped),
            Err(error) => return self.fail(error.into()),
        };
        // Staged and partial copies belong to the hidden sweep and never
        // credited the stored counters, so debiting them would underflow.
        if location.staging || location.partial {
            return self.drop_candidate(ReclaimVerdict::Dropped);
        }
        self.location = Some(location);
        self.scan_aliases(None)
    }

    fn scan_aliases(&mut self, start: Option<Key>) -> Effects {
        self.state = ReclaimState::ScanAliases;
        match iter_hash_path_index_effect(&self.key.blake3, start, self.txn_id) {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(error.into()),
        }
    }

    fn handle_alias_page(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.unexpected(
                "ScanAliases",
                "Event::Storage(StorageEvent::IterResult)",
                event,
            );
        };
        self.next_alias_page = next_start_after;
        let reads = match values
            .iter()
            .map(|(key, _)| {
                let alias = HashPathIndexKey::from_bytes(key.as_ref())?;
                let version = VersionKey::new(&alias.bucket, &alias.key, alias.version_id);
                Ok((
                    BLOB_VERSIONS_KEYSPACE.to_string(),
                    version.to_bytes()?.into(),
                ))
            })
            .collect::<Result<Vec<_>, ConversionError>>()
        {
            Ok(reads) => reads,
            Err(error) => return self.fail(error.into()),
        };
        if reads.is_empty() {
            return self.continue_scan();
        }
        self.state = ReclaimState::ReadVersions;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })]
    }

    /// A version that still names this exact copy pins it. An alias whose
    /// version row is gone does not pin; one that cannot be decoded fails the
    /// sweep closed and leaves the candidate queued.
    fn handle_versions(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected(
                "ReadVersions",
                "Event::Storage(StorageEvent::BatchReadResult)",
                event,
            );
        };
        let wanted = self.location_key();
        for (_, value) in values {
            let Some(value) = value else {
                continue;
            };
            let version = match BlobVersion::from_bytes(value.as_ref()) {
                Ok(version) => version,
                Err(error) => return self.fail(error.into()),
            };
            if version.location_key() == Some(wanted.clone()) {
                return self.drop_candidate(ReclaimVerdict::Pinned);
            }
        }
        self.continue_scan()
    }

    fn continue_scan(&mut self) -> Effects {
        match self.next_alias_page.take() {
            Some(start) => self.scan_aliases(Some(start)),
            None => self.free_copy(),
        }
    }

    /// The free path: location row, queue row, physical delete and the counter
    /// debit all commit together, so no window shows the copy as unreferenced
    /// while its bytes are neither queued for deletion nor still charged.
    fn free_copy(&mut self) -> Effects {
        let Some(location) = self.location.clone() else {
            return self.fail(ReclaimBlobError::Failed);
        };
        self.output = Some(Ok(ReclaimVerdict::Freed {
            bytes: location.blob_size,
        }));
        self.state = ReclaimState::DeleteRows;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: vec![
                (
                    BLOB_LOCATIONS_KEYSPACE.to_string(),
                    self.location_key().to_bytes().into()
                ),
                (
                    BLOB_RECLAIM_KEYSPACE.to_string(),
                    self.key.to_bytes().into()
                ),
            ],
            txn_id: self.txn_id,
        })]
    }

    fn handle_rows_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.unexpected(
                "DeleteRows",
                "Event::Storage(StorageEvent::BatchDeleteResult)",
                event,
            );
        };
        let Some(location) = self
            .location
            .clone()
            .filter(|_| matches!(self.output, Some(Ok(ReclaimVerdict::Freed { .. }))))
        else {
            return self.commit();
        };
        let work = match (BlobCleanupWork::DeleteBlob { location }).to_bytes() {
            Ok(work) => work,
            Err(error) => return self.fail(error.into()),
        };
        self.state = ReclaimState::QueueCleanup;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            key: Ulid::generate().to_bytes().to_vec().into(),
            value: work.into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_cleanup_queued(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.unexpected(
                "QueueCleanup",
                "Event::Storage(StorageEvent::WriteResult)",
                event,
            );
        };
        let (Some(txn_id), Some(location)) = (self.txn_id, self.location.as_ref()) else {
            return self.fail(ReclaimBlobError::Failed);
        };
        let mut update = UsageCounterUpdate::for_stored(StoredDelta::new(
            self.key.blake3,
            self.key.backend.clone(),
            -1,
            -i128::from(location.blob_size),
        ));
        if update.is_noop() {
            return self.commit();
        }
        self.state = ReclaimState::UpdateUsage;
        let effects = update.start(txn_id);
        self.usage_update = Some(update);
        effects
    }

    fn handle_usage(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(ReclaimBlobError::Failed);
        };
        let Some(update) = self.usage_update.as_mut() else {
            return self.fail(ReclaimBlobError::Failed);
        };
        match update.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => self.commit(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(ReclaimBlobError::Failed);
        };
        self.state = ReclaimState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        self.txn_id = None;
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.state = ReclaimState::Finish;
                match self.output {
                    Some(Ok(ReclaimVerdict::Freed { .. })) => {
                        smallvec![schedule_blob_cleanup_effect()]
                    }
                    _ => smallvec![],
                }
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.unexpected(
                "CommitTransaction",
                "Event::Storage(StorageEvent::TransactionCommitted)",
                received,
            ),
        }
    }

    fn handle_aborted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionAborted { .. })
            | Event::Storage(StorageEvent::Error { .. }) => {
                self.state = match self.output {
                    Some(Ok(_)) => ReclaimState::Finish,
                    _ => ReclaimState::Error,
                };
                smallvec![]
            }
            received => self.unexpected(
                "AbortTransaction",
                "Event::Storage(StorageEvent::TransactionAborted)",
                received,
            ),
        }
    }
}

impl Operation for ReclaimBlobOperation {
    type Output = ReclaimVerdict;
    type Error = ReclaimBlobError;

    fn start(&mut self) -> Effects {
        self.state = ReclaimState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReclaimState::Init => self.start(),
            ReclaimState::StartTransaction => self.handle_txn_started(event),
            ReclaimState::FenceBackend => self.handle_fence(event),
            ReclaimState::ReadLocation => self.handle_location(event),
            ReclaimState::ScanAliases => self.handle_alias_page(event),
            ReclaimState::ReadVersions => self.handle_versions(event),
            ReclaimState::DeleteRows => self.handle_rows_deleted(event),
            ReclaimState::QueueCleanup => self.handle_cleanup_queued(event),
            ReclaimState::UpdateUsage => self.handle_usage(event),
            ReclaimState::CommitTransaction => self.handle_committed(event),
            ReclaimState::AbortTransaction => self.handle_aborted(event),
            ReclaimState::Finish | ReclaimState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReclaimState::Finish | ReclaimState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            ReclaimState::Finish => self.output.unwrap_or(Err(ReclaimBlobError::Failed)),
            _ => match self.output {
                Some(Err(error)) => Err(error),
                _ => Err(ReclaimBlobError::Failed),
            },
        }
    }

    fn abort(&mut self) -> Effects {
        if let Some(txn_id) = self.txn_id.take() {
            self.state = ReclaimState::AbortTransaction;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keyspaces::HASH_PATHS_INDEX_KEYSPACE;
    use aruna_core::structs::{RealmId, UsageCounters, usage_backend_key, usage_hash_key};
    use aruna_core::types::Value;
    use std::collections::HashMap;
    use tempfile::tempdir;

    const HASH: [u8; 32] = [4u8; 32];

    /// Long past its grace, so only the case under test decides the verdict.
    fn reclaim_op(key: ReclaimCandidateKey) -> ReclaimBlobOperation {
        ReclaimBlobOperation::new(key, SystemTime::UNIX_EPOCH, SystemTime::now())
    }

    fn context(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    fn location(size: u64) -> BackendLocation {
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "storage".to_string(),
            backend_path: "bucket/key_01".to_string(),
            ulid: Ulid::from_bytes([5u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: size,
            hashes: HashMap::new(),
        }
    }

    fn candidate_key() -> ReclaimCandidateKey {
        ReclaimCandidateKey::new(BackendRef::node_default(), HASH)
    }

    async fn write(context: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn read(context: &DriverContext, key_space: &str, key: Vec<u8>) -> Option<Value> {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: key.into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => panic!("unexpected read event: {other:?}"),
        }
    }

    /// Location row, queue row and the counters a write would have credited.
    async fn seed(context: &DriverContext, size: u64) {
        write(
            context,
            BLOB_LOCATIONS_KEYSPACE,
            BlobLocationKey::new(HASH, BackendRef::node_default()).to_bytes(),
            location(size).to_bytes().unwrap(),
        )
        .await;
        write(
            context,
            BLOB_RECLAIM_KEYSPACE,
            candidate_key().to_bytes(),
            ReclaimCandidate {
                enqueued_at: SystemTime::UNIX_EPOCH,
            }
            .to_bytes()
            .unwrap(),
        )
        .await;
        let counters = UsageCounters {
            stored_blobs: 1,
            stored_bytes: size,
            ..Default::default()
        };
        write(
            context,
            aruna_core::keyspaces::USAGE_STATS_KEYSPACE,
            usage_hash_key(&HASH),
            counters.to_bytes().unwrap(),
        )
        .await;
        write(
            context,
            aruna_core::keyspaces::USAGE_STATS_KEYSPACE,
            usage_backend_key(&BackendRef::node_default(), shard_of_hash()),
            UsageCounters {
                stored_bytes: size,
                ..Default::default()
            }
            .to_bytes()
            .unwrap(),
        )
        .await;
    }

    fn shard_of_hash() -> usize {
        aruna_core::structs::shard_for_hash(&HASH)
    }

    async fn add_alias(context: &DriverContext, version_id: Ulid, pins: bool) {
        let alias = HashPathIndexKey::new(
            HASH,
            version_id,
            RealmId::from_bytes([1u8; 32]),
            Ulid::from_bytes([2u8; 16]),
            iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            "bucket",
            "key",
        );
        write(
            context,
            HASH_PATHS_INDEX_KEYSPACE,
            alias.to_bytes().unwrap(),
            Vec::new(),
        )
        .await;
        let backend = if pins {
            BackendRef::node_default()
        } else {
            BackendRef::Node("cold".to_string())
        };
        write(
            context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("bucket", "key", version_id)
                .to_bytes()
                .unwrap(),
            BlobVersion::materialized(
                HASH,
                backend,
                SystemTime::UNIX_EPOCH,
                Default::default(),
                None,
            )
            .to_bytes()
            .unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn frees_unreferenced_copy() {
        // The location row, the queue row, the counters and the queued physical
        // delete all move in one transaction.
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;

        let verdict = drive(reclaim_op(candidate_key()), &context).await.unwrap();

        assert_eq!(verdict, ReclaimVerdict::Freed { bytes: 10 });
        assert!(
            read(
                &context,
                BLOB_LOCATIONS_KEYSPACE,
                BlobLocationKey::new(HASH, BackendRef::node_default()).to_bytes()
            )
            .await
            .is_none()
        );
        assert!(
            read(&context, BLOB_RECLAIM_KEYSPACE, candidate_key().to_bytes())
                .await
                .is_none()
        );
        let counters = read(
            &context,
            aruna_core::keyspaces::USAGE_STATS_KEYSPACE,
            usage_hash_key(&HASH),
        )
        .await
        .unwrap();
        assert_eq!(
            UsageCounters::from_bytes(counters.as_ref()).unwrap(),
            UsageCounters::default()
        );
        let (queued, _) = iter_prefix_page(
            &context.storage_handle,
            BLOB_CLEANUP_KEYSPACE,
            None,
            None,
            10,
            None,
        )
        .await
        .unwrap();
        assert_eq!(queued.len(), 1);
    }

    #[tokio::test]
    async fn pinned_copy_survives() {
        // An alias whose version still names this backend cancels the candidate.
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;
        add_alias(&context, Ulid::from_bytes([6u8; 16]), true).await;

        let verdict = drive(reclaim_op(candidate_key()), &context).await.unwrap();

        assert_eq!(verdict, ReclaimVerdict::Pinned);
        assert!(
            read(
                &context,
                BLOB_LOCATIONS_KEYSPACE,
                BlobLocationKey::new(HASH, BackendRef::node_default()).to_bytes()
            )
            .await
            .is_some()
        );
        assert!(
            read(&context, BLOB_RECLAIM_KEYSPACE, candidate_key().to_bytes())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn other_backend_never_pins() {
        // Deduplication is per backend, so a copy elsewhere holds nothing here.
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;
        add_alias(&context, Ulid::from_bytes([7u8; 16]), false).await;

        let verdict = drive(reclaim_op(candidate_key()), &context).await.unwrap();

        assert_eq!(verdict, ReclaimVerdict::Freed { bytes: 10 });
    }

    #[tokio::test]
    async fn missing_location_drops() {
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        write(
            &context,
            BLOB_RECLAIM_KEYSPACE,
            candidate_key().to_bytes(),
            ReclaimCandidate {
                enqueued_at: SystemTime::UNIX_EPOCH,
            }
            .to_bytes()
            .unwrap(),
        )
        .await;

        let verdict = drive(reclaim_op(candidate_key()), &context).await.unwrap();

        assert_eq!(verdict, ReclaimVerdict::Dropped);
        assert!(
            read(&context, BLOB_RECLAIM_KEYSPACE, candidate_key().to_bytes())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn grace_defers_candidate() {
        // Inside the grace window the sweep leaves both rows alone.
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;

        let outcome = sweep_at(&context, SystemTime::UNIX_EPOCH).await.unwrap();

        assert_eq!(outcome.not_due, 1);
        assert_eq!(outcome.freed, 0);
        assert!(
            read(&context, BLOB_RECLAIM_KEYSPACE, candidate_key().to_bytes())
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn sweep_frees_when_due() {
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;
        let due = SystemTime::UNIX_EPOCH
            + CleanupStrategy::DEFAULT_RECLAIM_AFTER
            + Duration::from_secs(1);

        let outcome = sweep_at(&context, due).await.unwrap();

        assert_eq!(outcome.freed, 1);
        assert_eq!(outcome.freed_bytes, 10);
    }

    #[test]
    fn fence_drops_retain() {
        // A tenant flip to retain is read inside the transaction and stands down.
        let backend_id = Ulid::from_bytes([8u8; 16]);
        let mut operation = reclaim_op(ReclaimCandidateKey::new(
            BackendRef::Group(backend_id),
            HASH,
        ));
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([9u8; 16]),
        }));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(group_record(backend_id, CleanupStrategy::Retain).into()),
        }));

        let [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })] = effects.as_slice()
        else {
            panic!("expected the queue row to be dropped, got {effects:?}")
        };
        assert_eq!(deletes.len(), 1);
        assert_eq!(deletes[0].0, BLOB_RECLAIM_KEYSPACE);
    }

    #[test]
    fn fence_keeps_not_due() {
        // A grace the tenant lengthened after the sweep read it must leave the
        // queue row alone rather than delete the copy under the old value.
        let backend_id = Ulid::from_bytes([8u8; 16]);
        let enqueued_at = SystemTime::UNIX_EPOCH;
        let sweep_time = enqueued_at + Duration::from_secs(60);
        let mut operation = ReclaimBlobOperation::new(
            ReclaimCandidateKey::new(BackendRef::Group(backend_id), HASH),
            enqueued_at,
            sweep_time,
        );
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([9u8; 16]),
        }));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(
                group_record(
                    backend_id,
                    CleanupStrategy::Reclaim {
                        after: Duration::from_secs(3_600),
                    },
                )
                .into(),
            ),
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: Ulid::from_bytes([9u8; 16]),
        }));
        assert_eq!(operation.finalize(), Ok(ReclaimVerdict::NotDue));
    }

    fn group_record(backend_id: Ulid, cleanup: CleanupStrategy) -> Vec<u8> {
        GroupStorageBackend {
            backend_id,
            group_id: Ulid::from_bytes([2u8; 16]),
            name: "tenant".to_string(),
            kind: aruna_core::structs::GroupBackendKind::S3,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: false,
            cleanup,
        }
        .to_bytes()
        .unwrap()
    }

    #[tokio::test]
    async fn status_counts_one_backend() {
        // Another backend's queue rows must not show up in this one's depth.
        let dir = tempdir().unwrap();
        let context = context(dir.path().to_str().unwrap());
        seed(&context, 10).await;
        write(
            &context,
            BLOB_RECLAIM_KEYSPACE,
            ReclaimCandidateKey::new(BackendRef::Node("cold".to_string()), [9u8; 32]).to_bytes(),
            ReclaimCandidate {
                enqueued_at: SystemTime::UNIX_EPOCH,
            }
            .to_bytes()
            .unwrap(),
        )
        .await;

        let status = backend_status(&context, &BackendRef::node_default())
            .await
            .unwrap();

        assert_eq!(status.pending_candidates, 1);
        assert_eq!(status.failing_cleanups, 0);
        assert_eq!(status.oldest_enqueued_at, Some(SystemTime::UNIX_EPOCH));
        assert!(!status.truncated);
    }

    #[test]
    fn rejects_stray_event() {
        // A write acknowledgement is not a transaction start.
        let mut operation = reclaim_op(candidate_key());
        operation.start();

        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));

        assert!(matches!(
            operation.finalize(),
            Err(ReclaimBlobError::InvalidStateEvent { .. })
        ));
    }
}
