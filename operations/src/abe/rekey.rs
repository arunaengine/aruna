//! Gives every version under a key prefix a new object key and envelope, one page per call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::KeyError;
use crate::blob::migration::queue::quota_origin;
use crate::blob::migration::rewrite::{RewriteOutcome, RewriteVersionOperation};
use crate::driver::{DriverContext, drive};
use crate::jobs::store::iter_prefix_page;
use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, RekeyOutcome, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    ABE_DUE_KEYSPACE, ABE_EPOCH_KEYSPACE, ABE_REKEY_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::realm::{QuotaConfig, RealmId};
use aruna_core::structs::storage::abe::AbeError;
use aruna_core::structs::storage::blob::{BucketInfo, VersionKey};
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyRecord, SealPlan};
use aruna_core::structs::storage::transition::{
    EncryptionTransition, TransitionKind, TransitionTarget,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use aruna_storage::StorageHandle;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use std::time::{SystemTime, UNIX_EPOCH};

/// Version rows one page scans for the prefix.
const SCAN: usize = 1024;

/// A bucket's unfinished re-key pass, keyed by bucket id in `abe_rekeys`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RekeyProgress {
    pub prefix: String,
    /// The epoch of the new envelopes; a raise since then starts the pass again.
    pub epoch: u64,
    /// The last version row handled.
    pub cursor: Vec<u8>,
    pub rekeyed: u64,
}

async fn read_rows(
    storage: &StorageHandle,
    reads: Vec<(String, Key)>,
    txn_id: Option<TxnId>,
) -> Result<Vec<Option<Value>>, KeyError> {
    match storage
        .send_storage_effect(StorageEffect::BatchRead { reads, txn_id })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
        _ => Err(KeyError::Storage),
    }
}

fn epoch_of(row: Option<&Value>) -> Option<u64> {
    Some(u64::from_be_bytes(row?.as_ref().try_into().ok()?))
}

/// Re-keys up to `page` versions under `prefix` after the saved cursor through the rotation
/// unit, then saves the cursor. Returns the progress and whether the pass is done.
pub async fn rekey_page(
    context: &DriverContext,
    bucket: &str,
    prefix: &str,
    page: usize,
) -> Result<(RekeyProgress, bool), KeyError> {
    let storage = &context.storage_handle;
    let name: Key = bucket.as_bytes().to_vec().into();
    let reads = vec![
        (BUCKET_ENCRYPTION_KEYSPACE.to_string(), name.clone()),
        (S3_BUCKET_KEYSPACE.to_string(), name),
    ];
    let [settings, info] = <[_; 2]>::try_from(read_rows(storage, reads, None).await?)
        .map_err(|_| KeyError::Storage)?;
    let settings =
        BucketEncryption::from_row(settings.as_deref()).map_err(|_| KeyError::Missing)?;
    let info = info.as_deref().map(BucketInfo::from_bytes);
    let (Some(key), Some(Ok(info))) = (settings.active_key(), info) else {
        return Err(KeyError::Missing);
    };
    let id: Key = key.bucket_id.to_bytes().to_vec().into();
    let reads = vec![
        (BUCKET_KEY_KEYSPACE.to_string(), key.key().into()),
        (ABE_EPOCH_KEYSPACE.to_string(), id.clone()),
        (ABE_REKEY_KEYSPACE.to_string(), id.clone()),
    ];
    let [record, epoch, seen] = <[_; 3]>::try_from(read_rows(storage, reads, None).await?)
        .map_err(|_| KeyError::Storage)?;
    let record = record.as_deref().map(BucketKeyRecord::from_bytes);
    let plan = match record {
        Some(Ok(record)) => SealPlan::capture(&settings, &record).ok().flatten(),
        _ => None,
    };
    let (Some(plan), Some(epoch)) = (plan, epoch_of(epoch.as_ref())) else {
        return Err(KeyError::Missing);
    };
    let fresh = RekeyProgress {
        prefix: prefix.to_string(),
        epoch,
        cursor: Vec::new(),
        rekeyed: 0,
    };
    let mut progress = match seen.as_deref().map(postcard::from_bytes::<RekeyProgress>) {
        None => fresh,
        Some(Ok(saved)) if saved.prefix != prefix => return Err(KeyError::Busy),
        Some(Ok(saved)) if saved.epoch != epoch => fresh,
        Some(Ok(saved)) => saved,
        Some(Err(_)) => return Err(AbeError::Context.into()),
    };
    let target = TransitionTarget {
        compression: info.compression,
        plan: Some(plan),
    };
    let now = aruna_core::time::unix_timestamp_millis();
    let generation = settings.storage_generation;
    let unit =
        EncryptionTransition::new(TransitionKind::Rotate, Some(key), target, generation, now);
    let quota = quota_origin(context).await.map_err(|_| KeyError::Storage)?;
    let versions = VersionKey::bucket_prefix(bucket).map_err(|_| KeyError::Storage)?;
    let after = (!progress.cursor.is_empty()).then(|| progress.cursor.clone().into());
    let scan = iter_prefix_page(
        storage,
        BLOB_VERSIONS_KEYSPACE,
        Some(versions.into()),
        after,
        SCAN,
        None,
    );
    let (rows, next) = scan.await.map_err(|_| KeyError::Storage)?;
    let (mut moved, mut handled, mut stopped) = (0, 0, None);
    for (row, _) in &rows {
        if moved >= page.max(1) {
            break;
        }
        let version = VersionKey::from_bytes(row).map_err(|_| KeyError::Storage)?;
        if version.key.starts_with(prefix) {
            let mut operation =
                RewriteVersionOperation::new(version, unit.clone(), SystemTime::now()).rekey();
            if let Some((quota, realm, node)) = &quota {
                operation = operation.with_quota(quota.clone(), *realm, *node);
            }
            match drive(operation, context).await {
                Ok(RewriteOutcome::Moved) => {
                    progress.rekeyed += 1;
                    moved += 1;
                }
                Ok(RewriteOutcome::Skipped) => {}
                Ok(RewriteOutcome::AwaitingKey) => {
                    stopped = Some(KeyError::Locked);
                    break;
                }
                Err(error) => {
                    tracing::warn!(event = "abe.rekey.failed", bucket, error = %error);
                    stopped = Some(KeyError::Storage);
                    break;
                }
            }
        }
        progress.cursor = row.to_vec();
        handled += 1;
    }
    let more = stopped.is_some() || handled < rows.len() || next.is_some();
    let done = save(storage, id, seen, &mut progress, more).await?;
    match stopped {
        Some(error) => Err(error),
        None => Ok((progress, done)),
    }
}

/// Saves the cursor while the row is still the one this page read. The last page finishes the
/// pass only without a raise or due removal since it began; otherwise the pass starts again.
async fn save(
    storage: &StorageHandle,
    id: Key,
    seen: Option<Value>,
    progress: &mut RekeyProgress,
    more: bool,
) -> Result<bool, KeyError> {
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        _ => return Err(KeyError::Storage),
    };
    let staged = stage(storage, txn_id, id, seen, progress, more).await;
    let commit = match staged {
        Ok(Some(_)) => StorageEffect::CommitTransaction { txn_id },
        _ => StorageEffect::AbortTransaction { txn_id },
    };
    let event = storage.send_storage_effect(commit).await;
    match (staged, event) {
        (Ok(Some(done)), Event::Storage(StorageEvent::TransactionCommitted { .. })) => Ok(done),
        (Ok(Some(_)), _) => Err(KeyError::Storage),
        // Another call moved the pass meanwhile; its own save stands.
        (Ok(None), _) => Ok(false),
        (Err(error), _) => Err(error),
    }
}

async fn stage(
    storage: &StorageHandle,
    txn_id: TxnId,
    id: Key,
    seen: Option<Value>,
    progress: &mut RekeyProgress,
    more: bool,
) -> Result<Option<bool>, KeyError> {
    let reads = vec![
        (ABE_REKEY_KEYSPACE.to_string(), id.clone()),
        (ABE_EPOCH_KEYSPACE.to_string(), id.clone()),
        (ABE_DUE_KEYSPACE.to_string(), id.clone()),
    ];
    let [row, epoch, due] = <[_; 3]>::try_from(read_rows(storage, reads, Some(txn_id)).await?)
        .map_err(|_| KeyError::Storage)?;
    if row != seen {
        return Ok(None);
    }
    // A removal during the walk is not covered by it, so the subtree is walked again.
    let restart = !more && (epoch_of(epoch.as_ref()) != Some(progress.epoch) || due.is_some());
    if restart {
        progress.cursor.clear();
        progress.rekeyed = 0;
    }
    let done = !more && !restart;
    let effect = match done {
        true => StorageEffect::BatchDelete {
            deletes: vec![(ABE_REKEY_KEYSPACE.to_string(), id)],
            txn_id: Some(txn_id),
        },
        false => StorageEffect::BatchWrite {
            writes: vec![(
                ABE_REKEY_KEYSPACE.to_string(),
                id,
                postcard::to_allocvec(progress)
                    .map_err(|_| KeyError::Storage)?
                    .into(),
            )],
            txn_id: Some(txn_id),
        },
    };
    match storage.send_storage_effect(effect).await {
        Event::Storage(
            StorageEvent::BatchDeleteResult { .. } | StorageEvent::BatchWriteResult { .. },
        ) => Ok(Some(done)),
        _ => Err(KeyError::Storage),
    }
}

/// Realm quota and origin that new envelope charges must fit.
type Quota = Option<(QuotaConfig, RealmId, NodeId)>;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Step {
    Settings,
    Rows,
    Scan,
    Run,
    Start,
    Check,
    Save,
    Commit,
    Done,
}

/// Re-keys up to `page` versions under `prefix` after the saved cursor through the rotation
/// unit, then saves the cursor. Outputs the progress and whether the pass is done.
#[derive(Debug, PartialEq)]
pub struct RekeyOperation {
    bucket: String,
    prefix: String,
    page: usize,
    quota: Quota,
    now: SystemTime,
    step: Step,
    id: Key,
    settings: Option<(BucketEncryption, BucketInfo)>,
    /// The progress row this page started from.
    seen: Option<Value>,
    unit: Option<EncryptionTransition>,
    progress: Option<RekeyProgress>,
    rows: Vec<Key>,
    handled: usize,
    next: bool,
    moved: usize,
    stopped: Option<KeyError>,
    done: bool,
    txn: Option<TxnId>,
    output: Option<Result<(RekeyProgress, bool), KeyError>>,
}

impl RekeyOperation {
    pub fn new(bucket: &str, prefix: &str, page: usize, quota: Quota, now: SystemTime) -> Self {
        Self {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            page: page.max(1),
            quota,
            now,
            step: Step::Settings,
            id: Key::from(Vec::new()),
            settings: None,
            seen: None,
            unit: None,
            progress: None,
            rows: Vec::new(),
            handled: 0,
            next: false,
            moved: 0,
            stopped: None,
            done: false,
            txn: None,
            output: None,
        }
    }

    fn finish(&mut self, result: Result<(RekeyProgress, bool), KeyError>) -> Effects {
        self.step = Step::Done;
        self.output = Some(result);
        self.abort()
    }

    /// Ends with the error that stopped the page, else with the progress.
    fn end(&mut self) -> Effects {
        let result = match (self.stopped.take(), self.progress.clone()) {
            (Some(error), _) => Err(error),
            (None, Some(progress)) => Ok((progress, self.done)),
            (None, None) => Err(KeyError::Storage),
        };
        self.finish(result)
    }

    fn settings_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let Ok([(_, settings), (_, info)]) = <[_; 2]>::try_from(values) else {
            return self.finish(Err(KeyError::Storage));
        };
        let settings = BucketEncryption::from_row(settings.as_deref());
        let info = info.as_deref().map(BucketInfo::from_bytes);
        let (Ok(settings), Some(Ok(info))) = (settings, info) else {
            return self.finish(Err(KeyError::Missing));
        };
        let Some(key) = settings.active_key() else {
            return self.finish(Err(KeyError::Missing));
        };
        self.id = key.bucket_id.to_bytes().to_vec().into();
        self.settings = Some((settings, info));
        self.step = Step::Rows;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (BUCKET_KEY_KEYSPACE.to_string(), key.key().into()),
                (ABE_EPOCH_KEYSPACE.to_string(), self.id.clone()),
                (ABE_REKEY_KEYSPACE.to_string(), self.id.clone()),
            ],
            txn_id: None,
        })]
    }

    fn rows_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let (Some((settings, info)), Ok([(_, record), (_, epoch), (_, seen)])) =
            (self.settings.take(), <[_; 3]>::try_from(values))
        else {
            return self.finish(Err(KeyError::Storage));
        };
        let record = record.as_deref().map(BucketKeyRecord::from_bytes);
        let plan = match record {
            Some(Ok(record)) => SealPlan::capture(&settings, &record).ok().flatten(),
            _ => None,
        };
        let (Some(key), Some(plan), Some(epoch)) =
            (settings.active_key(), plan, epoch_of(epoch.as_ref()))
        else {
            return self.finish(Err(KeyError::Missing));
        };
        let fresh = RekeyProgress {
            prefix: self.prefix.clone(),
            epoch,
            cursor: Vec::new(),
            rekeyed: 0,
        };
        let progress = match seen.as_deref().map(postcard::from_bytes::<RekeyProgress>) {
            None => fresh,
            Some(Ok(saved)) if saved.prefix != self.prefix => {
                return self.finish(Err(KeyError::Busy));
            }
            Some(Ok(saved)) if saved.epoch != epoch => fresh,
            Some(Ok(saved)) => saved,
            Some(Err(_)) => return self.finish(Err(AbeError::Context.into())),
        };
        let target = TransitionTarget {
            compression: info.compression,
            plan: Some(plan),
        };
        let now = self.now.duration_since(UNIX_EPOCH).unwrap_or_default();
        let (kind, generation) = (TransitionKind::Rotate, settings.storage_generation);
        let now = now.as_millis() as u64;
        let unit = EncryptionTransition::new(kind, Some(key), target, generation, now);
        self.unit = Some(unit);
        let Ok(versions) = VersionKey::bucket_prefix(&self.bucket) else {
            return self.finish(Err(KeyError::Storage));
        };
        let after = (!progress.cursor.is_empty()).then(|| progress.cursor.clone().into());
        self.seen = seen;
        self.progress = Some(progress);
        self.step = Step::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: Some(versions.into()),
            start: after.map(IterStart::After),
            limit: SCAN,
            txn_id: None,
        })]
    }

    /// Starts the next version under the prefix; others only move the cursor.
    fn run_next(&mut self) -> Effects {
        while let Some(row) = self.rows.get(self.handled) {
            if self.moved >= self.page {
                break;
            }
            let Ok(version) = VersionKey::from_bytes(row) else {
                return self.finish(Err(KeyError::Storage));
            };
            if version.key.starts_with(&self.prefix) {
                return self.rewrite(version);
            }
            self.advance();
        }
        self.save()
    }

    fn save(&mut self) -> Effects {
        self.step = Step::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn advance(&mut self) {
        if let (Some(row), Some(progress)) = (self.rows.get(self.handled), self.progress.as_mut()) {
            progress.cursor = row.to_vec();
        }
        self.handled += 1;
    }

    fn rewrite(&mut self, version: VersionKey) -> Effects {
        let Some(unit) = self.unit.clone() else {
            return self.finish(Err(KeyError::Storage));
        };
        let mut operation = RewriteVersionOperation::new(version, unit, self.now).rekey();
        if let Some((quota, realm, node)) = &self.quota {
            operation = operation.with_quota(quota.clone(), *realm, *node);
        }
        let bucket = self.bucket.clone();
        let sub = boxed_suboperation(operation, move |result| {
            let outcome = match result {
                Ok(RewriteOutcome::Moved) => RekeyOutcome::Moved,
                Ok(RewriteOutcome::Skipped) => RekeyOutcome::Skipped,
                Ok(RewriteOutcome::AwaitingKey) => RekeyOutcome::Locked,
                Err(error) => {
                    tracing::warn!(event = "abe.rekey.failed", bucket = %bucket, error = %error);
                    RekeyOutcome::Failed
                }
            };
            Event::SubOperation(SubOperationEvent::VersionRekeyed { outcome })
        });
        self.step = Step::Run;
        smallvec![Effect::SubOperation(sub)]
    }

    fn version_done(&mut self, outcome: RekeyOutcome) -> Effects {
        match outcome {
            RekeyOutcome::Moved => {
                if let Some(progress) = self.progress.as_mut() {
                    progress.rekeyed += 1;
                }
                self.moved += 1;
            }
            RekeyOutcome::Skipped => {}
            RekeyOutcome::Locked => self.stopped = Some(KeyError::Locked),
            RekeyOutcome::Failed => self.stopped = Some(KeyError::Storage),
        }
        // A stopped page keeps its cursor before this version.
        if self.stopped.is_some() {
            return self.save();
        }
        self.advance();
        self.run_next()
    }

    /// Saves the cursor while the row is still the one this page read. The last page finishes the
    /// pass only without a raise or due removal since it began; otherwise the pass starts again.
    fn check_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let (Some(txn_id), Ok([(_, row), (_, epoch), (_, due)])) =
            (self.txn, <[_; 3]>::try_from(values))
        else {
            return self.finish(Err(KeyError::Storage));
        };
        // Another call moved the pass meanwhile; its own save stands.
        if row != self.seen {
            return self.end();
        }
        let more = self.stopped.is_some() || self.handled < self.rows.len() || self.next;
        let Some(progress) = self.progress.as_mut() else {
            return self.finish(Err(KeyError::Storage));
        };
        // A removal during the walk is not covered by it, so the subtree is walked again.
        let restart = !more && (epoch_of(epoch.as_ref()) != Some(progress.epoch) || due.is_some());
        if restart {
            progress.cursor.clear();
            progress.rekeyed = 0;
        }
        self.done = !more && !restart;
        let effect = match (self.done, postcard::to_allocvec(&*progress)) {
            (true, _) => StorageEffect::BatchDelete {
                deletes: vec![(ABE_REKEY_KEYSPACE.to_string(), self.id.clone())],
                txn_id: Some(txn_id),
            },
            (false, Ok(row)) => StorageEffect::BatchWrite {
                writes: vec![(ABE_REKEY_KEYSPACE.to_string(), self.id.clone(), row.into())],
                txn_id: Some(txn_id),
            },
            (false, Err(_)) => return self.finish(Err(KeyError::Storage)),
        };
        self.step = Step::Save;
        smallvec![Effect::Storage(effect)]
    }
}

impl Operation for RekeyOperation {
    type Output = (RekeyProgress, bool);
    type Error = KeyError;
    fn start(&mut self) -> Effects {
        let name: Key = self.bucket.as_bytes().to_vec().into();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (BUCKET_ENCRYPTION_KEYSPACE.to_string(), name.clone()),
                (S3_BUCKET_KEYSPACE.to_string(), name),
            ],
            txn_id: None,
        })]
    }
    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (Step::Settings, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.settings_read(values)
            }
            (Step::Rows, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.rows_read(values)
            }
            (
                Step::Scan,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                self.rows = values.into_iter().map(|(key, _)| key).collect();
                self.next = next_start_after.is_some();
                self.run_next()
            }
            (Step::Run, Event::SubOperation(SubOperationEvent::VersionRekeyed { outcome })) => {
                self.version_done(outcome)
            }
            (Step::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn = Some(txn_id);
                self.step = Step::Check;
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
                        (ABE_REKEY_KEYSPACE.to_string(), self.id.clone()),
                        (ABE_EPOCH_KEYSPACE.to_string(), self.id.clone()),
                        (ABE_DUE_KEYSPACE.to_string(), self.id.clone()),
                    ],
                    txn_id: Some(txn_id),
                })]
            }
            (Step::Check, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.check_read(values)
            }
            (
                Step::Save,
                Event::Storage(
                    StorageEvent::BatchWriteResult { .. } | StorageEvent::BatchDeleteResult { .. },
                ),
            ) => match self.txn {
                Some(txn_id) => {
                    self.step = Step::Commit;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                None => self.finish(Err(KeyError::Storage)),
            },
            (Step::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn = None;
                self.end()
            }
            _ => self.finish(Err(KeyError::Storage)),
        }
    }
    fn is_complete(&self) -> bool {
        self.step == Step::Done
    }
    fn finalize(self) -> Result<(RekeyProgress, bool), KeyError> {
        self.output.unwrap_or(Err(KeyError::Storage))
    }
    fn abort(&mut self) -> Effects {
        self.txn.take().map_or_else(Effects::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}
