//! Gives every version under a key prefix a new object key and envelope, one page per call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::KeyError;
use crate::blob::migration::queue::quota_origin;
use crate::blob::migration::rewrite::{RewriteOutcome, RewriteVersionOperation};
use crate::driver::{DriverContext, drive};
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

fn epoch_of(row: Option<&Value>) -> Option<u64> {
    Some(u64::from_be_bytes(row?.as_ref().try_into().ok()?))
}

/// Realm quota and origin that new envelope charges must fit.
type Quota = Option<(QuotaConfig, RealmId, NodeId)>;

/// Re-keys one page under `prefix` and returns the progress and whether the pass is done.
pub async fn rekey_page(
    context: &DriverContext,
    bucket: &str,
    prefix: &str,
    page: usize,
) -> Result<(RekeyProgress, bool), KeyError> {
    let quota = quota_origin(context).await.map_err(|_| KeyError::Storage)?;
    let operation = RekeyOperation::new(bucket, prefix, page, quota, SystemTime::now());
    drive(operation, context).await
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Step {
    Settings,
    Rows,
    Claim,
    ClaimRead,
    ClaimWrite,
    ClaimCommit,
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
            ],
            txn_id: None,
        })]
    }

    fn rows_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let (Some((settings, info)), Ok([(_, record), (_, epoch)])) =
            (self.settings.take(), <[_; 2]>::try_from(values))
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
        self.progress = Some(RekeyProgress {
            prefix: self.prefix.clone(),
            epoch,
            cursor: Vec::new(),
            rekeyed: 0,
        });
        let target = TransitionTarget {
            compression: info.compression,
            plan: Some(plan),
        };
        let now = self.now.duration_since(UNIX_EPOCH).unwrap_or_default();
        let (kind, generation) = (TransitionKind::Rotate, settings.storage_generation);
        let now = now.as_millis() as u64;
        let unit = EncryptionTransition::new(kind, Some(key), target, generation, now);
        self.unit = Some(unit);
        self.step = Step::Claim;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    /// Stores the pass before any rewrite, so a call for another prefix finds it and stops.
    fn claim_read(&mut self, value: Option<Value>) -> Effects {
        let (Some(txn_id), Some(fresh)) = (self.txn, self.progress.take()) else {
            return self.finish(Err(KeyError::Storage));
        };
        let progress = match value.as_deref().map(postcard::from_bytes::<RekeyProgress>) {
            None => fresh,
            Some(Ok(saved)) if saved.prefix != self.prefix => {
                return self.finish(Err(KeyError::Busy));
            }
            Some(Ok(saved)) if saved.epoch != fresh.epoch => fresh,
            Some(Ok(saved)) => saved,
            Some(Err(_)) => return self.finish(Err(AbeError::Context.into())),
        };
        let Ok(row) = postcard::to_allocvec(&progress) else {
            return self.finish(Err(KeyError::Storage));
        };
        self.seen = Some(row.clone().into());
        self.progress = Some(progress);
        self.step = Step::ClaimWrite;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![(ABE_REKEY_KEYSPACE.to_string(), self.id.clone(), row.into())],
            txn_id: Some(txn_id),
        })]
    }

    fn scan(&mut self) -> Effects {
        let (Ok(versions), Some(progress)) = (
            VersionKey::bucket_prefix(&self.bucket),
            self.progress.as_ref(),
        ) else {
            return self.finish(Err(KeyError::Storage));
        };
        let after = (!progress.cursor.is_empty()).then(|| progress.cursor.clone().into());
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
                Ok(RewriteOutcome::Unfinished) => RekeyOutcome::Unfinished,
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
            // The pass stays unfinished while an in-scope version still waits.
            RekeyOutcome::Unfinished => return self.save(),
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
            (Step::Claim, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn = Some(txn_id);
                self.step = Step::ClaimRead;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: ABE_REKEY_KEYSPACE.to_string(),
                    key: self.id.clone(),
                    txn_id: Some(txn_id),
                })]
            }
            (Step::ClaimRead, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.claim_read(value)
            }
            (Step::ClaimWrite, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                match self.txn {
                    Some(txn_id) => {
                        self.step = Step::ClaimCommit;
                        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                    }
                    None => self.finish(Err(KeyError::Storage)),
                }
            }
            (Step::ClaimCommit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn = None;
                self.scan()
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::storage::format::Compression;
    use ulid::Ulid;

    fn row(key: &str) -> (Key, Value) {
        let version = VersionKey::new("bucket", key, Ulid::from_bytes([2; 16]));
        (version.to_bytes().unwrap().into(), Vec::new().into())
    }

    /// A page of `page` versions under foo/ that has read its rows and waits for the scan.
    fn scanning(page: usize) -> RekeyOperation {
        let mut operation = RekeyOperation::new("bucket", "foo/", page, None, UNIX_EPOCH);
        operation.id = vec![1; 16].into();
        operation.progress = Some(RekeyProgress {
            prefix: "foo/".into(),
            epoch: 2,
            cursor: Vec::new(),
            rekeyed: 0,
        });
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: None,
        };
        let unit = EncryptionTransition::new(TransitionKind::Rotate, None, target, 3, 0);
        operation.unit = Some(unit);
        operation.step = Step::Scan;
        operation
    }

    fn scanned(operation: &mut RekeyOperation, keys: &[&str]) -> Effects {
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: keys.iter().map(|key| row(key)).collect(),
            next_start_after: None,
        }))
    }

    fn ended(operation: &mut RekeyOperation, outcome: RekeyOutcome) -> Effects {
        operation.step(Event::SubOperation(SubOperationEvent::VersionRekeyed {
            outcome,
        }))
    }

    /// Answers the save transaction with the progress row `seen`, epoch 2 and no due marker.
    fn saved(operation: &mut RekeyOperation, seen: Option<Value>) -> Effects {
        let txn_id = TxnId::generate();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let values = vec![
            (Key::from(Vec::new()), seen),
            (
                Key::from(Vec::new()),
                Some(2u64.to_be_bytes().to_vec().into()),
            ),
            (Key::from(Vec::new()), None),
        ];
        operation.step(Event::Storage(StorageEvent::BatchReadResult { values }))
    }

    /// The progress a save writes.
    fn written(effects: &Effects) -> RekeyProgress {
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("one progress write: {effects:?}");
        };
        postcard::from_bytes(&writes[0].2).unwrap()
    }

    #[test]
    fn pages_then_saves() {
        // One version per page: the cursor stops after the first re-keyed version.
        let mut operation = scanning(1);
        let effects = scanned(&mut operation, &["foo/a", "bar/x", "foo/b"]);
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let effects = ended(&mut operation, RekeyOutcome::Moved);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
        let progress = written(&saved(&mut operation, None));
        assert_eq!(
            (progress.cursor, progress.rekeyed),
            (row("foo/a").0.to_vec(), 1)
        );
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        let txn_id = TxnId::generate();
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        let (progress, done) = operation.finalize().unwrap();
        assert_eq!((progress.rekeyed, done), (1, false));
    }

    #[test]
    fn locked_version_keeps() {
        // The cursor stays before a version the locked key could not re-key.
        let mut operation = scanning(4);
        scanned(&mut operation, &["foo/a", "foo/b"]);
        ended(&mut operation, RekeyOutcome::Moved);
        ended(&mut operation, RekeyOutcome::Locked);
        let progress = written(&saved(&mut operation, None));
        assert_eq!(progress.cursor, row("foo/a").0.to_vec());
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        let txn_id = TxnId::generate();
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert_eq!(operation.finalize(), Err(KeyError::Locked));
    }

    fn committed(operation: &mut RekeyOperation) {
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        let txn_id = TxnId::generate();
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
    }

    #[test]
    fn unfinished_version_stops() {
        // A pending version keeps the pass open with the cursor before it, on the last page too.
        let mut operation = scanning(4);
        scanned(&mut operation, &["foo/a", "foo/b", "foo/c"]);
        ended(&mut operation, RekeyOutcome::Moved);
        ended(&mut operation, RekeyOutcome::Unfinished);
        let progress = written(&saved(&mut operation, None));
        assert_eq!(
            (progress.cursor, progress.rekeyed),
            (row("foo/a").0.to_vec(), 1)
        );
        committed(&mut operation);
        let (_, done) = operation.finalize().unwrap();
        assert!(!done);
    }

    /// A first page for `prefix` that waits for the progress row read in its claim.
    fn claiming(prefix: &str) -> RekeyOperation {
        let mut operation = scanning(1);
        operation.prefix = prefix.into();
        operation.progress.as_mut().unwrap().prefix = prefix.into();
        operation.txn = Some(TxnId::generate());
        operation.step = Step::ClaimRead;
        operation
    }

    fn claim_read(operation: &mut RekeyOperation, value: Option<Value>) -> Effects {
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value,
        }))
    }

    #[test]
    fn claims_before_rewriting() {
        // Two first pages for different prefixes: the second stops before changing any version.
        let mut first = claiming("foo/");
        let effects = claim_read(&mut first, None);
        let claimed = written(&effects);
        assert_eq!(claimed.prefix, "foo/");
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("one claim write");
        };
        let row = writes[0].2.clone();
        let mut second = claiming("bar/");
        let effects = claim_read(&mut second, Some(row));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(second.finalize(), Err(KeyError::Busy));
        // The first scans only after its claim committed.
        first.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        let txn_id = TxnId::generate();
        let effects = first.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { .. })]
        ));
    }

    #[test]
    fn last_page_finishes() {
        let mut operation = scanning(4);
        scanned(&mut operation, &["bar/x", "foo/a"]);
        ended(&mut operation, RekeyOutcome::Skipped);
        let effects = saved(&mut operation, None);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchDelete { .. })]
        ));
    }
}
