//! Gives every version under a key prefix a new object key and envelope, one page per call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::abe::KeyError;
use crate::blob::migration::rewrite::{RewriteError, RewriteOutcome, RewriteVersionOperation};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{
    ABE_DUE_KEYSPACE, ABE_EPOCH_KEYSPACE, ABE_REKEY_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{read_row, values};
use aruna_core::structs::identity::realm::{QuotaConfig, RealmConfigDocument, RealmId};
use aruna_core::structs::storage::abe::AbeError;
use aruna_core::structs::storage::blob::{BucketInfo, VersionKey};
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyRecord, SealPlan};
use aruna_core::structs::storage::transition::{
    EncryptionTransition, TransitionKind, TransitionTarget,
};
use aruna_core::types::{Key, Value};
use serde::{Deserialize, Serialize};
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

/// What one page re-keys.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PageScope {
    bucket: String,
    prefix: String,
    page: usize,
    quota: Quota,
    now: SystemTime,
}

impl PageScope {
    pub(crate) fn new(
        bucket: &str,
        prefix: &str,
        page: usize,
        quota: Quota,
        now: SystemTime,
    ) -> Self {
        Self {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            page: page.max(1),
            quota,
            now,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Step {
    Config,
    Settings,
    Rows,
    Claim,
    Page,
}

type RekeyTurn = Result<Next<(RekeyProgress, bool)>, KeyError>;

/// Re-keys up to `page` versions under `prefix` after the saved cursor through the rotation
/// unit, then saves the cursor. Outputs the progress and whether the pass is done.
#[derive(Debug, PartialEq)]
pub struct RekeyOperation {
    scope: PageScope,
    /// Whose realm quota new envelopes are charged against, when the node has a realm.
    origin: Option<(RealmId, NodeId)>,
    step: Step,
    id: Key,
    settings: Option<(BucketEncryption, BucketInfo)>,
    unit: Option<EncryptionTransition>,
    fresh: Option<RekeyProgress>,
    /// The progress the claim stored, which stands when the page loses a conflict.
    stored: Option<RekeyProgress>,
}

impl RekeyOperation {
    pub fn new(
        bucket: &str,
        prefix: &str,
        page: usize,
        origin: Option<(RealmId, NodeId)>,
        now: SystemTime,
    ) -> Self {
        Self {
            scope: PageScope::new(bucket, prefix, page, None, now),
            origin,
            step: Step::Settings,
            id: Key::from(Vec::new()),
            settings: None,
            unit: None,
            fresh: None,
            stored: None,
        }
    }

    fn call(&mut self, step: Step, call: IoCall) -> RekeyTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn read_settings(&mut self) -> RekeyTurn {
        let name: Key = self.scope.bucket.as_bytes().to_vec().into();
        let reads = vec![
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), name.clone()),
            (S3_BUCKET_KEYSPACE.to_string(), name),
        ];
        self.call(Step::Settings, IoCall::Read(reads))
    }

    /// A realm without a config document has no quota.
    fn config_read(&mut self, config: Result<RealmConfigDocument, GetConfigError>) -> RekeyTurn {
        self.scope.quota = match (config, self.origin) {
            (Ok(config), Some((realm, node))) => Some((config.quota, realm, node)),
            (Err(GetConfigError::DocumentNotFound), _) => None,
            _ => return Err(KeyError::Storage),
        };
        self.read_settings()
    }

    fn settings_read(&mut self, [settings, info]: [Option<Value>; 2]) -> RekeyTurn {
        let settings = BucketEncryption::from_row(settings.as_deref());
        let info = info.as_deref().map(BucketInfo::from_bytes);
        let (Ok(settings), Some(Ok(info))) = (settings, info) else {
            return Err(KeyError::Missing);
        };
        let key = settings.active_key().ok_or(KeyError::Missing)?;
        self.id = key.bucket_id.to_bytes().to_vec().into();
        self.settings = Some((settings, info));
        let reads = vec![
            (BUCKET_KEY_KEYSPACE.to_string(), key.key().into()),
            (ABE_EPOCH_KEYSPACE.to_string(), self.id.clone()),
        ];
        self.call(Step::Rows, IoCall::Read(reads))
    }

    fn rows_read(&mut self, [record, epoch]: [Option<Value>; 2]) -> RekeyTurn {
        let (settings, info) = self.settings.take().ok_or(KeyError::Storage)?;
        let record = record.as_deref().map(BucketKeyRecord::from_bytes);
        let plan = match record {
            Some(Ok(record)) => SealPlan::capture(&settings, &record).ok().flatten(),
            _ => None,
        };
        let (Some(key), Some(plan), Some(epoch)) =
            (settings.active_key(), plan, epoch_of(epoch.as_ref()))
        else {
            return Err(KeyError::Missing);
        };
        let fresh = RekeyProgress {
            prefix: self.scope.prefix.clone(),
            epoch,
            cursor: Vec::new(),
            rekeyed: 0,
        };
        let target = TransitionTarget {
            compression: info.compression,
            plan: Some(plan),
        };
        let now = self
            .scope
            .now
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let (kind, generation) = (TransitionKind::Rotate, settings.storage_generation);
        let now = now.as_millis() as u64;
        self.unit = Some(EncryptionTransition::new(
            kind,
            Some(key),
            target,
            generation,
            now,
        ));
        self.fresh = Some(fresh.clone());
        let claim = ClaimRekey {
            id: self.id.clone(),
            prefix: self.scope.prefix.clone(),
            fresh,
            claimed: None,
        };
        self.call(Step::Claim, IoCall::child(claim))
    }

    fn claimed(&mut self, claim: Result<(RekeyProgress, Value), KeyError>) -> RekeyTurn {
        let (progress, seen) = claim.map_err(|error| match error {
            KeyError::Conflict => KeyError::Storage,
            other => other,
        })?;
        let unit = self.unit.take().ok_or(KeyError::Storage)?;
        self.stored = Some(progress.clone());
        let scope = self.scope.clone();
        let page = RekeyPage::new(scope, unit, self.id.clone(), progress, seen);
        self.call(Step::Page, IoCall::child(page))
    }

    /// Ends with the error that stopped the page, else with the progress.
    fn page_ended(&mut self, ended: Result<PageEnd, KeyError>) -> RekeyTurn {
        match ended {
            Ok(PageEnd {
                stopped: Some(error),
                ..
            }) => Err(error),
            Ok(PageEnd { progress, done, .. }) => Ok(Next::Done((progress, done))),
            // A copy, raise or removal wrote a read row during the page; the next call resumes.
            Err(KeyError::Conflict) => {
                let progress = self.stored.take().ok_or(KeyError::Storage)?;
                Ok(Next::Done((progress, false)))
            }
            Err(error) => Err(error),
        }
    }
}

impl Operation for RekeyOperation {
    type Output = (RekeyProgress, bool);
    type Error = KeyError;

    fn first(&mut self) -> RekeyTurn {
        match self.origin {
            Some((realm, _)) => {
                self.call(Step::Config, IoCall::child(GetConfigOperation::new(realm)))
            }
            None => self.read_settings(),
        }
    }

    fn next(&mut self, result: IoResult) -> RekeyTurn {
        match self.step {
            Step::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                self.config_read(config)
            }
            Step::Settings => self.settings_read(values::<2>(result)?),
            Step::Rows => self.rows_read(values::<2>(result)?),
            Step::Claim => self.claimed(result.child::<(RekeyProgress, Value), KeyError>()?),
            Step::Page => self.page_ended(result.child::<PageEnd, KeyError>()?),
        }
    }
}

/// Stores the pass before any rewrite, so a call for another prefix finds it and stops. Outputs
/// the pass and its stored row.
#[derive(Debug, PartialEq)]
struct ClaimRekey {
    id: Key,
    prefix: String,
    fresh: RekeyProgress,
    claimed: Option<(RekeyProgress, Value)>,
}

type ClaimTurn = Result<Next<(RekeyProgress, Value)>, KeyError>;

impl ClaimRekey {
    fn claim(&mut self, value: Option<Value>) -> ClaimTurn {
        let saved = value.as_deref().map(postcard::from_bytes::<RekeyProgress>);
        let progress = match saved {
            None => self.fresh.clone(),
            Some(Ok(saved)) if saved.prefix != self.prefix => return Err(KeyError::Busy),
            Some(Ok(saved)) if saved.epoch != self.fresh.epoch => self.fresh.clone(),
            Some(Ok(saved)) => saved,
            Some(Err(_)) => return Err(AbeError::Context.into()),
        };
        let row: Value = postcard::to_allocvec(&progress)
            .map_err(|_| KeyError::Storage)?
            .into();
        self.claimed = Some((progress, row.clone()));
        let write = (ABE_REKEY_KEYSPACE.to_string(), self.id.clone(), row);
        Ok(Next::Call(IoCall::Write(vec![write])))
    }
}

impl Operation for ClaimRekey {
    type Output = (RekeyProgress, Value);
    type Error = KeyError;

    fn first(&mut self) -> ClaimTurn {
        let read = read_row(ABE_REKEY_KEYSPACE, self.id.clone());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> ClaimTurn {
        match self.claimed.take() {
            Some(claimed) => {
                result.done()?;
                Ok(Next::Done(claimed))
            }
            None => self.claim(result.value()?),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// One scan of the bucket's version rows. It stays outside the page's transaction, whose
/// conflicts must come only from the pass rows it read.
#[derive(Debug, PartialEq)]
struct VersionScan {
    prefix: Key,
    after: Option<Key>,
}

impl Operation for VersionScan {
    type Output = (Vec<Key>, bool);
    type Error = KeyError;

    fn first(&mut self) -> Result<Next<(Vec<Key>, bool)>, KeyError> {
        Ok(Next::Call(IoCall::Page {
            space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: self.prefix.clone(),
            after: self.after.clone(),
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

    /// Saves in the transaction that read the row before the scan, so any write to it conflicts.
    fn save(&mut self) -> Effects {
        let Some(txn_id) = self.txn else {
            return self.finish(Err(KeyError::Storage));
        };
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
            RekeyOutcome::Unfinished => self.stopped = Some(KeyError::Unfinished),
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

impl EffectOperation for RekeyOperation {
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
                self.step = Step::Start;
                smallvec![Effect::Storage(StorageEffect::StartTransaction {
                    read: false
                })]
            }
            (Step::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn = Some(txn_id);
                self.step = Step::Hold;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: ABE_REKEY_KEYSPACE.to_string(),
                    key: self.id.clone(),
                    txn_id: Some(txn_id),
                })]
            }
            // Another call moved the pass after the claim; its own save stands.
            (Step::Hold, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                if value != self.seen {
                    return self.end();
                }
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
            // A copy, raise or removal wrote a read row during the page; the next call resumes.
            (
                Step::Commit,
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }),
            ) => {
                self.txn = None;
                self.done = false;
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
        operation.txn = Some(TxnId::generate());
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

    /// Answers the save reads with the progress row `seen`, epoch 2 and no due marker.
    fn saved(operation: &mut RekeyOperation, seen: Option<Value>) -> Effects {
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
            [Effect::Storage(StorageEffect::BatchRead {
                txn_id: Some(_),
                ..
            })]
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
        assert_eq!(operation.finalize(), Err(KeyError::Unfinished));
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
        first.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        let txn_id = TxnId::generate();
        first.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let seen = first.seen.clone();
        let effects = claim_read(&mut first, seen);
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
