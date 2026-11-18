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
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<(Vec<Key>, bool)>, KeyError> {
        let (rows, next) = result.page()?;
        let keys = rows.into_iter().map(|(key, _)| key).collect();
        Ok(Next::Done((keys, next.is_some())))
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum PageStep {
    Hold,
    Scan,
    Run,
    Check,
    Save,
}

/// How a page ended; a stopped page still saved its cursor.
#[derive(Debug, PartialEq)]
pub(crate) struct PageEnd {
    progress: RekeyProgress,
    done: bool,
    stopped: Option<KeyError>,
}

type PageTurn = Result<Next<PageEnd>, KeyError>;

/// Walks one page in the transaction that read the pass row before the scan, so any write to the
/// rows it read makes its save conflict.
#[derive(Debug, PartialEq)]
pub(crate) struct RekeyPage {
    scope: PageScope,
    unit: EncryptionTransition,
    id: Key,
    progress: RekeyProgress,
    /// The progress row this page started from.
    seen: Value,
    step: PageStep,
    rows: Vec<Key>,
    handled: usize,
    next: bool,
    moved: usize,
    stopped: Option<KeyError>,
    done: bool,
}

impl RekeyPage {
    pub(crate) fn new(
        scope: PageScope,
        unit: EncryptionTransition,
        id: Key,
        progress: RekeyProgress,
        seen: Value,
    ) -> Self {
        Self {
            scope,
            unit,
            id,
            progress,
            seen,
            step: PageStep::Hold,
            rows: Vec::new(),
            handled: 0,
            next: false,
            moved: 0,
            stopped: None,
            done: false,
        }
    }

    fn call(&mut self, step: PageStep, call: IoCall) -> PageTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn end(&mut self) -> PageTurn {
        Ok(Next::Done(PageEnd {
            progress: self.progress.clone(),
            done: self.done,
            stopped: self.stopped.take(),
        }))
    }

    fn scan(&mut self) -> PageTurn {
        let prefix =
            VersionKey::bucket_prefix(&self.scope.bucket).map_err(|_| KeyError::Storage)?;
        let cursor = &self.progress.cursor;
        let after = (!cursor.is_empty()).then(|| cursor.clone().into());
        let scan = VersionScan {
            prefix: prefix.into(),
            after,
        };
        self.call(PageStep::Scan, IoCall::child(scan))
    }

    /// Starts the next version under the prefix; others only move the cursor.
    fn run_next(&mut self) -> PageTurn {
        while let Some(row) = self.rows.get(self.handled) {
            if self.moved >= self.scope.page {
                break;
            }
            let version = VersionKey::from_bytes(row).map_err(|_| KeyError::Storage)?;
            if version.key.starts_with(&self.scope.prefix) {
                return self.rewrite(version);
            }
            self.advance();
        }
        self.save()
    }

    /// Reads in the transaction that read the row before the scan, so any write to it conflicts.
    fn save(&mut self) -> PageTurn {
        let reads = vec![
            (ABE_REKEY_KEYSPACE.to_string(), self.id.clone()),
            (ABE_EPOCH_KEYSPACE.to_string(), self.id.clone()),
            (ABE_DUE_KEYSPACE.to_string(), self.id.clone()),
        ];
        self.call(PageStep::Check, IoCall::Read(reads))
    }

    fn advance(&mut self) {
        if let Some(row) = self.rows.get(self.handled) {
            self.progress.cursor = row.to_vec();
        }
        self.handled += 1;
    }

    fn rewrite(&mut self, version: VersionKey) -> PageTurn {
        let unit = self.unit.clone();
        let mut operation = RewriteVersionOperation::new(version, unit, self.scope.now).rekey();
        if let Some((quota, realm, node)) = &self.scope.quota {
            operation = operation.with_quota(quota.clone(), *realm, *node);
        }
        self.call(PageStep::Run, IoCall::child(operation))
    }

    fn version_done(&mut self, result: Result<RewriteOutcome, RewriteError>) -> PageTurn {
        match result {
            Ok(RewriteOutcome::Moved) => {
                self.progress.rekeyed += 1;
                self.moved += 1;
            }
            Ok(RewriteOutcome::Skipped) => {}
            Ok(RewriteOutcome::AwaitingKey) => self.stopped = Some(KeyError::Locked),
            // The pass stays unfinished while an in-scope version still waits.
            Ok(RewriteOutcome::Unfinished) => self.stopped = Some(KeyError::Unfinished),
            Err(error) => {
                tracing::warn!(
                    event = "abe.rekey.failed",
                    bucket = %self.scope.bucket,
                    error = %error
                );
                self.stopped = Some(KeyError::Storage);
            }
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
    fn check_read(&mut self, [row, epoch, due]: [Option<Value>; 3]) -> PageTurn {
        // Another call moved the pass meanwhile; its own save stands.
        if row.as_ref() != Some(&self.seen) {
            return self.end();
        }
        let more = self.stopped.is_some() || self.handled < self.rows.len() || self.next;
        // A removal during the walk is not covered by it, so the subtree is walked again.
        let moved_epoch = epoch_of(epoch.as_ref()) != Some(self.progress.epoch);
        let restart = !more && (moved_epoch || due.is_some());
        if restart {
            self.progress.cursor.clear();
            self.progress.rekeyed = 0;
        }
        self.done = !more && !restart;
        let call = if self.done {
            IoCall::Delete(vec![(ABE_REKEY_KEYSPACE.to_string(), self.id.clone())])
        } else {
            let row = postcard::to_allocvec(&self.progress).map_err(|_| KeyError::Storage)?;
            let write = (ABE_REKEY_KEYSPACE.to_string(), self.id.clone(), row.into());
            IoCall::Write(vec![write])
        };
        self.call(PageStep::Save, call)
    }
}

impl Operation for RekeyPage {
    type Output = PageEnd;
    type Error = KeyError;

    fn first(&mut self) -> PageTurn {
        let read = read_row(ABE_REKEY_KEYSPACE, self.id.clone());
        self.call(PageStep::Hold, read)
    }

    fn next(&mut self, result: IoResult) -> PageTurn {
        match self.step {
            // Another call moved the pass after the claim; its own save stands.
            PageStep::Hold => match result.value()? {
                Some(row) if row == self.seen => self.scan(),
                _ => self.end(),
            },
            PageStep::Scan => {
                (self.rows, self.next) = result.child::<(Vec<Key>, bool), KeyError>()??;
                self.run_next()
            }
            PageStep::Run => {
                let rewritten = result.child::<RewriteOutcome, RewriteError>()?;
                self.version_done(rewritten)
            }
            PageStep::Check => self.check_read(values::<3>(result)?),
            PageStep::Save => {
                result.done()?;
                self.end()
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::storage::format::Compression;
    use ulid::Ulid;

    fn row(key: &str) -> Key {
        let version = VersionKey::new("bucket", key, Ulid::from_bytes([2; 16]));
        version.to_bytes().unwrap().into()
    }

    fn progress(prefix: &str) -> RekeyProgress {
        RekeyProgress {
            prefix: prefix.into(),
            epoch: 2,
            cursor: Vec::new(),
            rekeyed: 0,
        }
    }

    fn unit() -> EncryptionTransition {
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: None,
        };
        EncryptionTransition::new(TransitionKind::Rotate, None, target, 3, 0)
    }

    fn seen_row() -> Value {
        postcard::to_allocvec(&progress("foo/")).unwrap().into()
    }

    /// A page of `page` versions under foo/ that holds its transaction and waits for the scan.
    fn scanning(page: usize) -> RekeyPage {
        let scope = PageScope::new("bucket", "foo/", page, None, UNIX_EPOCH);
        let id = Key::from(vec![1; 16]);
        let mut operation = RekeyPage::new(scope, unit(), id, progress("foo/"), seen_row());
        assert_eq!(operation.transaction(), TransactionMode::Write);
        operation.first().unwrap();
        let turn = operation.next(IoResult::Values(vec![Some(seen_row())]));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        operation
    }

    fn scanned(operation: &mut RekeyPage, keys: &[&str]) -> PageTurn {
        let rows = keys.iter().map(|key| row(key)).collect::<Vec<_>>();
        let answer = Ok::<_, KeyError>((rows, false));
        operation.next(IoResult::Child(ChildOutcome::new(answer)))
    }

    fn ended(operation: &mut RekeyPage, outcome: RewriteOutcome) -> PageTurn {
        let answer = Ok::<_, RewriteError>(outcome);
        operation.next(IoResult::Child(ChildOutcome::new(answer)))
    }

    /// Answers the save reads with the progress row `seen`, epoch 2 and no due marker.
    fn saved(operation: &mut RekeyPage, seen: Option<Value>) -> PageTurn {
        let epoch = Some(2u64.to_be_bytes().to_vec().into());
        operation.next(IoResult::Values(vec![seen, epoch, None]))
    }

    /// The progress a save writes.
    fn written(turn: PageTurn) -> RekeyProgress {
        let Ok(Next::Call(IoCall::Write(writes))) = turn else {
            panic!("one progress write: {turn:?}");
        };
        postcard::from_bytes(&writes[0].2).unwrap()
    }

    fn page_end(operation: &mut RekeyPage) -> PageEnd {
        let Ok(Next::Done(end)) = operation.next(IoResult::Done) else {
            panic!("the saved page ends");
        };
        end
    }

    #[test]
    fn pages_then_saves() {
        // One version per page: the cursor stops after the first re-keyed version.
        let mut operation = scanning(1);
        let turn = scanned(&mut operation, &["foo/a", "bar/x", "foo/b"]);
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let turn = ended(&mut operation, RewriteOutcome::Moved);
        assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
        let progress = written(saved(&mut operation, Some(seen_row())));
        assert_eq!(
            (progress.cursor, progress.rekeyed),
            (row("foo/a").to_vec(), 1)
        );
        let end = page_end(&mut operation);
        assert_eq!(
            (end.progress.rekeyed, end.done, end.stopped),
            (1, false, None)
        );
    }

    #[test]
    fn locked_version_keeps() {
        // The cursor stays before a version the locked key could not re-key.
        let mut operation = scanning(4);
        scanned(&mut operation, &["foo/a", "foo/b"]).unwrap();
        ended(&mut operation, RewriteOutcome::Moved).unwrap();
        ended(&mut operation, RewriteOutcome::AwaitingKey).unwrap();
        let progress = written(saved(&mut operation, Some(seen_row())));
        assert_eq!(progress.cursor, row("foo/a").to_vec());
        assert_eq!(page_end(&mut operation).stopped, Some(KeyError::Locked));
    }

    #[test]
    fn unfinished_version_stops() {
        // A pending version keeps the pass open with the cursor before it, on the last page too.
        let mut operation = scanning(4);
        scanned(&mut operation, &["foo/a", "foo/b", "foo/c"]).unwrap();
        ended(&mut operation, RewriteOutcome::Moved).unwrap();
        ended(&mut operation, RewriteOutcome::Unfinished).unwrap();
        let progress = written(saved(&mut operation, Some(seen_row())));
        assert_eq!(
            (progress.cursor, progress.rekeyed),
            (row("foo/a").to_vec(), 1)
        );
        assert_eq!(page_end(&mut operation).stopped, Some(KeyError::Unfinished));
    }

    #[test]
    fn failed_rewrite_stops() {
        let mut operation = scanning(4);
        scanned(&mut operation, &["foo/a"]).unwrap();
        let failed =
            Err::<RewriteOutcome, _>(RewriteError::from(aruna_core::calls::IoError::Conflict));
        operation
            .next(IoResult::Child(ChildOutcome::new(failed)))
            .unwrap();
        written(saved(&mut operation, Some(seen_row())));
        assert_eq!(page_end(&mut operation).stopped, Some(KeyError::Storage));
    }

    fn claiming(prefix: &str) -> ClaimRekey {
        let mut claim = ClaimRekey {
            id: Key::from(vec![1; 16]),
            prefix: prefix.into(),
            fresh: progress(prefix),
            claimed: None,
        };
        assert_eq!(claim.transaction(), TransactionMode::Write);
        claim.first().unwrap();
        claim
    }

    #[test]
    fn claims_before_rewriting() {
        // Two first pages for different prefixes: the second stops before changing any version.
        let mut first = claiming("foo/");
        let turn = first.next(IoResult::Values(vec![None]));
        let Ok(Next::Call(IoCall::Write(writes))) = turn else {
            panic!("one claim write: {turn:?}");
        };
        let stored = writes[0].2.clone();
        let mut second = claiming("bar/");
        let refused = second.next(IoResult::Values(vec![Some(stored.clone())]));
        assert_eq!(refused, Err(KeyError::Busy));
        // The first page starts only from the claim that committed.
        assert_eq!(
            first.next(IoResult::Done),
            Ok(Next::Done((progress("foo/"), stored)))
        );
    }

    #[test]
    fn claim_restarts_raised() {
        // A pass saved under an older epoch starts over.
        let mut saved = progress("foo/");
        (saved.epoch, saved.rekeyed) = (1, 5);
        let row = postcard::to_allocvec(&saved).unwrap();
        let mut claim = claiming("foo/");
        claim
            .next(IoResult::Values(vec![Some(row.into())]))
            .unwrap();
        let Ok(Next::Done((claimed, _))) = claim.next(IoResult::Done) else {
            panic!("the claim commits");
        };
        assert_eq!(claimed, progress("foo/"));
    }

    #[test]
    fn last_page_finishes() {
        let mut operation = scanning(4);
        scanned(&mut operation, &["bar/x", "foo/a"]).unwrap();
        ended(&mut operation, RewriteOutcome::Skipped).unwrap();
        let turn = saved(&mut operation, Some(seen_row()));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
        assert!(page_end(&mut operation).done);
    }

    #[test]
    fn moved_pass_stands() {
        // Another call saved the pass during this page, so this page writes nothing.
        let mut operation = scanning(4);
        scanned(&mut operation, &[]).unwrap();
        let other = Value::from(vec![9]);
        let turn = saved(&mut operation, Some(other));
        assert!(matches!(turn, Ok(Next::Done(PageEnd { done: false, .. }))));
    }

    #[test]
    fn lost_conflict_resumes() {
        // A page that lost its commit keeps the stored pass open for the next call.
        let mut operation = RekeyOperation::new("bucket", "foo/", 1, None, UNIX_EPOCH);
        operation.step = Step::Page;
        operation.stored = Some(progress("foo/"));
        let lost = Err::<PageEnd, _>(KeyError::Conflict);
        assert_eq!(
            operation.next(IoResult::Child(ChildOutcome::new(lost))),
            Ok(Next::Done((progress("foo/"), false)))
        );
    }

    #[test]
    fn stopped_page_fails() {
        let mut operation = RekeyOperation::new("bucket", "foo/", 1, None, UNIX_EPOCH);
        operation.step = Step::Page;
        let end = Ok::<_, KeyError>(PageEnd {
            progress: progress("foo/"),
            done: false,
            stopped: Some(KeyError::Locked),
        });
        assert_eq!(
            operation.next(IoResult::Child(ChildOutcome::new(end))),
            Err(KeyError::Locked)
        );
    }

    #[test]
    fn missing_config_unlimited() {
        // A realm without a config document charges no quota.
        let realm = RealmId::from_bytes([1; 32]);
        let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let mut operation =
            RekeyOperation::new("bucket", "foo/", 1, Some((realm, node)), UNIX_EPOCH);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let absent = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
        let turn = operation.next(IoResult::Child(ChildOutcome::new(absent)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
        assert_eq!(operation.scope.quota, None);
    }
}
