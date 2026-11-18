//! Re-maps bucket keys by file name pattern, page by page, with the folder rename's re-keying
//! transaction. Previews and job pages share the selection and the checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::job_progress::JobStep;
use super::rename::{
    JOB_CHANGED, RenameError, RenameInput, RenameKeysOperation, RenamePlan, check_target,
};
use crate::auth::request_authorization::BucketAccess;
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::BLOB_HEAD_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer};
use aruna_core::structs::storage::linked_job::{LinkedJob, RemapPlan};
use aruna_core::types::{GroupId, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Entries one preview page shows by default and at most.
pub const DEFAULT_PAGE: usize = 100;
pub const MAX_PAGE: usize = 1000;
/// Bucket keys one preview request passes over at most, for its page and for its totals.
const PAGE_SCAN: usize = 10_000;
const TOTAL_SCAN: usize = 10_000;
/// Keys one job page re-maps at most, and bucket keys it passes over at most.
const JOB_KEYS: usize = 256;
const JOB_SCAN: usize = 20_000;
const HEAD_PAGE: usize = 1024;

/// Where a remap runs and who asks: the caller's access decides what it sees and moves.
#[derive(Clone, Debug, PartialEq)]
pub struct RemapScope {
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub bucket: String,
    pub access: Arc<BucketAccess>,
}

/// One selected key, its new key and why it stays, if it does.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RemapEntry {
    pub from: String,
    pub to: String,
    pub conflict: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RemapPage {
    /// Selected keys the caller may read; a lower bound while `complete` is false.
    pub total: u64,
    pub complete: bool,
    pub entries: Vec<RemapEntry>,
    /// The key after which the next page starts; a key the caller may read.
    pub next: Option<String>,
    /// Why no key of the bucket can be re-mapped now, such as a running import.
    pub blocked: Option<String>,
}

/// The call for the keys of a bucket under `prefix` after `after`.
fn head_call(bucket: &str, prefix: &str, after: Option<&str>) -> Result<IoCall, ConversionError> {
    let after = after.map(|after| BlobHeadKey::new(bucket, after).to_bytes());
    Ok(IoCall::Page {
        space: BLOB_HEAD_KEYSPACE.to_string(),
        prefix: BlobHeadKey::object_prefix(bucket, prefix)?.into(),
        after: after.transpose()?.map(Into::into),
        limit: HEAD_PAGE,
    })
}

/// The keys and head rows of a page, and whether it was the last one.
fn head_rows(result: IoResult) -> Result<(Vec<(String, Value)>, bool), RenameError> {
    let (rows, next) = result.page()?;
    let mut keys = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        keys.push((BlobHeadKey::from_bytes(&key)?.key, value));
    }
    Ok((keys, next.is_none()))
}

fn shown(plan: &RemapPlan, scope: &RemapScope, filter: Option<&str>, key: &str) -> bool {
    plan.selects(key)
        && filter.is_none_or(|filter| key.contains(filter))
        && scope.access.allows(Some(key), &Permission::READ)
}

/// A key a cursor may name: one the caller may read.
fn readable(scope: &RemapScope, key: &str) -> bool {
    scope.access.allows(Some(key), &Permission::READ)
}

type CountTurn = Result<Next<(u64, bool)>, RenameError>;

/// Counts the selected keys the caller may read and that contain `filter`. Outputs the count and
/// whether it covers every key.
#[derive(Debug, PartialEq)]
pub struct CountSelectedOperation {
    plan: RemapPlan,
    scope: RemapScope,
    filter: Option<String>,
    total: u64,
    scanned: usize,
    after: Option<String>,
}

impl CountSelectedOperation {
    pub fn new(plan: &RemapPlan, scope: &RemapScope, filter: Option<&str>) -> Self {
        Self {
            plan: plan.clone(),
            scope: scope.clone(),
            filter: filter.map(str::to_string),
            total: 0,
            scanned: 0,
            after: None,
        }
    }

    fn page(&self) -> CountTurn {
        let call = head_call(&self.scope.bucket, &self.plan.from, self.after.as_deref())?;
        Ok(Next::Call(call))
    }
}

impl Operation for CountSelectedOperation {
    type Output = (u64, bool);
    type Error = RenameError;

    fn first(&mut self) -> CountTurn {
        self.page()
    }

    fn next(&mut self, result: IoResult) -> CountTurn {
        let (keys, last) = head_rows(result)?;
        for (key, _) in &keys {
            if self.scanned == TOTAL_SCAN {
                return Ok(Next::Done((self.total, false)));
            }
            self.scanned += 1;
            let filter = self.filter.as_deref();
            self.total += u64::from(shown(&self.plan, &self.scope, filter, key));
        }
        match (last, keys.last()) {
            (false, Some((key, _))) => {
                self.after = Some(key.clone());
                self.page()
            }
            _ => Ok(Next::Done((self.total, true))),
        }
    }
}

/// The entries of the checked keys, and a reason that holds for the whole bucket.
type CheckedKeys = (Vec<RemapEntry>, Option<String>);

/// Pairs each key with its new key and the reason it stays. A reason for the whole bucket, such
/// as a running import, is answered apart and leaves the entries without one.
#[derive(Debug, PartialEq)]
struct CheckKeys {
    plan: RemapPlan,
    scope: RemapScope,
    keys: Vec<String>,
    entries: Vec<RemapEntry>,
}

impl CheckKeys {
    fn new(plan: &RemapPlan, scope: &RemapScope, keys: Vec<String>) -> Self {
        Self {
            plan: plan.clone(),
            scope: scope.clone(),
            keys,
            entries: Vec::new(),
        }
    }

    fn entry(&self, from: String, targets: &mut HashSet<String>) -> RemapEntry {
        let to = self.plan.target(&from).unwrap_or_default();
        let write = |key: &str| self.scope.access.allows(Some(key), &Permission::WRITE);
        let conflict = if let Err(reason) = check_target(&to) {
            Some(reason)
        } else if self.plan.selects(&to) {
            Some(format!("the new key `{to}` matches the pattern too"))
        } else if !write(&from) || !write(&to) {
            Some("no write access to the key or its new key".to_string())
        } else if !targets.insert(to.clone()) {
            Some(format!("another key gets the new key `{to}` as well"))
        } else {
            None
        };
        RemapEntry { from, to, conflict }
    }
}

/// The keys of the entries nothing keeps from moving.
fn free_pairs(entries: &[RemapEntry]) -> Vec<(String, String)> {
    (entries.iter())
        .filter(|entry| entry.conflict.is_none())
        .map(|entry| (entry.from.clone(), entry.to.clone()))
        .collect()
}

impl Operation for CheckKeys {
    type Output = CheckedKeys;
    type Error = RenameError;

    fn first(&mut self) -> Result<Next<CheckedKeys>, RenameError> {
        let mut targets = HashSet::new();
        let keys = std::mem::take(&mut self.keys);
        let entries = (keys.into_iter())
            .map(|from| self.entry(from, &mut targets))
            .collect();
        self.entries = entries;
        let pairs = free_pairs(&self.entries);
        if pairs.is_empty() {
            return Ok(Next::Done((std::mem::take(&mut self.entries), None)));
        }
        let preview = RenameKeysOperation::new(input(&self.scope, pairs, false));
        Ok(Next::Call(IoCall::child(preview)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<CheckedKeys>, RenameError> {
        let mut entries = std::mem::take(&mut self.entries);
        let plan = match result.child::<RenamePlan, RenameError>()? {
            Ok(plan) => plan,
            Err(RenameError::Conflict(reason)) => return Ok(Next::Done((entries, Some(reason)))),
            Err(error) => return Err(error),
        };
        let reasons: HashMap<_, _> = (plan.items.into_iter())
            .map(|item| (item.from, item.conflict))
            .collect();
        for entry in entries.iter_mut().filter(|entry| entry.conflict.is_none()) {
            entry.conflict = reasons.get(&entry.from).cloned().flatten();
        }
        Ok(Next::Done((entries, None)))
    }
}

fn input(scope: &RemapScope, pairs: Vec<(String, String)>, apply: bool) -> RenameInput {
    RenameInput {
        realm_id: scope.realm_id,
        node_id: scope.node_id,
        group_id: scope.group_id,
        bucket: scope.bucket.clone(),
        from: String::new(),
        to: String::new(),
        keys: None,
        pairs,
        apply,
        cutoff_ms: None,
        job: None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum PreviewStep {
    Count,
    Scan,
    Check,
}

type PreviewTurn = Result<Next<RemapPage>, RenameError>;

/// One preview page of the selected keys after `after` the caller may read, with the reason
/// each would stay, and the totals.
#[derive(Debug, PartialEq)]
pub struct RemapPreviewOperation {
    plan: RemapPlan,
    scope: RemapScope,
    filter: Option<String>,
    limit: usize,
    step: PreviewStep,
    cursor: Option<String>,
    page: RemapPage,
    keys: Vec<String>,
    scanned: usize,
    seen: Option<String>,
}

impl RemapPreviewOperation {
    pub fn new(
        plan: &RemapPlan,
        scope: &RemapScope,
        (filter, after, limit): (Option<&str>, Option<String>, usize),
    ) -> Self {
        Self {
            plan: plan.clone(),
            scope: scope.clone(),
            filter: filter.map(str::to_string),
            limit,
            step: PreviewStep::Count,
            cursor: after,
            page: RemapPage::default(),
            keys: Vec::new(),
            scanned: 0,
            seen: None,
        }
    }

    fn call(&mut self, step: PreviewStep, call: IoCall) -> PreviewTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn scan(&mut self) -> PreviewTurn {
        let call = head_call(&self.scope.bucket, &self.plan.from, self.cursor.as_deref())?;
        self.call(PreviewStep::Scan, call)
    }

    fn check(&mut self) -> PreviewTurn {
        let keys = std::mem::take(&mut self.keys);
        let check = CheckKeys::new(&self.plan, &self.scope, keys);
        self.call(PreviewStep::Check, IoCall::child(check))
    }

    fn scanned(&mut self, result: IoResult) -> PreviewTurn {
        let (keys, last) = head_rows(result)?;
        let empty = keys.is_empty();
        for (key, _) in keys {
            if self.keys.len() == self.limit || self.scanned == PAGE_SCAN {
                // The cursor is the last key the caller may read, selected or not.
                self.page.next = self.seen.clone();
                if self.page.next.is_none() {
                    return Err(RenameError::Conflict(
                        "too many keys in a row are hidden from the caller".to_string(),
                    ));
                }
                return self.check();
            }
            self.scanned += 1;
            self.cursor = Some(key.clone());
            if readable(&self.scope, &key) {
                self.seen = Some(key.clone());
            }
            if shown(&self.plan, &self.scope, self.filter.as_deref(), &key) {
                self.keys.push(key);
            }
        }
        match last || empty {
            true => self.check(),
            false => self.scan(),
        }
    }
}

impl Operation for RemapPreviewOperation {
    type Output = RemapPage;
    type Error = RenameError;

    fn first(&mut self) -> PreviewTurn {
        self.plan.check().map_err(RenameError::Invalid)?;
        let count = CountSelectedOperation::new(&self.plan, &self.scope, self.filter.as_deref());
        self.call(PreviewStep::Count, IoCall::child(count))
    }

    fn next(&mut self, result: IoResult) -> PreviewTurn {
        match self.step {
            PreviewStep::Count => {
                (self.page.total, self.page.complete) =
                    result.child::<(u64, bool), RenameError>()??;
                self.scan()
            }
            PreviewStep::Scan => self.scanned(result),
            PreviewStep::Check => {
                (self.page.entries, self.page.blocked) =
                    result.child::<CheckedKeys, RenameError>()??;
                Ok(Next::Done(std::mem::take(&mut self.page)))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum JobPageStep {
    Scan,
    Check,
    Apply,
}

type JobTurn = Result<Next<JobStep>, RenameError>;

/// Re-maps the next keys after the job's cursor; keys changed after the start or with a reason
/// stay, and a reason for the whole bucket makes the page wait. The keys move with the job's
/// progress, and only while the job is stored as `job`, so a paused or aborted job stops.
pub async fn job_page(
    context: &DriverContext,
    plan: &RemapPlan,
    scope: &RemapScope<'_>,
    job: &LinkedJob,
) -> Result<JobStep, RenameError> {
    let read = |error: String| RenameError::Authorization(error);
    let (cursor, started_ms) = (job.cursor.clone(), job.started_at_ms);
    let mut step = JobStep {
        cursor: cursor.clone(),
        ..JobStep::default()
    };
    let (mut keys, mut scanned) = (Vec::new(), 0usize);
    'scan: loop {
        let (page, last) = head_page(context, scope.bucket, &plan.from, step.cursor.as_deref())
            .await
            .map_err(read)?;
        for (key, head) in &page {
            if keys.len() == JOB_KEYS || scanned == JOB_SCAN {
                break 'scan;
            }
            scanned += 1;
            step.cursor = Some(key.clone());
            if !plan.selects(key) {
                continue;
            }
            let pointer = CurrentVersionPointer::from_bytes(head)?;
            match pointer.version_id.timestamp_ms() <= started_ms {
                true => keys.push(key.clone()),
                false => step.skipped += 1,
            }
        }
        if last || page.is_empty() {
            step.finished = true;
            break;
        }
    }
    let (entries, blocked) = check_keys(context, plan, scope, keys).await?;
    let unchanged = |waiting| JobStep {
        cursor: cursor.clone(),
        waiting,
        ..JobStep::default()
    };
    if let Some(reason) = blocked {
        return Ok(unchanged(Some(reason)));
    }
    let pairs: Vec<_> = entries
        .iter()
        .filter(|entry| entry.conflict.is_none())
        .map(|entry| (entry.from.clone(), entry.to.clone()))
        .collect();
    step.skipped += (entries.len() - pairs.len()) as u64;
    if pairs.is_empty() {
        return Ok(step);
    }
    let mut next = job.clone();
    next.cursor = step.cursor.clone();
    (next.done, next.skipped) = (job.done + pairs.len() as u64, job.skipped + step.skipped);
    next.waiting = None;
    let fence = (job.to_bytes()?.into(), next.to_bytes()?.into());
    let mut apply = input(scope, pairs, true);
    (apply.cutoff_ms, apply.job) = (Some(started_ms), Some(fence));
    match drive(RenameKeysOperation::new(apply), context).await {
        // The progress is stored with the keys.
        Ok(_) => Ok(JobStep {
            done: 0,
            skipped: 0,
            ..step
        }),
        Err(RenameError::Conflict(reason)) if reason == JOB_CHANGED => Ok(unchanged(None)),
        // Something changed between the check and the move; the page runs again.
        Err(RenameError::Conflict(reason)) => Ok(unchanged(Some(reason))),
        Err(RenameError::Storage(aruna_core::errors::StorageError::TransactionConflict)) => {
            Ok(unchanged(None))
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
#[path = "remap_tests.rs"]
mod tests;
