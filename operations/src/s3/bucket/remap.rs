//! Re-maps bucket keys by file name pattern, page by page, with the folder rename's re-keying
//! transaction. Previews and job pages share the selection and the checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::linked::JobStep;
use super::rename::{JOB_CHANGED, RenameError, RenameInput, RenameKeysOperation, check_target};
use crate::auth::request_authorization::BucketAccess;
use crate::driver::{DriverContext, drive};
use crate::jobs::store::iter_prefix_page;
use aruna_core::NodeId;
use aruna_core::keyspaces::BLOB_HEAD_KEYSPACE;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BlobHeadKey, CurrentVersionPointer};
use aruna_core::structs::storage::linked_job::{LinkedJob, RemapPlan};
use aruna_core::types::{GroupId, Key};
use std::collections::{HashMap, HashSet};

/// Entries one preview page shows by default and at most.
pub const DEFAULT_PAGE: usize = 100;
pub const MAX_PAGE: usize = 1000;
/// Bucket keys one preview request passes over at most, for its page and for its totals.
const PAGE_SCAN: usize = 200_000;
const TOTAL_SCAN: usize = 20_000;
/// Keys one job page re-maps at most, and bucket keys it passes over at most.
const JOB_KEYS: usize = 256;
const JOB_SCAN: usize = 20_000;
const HEAD_PAGE: usize = 1024;

/// Where a remap runs and who asks: the caller's access decides what it sees and moves.
pub struct RemapScope<'a> {
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub bucket: &'a str,
    pub access: &'a BucketAccess,
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

/// The keys of a bucket under `prefix` after `after`, one storage page at a time.
async fn head_page(
    context: &DriverContext,
    bucket: &str,
    prefix: &str,
    after: Option<&str>,
) -> Result<(Vec<(String, Key)>, bool), String> {
    let scan = BlobHeadKey::object_prefix(bucket, prefix).map_err(|error| error.to_string())?;
    let start = match after {
        Some(after) => Some(
            BlobHeadKey::new(bucket, after)
                .to_bytes()
                .map_err(|error| error.to_string())?
                .into(),
        ),
        None => None,
    };
    let page = iter_prefix_page(
        &context.storage_handle,
        BLOB_HEAD_KEYSPACE,
        Some(scan.into()),
        start,
        HEAD_PAGE,
        None,
    );
    let (rows, next) = page.await?;
    let mut keys = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        let head = BlobHeadKey::from_bytes(&key).map_err(|error| error.to_string())?;
        keys.push((head.key, value));
    }
    Ok((keys, next.is_none()))
}

/// Counts the selected keys the caller may read and that contain `filter`.
pub async fn count_selected(
    context: &DriverContext,
    plan: &RemapPlan,
    scope: &RemapScope<'_>,
    filter: Option<&str>,
) -> Result<(u64, bool), String> {
    let (mut total, mut scanned, mut after) = (0u64, 0usize, None::<String>);
    loop {
        let (keys, last) = head_page(context, scope.bucket, &plan.from, after.as_deref()).await?;
        for (key, _) in &keys {
            if scanned == TOTAL_SCAN {
                return Ok((total, false));
            }
            scanned += 1;
            total += u64::from(shown(plan, scope, filter, key));
        }
        match (last, keys.last()) {
            (false, Some((key, _))) => after = Some(key.clone()),
            _ => return Ok((total, true)),
        }
    }
}

fn shown(plan: &RemapPlan, scope: &RemapScope<'_>, filter: Option<&str>, key: &str) -> bool {
    plan.selects(key)
        && filter.is_none_or(|filter| key.contains(filter))
        && scope.access.allows(Some(key), &Permission::READ)
}

/// One preview page of the selected keys after `after` the caller may read, with the reason
/// each would stay, and the totals.
pub async fn preview_page(
    context: &DriverContext,
    plan: &RemapPlan,
    scope: &RemapScope<'_>,
    (filter, after, limit): (Option<&str>, Option<String>, usize),
) -> Result<RemapPage, RenameError> {
    plan.check().map_err(RenameError::Invalid)?;
    let read = |error: String| RenameError::Authorization(error);
    let (total, complete) = count_selected(context, plan, scope, filter)
        .await
        .map_err(read)?;
    let (mut keys, mut scanned, mut cursor) = (Vec::new(), 0usize, after.clone());
    let (mut next, mut seen) = (None, None);
    'scan: loop {
        let (page, last) = head_page(context, scope.bucket, &plan.from, cursor.as_deref())
            .await
            .map_err(read)?;
        for (key, _) in &page {
            if keys.len() == limit || scanned == PAGE_SCAN {
                // The cursor is the last key the caller may read, selected or not.
                next = seen.clone();
                if next.is_none() {
                    return Err(RenameError::Conflict(
                        "too many keys in a row are hidden from the caller".to_string(),
                    ));
                }
                break 'scan;
            }
            scanned += 1;
            cursor = Some(key.clone());
            if readable(scope, Some(key)).is_some() {
                seen = Some(key.clone());
            }
            if shown(plan, scope, filter, key) {
                keys.push(key.clone());
            }
        }
        if last || page.is_empty() {
            break;
        }
    }
    let (entries, blocked) = check_keys(context, plan, scope, keys).await?;
    Ok(RemapPage {
        total,
        complete,
        entries,
        next,
        blocked,
    })
}

/// A key a cursor may name: one the caller may read.
fn readable(scope: &RemapScope<'_>, key: Option<&str>) -> Option<String> {
    key.filter(|key| scope.access.allows(Some(key), &Permission::READ))
        .map(str::to_string)
}

/// Pairs each key with its new key and the reason it stays. A reason for the whole bucket,
/// such as a running import, is answered apart and leaves the entries without one.
async fn check_keys(
    context: &DriverContext,
    plan: &RemapPlan,
    scope: &RemapScope<'_>,
    keys: Vec<String>,
) -> Result<(Vec<RemapEntry>, Option<String>), RenameError> {
    let mut targets = HashSet::new();
    let mut entries = Vec::with_capacity(keys.len());
    for from in keys {
        let to = plan.target(&from).unwrap_or_default();
        let write = |key: &str| scope.access.allows(Some(key), &Permission::WRITE);
        let conflict = if let Err(reason) = check_target(&to) {
            Some(reason)
        } else if plan.selects(&to) {
            Some(format!("the new key `{to}` matches the pattern too"))
        } else if !write(&from) || !write(&to) {
            Some("no write access to the key or its new key".to_string())
        } else if !targets.insert(to.clone()) {
            Some(format!("another key gets the new key `{to}` as well"))
        } else {
            None
        };
        entries.push(RemapEntry { from, to, conflict });
    }
    let pairs: Vec<_> = entries
        .iter()
        .filter(|entry| entry.conflict.is_none())
        .map(|entry| (entry.from.clone(), entry.to.clone()))
        .collect();
    if pairs.is_empty() {
        return Ok((entries, None));
    }
    let planned = drive(
        RenameKeysOperation::new(input(scope, pairs, false)),
        context,
    )
    .await;
    let plan = match planned {
        Ok(plan) => plan,
        Err(RenameError::Conflict(reason)) => return Ok((entries, Some(reason))),
        Err(error) => return Err(error),
    };
    let reasons: HashMap<_, _> = plan
        .items
        .into_iter()
        .map(|item| (item.from, item.conflict))
        .collect();
    for entry in entries.iter_mut().filter(|entry| entry.conflict.is_none()) {
        entry.conflict = reasons.get(&entry.from).cloned().flatten();
    }
    Ok((entries, None))
}

fn input(scope: &RemapScope<'_>, pairs: Vec<(String, String)>, apply: bool) -> RenameInput {
    RenameInput {
        realm_id: scope.realm_id,
        node_id: scope.node_id,
        group_id: scope.group_id,
        bucket: scope.bucket.to_string(),
        from: String::new(),
        to: String::new(),
        keys: None,
        pairs,
        apply,
        cutoff_ms: None,
        job: None,
    }
}

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
