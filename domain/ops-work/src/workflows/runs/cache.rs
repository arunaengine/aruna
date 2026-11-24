//! The task cache of workflow runs: the scope a task may use, the lookup that checks the caller's
//! READ and the stored versions of every cached output, and the store of a successful result.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::WORKFLOW_CACHE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow_run::{
    CacheScope, RunFile, WorkflowCacheRecord, WorkflowRunRecord, cache_row,
};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_ops_data::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use aruna_workflow_contract::host::StartTask;
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use super::creator_auth;
use super::data::{ObjectAccessOperation, STAGING_SCHEME};
use crate::workflows::CatalogError;

/// Whether a task's result depends only on what its key names: no network, no secrets, an image
/// pinned to a digest and every direct input by content digest and exact version.
pub fn deterministic(request: &StartTask) -> bool {
    let pinned_image = (request.image.as_deref()).is_some_and(|image| image.contains("@sha256:"));
    let pinned_inputs = request.direct_inputs.iter().all(|input| {
        !input.digest.trim().is_empty()
            && (input.address.starts_with(STAGING_SCHEME) || input.address.contains("?version="))
    });
    !request.network_allowed
        && request.secret_placeholders.is_empty()
        && pinned_image
        && pinned_inputs
}

/// The scope a task uses: the run's own, narrowed to the run when the task is not deterministic.
pub fn task_scope(run: &WorkflowRunRecord, request: &StartTask) -> CacheScope {
    match deterministic(request) {
        true => run.cache_scope,
        false => CacheScope::Run,
    }
}

/// The namespaces a scope reads in order and writes to; the group comes before the realm, so a
/// hit of the own group is preferred.
pub fn namespaces(run: &WorkflowRunRecord, scope: CacheScope) -> Vec<String> {
    let group = format!("group:{}", run.group_id);
    match scope {
        CacheScope::Run => vec![format!("run:{}", run.run_id)],
        CacheScope::Group => vec![group],
        CacheScope::Realm => vec![group, "realm".to_owned()],
    }
}

/// A cached result the caller may reuse. `foreign` marks a result of another group, whose
/// outputs must be copied so that nothing of the producer shows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheHit {
    pub record: WorkflowCacheRecord,
    pub foreign: bool,
}

#[derive(Debug, PartialEq)]
enum LookupStep {
    Row,
    Access(RunFile),
    Head(RunFile),
}

/// Finds a reusable result of a task. Every output of a candidate must still exist in its stored
/// version and be readable by the run's creator; otherwise the candidate is a miss.
#[derive(Debug, PartialEq)]
pub struct LookupCacheOperation {
    run: WorkflowRunRecord,
    task_key: String,
    pending: Vec<String>,
    candidate: Option<WorkflowCacheRecord>,
    checks: Vec<RunFile>,
    step: LookupStep,
}

type LookupNext = Result<Next<Option<CacheHit>>, CatalogError>;

impl LookupCacheOperation {
    pub fn new(run: &WorkflowRunRecord, scope: CacheScope, task_key: &str) -> Self {
        let mut pending = namespaces(run, scope);
        pending.reverse();
        Self {
            run: run.clone(),
            task_key: task_key.to_owned(),
            pending,
            candidate: None,
            checks: Vec::new(),
            step: LookupStep::Row,
        }
    }

    fn next_namespace(&mut self) -> LookupNext {
        self.candidate = None;
        let Some(namespace) = self.pending.pop() else {
            return Ok(Next::Done(None));
        };
        self.step = LookupStep::Row;
        let row = cache_row(&namespace, &self.task_key);
        Ok(Next::Call(IoCall::Read(vec![(
            WORKFLOW_CACHE_KEYSPACE.to_string(),
            row,
        )])))
    }

    fn next_check(&mut self) -> LookupNext {
        let Some(file) = self.checks.pop() else {
            let Some(record) = self.candidate.take() else {
                return self.next_namespace();
            };
            let foreign = record.producer_group_id != self.run.group_id;
            return Ok(Next::Done(Some(CacheHit { record, foreign })));
        };
        let (Some(bucket), Some(key)) = (file.bucket.clone(), file.key.clone()) else {
            return self.next_namespace();
        };
        let access = ObjectAccessOperation::new(
            &creator_auth(&self.run),
            self.run.node_id,
            (&bucket, &key),
            Permission::READ,
        );
        self.step = LookupStep::Access(file);
        Ok(Next::Call(IoCall::child(access)))
    }
}

impl Operation for LookupCacheOperation {
    type Output = Option<CacheHit>;
    type Error = CatalogError;

    fn first(&mut self) -> LookupNext {
        self.next_namespace()
    }

    fn next(&mut self, result: IoResult) -> LookupNext {
        match std::mem::replace(&mut self.step, LookupStep::Row) {
            LookupStep::Row => {
                let Some(value) = result.value()? else {
                    return self.next_namespace();
                };
                let record = match WorkflowCacheRecord::from_bytes(&value) {
                    Ok(record) => record,
                    Err(error) => {
                        warn!(%error, run = %self.run.run_id, "an unreadable cache row is skipped");
                        return self.next_namespace();
                    }
                };
                let non_run = !record.scope_namespace.starts_with("run:");
                if record.exit_code != 0
                    || record.task_key != self.task_key
                    || (non_run && (record.has_secrets || !record.deterministic))
                {
                    return self.next_namespace();
                }
                self.checks = record.outputs.iter().rev().cloned().collect();
                self.candidate = Some(record);
                self.next_check()
            }
            // A refused or failed check is a miss: the task then runs as if nothing was cached.
            LookupStep::Access(file) => match result.child::<BucketInfo, CatalogError>()? {
                Ok(_) => {
                    let version = (file.version_id.as_deref()).and_then(|v| v.parse().ok());
                    let head = HeadObjectOperation::new(HeadObjectInput {
                        bucket: file.bucket.clone().unwrap_or_default(),
                        key: file.key.clone().unwrap_or_default(),
                        version_id: version,
                    });
                    self.step = LookupStep::Head(file);
                    Ok(Next::Call(IoCall::child(head)))
                }
                Err(_) => self.next_namespace(),
            },
            LookupStep::Head(file) => match result.child::<HeadObjectResult, HeadObjectError>()? {
                Ok(head) if same_version(&file, &head) => self.next_check(),
                _ => self.next_namespace(),
            },
        }
    }
}

/// Whether a head names the version a cached output records.
fn same_version(file: &RunFile, head: &HeadObjectResult) -> bool {
    let found = head.resolved_version_id.or(head.version_id);
    match (file.version_id.as_deref(), found) {
        (Some(recorded), Some(found)) => recorded == found.to_string(),
        _ => false,
    }
}

#[derive(Debug, PartialEq)]
enum StoreStep {
    Head(RunFile),
    Write,
}

/// Pins the outputs of a task that ended with exit code 0 to their written versions, and stores
/// the result under each namespace once every output is a pinned file. Answers the pinned files.
#[derive(Debug, PartialEq)]
pub struct StoreCacheOperation {
    namespaces: Vec<String>,
    record: WorkflowCacheRecord,
    pending: Vec<RunFile>,
    pinned: Vec<RunFile>,
    complete: bool,
    now_ms: u64,
    step: StoreStep,
}

/// The facts of a successful attempt that a cache row keeps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    pub task_key: String,
    pub outputs: Vec<RunFile>,
    pub stdout_tail: Vec<u8>,
    pub stderr_tail: Vec<u8>,
    pub deterministic: bool,
    pub has_secrets: bool,
}

impl StoreCacheOperation {
    pub fn new(run: &WorkflowRunRecord, scope: CacheScope, entry: CacheEntry) -> Self {
        let effective_scope = match entry.deterministic && !entry.has_secrets {
            true => scope,
            false => CacheScope::Run,
        };
        let record = WorkflowCacheRecord {
            task_key: entry.task_key,
            scope_namespace: String::new(),
            exit_code: 0,
            outputs: Vec::new(),
            stdout_tail: entry.stdout_tail,
            stderr_tail: entry.stderr_tail,
            created_at_ms: 0,
            producer_group_id: run.group_id,
            producer_run_id: run.run_id,
            deterministic: entry.deterministic,
            has_secrets: entry.has_secrets,
        };
        Self {
            namespaces: namespaces(run, effective_scope),
            record,
            pending: entry.outputs.into_iter().rev().collect(),
            pinned: Vec::new(),
            complete: true,
            now_ms: 0,
            step: StoreStep::Write,
        }
    }

    fn next_head(&mut self) -> Result<Next<Vec<RunFile>>, CatalogError> {
        let Some(file) = self.pending.pop() else {
            return self.write();
        };
        // An output the child's records name by its exact version, possibly on another node.
        if file.version_id.is_some() {
            self.pinned.push(file);
            return self.next_head();
        }
        let (Some(bucket), Some(key)) = (file.bucket.clone(), file.key.clone()) else {
            self.complete = false;
            self.pinned.push(file);
            return self.next_head();
        };
        let head = HeadObjectOperation::new(HeadObjectInput {
            bucket,
            key,
            version_id: None,
        });
        self.step = StoreStep::Head(file);
        Ok(Next::Call(IoCall::child(head)))
    }

    fn write(&mut self) -> Result<Next<Vec<RunFile>>, CatalogError> {
        if !self.complete {
            return Ok(Next::Done(std::mem::take(&mut self.pinned)));
        }
        let mut writes = Vec::new();
        for namespace in &self.namespaces {
            let mut record = self.record.clone();
            record.scope_namespace = namespace.clone();
            record.outputs = self.pinned.clone();
            record.created_at_ms = self.now_ms;
            writes.push((
                WORKFLOW_CACHE_KEYSPACE.to_string(),
                cache_row(namespace, &record.task_key),
                ByteView::from(record.to_bytes()?),
            ));
        }
        self.step = StoreStep::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for StoreCacheOperation {
    type Output = Vec<RunFile>;
    type Error = CatalogError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Vec<RunFile>>, CatalogError> {
        self.next_head()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<RunFile>>, CatalogError> {
        match std::mem::replace(&mut self.step, StoreStep::Write) {
            StoreStep::Head(mut file) => {
                match result.child::<HeadObjectResult, HeadObjectError>()? {
                    Ok(head) => {
                        let version: Option<Ulid> = head.resolved_version_id.or(head.version_id);
                        file.version_id = version.map(|version| version.to_string());
                        if let Some(location) = &head.location {
                            file.size = Some(location.blob_size);
                            file.digest = location.get_blake3().map(hex::encode);
                        }
                        self.complete &= file.version_id.is_some();
                    }
                    // A folder or a missing file has no single version, so the result is not kept.
                    Err(_) => self.complete = false,
                }
                self.pinned.push(file);
                self.next_head()
            }
            StoreStep::Write => {
                result.done()?;
                Ok(Next::Done(std::mem::take(&mut self.pinned)))
            }
        }
    }
}
