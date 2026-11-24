//! The workflow registry and the tool catalog of a node: imports from every source kind,
//! versions, stored preflight results, tools and the group access that guards them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod import;
pub mod inspect;
pub mod reads;
pub mod runs;
pub mod tools;
pub mod versions;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::ConversionError;
use aruna_core::events::Event;
use aruna_core::keyspaces::{WORKFLOW_KEYSPACE, WORKFLOW_TOOL_KEYSPACE, WORKFLOW_VERSION_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::request_policy::PolicyRequestExtras;
use aruna_core::structs::execution::workflow::{
    ToolRecord, WorkflowRecord, WorkflowVisibility, version_prefix, workflow_key,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::{GroupId, Key};
use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect, WorkflowError};
use aruna_ops_identity::auth::error::AuthorizeError;
use aruna_ops_identity::auth::request_authorization::AuthorizeOperation;
use aruna_workflow_contract::preflight::{Diagnostic, DiagnosticPointer, Severity, WorkflowGraph};
use aruna_workflow_contract::{PreflightKind, PreflightResult};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

/// The most bytes the files of one workflow version or tool may hold together.
pub const MAX_SOURCE_BYTES: u64 = 64 << 20;
/// Rows one storage page of a catalog scan reads.
const SCAN_ROWS: usize = 256;

/// Why a registry or catalog call failed.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum CatalogError {
    /// Unknown, or hidden from the caller.
    #[error("not found")]
    NotFound,
    #[error("permission denied")]
    Forbidden,
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Unsupported(String),
    #[error("{0}")]
    Conflict(String),
    #[error(transparent)]
    Coordinator(WorkflowError),
    #[error("storage failed: {0}")]
    Storage(String),
}

impl From<IoError> for CatalogError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.to_string())
    }
}

impl From<ConversionError> for CatalogError {
    fn from(error: ConversionError) -> Self {
        Self::Storage(error.to_string())
    }
}

/// What the caller may do in one group.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Access {
    pub read: bool,
    pub write: bool,
}

impl Access {
    /// Whether the caller sees a workflow of this group with `visibility`.
    pub fn sees(self, visibility: WorkflowVisibility) -> bool {
        self.read || visibility == WorkflowVisibility::Public
    }
}

/// The diagnostics of a check and whether the workflow passes it.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CheckReport {
    pub ok: bool,
    pub diagnostics: Vec<Diagnostic>,
}

/// A step graph and whether it is known whole before a run.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GraphView {
    pub graph: WorkflowGraph,
    pub complete: bool,
}

/// The permission path of the workflows and tools of a group, below its metadata.
pub fn workflow_path(realm_id: RealmId, group_id: GroupId) -> String {
    format!("/{realm_id}/g/{group_id}/meta/workflows")
}

pub(crate) fn diagnostic(severity: Severity, message: impl Into<String>) -> Diagnostic {
    Diagnostic {
        severity,
        message: message.into(),
        pointer: DiagnosticPointer::default(),
    }
}

/// One request to a preflight coordinator of this node.
pub(crate) fn preflight_call(kind: PreflightKind) -> IoCall {
    let effect = Effect::Workflow(Box::new(WorkflowEffect::Preflight(kind)));
    IoCall::Effect(Box::new(effect))
}

/// The answer to [`preflight_call`], with a coordinator failure kept apart from a refusal.
pub(crate) fn preflight_answer(
    result: IoResult,
) -> Result<Result<PreflightResult, WorkflowError>, CatalogError> {
    match result.event()? {
        Event::Workflow(answer) => match *answer {
            WorkflowAnswer::Preflight(answer) => Ok(answer),
            _ => Err(unexpected("WorkflowAnswer::Preflight")),
        },
        _ => Err(unexpected("Event::Workflow")),
    }
}

/// A refusal of the workflow itself is the caller's, any other failure the node's.
pub(crate) fn refused(error: WorkflowError) -> CatalogError {
    match error {
        WorkflowError::Refused(refusal) => CatalogError::Invalid(refusal.to_string()),
        other => CatalogError::Coordinator(other),
    }
}

pub(crate) fn unexpected(expected: &'static str) -> CatalogError {
    CatalogError::from(IoError::Unexpected {
        expected,
        got: "another answer",
    })
}

pub(crate) fn to_text<T: Serialize>(value: &T) -> Result<String, CatalogError> {
    serde_json::to_string(value).map_err(|error| CatalogError::Storage(error.to_string()))
}

pub(crate) fn from_text<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, CatalogError> {
    serde_json::from_str(text).map_err(|error| CatalogError::Storage(error.to_string()))
}

/// Decides for each group whether the caller may write, and else whether it may read, by the
/// group's roles and request policies.
#[derive(Debug, PartialEq)]
pub struct GroupAccessOperation {
    auth: AuthContext,
    pending: Vec<GroupId>,
    asking: Option<(GroupId, Permission)>,
    found: BTreeMap<GroupId, Access>,
}

impl GroupAccessOperation {
    pub fn new(auth: &AuthContext, groups: impl IntoIterator<Item = GroupId>) -> Self {
        let pending: BTreeSet<GroupId> = groups.into_iter().collect();
        Self {
            auth: auth.clone(),
            pending: pending.into_iter().rev().collect(),
            asking: None,
            found: BTreeMap::new(),
        }
    }

    fn ask(
        &mut self,
        group_id: GroupId,
        permission: Permission,
    ) -> Next<BTreeMap<GroupId, Access>> {
        let path = workflow_path(self.auth.realm_id, group_id);
        let check = AuthorizeOperation::new(
            self.auth.realm_id,
            &self.auth,
            &path,
            &permission,
            PolicyRequestExtras::rest(),
        );
        self.asking = Some((group_id, permission));
        Next::Call(IoCall::child(check))
    }

    fn advance(&mut self) -> Next<BTreeMap<GroupId, Access>> {
        match self.pending.pop() {
            Some(group_id) => self.ask(group_id, Permission::WRITE),
            None => Next::Done(std::mem::take(&mut self.found)),
        }
    }
}

impl Operation for GroupAccessOperation {
    type Output = BTreeMap<GroupId, Access>;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Self::Output>, CatalogError> {
        Ok(self.advance())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CatalogError> {
        let (group_id, permission) = self.asking.take().ok_or(IoError::Unfinished)?;
        let allowed = match result.child::<(), AuthorizeError>()? {
            Ok(()) => true,
            Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => false,
            Err(error) => return Err(CatalogError::Storage(error.to_string())),
        };
        match (permission, allowed) {
            (Permission::WRITE, false) => return Ok(self.ask(group_id, Permission::READ)),
            (Permission::WRITE, true) => {
                let access = Access {
                    read: true,
                    write: true,
                };
                self.found.insert(group_id, access);
            }
            (_, read) => {
                let access = Access { read, write: false };
                self.found.insert(group_id, access);
            }
        }
        Ok(self.advance())
    }
}

/// The answer of a [`GroupAccessOperation`] child.
pub(crate) fn access_answer(result: IoResult) -> Result<BTreeMap<GroupId, Access>, CatalogError> {
    result.child::<BTreeMap<GroupId, Access>, CatalogError>()?
}

/// What a caller must be allowed to do with a workflow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Need {
    Read,
    Write,
}

/// Reads a workflow and decides the caller's access: a workflow the caller cannot see is not
/// found, one it sees but may not change is forbidden for a write.
#[derive(Debug, PartialEq)]
pub struct WorkflowAccessOperation {
    auth: AuthContext,
    workflow_id: Ulid,
    need: Need,
    record: Option<WorkflowRecord>,
}

impl WorkflowAccessOperation {
    pub fn new(auth: &AuthContext, workflow_id: Ulid, need: Need) -> Self {
        Self {
            auth: auth.clone(),
            workflow_id,
            need,
            record: None,
        }
    }
}

impl Operation for WorkflowAccessOperation {
    type Output = (WorkflowRecord, Access);
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Self::Output>, CatalogError> {
        let key = workflow_key(self.workflow_id);
        Ok(Next::Call(aruna_core::rows::read_row(
            WORKFLOW_KEYSPACE,
            key,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CatalogError> {
        let Some(record) = self.record.take() else {
            let value = result.value()?.ok_or(CatalogError::NotFound)?;
            let record = WorkflowRecord::from_bytes(&value)?;
            let check = GroupAccessOperation::new(&self.auth, [record.group_id]);
            self.record = Some(record);
            return Ok(Next::Call(IoCall::child(check)));
        };
        let access = (access_answer(result)?.get(&record.group_id).copied()).unwrap_or_default();
        if !access.sees(record.visibility) {
            return Err(CatalogError::NotFound);
        }
        if self.need == Need::Write && !access.write {
            return Err(CatalogError::Forbidden);
        }
        Ok(Next::Done((record, access)))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound | CatalogError::Forbidden)
    }
}

/// A catalog row a scan reads: its keyspace, its codec and the group that owns it.
pub trait CatalogRow: Sized + Send + std::fmt::Debug + PartialEq + 'static {
    const SPACE: &'static str;

    fn decode(bytes: &[u8]) -> Result<Self, ConversionError>;

    fn group(&self) -> GroupId;
}

impl CatalogRow for ToolRecord {
    const SPACE: &'static str = WORKFLOW_TOOL_KEYSPACE;

    fn decode(bytes: &[u8]) -> Result<Self, ConversionError> {
        ToolRecord::from_bytes(bytes)
    }

    fn group(&self) -> GroupId {
        self.group_id
    }
}

impl CatalogRow for WorkflowRecord {
    const SPACE: &'static str = WORKFLOW_KEYSPACE;

    fn decode(bytes: &[u8]) -> Result<Self, ConversionError> {
        WorkflowRecord::from_bytes(bytes)
    }

    fn group(&self) -> GroupId {
        self.group_id
    }
}

/// Every row below a prefix, page by page, kept when its group is one of `keep`.
#[derive(Debug, PartialEq)]
pub struct ScanOperation<T> {
    prefix: Key,
    keep: Option<BTreeSet<GroupId>>,
    rows: Vec<(Key, T)>,
}

impl<T: CatalogRow> ScanOperation<T> {
    /// The rows of `groups`, or of every group, in key order.
    pub fn of_groups(groups: Option<BTreeSet<GroupId>>) -> Self {
        Self {
            prefix: Key::from(Vec::new()),
            keep: groups,
            rows: Vec::new(),
        }
    }

    /// Every row below `prefix`.
    pub fn below(prefix: Key) -> Self {
        Self {
            prefix,
            keep: None,
            rows: Vec::new(),
        }
    }

    fn page(&self, after: Option<Key>) -> Next<Vec<(Key, T)>> {
        Next::Call(IoCall::Page {
            space: T::SPACE.to_string(),
            prefix: self.prefix.clone(),
            after,
            limit: SCAN_ROWS,
        })
    }
}

impl<T: CatalogRow> Operation for ScanOperation<T> {
    type Output = Vec<(Key, T)>;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Self::Output>, CatalogError> {
        Ok(self.page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CatalogError> {
        let (rows, next) = result.page()?;
        for (key, value) in rows {
            let row = T::decode(&value)?;
            if (self.keep.as_ref()).is_none_or(|groups| groups.contains(&row.group())) {
                self.rows.push((key, row));
            }
        }
        match next {
            Some(after) => Ok(self.page(Some(after))),
            None => Ok(Next::Done(std::mem::take(&mut self.rows))),
        }
    }
}

/// One page of the version rows of a workflow, after `after`.
pub(crate) fn version_rows(workflow_id: Ulid, after: Option<Key>) -> IoCall {
    IoCall::Page {
        space: WORKFLOW_VERSION_KEYSPACE.into(),
        prefix: version_prefix(workflow_id),
        after,
        limit: SCAN_ROWS,
    }
}

/// The answer of a [`ScanOperation`] child.
pub(crate) fn scan_answer<T: 'static>(result: IoResult) -> Result<Vec<(Key, T)>, CatalogError> {
    result.child::<Vec<(Key, T)>, CatalogError>()?
}

/// One page of `items` after `after`, at most `limit` long, and the key to continue after.
pub(crate) fn page_of<K: Ord + Clone, T>(
    items: Vec<(K, T)>,
    after: Option<&K>,
    limit: usize,
) -> (Vec<T>, Option<K>, usize) {
    let total = items.len();
    let mut rest = (items.into_iter())
        .filter(|(key, _)| after.is_none_or(|after| key > after))
        .peekable();
    let mut page = Vec::new();
    let mut last = None;
    while page.len() < limit.max(1) {
        let Some((key, item)) = rest.next() else {
            break;
        };
        last = Some(key);
        page.push(item);
    }
    let next = rest.peek().and(last);
    (page, next, total)
}
