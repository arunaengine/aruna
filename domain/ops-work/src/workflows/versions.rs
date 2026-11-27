//! Lists and reads the versions of a workflow the caller sees, with their files when asked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{WORKFLOW_SOURCE_KEYSPACE, WORKFLOW_VERSION_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow::{
    WorkflowFiles, WorkflowRecord, WorkflowVersionRecord, version_key,
};
use aruna_core::structs::identity::auth::AuthContext;
use ulid::Ulid;

use super::{Access, CatalogError, Need, WorkflowAccessOperation, page_of, version_rows};

/// One page of versions, the number to continue after, and how many there are.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionPage {
    pub workflow: WorkflowRecord,
    pub items: Vec<WorkflowVersionRecord>,
    pub next: Option<u32>,
    pub total: usize,
}

/// Lists the versions of a workflow in number order.
#[derive(Debug, PartialEq)]
pub struct ListVersionsOperation {
    auth: AuthContext,
    workflow_id: Ulid,
    after: Option<u32>,
    limit: usize,
    workflow: Option<WorkflowRecord>,
    found: Vec<(u32, WorkflowVersionRecord)>,
}

impl ListVersionsOperation {
    pub fn new(auth: &AuthContext, workflow_id: Ulid, after: Option<u32>, limit: usize) -> Self {
        Self {
            auth: auth.clone(),
            workflow_id,
            after,
            limit,
            workflow: None,
            found: Vec::new(),
        }
    }
}

impl Operation for ListVersionsOperation {
    type Output = VersionPage;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<VersionPage>, CatalogError> {
        let access = WorkflowAccessOperation::new(&self.auth, self.workflow_id, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<VersionPage>, CatalogError> {
        if self.workflow.is_none() {
            let (record, _) = result.child::<(WorkflowRecord, Access), CatalogError>()??;
            self.workflow = Some(record);
            return Ok(Next::Call(version_rows(self.workflow_id, None)));
        }
        let (rows, next) = result.page()?;
        for (_, value) in rows {
            let version = WorkflowVersionRecord::from_bytes(&value)?;
            self.found.push((version.version, version));
        }
        if next.is_some() {
            return Ok(Next::Call(version_rows(self.workflow_id, next)));
        }
        let workflow = self.workflow.take().ok_or(IoError::Unfinished)?;
        let found = std::mem::take(&mut self.found);
        let (items, next, total) = page_of(found, self.after.as_ref(), self.limit);
        Ok(Next::Done(VersionPage {
            workflow,
            items,
            next,
            total,
        }))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound)
    }
}

/// One version of a workflow the caller sees, and its files when asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionView {
    pub workflow: WorkflowRecord,
    pub access: Access,
    pub version: WorkflowVersionRecord,
    pub files: Option<WorkflowFiles>,
}

/// Reads one version, checked like its workflow.
#[derive(Debug, PartialEq)]
pub struct GetVersionOperation {
    auth: AuthContext,
    workflow_id: Ulid,
    version: u32,
    need: Need,
    with_files: bool,
    workflow: Option<(WorkflowRecord, Access)>,
}

impl GetVersionOperation {
    pub fn new(auth: &AuthContext, workflow_id: Ulid, version: u32) -> Self {
        Self {
            auth: auth.clone(),
            workflow_id,
            version,
            need: Need::Read,
            with_files: false,
            workflow: None,
        }
    }

    /// Reads the files of the version too.
    pub fn with_files(mut self) -> Self {
        self.with_files = true;
        self
    }
}

impl Operation for GetVersionOperation {
    type Output = VersionView;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<VersionView>, CatalogError> {
        let access = WorkflowAccessOperation::new(&self.auth, self.workflow_id, self.need);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<VersionView>, CatalogError> {
        let Some((workflow, access)) = self.workflow.take() else {
            self.workflow = Some(result.child::<(WorkflowRecord, Access), CatalogError>()??);
            let key = version_key(self.workflow_id, self.version);
            let mut reads = vec![(WORKFLOW_VERSION_KEYSPACE.to_string(), key.clone())];
            if self.with_files {
                reads.push((WORKFLOW_SOURCE_KEYSPACE.to_string(), key));
            }
            return Ok(Next::Call(IoCall::Read(reads)));
        };
        let mut values = result.values()?.into_iter();
        let version = values.next().flatten().ok_or(CatalogError::NotFound)?;
        let files = match values.next().flatten() {
            Some(files) => Some(WorkflowFiles::from_bytes(&files)?),
            None if self.with_files => return Err(CatalogError::NotFound),
            None => None,
        };
        Ok(Next::Done(VersionView {
            workflow,
            access,
            version: WorkflowVersionRecord::from_bytes(&version)?,
            files,
        }))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound)
    }
}
