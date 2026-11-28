//! Lists, reads, changes and deletes workflows: a list holds what the caller's groups and the
//! public workflows give, filtered and paged by workflow id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::keys::record_key as automation_key;
use aruna_core::automation::record::Automation;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{
    AUTOMATIONS_KEYSPACE, WORKFLOW_KEYSPACE, WORKFLOW_SOURCE_KEYSPACE, WORKFLOW_VERSION_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::execution::workflow::{
    StoredWorkflowLanguage, WorkflowRecord, WorkflowVisibility, workflow_key,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::{GroupId, Key};
use ulid::Ulid;

use super::{
    Access, CatalogError, GroupAccessOperation, Need, ScanOperation, WorkflowAccessOperation,
    access_answer, page_of, scan_answer, version_rows,
};

/// A workflow and whether the caller may change it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowView {
    pub record: WorkflowRecord,
    pub can_write: bool,
}

/// What a workflow list keeps.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkflowFilter {
    pub group_id: Option<GroupId>,
    pub language: Option<StoredWorkflowLanguage>,
    pub edam_format: Option<String>,
    /// Words the name or description holds, in any case.
    pub text: Option<String>,
}

impl WorkflowFilter {
    fn keeps(&self, record: &WorkflowRecord) -> bool {
        let words = self.text.as_deref().map(str::to_lowercase);
        let described = |text: &str| {
            let name = record.name.to_lowercase();
            let description = record
                .description
                .as_deref()
                .unwrap_or_default()
                .to_lowercase();
            name.contains(text) || description.contains(text)
        };
        self.language
            .is_none_or(|language| language == record.language)
            && (self.edam_format.as_ref()).is_none_or(|format| record.edam_formats.contains(format))
            && words.as_deref().is_none_or(described)
    }
}

/// One page of workflows, the id to continue after, and how many match in all.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowPage {
    pub items: Vec<WorkflowView>,
    pub next: Option<Ulid>,
    pub total: usize,
}

/// Lists the workflows the caller sees: those of groups it may read and every public one.
#[derive(Debug, PartialEq)]
pub struct ListWorkflowsOperation {
    auth: AuthContext,
    filter: WorkflowFilter,
    after: Option<Ulid>,
    limit: usize,
    found: Vec<WorkflowRecord>,
}

impl ListWorkflowsOperation {
    pub fn new(
        auth: &AuthContext,
        filter: WorkflowFilter,
        after: Option<Ulid>,
        limit: usize,
    ) -> Self {
        Self {
            auth: auth.clone(),
            filter,
            after,
            limit,
            found: Vec::new(),
        }
    }
}

impl Operation for ListWorkflowsOperation {
    type Output = WorkflowPage;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<WorkflowPage>, CatalogError> {
        let groups = self.filter.group_id.map(|group| [group].into());
        let scan = ScanOperation::<WorkflowRecord>::of_groups(groups);
        Ok(Next::Call(IoCall::child(scan)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<WorkflowPage>, CatalogError> {
        if self.found.is_empty() {
            let rows = scan_answer::<WorkflowRecord>(result)?;
            self.found = (rows.into_iter().map(|(_, record)| record))
                .filter(|record| self.filter.keeps(record))
                .collect();
            if self.found.is_empty() {
                return Ok(Next::Done(empty_page()));
            }
            let groups = self.found.iter().map(|record| record.group_id);
            return Ok(Next::Call(IoCall::child(GroupAccessOperation::new(
                &self.auth, groups,
            ))));
        }
        let access = access_answer(result)?;
        let visible = (std::mem::take(&mut self.found).into_iter()).filter_map(|record| {
            let granted: Access = access.get(&record.group_id).copied().unwrap_or_default();
            let view = WorkflowView {
                can_write: granted.write,
                record,
            };
            granted
                .sees(view.record.visibility)
                .then_some((view.record.workflow_id, view))
        });
        let (items, next, total) = page_of(visible.collect(), self.after.as_ref(), self.limit);
        Ok(Next::Done(WorkflowPage { items, next, total }))
    }
}

fn empty_page() -> WorkflowPage {
    WorkflowPage {
        items: Vec::new(),
        next: None,
        total: 0,
    }
}

/// Reads one workflow the caller sees.
#[derive(Debug, PartialEq)]
pub struct GetWorkflowOperation {
    auth: AuthContext,
    workflow_id: Ulid,
}

impl GetWorkflowOperation {
    pub fn new(auth: &AuthContext, workflow_id: Ulid) -> Self {
        Self {
            auth: auth.clone(),
            workflow_id,
        }
    }
}

impl Operation for GetWorkflowOperation {
    type Output = WorkflowView;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<WorkflowView>, CatalogError> {
        let access = WorkflowAccessOperation::new(&self.auth, self.workflow_id, Need::Read);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<WorkflowView>, CatalogError> {
        let (record, access) = result.child::<(WorkflowRecord, Access), CatalogError>()??;
        Ok(Next::Done(WorkflowView {
            record,
            can_write: access.write,
        }))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound)
    }
}

/// The fields a change of a workflow sets; `description: Some(None)` clears it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkflowPatch {
    pub name: Option<String>,
    pub description: Option<Option<String>>,
    pub visibility: Option<WorkflowVisibility>,
}

/// Renames, describes or shares a workflow the caller may write.
#[derive(Debug, PartialEq)]
pub struct PatchWorkflowOperation {
    auth: AuthContext,
    workflow_id: Ulid,
    patch: Option<WorkflowPatch>,
}

impl PatchWorkflowOperation {
    pub fn new(auth: &AuthContext, workflow_id: Ulid, patch: WorkflowPatch) -> Self {
        Self {
            auth: auth.clone(),
            workflow_id,
            patch: Some(patch),
        }
    }
}

impl Operation for PatchWorkflowOperation {
    type Output = WorkflowView;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<WorkflowView>, CatalogError> {
        let patch = self.patch.as_ref().ok_or(IoError::Unfinished)?;
        if (patch.name.as_deref()).is_some_and(|name| name.trim().is_empty()) {
            return Err(CatalogError::Invalid("a workflow needs a name".into()));
        }
        let access = WorkflowAccessOperation::new(&self.auth, self.workflow_id, Need::Write);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<WorkflowView>, CatalogError> {
        let Some(patch) = self.patch.take() else {
            let record = result.child::<WorkflowRecord, CatalogError>()??;
            return Ok(Next::Done(WorkflowView {
                record,
                can_write: true,
            }));
        };
        result.child::<(WorkflowRecord, Access), CatalogError>()??;
        let change = PatchRowOperation {
            workflow_id: self.workflow_id,
            patch: Some(patch),
            now_ms: 0,
            record: None,
        };
        Ok(Next::Call(IoCall::child(change)))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound | CatalogError::Forbidden)
    }
}

/// Applies a patch to the stored workflow row in one transaction.
#[derive(Debug, PartialEq)]
struct PatchRowOperation {
    workflow_id: Ulid,
    patch: Option<WorkflowPatch>,
    now_ms: u64,
    record: Option<WorkflowRecord>,
}

impl Operation for PatchRowOperation {
    type Output = WorkflowRecord;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<WorkflowRecord>, CatalogError> {
        let key = workflow_key(self.workflow_id);
        Ok(Next::Call(aruna_core::rows::read_row(
            WORKFLOW_KEYSPACE,
            key,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<WorkflowRecord>, CatalogError> {
        if let Some(record) = self.record.take() {
            result.done()?;
            return Ok(Next::Done(record));
        }
        let value = result.value()?.ok_or(CatalogError::NotFound)?;
        let mut record = WorkflowRecord::from_bytes(&value)?;
        let patch = self.patch.take().ok_or(IoError::Unfinished)?;
        if let Some(name) = patch.name {
            record.name = name.trim().to_owned();
        }
        if let Some(description) = patch.description {
            record.description = description;
        }
        if let Some(visibility) = patch.visibility {
            record.visibility = visibility;
        }
        record.updated_at_ms = self.now_ms;
        let key = workflow_key(self.workflow_id);
        let row = (
            WORKFLOW_KEYSPACE.to_string(),
            key,
            record.to_bytes()?.into(),
        );
        self.record = Some(record);
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

/// Deletes a workflow the caller may write with every version and its files.
#[derive(Debug, PartialEq)]
pub struct DeleteWorkflowOperation {
    auth: AuthContext,
    workflow_id: Ulid,
    removing: bool,
}

impl DeleteWorkflowOperation {
    pub fn new(auth: &AuthContext, workflow_id: Ulid) -> Self {
        Self {
            auth: auth.clone(),
            workflow_id,
            removing: false,
        }
    }
}

impl Operation for DeleteWorkflowOperation {
    type Output = ();
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<()>, CatalogError> {
        let access = WorkflowAccessOperation::new(&self.auth, self.workflow_id, Need::Write);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, CatalogError> {
        if self.removing {
            return Ok(Next::Done(result.child::<(), CatalogError>()??));
        }
        result.child::<(WorkflowRecord, Access), CatalogError>()??;
        self.removing = true;
        let rows = DeleteRowsOperation {
            workflow_id: self.workflow_id,
            now_ms: 0,
            keys: Vec::new(),
            unlink: None,
            step: DeleteStep::ReadWorkflow,
        };
        Ok(Next::Call(IoCall::child(rows)))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound | CatalogError::Forbidden)
    }
}

#[derive(Debug, PartialEq)]
enum DeleteStep {
    ReadWorkflow,
    ReadAutomation,
    PageVersions,
    UnlinkAutomation,
    DeleteRows,
}

/// Removes the rows of a workflow, its versions and their files in one transaction, clearing any
/// paired automation backlink atomically.
#[derive(Debug, PartialEq)]
struct DeleteRowsOperation {
    workflow_id: Ulid,
    now_ms: u64,
    keys: Vec<Key>,
    unlink: Option<Box<Automation>>,
    step: DeleteStep,
}

impl DeleteRowsOperation {
    fn start_delete(&mut self) -> Next<()> {
        let mut rows = vec![(
            WORKFLOW_KEYSPACE.to_string(),
            workflow_key(self.workflow_id),
        )];
        for key in std::mem::take(&mut self.keys) {
            rows.push((WORKFLOW_VERSION_KEYSPACE.into(), key.clone()));
            rows.push((WORKFLOW_SOURCE_KEYSPACE.into(), key));
        }
        self.step = DeleteStep::DeleteRows;
        Next::Call(IoCall::Delete(rows))
    }
}

impl Operation for DeleteRowsOperation {
    type Output = ();
    type Error = CatalogError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<()>, CatalogError> {
        self.step = DeleteStep::ReadWorkflow;
        Ok(Next::Call(read_row(
            WORKFLOW_KEYSPACE,
            workflow_key(self.workflow_id),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, CatalogError> {
        match std::mem::replace(&mut self.step, DeleteStep::DeleteRows) {
            DeleteStep::ReadWorkflow => {
                if let Some(bytes) = result.value()? {
                    let record = WorkflowRecord::from_bytes(&bytes)?;
                    if let Some(automation_id) = record.automation_id {
                        self.step = DeleteStep::ReadAutomation;
                        return Ok(Next::Call(read_row(
                            AUTOMATIONS_KEYSPACE,
                            automation_key(automation_id),
                        )));
                    }
                }
                self.step = DeleteStep::PageVersions;
                Ok(Next::Call(version_rows(self.workflow_id, None)))
            }
            DeleteStep::ReadAutomation => {
                if let Some(bytes) = result.value()? {
                    let mut automation = Automation::from_bytes(&bytes)?;
                    if automation.workflow_id == Some(self.workflow_id) {
                        automation.workflow_id = None;
                        automation.updated_at_ms = self.now_ms;
                        automation.revision = automation.revision.saturating_add(1);
                        self.unlink = Some(Box::new(automation));
                    }
                }
                self.step = DeleteStep::PageVersions;
                Ok(Next::Call(version_rows(self.workflow_id, None)))
            }
            DeleteStep::PageVersions => {
                let (rows, next) = result.page()?;
                self.keys.extend(rows.into_iter().map(|(key, _)| key));
                if next.is_some() {
                    self.step = DeleteStep::PageVersions;
                    return Ok(Next::Call(version_rows(self.workflow_id, next)));
                }
                if let Some(automation) = self.unlink.take() {
                    let row = crate::automation_row(&automation)?;
                    self.step = DeleteStep::UnlinkAutomation;
                    return Ok(Next::Call(IoCall::Write(vec![row])));
                }
                Ok(self.start_delete())
            }
            DeleteStep::UnlinkAutomation => {
                result.done()?;
                Ok(self.start_delete())
            }
            DeleteStep::DeleteRows => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}
