//! Preflight and atomic single-transaction save of a workflow version and its paired automation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::automation::record::{Automation, Lifecycle};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::WORKFLOW_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::execution::workflow::{
    StoredWorkflowLanguage, WorkflowRecord, workflow_key, workflow_permission_path,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::features::Feature;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};

use super::commit_bundle::{CommitBundleInput, CommitBundleOperation, map_catalog};
pub use super::commit_bundle::{SaveAutomation, SaveBundle, SaveWorkflow, SavedBundle};
use super::identity::SyncIdentityOperation;
use super::{AutoError, allowed, authorize_path, auto_answer, check_feature, read_auto};
use crate::workflows::CatalogError;
use crate::workflows::import::{PrepareVersionOperation, Prepared};

#[derive(Debug, PartialEq)]
enum SaveStep {
    Config,
    AuthGroup,
    AuthWorkflow,
    LoadWorkflow,
    LoadAutomation,
    Prepare,
    Commit,
    Sync(Box<SavedBundle>),
    Done,
}

/// Checks feature switches and write permissions, preflights workflow source when provided,
/// commits workflow + version + automation in one atomic write transaction, and syncs identity.
#[derive(Debug, PartialEq)]
pub struct SaveBundleOperation {
    auth: AuthContext,
    local: NodeId,
    input: SaveBundle,
    disabled_features: BTreeSet<Feature>,
    ids: IdMint,
    now_ms: u64,
    workflow_language: Option<StoredWorkflowLanguage>,
    prepared: Option<Prepared>,
    step: SaveStep,
}

impl SaveBundleOperation {
    pub fn new(auth: &AuthContext, local: NodeId, input: SaveBundle) -> Self {
        Self {
            auth: auth.clone(),
            local,
            input,
            disabled_features: BTreeSet::new(),
            ids: IdMint::default(),
            now_ms: 0,
            workflow_language: None,
            prepared: None,
            step: SaveStep::Config,
        }
    }

    pub fn with_disabled_features(mut self, disabled: BTreeSet<Feature>) -> Self {
        self.disabled_features = disabled;
        self
    }

    fn start_prepare(&mut self) -> Result<Next<SavedBundle>, AutoError> {
        if let Some(source) = self.input.workflow.source.take() {
            let language = self.workflow_language.or(self.input.workflow.language);
            let prepare = PrepareVersionOperation::new(
                (&self.auth, self.local),
                self.input.group_id,
                language,
                source,
            );
            self.step = SaveStep::Prepare;
            return Ok(Next::Call(IoCall::child(prepare)));
        }
        self.start_commit()
    }

    fn start_commit(&mut self) -> Result<Next<SavedBundle>, AutoError> {
        let (workflow_id, workflow_new) = match self.input.workflow.workflow_id {
            Some(id) => (id, false),
            None => (self.ids.mint(self.now_ms), true),
        };
        let (automation_id, automation_new) = match self.input.automation.automation_id {
            Some(id) => (id, false),
            None => (self.ids.mint(self.now_ms), true),
        };
        let commit = CommitBundleOperation::new(CommitBundleInput {
            group_id: self.input.group_id,
            local: self.local,
            realm_id: self.auth.realm_id,
            actor: self.auth.user_id,
            workflow_id,
            workflow_new,
            automation_id,
            automation_new,
            workflow: self.input.workflow.clone(),
            automation: self.input.automation.clone(),
            prepared: self.prepared.take(),
            now_ms: self.now_ms,
        });
        self.step = SaveStep::Commit;
        Ok(Next::Call(IoCall::retry(commit, &automation_id.to_bytes())))
    }
}

impl Operation for SaveBundleOperation {
    type Output = SavedBundle;
    type Error = AutoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, seed: u128) {
        self.ids = IdMint::new(seed);
    }

    fn first(&mut self) -> Result<Next<SavedBundle>, AutoError> {
        if self.input.workflow.workflow_id.is_none() && self.input.workflow.source.is_none() {
            return Err(AutoError::Invalid(
                "creating a new workflow requires a workflow source".into(),
            ));
        }
        if self.input.workflow.workflow_id.is_none()
            && self
                .input
                .workflow
                .name
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .is_empty()
        {
            return Err(AutoError::Invalid("a workflow needs a name".into()));
        }
        if self.input.automation.automation_id.is_some()
            && self.input.automation.expected_revision.is_none()
        {
            return Err(AutoError::Invalid(
                "expected_revision is required when updating an automation".into(),
            ));
        }
        self.step = SaveStep::Config;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.auth.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SavedBundle>, AutoError> {
        match std::mem::replace(&mut self.step, SaveStep::Done) {
            SaveStep::Config => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|_| {
                        AutoError::Unavailable("the realm feature policy cannot be read".into())
                    })?;
                check_feature(
                    &config,
                    &self.disabled_features,
                    Feature::Workflows,
                    self.input.group_id,
                )?;
                check_feature(
                    &config,
                    &self.disabled_features,
                    Feature::Automations,
                    self.input.group_id,
                )?;
                if self.input.automation.spec.uses_applets() {
                    check_feature(
                        &config,
                        &self.disabled_features,
                        Feature::Applets,
                        self.input.group_id,
                    )?;
                }
                let path =
                    group_permission_path(self.auth.realm_id, self.input.group_id, self.local);
                self.step = SaveStep::AuthGroup;
                Ok(Next::Call(authorize_path(
                    &self.auth,
                    &path,
                    Permission::WRITE,
                )))
            }
            SaveStep::AuthGroup => {
                if !allowed(result)? {
                    return Err(AutoError::Forbidden);
                }
                let workflow_path =
                    workflow_permission_path(self.auth.realm_id, self.input.group_id);
                self.step = SaveStep::AuthWorkflow;
                Ok(Next::Call(authorize_path(
                    &self.auth,
                    &workflow_path,
                    Permission::WRITE,
                )))
            }
            SaveStep::AuthWorkflow => {
                if !allowed(result)? {
                    return Err(AutoError::Forbidden);
                }
                if let Some(workflow_id) = self.input.workflow.workflow_id {
                    self.step = SaveStep::LoadWorkflow;
                    return Ok(Next::Call(read_row(
                        WORKFLOW_KEYSPACE,
                        workflow_key(workflow_id),
                    )));
                }
                if let Some(automation_id) = self.input.automation.automation_id {
                    self.step = SaveStep::LoadAutomation;
                    return Ok(Next::Call(read_auto(automation_id)));
                }
                self.start_prepare()
            }
            SaveStep::LoadWorkflow => {
                let bytes = result.value()?.ok_or(AutoError::NotFound)?;
                let record = WorkflowRecord::from_bytes(&bytes)?;
                if record.group_id != self.input.group_id {
                    return Err(AutoError::Invalid(
                        "workflow belongs to a different group".into(),
                    ));
                }
                self.workflow_language = Some(record.language);
                if let Some(automation_id) = self.input.automation.automation_id {
                    self.step = SaveStep::LoadAutomation;
                    return Ok(Next::Call(read_auto(automation_id)));
                }
                self.start_prepare()
            }
            SaveStep::LoadAutomation => {
                let automation = auto_answer(result)?
                    .filter(|item| item.lifecycle != Lifecycle::Removed)
                    .ok_or(AutoError::NotFound)?;
                if automation.group_id != self.input.group_id {
                    return Err(AutoError::Invalid(
                        "automation belongs to a different group".into(),
                    ));
                }
                if self
                    .input
                    .automation
                    .expected_revision
                    .is_some_and(|expected| expected != automation.revision)
                {
                    return Err(AutoError::Conflict(
                        "automation changed meanwhile; read again".into(),
                    ));
                }
                self.start_prepare()
            }
            SaveStep::Prepare => {
                let prepared = result
                    .child::<Prepared, CatalogError>()?
                    .map_err(map_catalog)?;
                self.prepared = Some(prepared);
                self.start_commit()
            }
            SaveStep::Commit => {
                let mut saved = result.child::<SavedBundle, AutoError>()??;
                if saved.automation.needs_identity_sync() && saved.automation.granter().is_some() {
                    let sync = SyncIdentityOperation::new(
                        &saved.automation,
                        (self.local, self.auth.realm_id),
                    );
                    self.step = SaveStep::Sync(Box::new(saved));
                    return Ok(Next::Call(IoCall::child(sync)));
                }
                saved.identity_synced = true;
                Ok(Next::Done(saved))
            }
            SaveStep::Sync(mut saved) => {
                match result.child::<Automation, AutoError>()? {
                    Ok(updated) => {
                        saved.automation = updated;
                        saved.identity_synced = true;
                    }
                    Err(_) => {
                        // The workflow and automation rows already committed; execution paths
                        // follow `auto.spec`, and periodic upkeep repairs the role from the
                        // persisted `identity_synced_revision < revision` gap.
                        saved.identity_synced = false;
                    }
                }
                Ok(Next::Done(*saved))
            }
            SaveStep::Done => Err(IoError::Unfinished.into()),
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

#[cfg(test)]
#[path = "save_tests.rs"]
mod tests;
