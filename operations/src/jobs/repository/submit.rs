//! Submits the jobs repository links start, on this node and joined by their dedup key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::structs::execution::job::{
    ExportRoCrateSpec, ImportRoCrateSpec, JobPayload, WorkspaceMode, user_dedup_key,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::secondary_id::RegisterIdentifiersSpec;
use tracing::warn;

use super::NodeScope;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::submit::{
    SubmitJobError, SubmitJobOperation, SubmitJobResult, SubmitJobSpec, mint_configured_job,
};
use crate::jobs::submit_execution::mint_unplaced;

/// A job a repository link starts.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum LinkJob {
    Export(ExportRoCrateSpec),
    Import(ImportRoCrateSpec),
    Identifiers(RegisterIdentifiersSpec),
}

impl LinkJob {
    fn realm(&self) -> RealmId {
        match self {
            Self::Export(spec) => spec.auth_context.realm_id,
            Self::Import(spec) => spec.auth_context.realm_id,
            Self::Identifiers(spec) => spec.auth_context.realm_id,
        }
    }

    fn dedup_key(&self, key: &str) -> Vec<u8> {
        match self {
            Self::Export(spec) => user_dedup_key(spec.auth_context.user_id, key),
            Self::Import(spec) => user_dedup_key(spec.auth_context.user_id, key),
            // The key names the calling job, so a replayed step joins the queued job.
            Self::Identifiers(_) => key.as_bytes().to_vec(),
        }
    }

    fn spec(self, owner_node_id: NodeId, dedup_key: Vec<u8>, now_ms: u64) -> SubmitJobSpec {
        let (created_by, retention_ms, workspace_mode, payload) = match self {
            Self::Export(spec) => (
                spec.auth_context.user_id,
                spec.limits.artifact_retention_ms,
                WorkspaceMode::default(),
                JobPayload::ExportRoCrate(spec),
            ),
            Self::Import(spec) => (
                spec.auth_context.user_id,
                spec.limits.artifact_retention_ms,
                WorkspaceMode::default(),
                JobPayload::ImportRoCrate(spec),
            ),
            Self::Identifiers(spec) => (
                spec.auth_context.user_id,
                crate::jobs::JOB_RETENTION_MS,
                WorkspaceMode::None,
                JobPayload::RegisterIdentifiers(spec),
            ),
        };
        SubmitJobSpec {
            payload,
            created_by,
            owner_node_id,
            dedup_key: Some(dedup_key),
            now_ms,
            retention_ms,
            workspace_mode,
            workspace_bucket: None,
            active_cap: None,
        }
    }
}

#[derive(Debug, PartialEq)]
enum SubmitStep {
    Config,
    Submit,
}

/// Mints the job id from the realm config and submits the job, rerun after a lost conflict.
#[derive(Debug, PartialEq)]
pub(crate) struct LinkJobOperation {
    scope: NodeScope,
    owner: NodeId,
    job: Option<LinkJob>,
    key: String,
    now_ms: u64,
    step: SubmitStep,
}

impl LinkJobOperation {
    pub(crate) fn new(scope: NodeScope, owner: NodeId, job: LinkJob, key: String) -> Self {
        Self {
            scope,
            owner,
            job: Some(job),
            key,
            now_ms: 0,
            step: SubmitStep::Config,
        }
    }

    fn submit(&mut self, job_id: aruna_core::structs::execution::job::JobId) -> Turn {
        let job = self.job.take().ok_or_else(spent)?;
        let key = job.dedup_key(&self.key);
        let spec = job.spec(self.scope.node_or(self.owner), key, self.now_ms);
        self.step = SubmitStep::Submit;
        let build = move || SubmitJobOperation::new(spec.clone(), job_id);
        let child = boxed_retry(build, &job_id.to_bytes(), |result| {
            Event::Child(ChildOutcome::new(result))
        });
        Ok(Next::Call(IoCall::Child(child)))
    }
}

type Turn = Result<Next<SubmitJobResult>, SubmitJobError>;

fn spent() -> SubmitJobError {
    SubmitJobError::UnexpectedEvent("the link job was already submitted".to_string())
}

impl Operation for LinkJobOperation {
    type Output = SubmitJobResult;
    type Error = SubmitJobError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn {
        let job = self.job.as_ref().ok_or_else(spent)?;
        let realm = job.realm();
        let subject = job.dedup_key(&self.key);
        let Some((node, serving)) = self.scope.node else {
            let job_id = mint_unplaced(&subject)?;
            return self.submit(job_id);
        };
        if node != self.owner || serving != realm {
            return Err(SubmitJobError::PlacementUnavailable(
                "job owner does not match the serving node".to_string(),
            ));
        }
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm))))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.step {
            SubmitStep::Config => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|error| {
                        warn!(%error, "Realm config unavailable for a repository link job");
                        SubmitJobError::PlacementUnavailable("realm config unavailable".to_string())
                    })?;
                let job = self.job.as_ref().ok_or_else(spent)?;
                let subject = job.dedup_key(&self.key);
                let job_id = mint_configured_job(&config, self.owner, &subject)?;
                self.submit(job_id)
            }
            SubmitStep::Submit => {
                let submitted = result.child::<SubmitJobResult, SubmitJobError>()??;
                Ok(Next::Done(submitted))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::repository::push::creator_auth;
    use crate::jobs::repository::test_support::{
        NOW_MS, child, job_id, link, local_scope, other_node, realm, scope,
    };

    fn job(scope: &NodeScope) -> LinkJob {
        LinkJob::Identifiers(RegisterIdentifiersSpec {
            document_id: link().document_id,
            identifiers: Vec::new(),
            auth_context: creator_auth(scope, &link()),
        })
    }

    fn operation(scope: NodeScope, owner: NodeId) -> LinkJobOperation {
        let job = job(&scope);
        let mut operation = LinkJobOperation::new(scope, owner, job, "key".into());
        operation.stamp(NOW_MS);
        operation
    }

    #[test]
    fn placed_job_config() {
        let mut submit = operation(scope(), link().owner_node);
        assert_eq!(
            submit.first(),
            Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm()))))
        );
        let missing =
            child::<RealmConfigDocument, GetConfigError>(Err(GetConfigError::DocumentNotFound));
        assert_eq!(
            submit.next(missing),
            Err(SubmitJobError::PlacementUnavailable(
                "realm config unavailable".into()
            ))
        );
    }

    #[test]
    fn foreign_owner_refused() {
        let mut submit = operation(scope(), other_node());
        assert!(matches!(
            submit.first(),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
    }

    #[test]
    fn unplaced_job_submits() {
        let mut submit = operation(local_scope(), link().owner_node);
        assert!(matches!(submit.first(), Ok(Next::Call(IoCall::Child(_)))));
        let done = SubmitJobResult {
            job_id: job_id(1),
            created: true,
        };
        assert_eq!(
            submit.next(child::<_, SubmitJobError>(Ok(done.clone()))),
            Ok(Next::Done(done))
        );
    }

    #[test]
    fn identifiers_keep_key() {
        assert_eq!(job(&local_scope()).dedup_key("key"), b"key");
    }
}
