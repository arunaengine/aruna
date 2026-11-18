//! Queues one job on this node: mints its id, then admits it in one local transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::structs::execution::job::{JobId, JobPayload, WorkspaceMode, user_dedup_key};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::backend_import::BackendImportSpec;
use ulid::Ulid;

use super::records::pages::ServingNode;
use super::submit::{
    SubmitJobError, SubmitJobOperation, SubmitJobResult, SubmitJobSpec, mint_configured_job,
};
use super::submit_execution::mint_unplaced;
use crate::document_repository::{GetConfigError, GetConfigOperation};

fn unavailable(reason: &str) -> SubmitJobError {
    SubmitJobError::PlacementUnavailable(reason.to_string())
}

/// Mints a job id whose handle is the serving node's job control handle, so the owner is encoded
/// in the id itself. `dedup_key` makes the id repeatable; without one it is fresh. A node without
/// a network mints an unplaced id.
#[derive(Debug, PartialEq)]
pub struct MintJobOperation {
    realm_id: RealmId,
    owner_node_id: NodeId,
    subject: Vec<u8>,
    serving: ServingNode,
}

impl MintJobOperation {
    pub fn new(
        realm_id: RealmId,
        owner_node_id: NodeId,
        dedup_key: Option<&[u8]>,
        serving: ServingNode,
    ) -> Self {
        let subject = match dedup_key {
            Some(key) => key.to_vec(),
            None => Ulid::generate().to_bytes().to_vec(),
        };
        Self {
            realm_id,
            owner_node_id,
            subject,
            serving,
        }
    }
}

impl Operation for MintJobOperation {
    type Output = JobId;
    type Error = SubmitJobError;

    fn first(&mut self) -> Result<Next<JobId>, SubmitJobError> {
        let Some((local, serving_realm)) = self.serving else {
            return mint_unplaced(&self.subject).map(Next::Done);
        };
        if local != self.owner_node_id || serving_realm != self.realm_id {
            return Err(unavailable("job owner does not match the serving node"));
        }
        let read = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<JobId>, SubmitJobError> {
        let config = result.child::<RealmConfigDocument, GetConfigError>()?;
        let config = config.map_err(|_| unavailable("realm config unavailable"))?;
        mint_configured_job(&config, self.owner_node_id, &self.subject).map(Next::Done)
    }
}

/// Queues a job under a minted id. The accepting node is the immutable owner: creation, listing,
/// dedup, active cap and schedule rows commit in one local transaction, a failed submit leaves
/// no rows, and nothing is reserved on any other node.
#[derive(Debug, PartialEq)]
pub struct QueueJobOperation {
    spec: SubmitJobSpec,
    job_id: JobId,
}

impl QueueJobOperation {
    pub fn new(mut spec: SubmitJobSpec, job_id: JobId, serving: ServingNode) -> Self {
        if let Some((local, _)) = serving {
            spec.owner_node_id = local;
        }
        Self { spec, job_id }
    }
}

impl Operation for QueueJobOperation {
    type Output = SubmitJobResult;
    type Error = SubmitJobError;

    fn first(&mut self) -> SubmitNext {
        let (spec, job_id) = (self.spec.clone(), self.job_id);
        let build = move || SubmitJobOperation::new(spec.clone(), job_id);
        let child = boxed_retry(build, &job_id.to_bytes(), |result| {
            Event::Child(ChildOutcome::new(result))
        });
        Ok(Next::Call(IoCall::Child(child)))
    }

    fn next(&mut self, result: IoResult) -> SubmitNext {
        let submitted = result.child::<SubmitJobResult, SubmitJobError>()??;
        Ok(Next::Done(submitted))
    }
}

/// Mints an id for `spec` and queues it on this node.
#[derive(Debug, PartialEq)]
pub struct SubmitLocalOperation {
    spec: SubmitJobSpec,
    mint: Option<MintJobOperation>,
    serving: ServingNode,
    minted: bool,
}

type SubmitNext = Result<Next<SubmitJobResult>, SubmitJobError>;

impl SubmitLocalOperation {
    pub fn new(spec: SubmitJobSpec, realm_id: RealmId, serving: ServingNode) -> Self {
        let dedup = spec.dedup_key.as_deref();
        let mint = MintJobOperation::new(realm_id, spec.owner_node_id, dedup, serving);
        Self {
            mint: Some(mint),
            spec,
            serving,
            minted: false,
        }
    }

    /// One run of a backend import. A scheduled run passes its due time in `dedup`, so a
    /// repeated timer never starts the same run twice.
    pub fn backend_import(
        spec: BackendImportSpec,
        owner_node_id: NodeId,
        (dedup, retention_ms): (Option<String>, u64),
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        let created_by = spec.auth_context.user_id;
        let realm_id = spec.auth_context.realm_id;
        let spec = SubmitJobSpec {
            payload: JobPayload::BackendImport(spec),
            created_by,
            owner_node_id,
            dedup_key: dedup.map(|key| user_dedup_key(created_by, &key)),
            now_ms,
            retention_ms,
            workspace_mode: WorkspaceMode::default(),
            workspace_bucket: None,
            active_cap: None,
        };
        Self::new(spec, realm_id, serving)
    }
}

impl Operation for SubmitLocalOperation {
    type Output = SubmitJobResult;
    type Error = SubmitJobError;

    fn first(&mut self) -> SubmitNext {
        let mint = self
            .mint
            .take()
            .ok_or_else(|| unavailable("job submit already started"))?;
        Ok(Next::Call(IoCall::child(mint)))
    }

    fn next(&mut self, result: IoResult) -> SubmitNext {
        if self.minted {
            let submitted = result.child::<SubmitJobResult, SubmitJobError>()??;
            return Ok(Next::Done(submitted));
        }
        let job_id = result.child::<JobId, SubmitJobError>()??;
        self.minted = true;
        let queue = QueueJobOperation::new(self.spec.clone(), job_id, self.serving);
        Ok(Next::Call(IoCall::child(queue)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{Family, REALM, node, user};
    use aruna_core::errors::StorageError;

    fn spec(owner: NodeId) -> SubmitJobSpec {
        SubmitJobSpec {
            payload: JobPayload::Execution(crate::tests::records::payload()),
            created_by: user(),
            owner_node_id: owner,
            dedup_key: None,
            now_ms: 1,
            retention_ms: 1,
            workspace_mode: WorkspaceMode::default(),
            workspace_bucket: None,
            active_cap: None,
        }
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn job() -> JobId {
        JobId::from_bytes([4; 16])
    }

    // A node without a network mints an unplaced id without reading anything.
    #[test]
    fn offline_mints_directly() {
        let mut mint = MintJobOperation::new(REALM, node(1), Some(b"key"), None);
        assert!(matches!(mint.first(), Ok(Next::Done(_))));
    }

    // Only the serving node of its realm can own the job.
    #[test]
    fn owner_must_serve() {
        let serving = Some((node(2), REALM));
        let mut mint = MintJobOperation::new(REALM, node(1), None, serving);
        assert!(matches!(
            mint.first(),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
    }

    // An unreadable realm config, or a node without a binding, leaves the mint retryable.
    #[test]
    fn config_gives_binding() {
        let answer = |config| {
            let serving = Some((node(1), REALM));
            let mut mint = MintJobOperation::new(REALM, node(1), None, serving);
            mint.first().expect("the config is read");
            mint.next(child(config))
        };
        let failed = GetConfigError::StorageError(StorageError::Timeout);
        assert!(matches!(
            answer(Err::<RealmConfigDocument, _>(failed)),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
        let mut config = Family::new([1; 32]).config;
        assert!(matches!(
            answer(Ok::<_, GetConfigError>(config.clone())),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
        config.seed_job_control(node(1), 0);
        assert!(matches!(
            answer(Ok::<_, GetConfigError>(config)),
            Ok(Next::Done(_))
        ));
    }

    // The serving node is always the owner of what it queues.
    #[test]
    fn queue_owns_locally() {
        let queue = QueueJobOperation::new(spec(node(1)), job(), Some((node(2), REALM)));
        assert_eq!(queue.spec.owner_node_id, node(2));
    }

    // A submit mints first, queues second, and answers what the queue answered.
    #[test]
    fn submit_mints_then_queues() {
        let mut submit = SubmitLocalOperation::new(spec(node(1)), REALM, None);
        assert!(matches!(submit.first(), Ok(Next::Call(IoCall::Child(_)))));
        let queued = submit.next(child(Ok::<_, SubmitJobError>(job())));
        assert!(matches!(queued, Ok(Next::Call(IoCall::Child(_)))));
        let result = || SubmitJobResult {
            job_id: job(),
            created: true,
        };
        let done = submit.next(child(Ok::<_, SubmitJobError>(result())));
        assert_eq!(done, Ok(Next::Done(result())));
    }
}
