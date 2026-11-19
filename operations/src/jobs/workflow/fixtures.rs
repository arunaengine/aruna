//! The job, attempt and results the workflow operation tests share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::compute::effect::{BackendFacts, ComputeAnswer};
use aruna_core::compute::{
    ArtifactEvidence, AttemptPhase, AttemptRef, AttemptStatus, ExecutorKind, FenceContext, NOBODY,
    ReconcileEvidence, StagingMode,
};
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::jobs::{JobEvent, JobStops};
use aruna_core::structs::execution::job::{
    AttemptControl, AttemptIntent, ExecutionSpec, JobClaim, JobId, JobPayload, JobRecord,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::routing::NodeRouting;
use ulid::Ulid;

use super::attempt::{AttemptScope, RunWiring};
use super::prepare::PreparedTask;

pub(super) const EPOCH: u64 = 3;
pub(super) const BUCKET: &str = "work";
pub(super) const PINNED: &str = "alpine@sha256:1";

pub(super) fn job() -> JobId {
    JobId::from_bytes([1; 16])
}

pub(super) fn token() -> Ulid {
    Ulid::from_bytes([2; 16])
}

pub(super) fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[4; 32]).public()
}

pub(super) fn realm() -> RealmId {
    RealmId([1; 32])
}

pub(super) fn group_spec(group_id: Ulid) -> ExecutionSpec {
    ExecutionSpec { group_id, ..spec() }
}

pub(super) fn spec() -> ExecutionSpec {
    ExecutionSpec {
        group_id: Ulid::from_bytes([3; 16]),
        name: None,
        description: None,
        tags: Default::default(),
        image: "alpine".to_string(),
        entrypoint: None,
        command: Vec::new(),
        workdir: None,
        env: Default::default(),
        resources: Default::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    }
}

pub(super) fn record() -> JobRecord {
    let user = UserId::local(Ulid::from_bytes([5; 16]), realm());
    let payload = JobPayload::Execution(spec());
    JobRecord::new(job(), payload, user, node(), 1, 1, None)
}

pub(super) fn claimed() -> JobRecord {
    let mut job = record();
    job.claim = Some(JobClaim {
        holder_node_id: node(),
        claim_token: token(),
        lease_expires_ms: 1,
    });
    job
}

pub(super) fn artifact(exact_identity: bool) -> ReconcileEvidence {
    ReconcileEvidence::Unadoptable(ArtifactEvidence {
        artifact_kind: "helper".to_string(),
        backend_ref: None,
        observed_epoch: Some(EPOCH),
        observed_generation: Some(1),
        exact_identity,
        multiple: false,
        foreign: false,
    })
}

pub(super) fn prepared() -> PreparedTask {
    PreparedTask {
        inputs: Vec::new(),
        mounts: Vec::new(),
        secrets: BTreeMap::new(),
        staging: StagingMode::Files,
        workspace: None,
    }
}

pub(super) fn intent() -> AttemptIntent {
    AttemptIntent {
        attempt_no: 1,
        external_name: "attempt".to_string(),
        executor_kind: "docker".to_string(),
        pinned_image: PINNED.to_string(),
        attempt_epoch: EPOCH,
    }
}

pub(super) fn facts() -> BackendFacts {
    BackendFacts {
        kind: ExecutorKind::Docker,
        s3_mount: false,
        session: false,
        local_site: false,
        worker_location: None,
        run_as: NOBODY,
    }
}

pub(super) fn fence() -> FenceContext {
    FenceContext {
        attempt: AttemptRef::new(job().to_string(), 1),
        attempt_epoch: EPOCH,
        controller_generation: 1,
    }
}

pub(super) fn scope_of(spec: ExecutionSpec) -> Arc<AttemptScope> {
    Arc::new(AttemptScope {
        job_id: job(),
        token: token(),
        kind: ExecutorKind::Docker,
        fence: fence(),
        spec: Arc::new(spec),
        bucket: BUCKET.to_string(),
        run_as: NOBODY,
        wiring: RunWiring {
            serving: Some((node(), realm())),
            routing: NodeRouting::default(),
            encryption_key: None,
        },
    })
}

pub(super) fn scope() -> Arc<AttemptScope> {
    scope_of(spec())
}

pub(super) fn status(phase: AttemptPhase) -> AttemptStatus {
    AttemptStatus {
        phase,
        backend_ref: "attempt".to_string(),
        started_at_ms: None,
        finished_at_ms: None,
        detail: None,
    }
}

pub(super) fn control() -> AttemptControl {
    AttemptControl {
        attempt_epoch: EPOCH,
        execution_id: Ulid::from_bytes([6; 16]),
        controller_generation: 1,
        bound_token: Some(token()),
        tombstone_ref: None,
        output_commits: Vec::new(),
        output_record: None,
    }
}

pub(super) fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

pub(super) fn compute(answer: ComputeAnswer) -> IoResult {
    IoResult::Event(Box::new(Event::Compute(Box::new(answer))))
}

pub(super) fn stops(cancelled: bool) -> IoResult {
    let stops = JobStops {
        cancelled,
        shutdown: false,
    };
    IoResult::Event(Box::new(Event::Job(JobEvent::Stops(stops))))
}

pub(super) fn is_child<T, E>(next: &Result<Next<T>, E>) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}
