//! The fenced attempt one execution run owns, as its operations share it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::calls::IoCall;
use aruna_core::compute::effect::ComputeRequest;
use aruna_core::compute::{ExecutorKind, FenceContext, UserSpec};
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::effects::Effect;
use aruna_core::jobs::JobEffect;
use aruna_core::structs::execution::job::{ExecutionSpec, JobId, JobRecord};
use aruna_core::structs::storage::routing::NodeRouting;
use ulid::Ulid;

use super::compute::attempt_call;
use super::credential::RunIdentity;
use super::outputs::CaptureScope;
use super::settle::SettleScope;
use crate::jobs::records::pages::ServingNode;

/// What the node gives every execution run.
#[derive(Clone, Debug, PartialEq)]
pub struct RunWiring {
    pub serving: ServingNode,
    pub routing: NodeRouting,
    pub encryption_key: Option<CredentialEncryptionKey>,
}

/// The fenced attempt a run owns. Preparation produces it, supervision and recovery use it.
#[derive(Clone, Debug, PartialEq)]
pub struct AttemptScope {
    pub job_id: JobId,
    pub token: Ulid,
    pub kind: ExecutorKind,
    pub fence: FenceContext,
    pub spec: Arc<ExecutionSpec>,
    pub bucket: String,
    /// The identity the backend runs tasks as, which the task manifest reports.
    pub run_as: UserSpec,
    pub wiring: RunWiring,
}

impl AttemptScope {
    pub(super) fn backend(&self, request: ComputeRequest) -> IoCall {
        attempt_call(&self.kind, &self.fence, request)
    }

    pub(super) fn settle(&self, now_ms: u64) -> SettleScope {
        SettleScope {
            serving: self.wiring.serving,
            now_ms,
        }
    }

    /// Who the run acts as; `None` on a node without a network identity.
    pub(super) fn run(&self, record: &JobRecord) -> Option<RunIdentity> {
        let (node_id, realm_id) = self.wiring.serving?;
        Some(RunIdentity::new(&self.spec, record, realm_id, node_id))
    }

    pub(super) fn capture(
        &self,
        record: &Arc<JobRecord>,
        until_stop: bool,
    ) -> Option<CaptureScope> {
        Some(CaptureScope {
            kind: self.kind.clone(),
            fence: self.fence.clone(),
            spec: self.spec.clone(),
            record: record.clone(),
            run: self.run(record)?,
            routing: self.wiring.routing.clone(),
            until_stop,
        })
    }
}

/// Asks the job's host whether the job was told to stop.
pub(super) fn stops_call() -> IoCall {
    IoCall::Effect(Box::new(Effect::Job(JobEffect::Stops)))
}
