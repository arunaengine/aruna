//! Sends one plugin task's submission from its receiver: every send is marked durably with its
//! targets first, and only a refusal of the first send that lost no answer proves it refused.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id_mint::IdMint;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::execution::job::{
    ExecutionSpec, InputSource, JobFamilyId, PageManifest, RETENTION_MS,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_plugin_types::calls::{ErrorKind, HostError};
use ulid::Ulid;

use super::check::CallError;
use super::marks::{MARKED, is_marked};
use super::rows::{StoreError, decode, read_call, values};
use super::task_change::{ChangeTask, Changed, SendOutcome, TaskChange};
use super::task_row::{ADMIT_WINDOW_MS, TaskRow, task_place};
use super::unavailable;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::lifecycle::admit_here::AcceptedSubmission;
use crate::jobs::lifecycle::ids::{self, RequestIdentity};
use crate::jobs::lifecycle::submit_external::IngressNode;
use crate::jobs::lifecycle::task_job::{
    PrepareTaskJob, TaskSubmission, send_task_job, task_targets,
};
use crate::jobs::records::pages::{PageReadError, StoreEntriesOperation};
use crate::jobs::submit::SubmitJobError;

/// What one send proved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Sent {
    Accepted {
        alias: aruna_core::structs::execution::job::JobId,
        family: JobFamilyId,
    },
    /// A holder, or this node, read the request and committed nothing.
    Refused(String),
    /// A holder may have committed it; the answer did not say.
    Uncertain(String),
}

/// Sorts a submission answer: only a holder's reading of the request is a refusal.
pub fn classify(answer: Result<AcceptedSubmission, SubmitJobError>, family: JobFamilyId) -> Sent {
    match answer {
        Ok(accepted) => Sent::Accepted {
            alias: accepted.job_id,
            family,
        },
        Err(
            error @ (SubmitJobError::InvalidWorkspace(_)
            | SubmitJobError::QuotaDenied(_)
            | SubmitJobError::AuthorityDenied
            | SubmitJobError::InputDenied(_)),
        ) => Sent::Refused(error.to_string()),
        Err(error) => Sent::Uncertain(error.to_string()),
    }
}

/// A refusal proves the task was not admitted only for the first send of a submission that
/// never lost an answer before.
pub fn decide(attempt: u32, uncertain: bool, sent: Sent) -> SendOutcome {
    match sent {
        Sent::Accepted { alias, family } => SendOutcome::Dispatch { alias, family },
        Sent::Refused(reason) if attempt == 1 && !uncertain => SendOutcome::Fail(reason),
        Sent::Refused(_) | Sent::Uncertain(_) => SendOutcome::Keep,
    }
}

/// Errors before any send: nothing reached a holder, so the call may be made again.
fn prepare_error(error: SubmitJobError) -> CallError {
    CallError(match error {
        SubmitJobError::InvalidWorkspace(reason) => HostError::new(ErrorKind::Invalid, reason),
        SubmitJobError::InputDenied(reason) => HostError::new(ErrorKind::Denied, reason),
        other => unavailable(other.to_string()),
    })
}

fn stored(error: StoreError) -> CallError {
    CallError(unavailable(error.0))
}

#[derive(Debug, PartialEq)]
enum SendPhase {
    Task,
    Config(Box<TaskRow>),
    Prepare(Box<RealmConfigDocument>),
    Marks(
        Box<(RealmConfigDocument, TaskSubmission)>,
        VecDeque<(String, String, Ulid)>,
    ),
    Pages(Box<(RealmConfigDocument, TaskSubmission)>),
    Mark(Box<(RealmConfigDocument, TaskSubmission)>),
    Send(JobFamilyId, (u32, bool)),
    Record,
}

/// Sends the reserved task `work_id` once. The first send prepares its spec, refuses marked
/// inputs and pages; every later one sends the stored request. Each send is marked with its
/// targets first, goes to exactly those, and records what the answer proves.
#[derive(Debug, PartialEq)]
pub struct SendTask {
    install: PluginInstall,
    work_id: Ulid,
    spec: Option<ExecutionSpec>,
    node: IngressNode,
    admit_by: u64,
    phase: SendPhase,
    mint: IdMint,
}

type SendNext = Result<Next<()>, CallError>;

impl SendTask {
    /// A first send carries the checked `spec`; a resend sends the stored request.
    pub fn new(
        install: &PluginInstall,
        work_id: Ulid,
        spec: Option<ExecutionSpec>,
        node: IngressNode,
    ) -> Self {
        Self {
            install: install.clone(),
            work_id,
            spec,
            node,
            admit_by: 0,
            phase: SendPhase::Task,
            mint: IdMint::default(),
        }
    }

    fn ids(&self) -> (Ulid, Ulid) {
        (self.install.install_id, self.work_id)
    }

    /// The stored request with its stored digest, or a fresh preparation of the first send.
    fn configured(&mut self, task: TaskRow, config: RealmConfigDocument) -> SendNext {
        match (task.request, task.digest, self.spec.take()) {
            (Some(request), Some(digest), _) => {
                let identity = RequestIdentity {
                    submission_id: ids::submission_of(request.created_by, &request.scope),
                    request_digest: digest,
                };
                let targets = task_targets(&config, &identity, self.node.local);
                let targets = targets.map_err(prepare_error)?;
                let submission = TaskSubmission {
                    request,
                    identity,
                    targets,
                };
                self.mark(config, submission)
            }
            (None, _, Some(spec)) => {
                let key = format!("plugin-work/{}", self.work_id);
                let caller = (self.install.account, key);
                let bounds = (RETENTION_MS, self.admit_by);
                let prepare = PrepareTaskJob::new(spec, caller, bounds, self.node.clone());
                self.phase = SendPhase::Prepare(Box::new(config));
                Ok(Next::Call(IoCall::child(prepare)))
            }
            // A resend without a stored request has nothing to send.
            _ => Ok(Next::Done(())),
        }
    }

    /// Every resolved input version on this node, link targets and expanded entries included, is
    /// refused when a job with a secret wrote it.
    fn check_marks(&mut self, config: RealmConfigDocument, submission: TaskSubmission) -> SendNext {
        let request = &submission.request;
        let versions = (request.spec.inputs.iter()).filter_map(|input| {
            let InputSource::S3 { bucket, key, .. } = &input.source else {
                return None;
            };
            let captured = (request.captured_inputs.iter())
                .find(|captured| captured.destination_key == input.dest_key)?;
            Some((bucket.clone(), key.clone(), captured.version_id))
        });
        self.next_mark(Box::new((config, submission)), versions.collect())
    }

    fn next_mark(
        &mut self,
        prepared: Box<(RealmConfigDocument, TaskSubmission)>,
        mut versions: VecDeque<(String, String, Ulid)>,
    ) -> SendNext {
        let Some((bucket, key, version)) = versions.pop_front() else {
            return self.page(prepared);
        };
        let check = is_marked((&bucket, &key, version)).map_err(stored)?;
        self.phase = SendPhase::Marks(prepared, versions);
        Ok(Next::Call(IoCall::child(check)))
    }

    /// Moves inputs too large for the spec record into pages kept here, after the checks saw
    /// the complete lists.
    fn page(&mut self, prepared: Box<(RealmConfigDocument, TaskSubmission)>) -> SendNext {
        let request = &prepared.1.request;
        let submission = prepared.1.identity.submission_id;
        let store = StoreEntriesOperation::request(
            submission,
            &request.spec.inputs,
            &request.captured_inputs,
        );
        self.phase = SendPhase::Pages(prepared);
        Ok(Next::Call(IoCall::child(store)))
    }

    fn paged(
        &mut self,
        mut prepared: Box<(RealmConfigDocument, TaskSubmission)>,
        manifest: Option<PageManifest>,
    ) -> SendNext {
        let request = &mut prepared.1.request;
        if manifest.is_some() {
            // Inline inputs carry no capture, so they stay in the spec beside the pages.
            request.spec.inputs.retain(|input| input.source.is_inline());
            request.captured_inputs.clear();
        }
        request.input_pages = manifest;
        let (config, submission) = *prepared;
        self.mark(config, submission)
    }

    /// Marks the send with its targets before it happens.
    fn mark(&mut self, config: RealmConfigDocument, submission: TaskSubmission) -> SendNext {
        let change = TaskChange::Mark {
            targets: submission.targets.clone(),
            admit_by: self.admit_by,
            request: Box::new(submission.request.clone()),
            digest: submission.identity.request_digest,
        };
        let mark = ChangeTask::new(self.ids(), change, self.node.now_ms);
        self.phase = SendPhase::Mark(Box::new((config, submission)));
        Ok(Next::Call(IoCall::child(mark)))
    }

    /// Sends to exactly the marked targets, as the install account vouched for by this node.
    fn send(
        &mut self,
        prepared: (RealmConfigDocument, TaskSubmission),
        marked: (u32, bool),
    ) -> SendNext {
        let (config, submission) = prepared;
        let account = self.install.account;
        let auth = AuthContext {
            user_id: account,
            realm_id: account.realm_id,
            path_restrictions: None,
            session: None,
        };
        let token = AuthToken::Internal(auth);
        let send = send_task_job(&submission, (&config, &self.node), token, &mut self.mint);
        let send = send.map_err(prepare_error)?;
        self.phase = SendPhase::Send(submission.identity.family(), marked);
        Ok(Next::Call(send))
    }
}

impl Operation for SendTask {
    type Output = ();
    type Error = CallError;

    fn seed(&mut self, entropy: u128) {
        self.mint = IdMint::new(entropy);
    }

    fn first(&mut self) -> SendNext {
        let (install_id, work_id) = self.ids();
        Ok(Next::Call(read_call(vec![task_place(install_id, work_id)])))
    }

    fn next(&mut self, result: IoResult) -> SendNext {
        match std::mem::replace(&mut self.phase, SendPhase::Record) {
            SendPhase::Task => {
                let [task] = values::<1>(result).map_err(stored)?;
                let task = task.map(|bytes| decode::<TaskRow>(&bytes)).transpose();
                let task = task.map_err(stored)?.unwrap_or_default();
                self.admit_by = match task.request {
                    Some(_) => task.admit_by,
                    None => self.node.now_ms.saturating_add(ADMIT_WINDOW_MS),
                };
                // Past the deadline nothing is sent; the settle pass proves the outcome.
                if self.node.now_ms > self.admit_by {
                    return Ok(Next::Done(()));
                }
                self.phase = SendPhase::Config(Box::new(task));
                let read = GetConfigOperation::new(self.node.realm_id);
                Ok(Next::Call(IoCall::child(read)))
            }
            SendPhase::Config(task) => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|error| CallError(unavailable(error.to_string())))?;
                self.configured(*task, config)
            }
            SendPhase::Prepare(config) => {
                let submission = result.child::<TaskSubmission, SubmitJobError>()?;
                self.check_marks(*config, submission.map_err(prepare_error)?)
            }
            SendPhase::Marks(prepared, versions) => {
                if result.child::<bool, StoreError>()?.map_err(stored)? {
                    let marked = HostError::new(ErrorKind::Denied, MARKED);
                    return Err(CallError(marked));
                }
                self.next_mark(prepared, versions)
            }
            SendPhase::Pages(prepared) => {
                let manifest = result.child::<Option<PageManifest>, PageReadError>()?;
                let manifest =
                    manifest.map_err(|error| CallError(unavailable(error.to_string())))?;
                self.paged(prepared, manifest)
            }
            SendPhase::Mark(prepared) => {
                let changed = result.child::<Changed, StoreError>()?.map_err(stored)?;
                match changed.marked {
                    Some(marked) => self.send(*prepared, marked),
                    // The work is no longer reserved: nothing is sent.
                    None => Ok(Next::Done(())),
                }
            }
            SendPhase::Send(family, (attempt, uncertain)) => {
                let answer = result.child::<AcceptedSubmission, SubmitJobError>()?;
                let outcome = decide(attempt, uncertain, classify(answer, family));
                let record = TaskChange::Record(outcome);
                let record = ChangeTask::new(self.ids(), record, self.node.now_ms);
                Ok(Next::Call(IoCall::child(record)))
            }
            SendPhase::Record => {
                result.child::<Changed, StoreError>()?.map_err(stored)?;
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
#[path = "task_send_tests.rs"]
mod tests;
