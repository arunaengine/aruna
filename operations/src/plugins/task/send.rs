//! Sends one plugin task's submission from its receiver: every send is marked durably with its
//! targets first, and only a refusal of the first send that lost no answer proves it refused.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id_mint::IdMint;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::execution::job::family::JobFamilyId;
use aruna_core::structs::execution::job::identity::RETENTION_MS;
use aruna_core::structs::execution::job::input::{ExecutionSpec, InputSource};
use aruna_core::structs::execution::job::pages::{InputPage, PageManifest, spec_entries};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_plugin_types::calls::{ErrorKind, HostError};
use ulid::Ulid;

use super::super::check::CallError;
use super::super::marks::{MARKED, is_marked};
use super::super::rows::{StoreError, decode, read_call, values};
use super::super::unavailable;
use super::change::{ChangeTask, Changed, SendOutcome, TaskChange};
use crate::jobs::lifecycle::admit::here::AcceptedSubmission;
use crate::jobs::lifecycle::screen::{ScreenError, ScreenTask};
use crate::jobs::lifecycle::submit_external::IngressNode;
use crate::jobs::lifecycle::task_job::{
    PrepareTaskJob, TaskSubmission, send_task_job, task_targets,
};
use crate::jobs::records::pages::{LoadChainOperation, PageReadError, StoreEntriesOperation};
use aruna_core::install_work::{ADMIT_WINDOW_MS, TaskRow, task_place};
use aruna_core::job_request::SubmitJobError;
use aruna_core::job_request::{self, RequestIdentity, SubmissionRequest};
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};

/// What one send proved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Sent {
    Accepted {
        alias: aruna_core::structs::execution::job::identity::JobId,
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

/// The stored spec with the inputs of its pages back in place of the paged ones.
fn expanded(request: &SubmissionRequest, pages: Vec<InputPage>) -> SendSpec {
    let entries = spec_entries(pages).map_err(|error| unavailable(error.to_string()));
    let mut spec = request.spec.clone();
    spec.inputs.retain(|input| input.source.is_inline());
    spec.inputs.extend(
        entries
            .map_err(CallError)?
            .into_iter()
            .map(|(input, _)| input),
    );
    Ok(spec)
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
    CallError(unavailable(error.detail()))
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
    /// A resend's saved input pages, read back for its task check.
    Load(Box<(RealmConfigDocument, TaskSubmission)>),
    /// A resend's task check on every input of its stored request.
    Screen(Box<(RealmConfigDocument, TaskSubmission)>),
    Mark(Box<(RealmConfigDocument, TaskSubmission)>),
    Send(JobFamilyId, u32),
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
type SendSpec = Result<ExecutionSpec, CallError>;

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
                    submission_id: job_request::submission_of(request.created_by, &request.scope),
                    request_digest: digest,
                };
                let targets = task_targets(&config, &identity, self.node.local);
                let targets = targets.map_err(prepare_error)?;
                let submission = TaskSubmission {
                    request,
                    identity,
                    targets,
                };
                self.rescreen(Box::new((config, submission)))
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

    /// A resend checks its stored request again on every input, paged ones read back here, so a
    /// grant withdrawn since the first send stops it before any holder, this node too, admits it.
    fn rescreen(&mut self, prepared: Box<(RealmConfigDocument, TaskSubmission)>) -> SendNext {
        let Some(manifest) = prepared.1.request.input_pages else {
            let spec = prepared.1.request.spec.clone();
            return self.screen(prepared, spec);
        };
        let submission = prepared.1.identity.submission_id;
        let load = LoadChainOperation::new(submission, manifest, Vec::new());
        self.phase = SendPhase::Load(prepared);
        Ok(Next::Call(IoCall::child(load)))
    }

    fn screen(
        &mut self,
        prepared: Box<(RealmConfigDocument, TaskSubmission)>,
        spec: ExecutionSpec,
    ) -> SendNext {
        let request = &prepared.1.request;
        let task = (&spec, request.created_by, request.ingress_node_id);
        let screen = ScreenTask::new(Arc::new(prepared.0.clone()), task);
        self.phase = SendPhase::Screen(prepared);
        Ok(Next::Call(IoCall::child(screen)))
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
        let versions = versions.collect();
        self.next_mark(Box::new((config, submission)), versions)
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
        self.phase = SendPhase::Send(submission.identity.family(), marked.0);
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
            SendPhase::Load(prepared) => {
                let pages = result.child::<Vec<InputPage>, PageReadError>()?;
                let pages = pages.map_err(|error| CallError(unavailable(error.to_string())))?;
                let spec = expanded(&prepared.1.request, pages)?;
                self.screen(prepared, spec)
            }
            // A refused resend goes out to no holder; the deadline later proves the outcome.
            SendPhase::Screen(prepared) => match result.child::<PluginInstall, ScreenError>()? {
                Ok(_) => {
                    let (config, submission) = *prepared;
                    self.mark(config, submission)
                }
                Err(ScreenError::Unavailable(reason)) => Err(CallError(unavailable(reason))),
                Err(refused) => Err(CallError(HostError::new(
                    ErrorKind::Denied,
                    refused.to_string(),
                ))),
            },
            SendPhase::Mark(prepared) => {
                let changed = result.child::<Changed, StoreError>()?.map_err(stored)?;
                match changed.marked {
                    Some(marked) => self.send(*prepared, marked),
                    // The work is no longer reserved: nothing is sent.
                    None => Ok(Next::Done(())),
                }
            }
            // The recording transaction decides: a resend marked meanwhile makes a refusal of
            // this attempt prove nothing.
            SendPhase::Send(family, attempt) => {
                let answer = result.child::<AcceptedSubmission, SubmitJobError>()?;
                let sent = classify(answer, family);
                let record = TaskChange::Sent { attempt, sent };
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
#[path = "send_tests.rs"]
mod tests;
