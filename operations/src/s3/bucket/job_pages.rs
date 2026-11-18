//! Runs running key re-mapping and duplicate cleanup jobs of buckets page by page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::LINKED_JOB_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::PrefixPages;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::linked_job::{LinkedJob, LinkedState, LinkedTask};
use aruna_core::structs::storage::routing::NodeRouting;
use aruna_core::types::{GroupId, Key, Value};
use tracing::warn;
use ulid::Ulid;

use super::job_progress::{JobStep, StoreProgressError, StoreProgressOperation};
use super::rename::RenameError;
use crate::auth::bearer_token::GrantValidOperation;
use crate::auth::error::{ArunaBearerError, AuthorizeError, PolicyEnforcementError};
use crate::auth::request_authorization::{AuthorizeOperation, BucketAccess, BucketAccessOperation};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::blob::migration::shared::MigrationError;
use crate::blob::unify::UnifyJobOperation;
use crate::s3::bucket::remap::{RemapJobOperation, RemapScope};

/// Delay between pages of a running job, and before a waiting job tries again.
const CONTINUE: Duration = Duration::from_secs(1);
const WAIT: Duration = Duration::from_secs(30);
const JOB_PAGE: usize = 64;

type Turn<T> = Result<Next<T>, MigrationError>;

/// Runs one page of every running linked job; outputs when the worker must run again.
#[derive(Debug, PartialEq)]
pub struct ProcessLinkedOperation {
    routing: NodeRouting,
    pages: PrefixPages,
    rows: VecDeque<(Key, Value)>,
    now_ms: u64,
    running: bool,
    next: Option<Duration>,
}

impl ProcessLinkedOperation {
    pub fn new(routing: NodeRouting) -> Self {
        let prefix = Key::from(Vec::new());
        Self {
            routing,
            pages: PrefixPages::new(LINKED_JOB_KEYSPACE, prefix, JOB_PAGE),
            rows: VecDeque::new(),
            now_ms: 0,
            running: false,
            next: None,
        }
    }

    fn wait(&mut self, after: Duration) {
        self.next = Some(self.next.map_or(after, |next| next.min(after)));
    }

    fn next_job(&mut self) -> Turn<Option<Duration>> {
        while let Some((key, value)) = self.rows.pop_front() {
            let job = LinkedJob::from_bytes(&value)?;
            if job.state != LinkedState::Running {
                continue;
            }
            let bucket = String::from_utf8(key.to_vec()).map_err(MigrationError::failed)?;
            let due = job.updated_at_ms + WAIT.as_millis() as u64;
            if job.waiting.is_some() && self.now_ms < due {
                self.wait(Duration::from_millis(due - self.now_ms));
                continue;
            }
            self.running = true;
            let page = RunPageOperation::new(self.routing.clone(), bucket, job);
            return Ok(Next::Call(IoCall::child(page)));
        }
        if self.pages.done() {
            return Ok(Next::Done(self.next));
        }
        Ok(Next::Call(self.pages.call()))
    }
}

impl Operation for ProcessLinkedOperation {
    type Output = Option<Duration>;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<Option<Duration>> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<Duration>> {
        if std::mem::take(&mut self.running) {
            let after = result.child::<Duration, MigrationError>()??;
            self.wait(after);
        } else {
            self.rows = self.pages.take(result)?.into();
        }
        self.next_job()
    }
}

#[derive(Debug, PartialEq)]
enum PageStep {
    Access,
    Page,
    Store(Duration),
}

/// One page of a job, stored unless another job replaced it meanwhile. Outputs when the job
/// goes on.
#[derive(Debug, PartialEq)]
struct RunPageOperation {
    routing: NodeRouting,
    bucket: String,
    job: LinkedJob,
    step: PageStep,
}

impl RunPageOperation {
    fn new(routing: NodeRouting, bucket: String, job: LinkedJob) -> Self {
        Self {
            routing,
            bucket,
            job,
            step: PageStep::Access,
        }
    }

    fn waiting(&self, error: impl ToString) -> JobStep {
        JobStep {
            cursor: self.job.cursor.clone(),
            waiting: Some(error.to_string()),
            ..JobStep::default()
        }
    }

    fn store(&mut self, step: JobStep, ended: Option<LinkedState>) -> Turn<Duration> {
        let after = match step.waiting.is_some() && ended.is_none() {
            true => WAIT,
            false => CONTINUE,
        };
        self.step = PageStep::Store(after);
        let store = StoreJobOperation::new(&self.bucket, &self.job, step, ended);
        Ok(Next::Call(IoCall::child(store)))
    }

    fn run_page(&mut self, access: BucketAccess) -> Turn<Duration> {
        let job = &self.job;
        let access = Arc::new(access);
        let call = match &job.task {
            LinkedTask::Remap(plan) => {
                let scope = RemapScope {
                    realm_id: job.auth.realm_id,
                    group_id: job.group_id,
                    node_id: job.node_id,
                    bucket: self.bucket.clone(),
                    access,
                };
                IoCall::child(RemapJobOperation::new(plan, &scope, job))
            }
            LinkedTask::Unify { prefix } => {
                let scope = (self.bucket.as_str(), prefix.as_str());
                IoCall::child(UnifyJobOperation::new(
                    self.routing.clone(),
                    scope,
                    access,
                    job,
                ))
            }
        };
        self.step = PageStep::Page;
        Ok(Next::Call(call))
    }

    fn page_done(&mut self, result: IoResult) -> Turn<Duration> {
        let step = match &self.job.task {
            LinkedTask::Remap(_) => (result.child::<JobStep, RenameError>()?)
                .unwrap_or_else(|error| self.waiting(error)),
            LinkedTask::Unify { .. } => (result.child::<JobStep, MigrationError>()?)
                .unwrap_or_else(|error| self.waiting(error)),
        };
        self.store(step, None)
    }
}

impl Operation for RunPageOperation {
    type Output = Duration;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<Duration> {
        let access = JobAccessOperation::new(&self.bucket, &self.job);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Turn<Duration> {
        match self.step {
            PageStep::Access => match result.child::<Option<BucketAccess>, MigrationError>()? {
                Ok(Some(access)) => self.run_page(access),
                Ok(None) => {
                    let ended = JobStep {
                        waiting: Some("the caller's token or access ended".to_string()),
                        ..JobStep::default()
                    };
                    self.store(ended, Some(LinkedState::Aborted))
                }
                Err(error) => {
                    let step = self.waiting(error);
                    self.store(step, None)
                }
            },
            PageStep::Page => self.page_done(result),
            PageStep::Store(after) => {
                result.child::<(), MigrationError>()??;
                Ok(Next::Done(after))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum AccessStep {
    Grant,
    Admin,
    Bucket,
}

/// The caller's access to the bucket for one page; `None` once its token or access ended.
#[derive(Debug, PartialEq)]
pub(crate) struct JobAccessOperation {
    bucket: String,
    job: LinkedJob,
    step: AccessStep,
}

impl JobAccessOperation {
    pub(crate) fn new(bucket: &str, job: &LinkedJob) -> Self {
        Self {
            bucket: bucket.to_string(),
            job: job.clone(),
            step: AccessStep::Grant,
        }
    }
}

impl Operation for JobAccessOperation {
    type Output = Option<BucketAccess>;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<Option<BucketAccess>> {
        let job = &self.job;
        let valid = GrantValidOperation::new(&job.grant, &job.auth.user_id);
        Ok(Next::Call(IoCall::child(valid)))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<BucketAccess>> {
        let job = &self.job;
        match self.step {
            AccessStep::Grant => {
                let valid =
                    (result.child::<bool, ArunaBearerError>()?).map_err(MigrationError::failed)?;
                if !valid {
                    return Ok(Next::Done(None));
                }
                let admin = StillAdminOperation::new(&job.auth, job.group_id);
                self.step = AccessStep::Admin;
                Ok(Next::Call(IoCall::child(admin)))
            }
            AccessStep::Admin => {
                if !result.child::<bool, MigrationError>()?? {
                    return Ok(Next::Done(None));
                }
                let scope = (job.auth.realm_id, job.group_id, job.node_id);
                let load = BucketAccessOperation::new(&job.auth, scope, &self.bucket);
                self.step = AccessStep::Bucket;
                Ok(Next::Call(IoCall::child(load)))
            }
            AccessStep::Bucket => match result.child::<BucketAccess, AuthorizeError>()? {
                Ok(access) if access.allows(None, &Permission::WRITE) => {
                    Ok(Next::Done(Some(access)))
                }
                Ok(_)
                | Err(AuthorizeError::PermissionDenied)
                | Err(AuthorizeError::Policy(PolicyEnforcementError::Denied { .. })) => {
                    Ok(Next::Done(None))
                }
                Err(error) => Err(MigrationError::failed(error)),
            },
        }
    }
}

/// Whether `auth` still administers `group_id`, as starting a bucket job required; a denial
/// is a verdict, unreadable state an error to retry.
#[derive(Debug, PartialEq)]
pub(crate) struct StillAdminOperation {
    auth: AuthContext,
    group_id: GroupId,
}

impl StillAdminOperation {
    pub(crate) fn new(auth: &AuthContext, group_id: GroupId) -> Self {
        Self {
            auth: auth.clone(),
            group_id,
        }
    }
}

impl Operation for StillAdminOperation {
    type Output = bool;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<bool> {
        let auth = &self.auth;
        let path = format!("/{}/g/{}/admin/**", auth.realm_id, self.group_id);
        let check = AuthorizeOperation::new(
            auth.realm_id,
            auth,
            &path,
            &Permission::WRITE,
            PolicyRequestExtras::rest(),
        );
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        match result.child::<(), AuthorizeError>()? {
            Ok(()) => Ok(Next::Done(true)),
            Err(AuthorizeError::PermissionDenied)
            | Err(AuthorizeError::Policy(PolicyEnforcementError::Denied { .. })) => {
                Ok(Next::Done(false))
            }
            Err(error) => Err(MigrationError::failed(error)),
        }
    }
}

/// Stores a page's progress on the job it ran for; a pause or an abort meanwhile stays.
#[derive(Debug, PartialEq)]
pub(super) struct StoreJobOperation {
    bucket: String,
    job_id: Ulid,
    step: JobStep,
    ended: Option<LinkedState>,
    now_ms: u64,
    attempts: u32,
}

impl StoreJobOperation {
    pub(super) fn new(
        bucket: &str,
        job: &LinkedJob,
        step: JobStep,
        ended: Option<LinkedState>,
    ) -> Self {
        Self {
            bucket: bucket.to_string(),
            job_id: job.id,
            step,
            ended,
            now_ms: 0,
            attempts: 0,
        }
    }

    fn attempt(&mut self) -> Turn<()> {
        self.attempts += 1;
        let store = StoreProgressOperation::new(
            &self.bucket,
            self.job_id,
            self.step.clone(),
            self.ended,
            self.now_ms,
        );
        Ok(Next::Call(IoCall::child(store)))
    }
}

impl Operation for StoreJobOperation {
    type Output = ();
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<()> {
        self.attempt()
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        match result.child::<(), StoreProgressError>()? {
            Ok(()) => Ok(Next::Done(())),
            Err(StoreProgressError::Conflict)
                if self.attempts <= StoreProgressOperation::conflict_retries() =>
            {
                self.attempt()
            }
            Err(StoreProgressError::Conflict) => {
                warn!(bucket = %self.bucket, "The progress of a bucket job could not be stored");
                Ok(Next::Done(()))
            }
            Err(error) => Err(MigrationError::failed(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::realm::RealmId;

    fn empty_page() -> IoResult {
        IoResult::Page {
            rows: Vec::new(),
            next: None,
        }
    }

    fn auth() -> AuthContext {
        let realm_id = RealmId::from_bytes([1; 32]);
        AuthContext {
            user_id: UserId::local(Ulid::from_bytes([2; 16]), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    fn stored(outcome: Result<(), StoreProgressError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(outcome))
    }

    fn store_step() -> StoreJobOperation {
        StoreJobOperation {
            bucket: "bucket".to_string(),
            job_id: Ulid::from_bytes([3; 16]),
            step: JobStep::default(),
            ended: None,
            now_ms: 0,
            attempts: 0,
        }
    }

    #[test]
    fn no_jobs_ends() {
        let mut process = ProcessLinkedOperation::new(NodeRouting::default());
        assert!(matches!(
            process.first(),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        assert_eq!(process.next(empty_page()), Ok(Next::Done(None)));
    }

    #[test]
    fn denial_is_verdict() {
        let mut admin = StillAdminOperation::new(&auth(), Ulid::from_bytes([4; 16]));
        assert!(matches!(admin.first(), Ok(Next::Call(IoCall::Child(_)))));
        let denied: Result<(), AuthorizeError> = Err(AuthorizeError::PermissionDenied);
        let answer = IoResult::Child(ChildOutcome::new(denied));
        assert_eq!(admin.next(answer), Ok(Next::Done(false)));
    }

    #[test]
    fn conflict_retries_then_warns() {
        let mut store = store_step();
        store.first().unwrap();
        for _ in 0..StoreProgressOperation::conflict_retries() {
            let turn = store.next(stored(Err(StoreProgressError::Conflict)));
            assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        }
        let last = store.next(stored(Err(StoreProgressError::Conflict)));
        assert_eq!(last, Ok(Next::Done(())));
    }
}
