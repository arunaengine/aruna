//! Waits on bucket keys of another node over the job-control transport, and the wakes back.
//! Registration and wake grant no read: the staging read still passes the key node's admission.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::AuthorizationError;
use aruna_core::events::Event;
use aruna_core::jobs::{JobRequest, JobResponse, WaitTarget};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::rows::{read_row, value};
use aruna_core::structs::execution::job::{JobId, JobRecord, JobState, KeyWait};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{
    BucketInfo, HashIndex, VersionKey, object_permission_path,
};
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::Key;
use tracing::warn;
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::jobs::key_locks::{
    AliasPage, AliasPageOperation, FindAliasOperation, KeyCheckError, KeyUnlockedOperation,
    MAX_AWAITED, VersionWaitOperation,
};
use crate::jobs::records::pages::ServingNode;
use crate::jobs::route::{JobRouteError, JobRouteOperation, JobRouteOutcome};
use crate::jobs::store::JobMutationError;
use crate::jobs::store::remote_wait::{
    OwedPage, OwedWake, OwedWakesOperation, QueueWakesOperation, WaitRowOperation,
};
use crate::jobs::store::row_calls::{read_job_record, read_key_waits, satisfy_key_wait};

fn stored(error: JobMutationError) -> KeyCheckError {
    KeyCheckError(error.to_string())
}

/// Asks whether `auth` may READ one object here, within its path restrictions.
fn read_check(
    auth: &AuthContext,
    (node_id, group_id): (NodeId, Ulid),
    bucket: &str,
    key: &str,
) -> IoCall {
    let path = object_permission_path(auth.realm_id, group_id, node_id, bucket, key);
    IoCall::child(CheckPermissionsOperation::new(CheckPermissionsConfig {
        auth_context: auth.clone(),
        path,
        required_permission: Permission::READ,
    }))
}

/// A check that could not be answered grants nothing.
fn read_allowed(result: IoResult) -> Result<bool, IoError> {
    Ok(result.child::<bool, AuthorizationError>()?.unwrap_or(false))
}

fn answered_wait(result: IoResult) -> Result<Option<KeyWait>, KeyCheckError> {
    result.child::<Option<KeyWait>, KeyCheckError>()?
}

#[derive(Debug, PartialEq)]
enum TargetStep {
    Bucket,
    ObjectCheck(Ulid),
    Alias,
    AliasCheck(Box<HashIndex>),
    Wait,
    Page,
    PageCheck(Box<HashIndex>),
    PageWait,
}

/// Locked keys of one target the caller may READ here. An exact version is judged alone, so
/// another readable alias cannot hide it; a bare content waits only while none is readable.
#[derive(Debug, PartialEq)]
pub struct TargetLocksOperation {
    node_id: NodeId,
    auth: AuthContext,
    target: WaitTarget,
    step: TargetStep,
    /// Aliases of the current page still to judge, the next one last.
    aliases: Vec<HashIndex>,
    next_page: Option<Key>,
    locked: Vec<KeyWait>,
}

type TargetNext = Result<Next<Vec<KeyWait>>, KeyCheckError>;

impl TargetLocksOperation {
    pub fn new(node_id: NodeId, auth: &AuthContext, target: WaitTarget) -> Self {
        let step = match &target {
            WaitTarget::Object { .. } => TargetStep::Bucket,
            WaitTarget::Captured { .. } => TargetStep::Alias,
            WaitTarget::Content { .. } => TargetStep::Page,
        };
        Self {
            node_id,
            auth: auth.clone(),
            target,
            step,
            aliases: Vec::new(),
            next_page: None,
            locked: Vec::new(),
        }
    }

    fn alias_check(&self, alias: &HashIndex) -> IoCall {
        let place = (self.node_id, alias.group_id);
        read_check(&self.auth, place, &alias.bucket, &alias.key)
    }

    fn page_call(&self, blake3: [u8; 32], after: Option<Key>) -> IoCall {
        IoCall::child(AliasPageOperation::new(blake3, self.node_id, after))
    }

    /// The next alias of this realm, the next page, or the end of the content.
    fn next_alias(&mut self) -> TargetNext {
        while let Some(alias) = self.aliases.pop() {
            if alias.realm_id == self.auth.realm_id {
                let check = self.alias_check(&alias);
                self.step = TargetStep::PageCheck(Box::new(alias));
                return Ok(Next::Call(check));
            }
        }
        let WaitTarget::Content { blake3 } = self.target else {
            return Ok(Next::Done(std::mem::take(&mut self.locked)));
        };
        match self.next_page.take() {
            Some(after) => {
                self.step = TargetStep::Page;
                Ok(Next::Call(self.page_call(blake3, Some(after))))
            }
            None => Ok(Next::Done(std::mem::take(&mut self.locked))),
        }
    }

    fn bucket_read(&mut self, result: IoResult) -> TargetNext {
        let Some(info) = value(result)? else {
            return Ok(Next::Done(Vec::new()));
        };
        let group_id = BucketInfo::from_bytes(&info)
            .map_err(|error| KeyCheckError(error.to_string()))?
            .group_id;
        let WaitTarget::Object { bucket, key, .. } = &self.target else {
            return Err(KeyCheckError("target is no object".to_string()));
        };
        let check = read_check(&self.auth, (self.node_id, group_id), bucket, key);
        self.step = TargetStep::ObjectCheck(group_id);
        Ok(Next::Call(check))
    }

    fn object_wait(&mut self, group_id: Ulid) -> TargetNext {
        let WaitTarget::Object {
            bucket,
            key,
            version_id,
        } = &self.target
        else {
            return Err(KeyCheckError("target is no object".to_string()));
        };
        let version = VersionKey::new(bucket, key, *version_id);
        self.step = TargetStep::Wait;
        let wait = VersionWaitOperation::new((self.node_id, group_id), version);
        Ok(Next::Call(IoCall::child(wait)))
    }
}

impl Operation for TargetLocksOperation {
    type Output = Vec<KeyWait>;
    type Error = KeyCheckError;

    fn first(&mut self) -> TargetNext {
        Ok(Next::Call(match &self.target {
            WaitTarget::Object { bucket, .. } => {
                read_row(S3_BUCKET_KEYSPACE, bucket.as_bytes().to_vec())
            }
            WaitTarget::Captured { blake3, version_id } => {
                IoCall::child(FindAliasOperation::new(*blake3, *version_id, self.node_id))
            }
            WaitTarget::Content { blake3 } => self.page_call(*blake3, None),
        }))
    }

    fn next(&mut self, result: IoResult) -> TargetNext {
        match std::mem::replace(&mut self.step, TargetStep::Wait) {
            TargetStep::Bucket => self.bucket_read(result),
            TargetStep::ObjectCheck(group_id) => match read_allowed(result)? {
                true => self.object_wait(group_id),
                false => Ok(Next::Done(Vec::new())),
            },
            TargetStep::Alias => {
                let alias = result.child::<Option<HashIndex>, KeyCheckError>()??;
                let realm_id = self.auth.realm_id;
                let Some(alias) = alias.filter(|alias| alias.realm_id == realm_id) else {
                    return Ok(Next::Done(Vec::new()));
                };
                let check = self.alias_check(&alias);
                self.step = TargetStep::AliasCheck(Box::new(alias));
                Ok(Next::Call(check))
            }
            TargetStep::AliasCheck(alias) => {
                if !read_allowed(result)? {
                    return Ok(Next::Done(Vec::new()));
                }
                let wait = VersionWaitOperation::of_alias(&alias);
                Ok(Next::Call(IoCall::child(wait)))
            }
            TargetStep::Wait => Ok(Next::Done(answered_wait(result)?.into_iter().collect())),
            TargetStep::Page => {
                let (mut aliases, next) = result.child::<AliasPage, KeyCheckError>()??;
                aliases.reverse();
                self.aliases = aliases;
                self.next_page = next;
                self.next_alias()
            }
            TargetStep::PageCheck(alias) => {
                if !read_allowed(result)? {
                    return self.next_alias();
                }
                self.step = TargetStep::PageWait;
                let wait = VersionWaitOperation::of_alias(&alias);
                Ok(Next::Call(IoCall::child(wait)))
            }
            TargetStep::PageWait => match answered_wait(result)? {
                Some(wait) => {
                    self.locked.push(wait);
                    self.next_alias()
                }
                None => Ok(Next::Done(Vec::new())),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
enum RegisterStep {
    Targets,
    Rows,
    Unlocked(KeyWait),
}

/// Key node: registers `job_id` of `peer` on the locked keys of `targets`, then answers only
/// the keys still locked. A key unlocked in between is either seen here or wakes the job later.
#[derive(Debug, PartialEq)]
pub struct RegisterWaitsOperation {
    node_id: NodeId,
    peer: NodeId,
    auth: AuthContext,
    job_id: JobId,
    /// Targets still to judge, the next one last.
    targets: Vec<WaitTarget>,
    /// Keys found so far; while registering and checking, those still to handle.
    waits: Vec<KeyWait>,
    registered: usize,
    locked: Vec<KeyWait>,
    step: RegisterStep,
}

type RegisterNext = Result<Next<Vec<KeyWait>>, KeyCheckError>;

impl RegisterWaitsOperation {
    pub fn new(
        (node_id, peer): (NodeId, NodeId),
        auth: &AuthContext,
        job_id: JobId,
        targets: &[WaitTarget],
    ) -> Self {
        let mut targets = targets.to_vec();
        targets.reverse();
        Self {
            node_id,
            peer,
            auth: auth.clone(),
            job_id,
            targets,
            waits: Vec::new(),
            registered: 0,
            locked: Vec::new(),
            step: RegisterStep::Targets,
        }
    }

    fn next_target(&mut self) -> RegisterNext {
        match self.targets.pop() {
            Some(target) => {
                let locks = TargetLocksOperation::new(self.node_id, &self.auth, target);
                Ok(Next::Call(IoCall::child(locks)))
            }
            None => self.next_row(),
        }
    }

    fn next_row(&mut self) -> RegisterNext {
        let Some(wait) = self.waits.get(self.registered) else {
            self.waits.reverse();
            return self.next_check();
        };
        self.step = RegisterStep::Rows;
        self.registered += 1;
        let row = WaitRowOperation::register(wait.key, self.peer, self.job_id, &self.auth);
        Ok(Next::Call(IoCall::child(row)))
    }

    fn next_check(&mut self) -> RegisterNext {
        let Some(wait) = self.waits.pop() else {
            return Ok(Next::Done(std::mem::take(&mut self.locked)));
        };
        let check = KeyUnlockedOperation::new(wait.key);
        self.step = RegisterStep::Unlocked(wait);
        Ok(Next::Call(IoCall::child(check)))
    }
}

impl Operation for RegisterWaitsOperation {
    type Output = Vec<KeyWait>;
    type Error = KeyCheckError;

    fn first(&mut self) -> RegisterNext {
        if self.targets.len() > MAX_AWAITED {
            return Err(KeyCheckError(format!(
                "at most {MAX_AWAITED} targets per key wait"
            )));
        }
        self.next_target()
    }

    fn next(&mut self, result: IoResult) -> RegisterNext {
        match std::mem::replace(&mut self.step, RegisterStep::Targets) {
            RegisterStep::Targets => {
                for wait in result.child::<Vec<KeyWait>, KeyCheckError>()?? {
                    if !self.waits.contains(&wait) {
                        self.waits.push(wait);
                    }
                }
                self.next_target()
            }
            RegisterStep::Rows => {
                result.child::<(), JobMutationError>()?.map_err(stored)?;
                self.next_row()
            }
            RegisterStep::Unlocked(wait) => {
                if !result.child::<bool, IoError>()?.unwrap_or(false) {
                    self.locked.push(wait);
                }
                self.next_check()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum WakeStep {
    Record,
    Waits,
    Satisfy,
}

/// Waiting node: applies a wake from `peer`. False while the job may still be parking, so the
/// key node delivers again; any other state acknowledges, as the wake cannot matter any more.
#[derive(Debug, PartialEq)]
pub struct AcceptWakeOperation {
    peer: NodeId,
    user_id: UserId,
    job_id: JobId,
    key: BucketKeyRef,
    now_ms: u64,
    step: WakeStep,
}

impl AcceptWakeOperation {
    pub fn new(
        peer: NodeId,
        user_id: UserId,
        job_id: JobId,
        key: BucketKeyRef,
        now_ms: u64,
    ) -> Self {
        Self {
            peer,
            user_id,
            job_id,
            key,
            now_ms,
            step: WakeStep::Record,
        }
    }

    fn judged(&mut self, record: Option<JobRecord>) -> Result<Next<bool>, KeyCheckError> {
        let Some(record) = record.filter(|record| record.created_by == self.user_id) else {
            return Ok(Next::Done(true));
        };
        match record.state {
            JobState::Claimed | JobState::Preparing => Ok(Next::Done(false)),
            JobState::AwaitingKey => {
                self.step = WakeStep::Waits;
                Ok(Next::Call(IoCall::child(read_key_waits(self.job_id))))
            }
            _ => Ok(Next::Done(true)),
        }
    }
}

impl Operation for AcceptWakeOperation {
    type Output = bool;
    type Error = KeyCheckError;

    fn first(&mut self) -> Result<Next<bool>, KeyCheckError> {
        Ok(Next::Call(IoCall::child(read_job_record(self.job_id))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, KeyCheckError> {
        match self.step {
            WakeStep::Record => {
                let record = result
                    .child::<Option<JobRecord>, JobMutationError>()?
                    .map_err(stored)?;
                self.judged(record)
            }
            WakeStep::Waits => {
                let waits = result
                    .child::<Vec<KeyWait>, JobMutationError>()?
                    .map_err(stored)?;
                let (peer, key) = (self.peer, self.key);
                if !waits
                    .iter()
                    .any(|wait| wait.node_id == peer && wait.key == key)
                {
                    return Ok(Next::Done(true));
                }
                self.step = WakeStep::Satisfy;
                let satisfy = satisfy_key_wait(self.job_id, key, self.now_ms);
                Ok(Next::Call(IoCall::child(satisfy)))
            }
            WakeStep::Satisfy => {
                result.child::<bool, JobMutationError>()?.map_err(stored)?;
                Ok(Next::Done(true))
            }
        }
    }
}

/// Key node: turns the registrations on `key` into owed wakes, in bounded pages, and arms the
/// persisted delivery timer, so owed wakes are tried even on an idle node. The timer is armed
/// also when queueing failed, since earlier pages may already owe wakes.
#[derive(Debug, PartialEq)]
pub struct RemoteWakesOperation {
    key: BucketKeyRef,
    failure: Option<JobMutationError>,
    arming: bool,
}

impl RemoteWakesOperation {
    pub fn new(key: BucketKeyRef) -> Self {
        Self {
            key,
            failure: None,
            arming: false,
        }
    }

    fn queue_page(&self) -> Next<()> {
        let key = self.key;
        let build = move || QueueWakesOperation::new(key);
        let child = boxed_retry(build, &key.key(), |result| {
            Event::Child(ChildOutcome::new(result))
        });
        Next::Call(IoCall::Child(child))
    }
}

impl Operation for RemoteWakesOperation {
    type Output = ();
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<()>, JobMutationError> {
        Ok(self.queue_page())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, JobMutationError> {
        if self.arming {
            if let IoResult::Event(event) = &result
                && let Event::Task(TaskEvent::Error { message, .. }) = event.as_ref()
            {
                warn!(%message, "Failed to schedule key wake delivery");
            }
            return match self.failure.take() {
                Some(error) => Err(error),
                None => Ok(Next::Done(())),
            };
        }
        match result.child::<bool, JobMutationError>()? {
            Ok(true) => return Ok(self.queue_page()),
            Ok(false) => {}
            Err(error) => self.failure = Some(error),
        }
        self.arming = true;
        let timer = Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::DeliverKeyWakes,
            after: Duration::ZERO,
        });
        Ok(Next::Call(IoCall::Effect(Box::new(timer))))
    }
}

/// Wait before owed wakes are tried again once a full round left some unacknowledged.
pub const WAKE_RETRY: Duration = Duration::from_secs(30);

/// What one delivery pass left behind.
#[derive(Debug, Default, PartialEq)]
pub struct Delivery {
    pub acked: usize,
    /// Some wakes stay owed, so the delivery timer runs again.
    pub owed: bool,
    /// Where the next pass starts, so unreachable waiters never hide the rows after them.
    pub next: Option<Key>,
}

impl Delivery {
    /// The owed wakes could not be read, so a later pass starts over.
    pub fn unread() -> Self {
        Self {
            acked: 0,
            owed: true,
            next: None,
        }
    }
}

#[derive(Debug, PartialEq)]
enum DeliverStep {
    Owed,
    Sent(Box<OwedWake>),
    Dropped(JobId),
}

/// Key node: sends one page of owed wakes after `start_after` and drops each acknowledged one.
#[derive(Debug, PartialEq)]
pub struct DeliverWakesOperation {
    start_after: Option<Key>,
    serving: ServingNode,
    resumed: bool,
    /// Wakes of this page still to send, the next one last.
    pending: Vec<OwedWake>,
    next_page: Option<Key>,
    acked: usize,
    left: bool,
    step: DeliverStep,
}

type DeliverNext = Result<Next<Delivery>, IoError>;

impl DeliverWakesOperation {
    pub fn new(start_after: Option<Key>, serving: ServingNode) -> Self {
        Self {
            resumed: start_after.is_some(),
            start_after,
            serving,
            pending: Vec::new(),
            next_page: None,
            acked: 0,
            left: false,
            step: DeliverStep::Owed,
        }
    }

    fn send_next(&mut self) -> DeliverNext {
        let Some((local, realm_id)) = self.serving else {
            self.left |= !self.pending.is_empty();
            self.pending.clear();
            return Ok(self.finished());
        };
        let Some(wake) = self.pending.pop() else {
            return Ok(self.finished());
        };
        let request = JobRequest::KeyWake {
            auth_token: AuthToken::Internal(wake.auth.clone()),
            job_id: wake.job_id,
            key: wake.key,
        };
        let route = JobRouteOperation::new(realm_id, local, wake.job_id, Some(request))
            .with_responder(Some(wake.waiter));
        self.step = DeliverStep::Sent(Box::new(wake));
        Ok(Next::Call(IoCall::child(route)))
    }

    /// A pass that started mid-way must wrap around before it may report nothing owed.
    fn finished(&mut self) -> Next<Delivery> {
        let next = self.next_page.take();
        Next::Done(Delivery {
            acked: self.acked,
            owed: self.left || self.resumed || next.is_some(),
            next,
        })
    }
}

impl Operation for DeliverWakesOperation {
    type Output = Delivery;
    type Error = IoError;

    fn first(&mut self) -> DeliverNext {
        let owed = OwedWakesOperation::new(self.start_after.take());
        Ok(Next::Call(IoCall::child(owed)))
    }

    fn next(&mut self, result: IoResult) -> DeliverNext {
        match std::mem::replace(&mut self.step, DeliverStep::Owed) {
            DeliverStep::Owed => match result.child::<OwedPage, JobMutationError>()? {
                Ok((mut owed, next)) => {
                    owed.reverse();
                    self.pending = owed;
                    self.next_page = next;
                    self.send_next()
                }
                Err(error) => {
                    warn!(%error, "Owed key wakes could not be read");
                    Ok(Next::Done(Delivery::unread()))
                }
            },
            DeliverStep::Sent(wake) => {
                let answer = result.child::<JobRouteOutcome, JobRouteError>()?;
                let acked = match answer {
                    Ok(outcome) => outcome == JobRouteOutcome::Remote(JobResponse::KeyWakeAcked),
                    Err(error) => {
                        let (job_id, node) = (wake.job_id, wake.waiter);
                        warn!(job_id = %job_id, node = %node, %error, "Key wait request failed");
                        false
                    }
                };
                if !acked {
                    self.left = true;
                    return self.send_next();
                }
                self.step = DeliverStep::Dropped(wake.job_id);
                let drop_row = WaitRowOperation::acknowledge(wake.waiter, wake.job_id, wake.key);
                Ok(Next::Call(IoCall::child(drop_row)))
            }
            DeliverStep::Dropped(job_id) => {
                match result.child::<(), JobMutationError>()? {
                    Ok(()) => self.acked += 1,
                    Err(error) => {
                        warn!(job_id = %job_id, %error, "Acknowledged wake not dropped");
                        self.left = true;
                    }
                }
                self.send_next()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::jobs::store::key_wait::park_job;
    use crate::jobs::store::query::insert_job;
    use crate::jobs::store::state::{ClaimOutcome, claim_job, read_job_record};
    use crate::storage_read::iter_prefix_page;
    use aruna_core::UserId;
    use aruna_core::keyspaces::PATHS_INDEX_KEYSPACE;
    use aruna_core::structs::execution::job::JobPayload;
    use aruna_core::structs::execution::job::JobRecord;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::HashIndex;
    use aruna_core::time::unix_timestamp_millis;
    use std::future::Future;
    use ulid::Ulid;

    async fn accept_wake(
        context: &DriverContext,
        peer: NodeId,
        auth: &AuthContext,
        job_id: JobId,
        key: BucketKeyRef,
    ) -> Result<bool, KeyCheckError> {
        let now_ms = unix_timestamp_millis();
        let accept = AcceptWakeOperation::new(peer, auth.user_id, job_id, key, now_ms);
        drive(accept, context).await
    }

    async fn register_waits(
        context: &DriverContext,
        nodes: (NodeId, NodeId),
        auth: &AuthContext,
        job_id: JobId,
        targets: &[WaitTarget],
    ) -> Result<Vec<KeyWait>, KeyCheckError> {
        let register = RegisterWaitsOperation::new(nodes, auth, job_id, targets);
        drive(register, context).await
    }

    /// One delivery pass with `send` as the transport; true from `send` means acknowledged.
    async fn deliver_with<F, Fut>(
        context: &DriverContext,
        start_after: Option<Key>,
        mut send: F,
    ) -> Delivery
    where
        F: FnMut(OwedWake) -> Fut,
        Fut: Future<Output = bool>,
    {
        let resumed = start_after.is_some();
        let Ok((owed, next)) = drive(OwedWakesOperation::new(start_after), context).await else {
            return Delivery::unread();
        };
        let (mut acked, mut left) = (0, false);
        for wake in owed {
            let (waiter, job_id, key) = (wake.waiter, wake.job_id, wake.key);
            if !send(wake).await {
                left = true;
                continue;
            }
            match drive(WaitRowOperation::acknowledge(waiter, job_id, key), context).await {
                Ok(()) => acked += 1,
                Err(_) => left = true,
            }
        }
        Delivery {
            acked,
            owed: left || resumed || next.is_some(),
            next,
        }
    }

    fn owner_auth(record: &JobRecord, realm_id: RealmId) -> AuthContext {
        AuthContext {
            user_id: record.created_by,
            realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    #[tokio::test]
    async fn wake_requires_parked() {
        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let node = |seed| iroh::SecretKey::from_bytes(&[seed; 32]).public();
        let (local, source) = (node(1), node(2));
        let job_id = JobId::from_bytes([5; 16]);
        let payload = JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32]));
        let record = JobRecord::new(job_id, payload, owner, local, 1_000, 1_000, None);
        insert_job(&context, &record).await.unwrap();
        let ClaimOutcome::Claimed(claimed) =
            claim_job(&context, job_id, local, 2_000).await.unwrap()
        else {
            panic!("job must be claimed")
        };
        let auth = owner_auth(&claimed, RealmId([1; 32]));
        let key = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);

        // A wake that overtakes the parking is delivered again later.
        assert_eq!(
            accept_wake(&context, source, &auth, job_id, key).await,
            Ok(false)
        );

        let wait = KeyWait {
            node_id: source,
            bucket: "b".to_string(),
            group_id: None,
            key,
        };
        let token = claimed.claim.unwrap().claim_token;
        park_job(&context, job_id, token, 3_000, vec![wait])
            .await
            .unwrap();
        // Only the node holding the key wakes the job; another node's wake is acknowledged only.
        assert_eq!(
            accept_wake(&context, local, &auth, job_id, key).await,
            Ok(true)
        );
        let parked = read_job_record(&context, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(parked.state, JobState::AwaitingKey);

        assert_eq!(
            accept_wake(&context, source, &auth, job_id, key).await,
            Ok(true)
        );
        let woken = read_job_record(&context, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(woken.state, JobState::Queued);
    }

    /// A node holding content `[9; 32]` as `sealed/data.bin` of group `[4; 16]`, sealed with a
    /// locked key, and an owner who may read that group.
    async fn guarded_node(sealed: bool) -> (tempfile::TempDir, DriverContext, NodeId, UserId) {
        use aruna_core::effects::StorageEffect;
        use aruna_core::keyspaces::{
            AUTH_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
            BUCKET_ENCRYPTION_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
        };
        use aruna_core::structs::identity::auth::Actor;
        use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
        use aruna_core::structs::identity::realm::{
            RealmAuthorizationDocument, RealmConfigDocument,
        };
        use aruna_core::structs::storage::blob::{
            BackendLocation, BackendRef, BlobLocationKey, BlobVersion, VersionKey,
        };
        use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode};
        use aruna_core::structs::storage::format::{EncodingClass, PithosLayout, StoredFormat};

        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let realm_id = RealmId([1; 32]);
        let owner = UserId::local(Ulid::from_bytes([2; 16]), realm_id);
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let group_id = Ulid::from_bytes([4; 16]);
        let key = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);
        let actor = Actor {
            node_id: node,
            user_id: owner,
            realm_id,
        };
        let group_doc = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
        let group = Group {
            display_name: "group".to_string(),
            group_id,
            realm_id,
            roles: group_doc.roles.keys().copied().collect(),
            owner,
        };
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(key.bucket_id),
            key_generation: 1,
            ..Default::default()
        };
        let layout = PithosLayout {
            stored_size: 130,
            metadata_digest: [4; 32],
            storage_generation: 0,
        };
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::from_bytes([3; 16]),
            // An unconverted copy is plain, yet its encrypting bucket still gates it.
            format: match sealed {
                true => StoredFormat::pithos(layout, key),
                false => StoredFormat::default(),
            },
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            staging: false,
            partial: false,
            blob_size: 100,
            hashes: Default::default(),
        };
        let row = BlobLocationKey::new([9; 32], EncodingClass::Raw, BackendRef::node_default());
        let alias = HashIndex::new(
            [9; 32],
            Ulid::from_bytes([6; 16]),
            realm_id,
            group_id,
            node,
            "sealed".to_string(),
            "data.bin".to_string(),
        );
        let version_key = VersionKey::new("sealed", "data.bin", Ulid::from_bytes([6; 16]));
        let encoding = EncodingClass::Raw;
        let version = BlobVersion::materialized(
            [9; 32],
            BackendRef::node_default(),
            encoding,
            std::time::SystemTime::UNIX_EPOCH,
            owner,
            None,
        );
        let bucket = BucketInfo {
            group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: owner,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: None,
        };
        let realm_doc = RealmAuthorizationDocument::default_realm_doc(realm_id);
        let config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        let rows = [
            (
                AUTH_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                realm_doc.to_bytes(&actor).unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group_doc.to_bytes(&actor).unwrap(),
            ),
            (
                REALM_CONFIG_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                config.to_bytes(&actor).unwrap(),
            ),
            (
                GROUP_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            ),
            (
                BUCKET_ENCRYPTION_KEYSPACE,
                b"sealed".to_vec(),
                settings.to_bytes().unwrap(),
            ),
            (
                S3_BUCKET_KEYSPACE,
                b"sealed".to_vec(),
                bucket.to_bytes().unwrap(),
            ),
            (
                BLOB_LOCATIONS_KEYSPACE,
                row.to_bytes(),
                location.to_bytes().unwrap(),
            ),
            (PATHS_INDEX_KEYSPACE, alias.to_bytes().unwrap(), Vec::new()),
            (
                BLOB_VERSIONS_KEYSPACE,
                version_key.to_bytes().unwrap(),
                version.to_bytes().unwrap(),
            ),
        ];
        for (key_space, key, value) in rows {
            let effect = StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            };
            context.storage_handle.send_storage_effect(effect).await;
        }
        (dir, context, node, owner)
    }

    async fn registrations(context: &DriverContext) -> usize {
        use aruna_core::keyspaces::KEY_WAIT_KEYSPACE;
        let prefix = Some(b"r".to_vec().into());
        let rows = iter_prefix_page(
            &context.storage_handle,
            KEY_WAIT_KEYSPACE,
            prefix,
            None,
            8,
            None,
        );
        rows.await.unwrap().0.len()
    }

    #[tokio::test]
    async fn waits_need_read() {
        use aruna_core::structs::identity::auth::PathRestriction;

        let (_dir, context, node, owner) = guarded_node(true).await;
        let peer = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let job_id = JobId::from_bytes([5; 16]);
        let auth = |user_id: UserId, path_restrictions| AuthContext {
            user_id,
            realm_id: RealmId([1; 32]),
            path_restrictions,
            session: None,
        };
        let content = [sealed_object()];

        // A caller without READ learns nothing and subscribes to nothing.
        let stranger = UserId::local(Ulid::from_bytes([3; 16]), RealmId([1; 32]));
        let denied = auth(stranger, None);
        let waits = register_waits(&context, (node, peer), &denied, job_id, &content);
        assert_eq!(waits.await, Ok(Vec::new()));
        // A token restricted to another path is refused even for the owner.
        let elsewhere = vec![PathRestriction {
            pattern: "elsewhere/**".to_string(),
            permission: Permission::READ,
        }];
        let restricted = auth(owner, Some(elsewhere));
        let waits = register_waits(&context, (node, peer), &restricted, job_id, &content);
        assert_eq!(waits.await, Ok(Vec::new()));
        assert_eq!(registrations(&context).await, 0);

        let allowed = auth(owner, None);
        let waits = register_waits(&context, (node, peer), &allowed, job_id, &content);
        let waits = waits.await.unwrap();
        assert_eq!(waits.len(), 1);
        assert_eq!(waits[0].bucket, "sealed");
        assert_eq!(registrations(&context).await, 1);
    }

    fn empty_context(dir: &tempfile::TempDir) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }
    }

    #[tokio::test]
    async fn offline_waiter_recovers() {
        let (key_dir, waiter_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (key_node, waiter_node) = (empty_context(&key_dir), empty_context(&waiter_dir));
        let node = |seed| iroh::SecretKey::from_bytes(&[seed; 32]).public();
        let (source, waiter) = (node(1), node(2));
        let job_id = JobId::from_bytes([5; 16]);
        let payload = JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32]));
        let record = JobRecord::new(job_id, payload, owner, waiter, 1_000, 1_000, None);
        insert_job(&waiter_node, &record).await.unwrap();
        let ClaimOutcome::Claimed(claimed) = claim_job(&waiter_node, job_id, waiter, 2_000)
            .await
            .unwrap()
        else {
            panic!("job must be claimed")
        };
        let auth = owner_auth(&claimed, RealmId([1; 32]));
        let key = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);
        let wait = KeyWait {
            node_id: source,
            bucket: "b".to_string(),
            group_id: None,
            key,
        };
        let token = claimed.claim.unwrap().claim_token;
        park_job(&waiter_node, job_id, token, 3_000, vec![wait])
            .await
            .unwrap();
        let register = WaitRowOperation::register(key, waiter, job_id, &auth);
        drive(register, &key_node).await.unwrap();
        drive(QueueWakesOperation::new(key), &key_node)
            .await
            .unwrap();

        // The waiting node is offline: the wake stays owed and the timer runs again.
        let offline = deliver_with(&key_node, None, |_| async { false }).await;
        assert_eq!(
            offline,
            Delivery {
                acked: 0,
                owed: true,
                next: None
            }
        );

        // After recovery the retry reaches it, wakes the job and drops the owed wake.
        let online = |wake: OwedWake| {
            let context = &waiter_node;
            async move {
                let accepted = accept_wake(context, source, &wake.auth, wake.job_id, wake.key);
                accepted.await == Ok(true)
            }
        };
        let recovered = deliver_with(&key_node, None, online).await;
        assert_eq!(
            recovered,
            Delivery {
                acked: 1,
                owed: false,
                next: None
            }
        );
        let woken = read_job_record(&waiter_node, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(woken.state, JobState::Queued);
        let left = drive(OwedWakesOperation::new(None), &key_node)
            .await
            .unwrap();
        assert!(left.0.is_empty());
    }

    #[tokio::test]
    async fn paging_reaches_later() {
        use crate::jobs::store::key_wait::WAKE_PAGE;

        let dir = tempfile::tempdir().unwrap();
        let context = empty_context(&dir);
        let key = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);
        let auth = AuthContext {
            user_id: UserId::new(Ulid::from_bytes([2; 16]), RealmId([1; 32])),
            realm_id: RealmId([1; 32]),
            path_restrictions: None,
            session: None,
        };
        let waiter = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let job = |timestamp_ms: u64| {
            use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
            use aruna_core::structured_id::{BucketId, PlacementHandle};
            let handle = PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap();
            JobId::from_parts(timestamp_ms, handle, BucketId::new(0).unwrap(), 0).unwrap()
        };
        let last = job(1_000_000);
        for timestamp_ms in 1..=WAKE_PAGE as u64 {
            let job_id = job(timestamp_ms);
            let register = WaitRowOperation::register(key, waiter, job_id, &auth);
            drive(register, &context).await.unwrap();
        }
        let register = WaitRowOperation::register(key, waiter, last, &auth);
        drive(register, &context).await.unwrap();
        while drive(QueueWakesOperation::new(key), &context)
            .await
            .unwrap()
        {}

        // Only the row after a full page of unreachable jobs answers.
        let send = |wake: OwedWake| async move { wake.job_id == last };
        let first = deliver_with(&context, None, send).await;
        assert_eq!(first.acked, 0);
        assert!(first.owed && first.next.is_some());
        let second = deliver_with(&context, first.next, send).await;
        assert_eq!(second.acked, 1);
        // The pass started mid-way, so it wraps around for the rows still owed.
        assert_eq!(
            second,
            Delivery {
                acked: 1,
                owed: true,
                next: None
            }
        );
    }

    #[tokio::test]
    async fn plain_copy_waits() {
        let (_dir, context, node, owner) = guarded_node(false).await;
        let peer = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let job_id = JobId::from_bytes([5; 16]);
        let auth = AuthContext {
            user_id: owner,
            realm_id: RealmId([1; 32]),
            path_restrictions: None,
            session: None,
        };

        // The copy is not converted yet, but the bucket's locked key still gates it.
        let targets = [sealed_object()];
        let waits = register_waits(&context, (node, peer), &auth, job_id, &targets);
        let waits = waits.await.unwrap();

        assert_eq!(waits.len(), 1);
        assert_eq!(waits[0].bucket, "sealed");
        assert_eq!(
            waits[0].key,
            BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1)
        );
        assert_eq!(registrations(&context).await, 1);
    }

    /// The exact object `guarded_node` stores in its encrypting bucket.
    fn sealed_object() -> WaitTarget {
        WaitTarget::Object {
            bucket: "sealed".to_string(),
            key: "data.bin".to_string(),
            version_id: Ulid::from_bytes([6; 16]),
        }
    }

    async fn put_rows(context: &DriverContext, rows: Vec<(&str, Vec<u8>, Vec<u8>)>) {
        for (key_space, key, value) in rows {
            let effect = aruna_core::effects::StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            };
            context.storage_handle.send_storage_effect(effect).await;
        }
    }

    fn owner_context(owner: UserId) -> AuthContext {
        AuthContext {
            user_id: owner,
            realm_id: RealmId([1; 32]),
            path_restrictions: None,
            session: None,
        }
    }

    #[tokio::test]
    async fn exact_target_visible() {
        use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
        use aruna_core::structs::storage::blob::{BackendRef, BlobVersion};
        use aruna_core::structs::storage::format::EncodingClass;

        let (_dir, context, node, owner) = guarded_node(false).await;
        // The same content is also readable in a bucket that does not encrypt.
        let open = HashIndex::new(
            [9; 32],
            Ulid::from_bytes([7; 16]),
            RealmId([1; 32]),
            Ulid::from_bytes([4; 16]),
            node,
            "open",
            "copy.bin",
        );
        let version = BlobVersion::materialized(
            [9; 32],
            BackendRef::node_default(),
            EncodingClass::Raw,
            std::time::SystemTime::UNIX_EPOCH,
            owner,
            None,
        );
        let version_key = VersionKey::new("open", "copy.bin", Ulid::from_bytes([7; 16]));
        put_rows(
            &context,
            vec![
                (PATHS_INDEX_KEYSPACE, open.to_bytes().unwrap(), Vec::new()),
                (
                    BLOB_VERSIONS_KEYSPACE,
                    version_key.to_bytes().unwrap(),
                    version.to_bytes().unwrap(),
                ),
            ],
        )
        .await;
        let peer = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let job_id = JobId::from_bytes([5; 16]);
        let auth = owner_context(owner);
        let captured = WaitTarget::Captured {
            blake3: [9; 32],
            version_id: Ulid::from_bytes([6; 16]),
        };

        // The exact version and the captured version keep their dependency.
        for target in [sealed_object(), captured] {
            let targets = [target];
            let waits = register_waits(&context, (node, peer), &auth, job_id, &targets);
            assert_eq!(waits.await.unwrap().len(), 1);
        }
        // Only a bare content is satisfied by another readable version.
        let content = [WaitTarget::Content { blake3: [9; 32] }];
        let waits = register_waits(&context, (node, peer), &auth, job_id, &content);
        assert!(waits.await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn reference_target_waits() {
        use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
        use aruna_core::structs::execution::source_access::SourceMetadata;
        use aruna_core::structs::execution::source_connector::SourceConnectorKind;
        use aruna_core::structs::execution::staging::{
            PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
        };
        use aruna_core::structs::storage::blob::BlobVersion;

        let (_dir, context, node, owner) = guarded_node(true).await;
        // A reference in the encrypting bucket has no content hash this node knows.
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::ArunaNative,
                public_config: Default::default(),
                source_path: "source/object.bin".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        };
        let metadata = SourceMetadata {
            content_length: 10,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        };
        let epoch = std::time::SystemTime::UNIX_EPOCH;
        let reference = BlobVersion::reference(source, metadata, epoch, owner, epoch);
        let version_id = Ulid::from_bytes([11; 16]);
        let version_key = VersionKey::new("sealed", "ref.bin", version_id);
        let row = (
            BLOB_VERSIONS_KEYSPACE,
            version_key.to_bytes().unwrap(),
            reference.to_bytes().unwrap(),
        );
        put_rows(&context, vec![row]).await;
        let target = WaitTarget::Object {
            bucket: "sealed".to_string(),
            key: "ref.bin".to_string(),
            version_id,
        };
        let peer = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let auth = owner_context(owner);

        let targets = [target];
        let job_id = JobId::from_bytes([5; 16]);
        let waits = register_waits(&context, (node, peer), &auth, job_id, &targets);
        let waits = waits.await.unwrap();

        assert_eq!(waits.len(), 1);
        assert_eq!(
            waits[0].key,
            BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1)
        );
        assert_eq!(registrations(&context).await, 1);
    }
}
