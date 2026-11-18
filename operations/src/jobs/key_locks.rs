//! Finds the bucket keys a job's inputs need while they are locked, and parks the job on them.
//! Registration on other nodes grants no read: staging still passes their admission.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::jobs::{JobRequest, JobResponse, WaitTarget};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE,
    PATHS_INDEX_KEYSPACE, PENDING_LOCATION_KEYSPACE,
};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::rows::{page, read_row, value};
use aruna_core::structs::execution::job::{CapturedInput, JobError, JobId, JobRecord, KeyWait};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobVersion, BlobVersionState, HashIndex, VersionKey,
};
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyRef};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::records::pages::ServingNode;
use super::route::{JobRouteError, JobRouteOperation, JobRouteOutcome};
use super::store::mutation_error::JobMutationError;
use super::store::park_wake::AwaitOutcome;
use super::store::row_calls::{park_job, satisfy_key_wait};

/// Aliases of one content checked on this node; a content beyond them stays unknown.
pub(crate) const ALIAS_PAGE: usize = 64;

/// Targets one registration may name; more is refused rather than scanned.
pub(crate) const MAX_AWAITED: usize = 1024;

/// Why the keys of an input could not be checked.
#[derive(Clone, Debug, Error, PartialEq)]
#[error("{0}")]
pub struct KeyCheckError(pub String);

impl From<IoError> for KeyCheckError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

fn check_error(error: impl std::fmt::Display) -> KeyCheckError {
    KeyCheckError(error.to_string())
}

/// Whether this node holds an active unlock of `key`. A node that cannot tell holds none.
#[derive(Debug, PartialEq)]
pub struct KeyUnlockedOperation {
    key: BucketKeyRef,
}

impl KeyUnlockedOperation {
    pub fn new(key: BucketKeyRef) -> Self {
        Self { key }
    }
}

impl Operation for KeyUnlockedOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let effect = Effect::Blob(BlobEffect::ReadKeyStatus {
            bucket_id: self.key.bucket_id,
        });
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        let unlocked = match result {
            IoResult::Event(event) => match *event {
                Event::Blob(BlobEvent::KeyStatus { generations }) => generations
                    .iter()
                    .any(|status| status.key == self.key && status.active),
                _ => false,
            },
            _ => false,
        };
        Ok(Next::Done(unlocked))
    }
}

/// One page of a content's aliases on this node, and the cursor of the next page.
#[derive(Debug, PartialEq)]
pub struct AliasPageOperation {
    content: [u8; 32],
    node_id: NodeId,
    after: Option<Key>,
}

pub type AliasPage = (Vec<HashIndex>, Option<Key>);

impl AliasPageOperation {
    pub fn new(content: [u8; 32], node_id: NodeId, after: Option<Key>) -> Self {
        Self {
            content,
            node_id,
            after,
        }
    }
}

impl Operation for AliasPageOperation {
    type Output = AliasPage;
    type Error = KeyCheckError;

    fn first(&mut self) -> Result<Next<AliasPage>, KeyCheckError> {
        let prefix = HashIndex::hash_prefix(&self.content).map_err(check_error)?;
        Ok(Next::Call(IoCall::Page {
            space: PATHS_INDEX_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after: self.after.take(),
            limit: ALIAS_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AliasPage>, KeyCheckError> {
        let (rows, _) = page(result)?;
        let next = (rows.len() == ALIAS_PAGE)
            .then(|| rows.last().map(|(row, _)| row.clone()))
            .flatten();
        let aliases = rows
            .iter()
            .filter_map(|(row, _)| HashIndex::from_bytes(row).ok())
            .filter(|alias| alias.node_id == self.node_id)
            .collect();
        Ok(Next::Done((aliases, next)))
    }
}

/// The alias of exactly `version_id` among a content's aliases on this node, paging through
/// every alias page until it is found or the aliases end.
#[derive(Debug, PartialEq)]
pub struct FindAliasOperation {
    content: [u8; 32],
    version_id: Ulid,
    node_id: NodeId,
}

impl FindAliasOperation {
    pub fn new(content: [u8; 32], version_id: Ulid, node_id: NodeId) -> Self {
        Self {
            content,
            version_id,
            node_id,
        }
    }

    fn read(&self, after: Option<Key>) -> Next<Option<HashIndex>> {
        let page = AliasPageOperation::new(self.content, self.node_id, after);
        Next::Call(IoCall::child(page))
    }
}

impl Operation for FindAliasOperation {
    type Output = Option<HashIndex>;
    type Error = KeyCheckError;

    fn first(&mut self) -> Result<Next<Self::Output>, KeyCheckError> {
        Ok(self.read(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, KeyCheckError> {
        let (aliases, next) = result.child::<AliasPage, KeyCheckError>()??;
        let found = aliases
            .into_iter()
            .find(|alias| alias.version_id == self.version_id);
        Ok(match (found, next) {
            (Some(alias), _) => Next::Done(Some(alias)),
            (None, Some(next)) => self.read(Some(next)),
            (None, None) => Next::Done(None),
        })
    }
}

#[derive(Debug, PartialEq)]
enum WaitStep {
    Version,
    Location,
    Bucket,
    Unlocked(BucketKeyRef),
}

/// The locked key a read of exactly this version would need, following read admission: a sealed
/// copy needs its own key; a plain copy or a reference of an encrypting bucket needs the
/// bucket's active key.
#[derive(Debug, PartialEq)]
pub struct VersionWaitOperation {
    node_id: NodeId,
    group_id: Ulid,
    version: VersionKey,
    step: WaitStep,
}

type WaitNext = Result<Next<Option<KeyWait>>, KeyCheckError>;

impl VersionWaitOperation {
    pub fn new((node_id, group_id): (NodeId, Ulid), version: VersionKey) -> Self {
        Self {
            node_id,
            group_id,
            version,
            step: WaitStep::Version,
        }
    }

    /// The wait of the version an alias names.
    pub fn of_alias(alias: &HashIndex) -> Self {
        let version = VersionKey::new(&alias.bucket, &alias.key, alias.version_id);
        Self::new((alias.node_id, alias.group_id), version)
    }

    fn read_bucket(&mut self) -> WaitNext {
        self.step = WaitStep::Bucket;
        let bucket = self.version.bucket.as_bytes().to_vec();
        Ok(Next::Call(read_row(BUCKET_ENCRYPTION_KEYSPACE, bucket)))
    }

    fn versioned(&mut self, row: Option<Value>) -> WaitNext {
        let Some(row) = row else {
            return Ok(Next::Done(None));
        };
        let state = BlobVersion::from_bytes(&row).map_err(check_error)?.state;
        let location = match (&state.location_key(), state.pending_archive(), &state) {
            (Some(key), _, _) => read_row(BLOB_LOCATIONS_KEYSPACE, key.to_bytes()),
            (None, Some(archive), _) => read_row(PENDING_LOCATION_KEYSPACE, archive.to_bytes()),
            (None, None, BlobVersionState::Reference { .. }) => return self.read_bucket(),
            (None, None, _) => return Ok(Next::Done(None)),
        };
        self.step = WaitStep::Location;
        Ok(Next::Call(location))
    }

    fn located(&mut self, row: Option<Value>) -> WaitNext {
        let sealed = row
            .map(|row| BackendLocation::from_bytes(&row).map_err(check_error))
            .transpose()?
            .and_then(|location| location.format.bucket_key());
        match sealed {
            Some(key) => self.check(key),
            None => self.read_bucket(),
        }
    }

    fn check(&mut self, key: BucketKeyRef) -> WaitNext {
        self.step = WaitStep::Unlocked(key);
        Ok(Next::Call(IoCall::child(KeyUnlockedOperation::new(key))))
    }
}

impl Operation for VersionWaitOperation {
    type Output = Option<KeyWait>;
    type Error = KeyCheckError;

    fn first(&mut self) -> WaitNext {
        let key = self.version.to_bytes().map_err(check_error)?;
        Ok(Next::Call(read_row(BLOB_VERSIONS_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> WaitNext {
        match self.step {
            WaitStep::Version => self.versioned(value(result)?),
            WaitStep::Location => self.located(value(result)?),
            WaitStep::Bucket => {
                let settings = value(result)?;
                let key = BucketEncryption::from_row(settings.as_deref())
                    .map_err(check_error)?
                    .active_key();
                match key {
                    Some(key) => self.check(key),
                    None => Ok(Next::Done(None)),
                }
            }
            WaitStep::Unlocked(key) => {
                if result.child::<bool, IoError>()?.map_err(check_error)? {
                    return Ok(Next::Done(None));
                }
                Ok(Next::Done(Some(KeyWait {
                    node_id: self.node_id,
                    bucket: self.version.bucket.clone(),
                    group_id: Some(self.group_id),
                    key,
                })))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum LockedStep {
    Alias,
    Wait,
}

/// The keys locked here that local inputs need. Remote inputs are left to their source node, a
/// reference has no stored bytes here, and a version without a local alias is left to staging.
#[derive(Debug, PartialEq)]
pub struct LockedInputsOperation {
    /// Content and version of the inputs still to check, the next one last.
    inputs: Vec<([u8; 32], Ulid)>,
    node_id: NodeId,
    waits: Vec<KeyWait>,
    step: LockedStep,
}

type LockedNext = Result<Next<Vec<KeyWait>>, KeyCheckError>;

impl LockedInputsOperation {
    pub fn new(inputs: &[CapturedInput], node_id: NodeId) -> Self {
        let mut pending: Vec<_> = inputs
            .iter()
            .filter(|input| input.source_node_id == node_id)
            .filter_map(|input| Some((input.content.blake3()?, input.version_id)))
            .collect();
        pending.reverse();
        Self {
            inputs: pending,
            node_id,
            waits: Vec::new(),
            step: LockedStep::Alias,
        }
    }

    fn check_next(&mut self) -> LockedNext {
        let Some((content, version_id)) = self.inputs.pop() else {
            return Ok(Next::Done(std::mem::take(&mut self.waits)));
        };
        self.step = LockedStep::Alias;
        let find = FindAliasOperation::new(content, version_id, self.node_id);
        Ok(Next::Call(IoCall::child(find)))
    }
}

impl Operation for LockedInputsOperation {
    type Output = Vec<KeyWait>;
    type Error = KeyCheckError;

    fn first(&mut self) -> LockedNext {
        self.check_next()
    }

    fn next(&mut self, result: IoResult) -> LockedNext {
        match self.step {
            LockedStep::Alias => match result.child::<Option<HashIndex>, KeyCheckError>()?? {
                Some(alias) => {
                    self.step = LockedStep::Wait;
                    let wait = VersionWaitOperation::of_alias(&alias);
                    Ok(Next::Call(IoCall::child(wait)))
                }
                None => self.check_next(),
            },
            LockedStep::Wait => {
                let wait = result.child::<Option<KeyWait>, KeyCheckError>()??;
                if let Some(wait) = wait
                    && !self.waits.contains(&wait)
                {
                    self.waits.push(wait);
                }
                self.check_next()
            }
        }
    }
}

/// Registers one job on `node` for `targets` and answers the keys still locked there. An
/// unreachable or refusing node yields none.
#[derive(Debug, PartialEq)]
pub struct AskWaitsOperation {
    job_id: JobId,
    node: NodeId,
    auth: AuthContext,
    serving: ServingNode,
    /// Target batches still to send, the next one last.
    batches: Vec<Vec<WaitTarget>>,
    waits: Vec<KeyWait>,
}

impl AskWaitsOperation {
    pub fn new(
        auth: &AuthContext,
        job_id: JobId,
        node: NodeId,
        targets: &[WaitTarget],
        serving: ServingNode,
    ) -> Self {
        let mut batches: Vec<_> = targets.chunks(MAX_AWAITED).map(<[_]>::to_vec).collect();
        batches.reverse();
        Self {
            job_id,
            node,
            auth: auth.clone(),
            serving,
            batches,
            waits: Vec::new(),
        }
    }

    fn ask_next(&mut self) -> Result<Next<Vec<KeyWait>>, IoError> {
        let (Some((local, realm_id)), Some(targets)) = (self.serving, self.batches.pop()) else {
            return Ok(Next::Done(std::mem::take(&mut self.waits)));
        };
        let request = JobRequest::AwaitKeys {
            auth_token: AuthToken::Internal(self.auth.clone()),
            job_id: self.job_id,
            targets,
        };
        let route = JobRouteOperation::new(realm_id, local, self.job_id, Some(request))
            .with_responder(Some(self.node));
        Ok(Next::Call(IoCall::child(route)))
    }
}

impl Operation for AskWaitsOperation {
    type Output = Vec<KeyWait>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Vec<KeyWait>>, IoError> {
        self.ask_next()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<KeyWait>>, IoError> {
        let (job_id, node) = (self.job_id, self.node);
        match result.child::<JobRouteOutcome, JobRouteError>()? {
            Ok(JobRouteOutcome::Remote(JobResponse::KeysLocked(locked))) => {
                // The answering node names its own keys only.
                self.waits
                    .extend(locked.into_iter().filter(|wait| wait.node_id == node));
            }
            Ok(JobRouteOutcome::Remote(response)) => {
                warn!(job_id = %job_id, node = %node, ?response, "Key wait refused");
            }
            Ok(JobRouteOutcome::Local) => {}
            Err(error) => warn!(job_id = %job_id, node = %node, %error, "Key wait request failed"),
        }
        self.ask_next()
    }
}

/// The job owner a node vouches for in key wait requests, signed in to its realm.
fn owner_auth(record: &JobRecord, serving: ServingNode) -> AuthContext {
    AuthContext {
        user_id: record.created_by,
        realm_id: serving.map_or(record.created_by.realm_id, |(_, realm_id)| realm_id),
        path_restrictions: None,
        session: None,
    }
}

/// The remote source nodes of a record's inputs, each with what to wait for there.
fn remote_targets(record: &JobRecord, node_id: NodeId) -> BTreeMap<NodeId, Vec<WaitTarget>> {
    let mut by_node: BTreeMap<NodeId, Vec<WaitTarget>> = BTreeMap::new();
    for input in &record.captured_inputs {
        if let Some(blake3) = input.content.blake3()
            && input.source_node_id != node_id
        {
            by_node
                .entry(input.source_node_id)
                .or_default()
                .push(WaitTarget::Captured {
                    blake3,
                    version_id: input.version_id,
                });
        }
    }
    by_node
}

#[derive(Debug, PartialEq)]
enum ParkStep {
    Local,
    Remote,
    Park(Vec<BucketKeyRef>),
    Unlocked(Vec<BucketKeyRef>, BucketKeyRef),
    Wake(Vec<BucketKeyRef>),
}

/// Parks a claimed job whose inputs need locked keys. Answers true when the job no longer runs
/// here: it is parked, or its claim is gone. A cancel request or a failed check continues.
#[derive(Debug, PartialEq)]
pub struct ParkLockedOperation {
    record: JobRecord,
    token: Ulid,
    node_id: NodeId,
    serving: ServingNode,
    now_ms: u64,
    only_locked: Option<bool>,
    remote: Vec<(NodeId, Vec<WaitTarget>)>,
    waits: Vec<KeyWait>,
    step: ParkStep,
}

type ParkNext = Result<Next<bool>, IoError>;

impl ParkLockedOperation {
    pub fn new(
        record: &JobRecord,
        token: Ulid,
        (node_id, serving): (NodeId, ServingNode),
        now_ms: u64,
    ) -> Self {
        Self {
            remote: remote_targets(record, node_id).into_iter().rev().collect(),
            record: record.clone(),
            token,
            node_id,
            serving,
            now_ms,
            only_locked: None,
            waits: Vec::new(),
            step: ParkStep::Local,
        }
    }

    /// Parks a job whose preparation hit a key locked after the precheck, before any attempt
    /// intent exists. Any other error answers false and is left to the caller.
    pub fn on_error(
        record: &JobRecord,
        token: Ulid,
        node: (NodeId, ServingNode),
        error: &JobError,
        now_ms: u64,
    ) -> Self {
        let mut operation = Self::new(record, token, node, now_ms);
        operation.only_locked = Some(error.locked);
        operation
    }

    fn ask_next(&mut self) -> ParkNext {
        let Some((node, targets)) = self.remote.pop() else {
            return self.park();
        };
        self.step = ParkStep::Remote;
        let auth = owner_auth(&self.record, self.serving);
        let job_id = self.record.job_id;
        let ask = AskWaitsOperation::new(&auth, job_id, node, &targets, self.serving);
        Ok(Next::Call(IoCall::child(ask)))
    }

    fn park(&mut self) -> ParkNext {
        if self.waits.is_empty() {
            return Ok(Next::Done(false));
        }
        let waits = std::mem::take(&mut self.waits);
        self.step = ParkStep::Park(waits.iter().map(|wait| wait.key).collect());
        let park = park_job(self.record.job_id, self.token, self.now_ms, waits);
        Ok(Next::Call(IoCall::child(park)))
    }

    /// A key unlocked while the job parked wakes it here; no unlock call would.
    fn check_next(&mut self, mut keys: Vec<BucketKeyRef>) -> ParkNext {
        let Some(key) = keys.pop() else {
            return Ok(Next::Done(true));
        };
        self.step = ParkStep::Unlocked(keys, key);
        Ok(Next::Call(IoCall::child(KeyUnlockedOperation::new(key))))
    }
}

impl Operation for ParkLockedOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> ParkNext {
        if self.only_locked == Some(false) {
            return Ok(Next::Done(false));
        }
        let local = LockedInputsOperation::new(&self.record.captured_inputs, self.node_id);
        Ok(Next::Call(IoCall::child(local)))
    }

    fn next(&mut self, result: IoResult) -> ParkNext {
        let job_id = self.record.job_id;
        match std::mem::replace(&mut self.step, ParkStep::Local) {
            ParkStep::Local => match result.child::<Vec<KeyWait>, KeyCheckError>()? {
                Ok(waits) => {
                    self.waits = waits;
                    self.ask_next()
                }
                Err(error) => {
                    warn!(job_id = %job_id, %error, "Input key check failed; staging decides");
                    Ok(Next::Done(false))
                }
            },
            ParkStep::Remote => {
                let waits = result.child::<Vec<KeyWait>, IoError>()?;
                self.waits.extend(waits.unwrap_or_default());
                self.ask_next()
            }
            ParkStep::Park(mut keys) => match result.child::<AwaitOutcome, JobMutationError>()? {
                Ok(AwaitOutcome::Parked(_)) => {
                    keys.reverse();
                    self.check_next(keys)
                }
                Ok(AwaitOutcome::Skipped) => Ok(Next::Done(false)),
                Err(JobMutationError::TokenMismatch) => Ok(Next::Done(true)),
                Err(error) => {
                    warn!(job_id = %job_id, %error, "Failed to park job for a bucket key");
                    Ok(Next::Done(false))
                }
            },
            ParkStep::Unlocked(keys, key) => {
                if !result.child::<bool, IoError>()?.unwrap_or(false) {
                    return self.check_next(keys);
                }
                self.step = ParkStep::Wake(keys);
                let wake = satisfy_key_wait(job_id, key, self.now_ms);
                Ok(Next::Call(IoCall::child(wake)))
            }
            ParkStep::Wake(keys) => {
                if let Err(error) = result.child::<bool, JobMutationError>()? {
                    warn!(job_id = %job_id, %error, "Failed to wake parked job");
                }
                self.check_next(keys)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{REALM, captured_input, node, payload, user};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::job::{InputMode, InputSelection, InputSource, JobPayload};

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn key(seed: u8) -> BucketKeyRef {
        BucketKeyRef {
            bucket_id: Ulid::from_parts(u64::from(seed), 1),
            generation: 1,
        }
    }

    fn wait(seed: u8) -> KeyWait {
        KeyWait {
            node_id: node(1),
            bucket: "b".to_string(),
            group_id: None,
            key: key(seed),
        }
    }

    fn input(source: NodeId) -> CapturedInput {
        let selection = InputSelection {
            source: InputSource::S3 {
                bucket: "b".to_string(),
                key: "k".to_string(),
                version_id: None,
            },
            source_node_id: None,
            dest_key: "k".to_string(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        };
        captured_input(&selection, source)
    }

    fn record(inputs: Vec<CapturedInput>) -> JobRecord {
        let mut record = JobRecord::new(
            JobId::from_bytes([3; 16]),
            JobPayload::Execution(payload()),
            user(),
            node(1),
            1,
            1,
            None,
        );
        record.captured_inputs = inputs;
        record
    }

    fn park(record: &JobRecord) -> ParkLockedOperation {
        let token = Ulid::from_parts(4, 4);
        ParkLockedOperation::new(record, token, (node(1), Some((node(1), REALM))), 9)
    }

    fn calls_child(next: ParkNext) {
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))), "{next:?}");
    }

    // Only an active unlock of exactly this key counts; a missing blob handle holds none.
    #[test]
    fn unlock_needs_status() {
        let mut check = KeyUnlockedOperation::new(key(1));
        assert!(matches!(check.first(), Ok(Next::Call(IoCall::Effect(_)))));
        assert_eq!(check.next(IoResult::Done), Ok(Next::Done(false)));
    }

    // An alias page continues only when it came back full.
    #[test]
    fn short_page_ends() {
        let mut read = AliasPageOperation::new([1; 32], node(1), None);
        assert!(matches!(read.first(), Ok(Next::Call(IoCall::Page { .. }))));
        let done = read.next(IoResult::Page {
            rows: Vec::new(),
            next: None,
        });
        assert_eq!(done, Ok(Next::Done((Vec::new(), None))));
    }

    // The walk follows pages until the aliases end without the version.
    #[test]
    fn alias_walk_pages() {
        let mut find = FindAliasOperation::new([1; 32], Ulid::from_parts(1, 1), node(1));
        find.first().expect("the first page is read");
        let more = child(Ok::<AliasPage, KeyCheckError>((
            Vec::new(),
            Some(Key::from(vec![1])),
        )));
        assert!(matches!(find.next(more), Ok(Next::Call(IoCall::Child(_)))));
        let end = child(Ok::<AliasPage, KeyCheckError>((Vec::new(), None)));
        assert_eq!(find.next(end), Ok(Next::Done(None)));
    }

    fn version_wait() -> VersionWaitOperation {
        let version = VersionKey::new("b", "k", Ulid::from_parts(1, 1));
        VersionWaitOperation::new((node(1), Ulid::from_parts(2, 2)), version)
    }

    // A version that does not exist here needs no key.
    #[test]
    fn missing_version_free() {
        let mut wait = version_wait();
        assert!(matches!(wait.first(), Ok(Next::Call(IoCall::Read(_)))));
        let answer = wait.next(IoResult::Values(vec![None]));
        assert_eq!(answer, Ok(Next::Done(None)));
    }

    // A locked key becomes a wait naming this node and bucket; an unlocked one none.
    #[test]
    fn locked_key_waits() {
        let answer = |unlocked: bool| {
            let mut wait = version_wait();
            wait.step = WaitStep::Unlocked(key(1));
            wait.next(child(Ok::<_, IoError>(unlocked)))
        };
        assert_eq!(answer(true), Ok(Next::Done(None)));
        let Ok(Next::Done(Some(found))) = answer(false) else {
            panic!("a locked key is waited for");
        };
        assert_eq!((found.node_id, found.key), (node(1), key(1)));
    }

    // Inputs of other nodes are not checked here, and the same key is waited for once.
    #[test]
    fn local_inputs_only() {
        let inputs = [input(node(2))];
        let mut none = LockedInputsOperation::new(&inputs, node(1));
        assert_eq!(none.first(), Ok(Next::Done(Vec::new())));
        let inputs = [input(node(1)), input(node(1))];
        let mut locked = LockedInputsOperation::new(&inputs, node(1));
        locked.first().expect("the alias is looked up");
        locked.step = LockedStep::Wait;
        locked
            .next(child(Ok::<_, KeyCheckError>(Some(wait(1)))))
            .expect("the next input follows");
        locked.step = LockedStep::Wait;
        let done = locked.next(child(Ok::<_, KeyCheckError>(Some(wait(1)))));
        assert_eq!(done, Ok(Next::Done(vec![wait(1)])));
    }

    // Without a network no other node can be asked.
    #[test]
    fn offline_asks_nobody() {
        let auth = AuthContext::anonymous(REALM);
        let target = WaitTarget::Content { blake3: [1; 32] };
        let job = JobId::from_bytes([3; 16]);
        let mut ask = AskWaitsOperation::new(&auth, job, node(2), &[target], None);
        assert_eq!(ask.first(), Ok(Next::Done(Vec::new())));
    }

    // A node names its own keys only, and a failed request adds nothing.
    #[test]
    fn answers_filtered_by_node() {
        let auth = AuthContext::anonymous(REALM);
        let target = WaitTarget::Content { blake3: [1; 32] };
        let job = JobId::from_bytes([3; 16]);
        let mut ask =
            AskWaitsOperation::new(&auth, job, node(1), &[target], Some((node(2), REALM)));
        assert!(matches!(ask.first(), Ok(Next::Call(IoCall::Child(_)))));
        let mut foreign = wait(2);
        foreign.node_id = node(5);
        let locked = JobResponse::KeysLocked(vec![wait(1), foreign]);
        let answer = child(Ok::<_, JobRouteError>(JobRouteOutcome::Remote(locked)));
        assert_eq!(ask.next(answer), Ok(Next::Done(vec![wait(1)])));
    }

    // Any error other than a locked key leaves the job to its caller.
    #[test]
    fn other_errors_continue() {
        let job = record(Vec::new());
        let error = JobError::retryable("disk");
        let token = Ulid::from_parts(4, 4);
        let mut operation = ParkLockedOperation::on_error(&job, token, (node(1), None), &error, 9);
        assert_eq!(operation.first(), Ok(Next::Done(false)));
    }

    // A failed local check continues, and no locked key means nothing to park on.
    #[test]
    fn nothing_to_park() {
        let job = record(Vec::new());
        let mut failed = park(&job);
        calls_child(failed.first());
        let error = Err::<Vec<KeyWait>, _>(KeyCheckError("down".to_string()));
        assert_eq!(failed.next(child(error)), Ok(Next::Done(false)));
        let mut free = park(&job);
        free.first().expect("the local keys are checked");
        let none = child(Ok::<_, KeyCheckError>(Vec::<KeyWait>::new()));
        assert_eq!(free.next(none), Ok(Next::Done(false)));
    }

    // A lost claim means the job no longer runs here; a key already open wakes it again.
    #[test]
    fn park_outcomes() {
        let job = record(Vec::new());
        let parked = || {
            let mut operation = park(&job);
            operation.first().expect("the local keys are checked");
            let waits = child(Ok::<_, KeyCheckError>(vec![wait(1)]));
            calls_child(operation.next(waits));
            operation
        };
        let mut lost = parked();
        let refused = Err::<AwaitOutcome, _>(JobMutationError::TokenMismatch);
        assert_eq!(lost.next(child(refused)), Ok(Next::Done(true)));
        let mut skipped = parked();
        let answer = Ok::<_, JobMutationError>(AwaitOutcome::Skipped);
        assert_eq!(skipped.next(child(answer)), Ok(Next::Done(false)));
        let mut open = parked();
        let answer = Ok::<_, JobMutationError>(AwaitOutcome::Parked(job.clone()));
        calls_child(open.next(child(answer)));
        calls_child(open.next(child(Ok::<_, IoError>(true))));
        let woken = child(Ok::<_, JobMutationError>(true));
        assert_eq!(open.next(woken), Ok(Next::Done(true)));
    }
}
