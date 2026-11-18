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
use super::store::JobMutationError;
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
