//! Hands a range's open imports to its new coordinator, which fetches the named jobs with their
//! source roots and positions before it completes, and runs them once it coordinates the range.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{DISTRIBUTION_KEYSPACE, JOB_KEYSPACE, NODE_STATE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, read_row, value};
use aruna_core::structs::execution::job::{
    JobId, JobPayload, JobRecord, RECORD_KEY_PREFIX, StagingJobCheckpoint, StagingJobSpec,
};
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::distribution::source_root::SourceRoot;
use aruna_core::structs::storage::distribution::{DistributionSnapshot, DistributionView};
use aruna_core::types::{Key, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::peer_call::{peer_answer, peer_call};
use crate::connectors::create_connector::{
    SourceConnectorError, SourceConnectorInput, SourceConnectorOperation, SourceConnectorResult,
};
use crate::connectors::get_connector::{
    GetSourceError, GetSourceInput, GetSourceOperation, GetSourceResult,
};
use crate::connectors::list_connectors::{
    ListSourceError, ListSourceInput, ListSourceOperation, ListSourceResult,
};
use crate::jobs::staging::{CheckpointError, ReadCheckpointOperation, handed_checkpoint};
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// Job rows one scan page reads.
const JOB_PAGE: usize = 256;
const HANDOVER_PREFIX: &[u8] = b"import_handover/";
const RESUME_PREFIX: &[u8] = b"import_resume/";
const JOBS_PREFIX: &[u8] = b"import_jobs/";

/// Why handing over imports stopped.
#[derive(Clone, Debug, Error, PartialEq)]
#[error("{0}")]
pub(crate) struct ImportError(String);

impl From<IoError> for ImportError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

fn failed(error: impl std::fmt::Display) -> ImportError {
    ImportError(error.to_string())
}

/// A staging job a range's former coordinator hands over: its spec, the source root its
/// connector reads, and where the job stood before the range was fenced.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandedImport {
    pub spec: StagingJobSpec,
    pub root: SourceRoot,
    pub resume: Option<StagingJobCheckpoint>,
    /// The connector's record when it holds no credentials, so the new coordinator can register
    /// it under the same id, as the import's fan-out does.
    pub shared: Option<SourceConnector>,
}

/// A fenced job as its former coordinator keeps it until the transfer is no longer open: its
/// spec, so pruning its job record loses nothing, and where it stood before the fence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FencedJob {
    spec: StagingJobSpec,
    at: Option<StagingJobCheckpoint>,
}

/// What a new coordinator keeps of a range's imports: jobs still to fetch from the former
/// coordinator, and jobs ready to run here with their connector and position.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Kept {
    from: NodeId,
    pending: Vec<JobId>,
    pub(super) ready: Vec<(StagingJobSpec, Option<StagingJobCheckpoint>)>,
}

fn resume_key(transfer_id: Ulid, job_id: JobId) -> Vec<u8> {
    [RESUME_PREFIX, &transfer_id.to_bytes(), &job_id.to_bytes()].concat()
}

pub(super) fn jobs_key(transfer_id: Ulid) -> Vec<u8> {
    [JOBS_PREFIX, &transfer_id.to_bytes()].concat()
}

/// The prefix of the rows that resume the imports of a transfer.
pub(super) fn resume_prefix(transfer_id: Ulid) -> Vec<u8> {
    [RESUME_PREFIX, &transfer_id.to_bytes()].concat()
}

fn handover_key(set_id: Ulid, range_id: Ulid) -> Vec<u8> {
    [HANDOVER_PREFIX, &set_id.to_bytes(), &range_id.to_bytes()].concat()
}

fn handover_parts(key: &[u8]) -> Option<(Ulid, Ulid)> {
    let rest = key.strip_prefix(HANDOVER_PREFIX)?;
    let set: [u8; 16] = rest.get(..16)?.try_into().ok()?;
    let range: [u8; 16] = rest.get(16..32)?.try_into().ok()?;
    Some((Ulid::from_bytes(set), Ulid::from_bytes(range)))
}

fn state_row(key: Vec<u8>, value: Vec<u8>) -> IoCall {
    let row = (NODE_STATE_KEYSPACE.to_string(), key.into(), value.into());
    IoCall::Write(vec![row])
}

fn encoded_row<T: Serialize>(key: Vec<u8>, row: &T) -> Result<IoCall, ImportError> {
    Ok(state_row(key, postcard::to_allocvec(row).map_err(failed)?))
}

fn state_read(key: Vec<u8>) -> IoCall {
    read_row(NODE_STATE_KEYSPACE, key)
}

fn state_delete(key: Vec<u8>) -> IoCall {
    IoCall::Delete(vec![(NODE_STATE_KEYSPACE.to_string(), key.into())])
}

fn record_read(set_id: Ulid) -> IoCall {
    read_row(DISTRIBUTION_KEYSPACE, set_id.to_bytes().to_vec())
}

/// The open staging jobs of this node that import into `bucket`, with their specs.
#[derive(Debug, PartialEq)]
struct OpenImportsOperation {
    bucket: String,
    pages: PrefixPages,
    jobs: Vec<(JobId, StagingJobSpec)>,
}

impl OpenImportsOperation {
    fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            pages: PrefixPages::new(JOB_KEYSPACE, RECORD_KEY_PREFIX.to_vec(), JOB_PAGE),
            jobs: Vec::new(),
        }
    }
}

impl Operation for OpenImportsOperation {
    type Output = Vec<(JobId, StagingJobSpec)>;
    type Error = ImportError;

    fn first(&mut self) -> Result<Next<Self::Output>, ImportError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ImportError> {
        for (_, value) in self.pages.take(result)? {
            let Ok(record) = JobRecord::from_bytes(&value) else {
                continue;
            };
            if let JobPayload::Staging(spec) = record.payload
                && spec.bucket == self.bucket
                && !record.state.is_terminal()
            {
                self.jobs.push((record.job_id, spec));
            }
        }
        Ok(match self.pages.done() {
            true => Next::Done(std::mem::take(&mut self.jobs)),
            false => Next::Call(self.pages.call()),
        })
    }
}

/// What the pending call of keeping a transfer's imports answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeepStep {
    Open,
    Checkpoint,
    Kept,
    Listed,
}

/// Keeps the open imports of `bucket` and where each stands, before the range of `transfer_id`
/// is fenced: pages committed after it only repeat work the new coordinator does again.
#[derive(Debug, PartialEq)]
pub(super) struct KeepResumeOperation {
    bucket: String,
    transfer_id: Ulid,
    step: KeepStep,
    open: VecDeque<(JobId, StagingJobSpec)>,
    current: Option<(JobId, StagingJobSpec)>,
    jobs: Vec<JobId>,
}

impl KeepResumeOperation {
    pub(super) fn new(bucket: &str, transfer_id: Ulid) -> Self {
        Self {
            bucket: bucket.to_string(),
            transfer_id,
            step: KeepStep::Open,
            open: VecDeque::new(),
            current: None,
            jobs: Vec::new(),
        }
    }

    fn next_job(&mut self) -> Result<Next<()>, ImportError> {
        let Some((job_id, spec)) = self.open.pop_front() else {
            self.step = KeepStep::Listed;
            let row = encoded_row(jobs_key(self.transfer_id), &self.jobs)?;
            return Ok(Next::Call(row));
        };
        self.current = Some((job_id, spec));
        self.step = KeepStep::Checkpoint;
        Ok(Next::Call(IoCall::child(ReadCheckpointOperation::new(job_id))))
    }
}

impl Operation for KeepResumeOperation {
    type Output = ();
    type Error = ImportError;

    fn first(&mut self) -> Result<Next<()>, ImportError> {
        let open = OpenImportsOperation::new(&self.bucket);
        Ok(Next::Call(IoCall::child(open)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ImportError> {
        match self.step {
            KeepStep::Open => {
                let open = result.child::<Vec<(JobId, StagingJobSpec)>, ImportError>()??;
                self.open = open.into();
                self.next_job()
            }
            KeepStep::Checkpoint => {
                let read = result.child::<Option<StagingJobCheckpoint>, CheckpointError>()?;
                let at = read.map_err(failed)?;
                let (job_id, spec) = self.current.take().ok_or(IoError::Unfinished)?;
                self.jobs.push(job_id);
                self.step = KeepStep::Kept;
                let key = resume_key(self.transfer_id, job_id);
                Ok(Next::Call(encoded_row(key, &FencedJob { spec, at })?))
            }
            KeepStep::Kept => {
                result.done()?;
                self.next_job()
            }
            KeepStep::Listed => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// What the pending call of listing a transfer's imports answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FencedStep {
    Kept,
    Open,
    Write,
}

/// The imports of `bucket` to hand over: those open now and those kept before the fence of
/// `transfer_id`, finished ones too, since a job may end while it skips the fenced range's keys.
#[derive(Debug, PartialEq)]
pub(super) struct FencedImportsOperation {
    bucket: String,
    transfer_id: Ulid,
    step: FencedStep,
    jobs: Vec<JobId>,
    open: VecDeque<(JobId, StagingJobSpec)>,
}

impl FencedImportsOperation {
    pub(super) fn new(bucket: &str, transfer_id: Ulid) -> Self {
        Self {
            bucket: bucket.to_string(),
            transfer_id,
            step: FencedStep::Kept,
            jobs: Vec::new(),
            open: VecDeque::new(),
        }
    }

    fn next_open(&mut self) -> Result<Next<Vec<JobId>>, ImportError> {
        while let Some((job_id, spec)) = self.open.pop_front() {
            if self.jobs.contains(&job_id) {
                continue;
            }
            self.jobs.push(job_id);
            self.step = FencedStep::Write;
            let key = resume_key(self.transfer_id, job_id);
            let job = FencedJob { spec, at: None };
            return Ok(Next::Call(encoded_row(key, &job)?));
        }
        Ok(Next::Done(std::mem::take(&mut self.jobs)))
    }
}

impl Operation for FencedImportsOperation {
    type Output = Vec<JobId>;
    type Error = ImportError;

    fn first(&mut self) -> Result<Next<Vec<JobId>>, ImportError> {
        Ok(Next::Call(state_read(jobs_key(self.transfer_id))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<JobId>>, ImportError> {
        match self.step {
            FencedStep::Kept => {
                let kept = value(result)?;
                let kept = kept.as_deref().map(postcard::from_bytes).transpose();
                self.jobs = kept.map_err(failed)?.unwrap_or_default();
                self.step = FencedStep::Open;
                let open = OpenImportsOperation::new(&self.bucket);
                Ok(Next::Call(IoCall::child(open)))
            }
            FencedStep::Open => {
                let open = result.child::<Vec<(JobId, StagingJobSpec)>, ImportError>()??;
                self.open = open.into();
                self.next_open()
            }
            FencedStep::Write => {
                result.done()?;
                self.next_open()
            }
        }
    }
}

/// What the pending call of serving a handed job answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SpecStep {
    Resume,
    Bucket,
    Record,
    Source,
}

/// A staging job of this node as it hands it over to a member of the set its bucket is bound
/// to, with its connector's source root and its position before the fence of `transfer_id`.
#[derive(Debug, PartialEq)]
pub(crate) struct StagingSpecOperation {
    peer: NodeId,
    transfer_id: Ulid,
    job_id: JobId,
    step: SpecStep,
    job: Option<FencedJob>,
}

impl StagingSpecOperation {
    pub(crate) fn new(peer: NodeId, (transfer_id, job_id): (Ulid, JobId)) -> Self {
        Self {
            peer,
            transfer_id,
            job_id,
            step: SpecStep::Resume,
            job: None,
        }
    }

    fn job(&self) -> Result<&FencedJob, MetadataReadError> {
        self.job.as_ref().ok_or(MetadataReadError::Unavailable)
    }

    fn handed(&mut self, source: GetSourceResult) -> Result<HandedImport, MetadataReadError> {
        let FencedJob { spec, at } = self.job.take().ok_or(MetadataReadError::Unavailable)?;
        let connector = source.connector;
        let shared = (!source.has_secret_config).then(|| connector.clone());
        let root = SourceRoot {
            kind: connector.kind,
            public_config: connector.source_config().into_iter().collect(),
            namespace: connector.namespace,
        };
        let resume = at.map(|at| handed_checkpoint(&spec, &at));
        Ok(HandedImport {
            spec,
            root,
            resume,
            shared,
        })
    }
}

impl Operation for StagingSpecOperation {
    type Output = HandedImport;
    type Error = MetadataReadError;

    fn first(&mut self) -> Result<Next<HandedImport>, MetadataReadError> {
        let key = resume_key(self.transfer_id, self.job_id);
        Ok(Next::Call(state_read(key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<HandedImport>, MetadataReadError> {
        match self.step {
            SpecStep::Resume => {
                let stored = value(result)?.ok_or(MetadataReadError::NotFound)?;
                let job: FencedJob = postcard::from_bytes(&stored)
                    .map_err(|_| MetadataReadError::Unavailable)?;
                let bucket = GetBucketOperation::new(job.spec.bucket.clone());
                self.job = Some(job);
                self.step = SpecStep::Bucket;
                Ok(Next::Call(IoCall::child(bucket)))
            }
            SpecStep::Bucket => {
                let bucket = (result.child::<BucketInfo, GetBucketError>()?)
                    .map_err(|_| MetadataReadError::NotFound)?;
                let binding = bucket.distribution.ok_or(MetadataReadError::Forbidden)?;
                self.step = SpecStep::Record;
                Ok(Next::Call(record_read(binding.set_id)))
            }
            SpecStep::Record => {
                let record = value(result)?.ok_or(MetadataReadError::Forbidden)?;
                let view = DistributionView::from_bytes(&record)
                    .map_err(|_| MetadataReadError::Unavailable)?;
                let members = &view.snapshot().config.members;
                if !members.iter().any(|member| member.node_id == self.peer) {
                    return Err(MetadataReadError::Forbidden);
                }
                let spec = &self.job()?.spec;
                let input = GetSourceInput {
                    group_id: spec.group_id,
                    connector_id: spec.connector_id,
                };
                self.step = SpecStep::Source;
                Ok(Next::Call(IoCall::child(GetSourceOperation::new(input))))
            }
            SpecStep::Source => {
                let source = (result.child::<GetSourceResult, GetSourceError>()?)
                    .map_err(|_| MetadataReadError::Unavailable)?;
                Ok(Next::Done(self.handed(source)?))
            }
        }
    }
}

/// What the pending call of fetching handed imports answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FetchStep {
    Kept,
    Peer,
    List,
    Share,
    Write,
    Last,
}

/// Fetches the former coordinator's open jobs of a range, bound to this node's connector of each
/// source root, and keeps them until this node coordinates the range. `true` once all are
/// fetched; an unanswered request or a root no connector here reads waits for the next pass.
#[derive(Debug, PartialEq)]
pub(super) struct FetchImportsOperation {
    set_id: Ulid,
    range_id: Ulid,
    transfer_id: Ulid,
    local: NodeId,
    from: NodeId,
    jobs: Vec<JobId>,
    step: FetchStep,
    kept: Option<Kept>,
    handed: Option<HandedImport>,
}

type Fetching = Result<Next<bool>, ImportError>;

impl FetchImportsOperation {
    pub(super) fn new(
        (set_id, range_id, transfer_id): (Ulid, Ulid, Ulid),
        local: NodeId,
        from: NodeId,
        jobs: Vec<JobId>,
    ) -> Self {
        Self {
            set_id,
            range_id,
            transfer_id,
            local,
            from,
            jobs,
            step: FetchStep::Kept,
            kept: None,
            handed: None,
        }
    }

    fn key(&self) -> Vec<u8> {
        handover_key(self.set_id, self.range_id)
    }

    fn kept(&mut self) -> Result<&mut Kept, IoError> {
        self.kept.as_mut().ok_or(IoError::Unfinished)
    }

    fn job(&mut self) -> Result<JobId, IoError> {
        self.kept()?.pending.first().copied().ok_or(IoError::Unfinished)
    }

    /// Asks for the next pending job, or writes what is kept once none is left.
    fn ask(&mut self) -> Fetching {
        let kept = self.kept()?;
        let Some(job_id) = kept.pending.first().copied() else {
            return self.finish();
        };
        let peer = kept.from;
        let message = MetadataTransportMessage::StagingSpec {
            transfer_id: self.transfer_id,
            job_id,
        };
        match peer_call(peer, &message) {
            Ok(call) => {
                self.step = FetchStep::Peer;
                Ok(Next::Call(call))
            }
            Err(_) => self.finish(),
        }
    }

    fn finish(&mut self) -> Fetching {
        let kept = self.kept.as_ref().ok_or(IoError::Unfinished)?;
        let row = encoded_row(self.key(), kept)?;
        self.step = FetchStep::Last;
        Ok(Next::Call(row))
    }

    fn answered(&mut self, answer: Result<HandedImport, MetadataReadError>) -> Fetching {
        match answer {
            Ok(handed) => {
                let group_id = handed.spec.group_id;
                self.handed = Some(handed);
                self.step = FetchStep::List;
                let list = ListSourceOperation::new(ListSourceInput { group_id });
                Ok(Next::Call(IoCall::child(list)))
            }
            // The former coordinator keeps each handed job while the transfer is open.
            Err(MetadataReadError::NotFound) => {
                let (set_id, job_id) = (self.set_id, self.job()?);
                warn!(%set_id, %job_id, "The former coordinator holds no handed-over import");
                self.advance()
            }
            Err(_) => self.finish(),
        }
    }

    fn listed(&mut self, connectors: Vec<SourceConnector>) -> Fetching {
        let handed = self.handed.as_ref().ok_or(IoError::Unfinished)?;
        let root = &handed.root;
        let matched = connectors.into_iter().find(|connector| {
            let config: BTreeMap<String, String> = connector.source_config().into_iter().collect();
            connector.kind == root.kind
                && config == root.public_config
                && connector.namespace == root.namespace
        });
        if let Some(connector) = matched {
            return self.bound(Some(connector.connector_id));
        }
        // A connector without credentials of the former coordinator is registered here.
        let group_id = handed.spec.group_id;
        let Some(shared) = handed.shared.as_ref().filter(|s| s.group_id == group_id) else {
            return self.bound(None);
        };
        let input = SourceConnectorInput {
            group_id,
            created_by: shared.created_by,
            name: shared.name.clone(),
            kind: shared.kind,
            public_config: shared.public_config.clone(),
            secret_config: Default::default(),
        };
        let create = SourceConnectorOperation::with_id(input, shared.connector_id);
        self.step = FetchStep::Share;
        Ok(Next::Call(IoCall::child(create)))
    }

    fn bound(&mut self, connector: Option<Ulid>) -> Fetching {
        let handed = self.handed.take().ok_or(IoError::Unfinished)?;
        let Some(connector_id) = connector else {
            let (set_id, job_id, kind) = (self.set_id, self.job()?, handed.root.kind);
            warn!(%set_id, %job_id, ?kind, "No connector here reads a handed-over import");
            return self.finish();
        };
        let spec = StagingJobSpec {
            node_id: self.local,
            connector_id,
            ..handed.spec
        };
        self.kept()?.ready.push((spec, handed.resume));
        self.advance()
    }

    fn advance(&mut self) -> Fetching {
        self.kept()?.pending.remove(0);
        let kept = self.kept.as_ref().ok_or(IoError::Unfinished)?;
        let row = encoded_row(self.key(), kept)?;
        self.step = FetchStep::Write;
        Ok(Next::Call(row))
    }
}

impl Operation for FetchImportsOperation {
    type Output = bool;
    type Error = ImportError;

    fn first(&mut self) -> Fetching {
        Ok(Next::Call(state_read(self.key())))
    }

    fn next(&mut self, result: IoResult) -> Fetching {
        match self.step {
            FetchStep::Kept => {
                self.kept = Some(match value(result)? {
                    Some(stored) => postcard::from_bytes(&stored).map_err(failed)?,
                    None => Kept {
                        from: self.from,
                        pending: std::mem::take(&mut self.jobs),
                        ready: Vec::new(),
                    },
                });
                self.ask()
            }
            FetchStep::Peer => {
                let answer = match peer_answer(result)? {
                    Ok(MetadataTransportMessage::StagingSpecResult { result }) => {
                        result.map(|spec| *spec)
                    }
                    _ => Err(MetadataReadError::Unavailable),
                };
                self.answered(answer)
            }
            FetchStep::List => {
                let listed = result.child::<ListSourceResult, ListSourceError>()?;
                self.listed(listed.map_err(failed)?.connectors)
            }
            FetchStep::Share => {
                let created = result.child::<SourceConnectorResult, SourceConnectorError>()?;
                match created {
                    Ok(created) => self.bound(Some(created.connector.connector_id)),
                    Err(error) => {
                        warn!(%error, "A handed-over import's connector cannot be registered");
                        self.bound(None)
                    }
                }
            }
            FetchStep::Write => {
                result.done()?;
                self.ask()
            }
            FetchStep::Last => {
                result.done()?;
                let kept = self.kept.as_ref().ok_or(IoError::Unfinished)?;
                Ok(Next::Done(kept.pending.is_empty()))
            }
        }
    }
}

/// Kept imports of a range this node now coordinates, ready to run as its own jobs.
#[derive(Debug, PartialEq)]
pub(crate) struct KeptStart {
    pub(super) key: Vec<u8>,
    pub(super) kept: Kept,
    pub(super) bucket: BucketInfo,
}

/// What the pending call of looking at handed imports answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContinueStep {
    Page,
    Record,
    Bucket,
    Delete,
}

/// Finds the kept imports of every range this node now coordinates; a transfer that ended
/// without this node drops them, one still on its way leaves them waiting.
#[derive(Debug, PartialEq)]
pub(super) struct ContinueImportsOperation {
    local: NodeId,
    step: ContinueStep,
    rows: VecDeque<(Key, Value)>,
    /// The handover row being worked on, with its range.
    current: Option<(Vec<u8>, Value, Ulid)>,
    kept: Option<Kept>,
    starts: Vec<KeptStart>,
}

type Continuing = Result<Next<Vec<KeptStart>>, ImportError>;

impl ContinueImportsOperation {
    pub(super) fn new(local: NodeId) -> Self {
        Self {
            local,
            step: ContinueStep::Page,
            rows: VecDeque::new(),
            current: None,
            kept: None,
            starts: Vec::new(),
        }
    }

    fn next_row(&mut self) -> Continuing {
        while let Some((key, value)) = self.rows.pop_front() {
            let Some((set_id, range_id)) = handover_parts(&key) else {
                continue;
            };
            self.current = Some((key.to_vec(), value, range_id));
            self.step = ContinueStep::Record;
            return Ok(Next::Call(record_read(set_id)));
        }
        Ok(Next::Done(std::mem::take(&mut self.starts)))
    }

    fn drop_row(&mut self) -> Continuing {
        let (key, ..) = self.current.take().ok_or(IoError::Unfinished)?;
        self.step = ContinueStep::Delete;
        Ok(Next::Call(state_delete(key)))
    }

    /// Decides the row from the record: run its jobs, keep waiting for the transfer, or drop it.
    fn recorded(&mut self, snapshot: &DistributionSnapshot) -> Continuing {
        let (_, value, range_id) = self.current.as_ref().ok_or(IoError::Unfinished)?;
        let range_id = *range_id;
        let local = self.local;
        let member = (snapshot.config.members.iter()).find(|member| member.node_id == local);
        let range = (snapshot.config.ranges.iter()).find(|range| range.range_id == range_id);
        let arriving = (snapshot.lifecycle.transfers.iter()).any(|open| {
            open.range_id == range_id && member.is_some_and(|m| open.to == m.member_id)
        });
        match (member, range) {
            (Some(member), Some(range)) if range.coordinator == member.member_id => {
                self.kept = Some(postcard::from_bytes(value).map_err(failed)?);
                let bucket = GetBucketOperation::new(snapshot.config.bucket.clone());
                self.step = ContinueStep::Bucket;
                Ok(Next::Call(IoCall::child(bucket)))
            }
            // The transfer is still on its way here; the kept jobs wait for it.
            _ if arriving => self.next_row(),
            _ => self.drop_row(),
        }
    }
}

impl Operation for ContinueImportsOperation {
    type Output = Vec<KeptStart>;
    type Error = ImportError;

    fn first(&mut self) -> Continuing {
        let page = IoCall::Page {
            space: NODE_STATE_KEYSPACE.to_string(),
            prefix: HANDOVER_PREFIX.to_vec().into(),
            after: None,
            limit: JOB_PAGE,
        };
        Ok(Next::Call(page))
    }

    fn next(&mut self, result: IoResult) -> Continuing {
        match self.step {
            ContinueStep::Page => {
                let (rows, _) = result.page()?;
                self.rows = rows.into();
                self.next_row()
            }
            ContinueStep::Record => {
                let Some(record) = value(result)? else {
                    return self.drop_row();
                };
                let view = DistributionView::from_bytes(&record).map_err(failed)?;
                self.recorded(view.snapshot())
            }
            ContinueStep::Bucket => {
                let info = result.child::<BucketInfo, GetBucketError>()?;
                let bucket = info.map_err(failed)?;
                let (key, ..) = self.current.take().ok_or(IoError::Unfinished)?;
                let kept = self.kept.take().ok_or(IoError::Unfinished)?;
                self.starts.push(KeptStart { key, kept, bucket });
                self.next_row()
            }
            ContinueStep::Delete => {
                result.done()?;
                self.next_row()
            }
        }
    }
}

#[cfg(test)]
#[path = "imports_tests.rs"]
mod tests;
