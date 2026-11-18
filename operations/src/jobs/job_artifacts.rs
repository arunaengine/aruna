//! Finds the artifact of an export job and opens its bytes, here or at the job's owner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::ops::Range;
use std::path::Path;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, JobControlEffect, NetEffect};
use aruna_core::events::{BlobEvent, Event, JobControlEvent, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::jobs::{JobRequest, JobResponse, WireArtifact, WireRange};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::rows::event;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::job::{
    ArtifactRef, ExportRoCrateSpec, JobId, JobPayload, JobRecord, JobResultPayload, JobState,
};
use bytes::Bytes;
use ulid::Ulid;

use super::job_reads::{
    JobOwnerOperation, JobReadError, OwnedJobOperation, document_read, document_row, internal,
    unavailable,
};
use super::job_routes::refused;
use super::lifecycle::family_reads::FamilyResponderOperation;
use super::records::pages::ServingNode;
use super::route::{JobRouteError, JobRouteOperation};
use super::store::mutation_error::JobMutationError;
use super::store::row_calls::read_artifact_tombstone;

/// An export job's artifact as its owner describes it.
#[derive(Debug, PartialEq)]
pub struct OwnedArtifact {
    pub job_id: JobId,
    pub created_by: UserId,
    pub blake3: [u8; 32],
    pub size: u64,
    pub filename: String,
    pub(crate) artifact: Option<ArtifactRef>,
}

impl OwnedArtifact {
    pub(crate) fn source(&self) -> Option<&ArtifactRef> {
        self.artifact.as_ref()
    }

    pub fn same_content(&self, other: &Self) -> bool {
        self.blake3 == other.blake3 && self.size == other.size
    }
}

impl From<&OwnedArtifact> for WireArtifact {
    fn from(owned: &OwnedArtifact) -> Self {
        Self {
            job_id: owned.job_id,
            created_by: owned.created_by,
            blake3: owned.blake3,
            size: owned.size,
            filename: owned.filename.clone(),
        }
    }
}

impl From<WireArtifact> for OwnedArtifact {
    fn from(owned: WireArtifact) -> Self {
        Self {
            job_id: owned.job_id,
            created_by: owned.created_by,
            blake3: owned.blake3,
            size: owned.size,
            filename: owned.filename,
            artifact: None,
        }
    }
}

/// What an artifact read found.
#[derive(Debug, PartialEq)]
pub enum ArtifactLookup {
    NotFound,
    Pending(JobState),
    Gone,
    Ready(OwnedArtifact),
}

/// The open bytes of one artifact range.
#[derive(Debug, PartialEq)]
pub struct ArtifactRead {
    pub blob: BackendStream<Result<Bytes, StreamError>>,
    pub stream_size: u64,
}

fn artifact_filename(document_path: Option<&str>, document_id: Ulid) -> String {
    let stem = document_path
        .and_then(|path| Path::new(path.trim_end_matches('/')).file_stem())
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| document_id.to_string());
    format!("{stem}.zip")
}

#[derive(Debug, PartialEq)]
enum OwnedStep {
    Job,
    Tombstone,
    Document(Box<JobRecord>, Box<ArtifactRef>, Ulid),
}

/// The artifact of the caller's own export job. A pruned job leaves a tombstone, so its owner
/// reads `Gone` until the artifact would have expired.
#[derive(Debug, PartialEq)]
pub struct OwnedArtifactOperation {
    user_id: UserId,
    job_id: JobId,
    now_ms: u64,
    step: OwnedStep,
}

type OwnedNext = Result<Next<ArtifactLookup>, JobReadError>;

fn export_spec(record: &JobRecord) -> Option<&ExportRoCrateSpec> {
    match &record.payload {
        JobPayload::ExportRoCrate(spec) => Some(spec),
        _ => None,
    }
}

/// An artifact is served only when its record and its blob location name the same bytes.
fn verified_hash(artifact: &ArtifactRef) -> Result<(), JobReadError> {
    let location_hash: [u8; 32] = artifact
        .location
        .get_blake3()
        .ok_or_else(|| JobReadError("artifact location is missing its BLAKE3 hash".to_string()))?
        .try_into()
        .map_err(|_| JobReadError("artifact location has an invalid BLAKE3 hash".to_string()))?;
    if location_hash != artifact.blake3 {
        return Err(JobReadError(
            "artifact record does not match its blob location".to_string(),
        ));
    }
    Ok(())
}

impl OwnedArtifactOperation {
    pub fn new(user_id: UserId, job_id: JobId, now_ms: u64) -> Self {
        Self {
            user_id,
            job_id,
            now_ms,
            step: OwnedStep::Job,
        }
    }

    fn found(&mut self, record: JobRecord) -> OwnedNext {
        let Some(document_id) = export_spec(&record).map(|spec| spec.document_id) else {
            return Ok(Next::Done(ArtifactLookup::NotFound));
        };
        if !record.state.is_terminal() {
            return Ok(Next::Done(ArtifactLookup::Pending(record.state)));
        }
        let Some(JobResultPayload::ExportRoCrate(result)) = &record.result else {
            return Ok(Next::Done(ArtifactLookup::NotFound));
        };
        let Some(artifact) = result.artifact.clone() else {
            return Ok(Next::Done(ArtifactLookup::NotFound));
        };
        if artifact.expires_at_ms <= self.now_ms {
            return Ok(Next::Done(ArtifactLookup::Gone));
        }
        verified_hash(&artifact)?;
        self.step = OwnedStep::Document(Box::new(record), Box::new(artifact), document_id);
        Ok(Next::Call(document_read(document_id)))
    }
}

impl Operation for OwnedArtifactOperation {
    type Output = ArtifactLookup;
    type Error = JobReadError;

    fn first(&mut self) -> OwnedNext {
        let read = OwnedJobOperation::new(self.user_id, self.job_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> OwnedNext {
        match std::mem::replace(&mut self.step, OwnedStep::Job) {
            OwnedStep::Job => match result.child::<Option<JobRecord>, JobReadError>()?? {
                Some(record) => self.found(record),
                None => {
                    self.step = OwnedStep::Tombstone;
                    let read = read_artifact_tombstone(self.job_id, self.now_ms);
                    Ok(Next::Call(IoCall::child(read)))
                }
            },
            OwnedStep::Tombstone => {
                let owner = result.child::<Option<UserId>, JobMutationError>()??;
                Ok(Next::Done(match owner {
                    Some(owner) if owner == self.user_id => ArtifactLookup::Gone,
                    _ => ArtifactLookup::NotFound,
                }))
            }
            OwnedStep::Document(record, artifact, document_id) => {
                let document_path = document_row(result)?.map(|record| record.document_path);
                let filename = artifact_filename(document_path.as_deref(), document_id);
                Ok(Next::Done(ArtifactLookup::Ready(OwnedArtifact {
                    job_id: record.job_id,
                    created_by: record.created_by,
                    blake3: artifact.blake3,
                    size: artifact.size,
                    artifact: Some(*artifact),
                    filename,
                })))
            }
        }
    }
}

/// Opens one byte range of a stored artifact.
#[derive(Debug, PartialEq)]
pub struct ArtifactRangeOperation {
    artifact: ArtifactRef,
    range: Range<u64>,
}

impl ArtifactRangeOperation {
    pub fn new(artifact: &ArtifactRef, range: Range<u64>) -> Self {
        Self {
            artifact: artifact.clone(),
            range,
        }
    }
}

impl Operation for ArtifactRangeOperation {
    type Output = ArtifactRead;
    type Error = JobReadError;

    fn first(&mut self) -> Result<Next<ArtifactRead>, JobReadError> {
        let read = BlobEffect::ReadHiddenRange {
            location: self.artifact.location.clone(),
            range: self.range.clone(),
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Blob(read)))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ArtifactRead>, JobReadError> {
        match event(result)? {
            Event::Blob(BlobEvent::HiddenRead { blob, stream_size }) => {
                Ok(Next::Done(ArtifactRead { blob, stream_size }))
            }
            Event::Blob(BlobEvent::Error(error)) => Err(JobReadError(error.to_string())),
            event => Err(JobReadError(format!(
                "unexpected hidden artifact read event: {}",
                event.label()
            ))),
        }
    }
}

fn artifact_size_matches(range: Option<&Range<u64>>, stream_size: u64) -> bool {
    match range {
        Some(range) => range.end.checked_sub(range.start) == Some(stream_size),
        None => stream_size == 0,
    }
}

fn artifact_job_matches(artifact: &OwnedArtifact, user_id: UserId, job_id: JobId) -> bool {
    artifact.job_id == job_id && artifact.created_by == user_id
}

#[derive(Debug, PartialEq)]
enum RouteStep {
    Responder,
    Owner,
    Local,
    Range(Box<ArtifactLookup>),
    Remote,
}

/// An artifact and, for a range, its open bytes, from the node that answers for the job.
#[derive(Debug, PartialEq)]
pub struct ArtifactRouteOperation {
    user_id: UserId,
    job_id: JobId,
    now_ms: u64,
    range: Option<Range<u64>>,
    token: Option<AuthToken>,
    serving: ServingNode,
    step: RouteStep,
}

/// An artifact lookup with the bytes a ranged read opened.
pub type RoutedArtifact = (ArtifactLookup, Option<ArtifactRead>);

type RouteNext = Result<Next<RoutedArtifact>, JobRouteError>;

impl ArtifactRouteOperation {
    pub fn new(
        user_id: UserId,
        job_id: JobId,
        now_ms: u64,
        range: Option<Range<u64>>,
        token: Option<AuthToken>,
        serving: ServingNode,
    ) -> Self {
        Self {
            user_id,
            job_id,
            now_ms,
            range,
            token,
            serving,
            step: RouteStep::Responder,
        }
    }

    fn local(&mut self) -> RouteNext {
        self.step = RouteStep::Local;
        let read = OwnedArtifactOperation::new(self.user_id, self.job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn at_owner(&mut self, owner: NodeId) -> RouteNext {
        if self.serving.map(|(local, _)| local) == Some(owner) {
            return self.local();
        }
        let auth_token = self.token.take().ok_or(JobRouteError::Unauthorized)?;
        self.step = RouteStep::Remote;
        let request = JobRequest::Artifact {
            auth_token,
            job_id: self.job_id,
            range: self.range.clone().map(WireRange::from),
        };
        let effect = JobControlEffect { owner, request };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Net(
            NetEffect::JobControl(Box::new(effect)),
        )))))
    }

    fn found(&mut self, lookup: ArtifactLookup) -> RouteNext {
        let (ArtifactLookup::Ready(owned), Some(range)) = (&lookup, self.range.clone()) else {
            return Ok(Next::Done((lookup, None)));
        };
        let artifact = owned
            .source()
            .ok_or_else(|| internal("local artifact source is unavailable"))?;
        let read = ArtifactRangeOperation::new(artifact, range);
        self.step = RouteStep::Range(Box::new(lookup));
        Ok(Next::Call(IoCall::child(read)))
    }

    fn answered(
        &self,
        response: JobResponse,
        body: Option<BackendStream<Result<Bytes, StreamError>>>,
    ) -> RouteNext {
        let done = |lookup| Ok(Next::Done((lookup, None)));
        match response {
            JobResponse::ArtifactPending(state) => done(ArtifactLookup::Pending(state)),
            JobResponse::ArtifactGone => done(ArtifactLookup::Gone),
            JobResponse::NotFound => done(ArtifactLookup::NotFound),
            JobResponse::ArtifactReady { owned, stream_size } => {
                let owned = OwnedArtifact::from(owned);
                if !artifact_job_matches(&owned, self.user_id, self.job_id) {
                    return Err(unavailable("job owner returned a mismatched artifact"));
                }
                let sized = artifact_size_matches(self.range.as_ref(), stream_size);
                let read = match (self.range.is_some(), body) {
                    (true, Some(blob)) if sized => Some(ArtifactRead { blob, stream_size }),
                    (false, None) if sized => None,
                    _ => {
                        return Err(unavailable(
                            "job owner returned a mismatched artifact stream",
                        ));
                    }
                };
                Ok(Next::Done((ArtifactLookup::Ready(owned), read)))
            }
            response => Err(refused(response, "artifact")),
        }
    }

    fn reply(&self, result: IoResult) -> RouteNext {
        match event(result)? {
            Event::Net(NetEvent::JobControl(JobControlEvent::Response(response))) => {
                self.answered(*response, None)
            }
            Event::Net(NetEvent::JobControl(JobControlEvent::Streamed { response, body })) => {
                self.answered(*response, Some(body))
            }
            Event::Net(NetEvent::JobControl(JobControlEvent::Unavailable(message))) => {
                Err(unavailable(format!("job owner unreachable: {message}")))
            }
            other => Err(unavailable(format!(
                "unexpected event while awaiting job response: {}",
                other.label()
            ))),
        }
    }
}

impl Operation for ArtifactRouteOperation {
    type Output = RoutedArtifact;
    type Error = JobRouteError;

    fn first(&mut self) -> RouteNext {
        if self.serving.is_none() {
            return self.local();
        }
        let responder = FamilyResponderOperation::new(self.job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(responder)))
    }

    fn next(&mut self, result: IoResult) -> RouteNext {
        match std::mem::replace(&mut self.step, RouteStep::Local) {
            RouteStep::Responder => match result.child::<Option<NodeId>, JobRouteError>()?? {
                Some(responder) => self.at_owner(responder),
                None => {
                    self.step = RouteStep::Owner;
                    let owner = JobOwnerOperation::new(self.job_id, self.serving);
                    Ok(Next::Call(IoCall::child(owner)))
                }
            },
            RouteStep::Owner => {
                let owner = result.child::<NodeId, JobRouteError>()??;
                self.at_owner(owner)
            }
            RouteStep::Local => {
                let lookup = result
                    .child::<ArtifactLookup, JobReadError>()?
                    .map_err(internal)?;
                self.found(lookup)
            }
            RouteStep::Range(lookup) => {
                let read = result
                    .child::<ArtifactRead, JobReadError>()?
                    .map_err(internal)?;
                if !artifact_size_matches(self.range.as_ref(), read.stream_size) {
                    return Err(internal(
                        "artifact reader returned an unexpected range size",
                    ));
                }
                Ok(Next::Done((*lookup, Some(read))))
            }
            RouteStep::Remote => self.reply(result),
        }
    }

    fn expected_error(error: &JobRouteError) -> bool {
        JobRouteOperation::expected_error(error)
    }
}

#[cfg(test)]
#[path = "job_artifacts_tests.rs"]
mod tests;
