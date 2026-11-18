//! Opens the input streams of one execution attempt: each read straight from the object it
//! names at the version the launch pinned, here or from a legal holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::TaskInput;
use aruna_core::effects::Effect;
use aruna_core::errors::StagingSourceError;
use aruna_core::native_reference::GrantedBody;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    CapturedInput, ExecutionSpec, InputSelection, InputSource, JobError, JobId, JobRecord,
};
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::HashIndex;
use aruna_core::structs::storage::distribution::source_root::{RootEntry, SourceRoot};
use aruna_core::structs::storage::origin::SourcePin;
use aruna_core::structs::storage::read_grant::READ_OPERATION;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::types::GroupId;
use futures_util::StreamExt;
use tracing::warn;
use ulid::Ulid;

use super::credential::RunIdentity;
use super::workspace::{
    SourceObject, StagedSource, device_read_error, local_copies, source_input_error, source_object,
};
use crate::blob::permission_paths::{ResolvePathsError, ResolvePathsOperation};
use crate::connectors::root_record::{parse_root, read_root};
use crate::jobs::lifecycle::stage::{StageError, StageInputOperation, StagedInput};
use crate::jobs::read_access::{SourceReadError, SourceReadOperation};
use crate::jobs::store::job_calls::change_job;
use crate::jobs::store::store_call::StoreCall;
use crate::jobs::store::{JobMutation, JobMutationError};
use crate::replication::bao_read::{BaoReadError, BaoReadOutput};
use crate::replication::managed_read::{LocalUserOperation, ManagedReadOperation};
use crate::replication::protocol::{BaoReadRequest, BaoReadTarget};
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation, GetObjectResult};
use crate::staging::granted_read::GrantedReadOperation;

/// The creator as admitted: submissions refuse path restricted credentials.
fn creator_auth(run: &RunIdentity) -> AuthContext {
    AuthContext {
        user_id: run.user_id,
        realm_id: run.realm_id,
        path_restrictions: None,
        session: None,
    }
}

#[derive(Debug, PartialEq)]
enum SourceStep {
    Role(aruna_core::NodeId),
    Authorize,
    Read,
    Copies,
    CopyAuthorize(SourceObject),
    CopyRead,
    Stage,
    Granted,
    Device(String, String),
}

type SourceNext = Result<Next<StagedSource>, JobError>;

/// Finds the bytes of one input: the local copy when this node holds it, and the exact stored
/// version from a legal holder when it does not.
#[derive(Debug, PartialEq)]
pub struct InputSourceOperation {
    run: RunIdentity,
    record: Arc<JobRecord>,
    input: InputSelection,
    source: SourceObject,
    /// Why the named object could not be read here; it decides when no holder serves either.
    failure: Option<GetObjectError>,
    copies: VecDeque<HashIndex>,
    step: SourceStep,
}

impl InputSourceOperation {
    fn new(
        run: RunIdentity,
        record: Arc<JobRecord>,
        input: InputSelection,
        source: SourceObject,
    ) -> Self {
        Self {
            run,
            record,
            input,
            source,
            failure: None,
            copies: VecDeque::new(),
            step: SourceStep::Authorize,
        }
    }

    fn captured(&self) -> Option<&CapturedInput> {
        (self.record.captured_inputs.iter())
            .find(|captured| captured.destination_key == self.input.dest_key)
    }

    fn call(&mut self, step: SourceStep, call: IoCall) -> SourceNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    /// Fails with the local read's error when there was one: it says more than the fallback.
    fn fail(&mut self, error: JobError) -> SourceNext {
        Err(match self.failure.take() {
            Some(failure) => source_input_error(failure),
            None => error,
        })
    }

    /// Authorizes the creator like an S3 GET of the object under its bucket's own group.
    fn authorize(&mut self, step: SourceStep, bucket: &str, key: &str) -> SourceNext {
        let auth = creator_auth(&self.run);
        let object = (bucket, key);
        let read = SourceReadOperation::of_object(&auth, self.run.node_id, object, READ_OPERATION);
        self.call(step, IoCall::child(read))
    }

    fn get_call(&self, source: &SourceObject, group_id: GroupId) -> IoCall {
        IoCall::child(GetObjectOperation::new(GetObjectInput {
            bucket: source.bucket.clone(),
            key: source.key.clone(),
            version_id: source.version,
            range: None,
            group_id,
            user_identity: self.run.user_id,
            realm_id: self.run.realm_id,
            node_id: self.run.node_id,
        }))
    }

    /// Stages one exact version from a legal holder. The stored version and hash are resolved
    /// first, so a remote read can never substitute other bytes.
    fn remote(&mut self) -> SourceNext {
        let Some(captured) = self.captured().cloned() else {
            return self.fail(JobError::permanent("captured inputs are missing"));
        };
        let InputSource::S3 { version_id, .. } = &self.input.source;
        let version = version_id.as_deref().map(Ulid::from_string).transpose();
        let Ok(version) = version else {
            return self.fail(JobError::permanent("input version is invalid"));
        };
        // The record's source is the holder the plan picked, possibly any node with a
        // registered copy: only the pinned version must match; hash and size bind.
        if version != Some(captured.version_id) {
            let error = "captured remote input does not match the physical input";
            return self.fail(JobError::permanent(error));
        }
        let serving = Some((self.run.node_id, self.run.realm_id));
        let Some(blake3) = captured.content.blake3() else {
            let Some(grant) = captured.grant.as_ref() else {
                let error = "a reference input on another node has no read grant";
                return self.fail(JobError::permanent(error));
            };
            let read =
                GrantedReadOperation::new(serving, captured.source_node_id, grant, None, false);
            return self.call(SourceStep::Granted, IoCall::child(read));
        };
        let stage = StageInputOperation::new(
            &self.record,
            &self.input,
            captured.version_id,
            blake3,
            serving,
        );
        self.call(SourceStep::Stage, IoCall::child(stage))
    }

    /// One exact realm version a device fetches for itself. It plans nothing, so the request's
    /// own version is the only authority, and the bytes land in the run's workspace as a local
    /// copy, never as a reference. A device is never a managed-copy destination.
    fn device(&mut self, holder: aruna_core::NodeId) -> SourceNext {
        let InputSource::S3 {
            bucket,
            key,
            version_id,
        } = self.input.source.clone();
        let version = version_id
            .as_deref()
            .map(Ulid::from_string)
            .transpose()
            .map_err(|_| {
                JobError::permanent(format!("input version is invalid for {bucket}/{key}"))
            })?
            .ok_or_else(|| {
                JobError::permanent(format!("realm input {bucket}/{key} names no exact version"))
            })?;
        let realm_id = self.run.realm_id;
        let request = BaoReadRequest {
            auth_context: creator_auth(&self.run),
            realm_id,
            target: BaoReadTarget::ExactVersion(VersionedObjectArn {
                realm_id,
                node_id: holder,
                bucket: bucket.clone(),
                key: key.clone(),
                version,
            }),
            expected_blake3: None,
            metadata_only: false,
            destination: None,
            known_refs: Vec::new(),
            range: None,
        };
        let read = ManagedReadOperation::new(holder, request);
        self.call(SourceStep::Device(bucket, key), IoCall::child(read))
    }

    /// The planner credits a node holding a registered copy of the bytes, so the same content
    /// under another key is read here instead of fetched again.
    fn local_read_failed(&mut self, failure: GetObjectError) -> SourceNext {
        self.failure = Some(failure);
        let Some(captured) = self.captured() else {
            return self.fail(JobError::retryable("input is not readable"));
        };
        let Some(blake3) = captured.content.blake3() else {
            return self.remote();
        };
        let paths = ResolvePathsOperation::new(blake3);
        self.call(SourceStep::Copies, IoCall::child(paths))
    }

    /// Tries the next local copy; every copy is authorized like the named input.
    fn next_copy(&mut self) -> SourceNext {
        let Some(candidate) = self.copies.pop_front() else {
            return self.remote();
        };
        let copy = SourceObject {
            path: self.source.path.clone(),
            bucket: candidate.bucket.clone(),
            key: candidate.key.clone(),
            version: Some(candidate.version_id),
        };
        let (bucket, key) = (copy.bucket.clone(), copy.key.clone());
        self.authorize(SourceStep::CopyAuthorize(copy), &bucket, &key)
    }

    fn sized(&mut self, size: u64, expected: u64, source: StagedSource) -> SourceNext {
        if size != expected {
            let error = "staged input size differs from the captured input";
            return self.fail(JobError::permanent(error));
        }
        Ok(Next::Done(source))
    }
}

fn authorize_error(source: &SourceObject, error: SourceReadError) -> JobError {
    match error {
        SourceReadError::NotFound => {
            JobError::permanent(format!("input bucket {} not found", source.bucket))
        }
        SourceReadError::Denied => JobError::permanent(format!(
            "input {}/{} access denied",
            source.bucket, source.key
        )),
        SourceReadError::Unavailable(error) => {
            JobError::retryable(format!("input authorization failed: {error}"))
        }
    }
}

impl Operation for InputSourceOperation {
    type Output = StagedSource;
    type Error = JobError;

    fn first(&mut self) -> SourceNext {
        let node_id = self.run.node_id;
        // A forwarded plan validated the source at its ingress endpoint already; a device
        // plans nothing, so its own request is the only authority it has.
        if let Some(holder) = self.input.source_node_id.filter(|node| *node != node_id) {
            if self.captured().is_some() {
                return self.remote();
            }
            let role = LocalUserOperation::new(self.run.realm_id);
            return self.call(SourceStep::Role(holder), IoCall::child(role));
        }
        let (bucket, key) = (self.source.bucket.clone(), self.source.key.clone());
        self.authorize(SourceStep::Authorize, &bucket, &key)
    }

    fn next(&mut self, result: IoResult) -> SourceNext {
        match std::mem::replace(&mut self.step, SourceStep::Read) {
            SourceStep::Role(holder) => match result.child::<bool, IoError>()? {
                Ok(true) => self.device(holder),
                _ => Err(JobError::permanent("captured inputs are missing")),
            },
            SourceStep::Authorize => {
                let group = result
                    .child::<GroupId, SourceReadError>()?
                    .map_err(|error| authorize_error(&self.source, error))?;
                let read = self.get_call(&self.source, group);
                self.call(SourceStep::Read, read)
            }
            SourceStep::Read => match result.child::<GetObjectResult, GetObjectError>()? {
                Ok(get) => Ok(Next::Done(StagedSource::from_local(get))),
                Err(failure) => self.local_read_failed(failure),
            },
            SourceStep::Copies => {
                let candidates = result.child::<Vec<HashIndex>, ResolvePathsError>()?;
                let candidates = candidates.unwrap_or_default();
                let (realm_id, node_id) = (self.run.realm_id, self.run.node_id);
                self.copies = local_copies(candidates, realm_id, node_id).into();
                self.next_copy()
            }
            SourceStep::CopyAuthorize(copy) => match result.child::<GroupId, SourceReadError>()? {
                Ok(group) => {
                    let read = self.get_call(&copy, group);
                    self.call(SourceStep::CopyRead, read)
                }
                Err(_) => self.next_copy(),
            },
            SourceStep::CopyRead => match result.child::<GetObjectResult, GetObjectError>()? {
                Ok(get) => Ok(Next::Done(StagedSource::from_local(get))),
                Err(_) => self.next_copy(),
            },
            SourceStep::Stage => match result.child::<StagedInput, StageError>()? {
                Ok(staged) => {
                    let expected = self.captured().map_or(staged.size, |input| input.bytes);
                    let source = StagedSource {
                        blob: staged.blob,
                        location: None,
                        size: Some(expected),
                        served_source: None,
                    };
                    self.sized(staged.size, expected, source)
                }
                Err(StageError(error)) => self.fail(error),
            },
            SourceStep::Granted => match result.child::<GrantedBody, StagingSourceError>()? {
                Ok(body) => {
                    let size = body.metadata.content_length;
                    let expected = self.captured().map_or(size, |input| input.bytes);
                    let source = StagedSource {
                        blob: body.blob,
                        location: None,
                        size: Some(expected),
                        served_source: None,
                    };
                    self.sized(size, expected, source)
                }
                Err(error @ (StagingSourceError::AccessDenied | StagingSourceError::NotFound)) => {
                    self.fail(JobError::permanent(format!(
                        "reference input read refused: {error}"
                    )))
                }
                Err(error) => self.fail(JobError::retryable(format!(
                    "reference input read failed: {error}"
                ))),
            },
            SourceStep::Device(bucket, key) => {
                match result.child::<BaoReadOutput, BaoReadError>()? {
                    Ok(BaoReadOutput::Stream { blob, size, .. }) => Ok(Next::Done(StagedSource {
                        blob,
                        location: None,
                        size: Some(size),
                        served_source: None,
                    })),
                    Ok(BaoReadOutput::Metadata { .. }) => Err(JobError::retryable(format!(
                        "staging {bucket}/{key} received no bytes"
                    ))),
                    Err(error) => Err(device_read_error(&bucket, &key, error)),
                }
            }
        }
    }
}

/// Reads the stored root of a source; a root that cannot be read reads as absent.
#[derive(Debug, PartialEq)]
pub struct SourceRootOperation {
    digest: [u8; 32],
}

impl SourceRootOperation {
    pub fn new(digest: [u8; 32]) -> Self {
        Self { digest }
    }
}

impl Operation for SourceRootOperation {
    type Output = Option<SourceRoot>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        let read = Effect::Storage(read_root(self.digest, None));
        Ok(Next::Call(IoCall::Effect(Box::new(read))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        Ok(Next::Done(parse_root(result.event()?).ok().flatten()))
    }
}

/// The pin of the source that served a reference, with the settings of its source root put
/// back, so a public source is named by its URL; without its root it stays unnamed.
pub(super) fn serving_pin(source: &VersionSourceBinding, root: Option<&SourceRoot>) -> SourcePin {
    let full = root.and_then(|root| RootEntry::hydrate(source, root).ok().flatten());
    if full.is_none() {
        warn!("The source root of a served input is missing; its pin names no URL");
    }
    SourcePin::served(&full.as_ref().unwrap_or(source).descriptor)
}

/// Stores which source served each input of the claim's preparation.
pub(super) fn keep_served(
    job_id: JobId,
    token: Ulid,
    served: BTreeMap<String, SourcePin>,
) -> StoreCall<JobRecord> {
    change_job(job_id, move |record| {
        if record.claim.as_ref().map(|claim| claim.claim_token) != Some(token) {
            return Err(JobMutationError::TokenMismatch);
        }
        let mut changed = false;
        for input in &mut record.captured_inputs {
            let pin = served.get(&input.destination_key).cloned();
            changed |= input.served_by != pin;
            input.served_by = pin;
        }
        Ok(match changed {
            true => JobMutation::Persist,
            false => JobMutation::Skip,
        })
    })
}

#[derive(Debug, PartialEq)]
enum LoadStep {
    Source(SourceObject, String),
    Root {
        file: Box<PendingFile>,
        binding: Box<VersionSourceBinding>,
    },
    Served,
}

/// One input whose stream is open and whose serving source is still being named.
#[derive(Debug, PartialEq)]
struct PendingFile {
    dest_key: String,
    path: String,
    size: u64,
    source: StagedSource,
}

/// Opens one un-consumed stream per declared input that is not mounted, so peak memory stays
/// bounded by a chunk, and keeps which source served each reference on the job.
#[derive(Debug, PartialEq)]
pub struct LoadInputsOperation {
    run: RunIdentity,
    record: Arc<JobRecord>,
    disk_budget: Option<u64>,
    pending: VecDeque<InputSelection>,
    files: Vec<TaskInput>,
    served: BTreeMap<String, SourcePin>,
    total_bytes: u64,
    step: LoadStep,
}

type LoadNext = Result<Next<Vec<TaskInput>>, JobError>;

impl LoadInputsOperation {
    pub fn new(
        run: RunIdentity,
        spec: &ExecutionSpec,
        record: Arc<JobRecord>,
        mounted: &BTreeSet<String>,
    ) -> Self {
        let pending: VecDeque<_> = (spec.inputs.iter())
            .filter(|input| !mounted.contains(&input.dest_key))
            .cloned()
            .collect();
        Self {
            run,
            record,
            disk_budget: spec.resources.disk_bytes,
            files: Vec::with_capacity(pending.len()),
            pending,
            served: BTreeMap::new(),
            total_bytes: 0,
            step: LoadStep::Served,
        }
    }

    fn next_input(&mut self) -> LoadNext {
        let Some(input) = self.pending.pop_front() else {
            return self.keep_served();
        };
        let source = source_object(&input)?;
        let dest_key = input.dest_key.clone();
        let find = InputSourceOperation::new(
            self.run,
            self.record.clone(),
            input,
            SourceObject {
                path: source.path.clone(),
                bucket: source.bucket.clone(),
                key: source.key.clone(),
                version: source.version,
            },
        );
        self.step = LoadStep::Source(source, dest_key);
        Ok(Next::Call(IoCall::child(find)))
    }

    /// Checks the size against the run's disk request, which bounds what it may stage.
    fn found(
        &mut self,
        object: SourceObject,
        dest_key: String,
        mut source: StagedSource,
    ) -> LoadNext {
        let size = (source.location.as_ref())
            .map(|location| location.blob_size)
            .or(source.size)
            .ok_or_else(|| {
                JobError::retryable(format!(
                    "input {}/{} has no size",
                    object.bucket, object.key
                ))
            })?;
        self.total_bytes = self
            .total_bytes
            .checked_add(size)
            .ok_or_else(|| JobError::permanent("staged input bytes overflow"))?;
        if let Some(budget) = self.disk_budget
            && self.total_bytes > budget
        {
            return Err(JobError::permanent(format!(
                "staged inputs need more than {} bytes, the run requests {budget}",
                self.total_bytes
            )));
        }
        let served = source.served_source.take();
        let file = PendingFile {
            dest_key,
            path: object.path,
            size,
            source,
        };
        let Some(binding) = served else {
            return self.add_file(file, None);
        };
        let Some(digest) = binding.descriptor.root else {
            let pin = SourcePin::served(&binding.descriptor);
            return self.add_file(file, Some(pin));
        };
        self.step = LoadStep::Root {
            file: Box::new(file),
            binding: Box::new(binding),
        };
        Ok(Next::Call(IoCall::child(SourceRootOperation::new(digest))))
    }

    fn add_file(&mut self, file: PendingFile, pin: Option<SourcePin>) -> LoadNext {
        if let Some(pin) = pin {
            self.served.insert(file.dest_key, pin);
        }
        let stream = file
            .source
            .blob
            .map(|chunk| chunk.map_err(std::io::Error::other));
        self.files.push(TaskInput::from_stream(
            file.path,
            file.size,
            Box::pin(stream),
        ));
        self.next_input()
    }

    /// Keeps which source delivered each staged reference of this preparation and clears the
    /// pins earlier preparations left. Only the claim that prepared writes.
    fn keep_served(&mut self) -> LoadNext {
        let Some(token) = self.record.claim.as_ref().map(|claim| claim.claim_token) else {
            return Ok(Next::Done(std::mem::take(&mut self.files)));
        };
        let served = std::mem::take(&mut self.served);
        let keep = keep_served(self.record.job_id, token, served);
        self.step = LoadStep::Served;
        Ok(Next::Call(IoCall::child(keep)))
    }
}

impl Operation for LoadInputsOperation {
    type Output = Vec<TaskInput>;
    type Error = JobError;

    fn first(&mut self) -> LoadNext {
        self.next_input()
    }

    fn next(&mut self, result: IoResult) -> LoadNext {
        match std::mem::replace(&mut self.step, LoadStep::Served) {
            LoadStep::Source(object, dest_key) => {
                let source = result.child::<StagedSource, JobError>()??;
                self.found(object, dest_key, source)
            }
            LoadStep::Root { file, binding } => {
                let root = result.child::<Option<SourceRoot>, IoError>()?;
                let pin = serving_pin(&binding, root.ok().flatten().as_ref());
                self.add_file(*file, Some(pin))
            }
            LoadStep::Served => {
                result
                    .child::<JobRecord, JobMutationError>()?
                    .map_err(|error| {
                        JobError::retryable(format!("served sources not kept: {error}"))
                    })?;
                Ok(Next::Done(std::mem::take(&mut self.files)))
            }
        }
    }
}

#[cfg(test)]
#[path = "inputs_tests.rs"]
mod tests;
