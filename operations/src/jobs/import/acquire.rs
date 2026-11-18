//! Brings an import's source into a hidden blob: a claimed upload, a repository record, or an
//! object or connector file spooled through this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::jobs::{ByteStream, JobEffect, JobEvent};
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::repository::RepositoryPull;
use aruna_core::structs::execution::job::{
    ImportRoCrateSource, RoCrateMediaType, RoCrateUploadRecord,
};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::secondary_id::SecondaryIdentifier;
use aruna_core::structs::storage::blob::{CONTENT_TYPE_KEY, object_permission_path};
use ulid::Ulid;

use super::upload::{ClaimUploadOperation, UploadClaimError};
use super::{
    ImportFailure, ImportInput, ImportRun, bucket_call, bucket_result, classify_blob, classify_get,
    classify_read, hidden_delete, job_call, permission_call, permission_result, precheck_size,
    source_permission_path, source_uses_eln, stopped_failure, stops_of, transfer_failure,
    unexpected,
};
use crate::harvest::read_connector::ConnectorView;
use crate::jobs::export::CrateJsonOperation;
use crate::jobs::repository::connector::ConnectorViewOperation;
use crate::jobs::repository::invenio::import::{AcquireOperation, AcquireRequest, Acquired};
use crate::jobs::repository::pull::PullProgress;
use crate::jobs::repository::{Action, TransferError, ensure_supported};
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation, GetObjectResult};
use crate::staging::read_source::{
    ReadSourceError, ReadSourceInput, ReadSourceOperation, ReadSourceResult,
};

/// What acquiring a source leaves.
#[derive(Debug, PartialEq)]
pub(super) struct AcquiredSource {
    pub(super) input: ImportInput,
    pub(super) identifiers: Vec<SecondaryIdentifier>,
    pub(super) pull: Option<PullProgress>,
}

#[derive(Debug, PartialEq)]
enum Step {
    Connector,
    Current(aruna_core::structs::execution::harvest::RepositoryConnectorKind),
    Repository,
    Claim,
    Bucket,
    ObjectAllowed(Ulid),
    Object,
    SourceAllowed,
    Source,
    Spool,
}

/// Acquires the source the job's spec names.
#[derive(Debug, PartialEq)]
pub(super) struct AcquireSourceOperation {
    run: ImportRun,
    now_ms: u64,
    step: Step,
}

type Turn = Result<Next<AcquiredSource>, ImportFailure>;

impl AcquireSourceOperation {
    pub(super) fn new(run: ImportRun) -> Self {
        Self {
            run,
            now_ms: 0,
            step: Step::Connector,
        }
    }

    fn acquire(
        &mut self,
        kind: aruna_core::structs::execution::harvest::RepositoryConnectorKind,
        current: Option<(String, Ulid)>,
    ) -> Turn {
        let spec = self.run.spec.clone();
        let ImportRoCrateSource::Repository {
            group_id,
            connector_id,
            record_id,
            options,
            pull,
        } = &spec.source
        else {
            return Err(wrong_source());
        };
        self.step = Step::Repository;
        let acquire = AcquireOperation::new(AcquireRequest {
            kind,
            scope: self.run.scope.clone(),
            job: self.run.job.clone(),
            spec: (*spec).clone(),
            group_id: *group_id,
            connector_id: *connector_id,
            record_id: record_id.clone(),
            options: options.clone(),
            pull: pull.clone(),
            current,
        });
        Ok(Next::Call(IoCall::child(acquire)))
    }

    fn spool(&mut self, stream: ByteStream, expected_size: Option<u64>, eln: bool) -> Turn {
        self.step = Step::Spool;
        let spool = SpoolSourceOperation {
            run: self.run.clone(),
            stream: Some(stream),
            expected_size,
            eln,
            spooled: None,
            step: SpoolStep::Guard,
        };
        Ok(Next::Call(IoCall::child(spool)))
    }
}

fn wrong_source() -> ImportFailure {
    ImportFailure::Retryable("the import source changed while it was read".to_string())
}

fn acquired(input: ImportInput) -> Next<AcquiredSource> {
    Next::Done(AcquiredSource {
        input,
        identifiers: Vec::new(),
        pull: None,
    })
}

impl Operation for AcquireSourceOperation {
    type Output = AcquiredSource;
    type Error = ImportFailure;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn {
        let spec = self.run.spec.clone();
        let auth = &spec.auth_context;
        match &spec.source {
            ImportRoCrateSource::Repository {
                group_id,
                connector_id,
                ..
            } => {
                self.step = Step::Connector;
                let view = ConnectorViewOperation::new(*group_id, *connector_id);
                Ok(Next::Call(IoCall::child(view)))
            }
            ImportRoCrateSource::Upload { upload_id } => {
                self.step = Step::Claim;
                let (upload_id, owner) = (*upload_id, auth.user_id);
                let (job_id, now_ms) = (self.run.job_id, self.now_ms);
                let build = move || ClaimUploadOperation::new(upload_id, owner, job_id, now_ms);
                let seed = upload_id.to_bytes();
                let claim = boxed_retry(build, &seed, |result| {
                    Event::Child(ChildOutcome::new(result))
                });
                Ok(Next::Call(IoCall::Child(claim)))
            }
            ImportRoCrateSource::Object { bucket, .. } => {
                self.step = Step::Bucket;
                Ok(Next::Call(bucket_call(bucket)))
            }
            ImportRoCrateSource::Connector {
                group_id,
                connector_id,
                path,
            } => {
                self.step = Step::SourceAllowed;
                let allowed = source_permission_path(
                    auth.realm_id,
                    *group_id,
                    self.run.owner_node,
                    *connector_id,
                    path,
                );
                Ok(Next::Call(permission_call(auth, allowed, Permission::READ)))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> Turn {
        let spec = self.run.spec.clone();
        let auth = &spec.auth_context;
        let limit_source = &spec.source;
        match std::mem::replace(&mut self.step, Step::Spool) {
            Step::Connector => {
                let view =
                    (result.child::<ConnectorView, TransferError>()?).map_err(transfer_failure)?;
                let kind = view.connector.kind;
                ensure_supported(kind, Action::Import).map_err(transfer_failure)?;
                let ImportRoCrateSource::Repository { pull, .. } = limit_source else {
                    return Err(wrong_source());
                };
                if pull.is_some() {
                    ensure_supported(kind, Action::Pull).map_err(transfer_failure)?;
                }
                // A pull update merges into the dataset's current crate.
                if matches!(pull, Some(RepositoryPull::Update { .. })) {
                    self.step = Step::Current(kind);
                    let read = CrateJsonOperation::new(
                        self.run.local_node,
                        auth.clone(),
                        spec.document_id,
                        spec.limits.metadata_bytes,
                    );
                    return Ok(Next::Call(IoCall::child(read)));
                }
                self.acquire(kind, None)
            }
            Step::Current(kind) => {
                let current =
                    (result.child::<(String, Ulid), TransferError>()?).map_err(transfer_failure)?;
                self.acquire(kind, Some(current))
            }
            Step::Repository => {
                let (artifact, identifiers, pull) =
                    (result.child::<Acquired, TransferError>()?).map_err(transfer_failure)?;
                Ok(Next::Done(AcquiredSource {
                    input: ImportInput {
                        location: artifact.location,
                        size: artifact.size,
                        blake3: artifact.blake3,
                        upload_id: None,
                        eln: false,
                    },
                    identifiers,
                    pull,
                }))
            }
            Step::Claim => {
                let claim = result.child::<RoCrateUploadRecord, UploadClaimError>()?;
                let record = claim.map_err(|error| match error {
                    UploadClaimError::Storage(message) => ImportFailure::Retryable(message),
                    other => ImportFailure::Permanent(other.to_string()),
                })?;
                Ok(acquired(ImportInput {
                    location: record.location,
                    size: record.size,
                    blake3: record.blake3,
                    upload_id: Some(record.upload_id),
                    eln: record.media_type == RoCrateMediaType::Eln,
                }))
            }
            Step::Bucket => {
                let ImportRoCrateSource::Object { bucket, key, .. } = limit_source else {
                    return Err(wrong_source());
                };
                let info = bucket_result(bucket, result)?;
                self.step = Step::ObjectAllowed(info.group_id);
                let path = object_permission_path(
                    auth.realm_id,
                    info.group_id,
                    self.run.owner_node,
                    bucket,
                    key,
                );
                Ok(Next::Call(permission_call(auth, path, Permission::READ)))
            }
            Step::ObjectAllowed(group_id) => {
                permission_result(result)?;
                let ImportRoCrateSource::Object {
                    bucket,
                    key,
                    version,
                } = limit_source
                else {
                    return Err(wrong_source());
                };
                self.step = Step::Object;
                let get = GetObjectOperation::new(GetObjectInput {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: *version,
                    range: None,
                    group_id,
                    user_identity: auth.user_id,
                    realm_id: auth.realm_id,
                    node_id: self.run.owner_node,
                })
                .with_restrictions(auth.path_restrictions.clone());
                Ok(Next::Call(IoCall::child(get)))
            }
            Step::Object => {
                let ImportRoCrateSource::Object { key, .. } = limit_source else {
                    return Err(wrong_source());
                };
                let object =
                    (result.child::<GetObjectResult, GetObjectError>()?).map_err(classify_get)?;
                let source_metadata = object.source_metadata.as_ref();
                let expected = (object.location.as_ref())
                    .map(|location| location.blob_size)
                    .or_else(|| source_metadata.map(|metadata| metadata.content_length));
                let content_type = source_metadata
                    .and_then(|metadata| metadata.content_type.as_deref())
                    .or_else(|| object.metadata.get(CONTENT_TYPE_KEY).map(String::as_str));
                let eln = source_uses_eln(key, content_type);
                self.spool(object.blob, expected, eln)
            }
            Step::SourceAllowed => {
                permission_result(result)?;
                let ImportRoCrateSource::Connector {
                    group_id,
                    connector_id,
                    path,
                } = limit_source
                else {
                    return Err(wrong_source());
                };
                self.step = Step::Source;
                let read = ReadSourceOperation::new(ReadSourceInput {
                    group_id: *group_id,
                    connector_id: *connector_id,
                    source_path: path.clone(),
                    range: None,
                    expected: None,
                });
                Ok(Next::Call(IoCall::child(read)))
            }
            Step::Source => {
                let ImportRoCrateSource::Connector { path, .. } = limit_source else {
                    return Err(wrong_source());
                };
                let read = (result.child::<ReadSourceResult, ReadSourceError>()?)
                    .map_err(classify_read)?;
                let metadata = &read.metadata;
                let eln = source_uses_eln(path, metadata.content_type.as_deref());
                let expected = Some(metadata.content_length);
                self.spool(read.stream, expected, eln)
            }
            Step::Spool => Ok(acquired(result.child::<ImportInput, ImportFailure>()??)),
        }
    }
}

#[derive(Debug, PartialEq)]
enum SpoolStep {
    Guard,
    Spool,
    Stops,
    StopsAfterError(Box<aruna_core::errors::BlobError>),
    Discard(ImportFailure),
}

/// Stores a source stream as the job's hidden input. A stop ends the stream, and a blob that
/// was spooled while the job stopped is deleted again.
#[derive(Debug, PartialEq)]
struct SpoolSourceOperation {
    run: ImportRun,
    stream: Option<ByteStream>,
    expected_size: Option<u64>,
    eln: bool,
    spooled: Option<ImportInput>,
    step: SpoolStep,
}

impl Operation for SpoolSourceOperation {
    type Output = ImportInput;
    type Error = ImportFailure;

    fn first(&mut self) -> Result<Next<ImportInput>, ImportFailure> {
        let limit = self.run.spec.limits.import_source_bytes;
        if let Some(size) = self.expected_size {
            precheck_size(size, limit)?;
        }
        let stream = self.stream.take().ok_or_else(wrong_source)?;
        Ok(Next::Call(job_call(JobEffect::StoppableStream(stream))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ImportInput>, ImportFailure> {
        match std::mem::replace(&mut self.step, SpoolStep::Spool) {
            SpoolStep::Guard => {
                let blob = match result.event()? {
                    Event::Job(JobEvent::Stream(stream)) => stream,
                    other => return Err(unexpected("the guarded stream", &other).into()),
                };
                let spec = &self.run.spec;
                let spool = BlobEffect::SpoolHidden {
                    namespace: self.run.job_id.as_ulid(),
                    name: "input".to_string(),
                    created_by: spec.auth_context.user_id,
                    max_bytes: Some(spec.limits.import_source_bytes),
                    deadline: None,
                    blob,
                };
                Ok(Next::Call(IoCall::Effect(Box::new(Effect::Blob(spool)))))
            }
            SpoolStep::Spool => match result.event()? {
                Event::Blob(BlobEvent::HiddenSpooled {
                    location,
                    blake3,
                    size,
                }) => {
                    self.spooled = Some(ImportInput {
                        location,
                        size,
                        blake3,
                        upload_id: None,
                        eln: self.eln,
                    });
                    self.step = SpoolStep::Stops;
                    Ok(Next::Call(job_call(JobEffect::Stops)))
                }
                Event::Blob(BlobEvent::Error(error)) => {
                    self.step = SpoolStep::StopsAfterError(Box::new(error));
                    Ok(Next::Call(job_call(JobEffect::Stops)))
                }
                other => Err(ImportFailure::Retryable(format!(
                    "unexpected hidden spool event: {}",
                    other.label()
                ))),
            },
            SpoolStep::Stops => {
                let input = self.spooled.take().ok_or_else(wrong_source)?;
                let Some(stopped) = stopped_failure(stops_of(result)?) else {
                    return Ok(Next::Done(input));
                };
                match hidden_delete(&input.location) {
                    Ok(delete) => {
                        self.step = SpoolStep::Discard(stopped);
                        Ok(Next::Call(delete))
                    }
                    Err(_) => Err(stopped),
                }
            }
            SpoolStep::StopsAfterError(error) => {
                Err(stopped_failure(stops_of(result)?).unwrap_or_else(|| classify_blob(*error)))
            }
            // The delete is best effort; the stop decides the outcome.
            SpoolStep::Discard(stopped) => Err(stopped),
        }
    }
}

#[cfg(test)]
#[path = "acquire_tests.rs"]
mod tests;
