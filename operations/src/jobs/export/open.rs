//! Opens one export candidate: a local copy under the caller's access, or a read from a holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::jobs::WaitTarget;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::KeyWait;
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::encryption::ReadLease;

use super::resolve::{
    CandidateSource, ExportCandidate, LocalCandidate, LocalOpen, OpenLocalOperation, ResolveError,
};
use super::sources::export_failure;
use super::{CandidateOpen, ExportFailure, OpenStatus, SourceScope};
use crate::replication::bao_read::{BaoReadError, BaoReadOutput};
use crate::replication::managed_read::ManagedReadOperation;
use crate::replication::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget};
use crate::s3::object::get::leased;

/// What a local open needs after the access check.
#[derive(Debug, PartialEq)]
struct LocalRead {
    location: BackendLocation,
    blake3: [u8; 32],
    node_id: NodeId,
    bucket: String,
    group_id: aruna_core::types::GroupId,
}

#[derive(Debug, PartialEq)]
enum OpenStep {
    Start,
    Access(LocalRead),
    Read {
        blake3: [u8; 32],
        held: Option<ReadLease>,
    },
    Remote {
        node_id: NodeId,
        target: WaitTarget,
    },
}

/// Opens `candidate`, for its metadata alone or for its bytes.
#[derive(Debug, PartialEq)]
pub struct OpenCandidateOperation {
    scope: SourceScope,
    candidate: ExportCandidate,
    metadata_only: bool,
    step: OpenStep,
}

type OpenTurn = Result<Next<CandidateOpen>, ExportFailure>;

fn status(status: OpenStatus) -> Next<CandidateOpen> {
    Next::Done(CandidateOpen::Status(status))
}

impl OpenCandidateOperation {
    pub fn new(scope: SourceScope, candidate: ExportCandidate, metadata_only: bool) -> Self {
        Self {
            scope,
            candidate,
            metadata_only,
            step: OpenStep::Start,
        }
    }

    fn open_remote(&mut self, node_id: NodeId, target: BaoReadTarget) -> OpenTurn {
        let expected_blake3 = self.candidate.expected_blake3;
        let wait = match &target {
            BaoReadTarget::ExactVersion(arn) => WaitTarget::Object {
                bucket: arn.bucket.clone(),
                key: arn.key.clone(),
                version_id: arn.version,
            },
            BaoReadTarget::Blake3(blake3) => WaitTarget::Content { blake3: *blake3 },
            BaoReadTarget::Version { .. } => WaitTarget::Content {
                blake3: expected_blake3.unwrap_or_default(),
            },
        };
        let auth = &self.scope.spec.auth_context;
        // The challenge loop fills in this node's advertised subject and the refs it learns, so
        // a governed copy can be staged instead of dead-ending.
        let read = ManagedReadOperation::new(
            node_id,
            BaoReadRequest {
                auth_context: auth.clone(),
                realm_id: auth.realm_id,
                target,
                expected_blake3,
                metadata_only: self.metadata_only,
                destination: None,
                known_refs: Vec::new(),
                range: None,
            },
        );
        self.step = OpenStep::Remote {
            node_id,
            target: wait,
        };
        Ok(Next::Call(IoCall::child(read)))
    }

    /// Reads the bytes of a local candidate the open admitted, under its lease when it has one.
    fn read_local(&mut self, read: LocalRead, lease: Option<ReadLease>) -> OpenTurn {
        let LocalRead {
            location, blake3, ..
        } = read;
        let (effect, held) = match (lease, location.format.bucket_key()) {
            (Some(lease), Some(_)) => {
                let read = BlobEffect::ReadSealed {
                    location,
                    range: None,
                    lease: Box::new(lease),
                };
                (read, None)
            }
            (lease, _) => (BlobEffect::Read { location }, lease),
        };
        self.step = OpenStep::Read { blake3, held };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Blob(effect)))))
    }

    fn local_read(&self, event: Event, blake3: [u8; 32], held: Option<ReadLease>) -> OpenTurn {
        match event {
            Event::Blob(BlobEvent::ReadFinished { blob, stream_size }) => {
                let size = stream_size;
                let hashes = HashMap::new();
                Ok(Next::Done(CandidateOpen::Opened(if self.metadata_only {
                    BaoReadOutput::Metadata {
                        size,
                        blake3,
                        etag: None,
                        hashes,
                    }
                } else {
                    let blob = match held {
                        Some(lease) => leased(blob, lease),
                        None => blob,
                    };
                    BaoReadOutput::Stream {
                        blob,
                        size,
                        blake3,
                        etag: None,
                        hashes,
                    }
                })))
            }
            Event::Blob(BlobEvent::Error(BlobError::HandleMissing)) => Err(
                ExportFailure::Retryable("blob handle unavailable".to_string()),
            ),
            Event::Blob(BlobEvent::Error(BlobError::IntegrityCheckFailed(_))) => {
                Ok(status(OpenStatus::Corrupt))
            }
            Event::Blob(BlobEvent::Error(_)) => Ok(status(OpenStatus::Offline)),
            event => Err(ExportFailure::Retryable(format!(
                "unexpected local blob read event: {}",
                event.label()
            ))),
        }
    }
}

fn remote_read(
    read: Result<BaoReadOutput, BaoReadError>,
    node_id: NodeId,
    target: WaitTarget,
    auth: &aruna_core::structs::identity::auth::AuthContext,
) -> OpenTurn {
    let error = match read {
        Ok(output) => return Ok(Next::Done(CandidateOpen::Opened(output))),
        Err(error) => error,
    };
    match error {
        BaoReadError::Refused(BaoReadRefusal::ReadDenied | BaoReadRefusal::RealmPeerDenied) => {
            Ok(status(OpenStatus::Denied))
        }
        BaoReadError::Refused(BaoReadRefusal::NotFound | BaoReadRefusal::InvalidTarget) => {
            Ok(status(OpenStatus::Missing))
        }
        BaoReadError::Refused(BaoReadRefusal::HashMismatch)
        | BaoReadError::Blob(BlobError::IntegrityCheckFailed(_)) => Ok(status(OpenStatus::Corrupt)),
        BaoReadError::Refused(BaoReadRefusal::BucketLocked(_)) => {
            Err(ExportFailure::RemoteLocked {
                node_id,
                target,
                auth: Box::new(auth.clone()),
            })
        }
        // Non-disclosing: a refused destination looks exactly like a refused authorization, and
        // neither names a policy.
        BaoReadError::PolicyDenied { .. }
        | BaoReadError::PolicyRequired { .. }
        | BaoReadError::Gate(_)
        | BaoReadError::NoDestination => Ok(status(OpenStatus::Denied)),
        BaoReadError::Refused(BaoReadRefusal::BackendFailure)
        | BaoReadError::Blob(
            BlobError::ConnectionFailed(_)
            | BlobError::ChannelClosed
            | BlobError::ReadError(_)
            | BlobError::OperatorCreationFailed(_)
            | BlobError::HandleMissing
            | BlobError::SendError
            | BlobError::ReplicationFailed(_)
            | BlobError::ReplicationRejected(_),
        ) => Ok(status(OpenStatus::Offline)),
        error => Err(ExportFailure::Retryable(error.to_string())),
    }
}

impl Operation for OpenCandidateOperation {
    type Output = CandidateOpen;
    type Error = ExportFailure;

    fn first(&mut self) -> OpenTurn {
        // Another node cannot enforce the export consent, so an export into a realm reads locally.
        let selection = self.scope.spec.selection.is_some();
        match self.candidate.source.clone() {
            CandidateSource::Local {
                location,
                group_id,
                permission_path,
                node_id,
                bucket,
                key,
            } => {
                let Some(blake3) = self.candidate.expected_blake3 else {
                    return Err(ExportFailure::Permanent(
                        "local export candidate has no BLAKE3 hash".to_string(),
                    ));
                };
                let Some(version_id) = self.candidate.resolved_version else {
                    return Ok(status(OpenStatus::Missing));
                };
                let local = LocalCandidate {
                    location: location.clone(),
                    group_id,
                    permission_path,
                    node_id,
                    bucket: bucket.clone(),
                    key,
                    blake3,
                    version_id,
                };
                let auth = self.scope.spec.auth_context.clone();
                let open = OpenLocalOperation::new(auth, selection, local);
                self.step = OpenStep::Access(LocalRead {
                    location,
                    blake3,
                    node_id,
                    bucket,
                    group_id,
                });
                Ok(Next::Call(IoCall::child(open)))
            }
            _ if selection => Ok(status(OpenStatus::Denied)),
            CandidateSource::RemoteExact { node_id, target } => {
                self.open_remote(node_id, BaoReadTarget::ExactVersion(target))
            }
            CandidateSource::RemoteHash { node_id, hash } => {
                self.open_remote(node_id, BaoReadTarget::Blake3(hash))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> OpenTurn {
        match std::mem::replace(&mut self.step, OpenStep::Start) {
            OpenStep::Start => Err(ExportFailure::Retryable(
                "the candidate open has no running step".to_string(),
            )),
            OpenStep::Access(read) => {
                let open = result.child::<LocalOpen, ResolveError>()?;
                match open.map_err(export_failure)? {
                    LocalOpen::Missing => Ok(status(OpenStatus::Missing)),
                    LocalOpen::Denied => Ok(status(OpenStatus::Denied)),
                    LocalOpen::NotHolder => Ok(status(OpenStatus::NotHolder)),
                    LocalOpen::Locked(key) => Err(ExportFailure::KeyLocked(KeyWait {
                        node_id: read.node_id,
                        bucket: read.bucket,
                        group_id: Some(read.group_id),
                        key,
                    })),
                    LocalOpen::Admitted(lease) => self.read_local(read, lease),
                }
            }
            OpenStep::Read { blake3, held } => self.local_read(result.event()?, blake3, held),
            OpenStep::Remote { node_id, target } => {
                let read = result.child::<BaoReadOutput, BaoReadError>()?;
                remote_read(read, node_id, target, &self.scope.spec.auth_context)
            }
        }
    }
}

#[cfg(test)]
#[path = "open_tests.rs"]
mod tests;
