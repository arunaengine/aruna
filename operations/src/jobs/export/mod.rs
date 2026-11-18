//! Runs the RO-Crate export job that streams the selected objects into a resumable ZIP archive.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use aruna_core::archive_path::StorageKey;
use aruna_core::calls::IoError;
use aruna_core::id::NodeId;
use aruna_core::jobs::WaitTarget;
use aruna_core::metadata::MetadataValidationViolation;
use aruna_core::structs::execution::job::{
    ArtifactRef, ExportReportRow, ExportReportSource, ExportRoCrateSpec, JobId, KeyWait,
    ReasonCode, RoCrateCheckpointRefs,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::repository::{JobScope, NodeScope};
use crate::replication::bao_read::BaoReadOutput;
use archive::blocking_omissions;
use resolve::ExportCandidate;

pub(crate) mod archive;
mod assemble;
mod consent;
mod job;
mod open;
mod probe;
mod publish;
mod resolve;
mod snapshot;
mod sources;

pub use consent::{ConsentCheckError, UnheldBucketOperation};
pub use job::{ExportJobError, ExportJobOperation};
pub use snapshot::CrateJsonOperation;

const METADATA_PATH: &str = aruna_core::repository::rules::CRATE_FILES[0];
const REPORT_PATH: &str = aruna_core::repository::rules::CRATE_FILES[1];
const REMOTE_ATTEMPTS: usize = 8;
const MAX_LOCAL_CANDIDATES: usize = REMOTE_ATTEMPTS / 2;
const JSONLD_BASE_IRI: &str = "https://craqle.invalid/";
const SCHEMA_CONTENT_IRI: &str = "http://schema.org/contentUrl";
const CONTENT_HTTPS_IRI: &str = "https://schema.org/contentUrl";
const SCHEMA_ABOUT_IRI: &str = "http://schema.org/about";
const ABOUT_HTTPS_IRI: &str = "https://schema.org/about";
const SCHEMA_PART_IRI: &str = "http://schema.org/hasPart";
const PART_HTTPS_IRI: &str = "https://schema.org/hasPart";
const SCHEMA_SUBJECT_IRI: &str = "http://schema.org/subjectOf";
const SUBJECT_HTTPS_IRI: &str = "https://schema.org/subjectOf";
const SCHEMA_ENCODING_IRI: &str = "http://schema.org/encodingFormat";
const ENCODING_HTTPS_IRI: &str = "https://schema.org/encodingFormat";
const SCHEMA_NAME_IRI: &str = "http://schema.org/name";
const NAME_HTTPS_IRI: &str = "https://schema.org/name";
const LOCAL_PATH_IRI: &str = "https://w3id.org/ro/terms#localPath";
const PATH_HTTP_IRI: &str = "http://w3id.org/ro/terms#localPath";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum ExportPhase {
    Snapshot,
    Resolve,
    Plan,
    Assemble,
    Publish,
}

/// Progress an export job keeps under its id; public so the doctor can migrate stored rows.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExportCheckpoint {
    pub(crate) repository_started: bool,
    pub(crate) repository_complete: bool,
    pub(crate) repository_metadata: Option<[u8; 32]>,
    pub(crate) repository: Option<aruna_core::repository::RepositoryRecord>,
    /// A link's resolved lineage base: the latest published version it continues.
    pub(crate) repository_base: Option<String>,
    pub(crate) link_failure: Option<aruna_core::repository::LinkFailure>,
    /// File keys the repository record holds after the upload.
    pub(crate) repository_files: Vec<String>,
    /// The dataset's identifiers when the snapshot was taken.
    pub(crate) identity: aruna_core::repository::ExportIdentity,
    refs: RoCrateCheckpointRefs,
    phase: ExportPhase,
    winning_event_id: Option<Ulid>,
    context_digest: Option<[u8; 32]>,
    dataset_digest: Option<[u8; 32]>,
    pub(crate) raw_jsonld: Option<String>,
    entities: Vec<ExportEntity>,
    rewritten_jsonld: Option<Vec<u8>>,
    report_json: Option<Vec<u8>>,
    report: Vec<ExportReportRow>,
    pub(crate) artifact: Option<ArtifactRef>,
}

impl Default for ExportCheckpoint {
    fn default() -> Self {
        Self {
            repository_started: false,
            repository_complete: false,
            repository_metadata: None,
            repository: None,
            repository_base: None,
            link_failure: None,
            repository_files: Vec::new(),
            identity: Default::default(),
            refs: RoCrateCheckpointRefs::default(),
            phase: ExportPhase::Snapshot,
            winning_event_id: None,
            context_digest: None,
            dataset_digest: None,
            raw_jsonld: None,
            entities: Vec::new(),
            rewritten_jsonld: None,
            report_json: None,
            report: Vec::new(),
            artifact: None,
        }
    }
}

impl ExportCheckpoint {
    /// The snapshot revision this export pushed, with its dataset digest.
    pub(crate) fn pushed_revision(&self) -> Option<(Ulid, Option<[u8; 32]>)> {
        self.winning_event_id
            .map(|event_id| (event_id, self.dataset_digest))
    }

    /// Why a push failed: a recorded refusal, left-out files, or the job message.
    pub(crate) fn push_failure(&self, message: &str) -> aruna_core::repository::LinkFailure {
        use aruna_core::repository::LinkFailure;
        if let Some(failure) = &self.link_failure {
            return failure.clone();
        }
        if blocking_omissions(&self.report) > 0 {
            return LinkFailure::SourceUnavailable;
        }
        LinkFailure::Other(message.to_string())
    }

    /// Archive paths of the crate's data entities, by entity id.
    pub(crate) fn archive_paths(&self) -> HashMap<&str, &str> {
        self.entities
            .iter()
            .filter_map(|entity| Some((entity.entity_id.as_str(), entity.zip_path.as_deref()?)))
            .collect()
    }

    /// The finished push as a link outcome, once the repository holds the complete record.
    pub(crate) fn pushed_outcome(&self) -> Option<aruna_core::repository::PushOutcome> {
        let record = self
            .repository
            .clone()
            .filter(|_| self.repository_complete)?;
        let (event_id, dataset_digest) = self.pushed_revision()?;
        Some(aruna_core::repository::PushOutcome::Pushed {
            record: Box::new(record),
            event_id,
            dataset_digest,
            files: self.repository_files.clone(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ExportEntity {
    entity_id: String,
    local_path: Option<String>,
    storage_key: Option<StorageKey>,
    exact: Option<VersionedObjectArn>,
    hash: Option<[u8; 32]>,
    hash_realm: Option<RealmId>,
    candidates: Vec<ExportCandidate>,
    omission: Option<ReasonCode>,
    message: Option<String>,
    zip_path: Option<String>,
    report_source: Option<ExportReportSource>,
    resolved_version: Option<Ulid>,
    path_synthesized: bool,
    /// Plaintext hash and size of the included version.
    content: Option<([u8; 32], u64)>,
}

#[derive(Debug, PartialEq)]
struct ProbedEntry {
    entity_index: usize,
    candidate_index: usize,
    size: u64,
    hash: [u8; 32],
    report_source: ExportReportSource,
    resolved_version: Option<Ulid>,
}

/// One archive entry with the copy it is read from.
#[derive(Debug, PartialEq)]
struct PlannedEntry {
    entity_index: usize,
    candidate_index: usize,
    path: String,
    candidate: ExportCandidate,
    expected_blake3: [u8; 32],
    modified_ms: u64,
}

#[derive(Debug, PartialEq)]
enum ExportFailure {
    Permanent(String),
    Retryable(String),
    /// A local input's bucket key is locked; the job waits for it.
    KeyLocked(KeyWait),
    /// A remote holder refused because its bucket key is locked; the job registers there.
    RemoteLocked {
        node_id: NodeId,
        /// The exact source the refused read named, so the holder admits only that.
        target: WaitTarget,
        auth: Box<AuthContext>,
    },
    Validation(Vec<MetadataValidationViolation>),
    Candidate {
        entity_index: usize,
        candidate_index: usize,
        status: OpenStatus,
        message: String,
    },
    Cancelled,
    Interrupted,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum OpenStatus {
    Denied,
    /// An export into another realm needs a bucket key of this copy that the caller lacks.
    NotHolder,
    Missing,
    Offline,
    Corrupt,
}

#[derive(Debug, PartialEq)]
enum CandidateOpen {
    Opened(BaoReadOutput),
    Status(OpenStatus),
}

/// What every operation of one export run knows about it.
#[derive(Clone, Debug, PartialEq)]
pub struct ExportScope {
    pub spec: Arc<ExportRoCrateSpec>,
    pub job_id: JobId,
    pub claim_token: Ulid,
    pub owner_node: NodeId,
    /// This node on the network, if it has one.
    pub local_node: Option<NodeId>,
    pub scope: NodeScope,
    pub job: JobScope,
}

impl ExportScope {
    fn sources(&self) -> SourceScope {
        SourceScope {
            spec: self.spec.clone(),
            owner_node: self.owner_node,
        }
    }
}

/// What looking up and opening the sources of an export needs.
#[derive(Clone, Debug, PartialEq)]
struct SourceScope {
    spec: Arc<ExportRoCrateSpec>,
    owner_node: NodeId,
}

impl From<IoError> for ExportFailure {
    fn from(error: IoError) -> Self {
        Self::Retryable(error.to_string())
    }
}

#[cfg(test)]
mod tests;
