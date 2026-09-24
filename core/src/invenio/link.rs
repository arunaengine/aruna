//! Keeps one dataset linked to a repository record lineage and tracks its pushes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use super::{InvenioDestination, InvenioRecord};
use crate::document::{DocumentChange, DocumentChangeKind, DocumentSyncRevision, DocumentTarget};
use crate::errors::ConversionError;
use crate::structs::execution::job::{JobId, RoCrateLimits};
use crate::structs::placement::record::PlacementRef;
use crate::{NodeId, UserId};

/// Quiet time after a change before a link pushes, so a burst of edits becomes one push.
pub const LINK_DEBOUNCE_MS: u64 = 10_000;
/// Longest wait after the first queued change, so constant editing still pushes.
pub const LINK_DEBOUNCE_CAP_MS: u64 = 300_000;
/// Quiet time after the last push before auto_publish publishes the draft.
pub const AUTO_PUBLISH_QUIET_MS: u64 = 900_000;
/// How often a link asks the repository about a pending community review.
pub const REVIEW_POLL_MS: u64 = 3_600_000;
/// Zenodo accepts at most this many files per record.
pub const MAX_RECORD_FILES: usize = 100;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LinkFailure {
    RemoteChanged,
    TokenRejected,
    SourceUnavailable,
    Other(String),
    /// The owner node no longer holds the dataset, so it cannot push it.
    OwnerNotHolder,
    /// The crate has more files than one repository record accepts.
    TooManyFiles,
}

impl LinkFailure {
    pub fn reason(&self) -> &str {
        match self {
            Self::RemoteChanged => "remote_changed",
            Self::TokenRejected => "token_rejected",
            Self::SourceUnavailable => "source_unavailable",
            Self::Other(reason) => reason,
            Self::OwnerNotHolder => "owner_not_holder",
            Self::TooManyFiles => "too_many_files",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LinkStatus {
    Enabled,
    Paused,
    Failed { reason: LinkFailure },
}

/// State of the community review that the first version of a record waits for.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum LinkReview {
    #[default]
    None,
    Pending,
    Accepted,
    Declined,
}

impl LinkReview {
    pub fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Pending => "pending",
            Self::Accepted => "accepted",
            Self::Declined => "declined",
        }
    }
}

/// The repository side: `record_id` is the last version this link published.
/// `doi` is the open draft's reserved DOI while `doi_reserved` is set.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LinkRemote {
    pub parent_id: Option<String>,
    pub draft_id: Option<String>,
    pub record_id: Option<String>,
    pub doi: Option<String>,
    pub record_url: Option<String>,
    pub published: bool,
    pub concept_doi: Option<String>,
    pub doi_reserved: bool,
    /// Draft revision after this link's last write; another revision means a remote edit.
    pub revision_id: Option<u64>,
    pub review: LinkReview,
    /// File keys of the last push, so a push only deletes files this link put there.
    pub files: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LinkPush {
    pub event_id: Ulid,
    pub dataset_digest: Option<[u8; 32]>,
    pub job_id: JobId,
    pub pushed_at: SystemTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvenioLink {
    pub link_id: Ulid,
    pub document_id: Ulid,
    pub group_id: Ulid,
    pub connector_id: Ulid,
    pub endpoint: String,
    pub owner_node: NodeId,
    pub owner_node_url: String,
    pub created_by: UserId,
    pub status: LinkStatus,
    pub auto_publish: bool,
    pub public_files: bool,
    pub metadata_json: String,
    pub remote: LinkRemote,
    pub last_push: Option<LinkPush>,
    /// The push job running now; a link runs one push at a time.
    pub active_job: Option<JobId>,
    /// Counts started pushes, so a repeated push of one revision gets its own job.
    pub sequence: u64,
    /// The node's crate limits when the link was made; later pushes use them.
    pub limits: RoCrateLimits,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    /// Sync generation of the stored row; it grows with every change the owner stores.
    pub generation: u64,
    /// A problem found after the repository already published, such as a failed check.
    pub warning: Option<String>,
}

/// Link identity and lineage a push job checks before it touches the repository.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LinkTarget {
    pub link_id: Ulid,
    pub published_id: Option<String>,
    pub parent_id: Option<String>,
    pub revision_id: Option<u64>,
    pub files: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LinkPatch {
    pub paused: Option<bool>,
    pub auto_publish: Option<bool>,
    pub public_files: Option<bool>,
    pub metadata_json: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PushOutcome {
    Pushed {
        record: Box<InvenioRecord>,
        event_id: Ulid,
        dataset_digest: Option<[u8; 32]>,
        /// The file keys the record holds after the push.
        files: Vec<String>,
    },
    Failed(LinkFailure),
    Cancelled,
}

/// A durable request to compare a link with its dataset once `due_at_ms` passes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LinkQueueEntry {
    pub document_id: Ulid,
    pub due_at_ms: u64,
    /// When the oldest change still waiting was queued; it caps the debounce.
    pub first_at_ms: u64,
}

impl LinkQueueEntry {
    /// Queues a change at `now_ms`: each change moves the due time, up to the cap.
    pub fn debounce(document_id: Ulid, previous: Option<&Self>, now_ms: u64) -> Self {
        let first_at_ms = previous.map_or(now_ms, |entry| entry.first_at_ms.min(now_ms));
        Self {
            document_id,
            due_at_ms: now_ms
                .saturating_add(LINK_DEBOUNCE_MS)
                .min(first_at_ms.saturating_add(LINK_DEBOUNCE_CAP_MS)),
            first_at_ms,
        }
    }
}

/// What the repository says about a link's record, read when a review ends or the user
/// accepts remote changes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteState {
    /// The open draft with its revision, if one remains.
    pub draft: Option<InvenioRecord>,
    /// The latest published version of the lineage.
    pub latest: Option<InvenioRecord>,
    pub review: LinkReview,
    /// File keys of the open draft; accepting them lets later pushes replace them.
    pub files: Vec<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("another push of this link is still running")]
pub struct LinkBusy;

impl InvenioLink {
    pub fn target(&self) -> DocumentTarget {
        DocumentTarget::InvenioLink {
            document_id: self.document_id,
            link_id: self.link_id,
        }
    }

    /// Moves the sync generation past the stored one before the owner writes the row.
    pub fn stamp(&mut self, now_ms: u64) {
        self.generation = now_ms.max(self.generation.saturating_add(1));
    }

    /// Sync change of the stored row, derived from the row alone so every holder derives the same.
    pub fn sync_change(&self, placement: PlacementRef) -> DocumentChange {
        self.change_at(self.generation, DocumentChangeKind::Upsert, placement)
    }

    /// Sync change that removes the row; it orders after the last stored generation.
    pub fn delete_change(&self, placement: PlacementRef) -> DocumentChange {
        let generation = self.generation.saturating_add(1);
        self.change_at(generation, DocumentChangeKind::Delete, placement)
    }

    fn change_at(
        &self,
        generation: u64,
        kind: DocumentChangeKind,
        placement: PlacementRef,
    ) -> DocumentChange {
        DocumentChange {
            base: None,
            current: DocumentSyncRevision {
                generation,
                event_id: Ulid::from_parts(generation, self.link_id.random()),
                actor: self.owner_node,
                updated_at_ms: generation,
            },
            kind,
            placement,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Applies a patch; true when a resumed link should look for unpushed changes.
    pub fn patch(&mut self, patch: &LinkPatch, now: SystemTime) -> bool {
        if let Some(auto_publish) = patch.auto_publish {
            self.auto_publish = auto_publish;
        }
        if let Some(public_files) = patch.public_files {
            self.public_files = public_files;
        }
        if let Some(metadata) = &patch.metadata_json {
            self.metadata_json = metadata.clone();
        }
        self.updated_at = now;
        match patch.paused {
            Some(true) => {
                self.status = LinkStatus::Paused;
                false
            }
            Some(false) if self.status != LinkStatus::Enabled => {
                self.status = LinkStatus::Enabled;
                true
            }
            _ => false,
        }
    }

    /// A new token ends a token failure; true when pushing resumes.
    pub fn rotate(&mut self, now: SystemTime) -> bool {
        self.updated_at = now;
        let rejected = LinkStatus::Failed {
            reason: LinkFailure::TokenRejected,
        };
        if self.status == rejected {
            self.status = LinkStatus::Enabled;
            return true;
        }
        false
    }

    /// Records the push job; repeating it for the same job changes nothing.
    pub fn begin(&mut self, job_id: JobId, now: SystemTime) -> Result<(), LinkBusy> {
        match self.active_job {
            Some(active) if active == job_id => Ok(()),
            Some(_) => Err(LinkBusy),
            None => {
                self.active_job = Some(job_id);
                self.sequence = self.sequence.saturating_add(1);
                if matches!(self.status, LinkStatus::Failed { .. }) {
                    self.status = LinkStatus::Enabled;
                }
                self.updated_at = now;
                Ok(())
            }
        }
    }

    /// Applies the outcome of the running push; other jobs are ignored.
    pub fn finish(&mut self, job_id: JobId, outcome: &PushOutcome, now: SystemTime) -> bool {
        if self.active_job != Some(job_id) {
            return false;
        }
        self.active_job = None;
        self.updated_at = now;
        match outcome {
            PushOutcome::Pushed {
                record,
                event_id,
                dataset_digest,
                files,
            } => {
                self.adopt(record);
                if record.in_review {
                    self.remote.review = LinkReview::Pending;
                }
                self.remote.files = files.clone();
                self.warning = record.warning.clone();
                self.last_push = Some(LinkPush {
                    event_id: *event_id,
                    dataset_digest: *dataset_digest,
                    job_id,
                    pushed_at: now,
                });
            }
            PushOutcome::Failed(reason) if self.status != LinkStatus::Paused => {
                self.status = LinkStatus::Failed {
                    reason: reason.clone(),
                };
            }
            PushOutcome::Failed(_) | PushOutcome::Cancelled => {}
        }
        true
    }

    /// Takes the repository's view of the draft or published record as the link's remote.
    pub fn adopt(&mut self, record: &InvenioRecord) {
        let remote = &mut self.remote;
        remote.parent_id = Some(record.parent_id.clone());
        if record.published {
            remote.record_id = Some(record.id.clone());
            remote.draft_id = None;
            remote.revision_id = None;
        } else {
            remote.draft_id = Some(record.id.clone());
            remote.revision_id = Some(record.revision_id);
        }
        remote.published = record.published;
        remote.doi_reserved = !record.published && record.doi.is_some();
        if record.doi.is_some() || !record.published {
            remote.doi = record.doi.clone();
        }
        if record.concept_doi.is_some() {
            remote.concept_doi = record.concept_doi.clone();
        }
        remote.record_url = Some(record.html_url.clone().unwrap_or(record.url.clone()));
    }

    /// Stores the draft of the running push as soon as it exists, so a retry continues it.
    pub fn draft(&mut self, job_id: JobId, record: &InvenioRecord, now: SystemTime) -> bool {
        if self.active_job != Some(job_id) || record.published {
            return false;
        }
        self.adopt(record);
        self.updated_at = now;
        true
    }

    /// Makes the repository's current state the new base and ends a remote change failure.
    pub fn accept(&mut self, state: &RemoteState, now: SystemTime) {
        if let Some(latest) = &state.latest {
            self.adopt(latest);
        }
        match &state.draft {
            Some(draft) => self.adopt(draft),
            None => self.remote.draft_id = None,
        }
        self.remote.review = state.review;
        self.remote.files = state.files.clone();
        if matches!(self.status, LinkStatus::Failed { .. }) {
            self.status = LinkStatus::Enabled;
        }
        self.warning = None;
        self.updated_at = now;
    }

    /// Whether auto_publish still has an open draft to publish once the dataset is quiet.
    pub fn publish_waits(&self) -> bool {
        self.auto_publish
            && self.status == LinkStatus::Enabled
            && self.remote.draft_id.is_some()
            && self.remote.review != LinkReview::Pending
            && self.last_push.is_some()
    }

    /// When auto_publish may publish: a quiet period after the last push.
    pub fn publish_due_ms(&self) -> Option<u64> {
        let pushed = self.last_push.as_ref()?.pushed_at;
        let pushed_ms = pushed
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis() as u64);
        self.publish_waits()
            .then(|| pushed_ms.saturating_add(AUTO_PUBLISH_QUIET_MS))
    }

    /// Whether the displayed revision differs from the last pushed one.
    pub fn changed(&self, event_id: Ulid, dataset_digest: Option<[u8; 32]>) -> bool {
        self.last_push.as_ref().is_none_or(|push| {
            push.event_id != event_id
                && (dataset_digest.is_none() || push.dataset_digest != dataset_digest)
        })
    }

    /// Updates the open draft, else starts a new version of the lineage, else a new record.
    pub fn destination(&self, publish: bool) -> InvenioDestination {
        let draft = self.remote.draft_id.clone();
        InvenioDestination {
            group_id: self.group_id,
            connector_id: self.connector_id,
            new_version: draft
                .is_none()
                .then(|| self.remote.record_id.clone())
                .flatten(),
            draft_id: draft,
            metadata_json: self.metadata_json.clone(),
            publish,
            public_files: self.public_files,
            credential: None,
            link: Some(LinkTarget {
                link_id: self.link_id,
                published_id: self.remote.record_id.clone(),
                parent_id: self.remote.parent_id.clone(),
                revision_id: self.remote.revision_id,
                files: self.remote.files.clone(),
            }),
        }
    }

    /// Job idempotency key: retries reuse it until the link changes or the push starts.
    pub fn push_key(&self, event_id: Ulid, publish: bool) -> String {
        let action = if publish { "publish" } else { "push" };
        let updated = self
            .updated_at
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis());
        format!(
            "invenio-link/{}/{event_id}/{}/{updated}/{action}",
            self.link_id, self.sequence
        )
    }
}

pub fn link_key(document_id: Ulid, link_id: Ulid) -> Vec<u8> {
    let mut key = link_prefix(document_id);
    key.extend_from_slice(&link_id.to_bytes());
    key
}

pub fn link_prefix(document_id: Ulid) -> Vec<u8> {
    document_id.to_bytes().to_vec()
}

pub fn connector_link_key(connector_id: Ulid, link_id: Ulid) -> Vec<u8> {
    let mut key = connector_id.to_bytes().to_vec();
    key.extend_from_slice(&link_id.to_bytes());
    key
}

#[cfg(test)]
#[path = "link_tests.rs"]
mod tests;
