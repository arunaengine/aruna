//! Keeps one dataset linked to a repository record lineage and tracks its pushes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use super::{InvenioDestination, InvenioRecord};
use crate::errors::ConversionError;
use crate::structs::execution::job::JobId;
use crate::{NodeId, UserId};

/// Quiet time after a change before a link pushes, so a burst of edits becomes one push.
pub const LINK_DEBOUNCE_MS: u64 = 10_000;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LinkFailure {
    RemoteChanged,
    TokenRejected,
    SourceUnavailable,
    Other(String),
}

impl LinkFailure {
    pub fn reason(&self) -> &str {
        match self {
            Self::RemoteChanged => "remote_changed",
            Self::TokenRejected => "token_rejected",
            Self::SourceUnavailable => "source_unavailable",
            Self::Other(reason) => reason,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LinkStatus {
    Enabled,
    Paused,
    Failed { reason: LinkFailure },
}

/// The repository side: `record_id` is the last version this link published.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LinkRemote {
    pub parent_id: Option<String>,
    pub draft_id: Option<String>,
    pub record_id: Option<String>,
    pub doi: Option<String>,
    pub record_url: Option<String>,
    pub published: bool,
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
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

/// Link identity and lineage a push job checks before it touches the repository.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LinkTarget {
    pub link_id: Ulid,
    pub published_id: Option<String>,
    pub parent_id: Option<String>,
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
        record: InvenioRecord,
        event_id: Ulid,
        dataset_digest: Option<[u8; 32]>,
    },
    Failed(LinkFailure),
    Cancelled,
}

/// A durable request to compare a link with its dataset once `due_at_ms` passes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LinkQueueEntry {
    pub document_id: Ulid,
    pub due_at_ms: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("another push of this link is still running")]
pub struct LinkBusy;

impl InvenioLink {
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
            } => {
                let remote = &mut self.remote;
                remote.parent_id = Some(record.parent_id.clone());
                if record.published {
                    remote.record_id = Some(record.id.clone());
                    remote.draft_id = None;
                } else {
                    remote.draft_id = Some(record.id.clone());
                }
                remote.published = record.published;
                if record.doi.is_some() {
                    remote.doi = record.doi.clone();
                }
                remote.record_url = Some(record.html_url.clone().unwrap_or(record.url.clone()));
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
            publish: publish || self.auto_publish,
            public_files: self.public_files,
            credential: None,
            link: Some(LinkTarget {
                link_id: self.link_id,
                published_id: self.remote.record_id.clone(),
                parent_id: self.remote.parent_id.clone(),
            }),
        }
    }

    /// Job idempotency key: retries reuse it until the push is recorded as started.
    pub fn push_key(&self, event_id: Ulid, publish: bool) -> String {
        let action = if publish { "publish" } else { "push" };
        format!(
            "invenio-link/{}/{event_id}/{}/{action}",
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
