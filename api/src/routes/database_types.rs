//! Request and response shapes of the database routes. Entries carry only ids; the dataset
//! the portal shows next to a database is read with the dataset's own access rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::identity::database::{DatabaseEntry, DatabaseRequest, DatabaseTarget};
use aruna_operations::realm::database_list::{DatabaseListing, DatabaseState};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};

use crate::error::ServerError;

/// The bucket of a database: a distributed bucket by its set, an ordinary one by node and name.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DatabaseTargetBody {
    Distributed { set_id: String },
    Ordinary { node_id: String, bucket: String },
}

impl From<&DatabaseTarget> for DatabaseTargetBody {
    fn from(target: &DatabaseTarget) -> Self {
        match target {
            DatabaseTarget::Distributed { set_id } => Self::Distributed {
                set_id: set_id.to_string(),
            },
            DatabaseTarget::Ordinary { node_id, bucket } => Self::Ordinary {
                node_id: node_id.to_string(),
                bucket: bucket.clone(),
            },
        }
    }
}

impl TryFrom<DatabaseTargetBody> for DatabaseTarget {
    type Error = ServerError;

    fn try_from(body: DatabaseTargetBody) -> Result<Self, Self::Error> {
        let invalid = |what: &str| ServerError::BadRequestReason(format!("invalid {what}"));
        Ok(match body {
            DatabaseTargetBody::Distributed { set_id } => Self::Distributed {
                set_id: Ulid::from_string(&set_id).map_err(|_| invalid("set_id"))?,
            },
            DatabaseTargetBody::Ordinary { node_id, bucket } => Self::Ordinary {
                node_id: node_id.parse().map_err(|_| invalid("node_id"))?,
                bucket,
            },
        })
    }
}

/// One database entry: only ids; texts come from the dataset.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatabaseEntryView {
    pub entry_id: String,
    pub dataset_id: String,
    pub target: DatabaseTargetBody,
    pub owner_group: String,
}

impl From<&DatabaseEntry> for DatabaseEntryView {
    fn from(entry: &DatabaseEntry) -> Self {
        Self {
            entry_id: entry.entry_id.to_string(),
            dataset_id: entry.dataset_id.to_string(),
            target: (&entry.target).into(),
            owner_group: entry.owner_group.to_string(),
        }
    }
}

/// One database as the caller may see it.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatabaseView {
    #[serde(flatten)]
    pub entry: DatabaseEntryView,
    /// The bucket name; empty in the curator view for a bucket the caller cannot read.
    pub bucket: String,
    /// `active`, `description_unavailable`; in the curator view also `dataset_hidden` and
    /// `no_longer_linked`.
    pub state: String,
    /// The dataset's title, when the caller may read the dataset.
    pub title: Option<String>,
    pub graph_iri: Option<String>,
    pub document_path: Option<String>,
    /// Anyone, also without a login, may read the bucket; otherwise realm users with access.
    pub public: bool,
}

impl From<DatabaseListing> for DatabaseView {
    fn from(listing: DatabaseListing) -> Self {
        Self {
            entry: (&listing.entry).into(),
            bucket: listing.bucket,
            state: match listing.state {
                DatabaseState::Active => "active",
                DatabaseState::Unavailable => "description_unavailable",
                DatabaseState::DatasetHidden => "dataset_hidden",
                DatabaseState::Unlinked => "no_longer_linked",
            }
            .to_string(),
            title: listing.title,
            graph_iri: listing.graph_iri,
            document_path: listing.document_path,
            public: listing.public,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatabaseListResponse {
    pub databases: Vec<DatabaseView>,
    /// Pass as `after` for the next page; absent on the last page.
    pub next_after: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct DatabaseListParams {
    pub after: Option<Ulid>,
    /// Entries per page, 1 to 100 (default 25).
    pub limit: Option<usize>,
    /// Curator view: also inactive entries and why. Needs WRITE on the databases path.
    #[serde(default)]
    pub curation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MarkDatabaseRequest {
    /// An existing entry to retarget; a new entry when absent.
    pub entry_id: Option<String>,
    /// The dataset that describes the bucket. Its root must already name the bucket root
    /// as a Dataset-typed `hasPart`.
    pub dataset_id: String,
    pub target: DatabaseTargetBody,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MarkDatabaseResponse {
    /// `published`, or `pending_consent` until an admin of the owning group approves.
    pub status: String,
    pub entry: DatabaseEntryView,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatabaseRequestView {
    pub request_id: String,
    pub entry: DatabaseEntryView,
    /// The curator who asked.
    pub curator: String,
    pub created_at_ms: u64,
}

impl From<&DatabaseRequest> for DatabaseRequestView {
    fn from(request: &DatabaseRequest) -> Self {
        Self {
            request_id: request.request_id.to_string(),
            entry: (&request.entry).into(),
            curator: request.curator.to_string(),
            created_at_ms: request.created_at_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatabaseRequestList {
    pub requests: Vec<DatabaseRequestView>,
}
