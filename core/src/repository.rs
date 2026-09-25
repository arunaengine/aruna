//! Repository records, destinations, links and credentials shared by every repository kind.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::structs::execution::harvest::RepositoryConnectorKind;
use crate::structs::secondary_id::{
    IdentifierOrigin, SecondaryIdKind, SecondaryIdentifier, normalize_value,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

mod credential;
pub use credential::RepositoryCredential;
pub mod fields;
pub mod invenio;
mod kind;
pub use kind::{KindDescriptor, RequirementProfile, builtin_profile, descriptor, kinds};
mod link;
pub mod rules;
pub use link::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepositoryQuery {
    pub group_id: Ulid,
    pub connector_id: Ulid,
    pub q: String,
    pub page: u32,
    pub size: u8,
    pub all_versions: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportMode {
    #[default]
    Copy,
    Reference,
    Metadata,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ImportOptions {
    pub mode: ImportMode,
    pub all_versions: bool,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            mode: ImportMode::Copy,
            all_versions: true,
        }
    }
}

/// How a repository import relates to a pull link.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RepositoryPull {
    /// After the import, a new pull link keeps the dataset updated from the lineage.
    Keep {
        auto_update: bool,
        owner_node_url: String,
    },
    /// Imports the lineage's new versions into the dataset of this pull link.
    Update { link_id: Ulid },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RepositoryDestination {
    pub group_id: Ulid,
    pub connector_id: Ulid,
    pub draft_id: Option<String>,
    pub published_id: Option<String>,
    pub metadata_json: String,
    pub publish: bool,
    pub public_files: bool,
    pub credential: Option<RepositoryCredential>,
    /// Set when a lasting link pushes; its token then comes from the link's sealed secret.
    pub link: Option<LinkTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RepositoryRecord {
    pub id: String,
    pub url: String,
    pub published: bool,
    pub parent_id: String,
    pub revision_id: u64,
    /// The record's identifier of the kind's identifier kind, such as its DOI.
    pub identifier: Option<String>,
    /// The repository's page for people, when it names one on its own origin.
    pub html_url: Option<String>,
    /// Identifier of the record's parent, which names every version.
    pub concept_identifier: Option<String>,
    /// Submitted to the connector's community for review instead of published.
    pub in_review: bool,
    /// A check that failed after the repository had already published the record.
    pub warning: Option<String>,
    /// The record's identifiers as `Published`, filled by the adapter of its repository kind.
    pub identifiers: Vec<SecondaryIdentifier>,
}

/// What a repository kind supports; the kind refuses every other action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Capabilities {
    pub drafts: bool,
    pub reserve_identifier: bool,
    pub versions: bool,
    pub review: bool,
    pub pull: bool,
    pub search: bool,
    /// Records can be imported as datasets.
    pub import: bool,
    /// A record can wait for a release date before it becomes public.
    pub release_date: bool,
    /// The identifier a published record receives, such as a DOI.
    pub identifier_kind: SecondaryIdKind,
}

/// The capabilities of a kind that publishes records; `None` for kinds that only harvest.
pub fn capabilities(kind: RepositoryConnectorKind) -> Option<Capabilities> {
    descriptor(kind).map(|descriptor| descriptor.capabilities)
}

#[derive(Debug, Error)]
#[error("invalid repository data: {0}")]
pub struct RepositoryError(pub &'static str);

/// The dataset's own identifiers, read when an export starts. Exports add them to what they
/// send, so the document itself is never edited and a push never causes another push.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExportIdentity {
    /// The dataset's active w3id PID; exports relate the record to it as the same dataset.
    pub own: Vec<String>,
    /// Registered repository identifiers of the dataset.
    pub identifiers: Vec<SecondaryIdentifier>,
    /// Web data entities the crate names without Aruna bytes; exports relate them as
    /// `references`.
    pub references: Vec<String>,
}

impl ExportIdentity {
    /// Whether this dataset published the identifier to a repository.
    pub(crate) fn published(&self, kind: SecondaryIdKind, value: &str) -> bool {
        normalize_value(kind, value).is_ok_and(|value| {
            self.identifiers.iter().any(|known| {
                known.kind == kind
                    && known.origin == IdentifierOrigin::Published
                    && known.value == value
            })
        })
    }
}
