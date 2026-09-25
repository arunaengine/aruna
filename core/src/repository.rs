//! Repository records, destinations, links and credentials shared by every repository kind.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::structs::secondary_id::{
    IdentifierOrigin, SecondaryIdKind, SecondaryIdentifier, normalize_doi,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

mod credential;
pub use credential::RepositoryCredential;
pub mod invenio;
mod link;
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

/// How an Invenio import relates to a pull link.
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
pub struct InvenioDestination {
    pub group_id: Ulid,
    pub connector_id: Ulid,
    pub draft_id: Option<String>,
    pub new_version: Option<String>,
    pub metadata_json: String,
    pub publish: bool,
    pub public_files: bool,
    pub credential: Option<RepositoryCredential>,
    /// Set when a lasting link pushes; its token then comes from the link's sealed secret.
    pub link: Option<LinkTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvenioRecord {
    pub id: String,
    pub url: String,
    pub published: bool,
    pub parent_id: String,
    pub revision_id: u64,
    pub doi: Option<String>,
    /// The repository's page for people, when it names one on its own origin.
    pub html_url: Option<String>,
    /// DOI of the record's parent, which names every version.
    pub concept_doi: Option<String>,
    /// Submitted to the connector's community for review instead of published.
    pub in_review: bool,
    /// A check that failed after the repository had already published the record.
    pub warning: Option<String>,
}

impl InvenioRecord {
    /// The version DOI, concept DOI, record id and parent id as secondary identifiers.
    pub fn identifiers(
        &self,
        endpoint: &str,
        origin: IdentifierOrigin,
    ) -> Vec<SecondaryIdentifier> {
        invenio::build_identifiers(
            endpoint,
            origin,
            [
                self.doi.as_deref(),
                self.concept_doi.as_deref(),
                Some(self.id.as_str()),
                Some(self.parent_id.as_str()),
            ],
        )
    }
}

#[derive(Debug, Error)]
#[error("invalid Invenio record: {0}")]
pub struct InvenioError(pub &'static str);

/// The dataset's own identifiers, read when an export starts. Exports add them to what they
/// send, so the document itself is never edited and a push never causes another push.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExportIdentity {
    /// The dataset's active w3id PID; Invenio exports relate it as `isidenticalto`.
    pub own: Vec<String>,
    /// Registered repository identifiers of the dataset.
    pub identifiers: Vec<SecondaryIdentifier>,
    /// Web data entities the crate names without Aruna bytes; exports relate them as
    /// `references`.
    pub references: Vec<String>,
}

impl ExportIdentity {
    pub(crate) fn published_doi(&self, doi: &str) -> bool {
        normalize_doi(doi).is_ok_and(|doi| {
            self.identifiers.iter().any(|known| {
                known.kind == SecondaryIdKind::Doi
                    && known.origin == IdentifierOrigin::Published
                    && known.value == doi
            })
        })
    }
}
