//! Holds the metadata payloads that travel between nodes: reference preflights, link pages,
//! exports, document writes and identifier matches.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::metadata::{
    MaterializationState, MetadataBatch, MetadataBatchSource, MetadataRawRevision,
    MetadataRoCratePage,
};
use crate::structs::secondary_id::IdentifierOrigin;
use crate::structs::storage::data::link::{DataLink, LinkScope};
use crate::structs::storage::metadata_registry::MetadataRegistryRecord;
use crate::structs::storage::replication::ARUNA_DATA_PREFIX;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPreflightLocation {
    pub node_id: NodeId,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataResolvedTarget {
    /// The content w3id, or the `s3://` location IRI of a location target; it keys the target.
    pub content_w3id: String,
    /// All zeros for a location target; a node of an earlier build then finds no copy by it and
    /// still looks up the target's IRIs.
    pub content_hash: [u8; 32],
    pub queried_iris: Vec<String>,
    pub targeted_versions: Vec<MetadataPreflightLocation>,
    pub removed_locations: Vec<MetadataPreflightLocation>,
    #[serde(rename = "remove_all_resolvable_locations")]
    pub remove_resolvable_locations: bool,
}

impl MetadataResolvedTarget {
    /// The location IRI of a target named by its location, as its bytes were never read.
    pub fn location(&self) -> Option<&str> {
        (!self.content_w3id.starts_with(ARUNA_DATA_PREFIX)).then_some(self.content_w3id.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceNodeRequest {
    pub targets: Vec<MetadataResolvedTarget>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataVisibleReference {
    pub content_w3id: String,
    pub document_id: String,
    pub title: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataIndexState {
    Current,
    Pending,
    Failed,
    Mixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataNodeFreshness {
    pub node_id: NodeId,
    pub index_state: MetadataIndexState,
    #[serde(rename = "oldest_status_updated_at_ms")]
    pub oldest_status_updated: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceNodeTarget {
    pub content_w3id: String,
    pub hidden_references_exist: bool,
    pub resolvable_location_found: bool,
    #[serde(rename = "resolvable_location_after_operation")]
    pub location_after_operation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceNodeExecution {
    pub visible_references: Vec<MetadataVisibleReference>,
    pub targets: Vec<ReferenceNodeTarget>,
    pub freshness: MetadataNodeFreshness,
    #[serde(rename = "path_style_endpoint_available")]
    pub path_style_available: bool,
    pub saturated: bool,
}

/// One identifier, or the prefixes of a key a bucket's prefix postings are probed for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkQuery {
    Exact(DataLink),
    KeyPrefixes { scope: LinkScope, key: String },
}

/// What one metadata node is asked: the queries, the page position and size.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkNodeRequest {
    pub queries: Vec<LinkQuery>,
    /// Only postings of linked parts (Dataset-typed `hasPart` of a root) count.
    pub parts_only: bool,
    /// Documents after this id; documents sort by id on every node.
    pub after: Option<Ulid>,
    pub limit: usize,
    /// A document whose link is reported by `confirmed` even if the caller may not read it.
    pub confirm: Option<Ulid>,
}

/// One link of a document to one of the queried identifiers.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinkHit {
    pub id: String,
    pub part: bool,
    pub subject_iris: Vec<String>,
}

/// One readable document that links the target.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinkDocument {
    pub document_id: Ulid,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub title: Option<String>,
    pub links: Vec<LinkHit>,
}

/// One node's answer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkNodePage {
    pub documents: Vec<LinkDocument>,
    /// Every matching document up to this id was examined, readable or not, and more may
    /// follow it; `None` once nothing follows.
    pub next: Option<Ulid>,
    /// The budget ran out before every queried identifier was read.
    pub cut: bool,
    /// A matching document exists that the caller may not read.
    pub hidden: bool,
    pub pending_materializations: usize,
    pub failed_materializations: usize,
    /// Documents whose links were cut at the projection limit; any of them may match.
    pub truncated_documents: usize,
    /// The requested `confirm` document matches but the caller may not read it.
    pub confirmed: bool,
}

/// One node's page of the documents that link a distributed set: its prefix links and its
/// verified version links, in posting key order. For the set's deletion preview.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetPreviewPage {
    pub page: LinkNodePage,
    /// The posting key to continue after; `None` once both ranges are read.
    pub next: Option<Vec<u8>>,
    /// Version links on this node that no member could verify, counted up to a cap; while
    /// any exist the preview is incomplete.
    pub unverified: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoCrateExportView {
    Full,
    Summary,
    Page,
    Raw,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExportMetadataResult {
    Full {
        record: MetadataRegistryRecord,
        jsonld: String,
    },
    Summary {
        record: MetadataRegistryRecord,
        jsonld: String,
    },
    Page {
        record: MetadataRegistryRecord,
        page: MetadataRoCratePage,
    },
    Raw {
        record: MetadataRegistryRecord,
        raw: MetadataRawView,
        dataset_digest: Option<[u8; 32]>,
    },
}

/// A document's raw revision with how far its projection has caught up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataRawView {
    pub revision: MetadataRawRevision,
    pub projection_state: MaterializationState,
    pub projected_event_id: Option<Ulid>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CreateDocumentPayload {
    Scaffold {
        name: String,
        description: String,
        date_published: String,
        license: Option<String>,
    },
    RoCrate {
        jsonld: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UpdateDocumentMutation {
    ReplaceRoCrate {
        jsonld: String,
    },
    UpsertDataEntity {
        jsonld: String,
    },
    UpsertContextualEntity {
        jsonld: String,
    },
    /// Replaces one data entity by `@id`, so the properties it omits go.
    ReplaceDataEntity {
        jsonld: String,
    },
    /// Removes one root-linked `File` entity by `@id` and the root's link to it.
    RemoveDataEntity {
        id: String,
    },
    /// A batch a device planned against its own replica. It is appended
    /// verbatim, under the device's actor, so both replicas converge on it.
    ApplyBatch {
        batch: Box<MetadataBatch>,
        authored: MetadataBatchSource,
    },
    /// Opens a new history window with this holder's current graph state.
    Checkpoint,
}

/// One readable document that holds a looked-up identifier.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentifierMatch {
    pub document_id: Ulid,
    /// The document's w3id PID once it is active.
    pub pid: Option<String>,
    pub origin: IdentifierOrigin,
}
