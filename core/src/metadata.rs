use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::structs::{AuthContext, MetadataRegistryRecord};
use crate::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataGraphPolicy {
    pub public: bool,
    pub permission_paths: Vec<String>,
}

impl MetadataGraphPolicy {
    pub fn normalized(mut self) -> Self {
        self.permission_paths.sort();
        self.permission_paths.dedup();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataCreateCrateRequest {
    pub graph_iri: String,
    pub name: String,
    pub description: String,
    pub date_published: String,
    pub license: String,
    pub policy: MetadataGraphPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataApplyRoCrateRequest {
    pub graph_iri: String,
    pub jsonld: String,
    pub policy: MetadataGraphPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataDocumentView {
    pub record: MetadataRegistryRecord,
    pub jsonld: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataRoCratePage {
    pub jsonld: String,
    pub total_data_entities: usize,
    pub returned_data_entities: usize,
    pub next_offset: Option<usize>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetadataSearchHit {
    pub document_id: String,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub subject_iri: String,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataDot {
    pub actor: [u8; 32],
    pub counter: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataVectorClock(pub BTreeMap<[u8; 32], u64>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataQuadOp {
    Add {
        subject: String,
        predicate: String,
        object: String,
        dot: MetadataDot,
    },
    Remove {
        subject: String,
        predicate: String,
        object: String,
        witnessed: MetadataVectorClock,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataBatch {
    pub graph_iri: String,
    pub actor: [u8; 32],
    pub counter: u64,
    pub base_clock: MetadataVectorClock,
    pub ops: Vec<MetadataQuadOp>,
    pub timestamp_millis: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataQueryResults {
    Solutions(Vec<BTreeMap<String, String>>),
    Boolean(bool),
    Graph(Vec<(String, String, String)>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataEffect {
    CreateCrate {
        request: MetadataCreateCrateRequest,
    },
    ApplyRoCrate {
        request: MetadataApplyRoCrateRequest,
    },
    SetGraphPolicy {
        graph_iri: String,
        policy: MetadataGraphPolicy,
    },
    GetGraphPolicy {
        graph_iri: String,
    },
    ExportRoCrate {
        graph_iri: String,
    },
    ExportRoCrateSummary {
        graph_iri: String,
    },
    ExportRoCratePage {
        graph_iri: String,
        offset: Option<usize>,
        after: Option<String>,
        limit: usize,
    },
    SearchGraphs {
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
    },
    QueryGraphs {
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        sparql: String,
    },
    DeleteGraph {
        graph_iri: String,
    },
    ListGraphs,
    ContainsGraph {
        graph_iri: String,
    },
    VectorClock {
        graph_iri: String,
    },
    CatchupBatches {
        graph_iri: String,
        remote_clock: MetadataVectorClock,
    },
    ReplicateBootstrap {
        record: MetadataRegistryRecord,
    },
    ReplicateBatch {
        record: MetadataRegistryRecord,
        batch: MetadataBatch,
    },
    ReplicateDelete {
        record: MetadataRegistryRecord,
    },
    ApplyRemoteBatch {
        batch: MetadataBatch,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetadataEvent {
    CreateCrateResult {
        graph_iri: String,
        batch: MetadataBatch,
    },
    ApplyRoCrateResult {
        graph_iri: String,
        batch: MetadataBatch,
    },
    GraphPolicySet {
        graph_iri: String,
    },
    GraphPolicyResult {
        graph_iri: String,
        policy: MetadataGraphPolicy,
    },
    RoCrateExportResult {
        graph_iri: String,
        jsonld: String,
    },
    RoCrateSummaryResult {
        graph_iri: String,
        jsonld: String,
    },
    RoCratePageResult {
        graph_iri: String,
        page: MetadataRoCratePage,
    },
    SearchResult {
        hits: Vec<MetadataSearchHit>,
    },
    QueryResult {
        results: MetadataQueryResults,
    },
    GraphDeleted {
        graph_iri: String,
    },
    GraphListResult {
        graph_iris: Vec<String>,
    },
    ContainsGraphResult {
        graph_iri: String,
        exists: bool,
    },
    VectorClockResult {
        graph_iri: String,
        clock: MetadataVectorClock,
    },
    CatchupBatchesResult {
        graph_iri: String,
        batches: Vec<MetadataBatch>,
    },
    BootstrapReplicated {
        graph_iri: String,
        replicated_node_ids: Vec<NodeId>,
    },
    BatchReplicated {
        graph_iri: String,
        replicated_node_ids: Vec<NodeId>,
    },
    DeleteReplicated {
        graph_iri: String,
        replicated_node_ids: Vec<NodeId>,
    },
    RemoteBatchApplied {
        graph_iri: String,
    },
    Error {
        graph_iri: Option<String>,
        error: MetadataError,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MetadataError {
    #[error("channel closed")]
    ChannelClosed,
    #[error("invalid effect type")]
    InvalidEffect,
    #[error("metadata backend unavailable")]
    HandleMissing,
    #[error("backend task failed: {0}")]
    TaskJoin(String),
    #[error("metadata backend error: {0}")]
    Backend(String),
}
