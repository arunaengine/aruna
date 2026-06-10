use std::collections::BTreeMap;

use craqle::VectorClock;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::NodeId;
use crate::structs::{AuthContext, MetadataRegistryRecord, RealmId};
use crate::types::{GroupId, UserId};

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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataRequestDurability {
    #[default]
    Durable,
    WalAlreadyDurable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataCreateCrateRequest {
    pub graph_iri: String,
    pub name: String,
    pub description: String,
    pub date_published: String,
    pub license: String,
    pub policy: MetadataGraphPolicy,
    #[serde(default)]
    pub durability: MetadataRequestDurability,
    #[serde(default)]
    pub deterministic_actor: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataCreateEventPayload {
    Scaffold {
        name: String,
        description: String,
        date_published: String,
        license: String,
    },
    RoCrate {
        jsonld: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataCreateEventRecord {
    pub event_id: Ulid,
    pub record: MetadataRegistryRecord,
    pub user_id: UserId,
    pub node_id: NodeId,
    pub payload: MetadataCreateEventPayload,
    pub occurred_at_ms: u64,
}

/// CRDT actor used when materializing `event_id` into the local graph store,
/// identical on every holder so replayed materializations dedupe exactly.
pub fn deterministic_materialization_actor(event_id: Ulid) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna-metadata-materialization-v1\0");
    hasher.update(&event_id.to_bytes());
    *hasher.finalize().as_bytes()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataMaterializationState {
    Pending,
    Materialized,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataMaterializationStatusRecord {
    pub document_id: Ulid,
    pub event_id: Ulid,
    pub graph_iri: String,
    pub state: MetadataMaterializationState,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub updated_at_ms: u64,
}

impl MetadataMaterializationStatusRecord {
    pub fn pending(event: &MetadataCreateEventRecord, updated_at_ms: u64) -> Self {
        Self {
            document_id: event.record.document_id,
            event_id: event.event_id,
            graph_iri: event.record.graph_iri.clone(),
            state: MetadataMaterializationState::Pending,
            attempts: 0,
            last_error: None,
            updated_at_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataMaterializationJobRecord {
    pub document_id: Ulid,
    pub event_id: Ulid,
    pub due_at_ms: u64,
    pub attempts: u32,
}

impl MetadataMaterializationJobRecord {
    pub fn new(event: &MetadataCreateEventRecord, due_at_ms: u64) -> Self {
        Self {
            document_id: event.record.document_id,
            event_id: event.event_id,
            due_at_ms,
            attempts: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataApplyRoCrateRequest {
    pub graph_iri: String,
    pub jsonld: String,
    pub policy: MetadataGraphPolicy,
    #[serde(default)]
    pub durability: MetadataRequestDurability,
    #[serde(default)]
    pub deterministic_actor: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataUpsertEntityRequest {
    pub graph_iri: String,
    pub jsonld: String,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataClockRelation {
    Equal,
    LocalAhead,
    RemoteAhead,
    Concurrent,
}

pub fn compare_metadata_clocks(local: &VectorClock, remote: &VectorClock) -> MetadataClockRelation {
    let mut local_ahead = false;
    let mut remote_ahead = false;

    for actor in local.0.keys().chain(remote.0.keys()) {
        let local = local.0.get(actor).copied().unwrap_or_default();
        let remote = remote.0.get(actor).copied().unwrap_or_default();
        if local > remote {
            local_ahead = true;
        }
        if remote > local {
            remote_ahead = true;
        }
    }

    match (local_ahead, remote_ahead) {
        (false, false) => MetadataClockRelation::Equal,
        (true, false) => MetadataClockRelation::LocalAhead,
        (false, true) => MetadataClockRelation::RemoteAhead,
        (true, true) => MetadataClockRelation::Concurrent,
    }
}

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
        witnessed: VectorClock,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataBatch {
    pub graph_iri: String,
    pub actor: [u8; 32],
    pub counter: u64,
    pub base_clock: VectorClock,
    pub ops: Vec<MetadataQuadOp>,
    pub timestamp_millis: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataGraphLifecycleStatus {
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataGraphLifecycleRecord {
    pub graph_iri: String,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub status: MetadataGraphLifecycleStatus,
    pub updated_at_ms: u64,
}

impl MetadataGraphLifecycleRecord {
    pub fn deleted(
        graph_iri: String,
        realm_id: RealmId,
        group_id: GroupId,
        document_id: Ulid,
        updated_at_ms: u64,
    ) -> Self {
        Self {
            graph_iri,
            realm_id,
            group_id,
            document_id,
            status: MetadataGraphLifecycleStatus::Deleted,
            updated_at_ms,
        }
    }

    pub fn is_deleted(&self) -> bool {
        matches!(self.status, MetadataGraphLifecycleStatus::Deleted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataQueryResults {
    Solutions(Vec<BTreeMap<String, String>>),
    Boolean(bool),
    Graph(Vec<(String, String, String)>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataEffect {
    ValidateCreateCrate {
        request: MetadataCreateCrateRequest,
    },
    ValidateRoCrate {
        request: MetadataApplyRoCrateRequest,
    },
    CreateCrate {
        request: MetadataCreateCrateRequest,
    },
    ApplyRoCrate {
        request: MetadataApplyRoCrateRequest,
    },
    UpsertDataEntity {
        request: MetadataUpsertEntityRequest,
    },
    UpsertContextualEntity {
        request: MetadataUpsertEntityRequest,
    },
    SetGraphPolicy {
        graph_iri: String,
        policy: MetadataGraphPolicy,
    },
    AddGraphPeer {
        graph_iri: String,
        node_id: NodeId,
    },
    SyncGraphBestEffort {
        graph_iri: String,
        peers: Vec<NodeId>,
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetadataEvent {
    ValidationResult {
        graph_iri: String,
    },
    CreateCrateResult {
        graph_iri: String,
        batch: MetadataBatch,
    },
    ApplyRoCrateResult {
        graph_iri: String,
        batch: MetadataBatch,
    },
    EntityUpsertResult {
        graph_iri: String,
        batch: MetadataBatch,
    },
    GraphPolicySet {
        graph_iri: String,
    },
    GraphPeerAdded {
        graph_iri: String,
        node_id: NodeId,
    },
    GraphSyncScheduled {
        graph_iri: String,
        peers: Vec<NodeId>,
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
    #[error("invalid metadata input: {0}")]
    InvalidInput(String),
    #[error("metadata graph not found")]
    GraphNotFound,
    #[error("metadata backend error: {0}")]
    Backend(String),
}

#[cfg(test)]
mod tests {
    use super::{MetadataClockRelation, compare_metadata_clocks};
    use craqle::{ActorId, VectorClock};
    use std::collections::BTreeMap;

    #[test]
    fn compares_metadata_vector_clocks() {
        let empty = VectorClock::default();
        let local = VectorClock(BTreeMap::from([(ActorId::from_bytes([1u8; 32]), 2)]));
        let remote = VectorClock(BTreeMap::from([(ActorId::from_bytes([1u8; 32]), 1)]));
        let concurrent = VectorClock(BTreeMap::from([(ActorId::from_bytes([2u8; 32]), 1)]));

        assert_eq!(
            compare_metadata_clocks(&empty, &empty),
            MetadataClockRelation::Equal
        );
        assert_eq!(
            compare_metadata_clocks(&local, &remote),
            MetadataClockRelation::LocalAhead
        );
        assert_eq!(
            compare_metadata_clocks(&remote, &local),
            MetadataClockRelation::RemoteAhead
        );
        assert_eq!(
            compare_metadata_clocks(&local, &concurrent),
            MetadataClockRelation::Concurrent
        );
    }
}
