use std::collections::{BTreeMap, HashMap};

use craqle::VectorClock;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::NodeId;
use crate::structs::{AuthContext, MetadataAuditOperation, MetadataRegistryRecord, RealmId};
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

/// Durability policy for metadata backend mutations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataRequestDurability {
    /// Persist the metadata backend before acknowledging the request.
    ///
    /// The local flush strength is controlled by `ARUNA_FJALL_PERSIST_MODE`:
    /// `buffer` flushes to OS buffers, while `sync_all` waits for Fjall's
    /// data-and-metadata fsync path.
    #[default]
    Durable,
    /// Use when the metadata event has already been accepted by the WAL path.
    ///
    /// Craqle/document-sync projection persistence may be deferred, but this does not
    /// upgrade the WAL write beyond the configured Fjall persist mode.
    WalAlreadyDurable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataCreateCrateRequest {
    pub graph_iri: String,
    pub name: String,
    pub description: String,
    pub date_published: String,
    pub license: Option<String>,
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
        license: Option<String>,
    },
    RoCrate {
        jsonld: String,
    },
    ReplaceRoCrate {
        jsonld: String,
    },
    UpsertDataEntity {
        jsonld: String,
    },
    UpsertContextualEntity {
        jsonld: String,
    },
}

impl MetadataCreateEventPayload {
    pub fn audit_operation(&self) -> MetadataAuditOperation {
        match self {
            Self::Scaffold { .. } | Self::RoCrate { .. } => MetadataAuditOperation::Create,
            Self::ReplaceRoCrate { .. } => MetadataAuditOperation::ReplaceRoCrate,
            Self::UpsertDataEntity { .. } => MetadataAuditOperation::UpsertDataEntity,
            Self::UpsertContextualEntity { .. } => MetadataAuditOperation::UpsertContextualEntity,
        }
    }

    pub fn requires_existing_graph(&self) -> bool {
        matches!(
            self,
            Self::ReplaceRoCrate { .. }
                | Self::UpsertDataEntity { .. }
                | Self::UpsertContextualEntity { .. }
        )
    }

    pub fn materialization_kind(&self) -> &'static str {
        match self {
            Self::Scaffold { .. } => "scaffold",
            Self::RoCrate { .. } => "rocrate",
            Self::ReplaceRoCrate { .. } => "replace_rocrate",
            Self::UpsertDataEntity { .. } => "upsert_data_entity",
            Self::UpsertContextualEntity { .. } => "upsert_contextual_entity",
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataRawRevision {
    pub jsonld: String,
    pub winning_event_id: Ulid,
    pub context_digest: [u8; 32],
    pub dataset_digest: Option<[u8; 32]>,
}

#[derive(Deserialize)]
struct MetadataRawContext<'a> {
    #[serde(borrow, rename = "@context")]
    context: &'a serde_json::value::RawValue,
}

pub fn resolve_raw_revision(
    events: &[MetadataCreateEventRecord],
) -> Result<Option<MetadataRawRevision>, MetadataError> {
    let Some(base) = events
        .iter()
        .filter(|event| {
            matches!(
                event.payload,
                MetadataCreateEventPayload::RoCrate { .. }
                    | MetadataCreateEventPayload::ReplaceRoCrate { .. }
            )
        })
        .max_by_key(|event| (event.record.updated_at_ms, event.event_id))
    else {
        return Ok(None);
    };
    let jsonld = match &base.payload {
        MetadataCreateEventPayload::RoCrate { jsonld }
        | MetadataCreateEventPayload::ReplaceRoCrate { jsonld } => jsonld,
        _ => unreachable!(),
    };
    let raw_context: MetadataRawContext = serde_json::from_str(jsonld)
        .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
    let context_digest = *blake3::hash(raw_context.context.get().as_bytes()).as_bytes();
    let mut document: serde_json::Value = serde_json::from_str(jsonld)
        .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
    let mut updates = events
        .iter()
        .filter(|event| {
            event.event_id > base.event_id
                && matches!(
                    event.payload,
                    MetadataCreateEventPayload::UpsertDataEntity { .. }
                        | MetadataCreateEventPayload::UpsertContextualEntity { .. }
                )
        })
        .collect::<Vec<_>>();
    updates.sort_by_key(|event| event.event_id);
    let mut winning_event_id = base.event_id;
    for event in updates {
        let (jsonld, link_root) = match &event.payload {
            MetadataCreateEventPayload::UpsertDataEntity { jsonld } => (jsonld, true),
            MetadataCreateEventPayload::UpsertContextualEntity { jsonld } => (jsonld, false),
            _ => unreachable!(),
        };
        apply_raw_upsert(&mut document, jsonld, link_root)?;
        winning_event_id = event.event_id;
    }
    let jsonld = serde_json::to_string(&document)
        .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
    let dataset_digest = craqle::canonicalize_jsonld(&jsonld)
        .ok()
        .map(|canonical| canonical.digest);
    Ok(Some(MetadataRawRevision {
        jsonld,
        winning_event_id,
        context_digest,
        dataset_digest,
    }))
}

fn apply_raw_upsert(
    document: &mut serde_json::Value,
    jsonld: &str,
    link_root: bool,
) -> Result<(), MetadataError> {
    let entity: serde_json::Value = serde_json::from_str(jsonld)
        .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
    let terms = RawTerms::new(document);
    let entity_id = raw_entity_id(&entity, &terms)
        .ok_or_else(|| MetadataError::InvalidInput("entity @id is missing".to_string()))?;
    let graph = document
        .as_object_mut()
        .and_then(|object| {
            object
                .iter_mut()
                .find_map(|(key, value)| terms.is_graph(key).then_some(value))
        })
        .and_then(serde_json::Value::as_array_mut)
        .ok_or_else(|| {
            MetadataError::InvalidInput("RO-Crate @graph array is missing".to_string())
        })?;
    if let Some(existing) = graph
        .iter_mut()
        .find(|entry| raw_entity_id(entry, &terms).as_deref() == Some(entity_id.as_str()))
    {
        let existing = existing.as_object_mut().ok_or_else(|| {
            MetadataError::InvalidInput("RO-Crate entity must be an object".to_string())
        })?;
        let update = entity.as_object().ok_or_else(|| {
            MetadataError::InvalidInput("entity payload must be an object".to_string())
        })?;
        for (property, value) in update {
            if terms.is_id(property) {
                existing.retain(|key, _| !terms.is_id(key));
            }
            existing.insert(property.clone(), value.clone());
        }
    } else {
        graph.push(entity);
    }
    if link_root {
        link_raw_entity(graph, &entity_id, &terms)?;
    }
    Ok(())
}

struct RawTerms {
    terms: HashMap<String, Option<String>>,
}

impl RawTerms {
    fn new(document: &serde_json::Value) -> Self {
        let mut terms = HashMap::new();
        if let Some(context) = document.get("@context") {
            collect_raw_terms(context, &mut terms);
        }
        Self { terms }
    }

    fn is_id(&self, key: &str) -> bool {
        key == "@id"
            || self
                .terms
                .get(key)
                .is_some_and(|iri| iri.as_deref() == Some("@id"))
    }

    fn is_graph(&self, key: &str) -> bool {
        key == "@graph"
            || self
                .terms
                .get(key)
                .is_some_and(|iri| iri.as_deref() == Some("@graph"))
    }

    fn expands_to(&self, key: &str, values: &[&str]) -> bool {
        match self.terms.get(key) {
            Some(Some(iri)) => values.contains(&iri.as_str()),
            Some(None) => false,
            None => values.contains(&key),
        }
    }

    fn term_matches(&self, term: &str, values: &[&str]) -> bool {
        match self.terms.get(term) {
            Some(Some(iri)) => values.contains(&iri.as_str()),
            Some(None) => false,
            None => true,
        }
    }
}

fn collect_raw_terms(context: &serde_json::Value, terms: &mut HashMap<String, Option<String>>) {
    match context {
        serde_json::Value::Array(values) => {
            for value in values {
                collect_raw_terms(value, terms);
            }
        }
        serde_json::Value::Object(values) => {
            for (term, definition) in values {
                let iri = match definition {
                    serde_json::Value::String(iri) => Some(iri.as_str()),
                    serde_json::Value::Object(definition) => {
                        definition.get("@id").and_then(serde_json::Value::as_str)
                    }
                    _ => None,
                };
                terms.insert(term.clone(), iri.map(str::to_string));
            }
        }
        _ => {}
    }
}

fn raw_entity_id(entity: &serde_json::Value, terms: &RawTerms) -> Option<String> {
    let id = entity
        .as_object()?
        .iter()
        .find_map(|(key, value)| terms.is_id(key).then(|| value.as_str()).flatten())?;
    Some(
        if id == "ro-crate-metadata.json"
            || id.starts_with("./")
            || id.starts_with("../")
            || id.starts_with('#')
            || id.starts_with("_:")
            || id.contains("://")
            || (id.contains(':') && !id.contains('/'))
        {
            id.to_string()
        } else {
            format!("./{id}")
        },
    )
}

fn link_raw_entity(
    graph: &mut [serde_json::Value],
    entity_id: &str,
    terms: &RawTerms,
) -> Result<(), MetadataError> {
    let root_id = graph
        .iter()
        .find(|entry| raw_entity_id(entry, terms).as_deref() == Some("ro-crate-metadata.json"))
        .and_then(serde_json::Value::as_object)
        .and_then(|descriptor| {
            descriptor.iter().find_map(|(key, value)| {
                terms
                    .expands_to(
                        key,
                        &[
                            "about",
                            "schema:about",
                            "http://schema.org/about",
                            "https://schema.org/about",
                        ],
                    )
                    .then(|| raw_entity_id(value, terms))
                    .flatten()
            })
        })
        .unwrap_or_else(|| "./".to_string());
    let root = graph
        .iter_mut()
        .find(|entry| raw_entity_id(entry, terms).as_deref() == Some(root_id.as_str()))
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| MetadataError::InvalidInput("RO-Crate root is missing".to_string()))?;
    let has_part = root
        .keys()
        .find(|key| {
            terms.expands_to(
                key,
                &[
                    "hasPart",
                    "schema:hasPart",
                    "http://schema.org/hasPart",
                    "https://schema.org/hasPart",
                ],
            )
        })
        .cloned()
        .unwrap_or_else(|| {
            if terms.term_matches(
                "hasPart",
                &[
                    "schema:hasPart",
                    "http://schema.org/hasPart",
                    "https://schema.org/hasPart",
                ],
            ) {
                "hasPart".to_string()
            } else {
                "https://schema.org/hasPart".to_string()
            }
        });
    let reference = serde_json::json!({ "@id": entity_id });
    match root.get_mut(&has_part) {
        Some(serde_json::Value::Array(values))
            if values
                .iter()
                .any(|value| raw_entity_id(value, terms).as_deref() == Some(entity_id)) => {}
        Some(serde_json::Value::Array(values)) => values.push(reference),
        Some(value) if raw_entity_id(value, terms).as_deref() == Some(entity_id) => {}
        Some(value) => {
            *value = serde_json::Value::Array(vec![value.clone(), reference]);
        }
        None => {
            root.insert(has_part, serde_json::Value::Array(vec![reference]));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataDocumentLifecycleRecord {
    Upsert {
        event: Box<MetadataCreateEventRecord>,
    },
    Delete {
        event: MetadataDocumentDeleteRecord,
    },
}

impl MetadataDocumentLifecycleRecord {
    pub fn document_id(&self) -> Ulid {
        match self {
            Self::Upsert { event } => event.record.document_id,
            Self::Delete { event } => event.tombstone.document_id,
        }
    }

    pub fn event_id(&self) -> Ulid {
        match self {
            Self::Upsert { event } => event.event_id,
            Self::Delete { event } => event.event_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataDocumentDeleteRecord {
    pub event_id: Ulid,
    pub tombstone: MetadataGraphLifecycleRecord,
    pub deleted_after_event_id: Ulid,
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
    pub context_digest: Option<[u8; 32]>,
    pub dataset_digest: Option<[u8; 32]>,
    pub state: MetadataMaterializationState,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataIriReferenceIndexRecord {
    pub document_id: Ulid,
    pub document_cursor: Ulid,
    pub predicate_iri: String,
    pub object_iri: String,
    pub subject_iris: Vec<String>,
}

impl MetadataMaterializationStatusRecord {
    pub fn pending(event: &MetadataCreateEventRecord, updated_at_ms: u64) -> Self {
        Self {
            document_id: event.record.document_id,
            event_id: event.event_id,
            graph_iri: event.record.graph_iri.clone(),
            context_digest: None,
            dataset_digest: None,
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
pub struct MetadataGraphPruneJobRecord {
    pub graph_iri: String,
    pub due_at_ms: u64,
    pub attempts: u32,
    pub last_error: Option<String>,
}

impl MetadataGraphPruneJobRecord {
    pub fn new(graph_iri: String, due_at_ms: u64) -> Self {
        Self {
            graph_iri,
            due_at_ms,
            attempts: 0,
            last_error: None,
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
    #[serde(default)]
    pub durability: MetadataRequestDurability,
    #[serde(default)]
    pub deterministic_actor: Option<[u8; 32]>,
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
    pub title: String,
    pub snippet: Option<String>,
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

impl MetadataQueryResults {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Solutions(_) => "solutions",
            Self::Boolean(_) => "boolean",
            Self::Graph(_) => "graph",
        }
    }
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
    #[error("metadata validation failed: {0:?}")]
    Validation(Vec<MetadataValidationViolation>),
    #[error("metadata graph not found")]
    GraphNotFound,
    #[error("metadata backend error: {0}")]
    Backend(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataValidationViolation {
    pub code: String,
    pub message: String,
    pub pointer: String,
    pub entity_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{
        MetadataClockRelation, MetadataCreateEventPayload, MetadataCreateEventRecord,
        MetadataDocumentDeleteRecord, MetadataDocumentLifecycleRecord,
        MetadataGraphLifecycleRecord, MetadataQueryResults, apply_raw_upsert,
        compare_metadata_clocks, resolve_raw_revision,
    };
    use crate::structs::{MetadataRegistryRecord, PlacementRef, RealmId};
    use crate::{NodeId, UserId};
    use craqle::{ActorId, VectorClock};
    use std::collections::BTreeMap;
    use ulid::Ulid;

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

    #[test]
    fn metadata_query_results_kind_labels_variants() {
        assert_eq!(
            MetadataQueryResults::Solutions(Vec::new()).kind(),
            "solutions"
        );
        assert_eq!(MetadataQueryResults::Boolean(true).kind(), "boolean");
        assert_eq!(MetadataQueryResults::Graph(Vec::new()).kind(), "graph");
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn create_event(document_id: Ulid, event_id: Ulid) -> MetadataCreateEventRecord {
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let group_id = Ulid::generate();
        let document_path = "datasets/lifecycle";
        let record = MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: document_path.to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                document_path,
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: vec![node(1)],
            created_at_ms: 1,
            updated_at_ms: 1,
            last_event_id: event_id,
        };
        MetadataCreateEventRecord {
            event_id,
            record,
            user_id: UserId::local(Ulid::generate(), realm_id),
            node_id: node(1),
            payload: MetadataCreateEventPayload::Scaffold {
                name: "Lifecycle".to_string(),
                description: "Lifecycle envelope".to_string(),
                date_published: "2026-01-01".to_string(),
                license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            },
            occurred_at_ms: 1,
        }
    }

    fn raw_event(
        document_id: Ulid,
        event_id: Ulid,
        updated_at_ms: u64,
        payload: MetadataCreateEventPayload,
    ) -> MetadataCreateEventRecord {
        let mut event = create_event(document_id, event_id);
        event.record.updated_at_ms = updated_at_ms;
        event.record.last_event_id = event_id;
        event.occurred_at_ms = updated_at_ms;
        event.payload = payload;
        event
    }

    #[test]
    fn raw_revision_replays() {
        let document_id = Ulid::generate();
        let base_id = Ulid::from_parts(2, 0);
        let context_id = Ulid::from_parts(3, 0);
        let data_id = Ulid::from_parts(4, 0);
        let base = serde_json::json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "about": { "@id": "./" }
                },
                {
                    "@id": "./",
                    "@type": "Dataset",
                    "name": "crate"
                },
                {
                    "@id": "#person",
                    "@type": "Person",
                    "name": "before",
                    "affiliation": { "@id": "#org" }
                }
            ]
        });
        let events = vec![
            raw_event(
                document_id,
                base_id,
                2,
                MetadataCreateEventPayload::RoCrate {
                    jsonld: base.to_string(),
                },
            ),
            raw_event(
                document_id,
                context_id,
                3,
                MetadataCreateEventPayload::UpsertContextualEntity {
                    jsonld: serde_json::json!({
                        "@id": "#person",
                        "@type": "Person",
                        "name": "after"
                    })
                    .to_string(),
                },
            ),
            raw_event(
                document_id,
                data_id,
                4,
                MetadataCreateEventPayload::UpsertDataEntity {
                    jsonld: serde_json::json!({
                        "@id": "data/file.txt",
                        "@type": "File",
                        "name": "file"
                    })
                    .to_string(),
                },
            ),
        ];

        let revision = resolve_raw_revision(&events).unwrap().unwrap();
        let raw: serde_json::Value = serde_json::from_str(&revision.jsonld).unwrap();
        let graph = raw["@graph"].as_array().unwrap();
        let person = graph
            .iter()
            .find(|entry| entry["@id"] == "#person")
            .unwrap();
        assert_eq!(person["name"], "after");
        assert_eq!(person["affiliation"]["@id"], "#org");
        let root = graph.iter().find(|entry| entry["@id"] == "./").unwrap();
        assert_eq!(root["hasPart"][0]["@id"], "./data/file.txt");
        assert_eq!(revision.winning_event_id, data_id);
    }

    #[test]
    fn raw_uses_lww() {
        let document_id = Ulid::generate();
        let newer_id = Ulid::from_parts(2, 0);
        let later_id = Ulid::from_parts(3, 0);
        let document = |name: &str| {
            serde_json::json!({
                "@context": "https://w3id.org/ro/crate/1.2/context",
                "@graph": [{ "@id": "./", "@type": "Dataset", "name": name }]
            })
            .to_string()
        };
        let events = vec![
            raw_event(
                document_id,
                newer_id,
                10,
                MetadataCreateEventPayload::RoCrate {
                    jsonld: document("winner"),
                },
            ),
            raw_event(
                document_id,
                later_id,
                9,
                MetadataCreateEventPayload::ReplaceRoCrate {
                    jsonld: document("later event"),
                },
            ),
        ];

        let revision = resolve_raw_revision(&events).unwrap().unwrap();
        let raw: serde_json::Value = serde_json::from_str(&revision.jsonld).unwrap();
        assert_eq!(raw["@graph"][0]["name"], "winner");
        assert_eq!(revision.winning_event_id, newer_id);
    }

    #[test]
    fn raw_context_digest() {
        let document_id = Ulid::generate();
        let event_id = Ulid::from_parts(2, 0);
        let jsonld = r#"{"@context":[ "https://w3id.org/ro/crate/1.2/context" ],"@graph":[]}"#;
        let events = vec![raw_event(
            document_id,
            event_id,
            1,
            MetadataCreateEventPayload::RoCrate {
                jsonld: jsonld.to_string(),
            },
        )];

        let revision = resolve_raw_revision(&events).unwrap().unwrap();
        assert_eq!(
            revision.context_digest,
            *blake3::hash(br#"[ "https://w3id.org/ro/crate/1.2/context" ]"#).as_bytes()
        );
        assert!(revision.dataset_digest.is_some());
    }

    #[test]
    fn raw_aliases_apply() {
        let mut document = serde_json::json!({
            "@context": [
                "https://w3id.org/ro/crate/1.2/context",
                {
                    "items": "@graph",
                    "node": "@id",
                    "relation": "http://schema.org/about",
                    "parts": "http://schema.org/hasPart"
                }
            ],
            "items": [
                {
                    "node": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "relation": {"node": "./"}
                },
                {
                    "node": "./",
                    "@type": "Dataset",
                    "parts": []
                }
            ]
        });
        let entity = serde_json::json!({
            "node": "data/a.txt",
            "@type": "File",
            "name": "a"
        })
        .to_string();

        apply_raw_upsert(&mut document, &entity, true).unwrap();

        assert_eq!(document["items"][2]["node"], "data/a.txt");
        assert_eq!(document["items"][1]["parts"][0]["@id"], "./data/a.txt");
    }

    #[test]
    fn raw_overrides_preserved() {
        let mut document = serde_json::json!({
            "@context": [
                "https://w3id.org/ro/crate/1.2/context",
                {
                    "about": "https://example.test/about",
                    "hasPart": "https://example.test/hasPart"
                }
            ],
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "http://schema.org/about": {"@id": "./"}
                },
                {
                    "@id": "./",
                    "@type": "Dataset",
                    "hasPart": "preserved"
                }
            ]
        });
        let entity = serde_json::json!({
            "@id": "data/a.txt",
            "@type": "File"
        })
        .to_string();

        apply_raw_upsert(&mut document, &entity, true).unwrap();

        assert_eq!(document["@graph"][1]["hasPart"], "preserved");
        assert_eq!(
            document["@graph"][1]["https://schema.org/hasPart"][0]["@id"],
            "./data/a.txt"
        );
    }

    #[test]
    fn metadata_document_lifecycle_upsert_wraps_create_event() {
        let document_id = Ulid::generate();
        let event_id = Ulid::generate();
        let create = create_event(document_id, event_id);

        let lifecycle = MetadataDocumentLifecycleRecord::Upsert {
            event: Box::new(create.clone()),
        };

        assert_eq!(lifecycle.document_id(), document_id);
        assert_eq!(lifecycle.event_id(), event_id);
        assert_eq!(
            postcard::from_bytes::<MetadataDocumentLifecycleRecord>(
                &postcard::to_allocvec(&lifecycle).expect("lifecycle serializes")
            )
            .expect("lifecycle decodes"),
            lifecycle
        );
    }

    #[test]
    fn metadata_document_lifecycle_delete_carries_tombstone_and_fence() {
        let document_id = Ulid::generate();
        let event_id = Ulid::generate();
        let deleted_after_event_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let group_id = Ulid::generate();
        let graph_iri = MetadataRegistryRecord::graph_iri_for(document_id);
        let tombstone = MetadataGraphLifecycleRecord::deleted(
            graph_iri.clone(),
            realm_id,
            group_id,
            document_id,
            2,
        );

        let lifecycle = MetadataDocumentLifecycleRecord::Delete {
            event: MetadataDocumentDeleteRecord {
                event_id,
                tombstone: tombstone.clone(),
                deleted_after_event_id,
            },
        };

        assert_eq!(lifecycle.document_id(), document_id);
        assert_eq!(lifecycle.event_id(), event_id);
        let MetadataDocumentLifecycleRecord::Delete { event } = lifecycle else {
            panic!("expected delete lifecycle record");
        };
        assert_eq!(event.tombstone, tombstone);
        assert_eq!(event.deleted_after_event_id, deleted_after_event_id);
    }
}
