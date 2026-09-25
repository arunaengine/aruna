//! Defines metadata auth tokens, crate events, raw revisions and validation records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap};

use craqle::{GraphReplicaSnapshot, VectorClock};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::NodeId;
use crate::UserId;
use crate::errors::StorageError;
use crate::structs::identity::auth::AuthContext;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::metadata_registry::{MetadataAuditOperation, MetadataRegistryRecord};
use crate::types::GroupId;

pub const MAX_TOKEN_LEN: usize = 4096;

/// The community Profile the node carries shapes for, so a crate tagged with it
/// is validated without a realm document registering it.
pub const CRATE_PROFILE_IRI: &str = "https://w3id.org/ro/wfrun/process/0.5";

/// Requirements of a Zenodo record: the DataCite fields.
pub const ZENODO_PROFILE_IRI: &str = "https://w3id.org/aruna/profiles/repository/zenodo";

/// Requirements of an InvenioRDM record: the DataCite fields and a publisher.
pub const INVENIO_PROFILE_IRI: &str = "https://w3id.org/aruna/profiles/repository/invenio";

/// Whether the node validates this IRI from its own embedded shapes.
pub fn is_builtin_profile(iri: &str) -> bool {
    iri == CRATE_PROFILE_IRI || crate::repository::builtin_profile(iri).is_some()
}

/// Supported RO-Crate specification IRIs and the remaining RO-Crate community profiles (workflow run
/// crates, Workflow RO-Crate) are version markers, not Profiles. A built-in Profile is deliberately not
/// a marker: it has to reach validation as a Profile tag for its embedded shapes to run.
pub fn is_rocrate_specification(iri: &str) -> bool {
    !is_builtin_profile(iri)
        && (matches!(
            iri,
            "https://w3id.org/ro/crate/1.2" | "https://w3id.org/ro/crate/1.3"
        ) || iri.starts_with("https://w3id.org/ro/wfrun/")
            || iri.starts_with("https://w3id.org/workflowhub/workflow-ro-crate/"))
}

#[cfg(test)]
mod specification_tests {
    use super::{CRATE_PROFILE_IRI, is_builtin_profile, is_rocrate_specification};

    #[test]
    fn community_profiles_markers() {
        assert!(is_rocrate_specification("https://w3id.org/ro/crate/1.3"));
        assert!(is_rocrate_specification(
            "https://w3id.org/ro/wfrun/workflow/0.5"
        ));
        assert!(is_rocrate_specification(
            "https://w3id.org/workflowhub/workflow-ro-crate/1.0"
        ));
        assert!(!is_rocrate_specification(
            "https://w3id.org/aruna/profile/01J"
        ));
        assert!(!is_rocrate_specification("https://example.org/profile"));
    }

    #[test]
    fn builtin_profile_tags() {
        // The built-in Profile must tag, so it is never a bare version marker.
        assert!(is_builtin_profile(CRATE_PROFILE_IRI));
        assert!(!is_rocrate_specification(CRATE_PROFILE_IRI));
        assert!(!is_builtin_profile("https://w3id.org/ro/wfrun/process/0.4"));
        for iri in [super::ZENODO_PROFILE_IRI, super::INVENIO_PROFILE_IRI] {
            assert!(is_builtin_profile(iri) && !is_rocrate_specification(iri));
        }
    }
}

/// Credential a forwarded metadata or job-control request carries to the holder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthToken {
    Bearer(MetadataBearerToken),
    Internal(AuthContext),
}

impl AuthToken {
    pub fn bearer(token: impl Into<String>) -> Result<Self, AuthTokenError> {
        MetadataBearerToken::new(token).map(Self::Bearer)
    }

    pub fn internal(auth: AuthContext) -> Self {
        Self::Internal(auth)
    }
}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct MetadataBearerToken(String);

/// Redacted so a peer reply logged with `?` can never print a credential.
impl std::fmt::Debug for MetadataBearerToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MetadataBearerToken(redacted)")
    }
}

impl MetadataBearerToken {
    pub fn new(token: impl Into<String>) -> Result<Self, AuthTokenError> {
        let token = token.into();
        if token.len() > MAX_TOKEN_LEN {
            return Err(AuthTokenError {
                length: token.len(),
            });
        }
        Ok(Self(token))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for MetadataBearerToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let token = String::deserialize(deserializer)?;
        Self::new(token).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("metadata bearer token length {length} exceeds maximum {MAX_TOKEN_LEN}")]
pub struct AuthTokenError {
    length: usize,
}

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
    /// Persist the metadata backend before acknowledging the request. The local flush strength is
    /// controlled by `ARUNA_FJALL_PERSIST_MODE`: `buffer` flushes to OS buffers, while `sync_all` waits for
    /// Fjall's data-and-metadata fsync path.
    #[default]
    Durable,
    /// Use when the metadata event has already been accepted by the WAL path. Craqle/document-sync
    /// projection persistence may be deferred, but this does not upgrade the WAL write beyond the
    /// configured Fjall persist mode.
    WalAlreadyDurable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataCrateRequest {
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
pub enum MetadataEventPayload {
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
    /// An OR-Set change set fixed at the origin. Every holder materializes it
    /// by merging `batch`; `authored` is kept for audit only.
    ApplyBatch {
        batch: MetadataBatch,
        authored: MetadataBatchSource,
    },
}

/// The author's submission behind a planned batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataBatchSource {
    ReplaceRoCrate { jsonld: String },
    UpsertDataEntity { jsonld: String },
    UpsertContextualEntity { jsonld: String },
}

impl MetadataBatchSource {
    pub fn jsonld(&self) -> &str {
        match self {
            Self::ReplaceRoCrate { jsonld }
            | Self::UpsertDataEntity { jsonld }
            | Self::UpsertContextualEntity { jsonld } => jsonld,
        }
    }

    pub fn audit_operation(&self) -> MetadataAuditOperation {
        match self {
            Self::ReplaceRoCrate { .. } => MetadataAuditOperation::ReplaceRoCrate,
            Self::UpsertDataEntity { .. } => MetadataAuditOperation::UpsertDataEntity,
            Self::UpsertContextualEntity { .. } => MetadataAuditOperation::UpsertContextualEntity,
        }
    }
}

impl MetadataEventPayload {
    pub fn audit_operation(&self) -> MetadataAuditOperation {
        match self {
            Self::Scaffold { .. } | Self::RoCrate { .. } => MetadataAuditOperation::Create,
            Self::ReplaceRoCrate { .. } => MetadataAuditOperation::ReplaceRoCrate,
            Self::UpsertDataEntity { .. } => MetadataAuditOperation::UpsertDataEntity,
            Self::UpsertContextualEntity { .. } => MetadataAuditOperation::UpsertContextualEntity,
            Self::ApplyBatch { authored, .. } => authored.audit_operation(),
        }
    }

    pub fn requires_existing_graph(&self) -> bool {
        matches!(
            self,
            Self::ReplaceRoCrate { .. }
                | Self::UpsertDataEntity { .. }
                | Self::UpsertContextualEntity { .. }
                | Self::ApplyBatch { .. }
        )
    }

    pub fn materialization_kind(&self) -> &'static str {
        match self {
            Self::Scaffold { .. } => "scaffold",
            Self::RoCrate { .. } => "rocrate",
            Self::ReplaceRoCrate { .. } => "replace_rocrate",
            Self::UpsertDataEntity { .. } => "upsert_data_entity",
            Self::UpsertContextualEntity { .. } => "upsert_contextual_entity",
            Self::ApplyBatch { .. } => "apply_batch",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataEventRecord {
    pub event_id: Ulid,
    pub record: MetadataRegistryRecord,
    pub user_id: UserId,
    pub node_id: NodeId,
    pub payload: MetadataEventPayload,
    pub occurred_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataRawRevision {
    /// The displayed revision: the last render that passed profile validation.
    pub jsonld: String,
    /// The last event whose merge is reflected in `jsonld`.
    pub winning_event_id: Ulid,
    pub context_digest: [u8; 32],
    pub dataset_digest: Option<[u8; 32]>,
    pub merged: Option<MetadataMergedRevision>,
}

/// The merged graph render while it fails profile validation. The editor opens
/// it so the owner can fix the document; export keeps serving the displayed one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataMergedRevision {
    pub jsonld: String,
    pub findings: u32,
}

pub const EVENT_LIMIT: u32 = 1024;
pub const RAW_BYTES_LIMIT: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawOriginBudget {
    pub document_id: Ulid,
    pub node_id: NodeId,
    pub event_limit: u32,
    pub byte_limit: u64,
    pub events: u32,
    pub encoded_bytes: u64,
}

/// This node's CRDT actor for one document. Reusing it keeps vector clocks bounded by the
/// number of writing nodes; the counter and last event id only ever grow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataActor {
    pub document_id: Ulid,
    pub actor: [u8; 32],
    pub counter: u64,
    pub last_event_id: Ulid,
}

impl MetadataActor {
    /// The next dot after `current`; a node without one, or whose storage was reset,
    /// starts a fresh actor so it never repeats a dot it may have used before.
    pub fn next(
        current: Option<&Self>,
        document_id: Ulid,
        node_id: NodeId,
        event_id: Ulid,
    ) -> Option<Self> {
        match current {
            Some(current) => Some(Self {
                document_id,
                actor: current.actor,
                counter: current.counter.checked_add(1)?,
                last_event_id: event_id,
            }),
            None => {
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"aruna-metadata-actor-v2\0");
                hasher.update(node_id.as_bytes());
                hasher.update(&document_id.to_bytes());
                hasher.update(&event_id.to_bytes());
                Some(Self {
                    document_id,
                    actor: *hasher.finalize().as_bytes(),
                    counter: 1,
                    last_event_id: event_id,
                })
            }
        }
    }
}

pub fn raw_quotas(
    document_id: Ulid,
    origins: &[NodeId],
    creator: NodeId,
    create_bytes: u64,
) -> Option<Vec<RawOriginBudget>> {
    if create_bytes > RAW_BYTES_LIMIT || origins.is_empty() {
        return None;
    }
    let mut origins = origins.to_vec();
    origins.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    origins.dedup();
    let creator_index = origins.iter().position(|origin| *origin == creator)?;
    let origin_count = u32::try_from(origins.len()).ok()?;
    let remaining_events = EVENT_LIMIT.checked_sub(1)?;
    let event_share = remaining_events / origin_count;
    let event_remainder = remaining_events % origin_count;
    let remaining_bytes = RAW_BYTES_LIMIT.checked_sub(create_bytes)?;
    let origin_count_bytes = u64::from(origin_count);
    let byte_share = remaining_bytes / origin_count_bytes;
    let byte_remainder = remaining_bytes % origin_count_bytes;

    let quotas = origins
        .into_iter()
        .enumerate()
        .map(|(index, node_id)| {
            let index = u64::try_from(index).ok()?;
            let creator = usize::try_from(index).ok()? == creator_index;
            let event_limit =
                event_share + u32::from(index < u64::from(event_remainder)) + u32::from(creator);
            let byte_limit =
                byte_share + u64::from(index < byte_remainder) + u64::from(creator) * create_bytes;
            Some(RawOriginBudget {
                document_id,
                node_id,
                event_limit,
                byte_limit,
                events: u32::from(creator),
                encoded_bytes: if creator { create_bytes } else { 0 },
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(quotas)
}

#[derive(Deserialize)]
struct MetadataRawContext<'a> {
    #[serde(borrow, rename = "@context")]
    context: &'a serde_json::value::RawValue,
}

/// Digest of a crate's `@context`, binding a revision to the terms it was
/// written against.
pub fn raw_context_digest(jsonld: &str) -> Result<[u8; 32], MetadataError> {
    let raw_context: MetadataRawContext = serde_json::from_str(jsonld)
        .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
    Ok(*blake3::hash(raw_context.context.get().as_bytes()).as_bytes())
}

/// The crate text an event installs as the raw base, if it is a base event. A batch event answers with
/// its authored crate: the replay only serves a document until its first merge renders the graph.
pub fn raw_base_jsonld(payload: &MetadataEventPayload) -> Option<&str> {
    match payload {
        MetadataEventPayload::RoCrate { jsonld }
        | MetadataEventPayload::ReplaceRoCrate { jsonld }
        | MetadataEventPayload::ApplyBatch {
            authored: MetadataBatchSource::ReplaceRoCrate { jsonld },
            ..
        } => Some(jsonld),
        _ => None,
    }
}

/// The entity an event upserts onto the raw base, and whether the root links it.
pub fn raw_upsert_entity(payload: &MetadataEventPayload) -> Option<(&str, bool)> {
    match payload {
        MetadataEventPayload::UpsertDataEntity { jsonld }
        | MetadataEventPayload::ApplyBatch {
            authored: MetadataBatchSource::UpsertDataEntity { jsonld },
            ..
        } => Some((jsonld, true)),
        MetadataEventPayload::UpsertContextualEntity { jsonld }
        | MetadataEventPayload::ApplyBatch {
            authored: MetadataBatchSource::UpsertContextualEntity { jsonld },
            ..
        } => Some((jsonld, false)),
        _ => None,
    }
}

pub fn resolve_raw_revision(
    events: &[MetadataEventRecord],
) -> Result<Option<MetadataRawRevision>, MetadataError> {
    let Some(base) = events
        .iter()
        .filter(|event| raw_base_jsonld(&event.payload).is_some())
        .max_by_key(|event| (event.record.updated_at_ms, event.event_id))
    else {
        return Ok(None);
    };
    let Some(jsonld) = raw_base_jsonld(&base.payload) else {
        return Ok(None);
    };
    let context_digest = raw_context_digest(jsonld)?;
    let mut document: serde_json::Value = serde_json::from_str(jsonld)
        .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
    let mut updates = events
        .iter()
        .filter(|event| {
            event.event_id > base.event_id && raw_upsert_entity(&event.payload).is_some()
        })
        .collect::<Vec<_>>();
    updates.sort_by_key(|event| event.event_id);
    let mut winning_event_id = base.event_id;
    for event in updates {
        let Some((jsonld, link_root)) = raw_upsert_entity(&event.payload) else {
            continue;
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
        merged: None,
    }))
}

pub fn apply_raw_upsert(
    document: &mut serde_json::Value,
    jsonld: &str,
    link_root: bool,
) -> Result<(), MetadataError> {
    let mut entity: serde_json::Value = serde_json::from_str(jsonld)
        .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
    let terms = RawTerms::new(document);
    let entity_id = raw_entity_id(&entity, &terms)
        .ok_or_else(|| MetadataError::InvalidInput("entity @id is missing".to_string()))?;
    if let Some(value) = entity.as_object_mut().and_then(|entity| {
        entity
            .iter_mut()
            .find_map(|(key, value)| terms.is_id(key).then_some(value))
    }) {
        *value = serde_json::Value::String(entity_id.clone());
    }
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
pub enum MetadataLifecycleRecord {
    Upsert { event: Box<MetadataEventRecord> },
    Delete { event: MetadataDeleteRecord },
}

impl MetadataLifecycleRecord {
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
pub struct MetadataDeleteRecord {
    pub event_id: Ulid,
    pub tombstone: GraphLifecycleRecord,
    #[serde(rename = "deleted_after_event_id")]
    pub deleted_after_id: Ulid,
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
pub enum MaterializationState {
    Pending,
    Materialized,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializationStatusRecord {
    pub document_id: Ulid,
    pub event_id: Ulid,
    pub graph_iri: String,
    pub context_digest: Option<[u8; 32]>,
    pub dataset_digest: Option<[u8; 32]>,
    pub state: MaterializationState,
    pub attempts: u32,
    /// Application-level failures only; infrastructure errors retry without
    /// counting so an overloaded node never gives up on a document.
    pub failures: u32,
    pub last_error: Option<String>,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IriIndexRecord {
    pub document_id: Ulid,
    pub document_cursor: Ulid,
    pub predicate_iri: String,
    pub object_iri: String,
    pub subject_iris: Vec<String>,
}

impl MaterializationStatusRecord {
    pub fn pending(event: &MetadataEventRecord, updated_at_ms: u64) -> Self {
        Self {
            document_id: event.record.document_id,
            event_id: event.event_id,
            graph_iri: event.record.graph_iri.clone(),
            context_digest: None,
            dataset_digest: None,
            state: MaterializationState::Pending,
            attempts: 0,
            failures: 0,
            last_error: None,
            updated_at_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataMaterializationRecord {
    pub document_id: Ulid,
    pub event_id: Ulid,
    pub due_at_ms: u64,
    /// Total tries; drives retry backoff and stale-duplicate detection.
    pub attempts: u32,
    /// Application-level failures only; the attempt cap tests this counter so a
    /// storm of timeouts cannot park a job.
    pub failures: u32,
    /// How often this job was already parked. Carried across requeue so the
    /// dead-letter backoff of a poison document keeps growing.
    pub parks: u32,
}

/// A job that exhausted its failure budget. Kept so the queue drain can pick it
/// up again later: parking must never silently drop a document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetterRecord {
    pub job: MetadataMaterializationRecord,
    pub last_error: String,
    pub parked_at_ms: u64,
    /// How often this job has been parked; drives the requeue backoff.
    pub parks: u32,
    pub requeue_at_ms: u64,
}

impl MetadataMaterializationRecord {
    pub fn new(event: &MetadataEventRecord, due_at_ms: u64) -> Self {
        Self {
            document_id: event.record.document_id,
            event_id: event.event_id,
            due_at_ms,
            attempts: 0,
            failures: 0,
            parks: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphPruneRecord {
    pub graph_iri: String,
    pub due_at_ms: u64,
    pub attempts: u32,
    pub last_error: Option<String>,
}

impl GraphPruneRecord {
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
pub struct ApplyRoCrateRequest {
    pub graph_iri: String,
    pub jsonld: String,
    pub policy: MetadataGraphPolicy,
    #[serde(default)]
    pub durability: MetadataRequestDurability,
    #[serde(default)]
    pub deterministic_actor: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpsertEntityRequest {
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
    /// `rdf:type` IRIs of the matched subject, so a caller can tell a file
    /// entity from the dataset it belongs to. Capped by the answering node.
    pub subject_types: Vec<String>,
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
pub enum GraphLifecycleStatus {
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphLifecycleRecord {
    pub graph_iri: String,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub document_id: Ulid,
    pub status: GraphLifecycleStatus,
    pub updated_at_ms: u64,
}

impl GraphLifecycleRecord {
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
            status: GraphLifecycleStatus::Deleted,
            updated_at_ms,
        }
    }

    pub fn is_deleted(&self) -> bool {
        matches!(self.status, GraphLifecycleStatus::Deleted)
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
        request: MetadataCrateRequest,
    },
    ValidateRoCrate {
        request: ApplyRoCrateRequest,
    },
    CreateCrate {
        request: MetadataCrateRequest,
    },
    ApplyRoCrate {
        request: ApplyRoCrateRequest,
    },
    UpsertDataEntity {
        request: UpsertEntityRequest,
    },
    UpsertContextualEntity {
        request: UpsertEntityRequest,
    },
    SetGraphPolicy {
        graph_iri: String,
        policy: MetadataGraphPolicy,
    },
    AddGraphPeer {
        graph_iri: String,
        node_id: NodeId,
    },
    SyncBestEffort {
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
    // Device replicas
    GraphSnapshot {
        graph_iri: String,
    },
    InstallSnapshot {
        graph_iri: String,
        snapshot: Box<GraphReplicaSnapshot>,
    },
    // OR-Set metadata graphs
    /// Change set `source` would commit against the local graph, published as a
    /// batch under `actor`. Plans only: the graph is not mutated.
    /// Plans `source` as dot `(actor, counter)`; a counter above one depends on the actor's
    /// previous dot, so every replica applies one actor's batches in order.
    PlanBatch {
        graph_iri: String,
        actor: [u8; 32],
        counter: u64,
        source: MetadataBatchSource,
    },
    MergeBatch {
        graph_iri: String,
        batch: MetadataBatch,
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
    // Device replicas
    GraphSnapshotResult {
        graph_iri: String,
        snapshot: Box<GraphReplicaSnapshot>,
    },
    SnapshotInstalled {
        graph_iri: String,
        applied: bool,
    },
    // OR-Set metadata graphs
    BatchPlanned {
        graph_iri: String,
        batch: MetadataBatch,
    },
    BatchMerged {
        graph_iri: String,
        applied: bool,
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
    #[error("metadata profile validation failed: {0:?}")]
    ProfileValidation(Vec<ProfileValidationFinding>),
    #[error("metadata graph not found")]
    GraphNotFound,
    /// Durability failure while persisting backend state. Infrastructure, not the
    /// document: retrying it is always valid.
    #[error("metadata persist failed: {0}")]
    Persist(String),
    /// Storage adapter failure, kept typed so callers can tell an overloaded
    /// node from a payload the backend will never accept.
    #[error("metadata storage error: {0}")]
    Storage(#[from] StorageError),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileValidationSeverity {
    Violation,
    Warning,
    Info,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileValidationCompleteness {
    Complete,
    Incomplete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileValidationFinding {
    pub code: String,
    pub severity: ProfileValidationSeverity,
    pub focus_node: Option<String>,
    pub path: Option<String>,
    pub rule: String,
    pub message: String,
    /// Registry event id of the evaluated Profile revision, or `builtin`.
    pub profile_revision: Option<String>,
    pub completeness: ProfileValidationCompleteness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileValidationState {
    NotProfiled,
    Valid,
    Invalid,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileValidationStatus {
    pub document_id: Ulid,
    /// The last event merged into the validated render; display only.
    pub dataset_revision: Ulid,
    pub state: ProfileValidationState,
    /// Absent for a built-in Profile, which no registry row defines.
    pub profile_id: Option<Ulid>,
    pub profile_iri: Option<String>,
    pub profile_revision: Option<String>,
    pub evaluator: String,
    pub validated_at_ms: Option<u64>,
    pub findings: Vec<ProfileValidationFinding>,
    pub completeness: ProfileValidationCompleteness,
    pub stale_reason: Option<String>,
    /// Digest of the validated render. Freshness is keyed by this, because a
    /// merge can leave the displayed revision behind the newest event.
    pub dataset_digest: Option<[u8; 32]>,
}

#[cfg(test)]
mod tests {
    use super::{
        EVENT_LIMIT, GraphLifecycleRecord, MetadataBearerToken, MetadataClockRelation,
        MetadataDeleteRecord, MetadataEventPayload, MetadataEventRecord, MetadataLifecycleRecord,
        MetadataQueryResults, ProfileValidationCompleteness, ProfileValidationSeverity,
        ProfileValidationState, ProfileValidationStatus, RAW_BYTES_LIMIT, apply_raw_upsert,
        compare_metadata_clocks, raw_quotas, resolve_raw_revision,
    };
    use crate::structs::identity::realm::RealmId;
    use crate::structs::placement::record::PlacementRef;
    use crate::structs::storage::metadata_registry::MetadataRegistryRecord;
    use crate::{NodeId, UserId};
    use craqle::{ActorId, VectorClock};
    use std::collections::BTreeMap;
    use ulid::Ulid;

    #[test]
    fn compares_metadata_clocks() {
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
    fn metadata_query_variants() {
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

    fn create_event(document_id: Ulid, event_id: Ulid) -> MetadataEventRecord {
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
            establishing_event_id: event_id,
            last_event_id: event_id,
        };
        MetadataEventRecord {
            event_id,
            record,
            user_id: UserId::local(Ulid::generate(), realm_id),
            node_id: node(1),
            payload: MetadataEventPayload::Scaffold {
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
        payload: MetadataEventPayload,
    ) -> MetadataEventRecord {
        let mut event = create_event(document_id, event_id);
        event.record.updated_at_ms = updated_at_ms;
        event.record.last_event_id = event_id;
        event.occurred_at_ms = updated_at_ms;
        event.payload = payload;
        event
    }

    #[test]
    fn raw_quotas_sum() {
        let document_id = Ulid::generate();
        let create = create_event(document_id, Ulid::from_parts(1, 1));
        let create_bytes = u64::try_from(postcard::to_allocvec(&create).unwrap().len()).unwrap();
        let budgets = raw_quotas(
            document_id,
            &[node(2), node(1), node(2)],
            node(2),
            create_bytes,
        )
        .unwrap();

        let mut expected = [node(1), node(2)];
        expected.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        assert_eq!(budgets.len(), 2);
        assert_eq!(budgets[0].node_id, expected[0]);
        assert_eq!(budgets[1].node_id, expected[1]);
        assert_eq!(
            budgets.iter().map(|budget| budget.event_limit).sum::<u32>(),
            EVENT_LIMIT
        );
        assert_eq!(
            budgets.iter().map(|budget| budget.byte_limit).sum::<u64>(),
            RAW_BYTES_LIMIT
        );
        let creator = budgets
            .iter()
            .find(|budget| budget.node_id == node(2))
            .unwrap();
        assert_eq!(creator.events, 1);
        assert_eq!(creator.encoded_bytes, create_bytes);
        assert!(
            budgets
                .iter()
                .all(|budget| budget.events <= budget.event_limit)
        );
    }

    #[test]
    fn raw_quotas_reject() {
        let document_id = Ulid::generate();
        assert!(raw_quotas(document_id, &[], node(1), 0).is_none());
        assert!(raw_quotas(document_id, &[node(1)], node(2), 0).is_none());
        assert!(raw_quotas(document_id, &[node(1)], node(1), RAW_BYTES_LIMIT + 1,).is_none());
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
                MetadataEventPayload::RoCrate {
                    jsonld: base.to_string(),
                },
            ),
            raw_event(
                document_id,
                context_id,
                3,
                MetadataEventPayload::UpsertContextualEntity {
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
                MetadataEventPayload::UpsertDataEntity {
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
                MetadataEventPayload::RoCrate {
                    jsonld: document("winner"),
                },
            ),
            raw_event(
                document_id,
                later_id,
                9,
                MetadataEventPayload::ReplaceRoCrate {
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
            MetadataEventPayload::RoCrate {
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

        assert_eq!(document["items"][2]["node"], "./data/a.txt");
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
    fn metadata_document_event() {
        let document_id = Ulid::generate();
        let event_id = Ulid::generate();
        let create = create_event(document_id, event_id);

        let lifecycle = MetadataLifecycleRecord::Upsert {
            event: Box::new(create.clone()),
        };

        assert_eq!(lifecycle.document_id(), document_id);
        assert_eq!(lifecycle.event_id(), event_id);
        assert_eq!(
            postcard::from_bytes::<MetadataLifecycleRecord>(
                &postcard::to_allocvec(&lifecycle).expect("lifecycle serializes")
            )
            .expect("lifecycle decodes"),
            lifecycle
        );
    }

    #[test]
    fn metadata_document_fence() {
        let document_id = Ulid::generate();
        let event_id = Ulid::generate();
        let deleted_after_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let group_id = Ulid::generate();
        let graph_iri = MetadataRegistryRecord::graph_iri_for(document_id);
        let tombstone =
            GraphLifecycleRecord::deleted(graph_iri.clone(), realm_id, group_id, document_id, 2);

        let lifecycle = MetadataLifecycleRecord::Delete {
            event: MetadataDeleteRecord {
                event_id,
                tombstone: tombstone.clone(),
                deleted_after_id,
            },
        };

        assert_eq!(lifecycle.document_id(), document_id);
        assert_eq!(lifecycle.event_id(), event_id);
        let MetadataLifecycleRecord::Delete { event } = lifecycle else {
            panic!("expected delete lifecycle record");
        };
        assert_eq!(event.tombstone, tombstone);
        assert_eq!(event.deleted_after_id, deleted_after_id);
    }

    #[test]
    fn debug_hides_token() {
        // A peer reply logged with `?` must never print the credential.
        let token = MetadataBearerToken::new("super-secret-value").unwrap();
        let rendered = format!("{token:?}");
        assert!(!rendered.contains("super-secret-value"));
        assert!(format!("{:?}", Some(token)).contains("redacted"));
    }

    #[test]
    fn legacy_revision_decodes() {
        // A Ulid postcard-encodes as its 26 character text, so rows written
        // while the revision was a Ulid decode into the current String field.
        #[derive(serde::Serialize)]
        struct LegacyFinding {
            code: String,
            severity: ProfileValidationSeverity,
            focus_node: Option<String>,
            path: Option<String>,
            rule: String,
            message: String,
            profile_revision: Option<Ulid>,
            completeness: ProfileValidationCompleteness,
        }

        #[derive(serde::Serialize)]
        struct LegacyStatus {
            document_id: Ulid,
            dataset_revision: Ulid,
            state: ProfileValidationState,
            profile_id: Option<Ulid>,
            profile_iri: Option<String>,
            profile_revision: Option<Ulid>,
            evaluator: String,
            validated_at_ms: Option<u64>,
            findings: Vec<LegacyFinding>,
            completeness: ProfileValidationCompleteness,
            stale_reason: Option<String>,
            dataset_digest: Option<[u8; 32]>,
        }

        let revision = Ulid::generate();
        let legacy = LegacyStatus {
            document_id: Ulid::generate(),
            dataset_revision: Ulid::generate(),
            state: ProfileValidationState::Invalid,
            profile_id: Some(Ulid::generate()),
            profile_iri: Some("https://example.org/profile".to_string()),
            profile_revision: Some(revision),
            evaluator: "shacl".to_string(),
            validated_at_ms: Some(7),
            findings: vec![LegacyFinding {
                code: "missing".to_string(),
                severity: ProfileValidationSeverity::Violation,
                focus_node: None,
                path: None,
                rule: "rule".to_string(),
                message: "message".to_string(),
                profile_revision: Some(revision),
                completeness: ProfileValidationCompleteness::Complete,
            }],
            completeness: ProfileValidationCompleteness::Complete,
            stale_reason: None,
            dataset_digest: Some([3u8; 32]),
        };

        let bytes = postcard::to_allocvec(&legacy).unwrap();
        let decoded: ProfileValidationStatus = postcard::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.profile_revision, Some(revision.to_string()));
        assert_eq!(
            decoded.findings[0].profile_revision,
            Some(revision.to_string())
        );
    }
}

#[cfg(test)]
mod actor_tests {
    use super::*;

    #[test]
    fn actors_stay_distinct() {
        let node = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let document = Ulid::from(1);
        let first = MetadataActor::next(None, document, node, Ulid::from(10)).expect("first");
        assert_eq!(first.counter, 1);
        let second =
            MetadataActor::next(Some(&first), document, node, Ulid::from(11)).expect("next");
        assert_eq!((second.actor, second.counter), (first.actor, 2));
        // A node that lost its record must not reuse the old actor's dots.
        let reset = MetadataActor::next(None, document, node, Ulid::from(12)).expect("reset");
        assert_ne!(reset.actor, first.actor);
        let other = MetadataActor::next(None, Ulid::from(2), node, Ulid::from(10)).expect("other");
        assert_ne!(other.actor, first.actor);
    }
}
