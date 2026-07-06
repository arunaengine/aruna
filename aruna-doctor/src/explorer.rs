use crate::error::CliError;
use aruna::config::PersistedNodeState;
use aruna_api::server_state::INITIAL_REALM_ADMIN_CLAIMED_KEY;
use aruna_core::auth::{TOKEN_REVOCATION_LIST_KEY, TRUSTED_REALMS_LIST_KEY};
use aruna_core::document::{DocumentSyncTarget, PendingDocumentPlacement};
use aruna_core::id::DhtKeyId;
use aruna_core::keyspaces::{
    ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE, API_STATE_KEYSPACE,
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    BUCKET_STATS_DB, CRAQLE_GRAPHS_KEYSPACE, CRAQLE_LOG_KEYSPACE, CRAQLE_QUADS_KEYSPACE,
    CRAQLE_TERMS_KEYSPACE, DHT_KEYSPACE, DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE, GROUP_KEYSPACE,
    HASH_PATHS_INDEX_KEYSPACE, METADATA_AUDIT_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE,
    METADATA_HOLDERS_KEYSPACE, METADATA_INDEX_KEYSPACE, NODE_STATE_KEYSPACE, ONBOARDING_KEYSPACE,
    REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, S3_BUCKET_REPLICATION_KEYSPACE,
    S3_MULTIPART_OBJECT_METADATA_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE,
    S3_MULTIPART_UPLOAD_PART_KEYSPACE, SOURCE_CONNECTOR_INDEX_KEYSPACE,
    SOURCE_CONNECTOR_SECRET_KEYSPACE, SYNC_PLACEMENT_KEYSPACE, USER_ACCESS_KEYSPACE,
};
use aruna_core::onboarding::OnboardingSecretRecord;
use aruna_core::structs::{
    BlobHeadKey, BlobVersion, BucketInfo, BucketReplicationConfig, CurrentVersionPointer, Group,
    GroupAuthorizationDocument, HashPathIndexKey, MultipartObjectMetadataKey, MultipartObjectPart,
    MultipartObjectSummary, MultipartUpload, MultipartUploadPart, MultipartUploadPartKey,
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, UserAccess, VersionKey,
};
use aruna_net::dht::storage::StoredEntry;
use chrono::{DateTime, Utc};
use craqle::{
    ActorId as CraqleActorId, Dot as CraqleDot, GraphPolicy as CraqleGraphPolicy,
    VectorClock as CraqleVectorClock,
};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use ulid::Ulid;

const CRAQLE_DOT_ENCODING_TAG: u8 = b'D';
const CRAQLE_BATCH_LOG_ENCODING_TAG: u8 = b'B';
const CRAQLE_GRAPH_META_PREFIX: u8 = b'M';
const CRAQLE_GRAPH_DIRTY_PREFIX: u8 = b'D';
const CRAQLE_GRAPH_REINDEX_PREFIX: u8 = b'R';
const CRAQLE_LOG_HEAD_PREFIX: u8 = b'H';
const CRAQLE_LOG_BATCH_PREFIX: u8 = b'B';

#[derive(Debug, thiserror::Error)]
pub enum ExplorerError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Fjall(#[from] fjall::Error),
    #[error("keyspace not found: {0}")]
    KeyspaceNotFound(String),
    #[error("decode failed: {0}")]
    Decode(String),
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct KeyspacesOutput {
    database_path: String,
    keyspaces: Vec<KeyspaceEntry>,
    missing_keyspaces: Vec<KeyspaceEntry>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct KeyspaceEntry {
    name: String,
}

#[derive(Debug, Serialize, PartialEq)]
struct EntriesOutput {
    database_path: String,
    keyspace: String,
    entries: Vec<EntryOutput>,
}

#[derive(Debug, Serialize, PartialEq)]
struct TopicsListOutput {
    database_path: String,
    topics: Vec<TopicListEntry>,
}

#[derive(Debug, Serialize, PartialEq)]
struct TopicListEntry {
    topic_id: String,
    target: JsonDocumentSyncTarget,
    status: &'static str,
    desired_peer_count: usize,
    selected_peer_count: usize,
    missing_peer_count: usize,
}

#[derive(Debug, Serialize, PartialEq)]
struct TopicStatusOutput {
    database_path: String,
    topic_id: String,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_placement: Option<JsonPendingDocumentPlacement>,
}

#[derive(Debug, Serialize, PartialEq)]
struct TopicPlacementsOutput {
    database_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_id: Option<String>,
    placements: Vec<JsonPendingDocumentPlacement>,
}

#[derive(Debug, Serialize, PartialEq)]
struct EntryOutput {
    key: DecodedField,
    value: DecodedValue,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "format")]
enum DecodedField {
    #[serde(rename = "ulid")]
    Ulid { value: String },
    #[serde(rename = "realm_id")]
    RealmId { value: String },
    #[serde(rename = "dht_key")]
    DhtKeyId { value: String },
    #[serde(rename = "craqle_term_id")]
    CraqleTermId { value: String },
    #[serde(rename = "craqle_quad_key")]
    CraqleQuadKey { value: JsonCraqleQuadKey },
    #[serde(rename = "craqle_graph_key")]
    CraqleGraphKey { value: JsonCraqleGraphKey },
    #[serde(rename = "craqle_log_key")]
    CraqleLogKey { value: JsonCraqleLogKey },
    #[serde(rename = "utf8")]
    Utf8 { value: String },
    #[serde(rename = "blob_head_key")]
    BlobHeadKey { value: BlobHeadKey },
    #[serde(rename = "hash_path_index_key")]
    HashPathIndexKey { value: HashPathIndexKey },
    #[serde(rename = "version_key")]
    VersionKey { value: VersionKey },
    #[serde(rename = "multipart_upload_part_key")]
    MultipartUploadPartKey { value: MultipartUploadPartKey },
    #[serde(rename = "multipart_object_metadata_key")]
    MultipartObjectMetadataKey { value: MultipartObjectMetadataKey },
    #[serde(rename = "raw")]
    Raw { hex: String },
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(tag = "type")]
#[allow(clippy::large_enum_variant)]
enum DecodedValue {
    Group {
        data: JsonGroup,
    },
    GroupAuthorizationDocument {
        data: GroupAuthorizationDocument,
    },
    RealmAuthorizationDocument {
        data: JsonRealmAuthorizationDocument,
    },
    RealmConfigDocument {
        data: JsonRealmConfigDocument,
    },
    UserAccess {
        data: JsonUserAccess,
    },
    BucketInfo {
        data: BucketInfo,
    },
    BucketReplicationConfig {
        data: BucketReplicationConfig,
    },
    CurrentVersionPointer {
        data: CurrentVersionPointer,
    },
    BackendLocation {
        data: aruna_core::structs::BackendLocation,
    },
    BlobVersion {
        data: BlobVersion,
    },
    MultipartUpload {
        data: MultipartUpload,
    },
    MultipartUploadPart {
        data: MultipartUploadPart,
    },
    MultipartObjectSummary {
        data: MultipartObjectSummary,
    },
    MultipartObjectPart {
        data: MultipartObjectPart,
    },
    ApiTokenRevocationList {
        data: HashSet<String>,
    },
    ApiTrustedRealmsList {
        data: Vec<String>,
    },
    ApiInitialRealmAdminClaimed {
        data: bool,
    },
    NodeState {
        data: JsonPersistedNodeState,
    },
    PendingDocumentPlacement {
        data: JsonPendingDocumentPlacement,
    },
    OnboardingSecretRecord {
        data: OnboardingSecretRecord,
    },
    DhtEntries {
        data: Vec<JsonStoredEntry>,
    },
    CraqleTerm {
        data: String,
    },
    CraqleQuadDots {
        data: Vec<JsonCraqleDot>,
    },
    CraqleGraphMeta {
        data: JsonCraqleGraphMeta,
    },
    CraqleGraphDirtyToken {
        data: u64,
    },
    CraqleGraphReindexToken {
        data: u64,
    },
    CraqleLogHead {
        data: u64,
    },
    CraqleLogBatch {
        data: JsonCraqleStoredBatch,
    },
    Raw {
        hex: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        decode_error: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
struct CraqleTermId(u128);

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct CraqleStoredGraphMeta {
    policy: CraqleGraphPolicy,
    clock: CraqleVectorClock,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
enum CraqleStoredQuadOp {
    Add {
        subject: CraqleTermId,
        predicate: CraqleTermId,
        object: CraqleTermId,
        dot: CraqleDot,
    },
    Remove {
        subject: CraqleTermId,
        predicate: CraqleTermId,
        object: CraqleTermId,
        witnessed: CraqleVectorClock,
    },
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct CraqleStoredBatch {
    actor: CraqleActorId,
    counter: u64,
    base_clock: CraqleVectorClock,
    ops: Vec<CraqleStoredQuadOp>,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CraqleQuadKeyParts {
    graph: CraqleTermId,
    subject: CraqleTermId,
    predicate: CraqleTermId,
    object: CraqleTermId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CraqleGraphKeyParts {
    Meta {
        graph: CraqleTermId,
    },
    Dirty {
        graph: CraqleTermId,
        subject: CraqleTermId,
    },
    Reindex {
        graph: CraqleTermId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CraqleLogKeyParts {
    Head {
        graph: CraqleTermId,
        actor: CraqleActorId,
    },
    Batch {
        graph: CraqleTermId,
        actor: CraqleActorId,
        counter: u64,
    },
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct JsonCraqleQuadKey {
    graph: String,
    subject: String,
    predicate: String,
    object: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "kind")]
enum JsonCraqleGraphKey {
    Meta { graph: String },
    Dirty { graph: String, subject: String },
    Reindex { graph: String },
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "kind")]
enum JsonCraqleLogKey {
    Head {
        graph: String,
        actor: String,
    },
    Batch {
        graph: String,
        actor: String,
        counter: u64,
    },
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct JsonCraqleClockEntry {
    actor: String,
    counter: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct JsonCraqleVectorClock {
    entries: Vec<JsonCraqleClockEntry>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct JsonCraqleDot {
    actor: String,
    counter: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct JsonCraqleGraphPolicy {
    public: bool,
    permission_paths: Vec<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct JsonCraqleGraphMeta {
    graph: String,
    policy: JsonCraqleGraphPolicy,
    clock: JsonCraqleVectorClock,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "kind")]
enum JsonCraqleStoredBatchOp {
    Add {
        subject: String,
        predicate: String,
        object: String,
        dot: JsonCraqleDot,
    },
    Remove {
        subject: String,
        predicate: String,
        object: String,
        witnessed: JsonCraqleVectorClock,
    },
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct JsonCraqleStoredBatch {
    graph: String,
    actor: String,
    counter: u64,
    base_clock: JsonCraqleVectorClock,
    ops: Vec<JsonCraqleStoredBatchOp>,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, PartialEq, Eq)]
struct JsonGroup(Group);

impl Serialize for JsonGroup {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Group", 4)?;
        state.serialize_field("display_name", &self.0.display_name)?;
        state.serialize_field("group_id", &self.0.group_id.to_string())?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("roles", &self.0.roles)?;
        state.end()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct JsonRealmAuthorizationDocument(RealmAuthorizationDocument);

impl Serialize for JsonRealmAuthorizationDocument {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RealmAuthorizationDocument", 3)?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("roles", &self.0.roles)?;
        state.serialize_field("operation_restrictions", &self.0.operation_restrictions)?;
        state.end()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct JsonRealmConfigDocument(RealmConfigDocument);

impl Serialize for JsonRealmConfigDocument {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RealmConfigDocument", 3)?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("description", &self.0.description)?;
        state.serialize_field("metadata_replication", &self.0.metadata_replication)?;
        state.end()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct JsonUserAccess(UserAccess);

impl Serialize for JsonUserAccess {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("UserAccess", 8)?;
        state.serialize_field("access_key", &self.0.access_key)?;
        state.serialize_field("user_identity", &self.0.user_identity)?;
        state.serialize_field("group_id", &self.0.group_id.to_string())?;
        state.serialize_field("secret", &self.0.secret)?;
        state.serialize_field("expiry", &self.0.expiry)?;
        state.serialize_field("path_restrictions", &self.0.path_restrictions)?;
        state.serialize_field("issued_by", &self.0.issued_by)?;
        state.serialize_field("revoked_at", &self.0.revoked_at)?;
        state.end()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct JsonPersistedNodeState(PersistedNodeState);

impl Serialize for JsonPersistedNodeState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PersistedNodeState", 6)?;
        state.serialize_field("boot_origin", &self.0.boot_origin)?;
        state.serialize_field("status", &self.0.status)?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("net_secret_key", &hex::encode(self.0.net_secret_key))?;
        state.serialize_field("onboarding_phase", &self.0.onboarding_phase)?;
        state.serialize_field("onboarding_sync_ticket", &self.0.onboarding_sync_ticket)?;
        state.serialize_field("identity", &self.0.identity)?;
        state.end()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct JsonPendingDocumentPlacement(PendingDocumentPlacement);

impl Serialize for JsonPendingDocumentPlacement {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PendingDocumentPlacement", 8)?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("target", &json_document_sync_target(&self.0.target))?;
        state.serialize_field("topic_id", &placement_topic_id(&self.0))?;
        state.serialize_field(
            "authoritative_node_id",
            &self.0.authoritative_node_id.to_string(),
        )?;
        state.serialize_field("desired_peer_count", &self.0.desired_peer_count)?;
        state.serialize_field(
            "selected_peers",
            &self
                .0
                .selected_peers
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>(),
        )?;
        state.serialize_field("missing_peer_count", &placement_missing_peer_count(&self.0))?;
        state.serialize_field("updated_at", &self.0.updated_at)?;
        state.end()
    }
}

fn placement_topic_id(placement: &PendingDocumentPlacement) -> String {
    placement.target.sync_topic_id().to_string()
}

fn placement_missing_peer_count(placement: &PendingDocumentPlacement) -> usize {
    let mut selected_peers = placement.selected_peers.clone();
    selected_peers.retain(|node_id| *node_id != placement.authoritative_node_id);
    selected_peers.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    selected_peers.dedup();
    placement
        .desired_peer_count
        .saturating_sub(selected_peers.len().saturating_add(1))
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "kind")]
enum JsonDocumentSyncTarget {
    Group {
        group_id: String,
    },
    GroupAuthorization {
        group_id: String,
    },
    RealmAuthorization {
        realm_id: String,
    },
    RealmConfig {
        realm_id: String,
    },
    User {
        user_id: String,
    },
    MetadataRegistry {
        group_id: String,
        document_id: String,
    },
    MetadataCreateEvent {
        document_id: String,
        event_id: String,
    },
    MetadataDocumentLifecycle {
        document_id: String,
    },
    MetadataGraphLifecycle {
        graph_iri: String,
    },
    NodeUsage {
        realm_id: String,
        node_id: String,
        group_id: Option<String>,
    },
    WatchInterest {
        realm_id: String,
        node_id: String,
    },
    NodeInfo {
        realm_id: String,
        node_id: String,
    },
}

fn json_document_sync_target(target: &DocumentSyncTarget) -> JsonDocumentSyncTarget {
    match target {
        DocumentSyncTarget::Group { group_id } => JsonDocumentSyncTarget::Group {
            group_id: group_id.to_string(),
        },
        DocumentSyncTarget::GroupAuthorization { group_id } => {
            JsonDocumentSyncTarget::GroupAuthorization {
                group_id: group_id.to_string(),
            }
        }
        DocumentSyncTarget::RealmAuthorization { realm_id } => {
            JsonDocumentSyncTarget::RealmAuthorization {
                realm_id: realm_id.to_string(),
            }
        }
        DocumentSyncTarget::RealmConfig { realm_id } => JsonDocumentSyncTarget::RealmConfig {
            realm_id: realm_id.to_string(),
        },
        DocumentSyncTarget::User { user_id } => JsonDocumentSyncTarget::User {
            user_id: user_id.to_string(),
        },
        DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        } => JsonDocumentSyncTarget::MetadataRegistry {
            group_id: group_id.to_string(),
            document_id: document_id.to_string(),
        },
        DocumentSyncTarget::MetadataCreateEvent {
            document_id,
            event_id,
        } => JsonDocumentSyncTarget::MetadataCreateEvent {
            document_id: document_id.to_string(),
            event_id: event_id.to_string(),
        },
        DocumentSyncTarget::MetadataDocumentLifecycle { document_id } => {
            JsonDocumentSyncTarget::MetadataDocumentLifecycle {
                document_id: document_id.to_string(),
            }
        }
        DocumentSyncTarget::MetadataGraphLifecycle { graph_iri } => {
            JsonDocumentSyncTarget::MetadataGraphLifecycle {
                graph_iri: graph_iri.clone(),
            }
        }
        DocumentSyncTarget::NodeUsage {
            realm_id,
            node_id,
            group_id,
        } => JsonDocumentSyncTarget::NodeUsage {
            realm_id: realm_id.to_string(),
            node_id: node_id.to_string(),
            group_id: group_id.map(|group_id| group_id.to_string()),
        },
        DocumentSyncTarget::WatchInterest { realm_id, node_id } => {
            JsonDocumentSyncTarget::WatchInterest {
                realm_id: realm_id.to_string(),
                node_id: node_id.to_string(),
            }
        }
        DocumentSyncTarget::NodeInfo { realm_id, node_id } => JsonDocumentSyncTarget::NodeInfo {
            realm_id: realm_id.to_string(),
            node_id: node_id.to_string(),
        },
    }
}

#[derive(Debug)]
struct JsonStoredEntry(StoredEntry);

impl PartialEq for JsonStoredEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.publisher == other.0.publisher
            && self.0.realm_id == other.0.realm_id
            && self.0.value == other.0.value
            && self.0.expires_at == other.0.expires_at
            && self.0.signature == other.0.signature
    }
}

impl Serialize for JsonStoredEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("StoredEntry", 6)?;
        state.serialize_field("publisher", &self.0.publisher.to_string())?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("expires_at", &self.0.expires_at)?;
        state.serialize_field(
            "signature",
            &self
                .0
                .signature
                .as_ref()
                .map(std::string::ToString::to_string),
        )?;
        state.serialize_field("value_len", &self.0.value.len())?;
        state.serialize_field("value_hex", &hex::encode(&self.0.value))?;
        state.end()
    }
}

fn craqle_term_id_string(id: CraqleTermId) -> String {
    format!("{:032x}", id.0)
}

fn decode_craqle_term_id(bytes: &[u8], context: &'static str) -> Result<CraqleTermId, String> {
    let raw: [u8; 16] = bytes.try_into().map_err(|_| {
        format!(
            "invalid {context}: expected 16 bytes, found {}",
            bytes.len()
        )
    })?;
    Ok(CraqleTermId(u128::from_be_bytes(raw)))
}

fn decode_craqle_u64(bytes: &[u8], context: &'static str) -> Result<u64, String> {
    let raw: [u8; 8] = bytes
        .try_into()
        .map_err(|_| format!("invalid {context}: expected 8 bytes, found {}", bytes.len()))?;
    Ok(u64::from_be_bytes(raw))
}

fn json_craqle_quad_key(parts: CraqleQuadKeyParts) -> JsonCraqleQuadKey {
    JsonCraqleQuadKey {
        graph: craqle_term_id_string(parts.graph),
        subject: craqle_term_id_string(parts.subject),
        predicate: craqle_term_id_string(parts.predicate),
        object: craqle_term_id_string(parts.object),
    }
}

fn json_craqle_graph_key(parts: CraqleGraphKeyParts) -> JsonCraqleGraphKey {
    match parts {
        CraqleGraphKeyParts::Meta { graph } => JsonCraqleGraphKey::Meta {
            graph: craqle_term_id_string(graph),
        },
        CraqleGraphKeyParts::Dirty { graph, subject } => JsonCraqleGraphKey::Dirty {
            graph: craqle_term_id_string(graph),
            subject: craqle_term_id_string(subject),
        },
        CraqleGraphKeyParts::Reindex { graph } => JsonCraqleGraphKey::Reindex {
            graph: craqle_term_id_string(graph),
        },
    }
}

fn json_craqle_log_key(parts: CraqleLogKeyParts) -> JsonCraqleLogKey {
    match parts {
        CraqleLogKeyParts::Head { graph, actor } => JsonCraqleLogKey::Head {
            graph: craqle_term_id_string(graph),
            actor: actor.to_string(),
        },
        CraqleLogKeyParts::Batch {
            graph,
            actor,
            counter,
        } => JsonCraqleLogKey::Batch {
            graph: craqle_term_id_string(graph),
            actor: actor.to_string(),
            counter,
        },
    }
}

fn json_craqle_dot(dot: CraqleDot) -> JsonCraqleDot {
    JsonCraqleDot {
        actor: dot.actor.to_string(),
        counter: dot.counter,
    }
}

fn json_craqle_vector_clock(clock: CraqleVectorClock) -> JsonCraqleVectorClock {
    JsonCraqleVectorClock {
        entries: clock
            .0
            .into_iter()
            .map(|(actor, counter)| JsonCraqleClockEntry {
                actor: actor.to_string(),
                counter,
            })
            .collect(),
    }
}

fn json_craqle_graph_policy(policy: CraqleGraphPolicy) -> JsonCraqleGraphPolicy {
    let mut permission_paths = policy.permission_paths;
    permission_paths.sort();
    permission_paths.dedup();
    JsonCraqleGraphPolicy {
        public: policy.public,
        permission_paths,
    }
}

fn json_craqle_graph_meta(graph: CraqleTermId, meta: CraqleStoredGraphMeta) -> JsonCraqleGraphMeta {
    JsonCraqleGraphMeta {
        graph: craqle_term_id_string(graph),
        policy: json_craqle_graph_policy(meta.policy),
        clock: json_craqle_vector_clock(meta.clock),
    }
}

fn json_craqle_stored_batch(
    graph: CraqleTermId,
    batch: CraqleStoredBatch,
) -> JsonCraqleStoredBatch {
    JsonCraqleStoredBatch {
        graph: craqle_term_id_string(graph),
        actor: batch.actor.to_string(),
        counter: batch.counter,
        base_clock: json_craqle_vector_clock(batch.base_clock),
        ops: batch
            .ops
            .into_iter()
            .map(|op| match op {
                CraqleStoredQuadOp::Add {
                    subject,
                    predicate,
                    object,
                    dot,
                } => JsonCraqleStoredBatchOp::Add {
                    subject: craqle_term_id_string(subject),
                    predicate: craqle_term_id_string(predicate),
                    object: craqle_term_id_string(object),
                    dot: json_craqle_dot(dot),
                },
                CraqleStoredQuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed,
                } => JsonCraqleStoredBatchOp::Remove {
                    subject: craqle_term_id_string(subject),
                    predicate: craqle_term_id_string(predicate),
                    object: craqle_term_id_string(object),
                    witnessed: json_craqle_vector_clock(witnessed),
                },
            })
            .collect(),
        timestamp: batch.timestamp,
    }
}

fn decode_craqle_quad_key(key: &[u8]) -> Result<CraqleQuadKeyParts, String> {
    if key.len() != 64 {
        return Err(format!(
            "invalid craqle quad key: expected 64 bytes, found {}",
            key.len()
        ));
    }
    Ok(CraqleQuadKeyParts {
        graph: decode_craqle_term_id(&key[0..16], "craqle quad graph")?,
        subject: decode_craqle_term_id(&key[16..32], "craqle quad subject")?,
        predicate: decode_craqle_term_id(&key[32..48], "craqle quad predicate")?,
        object: decode_craqle_term_id(&key[48..64], "craqle quad object")?,
    })
}

fn decode_craqle_graph_key(key: &[u8]) -> Result<CraqleGraphKeyParts, String> {
    match key.first().copied() {
        Some(CRAQLE_GRAPH_META_PREFIX) if key.len() == 17 => Ok(CraqleGraphKeyParts::Meta {
            graph: decode_craqle_term_id(&key[1..17], "craqle graph meta graph")?,
        }),
        Some(CRAQLE_GRAPH_DIRTY_PREFIX) if key.len() == 33 => Ok(CraqleGraphKeyParts::Dirty {
            graph: decode_craqle_term_id(&key[1..17], "craqle graph dirty graph")?,
            subject: decode_craqle_term_id(&key[17..33], "craqle graph dirty subject")?,
        }),
        Some(CRAQLE_GRAPH_REINDEX_PREFIX) if key.len() == 17 => Ok(CraqleGraphKeyParts::Reindex {
            graph: decode_craqle_term_id(&key[1..17], "craqle graph reindex graph")?,
        }),
        Some(prefix) => Err(format!(
            "invalid craqle graph key prefix `{}` with length {}",
            prefix as char,
            key.len()
        )),
        None => Err("invalid craqle graph key: empty key".to_string()),
    }
}

fn decode_craqle_log_key(key: &[u8]) -> Result<CraqleLogKeyParts, String> {
    match key.first().copied() {
        Some(CRAQLE_LOG_HEAD_PREFIX) if key.len() == 49 => Ok(CraqleLogKeyParts::Head {
            graph: decode_craqle_term_id(&key[1..17], "craqle log head graph")?,
            actor: CraqleActorId::from_bytes(
                key[17..49]
                    .try_into()
                    .map_err(|_| "invalid craqle log head actor".to_string())?,
            ),
        }),
        Some(CRAQLE_LOG_BATCH_PREFIX) if key.len() == 57 => Ok(CraqleLogKeyParts::Batch {
            graph: decode_craqle_term_id(&key[1..17], "craqle log batch graph")?,
            actor: CraqleActorId::from_bytes(
                key[17..49]
                    .try_into()
                    .map_err(|_| "invalid craqle log batch actor".to_string())?,
            ),
            counter: decode_craqle_u64(&key[49..57], "craqle log batch counter")?,
        }),
        Some(prefix) => Err(format!(
            "invalid craqle log key prefix `{}` with length {}",
            prefix as char,
            key.len()
        )),
        None => Err("invalid craqle log key: empty key".to_string()),
    }
}

fn decode_craqle_dots(value: &[u8]) -> Result<Vec<JsonCraqleDot>, String> {
    let dots = if value.first().copied() == Some(CRAQLE_DOT_ENCODING_TAG) {
        if !(value.len() - 1).is_multiple_of(40) {
            return Err(format!("invalid craqle dot payload length {}", value.len()));
        }
        let (chunks, _) = value[1..].as_chunks::<40>();
        chunks
            .iter()
            .map(|chunk| CraqleDot {
                actor: CraqleActorId::from_bytes(chunk[0..32].try_into().unwrap()),
                counter: u64::from_be_bytes(chunk[32..40].try_into().unwrap()),
            })
            .collect()
    } else {
        postcard::from_bytes::<Vec<CraqleDot>>(value).map_err(|error| error.to_string())?
    };
    Ok(dots.into_iter().map(json_craqle_dot).collect())
}

fn decode_craqle_graph_value(key: &[u8], value: &[u8]) -> DecodedValue {
    match decode_craqle_graph_key(key) {
        Ok(CraqleGraphKeyParts::Meta { graph }) => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<CraqleStoredGraphMeta>(bytes),
            |data| DecodedValue::CraqleGraphMeta {
                data: json_craqle_graph_meta(graph, data),
            },
        ),
        Ok(CraqleGraphKeyParts::Dirty { .. }) => decode_value_with(
            value,
            |bytes| decode_craqle_u64(bytes, "craqle graph dirty token"),
            |data| DecodedValue::CraqleGraphDirtyToken { data },
        ),
        Ok(CraqleGraphKeyParts::Reindex { .. }) => decode_value_with(
            value,
            |bytes| decode_craqle_u64(bytes, "craqle graph reindex token"),
            |data| DecodedValue::CraqleGraphReindexToken { data },
        ),
        Err(error) => raw_value(value, Some(error)),
    }
}

fn decode_craqle_log_batch(key: &[u8], value: &[u8]) -> Result<JsonCraqleStoredBatch, String> {
    let CraqleLogKeyParts::Batch { graph, .. } = decode_craqle_log_key(key)? else {
        return Err("craqle log batch value requires a batch key".to_string());
    };
    if value.first().copied() != Some(CRAQLE_BATCH_LOG_ENCODING_TAG) {
        return Err("unsupported craqle log batch encoding".to_string());
    }
    let batch = postcard::from_bytes::<CraqleStoredBatch>(&value[1..])
        .map_err(|error| error.to_string())?;
    Ok(json_craqle_stored_batch(graph, batch))
}

fn decode_craqle_log_value(key: &[u8], value: &[u8]) -> DecodedValue {
    match decode_craqle_log_key(key) {
        Ok(CraqleLogKeyParts::Head { .. }) => decode_value_with(
            value,
            |bytes| decode_craqle_u64(bytes, "craqle log head"),
            |data| DecodedValue::CraqleLogHead { data },
        ),
        Ok(CraqleLogKeyParts::Batch { .. }) => decode_value_with(
            value,
            |bytes| decode_craqle_log_batch(key, bytes),
            |data| DecodedValue::CraqleLogBatch { data },
        ),
        Err(error) => raw_value(value, Some(error)),
    }
}

pub async fn explore_keyspaces(database_path: String) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        move || list_keyspaces(&database_path)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

pub async fn explore_entries(database_path: String, keyspace: String) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        let keyspace = keyspace.clone();
        move || list_entries(&database_path, &keyspace)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

pub async fn print_node_state(database_path: String) -> Result<(), CliError> {
    explore_entries(database_path, NODE_STATE_KEYSPACE.to_string()).await
}

pub async fn print_topics_list(database_path: String) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        move || topics_list_output(&database_path)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

pub async fn print_topic_status(database_path: String, topic_id: String) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        let topic_id = topic_id.clone();
        move || topic_status_output(&database_path, &topic_id)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

pub async fn print_topic_placements(
    database_path: String,
    topic_id: Option<String>,
) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        let topic_id = topic_id.clone();
        move || topic_placements_output(&database_path, topic_id.as_deref())
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn list_keyspaces(database_path: &str) -> Result<KeyspacesOutput, ExplorerError> {
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let mut keyspaces = db.list_keyspace_names();
    keyspaces.sort();
    let existing = keyspaces
        .iter()
        .map(|name| name.as_ref())
        .collect::<HashSet<_>>();
    let mut missing_keyspaces = defined_keyspaces()
        .into_iter()
        .filter(|name| !existing.contains(name))
        .map(|name| KeyspaceEntry {
            name: name.to_string(),
        })
        .collect::<Vec<_>>();
    missing_keyspaces.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(KeyspacesOutput {
        database_path: database_path.to_string(),
        keyspaces: keyspaces
            .into_iter()
            .map(|name| KeyspaceEntry {
                name: name.to_string(),
            })
            .collect(),
        missing_keyspaces,
    })
}

fn defined_keyspaces() -> [&'static str; 32] {
    [
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
        ADMIN_DOCUMENT_STATE_KEYSPACE,
        API_STATE_KEYSPACE,
        AUTH_KEYSPACE,
        BLOB_HEAD_KEYSPACE,
        BLOB_LOCATIONS_KEYSPACE,
        BLOB_VERSIONS_KEYSPACE,
        BUCKET_STATS_DB,
        CRAQLE_GRAPHS_KEYSPACE,
        CRAQLE_LOG_KEYSPACE,
        CRAQLE_QUADS_KEYSPACE,
        CRAQLE_TERMS_KEYSPACE,
        DHT_KEYSPACE,
        GROUP_KEYSPACE,
        HASH_PATHS_INDEX_KEYSPACE,
        DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE,
        METADATA_AUDIT_KEYSPACE,
        METADATA_DOCUMENT_INDEX_KEYSPACE,
        METADATA_HOLDERS_KEYSPACE,
        METADATA_INDEX_KEYSPACE,
        NODE_STATE_KEYSPACE,
        ONBOARDING_KEYSPACE,
        REALM_CONFIG_KEYSPACE,
        S3_BUCKET_KEYSPACE,
        S3_BUCKET_REPLICATION_KEYSPACE,
        S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
        S3_MULTIPART_UPLOAD_KEYSPACE,
        S3_MULTIPART_UPLOAD_PART_KEYSPACE,
        SOURCE_CONNECTOR_INDEX_KEYSPACE,
        SOURCE_CONNECTOR_SECRET_KEYSPACE,
        SYNC_PLACEMENT_KEYSPACE,
        USER_ACCESS_KEYSPACE,
    ]
}

fn list_entries(database_path: &str, keyspace_name: &str) -> Result<EntriesOutput, ExplorerError> {
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let keyspace_names = db.list_keyspace_names();
    if !keyspace_names
        .iter()
        .any(|name| name.as_ref() == keyspace_name)
    {
        return Err(ExplorerError::KeyspaceNotFound(keyspace_name.to_string()));
    }

    let keyspace = db.keyspace(keyspace_name, KeyspaceCreateOptions::default)?;
    let snapshot = db.read_tx();
    let mut entries = Vec::new();

    for entry in snapshot.iter(&keyspace) {
        let (key, value) = entry.into_inner()?;
        entries.push(decode_entry(keyspace_name, key.as_ref(), value.as_ref()));
    }

    Ok(EntriesOutput {
        database_path: database_path.to_string(),
        keyspace: keyspace_name.to_string(),
        entries,
    })
}

fn topics_list_output(database_path: &str) -> Result<TopicsListOutput, ExplorerError> {
    let mut topics = load_pending_placements(database_path)?
        .into_iter()
        .map(|placement| TopicListEntry {
            topic_id: placement_topic_id(&placement),
            target: json_document_sync_target(&placement.target),
            status: "under_replicated",
            desired_peer_count: placement.desired_peer_count,
            selected_peer_count: placement.selected_peers.len(),
            missing_peer_count: placement_missing_peer_count(&placement),
        })
        .collect::<Vec<_>>();
    topics.sort_by(|left, right| left.topic_id.cmp(&right.topic_id));

    Ok(TopicsListOutput {
        database_path: database_path.to_string(),
        topics,
    })
}

fn topic_status_output(
    database_path: &str,
    topic_id: &str,
) -> Result<TopicStatusOutput, ExplorerError> {
    let pending_placement = load_pending_placements(database_path)?
        .into_iter()
        .find(|placement| placement_topic_id(placement) == topic_id)
        .map(JsonPendingDocumentPlacement);
    let status = if pending_placement.is_some() {
        "under_replicated"
    } else {
        "not_pending"
    };

    Ok(TopicStatusOutput {
        database_path: database_path.to_string(),
        topic_id: topic_id.to_string(),
        status,
        pending_placement,
    })
}

fn topic_placements_output(
    database_path: &str,
    topic_id: Option<&str>,
) -> Result<TopicPlacementsOutput, ExplorerError> {
    let mut placements = load_pending_placements(database_path)?;
    if let Some(topic_id) = topic_id {
        placements.retain(|placement| placement_topic_id(placement) == topic_id);
    }
    placements.sort_by_key(placement_topic_id);

    Ok(TopicPlacementsOutput {
        database_path: database_path.to_string(),
        topic_id: topic_id.map(str::to_string),
        placements: placements
            .into_iter()
            .map(JsonPendingDocumentPlacement)
            .collect(),
    })
}

fn load_pending_placements(
    database_path: &str,
) -> Result<Vec<PendingDocumentPlacement>, ExplorerError> {
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let keyspace_names = db.list_keyspace_names();
    if !keyspace_names
        .iter()
        .any(|name| name.as_ref() == SYNC_PLACEMENT_KEYSPACE)
    {
        return Ok(Vec::new());
    }

    let keyspace = db.keyspace(SYNC_PLACEMENT_KEYSPACE, KeyspaceCreateOptions::default)?;
    let snapshot = db.read_tx();
    let mut placements = Vec::new();
    for entry in snapshot.iter(&keyspace) {
        let (_, value) = entry.into_inner()?;
        placements.push(
            aruna_operations::sync_placement::decode_placement(value.as_ref())
                .map_err(|error| ExplorerError::Decode(error.to_string()))?,
        );
    }
    Ok(placements)
}

fn decode_entry(keyspace_name: &str, key: &[u8], value: &[u8]) -> EntryOutput {
    EntryOutput {
        key: decode_key(keyspace_name, key),
        value: decode_value(keyspace_name, key, value),
    }
}

fn decode_key(keyspace_name: &str, key: &[u8]) -> DecodedField {
    match keyspace_name {
        GROUP_KEYSPACE | AUTH_KEYSPACE => decode_ulid_key(key),
        REALM_CONFIG_KEYSPACE => decode_realm_id_key(key),
        CRAQLE_TERMS_KEYSPACE => decode_craqle_term_id(key, "craqle term key")
            .map(|value| DecodedField::CraqleTermId {
                value: craqle_term_id_string(value),
            })
            .unwrap_or_else(|_| raw_field(key)),
        CRAQLE_QUADS_KEYSPACE => decode_craqle_quad_key(key)
            .map(|value| DecodedField::CraqleQuadKey {
                value: json_craqle_quad_key(value),
            })
            .unwrap_or_else(|_| raw_field(key)),
        CRAQLE_GRAPHS_KEYSPACE => decode_craqle_graph_key(key)
            .map(|value| DecodedField::CraqleGraphKey {
                value: json_craqle_graph_key(value),
            })
            .unwrap_or_else(|_| raw_field(key)),
        CRAQLE_LOG_KEYSPACE => decode_craqle_log_key(key)
            .map(|value| DecodedField::CraqleLogKey {
                value: json_craqle_log_key(value),
            })
            .unwrap_or_else(|_| raw_field(key)),
        USER_ACCESS_KEYSPACE
        | S3_BUCKET_KEYSPACE
        | S3_BUCKET_REPLICATION_KEYSPACE
        | API_STATE_KEYSPACE
        | DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE
        | NODE_STATE_KEYSPACE
        | ONBOARDING_KEYSPACE => decode_utf8_key(key),
        SYNC_PLACEMENT_KEYSPACE => raw_field(key),
        S3_MULTIPART_UPLOAD_KEYSPACE => decode_ulid_key(key),
        S3_MULTIPART_UPLOAD_PART_KEYSPACE => MultipartUploadPartKey::from_bytes(key)
            .map(|value| DecodedField::MultipartUploadPartKey { value })
            .unwrap_or_else(|_| raw_field(key)),
        S3_MULTIPART_OBJECT_METADATA_KEYSPACE => MultipartObjectMetadataKey::from_bytes(key)
            .map(|value| DecodedField::MultipartObjectMetadataKey { value })
            .unwrap_or_else(|_| raw_field(key)),
        DHT_KEYSPACE => decode_dht_key(key),
        BLOB_HEAD_KEYSPACE => BlobHeadKey::from_bytes(key)
            .map(|value| DecodedField::BlobHeadKey { value })
            .unwrap_or_else(|_| raw_field(key)),
        HASH_PATHS_INDEX_KEYSPACE => HashPathIndexKey::from_bytes(key)
            .map(|value| DecodedField::HashPathIndexKey { value })
            .unwrap_or_else(|_| raw_field(key)),
        BLOB_VERSIONS_KEYSPACE => VersionKey::from_bytes(key)
            .map(|value| DecodedField::VersionKey { value })
            .unwrap_or_else(|_| raw_field(key)),
        _ => raw_field(key),
    }
}

fn decode_value(keyspace_name: &str, key: &[u8], value: &[u8]) -> DecodedValue {
    match keyspace_name {
        GROUP_KEYSPACE => decode_value_with(value, Group::from_bytes, |data| DecodedValue::Group {
            data: JsonGroup(data),
        }),
        REALM_CONFIG_KEYSPACE => {
            decode_value_with(value, RealmConfigDocument::from_bytes, |data| {
                DecodedValue::RealmConfigDocument {
                    data: JsonRealmConfigDocument(data),
                }
            })
        }
        USER_ACCESS_KEYSPACE => decode_value_with(value, UserAccess::from_bytes, |data| {
            DecodedValue::UserAccess {
                data: JsonUserAccess(data),
            }
        }),
        S3_BUCKET_KEYSPACE => decode_value_with(value, BucketInfo::from_bytes, |data| {
            DecodedValue::BucketInfo { data }
        }),
        S3_BUCKET_REPLICATION_KEYSPACE => {
            decode_value_with(value, BucketReplicationConfig::from_bytes, |data| {
                DecodedValue::BucketReplicationConfig { data }
            })
        }
        BLOB_HEAD_KEYSPACE => decode_value_with(value, CurrentVersionPointer::from_bytes, |data| {
            DecodedValue::CurrentVersionPointer { data }
        }),
        BLOB_LOCATIONS_KEYSPACE => decode_value_with(
            value,
            aruna_core::structs::BackendLocation::from_bytes,
            |data| DecodedValue::BackendLocation { data },
        ),
        BLOB_VERSIONS_KEYSPACE => decode_value_with(value, BlobVersion::from_bytes, |data| {
            DecodedValue::BlobVersion { data }
        }),
        S3_MULTIPART_UPLOAD_KEYSPACE => {
            decode_value_with(value, MultipartUpload::from_bytes, |data| {
                DecodedValue::MultipartUpload { data }
            })
        }
        S3_MULTIPART_UPLOAD_PART_KEYSPACE => {
            decode_value_with(value, MultipartUploadPart::from_bytes, |data| {
                DecodedValue::MultipartUploadPart { data }
            })
        }
        S3_MULTIPART_OBJECT_METADATA_KEYSPACE => decode_multipart_object_metadata_value(key, value),
        AUTH_KEYSPACE => decode_auth_value(value),
        API_STATE_KEYSPACE => decode_api_state_value(key, value),
        DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE => {
            raw_value(value, Some("document sync applied op".to_string()))
        }
        NODE_STATE_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<PersistedNodeState>(bytes),
            |data| DecodedValue::NodeState {
                data: JsonPersistedNodeState(data),
            },
        ),
        SYNC_PLACEMENT_KEYSPACE => decode_value_with(
            value,
            aruna_operations::sync_placement::decode_placement,
            |data| DecodedValue::PendingDocumentPlacement {
                data: JsonPendingDocumentPlacement(data),
            },
        ),
        ONBOARDING_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<OnboardingSecretRecord>(bytes),
            |data| DecodedValue::OnboardingSecretRecord { data },
        ),
        CRAQLE_TERMS_KEYSPACE => decode_value_with(
            value,
            |bytes| String::from_utf8(bytes.to_vec()),
            |data| DecodedValue::CraqleTerm { data },
        ),
        CRAQLE_QUADS_KEYSPACE => decode_value_with(value, decode_craqle_dots, |data| {
            DecodedValue::CraqleQuadDots { data }
        }),
        CRAQLE_GRAPHS_KEYSPACE => decode_craqle_graph_value(key, value),
        CRAQLE_LOG_KEYSPACE => decode_craqle_log_value(key, value),
        DHT_KEYSPACE => decode_value_with(value, decode_dht_entries, |data| {
            DecodedValue::DhtEntries { data }
        }),
        _ => raw_value(value, None),
    }
}

fn decode_auth_value(value: &[u8]) -> DecodedValue {
    if let Ok(data) = GroupAuthorizationDocument::from_bytes(value) {
        return DecodedValue::GroupAuthorizationDocument { data };
    }
    if let Ok(data) = RealmAuthorizationDocument::from_bytes(value) {
        return DecodedValue::RealmAuthorizationDocument {
            data: JsonRealmAuthorizationDocument(data),
        };
    }

    raw_value(
        value,
        Some(
            "failed to decode as GroupAuthorizationDocument or RealmAuthorizationDocument"
                .to_string(),
        ),
    )
}

fn decode_api_state_value(key: &[u8], value: &[u8]) -> DecodedValue {
    match key {
        TOKEN_REVOCATION_LIST_KEY => postcard::from_bytes::<HashSet<String>>(value)
            .map(|data| DecodedValue::ApiTokenRevocationList { data })
            .unwrap_or_else(|error| raw_value(value, Some(error.to_string()))),
        TRUSTED_REALMS_LIST_KEY => postcard::from_bytes::<HashSet<RealmId>>(value)
            .map(|data| {
                let mut data = data
                    .into_iter()
                    .map(|realm_id| realm_id.to_string())
                    .collect::<Vec<_>>();
                data.sort();
                DecodedValue::ApiTrustedRealmsList { data }
            })
            .unwrap_or_else(|error| raw_value(value, Some(error.to_string()))),
        INITIAL_REALM_ADMIN_CLAIMED_KEY => postcard::from_bytes::<bool>(value)
            .map(|data| DecodedValue::ApiInitialRealmAdminClaimed { data })
            .unwrap_or_else(|error| raw_value(value, Some(error.to_string()))),
        _ => raw_value(value, Some("unsupported api_state key".to_string())),
    }
}

fn decode_multipart_object_metadata_value(key: &[u8], value: &[u8]) -> DecodedValue {
    match MultipartObjectMetadataKey::from_bytes(key) {
        Ok(MultipartObjectMetadataKey::Summary { .. }) => {
            decode_value_with(value, MultipartObjectSummary::from_bytes, |data| {
                DecodedValue::MultipartObjectSummary { data }
            })
        }
        Ok(MultipartObjectMetadataKey::Part { .. }) => {
            decode_value_with(value, MultipartObjectPart::from_bytes, |data| {
                DecodedValue::MultipartObjectPart { data }
            })
        }
        Err(error) => raw_value(value, Some(error.to_string())),
    }
}

fn decode_value_with<T, E>(
    value: &[u8],
    decoder: impl Fn(&[u8]) -> Result<T, E>,
    mapper: impl Fn(T) -> DecodedValue,
) -> DecodedValue
where
    E: std::fmt::Display,
{
    match decoder(value) {
        Ok(data) => mapper(data),
        Err(error) => raw_value(value, Some(error.to_string())),
    }
}

fn decode_ulid_key(key: &[u8]) -> DecodedField {
    if key.len() == 16 {
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(key);
        DecodedField::Ulid {
            value: Ulid::from_bytes(bytes).to_string(),
        }
    } else {
        raw_field(key)
    }
}

fn decode_realm_id_key(key: &[u8]) -> DecodedField {
    if key.len() == 32 {
        let mut bytes = [0_u8; 32];
        bytes.copy_from_slice(key);
        DecodedField::RealmId {
            value: RealmId::from_bytes(bytes).to_string(),
        }
    } else {
        raw_field(key)
    }
}

fn decode_utf8_key(key: &[u8]) -> DecodedField {
    String::from_utf8(key.to_vec())
        .map(|value| DecodedField::Utf8 { value })
        .unwrap_or_else(|_| raw_field(key))
}

fn decode_dht_key(key: &[u8]) -> DecodedField {
    if key.len() == 32 {
        let mut bytes = [0_u8; 32];
        bytes.copy_from_slice(key);
        DecodedField::DhtKeyId {
            value: DhtKeyId::from_bytes(bytes).to_string(),
        }
    } else {
        raw_field(key)
    }
}

fn decode_dht_entries(value: &[u8]) -> Result<Vec<JsonStoredEntry>, postcard::Error> {
    postcard::from_bytes::<Vec<StoredEntry>>(value)
        .map(|entries| entries.into_iter().map(JsonStoredEntry).collect())
}

fn raw_field(bytes: &[u8]) -> DecodedField {
    DecodedField::Raw {
        hex: hex::encode(bytes),
    }
}

fn raw_value(value: &[u8], decode_error: Option<String>) -> DecodedValue {
    DecodedValue::Raw {
        hex: hex::encode(value),
        decode_error,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CRAQLE_BATCH_LOG_ENCODING_TAG, CRAQLE_DOT_ENCODING_TAG, CRAQLE_GRAPH_META_PREFIX,
        CRAQLE_GRAPHS_KEYSPACE, CRAQLE_LOG_BATCH_PREFIX, CRAQLE_LOG_KEYSPACE,
        CRAQLE_QUADS_KEYSPACE, CRAQLE_TERMS_KEYSPACE, CraqleStoredBatch, CraqleStoredGraphMeta,
        CraqleStoredQuadOp, DecodedField, DecodedValue, decode_entry, list_entries, list_keyspaces,
        placement_missing_peer_count, raw_field,
    };
    use aruna::config::{
        BootOrigin, PersistedNodeIdentity, PersistedNodeState, PersistedNodeStatus,
    };
    use aruna_core::document::DocumentSyncTarget;
    use aruna_core::id::DhtKeyId;
    use aruna_core::keyspaces::{
        ADMIN_DOCUMENT_CONFLICT_KEYSPACE, ADMIN_DOCUMENT_STATE_KEYSPACE, API_STATE_KEYSPACE,
        AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
        BUCKET_STATS_DB, DHT_KEYSPACE, DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE, GROUP_KEYSPACE,
        HASH_PATHS_INDEX_KEYSPACE, METADATA_AUDIT_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE,
        METADATA_HOLDERS_KEYSPACE, METADATA_INDEX_KEYSPACE, NODE_STATE_KEYSPACE,
        ONBOARDING_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
        S3_BUCKET_REPLICATION_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
        S3_MULTIPART_UPLOAD_KEYSPACE, S3_MULTIPART_UPLOAD_PART_KEYSPACE,
        SOURCE_CONNECTOR_INDEX_KEYSPACE, SOURCE_CONNECTOR_SECRET_KEYSPACE, SYNC_PLACEMENT_KEYSPACE,
        USER_ACCESS_KEYSPACE,
    };
    use aruna_core::onboarding::{OnboardingMode, OnboardingSecretRecord};
    use aruna_core::structs::{
        Actor, BackendLocation, BlobHeadKey, BlobVersion, BucketReplicationConfig,
        BucketReplicationTarget, Group, HashPathIndexKey, MultipartChecksumType,
        MultipartObjectMetadataKey, MultipartObjectPart, MultipartObjectSummary, MultipartUpload,
        MultipartUploadPart, MultipartUploadPartKey, MultipartUploadStatus, RealmConfigDocument,
        RealmId,
    };
    use aruna_net::dht::storage::StoredEntry;
    use chrono::{DateTime, Utc};
    use craqle::{
        ActorId as CraqleActorId, Dot as CraqleDot, GraphPolicy as CraqleGraphPolicy,
        VectorClock as CraqleVectorClock,
    };
    use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase};
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[test]
    fn lists_sorted_keyspaces() {
        let temp = tempdir().unwrap();
        {
            let db = OptimisticTxDatabase::builder(temp.path()).open().unwrap();
            db.keyspace("zeta", KeyspaceCreateOptions::default).unwrap();
            db.keyspace("alpha", KeyspaceCreateOptions::default)
                .unwrap();
            db.keyspace(GROUP_KEYSPACE, KeyspaceCreateOptions::default)
                .unwrap();
        }

        let output = list_keyspaces(temp.path().to_str().unwrap()).unwrap();
        let names = output
            .keyspaces
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "alpha".to_string(),
                GROUP_KEYSPACE.to_string(),
                "zeta".to_string()
            ]
        );

        let missing = output
            .missing_keyspaces
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>();
        let mut expected_missing = vec![
            ADMIN_DOCUMENT_CONFLICT_KEYSPACE.to_string(),
            ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
            API_STATE_KEYSPACE.to_string(),
            AUTH_KEYSPACE.to_string(),
            BLOB_HEAD_KEYSPACE.to_string(),
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            BLOB_VERSIONS_KEYSPACE.to_string(),
            BUCKET_STATS_DB.to_string(),
            CRAQLE_GRAPHS_KEYSPACE.to_string(),
            CRAQLE_LOG_KEYSPACE.to_string(),
            CRAQLE_QUADS_KEYSPACE.to_string(),
            CRAQLE_TERMS_KEYSPACE.to_string(),
            DHT_KEYSPACE.to_string(),
            HASH_PATHS_INDEX_KEYSPACE.to_string(),
            DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
            METADATA_AUDIT_KEYSPACE.to_string(),
            METADATA_DOCUMENT_INDEX_KEYSPACE.to_string(),
            METADATA_HOLDERS_KEYSPACE.to_string(),
            METADATA_INDEX_KEYSPACE.to_string(),
            NODE_STATE_KEYSPACE.to_string(),
            ONBOARDING_KEYSPACE.to_string(),
            REALM_CONFIG_KEYSPACE.to_string(),
            S3_BUCKET_KEYSPACE.to_string(),
            S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            S3_MULTIPART_UPLOAD_PART_KEYSPACE.to_string(),
            SOURCE_CONNECTOR_INDEX_KEYSPACE.to_string(),
            SOURCE_CONNECTOR_SECRET_KEYSPACE.to_string(),
            SYNC_PLACEMENT_KEYSPACE.to_string(),
            USER_ACCESS_KEYSPACE.to_string(),
        ];
        expected_missing.sort();
        assert_eq!(missing, expected_missing);
    }

    #[test]
    fn decodes_bucket_replication_entry() {
        let realm_id = RealmId::from_bytes([4_u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[6_u8; 32]).public();
        let config = BucketReplicationConfig {
            targets: vec![BucketReplicationTarget {
                node_id,
                realm_id,
                bucket: "replica-bucket".to_string(),
                arn: format!("arn:aruna:{realm_id}:{node_id}:s3/replica-bucket"),
                replicate_delete_markers: true,
            }],
        };

        let decoded = decode_entry(
            S3_BUCKET_REPLICATION_KEYSPACE,
            b"primary-bucket",
            &config.to_bytes().unwrap(),
        );
        assert_eq!(
            decoded.key,
            DecodedField::Utf8 {
                value: "primary-bucket".to_string()
            }
        );
        match decoded.value {
            DecodedValue::BucketReplicationConfig { data } => assert_eq!(data, config),
            other => panic!("expected bucket replication config, got {other:?}"),
        }
    }

    #[test]
    fn decodes_typed_group_entries() {
        let temp = tempdir().unwrap();
        let group_id = Ulid::new();
        let realm_id = RealmId::from_bytes([7_u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[9_u8; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
            realm_id,
        };
        let group = Group {
            display_name: "Explorer Group".to_string(),
            group_id,
            realm_id,
            roles: Default::default(),
            owner: aruna_core::UserId::local(Ulid::new(), realm_id),
        };

        {
            let db = OptimisticTxDatabase::builder(temp.path()).open().unwrap();
            let keyspace = db
                .keyspace(GROUP_KEYSPACE, KeyspaceCreateOptions::default)
                .unwrap();
            let mut txn = db.write_tx().unwrap();
            txn.insert(
                keyspace,
                group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            );
            let _ = txn.commit().unwrap();
        }

        let output = list_entries(temp.path().to_str().unwrap(), GROUP_KEYSPACE).unwrap();
        assert_eq!(output.entries.len(), 1);
        assert_eq!(
            output.entries[0].key,
            DecodedField::Ulid {
                value: group_id.to_string()
            }
        );
        match &output.entries[0].value {
            DecodedValue::Group { data } => assert_eq!(data.0.display_name, "Explorer Group"),
            other => panic!("expected group, got {other:?}"),
        }
    }

    #[test]
    fn falls_back_to_raw_for_unknown_keyspace() {
        let entry = decode_entry("unknown", b"\x01\x02", b"\x03\x04");
        assert_eq!(entry.key, raw_field(b"\x01\x02"));
        match entry.value {
            DecodedValue::Raw { hex, .. } => assert_eq!(hex, "0304"),
            other => panic!("expected raw fallback, got {other:?}"),
        }
    }

    #[test]
    fn decodes_realm_config_key() {
        let realm_id = RealmId::from_bytes([5_u8; 32]);
        let entry = decode_entry(REALM_CONFIG_KEYSPACE, realm_id.as_bytes(), b"not-a-config");
        assert_eq!(
            entry.key,
            DecodedField::RealmId {
                value: realm_id.to_string()
            }
        );
    }

    #[test]
    fn returns_missing_keyspace_error() {
        let temp = tempdir().unwrap();
        let error = list_entries(temp.path().to_str().unwrap(), "missing").unwrap_err();
        assert!(error.to_string().contains("keyspace not found"));
    }

    #[test]
    fn decodes_typed_realm_config_value_with_raw_error_fallback() {
        let realm_id = RealmId::from_bytes([1_u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[3_u8; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::new(), realm_id),
            realm_id,
        };
        let mut realm_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        realm_config.description = "Explorer Realm".to_string();

        let decoded = decode_entry(
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes(),
            &realm_config.to_bytes(&actor).unwrap(),
        );
        match decoded.value {
            DecodedValue::RealmConfigDocument { data } => {
                assert_eq!(data.0.description, "Explorer Realm")
            }
            other => panic!("expected realm config, got {other:?}"),
        }

        let fallback = decode_entry(REALM_CONFIG_KEYSPACE, realm_id.as_bytes(), b"broken");
        match fallback.value {
            DecodedValue::Raw {
                decode_error: Some(_),
                ..
            } => {}
            other => panic!("expected raw decode fallback, got {other:?}"),
        }
    }

    #[test]
    fn decodes_onboarding_secret_record_value() {
        let record = OnboardingSecretRecord {
            enrollment_id: Ulid::new(),
            secret_hash: "hash123".to_string(),
            mode: OnboardingMode::Server,
            expires_at: 1234,
            claimed_node_id: None,
        };
        let value = postcard::to_allocvec(&record).unwrap();

        let decoded = decode_entry(ONBOARDING_KEYSPACE, b"secret:test", &value);
        assert_eq!(
            decoded.key,
            DecodedField::Utf8 {
                value: "secret:test".to_string()
            }
        );
        match decoded.value {
            DecodedValue::OnboardingSecretRecord { data } => assert_eq!(data, record),
            other => panic!("expected onboarding secret record, got {other:?}"),
        }
    }

    #[test]
    fn decodes_node_state_value() {
        let realm_id = RealmId::from_bytes([4_u8; 32]);
        let state = PersistedNodeState {
            boot_origin: BootOrigin::Onboarded,
            status: PersistedNodeStatus::PendingOnboarding,
            realm_id,
            net_secret_key: [11_u8; 32],
            onboarding_phase: None,
            onboarding_sync_ticket: Some("ticket".to_string()),
            identity: PersistedNodeIdentity::Local,
        };
        let value = postcard::to_allocvec(&state).unwrap();

        let decoded = decode_entry(NODE_STATE_KEYSPACE, b"node_state", &value);
        assert_eq!(
            decoded.key,
            DecodedField::Utf8 {
                value: "node_state".to_string()
            }
        );
        match decoded.value {
            DecodedValue::NodeState { data } => assert_eq!(data.0, state),
            other => panic!("expected node state, got {other:?}"),
        }
    }

    #[test]
    fn decodes_pending_topic_placement_value() {
        let target = DocumentSyncTarget::RealmConfig {
            realm_id: RealmId::from_bytes([4_u8; 32]),
        };
        let realm_id = RealmId::from_bytes([4_u8; 32]);
        let selected_peer = iroh::SecretKey::from_bytes(&[7_u8; 32]).public();
        let authoritative_node_id = iroh::SecretKey::from_bytes(&[6_u8; 32]).public();
        let placement = aruna_operations::sync_placement::new_placement(
            realm_id,
            target.clone(),
            authoritative_node_id,
            3,
            vec![selected_peer],
            aruna_core::structs::PlacementRef::NIL,
        );
        let value = postcard::to_allocvec(&placement).unwrap();
        let key = aruna_operations::sync_placement::placement_key(realm_id, &target);

        let decoded = decode_entry(SYNC_PLACEMENT_KEYSPACE, key.as_ref(), &value);
        assert_eq!(
            decoded.key,
            DecodedField::Raw {
                hex: hex::encode(key.as_ref())
            }
        );
        match decoded.value {
            DecodedValue::PendingDocumentPlacement { data } => {
                assert_eq!(data.0.realm_id, realm_id);
                assert_eq!(data.0.target, target);
                assert_eq!(data.0.authoritative_node_id, authoritative_node_id);
                assert_eq!(data.0.desired_peer_count, 3);
                assert_eq!(data.0.selected_peers, vec![selected_peer]);
                assert_eq!(placement_missing_peer_count(&data.0), 1);
            }
            other => panic!("expected pending topic placement, got {other:?}"),
        }
    }

    #[test]
    fn decodes_dht_entries_and_key() {
        let key = DhtKeyId::from_bytes([6_u8; 32]);
        let realm_id = RealmId::from_bytes([7_u8; 32]);
        let publisher = iroh::SecretKey::from_bytes(&[5_u8; 32]).public();
        let entries = vec![StoredEntry {
            publisher,
            realm_id,
            value: vec![1, 2, 3, 4],
            expires_at: 42,
            signature: None,
        }];
        let value = postcard::to_allocvec(&entries).unwrap();

        let decoded = decode_entry(DHT_KEYSPACE, key.as_bytes(), &value);
        assert_eq!(
            decoded.key,
            DecodedField::DhtKeyId {
                value: key.to_string()
            }
        );
        match decoded.value {
            DecodedValue::DhtEntries { data } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0].0.publisher, publisher);
                assert_eq!(data[0].0.realm_id, realm_id);
                assert_eq!(data[0].0.value, vec![1, 2, 3, 4]);
            }
            other => panic!("expected dht entries, got {other:?}"),
        }
    }

    #[test]
    fn decodes_multipart_upload_entry() {
        let realm_id = RealmId::from_bytes([3_u8; 32]);
        let created_by = aruna_core::UserId::local(Ulid::new(), realm_id);
        let upload = MultipartUpload {
            upload_id: Ulid::from_bytes([7_u8; 16]),
            bucket: "bucket-a".to_string(),
            key: "parts/big.bin".to_string(),
            group_id: Ulid::from_bytes([8_u8; 16]),
            created_by,
            created_at: SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
        };

        let decoded = decode_entry(
            S3_MULTIPART_UPLOAD_KEYSPACE,
            &upload.upload_id.to_bytes(),
            &upload.to_bytes().unwrap(),
        );
        assert_eq!(
            decoded.key,
            DecodedField::Ulid {
                value: upload.upload_id.to_string()
            }
        );
        match decoded.value {
            DecodedValue::MultipartUpload { data } => assert_eq!(data, upload),
            other => panic!("expected multipart upload, got {other:?}"),
        }
    }

    #[test]
    fn decodes_multipart_upload_part_entry() {
        let realm_id = RealmId::from_bytes([9_u8; 32]);
        let created_by = aruna_core::UserId::local(Ulid::new(), realm_id);
        let key = MultipartUploadPartKey::new(Ulid::from_bytes([2_u8; 16]), 5);
        let part = MultipartUploadPart {
            part_number: 5,
            location: BackendLocation {
                root: "/tmp".to_string(),
                storage_bucket: "blob-bucket".to_string(),
                backend_path: "multipart/part-5.bin".to_string(),
                ulid: Ulid::from_bytes([4_u8; 16]),
                compressed: false,
                encrypted: false,
                created_by,
                created_at: SystemTime::UNIX_EPOCH,
                staging: false,
                partial: true,
                blob_size: 42,
                hashes: HashMap::from([("md5".to_string(), vec![1_u8; 16])]),
            },
            created_at: SystemTime::UNIX_EPOCH,
        };

        let decoded = decode_entry(
            S3_MULTIPART_UPLOAD_PART_KEYSPACE,
            &key.to_bytes().unwrap(),
            &part.to_bytes().unwrap(),
        );
        assert_eq!(
            decoded.key,
            DecodedField::MultipartUploadPartKey { value: key.clone() }
        );
        match decoded.value {
            DecodedValue::MultipartUploadPart { data } => assert_eq!(data, part),
            other => panic!("expected multipart upload part, got {other:?}"),
        }
    }

    #[test]
    fn decodes_multipart_object_summary_entry() {
        let key = MultipartObjectMetadataKey::summary(Ulid::from_bytes([1_u8; 16]));
        let summary = MultipartObjectSummary {
            checksum_type: MultipartChecksumType::Composite,
            part_count: 3,
        };

        let decoded = decode_entry(
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
            &key.to_bytes().unwrap(),
            &summary.to_bytes().unwrap(),
        );
        assert_eq!(
            decoded.key,
            DecodedField::MultipartObjectMetadataKey { value: key.clone() }
        );
        match decoded.value {
            DecodedValue::MultipartObjectSummary { data } => assert_eq!(data, summary),
            other => panic!("expected multipart object summary, got {other:?}"),
        }
    }

    #[test]
    fn decodes_multipart_object_part_entry() {
        let key = MultipartObjectMetadataKey::part(Ulid::from_bytes([5_u8; 16]), 2);
        let part = MultipartObjectPart {
            part_number: 2,
            size: 64,
            hashes: HashMap::from([
                ("blake3".to_string(), vec![7_u8; 32]),
                ("md5".to_string(), vec![8_u8; 16]),
            ]),
        };

        let decoded = decode_entry(
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
            &key.to_bytes().unwrap(),
            &part.to_bytes().unwrap(),
        );
        assert_eq!(
            decoded.key,
            DecodedField::MultipartObjectMetadataKey { value: key.clone() }
        );
        match decoded.value {
            DecodedValue::MultipartObjectPart { data } => assert_eq!(data, part),
            other => panic!("expected multipart object part, got {other:?}"),
        }
    }

    #[test]
    fn falls_back_to_raw_for_invalid_multipart_object_metadata_value() {
        let key = MultipartObjectMetadataKey::summary(Ulid::from_bytes([3_u8; 16]));
        let decoded = decode_entry(
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
            &key.to_bytes().unwrap(),
            b"broken",
        );
        match decoded.value {
            DecodedValue::Raw {
                decode_error: Some(_),
                ..
            } => {}
            other => panic!("expected raw decode fallback, got {other:?}"),
        }
    }

    #[test]
    fn decodes_craqle_term_entry() {
        let term_id = 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10_u128;
        let decoded = decode_entry(
            CRAQLE_TERMS_KEYSPACE,
            &term_id.to_be_bytes(),
            b"<https://example.org/dataset>",
        );

        assert_eq!(
            decoded.key,
            DecodedField::CraqleTermId {
                value: format!("{term_id:032x}")
            }
        );
        match decoded.value {
            DecodedValue::CraqleTerm { data } => {
                assert_eq!(data, "<https://example.org/dataset>")
            }
            other => panic!("expected craqle term, got {other:?}"),
        }
    }

    #[test]
    fn decodes_craqle_quad_entry() {
        let graph = 1_u128;
        let subject = 2_u128;
        let predicate = 3_u128;
        let object = 4_u128;
        let actor = CraqleActorId::from_bytes([8_u8; 32]);

        let mut key = Vec::new();
        key.extend_from_slice(&graph.to_be_bytes());
        key.extend_from_slice(&subject.to_be_bytes());
        key.extend_from_slice(&predicate.to_be_bytes());
        key.extend_from_slice(&object.to_be_bytes());

        let mut value = vec![CRAQLE_DOT_ENCODING_TAG];
        value.extend_from_slice(actor.as_bytes());
        value.extend_from_slice(&7_u64.to_be_bytes());

        let decoded = decode_entry(CRAQLE_QUADS_KEYSPACE, &key, &value);
        assert_eq!(
            decoded.key,
            DecodedField::CraqleQuadKey {
                value: super::JsonCraqleQuadKey {
                    graph: format!("{graph:032x}"),
                    subject: format!("{subject:032x}"),
                    predicate: format!("{predicate:032x}"),
                    object: format!("{object:032x}"),
                }
            }
        );
        match decoded.value {
            DecodedValue::CraqleQuadDots { data } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0].actor, actor.to_string());
                assert_eq!(data[0].counter, 7);
            }
            other => panic!("expected craqle quad dots, got {other:?}"),
        }
    }

    #[test]
    fn decodes_craqle_graph_meta_entry() {
        let graph = 9_u128;
        let actor = CraqleActorId::from_bytes([5_u8; 32]);
        let mut key = vec![CRAQLE_GRAPH_META_PREFIX];
        key.extend_from_slice(&graph.to_be_bytes());

        let value = postcard::to_allocvec(&CraqleStoredGraphMeta {
            policy: CraqleGraphPolicy {
                public: true,
                permission_paths: vec!["/b".to_string(), "/a".to_string(), "/a".to_string()],
            },
            clock: CraqleVectorClock(BTreeMap::from([(actor, 11_u64)])),
        })
        .unwrap();

        let decoded = decode_entry(CRAQLE_GRAPHS_KEYSPACE, &key, &value);
        match decoded.key {
            DecodedField::CraqleGraphKey { value } => assert_eq!(
                value,
                super::JsonCraqleGraphKey::Meta {
                    graph: format!("{graph:032x}")
                }
            ),
            other => panic!("expected craqle graph key, got {other:?}"),
        }
        match decoded.value {
            DecodedValue::CraqleGraphMeta { data } => {
                assert_eq!(data.graph, format!("{graph:032x}"));
                assert!(data.policy.public);
                assert_eq!(data.policy.permission_paths, vec!["/a", "/b"]);
                assert_eq!(data.clock.entries.len(), 1);
                assert_eq!(data.clock.entries[0].actor, actor.to_string());
                assert_eq!(data.clock.entries[0].counter, 11);
            }
            other => panic!("expected craqle graph meta, got {other:?}"),
        }
    }

    #[test]
    fn decodes_craqle_log_batch_entry() {
        let graph = 12_u128;
        let subject = 13_u128;
        let predicate = 14_u128;
        let object = 15_u128;
        let actor = CraqleActorId::from_bytes([3_u8; 32]);

        let mut key = vec![CRAQLE_LOG_BATCH_PREFIX];
        key.extend_from_slice(&graph.to_be_bytes());
        key.extend_from_slice(actor.as_bytes());
        key.extend_from_slice(&17_u64.to_be_bytes());

        let batch = CraqleStoredBatch {
            actor,
            counter: 17,
            base_clock: CraqleVectorClock(BTreeMap::from([(actor, 16_u64)])),
            ops: vec![CraqleStoredQuadOp::Add {
                subject: super::CraqleTermId(subject),
                predicate: super::CraqleTermId(predicate),
                object: super::CraqleTermId(object),
                dot: CraqleDot { actor, counter: 17 },
            }],
            timestamp: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
        };

        let mut value = vec![CRAQLE_BATCH_LOG_ENCODING_TAG];
        value.extend_from_slice(&postcard::to_allocvec(&batch).unwrap());

        let decoded = decode_entry(CRAQLE_LOG_KEYSPACE, &key, &value);
        match decoded.key {
            DecodedField::CraqleLogKey { value } => assert_eq!(
                value,
                super::JsonCraqleLogKey::Batch {
                    graph: format!("{graph:032x}"),
                    actor: actor.to_string(),
                    counter: 17,
                }
            ),
            other => panic!("expected craqle log key, got {other:?}"),
        }
        match decoded.value {
            DecodedValue::CraqleLogBatch { data } => {
                assert_eq!(data.graph, format!("{graph:032x}"));
                assert_eq!(data.actor, actor.to_string());
                assert_eq!(data.counter, 17);
                assert_eq!(data.base_clock.entries.len(), 1);
                assert_eq!(data.base_clock.entries[0].counter, 16);
                assert_eq!(data.ops.len(), 1);
                match &data.ops[0] {
                    super::JsonCraqleStoredBatchOp::Add {
                        subject: got_subject,
                        predicate: got_predicate,
                        object: got_object,
                        dot,
                    } => {
                        assert_eq!(got_subject, &format!("{subject:032x}"));
                        assert_eq!(got_predicate, &format!("{predicate:032x}"));
                        assert_eq!(got_object, &format!("{object:032x}"));
                        assert_eq!(dot.actor, actor.to_string());
                        assert_eq!(dot.counter, 17);
                    }
                    other => panic!("expected add op, got {other:?}"),
                }
            }
            other => panic!("expected craqle log batch, got {other:?}"),
        }
    }

    #[test]
    fn falls_back_to_raw_for_invalid_node_state_value() {
        let decoded = decode_entry(NODE_STATE_KEYSPACE, b"node_state", b"broken");
        match decoded.value {
            DecodedValue::Raw {
                decode_error: Some(_),
                ..
            } => {}
            other => panic!("expected raw decode fallback, got {other:?}"),
        }
    }

    #[test]
    fn decodes_new_blob_keyspaces() {
        let realm_id = RealmId::from_bytes([1_u8; 32]);
        let group_id = Ulid::from_bytes([2_u8; 16]);
        let node_id = iroh::SecretKey::from_bytes(&[3_u8; 32]).public();
        let created_by = aruna_core::UserId::local(Ulid::new(), realm_id);
        let head_key = BlobHeadKey::new("bucket", "path/file.txt");
        let head_value = aruna_core::structs::CurrentVersionPointer::new_with_generation(
            Ulid::from_bytes([4_u8; 16]),
            7,
        )
        .to_bytes()
        .unwrap();
        let location = BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "blob-bucket".to_string(),
            backend_path: "path/blob.bin".to_string(),
            ulid: Ulid::from_bytes([5_u8; 16]),
            compressed: false,
            encrypted: false,
            created_by,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 11,
            hashes: HashMap::from([("blake3".to_string(), vec![9_u8; 32])]),
        };
        let version = BlobVersion::materialized(
            [9_u8; 32],
            std::time::SystemTime::UNIX_EPOCH,
            created_by,
            None,
        );
        let hash_path_key = HashPathIndexKey::new(
            [9_u8; 32],
            Ulid::from_bytes([4_u8; 16]),
            realm_id,
            group_id,
            node_id,
            "bucket",
            "path/file.txt",
        );

        let decoded_head = decode_entry(
            BLOB_HEAD_KEYSPACE,
            &head_key.to_bytes().unwrap(),
            &head_value,
        );
        assert_eq!(
            decoded_head.key,
            DecodedField::BlobHeadKey { value: head_key }
        );
        match decoded_head.value {
            DecodedValue::CurrentVersionPointer { data } => {
                assert_eq!(data.version_id, Ulid::from_bytes([4_u8; 16]));
                assert_eq!(data.generation, 7);
            }
            other => panic!("expected current version pointer, got {other:?}"),
        }

        let decoded_location = decode_entry(
            BLOB_LOCATIONS_KEYSPACE,
            &[9_u8; 32],
            &location.to_bytes().unwrap(),
        );
        match decoded_location.value {
            DecodedValue::BackendLocation { data } => assert_eq!(data, location),
            other => panic!("expected backend location, got {other:?}"),
        }

        let version_key = aruna_core::structs::VersionKey::new(
            "bucket",
            "path/file.txt",
            Ulid::from_bytes([6_u8; 16]),
        );
        let decoded_version = decode_entry(
            BLOB_VERSIONS_KEYSPACE,
            &version_key.to_bytes().unwrap(),
            &version.to_bytes().unwrap(),
        );
        match decoded_version.value {
            DecodedValue::BlobVersion { data } => assert_eq!(data, version),
            other => panic!("expected blob version, got {other:?}"),
        }

        let decoded_index = decode_entry(
            HASH_PATHS_INDEX_KEYSPACE,
            &hash_path_key.to_bytes().unwrap(),
            &[],
        );
        assert_eq!(
            decoded_index.key,
            DecodedField::HashPathIndexKey {
                value: hash_path_key.clone()
            }
        );
        match decoded_index.value {
            DecodedValue::Raw {
                hex,
                decode_error: None,
            } => assert_eq!(hex, ""),
            other => panic!("expected raw marker value, got {other:?}"),
        }
        assert_eq!(
            hash_path_key.permission_path(),
            format!(
                "/{realm_id}/g/{group_id}/data/{}/bucket/path/file.txt",
                node_id
            )
        );
    }
}
