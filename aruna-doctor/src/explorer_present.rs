//! Presentation of decoded keyspace rows: the JSON output records the doctor
//! prints and the field-by-field projections that keep wire records readable
//! without exposing unprojected bodies.

use aruna::identity::PersistedNodeState;
use aruna_core::compute_quota::{ComputeDepartureReport, JobReservationRecord};
use aruna_core::document::{PendingShardPlacement, shard_topic_id};
use aruna_core::onboarding::OnboardingSecretRecord;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BucketInfo, CurrentVersionPointer, HashIndex, ManagedCopyKey,
    ManagedCopyRecord, UserAccess, VersionKey,
};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::execution::job::{JobFamilyId, JobRecordEnvelope, JobRecordKey};
use aruna_core::structs::storage::multipart::{
    MultipartObjectKey, MultipartObjectPart, MultipartObjectSummary, MultipartPart,
    MultipartPartKey, MultipartUpload,
};
use aruna_core::structs::placement::node_subject::NodeSubjectRecord;
use aruna_core::structs::placement::policy_document::PlacementPolicyDocument;
use aruna_core::structs::placement::policy_attachment::{
    PolicyBulkRun, PolicyIntent, PolicyMutationRecord,
};
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmConfigDocument};
use aruna_net::dht::storage::StoredEntry;
use aruna_operations::jobs::lifecycle::witness::WitnessDeadline;
use aruna_operations::jobs::records::rows::OutboxEntry;
use aruna_operations::placement::policy::PolicyCacheEntry;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::ser::{SerializeStruct, Serializer};

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct LocationScanOutput {
    pub(super) database_path: String,
    pub(super) backends_path: Option<String>,
    pub(super) scanned: usize,
    pub(super) unresolved: Vec<UnresolvedLocation>,
}

#[derive(Debug, Serialize, PartialEq, Eq, Ord, PartialOrd)]
pub(super) struct UnresolvedLocation {
    pub(super) backend: String,
    pub(super) storage_bucket: String,
    pub(super) backend_path: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct KeyspacesOutput {
    pub(super) database_path: String,
    pub(super) keyspaces: Vec<KeyspaceEntry>,
    pub(super) missing_keyspaces: Vec<KeyspaceEntry>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct KeyspaceEntry {
    pub(super) name: String,
}

#[derive(Debug, Serialize, PartialEq)]
pub(super) struct EntriesOutput {
    pub(super) database_path: String,
    pub(super) keyspace: String,
    pub(super) entries: Vec<EntryOutput>,
}

#[derive(Debug, Serialize, PartialEq)]
pub(super) struct TopicsListOutput {
    pub(super) database_path: String,
    pub(super) topics: Vec<TopicListEntry>,
}

#[derive(Debug, Serialize, PartialEq)]
pub(super) struct TopicListEntry {
    pub(super) topic_id: String,
    pub(super) strategy_id: String,
    pub(super) shard: u32,
    pub(super) status: &'static str,
    pub(super) selected_peer_count: usize,
}

#[derive(Debug, Serialize, PartialEq)]
pub(super) struct TopicStatusOutput {
    pub(super) database_path: String,
    pub(super) topic_id: String,
    pub(super) status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) pending_placement: Option<JsonPendingPlacement>,
}

#[derive(Debug, Serialize, PartialEq)]
pub(super) struct TopicPlacementsOutput {
    pub(super) database_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) topic_id: Option<String>,
    pub(super) placements: Vec<JsonPendingPlacement>,
}

#[derive(Debug, Serialize, PartialEq)]
pub(super) struct EntryOutput {
    pub(super) key: DecodedField,
    pub(super) value: DecodedValue,
}
#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "format")]
pub(super) enum DecodedField {
    #[serde(rename = "ulid")]
    Ulid { value: String },
    #[serde(rename = "realm_id")]
    RealmId { value: String },
    #[serde(rename = "dht_key")]
    DhtKeyId { value: String },
    #[serde(rename = "craqle_term_id")]
    CraqleTermId { value: String },
    #[serde(rename = "craqle_quad_key")]
    CraqleQuadKey { value: JsonQuadKey },
    #[serde(rename = "craqle_graph_key")]
    CraqleGraphKey { value: JsonGraphKey },
    #[serde(rename = "craqle_log_key")]
    CraqleLogKey { value: JsonLogKey },
    #[serde(rename = "utf8")]
    Utf8 { value: String },
    #[serde(rename = "blob_head_key")]
    BlobHeadKey { value: BlobHeadKey },
    #[serde(rename = "hash_path_index_key")]
    HashIndex { value: HashIndex },
    #[serde(rename = "blob_location_key")]
    BlobLocationKey { blake3: String, backend: String },
    #[serde(rename = "version_key")]
    VersionKey { value: VersionKey },
    #[serde(rename = "managed_copy_key")]
    ManagedCopyKey { value: ManagedCopyKey },
    #[serde(rename = "multipart_upload_part_key")]
    MultipartPartKey { value: MultipartPartKey },
    #[serde(rename = "multipart_object_metadata_key")]
    MultipartObjectKey { value: MultipartObjectKey },
    #[serde(rename = "attempt_key")]
    AttemptKey { job_id: String, attempt_epoch: u64 },
    #[serde(rename = "policy_cache_key")]
    PolicyCacheKey { policy_id: String, digest: String },
    #[serde(rename = "policy_bulk_intent_key")]
    PolicyIntentKey { operation_id: String, key: String },
    #[serde(rename = "job_record_key")]
    JobRecordKey { value: JsonRecordKey },
    #[serde(rename = "job_conflict_key")]
    JobConflictKey {
        record: JsonRecordKey,
        digest: String,
    },
    #[serde(rename = "job_alias_key")]
    JobAliasKey { job_id: String, family: String },
    #[serde(rename = "job_family_key")]
    JobFamilyKey { family: String },
    #[serde(rename = "job_explain_key")]
    JobExplainKey { family: String, node_id: String },
    #[serde(rename = "raw")]
    Raw { hex: String },
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(tag = "type")]
#[allow(clippy::large_enum_variant)]
pub(super) enum DecodedValue {
    Group {
        data: JsonGroup,
    },
    GroupAuthorizationDocument {
        data: GroupAuthorizationDocument,
    },
    RealmAuthorizationDocument {
        data: JsonAuthorizationDocument,
    },
    RealmConfigDocument {
        data: JsonConfigDocument,
    },
    UserAccess {
        data: JsonUserAccess,
    },
    BucketInfo {
        data: BucketInfo,
    },
    CurrentVersionPointer {
        data: CurrentVersionPointer,
    },
    BackendLocation {
        data: aruna_core::structs::storage::blob::BackendLocation,
    },
    BlobVersion {
        data: BlobVersion,
    },
    ManagedCopyRecord {
        data: ManagedCopyRecord,
    },
    NodeSubjectRecord {
        data: NodeSubjectRecord,
    },
    JobOutputRecord {
        data: JsonRecordEnvelope,
    },
    JobFamilyRecord {
        data: JsonRecordEnvelope,
    },
    JobPendingRecord {
        envelope: JsonRecordEnvelope,
        need: String,
        first_seen_ms: u64,
        attempts: u32,
    },
    JobConflictRecord {
        envelope: JsonRecordEnvelope,
        retained: String,
        observed_at_ms: u64,
        relayed_by: Option<String>,
    },
    JobAliasTarget {
        data: JsonRecordKey,
    },
    JobProjectionCache {
        revision: u64,
        stale: bool,
        projected: bool,
    },
    JobOutboxEntry {
        data: OutboxEntry,
    },
    JobReservation {
        data: JsonJobReservation,
    },
    JobWitnessDeadline {
        data: WitnessDeadline,
    },
    JobPlanExplain {
        sequence: u32,
        selected: Option<String>,
        alternatives: usize,
        rejected: usize,
        overlapping: bool,
        stored_at_ms: u64,
    },
    ComputeDepartureReport {
        data: ComputeDepartureReport,
    },
    PlacementPolicyDocument {
        data: JsonPlacementDocument,
    },
    PolicyCacheEntry {
        data: JsonCacheEntry,
    },
    PolicyMutationRecord {
        data: PolicyMutationRecord,
    },
    PolicyBulkRun {
        data: PolicyBulkRun,
    },
    #[serde(rename = "PolicyBulkIntent")]
    PolicyIntent {
        data: PolicyIntent,
    },
    MultipartUpload {
        data: MultipartUpload,
    },
    #[serde(rename = "MultipartUploadPart")]
    MultipartPart {
        data: MultipartPart,
    },
    MultipartObjectSummary {
        data: MultipartObjectSummary,
    },
    MultipartObjectPart {
        data: MultipartObjectPart,
    },
    ApiTrustedRealmsList {
        data: Vec<String>,
    },
    ApiInitialRealmAdminClaimed {
        data: bool,
    },
    NodeState {
        data: JsonPersistedState,
    },
    PendingDocumentPlacement {
        data: JsonPendingPlacement,
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
        data: JsonGraphMeta,
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
        data: JsonStored,
    },
    Raw {
        hex: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        decode_error: Option<String>,
    },
}
#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct JsonQuadKey {
    pub(super) graph: String,
    pub(super) subject: String,
    pub(super) predicate: String,
    pub(super) object: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub(super) enum JsonGraphKey {
    Meta { graph: String },
    Dirty { graph: String, subject: String },
    Reindex { graph: String },
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub(super) enum JsonLogKey {
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
pub(super) struct JsonClockEntry {
    pub(super) actor: String,
    pub(super) counter: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct JsonVectorClock {
    pub(super) entries: Vec<JsonClockEntry>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct JsonCraqleDot {
    pub(super) actor: String,
    pub(super) counter: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct JsonGraphPolicy {
    pub(super) public: bool,
    pub(super) permission_paths: Vec<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct JsonGraphMeta {
    pub(super) graph: String,
    pub(super) policy: JsonGraphPolicy,
    pub(super) clock: JsonVectorClock,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "kind")]
pub(super) enum JsonStoredOp {
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
        witnessed: JsonVectorClock,
    },
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct JsonStored {
    pub(super) graph: String,
    pub(super) actor: String,
    pub(super) counter: u64,
    pub(super) base_clock: JsonVectorClock,
    pub(super) ops: Vec<JsonStoredOp>,
    pub(super) timestamp: DateTime<Utc>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct JsonGroup(pub(super) Group);

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
pub(super) struct JsonAuthorizationDocument(pub(super) RealmAuthorizationDocument);

impl Serialize for JsonAuthorizationDocument {
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
pub(super) struct JsonConfigDocument(pub(super) RealmConfigDocument);

impl Serialize for JsonConfigDocument {
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
pub(super) struct JsonUserAccess(pub(super) UserAccess);

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

/// Signed output record projection: identity, authorship and integrity only.
/// The record body stays out of the CLI so job payloads never reach a console.
#[derive(Debug, PartialEq)]
pub(super) struct JsonRecordEnvelope(pub(super) JobRecordEnvelope);

impl Serialize for JsonRecordEnvelope {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("JobRecordEnvelope", 5)?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("published_by", &self.0.published_by.to_string())?;
        state.serialize_field("kind", &format!("{:?}", self.0.kind()))?;
        state.serialize_field("digest", &self.0.digest().map(hex::encode).ok())?;
        state.serialize_field("signature", &hex::encode(self.0.signature.to_bytes()))?;
        state.end()
    }
}

/// The signed identity a job record is stored under, rendered as hex so one
/// key line stays readable next to its family.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct JsonRecordKey(pub(super) JobRecordKey);

impl Serialize for JsonRecordKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("JobRecordKey", 4)?;
        state.serialize_field("family", &family_id_string(&self.0.family))?;
        state.serialize_field("kind", &format!("{:?}", self.0.kind))?;
        state.serialize_field("subject", &hex::encode(self.0.subject))?;
        state.serialize_field("sequence", &self.0.sequence)?;
        state.end()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct JsonJobReservation(pub(super) JobReservationRecord);

impl Serialize for JsonJobReservation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("JobReservationRecord", 8)?;
        state.serialize_field("execution_id", &self.0.execution_id.to_string())?;
        state.serialize_field("job_id", &self.0.job_id.to_string())?;
        state.serialize_field("cpu_cores", &self.0.resources.cpu_cores)?;
        state.serialize_field("ram_bytes", &self.0.resources.ram_bytes)?;
        state.serialize_field("disk_bytes", &self.0.resources.disk_bytes)?;
        state.serialize_field("created_at_ms", &self.0.created_at_ms)?;
        // The stored site fence: a refusal to start is diagnosed from these two.
        state.serialize_field("subject_generation", &self.0.subject_generation)?;
        state.serialize_field("subject_digest", &hex::encode(self.0.subject_digest))?;
        state.end()
    }
}

/// Stable text identity of one request family: submission id and request digest.
pub(super) fn family_id_string(family: &JobFamilyId) -> String {
    format!(
        "{}:{}",
        hex::encode(family.submission_id.0),
        hex::encode(family.request_digest)
    )
}

#[derive(Debug, PartialEq)]
pub(super) struct JsonPlacementDocument(pub(super) PlacementPolicyDocument);

impl Serialize for JsonPlacementDocument {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PlacementPolicyDocument", 6)?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("policy_id", &self.0.policy.policy_id.to_string())?;
        state.serialize_field("name", &self.0.policy.name)?;
        state.serialize_field("allowed", &self.0.policy.allowed)?;
        state.serialize_field("publisher", &self.0.publication.publisher.to_string())?;
        state.serialize_field("created_at_ms", &self.0.publication.created_at_ms)?;
        state.end()
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct JsonCacheEntry(pub(super) PolicyCacheEntry);

impl Serialize for JsonCacheEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PolicyCacheEntry", 4)?;
        match &self.0 {
            PolicyCacheEntry::Verified {
                document,
                stored_at_ms,
            } => {
                state.serialize_field("kind", "verified")?;
                state.serialize_field("stored_at_ms", stored_at_ms)?;
                state.serialize_field("expires_at_ms", &None::<u64>)?;
                state.serialize_field("document", &JsonPlacementDocument(document.clone()))?;
            }
            PolicyCacheEntry::Unavailable {
                stored_at_ms,
                expires_at_ms,
            } => {
                state.serialize_field("kind", "unavailable")?;
                state.serialize_field("stored_at_ms", stored_at_ms)?;
                state.serialize_field("expires_at_ms", &Some(*expires_at_ms))?;
                state.serialize_field("document", &None::<JsonPlacementDocument>)?;
            }
        }
        state.end()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct JsonPersistedState(pub(super) PersistedNodeState);

impl Serialize for JsonPersistedState {
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
pub(super) struct JsonPendingPlacement(pub(super) PendingShardPlacement);

impl Serialize for JsonPendingPlacement {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PendingShardPlacement", 7)?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("topic_id", &placement_topic_id(&self.0))?;
        state.serialize_field("strategy_id", &self.0.placement.strategy_id.to_string())?;
        state.serialize_field("shard", &self.0.placement.shard)?;
        state.serialize_field(
            "authoritative_node_id",
            &self.0.authoritative_node_id.to_string(),
        )?;
        state.serialize_field(
            "selected_peers",
            &self
                .0
                .selected_peers
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>(),
        )?;
        state.serialize_field("updated_at", &self.0.updated_at)?;
        state.end()
    }
}

pub(super) fn placement_topic_id(placement: &PendingShardPlacement) -> String {
    shard_topic_id(placement.realm_id, &placement.placement).to_string()
}

#[derive(Debug)]
pub(super) struct JsonStoredEntry(pub(super) StoredEntry);

impl PartialEq for JsonStoredEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.publisher == other.0.publisher
            && self.0.realm_id == other.0.realm_id
            && self.0.value == other.0.value
            && self.0.expires_at == other.0.expires_at
            && self.0.revision == other.0.revision
            && self.0.signature == other.0.signature
            && self.0.retain_until == other.0.retain_until
    }
}

impl Serialize for JsonStoredEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("StoredEntry", 8)?;
        state.serialize_field("publisher", &self.0.publisher.to_string())?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("expires_at", &self.0.expires_at)?;
        state.serialize_field("revision", &self.0.revision)?;
        state.serialize_field("retain_until", &self.0.retain_until)?;
        state.serialize_field("signature", &self.0.signature.to_string())?;
        state.serialize_field("value_len", &self.0.value.len())?;
        state.serialize_field("value_hex", &hex::encode(&self.0.value))?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::{DecodedField, DecodedValue, EntryOutput, JsonPersistedState};
    use aruna::identity::PersistedNodeState;

    // The JSON record shape is a CLI contract: the tagged key field and the
    // tagged value field with their fixed fixture values.
    #[test]
    fn entry_output_shape() {
        let entry = EntryOutput {
            key: DecodedField::Ulid {
                value: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string(),
            },
            value: DecodedValue::Raw {
                hex: "00ff".to_string(),
                decode_error: None,
            },
        };
        assert_eq!(
            serde_json::to_value(entry).unwrap(),
            serde_json::json!({
                "key": { "format": "ulid", "value": "01ARZ3NDEKTSV4RRFFQ69G5FAV" },
                "value": { "type": "Raw", "hex": "00ff" }
            })
        );
    }

    // The local operator output intentionally includes the persisted network
    // secret; a redaction change would be a CLI contract decision.
    #[test]
    fn network_secret_persisted() {
        let state = PersistedNodeState {
            boot_origin: aruna::identity::BootOrigin::InitializedRealm,
            status: aruna::identity::PersistedNodeStatus::Complete,
            realm_id: aruna_core::structs::identity::realm::RealmId([3u8; 32]),
            net_secret_key: [7u8; 32],
            onboarding_phase: None,
            onboarding_sync_ticket: None,
            identity: aruna::identity::PersistedNodeIdentity::Management {
                realm_private_key_pem: "synthetic-pem".to_string(),
            },
        };
        let json = serde_json::to_value(JsonPersistedState(state)).unwrap();
        assert_eq!(
            json["net_secret_key"].as_str(),
            Some("0707070707070707070707070707070707070707070707070707070707070707")
        );
    }
}
