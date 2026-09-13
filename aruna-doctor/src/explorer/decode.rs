//! Persisted decoding: one decoder per keyspace, mapping raw key/value bytes to
//! the presentation records in [`super::present`]. Malformed or unknown rows
//! fall back to a raw hex record instead of failing the whole listing.

use aruna::config::PersistedNodeState;
use aruna_api::server_state::INITIAL_REALM_ADMIN_CLAIMED_KEY;
use aruna_core::auth::TRUSTED_REALMS_LIST_KEY;
use aruna_core::compute_quota::{ComputeDepartureReport, JobReservationRecord};
use aruna_core::id::DhtKeyId;
use aruna_core::keyspaces::{
    API_STATE_KEYSPACE, AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE, COMPUTE_DEPARTURE_KEYSPACE, CRAQLE_GRAPHS_KEYSPACE,
    CRAQLE_LOG_KEYSPACE, CRAQLE_QUADS_KEYSPACE, CRAQLE_TERMS_KEYSPACE, DHT_KEYSPACE,
    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE, GROUP_KEYSPACE, HASH_PATHS_INDEX_KEYSPACE,
    JOB_FAMILY_ALIAS_KEYSPACE, JOB_FAMILY_CONFLICT_KEYSPACE, JOB_FAMILY_OUTBOX_KEYSPACE,
    JOB_FAMILY_PENDING_KEYSPACE, JOB_FAMILY_PROJECTION_KEYSPACE, JOB_FAMILY_RECORD_KEYSPACE,
    JOB_OUTPUT_RECORD_KEYSPACE, JOB_PLAN_EXPLAIN_KEYSPACE, JOB_RESERVATION_KEYSPACE,
    JOB_WITNESS_DEADLINE_INDEX_KEYSPACE, JOB_WITNESS_DEADLINE_KEYSPACE, MANAGED_COPY_KEYSPACE,
    NODE_STATE_KEYSPACE, NODE_SUBJECT_KEYSPACE, ONBOARDING_KEYSPACE,
    PLACEMENT_POLICY_CACHE_KEYSPACE, PLACEMENT_POLICY_KEYSPACE, REALM_CONFIG_KEYSPACE,
    S3_BUCKET_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE,
    S3_MULTIPART_UPLOAD_PART_KEYSPACE, SYNC_PLACEMENT_KEYSPACE, USER_ACCESS_KEYSPACE,
};
use aruna_core::onboarding::OnboardingSecretRecord;
use aruna_core::structs::{
    BlobHeadKey, BlobLocationKey, BlobVersion, BucketInfo, CurrentVersionPointer, Group,
    GroupAuthorizationDocument, HashPathIndexKey, JobFamilyId, JobRecordEnvelope, JobRecordKey,
    ManagedCopyKey, ManagedCopyRecord, MultipartObjectMetadataKey, MultipartObjectPart,
    MultipartObjectSummary, MultipartUpload, MultipartUploadPart, MultipartUploadPartKey,
    NodeSubjectRecord, POLICY_BULK_INTENT_KEYSPACE, POLICY_BULK_RUN_KEYSPACE,
    POLICY_MUTATION_KEYSPACE, PlacementPolicyDocument, PolicyBulkIntent, PolicyBulkIntentKey,
    PolicyBulkRun, PolicyMutationRecord, RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    UserAccess, VersionKey,
};
use aruna_net::dht::storage::StoredEntry;
use aruna_operations::jobs::lifecycle::witness::{WitnessDeadline, WitnessExplain};
use aruna_operations::jobs::records::keys::alias_family;
use aruna_operations::jobs::records::rows::{
    ConflictRecord, OutboxEntry, PendingNeed, PendingRecord, ProjectionCache,
};
use aruna_operations::placement::policy::PolicyCacheEntry;
use chrono::{DateTime, Utc};
use craqle::{
    ActorId as CraqleActorId, Dot as CraqleDot, GraphPolicy as CraqleGraphPolicy,
    VectorClock as CraqleVectorClock,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use ulid::Ulid;

use super::present::{
    DecodedField, DecodedValue, EntryOutput, JsonCraqleClockEntry, JsonCraqleDot,
    JsonCraqleGraphKey, JsonCraqleGraphMeta, JsonCraqleGraphPolicy, JsonCraqleLogKey,
    JsonCraqleQuadKey, JsonCraqleStoredBatch, JsonCraqleStoredBatchOp, JsonCraqleVectorClock,
    JsonGroup, JsonJobRecordEnvelope, JsonJobRecordKey, JsonJobReservation,
    JsonPendingDocumentPlacement, JsonPersistedNodeState, JsonPlacementPolicyDocument,
    JsonPolicyCacheEntry, JsonRealmAuthorizationDocument, JsonRealmConfigDocument, JsonStoredEntry,
    JsonUserAccess, family_id_string,
};

const CRAQLE_DOT_ENCODING_TAG: u8 = b'D';
const CRAQLE_BATCH_LOG_ENCODING_TAG: u8 = b'B';
const CRAQLE_GRAPH_META_PREFIX: u8 = b'M';
const CRAQLE_GRAPH_DIRTY_PREFIX: u8 = b'D';
const CRAQLE_GRAPH_REINDEX_PREFIX: u8 = b'R';
const CRAQLE_LOG_HEAD_PREFIX: u8 = b'H';
const CRAQLE_LOG_BATCH_PREFIX: u8 = b'B';
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct CraqleTermId(pub(super) u128);

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(super) struct CraqleStoredGraphMeta {
    pub(super) policy: CraqleGraphPolicy,
    pub(super) clock: CraqleVectorClock,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(super) enum CraqleStoredQuadOp {
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
pub(super) struct CraqleStoredBatch {
    pub(super) actor: CraqleActorId,
    pub(super) counter: u64,
    pub(super) base_clock: CraqleVectorClock,
    pub(super) ops: Vec<CraqleStoredQuadOp>,
    pub(super) timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CraqleQuadKeyParts {
    pub(super) graph: CraqleTermId,
    pub(super) subject: CraqleTermId,
    pub(super) predicate: CraqleTermId,
    pub(super) object: CraqleTermId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CraqleGraphKeyParts {
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
pub(super) enum CraqleLogKeyParts {
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
pub(super) fn decode_entry(keyspace_name: &str, key: &[u8], value: &[u8]) -> EntryOutput {
    EntryOutput {
        key: decode_key(keyspace_name, key),
        value: decode_value(keyspace_name, key, value),
    }
}

fn decode_key(keyspace_name: &str, key: &[u8]) -> DecodedField {
    match keyspace_name {
        GROUP_KEYSPACE | AUTH_KEYSPACE => decode_ulid_key(key),
        REALM_CONFIG_KEYSPACE => decode_realm_id(key),
        CRAQLE_TERMS_KEYSPACE => decode_term_id(key, "craqle term key")
            .map(|value| DecodedField::CraqleTermId {
                value: term_id_string(value),
            })
            .unwrap_or_else(|_| raw_field(key)),
        CRAQLE_QUADS_KEYSPACE => decode_quad_key(key)
            .map(|value| DecodedField::CraqleQuadKey {
                value: json_quad_key(value),
            })
            .unwrap_or_else(|_| raw_field(key)),
        CRAQLE_GRAPHS_KEYSPACE => decode_graph_key(key)
            .map(|value| DecodedField::CraqleGraphKey {
                value: json_graph_key(value),
            })
            .unwrap_or_else(|_| raw_field(key)),
        CRAQLE_LOG_KEYSPACE => decode_log_key(key)
            .map(|value| DecodedField::CraqleLogKey {
                value: json_log_key(value),
            })
            .unwrap_or_else(|_| raw_field(key)),
        USER_ACCESS_KEYSPACE
        | S3_BUCKET_KEYSPACE
        | API_STATE_KEYSPACE
        | DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE
        | NODE_STATE_KEYSPACE
        | NODE_SUBJECT_KEYSPACE
        | COMPUTE_DEPARTURE_KEYSPACE
        | ONBOARDING_KEYSPACE => decode_utf8_key(key),
        JOB_FAMILY_RECORD_KEYSPACE | JOB_FAMILY_PENDING_KEYSPACE | JOB_FAMILY_OUTBOX_KEYSPACE => {
            decode_record_key(key)
        }
        JOB_FAMILY_CONFLICT_KEYSPACE => decode_conflict_key(key),
        JOB_FAMILY_ALIAS_KEYSPACE => decode_alias_key(key),
        JOB_FAMILY_PROJECTION_KEYSPACE => decode_family_key(key),
        JOB_WITNESS_DEADLINE_KEYSPACE => decode_deadline_key(key),
        JOB_WITNESS_DEADLINE_INDEX_KEYSPACE => decode_family_key(key),
        JOB_PLAN_EXPLAIN_KEYSPACE => decode_explain_key(key),
        JOB_RESERVATION_KEYSPACE => decode_ulid_key(key),
        PLACEMENT_POLICY_KEYSPACE | POLICY_MUTATION_KEYSPACE | POLICY_BULK_RUN_KEYSPACE => {
            decode_policy_id(keyspace_name, key)
        }
        PLACEMENT_POLICY_CACHE_KEYSPACE => decode_policy_cache(key),
        POLICY_BULK_INTENT_KEYSPACE => PolicyBulkIntentKey::from_bytes(key)
            .map(|value| DecodedField::PolicyBulkIntentKey {
                operation_id: value.operation_id.to_string(),
                key: value.key,
            })
            .unwrap_or_else(|_| raw_field(key)),
        JOB_OUTPUT_RECORD_KEYSPACE => decode_attempt_key(key),
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
        MANAGED_COPY_KEYSPACE => ManagedCopyKey::from_bytes(key)
            .map(|value| DecodedField::ManagedCopyKey { value })
            .unwrap_or_else(|_| raw_field(key)),
        BLOB_LOCATIONS_KEYSPACE => BlobLocationKey::from_bytes(key)
            .map(|value| DecodedField::BlobLocationKey {
                blake3: hex::encode(value.blake3_hash),
                backend: value.backend.to_string(),
            })
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
        MANAGED_COPY_KEYSPACE => decode_value_with(value, ManagedCopyRecord::from_bytes, |data| {
            DecodedValue::ManagedCopyRecord { data }
        }),
        NODE_SUBJECT_KEYSPACE => decode_value_with(value, NodeSubjectRecord::from_bytes, |data| {
            DecodedValue::NodeSubjectRecord { data }
        }),
        JOB_OUTPUT_RECORD_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<JobRecordEnvelope>(bytes),
            |data| DecodedValue::JobOutputRecord {
                data: JsonJobRecordEnvelope(data),
            },
        ),
        JOB_FAMILY_RECORD_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<JobRecordEnvelope>(bytes),
            |data| DecodedValue::JobFamilyRecord {
                data: JsonJobRecordEnvelope(data),
            },
        ),
        JOB_FAMILY_PENDING_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<PendingRecord>(bytes),
            |data| DecodedValue::JobPendingRecord {
                envelope: JsonJobRecordEnvelope(data.envelope),
                need: pending_need_string(data.need),
                first_seen_ms: data.first_seen_ms,
                attempts: data.attempts,
            },
        ),
        JOB_FAMILY_CONFLICT_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<ConflictRecord>(bytes),
            |data| DecodedValue::JobConflictRecord {
                envelope: JsonJobRecordEnvelope(data.envelope),
                retained: hex::encode(data.retained),
                observed_at_ms: data.observed_at_ms,
                relayed_by: data.relayed_by.map(|node| node.to_string()),
            },
        ),
        JOB_FAMILY_ALIAS_KEYSPACE => decode_value_with(value, JobRecordKey::from_bytes, |data| {
            DecodedValue::JobAliasTarget {
                data: JsonJobRecordKey(data),
            }
        }),
        JOB_FAMILY_PROJECTION_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<ProjectionCache>(bytes),
            |data| DecodedValue::JobProjectionCache {
                revision: data.revision,
                stale: data.stale,
                projected: data.projection.is_some(),
            },
        ),
        JOB_FAMILY_OUTBOX_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<OutboxEntry>(bytes),
            |data| DecodedValue::JobOutboxEntry { data },
        ),
        JOB_RESERVATION_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<JobReservationRecord>(bytes),
            |data| DecodedValue::JobReservation {
                data: JsonJobReservation(data),
            },
        ),
        JOB_WITNESS_DEADLINE_INDEX_KEYSPACE | JOB_WITNESS_DEADLINE_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<WitnessDeadline>(bytes),
            |data| DecodedValue::JobWitnessDeadline { data },
        ),
        JOB_PLAN_EXPLAIN_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<WitnessExplain>(bytes),
            |data| DecodedValue::JobPlanExplain {
                sequence: data.sequence,
                selected: data
                    .plan
                    .selected
                    .as_ref()
                    .map(|selection| selection.target.node_id.to_string()),
                alternatives: data.plan.alternatives.len(),
                rejected: data.plan.rejected.len(),
                overlapping: data.overlapping,
                stored_at_ms: data.stored_at_ms,
            },
        ),
        COMPUTE_DEPARTURE_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<ComputeDepartureReport>(bytes),
            |data| DecodedValue::ComputeDepartureReport { data },
        ),
        PLACEMENT_POLICY_KEYSPACE => {
            decode_value_with(value, PlacementPolicyDocument::from_bytes, |data| {
                DecodedValue::PlacementPolicyDocument {
                    data: JsonPlacementPolicyDocument(data),
                }
            })
        }
        PLACEMENT_POLICY_CACHE_KEYSPACE => {
            decode_value_with(value, PolicyCacheEntry::from_bytes, |data| {
                DecodedValue::PolicyCacheEntry {
                    data: JsonPolicyCacheEntry(data),
                }
            })
        }
        POLICY_MUTATION_KEYSPACE => {
            decode_value_with(value, PolicyMutationRecord::from_bytes, |data| {
                DecodedValue::PolicyMutationRecord { data }
            })
        }
        POLICY_BULK_RUN_KEYSPACE => decode_value_with(value, PolicyBulkRun::from_bytes, |data| {
            DecodedValue::PolicyBulkRun { data }
        }),
        POLICY_BULK_INTENT_KEYSPACE => {
            decode_value_with(value, PolicyBulkIntent::from_bytes, |data| {
                DecodedValue::PolicyBulkIntent { data }
            })
        }
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
        S3_MULTIPART_OBJECT_METADATA_KEYSPACE => decode_object_metadata(key, value),
        AUTH_KEYSPACE => decode_auth_value(value),
        API_STATE_KEYSPACE => decode_api_state(key, value),
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
            aruna_operations::sync::shard_placement::decode_placement,
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
        CRAQLE_GRAPHS_KEYSPACE => decode_graph_value(key, value),
        CRAQLE_LOG_KEYSPACE => decode_log_value(key, value),
        DHT_KEYSPACE => decode_value_with(value, decode_dht_entries, |data| {
            DecodedValue::DhtEntries { data }
        }),
        _ => raw_value(value, None),
    }
}

/// Policy, mutation and bulk-run rows are all keyed by a postcard-encoded id;
/// the placement-policy document keyspace stores the raw ulid bytes instead.
fn decode_policy_id(keyspace_name: &str, key: &[u8]) -> DecodedField {
    if keyspace_name == PLACEMENT_POLICY_KEYSPACE {
        return decode_ulid_key(key);
    }
    postcard::from_bytes::<Ulid>(key)
        .map(|value| DecodedField::Ulid {
            value: value.to_string(),
        })
        .unwrap_or_else(|_| raw_field(key))
}

fn decode_policy_cache(key: &[u8]) -> DecodedField {
    let Some((policy_id, digest)) = key.split_at_checked(16) else {
        return raw_field(key);
    };
    let Ok(policy_id) = <[u8; 16]>::try_from(policy_id) else {
        return raw_field(key);
    };
    if digest.len() != 32 {
        return raw_field(key);
    }
    DecodedField::PolicyCacheKey {
        policy_id: Ulid::from_bytes(policy_id).to_string(),
        digest: hex::encode(digest),
    }
}

fn decode_record_key(key: &[u8]) -> DecodedField {
    JobRecordKey::from_bytes(key)
        .map(|value| DecodedField::JobRecordKey {
            value: JsonJobRecordKey(value),
        })
        .unwrap_or_else(|_| raw_field(key))
}

/// Conflict rows carry the refused digest after the record key, so both bytes
/// under one key stay addressable.
fn decode_conflict_key(key: &[u8]) -> DecodedField {
    let Some((record, digest)) = key.split_at_checked(key.len().saturating_sub(32)) else {
        return raw_field(key);
    };
    match JobRecordKey::from_bytes(record) {
        Ok(record) => DecodedField::JobConflictKey {
            record: JsonJobRecordKey(record),
            digest: hex::encode(digest),
        },
        Err(_) => raw_field(key),
    }
}

/// Alias rows are keyed by `job id || family`, so two families claiming one id
/// both stay visible.
fn decode_alias_key(key: &[u8]) -> DecodedField {
    let (Some(job_id), Some(family)) = (
        key.get(..16)
            .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok()),
        alias_family(key),
    ) else {
        return raw_field(key);
    };
    DecodedField::JobAliasKey {
        job_id: Ulid::from_bytes(job_id).to_string(),
        family: family_id_string(&family),
    }
}

fn decode_family_key(key: &[u8]) -> DecodedField {
    match family_from_key(key) {
        Some(family) => DecodedField::JobFamilyKey {
            family: family_id_string(&family),
        },
        None => raw_field(key),
    }
}

fn decode_deadline_key(key: &[u8]) -> DecodedField {
    if key.len() != 72 {
        return raw_field(key);
    }
    match key.get(8..).and_then(family_from_key) {
        Some(family) => DecodedField::JobFamilyKey {
            family: family_id_string(&family),
        },
        None => raw_field(key),
    }
}

/// Explain rows are keyed by `family || witness node`, one row per witness.
fn decode_explain_key(key: &[u8]) -> DecodedField {
    let (Some(family), Some(node)) = (family_from_key(key), key.get(64..)) else {
        return raw_field(key);
    };
    DecodedField::JobExplainKey {
        family: family_id_string(&family),
        node_id: hex::encode(node),
    }
}

fn family_from_key(key: &[u8]) -> Option<JobFamilyId> {
    Some(JobFamilyId {
        submission_id: aruna_core::structs::SubmissionId(key.get(..32)?.try_into().ok()?),
        request_digest: key.get(32..64)?.try_into().ok()?,
    })
}

fn pending_need_string(need: PendingNeed) -> String {
    match need {
        PendingNeed::Evidence(kind) => format!("evidence:{kind:?}"),
        PendingNeed::LocalView => "local_view".to_string(),
        PendingNeed::HolderView => "holder_view".to_string(),
    }
}

/// Attempt-scoped rows are keyed by `job id || attempt epoch` in big-endian.
fn decode_attempt_key(key: &[u8]) -> DecodedField {
    let Some((job_id, epoch)) = key.split_at_checked(key.len().saturating_sub(8)) else {
        return raw_field(key);
    };
    let (Ok(job_id), Ok(epoch)) = (<[u8; 16]>::try_from(job_id), <[u8; 8]>::try_from(epoch)) else {
        return raw_field(key);
    };
    DecodedField::AttemptKey {
        job_id: Ulid::from_bytes(job_id).to_string(),
        attempt_epoch: u64::from_be_bytes(epoch),
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

fn decode_api_state(key: &[u8], value: &[u8]) -> DecodedValue {
    match key {
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

fn decode_object_metadata(key: &[u8], value: &[u8]) -> DecodedValue {
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

fn decode_realm_id(key: &[u8]) -> DecodedField {
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
fn decode_term_id(bytes: &[u8], context: &'static str) -> Result<CraqleTermId, String> {
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

fn decode_quad_key(key: &[u8]) -> Result<CraqleQuadKeyParts, String> {
    if key.len() != 64 {
        return Err(format!(
            "invalid craqle quad key: expected 64 bytes, found {}",
            key.len()
        ));
    }
    Ok(CraqleQuadKeyParts {
        graph: decode_term_id(&key[0..16], "craqle quad graph")?,
        subject: decode_term_id(&key[16..32], "craqle quad subject")?,
        predicate: decode_term_id(&key[32..48], "craqle quad predicate")?,
        object: decode_term_id(&key[48..64], "craqle quad object")?,
    })
}

fn decode_graph_key(key: &[u8]) -> Result<CraqleGraphKeyParts, String> {
    match key.first().copied() {
        Some(CRAQLE_GRAPH_META_PREFIX) if key.len() == 17 => Ok(CraqleGraphKeyParts::Meta {
            graph: decode_term_id(&key[1..17], "craqle graph meta graph")?,
        }),
        Some(CRAQLE_GRAPH_DIRTY_PREFIX) if key.len() == 33 => Ok(CraqleGraphKeyParts::Dirty {
            graph: decode_term_id(&key[1..17], "craqle graph dirty graph")?,
            subject: decode_term_id(&key[17..33], "craqle graph dirty subject")?,
        }),
        Some(CRAQLE_GRAPH_REINDEX_PREFIX) if key.len() == 17 => Ok(CraqleGraphKeyParts::Reindex {
            graph: decode_term_id(&key[1..17], "craqle graph reindex graph")?,
        }),
        Some(prefix) => Err(format!(
            "invalid craqle graph key prefix `{}` with length {}",
            prefix as char,
            key.len()
        )),
        None => Err("invalid craqle graph key: empty key".to_string()),
    }
}

fn decode_log_key(key: &[u8]) -> Result<CraqleLogKeyParts, String> {
    match key.first().copied() {
        Some(CRAQLE_LOG_HEAD_PREFIX) if key.len() == 49 => Ok(CraqleLogKeyParts::Head {
            graph: decode_term_id(&key[1..17], "craqle log head graph")?,
            actor: CraqleActorId::from_bytes(
                key[17..49]
                    .try_into()
                    .map_err(|_| "invalid craqle log head actor".to_string())?,
            ),
        }),
        Some(CRAQLE_LOG_BATCH_PREFIX) if key.len() == 57 => Ok(CraqleLogKeyParts::Batch {
            graph: decode_term_id(&key[1..17], "craqle log batch graph")?,
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

fn decode_graph_value(key: &[u8], value: &[u8]) -> DecodedValue {
    match decode_graph_key(key) {
        Ok(CraqleGraphKeyParts::Meta { graph }) => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<CraqleStoredGraphMeta>(bytes),
            |data| DecodedValue::CraqleGraphMeta {
                data: json_graph_meta(graph, data),
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

fn decode_log_batch(key: &[u8], value: &[u8]) -> Result<JsonCraqleStoredBatch, String> {
    let CraqleLogKeyParts::Batch { graph, .. } = decode_log_key(key)? else {
        return Err("craqle log batch value requires a batch key".to_string());
    };
    if value.first().copied() != Some(CRAQLE_BATCH_LOG_ENCODING_TAG) {
        return Err("unsupported craqle log batch encoding".to_string());
    }
    let batch = postcard::from_bytes::<CraqleStoredBatch>(&value[1..])
        .map_err(|error| error.to_string())?;
    Ok(json_stored_batch(graph, batch))
}

fn decode_log_value(key: &[u8], value: &[u8]) -> DecodedValue {
    match decode_log_key(key) {
        Ok(CraqleLogKeyParts::Head { .. }) => decode_value_with(
            value,
            |bytes| decode_craqle_u64(bytes, "craqle log head"),
            |data| DecodedValue::CraqleLogHead { data },
        ),
        Ok(CraqleLogKeyParts::Batch { .. }) => decode_value_with(
            value,
            |bytes| decode_log_batch(key, bytes),
            |data| DecodedValue::CraqleLogBatch { data },
        ),
        Err(error) => raw_value(value, Some(error)),
    }
}

fn term_id_string(id: CraqleTermId) -> String {
    format!("{:032x}", id.0)
}

fn json_quad_key(parts: CraqleQuadKeyParts) -> JsonCraqleQuadKey {
    JsonCraqleQuadKey {
        graph: term_id_string(parts.graph),
        subject: term_id_string(parts.subject),
        predicate: term_id_string(parts.predicate),
        object: term_id_string(parts.object),
    }
}

fn json_graph_key(parts: CraqleGraphKeyParts) -> JsonCraqleGraphKey {
    match parts {
        CraqleGraphKeyParts::Meta { graph } => JsonCraqleGraphKey::Meta {
            graph: term_id_string(graph),
        },
        CraqleGraphKeyParts::Dirty { graph, subject } => JsonCraqleGraphKey::Dirty {
            graph: term_id_string(graph),
            subject: term_id_string(subject),
        },
        CraqleGraphKeyParts::Reindex { graph } => JsonCraqleGraphKey::Reindex {
            graph: term_id_string(graph),
        },
    }
}

fn json_log_key(parts: CraqleLogKeyParts) -> JsonCraqleLogKey {
    match parts {
        CraqleLogKeyParts::Head { graph, actor } => JsonCraqleLogKey::Head {
            graph: term_id_string(graph),
            actor: actor.to_string(),
        },
        CraqleLogKeyParts::Batch {
            graph,
            actor,
            counter,
        } => JsonCraqleLogKey::Batch {
            graph: term_id_string(graph),
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

fn json_vector_clock(clock: CraqleVectorClock) -> JsonCraqleVectorClock {
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

fn json_graph_policy(policy: CraqleGraphPolicy) -> JsonCraqleGraphPolicy {
    let mut permission_paths = policy.permission_paths;
    permission_paths.sort();
    permission_paths.dedup();
    JsonCraqleGraphPolicy {
        public: policy.public,
        permission_paths,
    }
}

fn json_graph_meta(graph: CraqleTermId, meta: CraqleStoredGraphMeta) -> JsonCraqleGraphMeta {
    JsonCraqleGraphMeta {
        graph: term_id_string(graph),
        policy: json_graph_policy(meta.policy),
        clock: json_vector_clock(meta.clock),
    }
}

fn json_stored_batch(graph: CraqleTermId, batch: CraqleStoredBatch) -> JsonCraqleStoredBatch {
    JsonCraqleStoredBatch {
        graph: term_id_string(graph),
        actor: batch.actor.to_string(),
        counter: batch.counter,
        base_clock: json_vector_clock(batch.base_clock),
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
                    subject: term_id_string(subject),
                    predicate: term_id_string(predicate),
                    object: term_id_string(object),
                    dot: json_craqle_dot(dot),
                },
                CraqleStoredQuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed,
                } => JsonCraqleStoredBatchOp::Remove {
                    subject: term_id_string(subject),
                    predicate: term_id_string(predicate),
                    object: term_id_string(object),
                    witnessed: json_vector_clock(witnessed),
                },
            })
            .collect(),
        timestamp: batch.timestamp,
    }
}
#[cfg(test)]
mod tests {
    use super::super::present::{
        DecodedField, DecodedValue, JsonPlacementPolicyDocument, JsonPolicyCacheEntry,
    };
    use super::super::present::{
        JsonCraqleGraphKey, JsonCraqleLogKey, JsonCraqleQuadKey, JsonCraqleStoredBatchOp,
    };
    use super::{
        CRAQLE_BATCH_LOG_ENCODING_TAG, CRAQLE_DOT_ENCODING_TAG, CRAQLE_GRAPH_META_PREFIX,
        CRAQLE_LOG_BATCH_PREFIX, CraqleStoredBatch, CraqleStoredGraphMeta, CraqleStoredQuadOp,
        CraqleTermId, decode_entry, raw_field,
    };
    use aruna::config::{
        BootOrigin, PersistedNodeIdentity, PersistedNodeState, PersistedNodeStatus,
    };
    use aruna_core::compute_quota::{ComputeDepartureReport, JobReservationRecord};
    use aruna_core::id::DhtKeyId;
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
        COMPUTE_DEPARTURE_KEYSPACE, CRAQLE_GRAPHS_KEYSPACE, CRAQLE_LOG_KEYSPACE,
        CRAQLE_QUADS_KEYSPACE, CRAQLE_TERMS_KEYSPACE, DHT_KEYSPACE, HASH_PATHS_INDEX_KEYSPACE,
        JOB_FAMILY_ALIAS_KEYSPACE, JOB_FAMILY_CONFLICT_KEYSPACE, JOB_FAMILY_OUTBOX_KEYSPACE,
        JOB_FAMILY_PENDING_KEYSPACE, JOB_FAMILY_PROJECTION_KEYSPACE, JOB_FAMILY_RECORD_KEYSPACE,
        JOB_OUTPUT_RECORD_KEYSPACE, JOB_PLAN_EXPLAIN_KEYSPACE, JOB_RESERVATION_KEYSPACE,
        JOB_WITNESS_DEADLINE_KEYSPACE, NODE_STATE_KEYSPACE, ONBOARDING_KEYSPACE,
        PLACEMENT_POLICY_CACHE_KEYSPACE, PLACEMENT_POLICY_KEYSPACE, REALM_CONFIG_KEYSPACE,
        S3_BUCKET_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE,
        S3_MULTIPART_UPLOAD_PART_KEYSPACE, SYNC_PLACEMENT_KEYSPACE,
    };
    use aruna_core::onboarding::{OnboardingMode, OnboardingPurpose, OnboardingSecretRecord};
    use aruna_core::structs::{
        Actor, BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion, BucketInfo,
        CurrentVersionPointer, HashPathIndexKey, JobFamilyId, JobRecordEnvelope,
        MultipartChecksumType, MultipartObjectMetadataKey, MultipartObjectPart,
        MultipartObjectSummary, MultipartUpload, MultipartUploadPart, MultipartUploadPartKey,
        MultipartUploadStatus, POLICY_BULK_INTENT_KEYSPACE, POLICY_BULK_RUN_KEYSPACE,
        POLICY_MUTATION_KEYSPACE, PlacementPolicy, PlacementPolicyDocument, PlacementPolicyRef,
        PolicyBulkIntent, PolicyBulkRun, PolicyBulkStatus, PolicyIntentOutcome,
        PolicyMutationParams, PolicyMutationRecord, PolicyPublication, PolicyRefMode,
        RealmConfigDocument, RealmId, placement_policy_key,
    };
    use aruna_net::dht::storage::StoredEntry;
    use aruna_operations::jobs::lifecycle::witness::{WitnessDeadline, WitnessExplain};
    use aruna_operations::jobs::records::rows::PROJECTION_CACHE_VERSION;
    use aruna_operations::jobs::records::rows::{ConflictRecord, PendingNeed, PendingRecord};
    use aruna_operations::jobs::records::rows::{OutboxEntry, ProjectionCache};
    use aruna_operations::placement::policy::PolicyCacheEntry;
    use chrono::{DateTime, Utc};
    use craqle::{
        ActorId as CraqleActorId, Dot as CraqleDot, GraphPolicy as CraqleGraphPolicy,
        VectorClock as CraqleVectorClock,
    };
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    #[test]
    fn decodes_bucket_record() {
        let realm_id = RealmId::from_bytes([4_u8; 32]);
        let info = BucketInfo {
            group_id: Ulid::from_bytes([7_u8; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::local(Ulid::from_bytes([8_u8; 16]), realm_id),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 7,
        };

        let decoded = decode_entry(
            S3_BUCKET_KEYSPACE,
            b"primary-bucket",
            &info.to_bytes().unwrap(),
        );
        assert_eq!(
            decoded.key,
            DecodedField::Utf8 {
                value: "primary-bucket".to_string()
            }
        );
        match decoded.value {
            DecodedValue::BucketInfo { data } => assert_eq!(data, info),
            other => panic!("expected bucket info, got {other:?}"),
        }
    }

    fn test_family() -> JobFamilyId {
        JobFamilyId {
            submission_id: aruna_core::structs::SubmissionId([9u8; 32]),
            request_digest: [8u8; 32],
        }
    }

    fn test_node() -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[6u8; 32]).public()
    }

    fn test_envelope() -> JobRecordEnvelope {
        let family = test_family();
        JobRecordEnvelope::sign(
            RealmId::from_bytes([2u8; 32]),
            aruna_core::structs::JobFamilyRecord::Claim(aruna_core::structs::SubmissionClaim {
                submission_id: family.submission_id,
                job_id: aruna_core::structs::JobId::from_bytes(
                    Ulid::from_bytes([5u8; 16]).to_bytes(),
                ),
                request_digest: family.request_digest,
                spec_digest: [7u8; 32],
                committing_node_id: test_node(),
                accepted_at_ms: 11,
            }),
            &iroh::SecretKey::from_bytes(&[6u8; 32]),
        )
        .unwrap()
    }

    #[test]
    fn decodes_family_rows() {
        // Every append-only family keyspace must decode: an operator inspecting
        // a stuck submission cannot be told the rows are opaque bytes.
        let envelope = test_envelope();
        let key = envelope.key();
        let key_bytes = key.to_bytes();

        let record = decode_entry(
            JOB_FAMILY_RECORD_KEYSPACE,
            &key_bytes,
            &postcard::to_allocvec(&envelope).unwrap(),
        );
        assert!(matches!(
            record.key,
            DecodedField::JobRecordKey { .. } // the signed identity, not raw bytes
        ));
        assert!(matches!(record.value, DecodedValue::JobFamilyRecord { .. }));

        let pending = PendingRecord {
            envelope: envelope.clone(),
            need: PendingNeed::Evidence(aruna_core::structs::JobRecordKind::Spec),
            first_seen_ms: 12,
            attempts: 2,
        };
        let decoded = decode_entry(
            JOB_FAMILY_PENDING_KEYSPACE,
            &key_bytes,
            &postcard::to_allocvec(&pending).unwrap(),
        );
        match decoded.value {
            DecodedValue::JobPendingRecord { need, attempts, .. } => {
                assert_eq!(need, "evidence:Spec");
                assert_eq!(attempts, 2);
            }
            other => panic!("expected a pending record, got {other:?}"),
        }

        let conflict = ConflictRecord {
            envelope: envelope.clone(),
            retained: [3u8; 32],
            observed_at_ms: 13,
            relayed_by: Some(test_node()),
        };
        let mut conflict_key = key_bytes.to_vec();
        conflict_key.extend_from_slice(&[3u8; 32]);
        let decoded = decode_entry(
            JOB_FAMILY_CONFLICT_KEYSPACE,
            &conflict_key,
            &postcard::to_allocvec(&conflict).unwrap(),
        );
        assert!(matches!(decoded.key, DecodedField::JobConflictKey { .. }));
        assert!(matches!(
            decoded.value,
            DecodedValue::JobConflictRecord { .. }
        ));

        let mut alias_key = Ulid::from_bytes([5u8; 16]).to_bytes().to_vec();
        alias_key.extend_from_slice(&test_family().to_bytes());
        let decoded = decode_entry(JOB_FAMILY_ALIAS_KEYSPACE, &alias_key, &key_bytes);
        assert!(matches!(decoded.key, DecodedField::JobAliasKey { .. }));
        assert!(matches!(decoded.value, DecodedValue::JobAliasTarget { .. }));

        let cache = ProjectionCache {
            version: PROJECTION_CACHE_VERSION,
            revision: 4,
            stale: true,
            projection: None,
        };
        let decoded = decode_entry(
            JOB_FAMILY_PROJECTION_KEYSPACE,
            &test_family().to_bytes(),
            &postcard::to_allocvec(&cache).unwrap(),
        );
        assert!(matches!(decoded.key, DecodedField::JobFamilyKey { .. }));
        assert_eq!(
            decoded.value,
            DecodedValue::JobProjectionCache {
                revision: 4,
                stale: true,
                projected: false,
            }
        );

        let outbox = OutboxEntry {
            queued_at_ms: 14,
            delivered: Vec::new(),
            next_holder: 0,
            rejections: 0,
        };
        let decoded = decode_entry(
            JOB_FAMILY_OUTBOX_KEYSPACE,
            &key_bytes,
            &postcard::to_allocvec(&outbox).unwrap(),
        );
        assert_eq!(decoded.value, DecodedValue::JobOutboxEntry { data: outbox });
    }

    #[test]
    fn decodes_compute_rows() {
        // Reservations, witness deadlines, plan explains, and the departure row
        // are the operator's only local evidence about scheduled work.
        let execution_id = Ulid::from_bytes([1u8; 16]);
        let reservation = JobReservationRecord {
            execution_id,
            job_id: aruna_core::structs::JobId::from_bytes(Ulid::from_bytes([5u8; 16]).to_bytes()),
            logical_job_id: aruna_core::structs::JobId::from_bytes([5u8; 16]),
            resources: aruna_core::structs::EffectiveResources {
                cpu_cores: 2,
                ram_bytes: 1024,
                disk_bytes: 2048,
                max_walltime_ms: 60_000,
                preemptible: false,
            },
            created_at_ms: 21,
            subject_generation: 3,
            subject_digest: [9u8; 32],
        };
        let decoded = decode_entry(
            JOB_RESERVATION_KEYSPACE,
            &execution_id.to_bytes(),
            &postcard::to_allocvec(&reservation).unwrap(),
        );
        assert_eq!(
            decoded.key,
            DecodedField::Ulid {
                value: execution_id.to_string()
            }
        );
        assert!(matches!(decoded.value, DecodedValue::JobReservation { .. }));

        let deadline = WitnessDeadline {
            due_at_ms: 22,
            rank: 1,
        };
        let mut deadline_key = 22u64.to_be_bytes().to_vec();
        deadline_key.extend_from_slice(&test_family().to_bytes());
        let decoded = decode_entry(
            JOB_WITNESS_DEADLINE_KEYSPACE,
            &deadline_key,
            &postcard::to_allocvec(&deadline).unwrap(),
        );
        assert!(matches!(decoded.key, DecodedField::JobFamilyKey { .. }));
        assert_eq!(
            decoded.value,
            DecodedValue::JobWitnessDeadline { data: deadline }
        );

        let explain = WitnessExplain {
            sequence: 3,
            plan: aruna_core::scheduling::ExecutionPlan {
                selected: None,
                retryable: true,
                alternatives: Vec::new(),
                rejected: Vec::new(),
                omitted: 0,
            },
            declined: Vec::new(),
            overlapping: false,
            stored_at_ms: 23,
        };
        let mut explain_key = test_family().to_bytes().to_vec();
        explain_key.extend_from_slice(test_node().as_bytes());
        let decoded = decode_entry(
            JOB_PLAN_EXPLAIN_KEYSPACE,
            &explain_key,
            &postcard::to_allocvec(&explain).unwrap(),
        );
        assert!(matches!(decoded.key, DecodedField::JobExplainKey { .. }));
        assert_eq!(
            decoded.value,
            DecodedValue::JobPlanExplain {
                sequence: 3,
                selected: None,
                alternatives: 0,
                rejected: 0,
                overlapping: false,
                stored_at_ms: 23,
            }
        );

        let report = ComputeDepartureReport {
            departed_at_ms: 24,
            membership_generation: 5,
            unresolved: vec![execution_id],
            truncated: false,
        };
        let decoded = decode_entry(
            COMPUTE_DEPARTURE_KEYSPACE,
            b"departure",
            &postcard::to_allocvec(&report).unwrap(),
        );
        assert_eq!(
            decoded.key,
            DecodedField::Utf8 {
                value: "departure".to_string()
            }
        );
        assert_eq!(
            decoded.value,
            DecodedValue::ComputeDepartureReport { data: report }
        );
    }

    #[test]
    fn decodes_output_key() {
        // Output records share the attempt-control key, so the epoch has to
        // survive decoding for a stuck attempt to be found at all.
        let job_id = Ulid::from_bytes([3_u8; 16]);
        let mut key = job_id.to_bytes().to_vec();
        key.extend_from_slice(&7_u64.to_be_bytes());

        let decoded = decode_entry(JOB_OUTPUT_RECORD_KEYSPACE, &key, b"not-an-envelope");

        assert_eq!(
            decoded.key,
            DecodedField::AttemptKey {
                job_id: job_id.to_string(),
                attempt_epoch: 7,
            }
        );
        assert!(matches!(
            decoded.value,
            DecodedValue::Raw {
                decode_error: Some(_),
                ..
            }
        ));
    }

    fn policy_document(policy_id: Ulid, realm_id: RealmId) -> PlacementPolicyDocument {
        let secret = iroh::SecretKey::from_bytes(&[11_u8; 32]);
        PlacementPolicyDocument {
            realm_id,
            policy: PlacementPolicy {
                policy_id,
                name: "eu-only".to_string(),
                owner_group_id: None,
                allowed: Vec::new(),
            },
            publication: PolicyPublication {
                publisher: secret.public(),
                created_by: aruna_core::UserId::local(Ulid::from_bytes([15_u8; 16]), realm_id),
                created_at_ms: 42,
                event_id: Ulid::from_bytes([12_u8; 16]),
                config_digest: [13_u8; 32],
                signature: secret.sign(b"publication"),
            },
        }
    }

    #[test]
    fn decodes_policy_document() {
        let realm_id = RealmId::from_bytes([10_u8; 32]);
        let policy_id = Ulid::from_bytes([14_u8; 16]);
        let document = policy_document(policy_id, realm_id);

        let decoded = decode_entry(
            PLACEMENT_POLICY_KEYSPACE,
            &placement_policy_key(policy_id),
            &document.to_bytes().unwrap(),
        );

        assert_eq!(
            decoded.key,
            DecodedField::Ulid {
                value: policy_id.to_string()
            }
        );
        assert_eq!(
            decoded.value,
            DecodedValue::PlacementPolicyDocument {
                data: JsonPlacementPolicyDocument(document)
            }
        );
    }

    #[test]
    fn decodes_policy_cache() {
        let policy_id = Ulid::from_bytes([15_u8; 16]);
        let policy_ref = PlacementPolicyRef {
            policy_id,
            digest: [16_u8; 32],
        };
        let entry = PolicyCacheEntry::Unavailable {
            stored_at_ms: 5,
            expires_at_ms: 15,
        };
        let mut key = policy_id.to_bytes().to_vec();
        key.extend_from_slice(&policy_ref.digest);

        let decoded = decode_entry(
            PLACEMENT_POLICY_CACHE_KEYSPACE,
            &key,
            &entry.to_bytes().unwrap(),
        );

        assert_eq!(
            decoded.key,
            DecodedField::PolicyCacheKey {
                policy_id: policy_id.to_string(),
                digest: hex::encode([16_u8; 32]),
            }
        );
        assert_eq!(
            decoded.value,
            DecodedValue::PolicyCacheEntry {
                data: JsonPolicyCacheEntry(entry)
            }
        );
    }

    #[test]
    fn decodes_policy_mutation() {
        let realm_id = RealmId::from_bytes([17_u8; 32]);
        let mutation_id = Ulid::from_bytes([18_u8; 16]);
        let record = PolicyMutationRecord {
            mutation_id,
            params: PolicyMutationParams {
                bucket: "bucket".to_string(),
                key: "a.tar".to_string(),
                expected_head: CurrentVersionPointer::new(Ulid::from_bytes([19_u8; 16])),
                bucket_identity: (
                    Ulid::from_bytes([20_u8; 16]),
                    std::time::SystemTime::UNIX_EPOCH,
                    aruna_core::UserId::local(Ulid::from_bytes([22_u8; 16]), realm_id),
                ),
                target_refs: Vec::new(),
                mode: PolicyRefMode::Union,
            },
            successor_version_id: Ulid::from_bytes([21_u8; 16]),
            effective_refs: Vec::new(),
            materialized: true,
        };

        let decoded = decode_entry(
            POLICY_MUTATION_KEYSPACE,
            &PolicyMutationRecord::key(mutation_id).unwrap(),
            &record.to_bytes().unwrap(),
        );

        assert_eq!(
            decoded.key,
            DecodedField::Ulid {
                value: mutation_id.to_string()
            }
        );
        assert_eq!(
            decoded.value,
            DecodedValue::PolicyMutationRecord { data: record }
        );
    }

    #[test]
    fn decodes_bulk_run() {
        let realm_id = RealmId::from_bytes([22_u8; 32]);
        let operation_id = Ulid::from_bytes([23_u8; 16]);
        let run = PolicyBulkRun {
            operation_id,
            bucket: "bucket".to_string(),
            bucket_identity: (
                Ulid::from_bytes([24_u8; 16]),
                std::time::SystemTime::UNIX_EPOCH,
                aruna_core::UserId::local(Ulid::from_bytes([25_u8; 16]), realm_id),
            ),
            generation: 3,
            target_refs: Vec::new(),
            status: PolicyBulkStatus::Active,
        };

        let decoded = decode_entry(
            POLICY_BULK_RUN_KEYSPACE,
            &PolicyBulkRun::key(operation_id).unwrap(),
            &run.to_bytes().unwrap(),
        );

        assert_eq!(
            decoded.key,
            DecodedField::Ulid {
                value: operation_id.to_string()
            }
        );
        assert_eq!(decoded.value, DecodedValue::PolicyBulkRun { data: run });
    }

    #[test]
    fn decodes_bulk_intent() {
        let operation_id = Ulid::from_bytes([25_u8; 16]);
        let intent = PolicyBulkIntent {
            operation_id,
            key: "a.tar".to_string(),
            observed_head: CurrentVersionPointer::new(Ulid::from_bytes([26_u8; 16])),
            successor_version_id: Ulid::from_bytes([27_u8; 16]),
            outcome: PolicyIntentOutcome::Planned,
        };

        let decoded = decode_entry(
            POLICY_BULK_INTENT_KEYSPACE,
            &intent.key().to_bytes().unwrap(),
            &intent.to_bytes().unwrap(),
        );

        assert_eq!(
            decoded.key,
            DecodedField::PolicyBulkIntentKey {
                operation_id: operation_id.to_string(),
                key: "a.tar".to_string(),
            }
        );
        assert_eq!(
            decoded.value,
            DecodedValue::PolicyBulkIntent { data: intent }
        );
    }
    #[test]
    fn unknown_keyspace_raw() {
        let entry = decode_entry("unknown", b"\x01\x02", b"\x03\x04");
        assert_eq!(entry.key, raw_field(b"\x01\x02"));
        match entry.value {
            DecodedValue::Raw { hex, .. } => assert_eq!(hex, "0304"),
            other => panic!("expected raw fallback, got {other:?}"),
        }
    }

    #[test]
    fn decodes_realm_key() {
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
    fn decodes_realm_config() {
        let realm_id = RealmId::from_bytes([1_u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[3_u8; 32]).public(),
            user_id: aruna_core::UserId::local(Ulid::from_bytes([4_u8; 16]), realm_id),
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
    fn decodes_onboarding_secret() {
        let record = OnboardingSecretRecord {
            enrollment_id: Ulid::from_bytes([6_u8; 16]),
            secret_hash: "hash123".to_string(),
            mode: OnboardingMode::Server,
            purpose: OnboardingPurpose::NodeEnrollment,
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
    fn decodes_node_state() {
        let realm_id = RealmId::from_bytes([4_u8; 32]);
        let state = PersistedNodeState {
            boot_origin: BootOrigin::Onboarded,
            status: PersistedNodeStatus::PendingOnboarding,
            realm_id,
            net_secret_key: [11_u8; 32],
            onboarding_phase: None,
            onboarding_sync_ticket: Some("ticket".to_string()),
            identity: PersistedNodeIdentity::User {
                owner: aruna_core::UserId::nil(realm_id),
            },
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
    fn decodes_pending_placement() {
        let realm_id = RealmId::from_bytes([4_u8; 32]);
        let placement_ref = aruna_core::structs::PlacementRef {
            strategy_id: ulid::Ulid::from_bytes([9_u8; 16]),
            shard: 5,
        };
        let selected_peer = iroh::SecretKey::from_bytes(&[7_u8; 32]).public();
        let authoritative_node_id = iroh::SecretKey::from_bytes(&[6_u8; 32]).public();
        let placement = aruna_operations::sync::shard_placement::new_placement(
            realm_id,
            placement_ref,
            authoritative_node_id,
            vec![selected_peer],
        );
        let value = postcard::to_allocvec(&placement).unwrap();
        let key = aruna_operations::sync::shard_placement::placement_key(realm_id, &placement_ref);

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
                assert_eq!(data.0.placement, placement_ref);
                assert_eq!(data.0.authoritative_node_id, authoritative_node_id);
                assert_eq!(data.0.selected_peers, vec![selected_peer]);
            }
            other => panic!("expected pending topic placement, got {other:?}"),
        }
    }

    #[test]
    fn decodes_dht_entry() {
        let key = DhtKeyId::from_bytes([6_u8; 32]);
        let realm_id = RealmId::from_bytes([7_u8; 32]);
        let publisher_secret = iroh::SecretKey::from_bytes(&[5_u8; 32]);
        let publisher = publisher_secret.public();
        let revision = 1;
        let signed = aruna_net::dht::rpc::signed_record_bytes(
            &key,
            &publisher,
            &realm_id,
            &[1, 2, 3, 4],
            42,
            revision,
        );
        let entries = vec![StoredEntry {
            publisher,
            realm_id,
            value: vec![1, 2, 3, 4],
            expires_at: 42,
            revision,
            signature: publisher_secret.sign(&signed),
            retain_until: 42,
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
    fn decodes_upload_entry() {
        let realm_id = RealmId::from_bytes([3_u8; 32]);
        let created_by = aruna_core::UserId::local(Ulid::from_bytes([10_u8; 16]), realm_id);
        let upload = MultipartUpload {
            backend: BackendRef::node_default(),
            storage_class: None,
            upload_id: Ulid::from_bytes([7_u8; 16]),
            bucket: "bucket-a".to_string(),
            key: "parts/big.bin".to_string(),
            group_id: Ulid::from_bytes([8_u8; 16]),
            created_by,
            created_at: SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
            metadata: HashMap::new(),
            placement_policies: Vec::new(),
            subject_generation: 0,
            completing_since_ms: None,
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
    fn decodes_upload_part() {
        let realm_id = RealmId::from_bytes([9_u8; 32]);
        let created_by = aruna_core::UserId::local(Ulid::from_bytes([11_u8; 16]), realm_id);
        let key = MultipartUploadPartKey::new(Ulid::from_bytes([2_u8; 16]), 5);
        let part = MultipartUploadPart {
            part_number: 5,
            location: BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
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
    fn decodes_object_summary() {
        let key = MultipartObjectMetadataKey::summary(Ulid::from_bytes([1_u8; 16]));
        let summary = MultipartObjectSummary {
            checksum_type: MultipartChecksumType::Composite,
            part_count: 3,
            composite_hashes: Default::default(),
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
    fn decodes_object_part() {
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
    fn invalid_metadata_raw() {
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
    fn decodes_term_entry() {
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
    fn decodes_quad_entry() {
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
                value: JsonCraqleQuadKey {
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
    fn decodes_graph_meta() {
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
                JsonCraqleGraphKey::Meta {
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
    fn decodes_log_batch() {
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
                subject: CraqleTermId(subject),
                predicate: CraqleTermId(predicate),
                object: CraqleTermId(object),
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
                JsonCraqleLogKey::Batch {
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
                    JsonCraqleStoredBatchOp::Add {
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
    fn invalid_node_raw() {
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
    fn decodes_blob_keyspaces() {
        let realm_id = RealmId::from_bytes([1_u8; 32]);
        let group_id = Ulid::from_bytes([2_u8; 16]);
        let node_id = iroh::SecretKey::from_bytes(&[3_u8; 32]).public();
        let created_by = aruna_core::UserId::local(Ulid::from_bytes([6_u8; 16]), realm_id);
        let head_key = BlobHeadKey::new("bucket", "path/file.txt");
        let head_value = aruna_core::structs::CurrentVersionPointer::new_with_generation(
            Ulid::from_bytes([4_u8; 16]),
            7,
        )
        .to_bytes()
        .unwrap();
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
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
            BackendRef::node_default(),
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

        let location_key = BlobLocationKey::new([9_u8; 32], location.backend.clone());
        let decoded_location = decode_entry(
            BLOB_LOCATIONS_KEYSPACE,
            &location_key.to_bytes(),
            &location.to_bytes().unwrap(),
        );
        assert_eq!(
            decoded_location.key,
            DecodedField::BlobLocationKey {
                blake3: hex::encode([9_u8; 32]),
                backend: location.backend.to_string(),
            }
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
