//! Decodes raw key and value bytes per keyspace into the presentation records.
//! Unknown or malformed rows fall back to a raw hex record instead of failing the listing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::auth::{ADMIN_CLAIMED_KEY, REALMS_LIST_KEY};
use aruna_core::compute::quota::{ComputeDepartureReport, JobReservationRecord};
use aruna_core::dht_protocol::StoredEntry;
use aruna_core::id::DhtKeyId;
use aruna_core::keyspaces::{
    API_STATE_KEYSPACE, APPLIED_OPS_KEYSPACE, AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE,
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, COMPUTE_DEPARTURE_KEYSPACE,
    CRAQLE_GRAPHS_KEYSPACE, CRAQLE_LOG_KEYSPACE, CRAQLE_QUADS_KEYSPACE, CRAQLE_TERMS_KEYSPACE,
    DEADLINE_INDEX_KEYSPACE, DHT_KEYSPACE, FAMILY_ALIAS_KEYSPACE, FAMILY_CONFLICT_KEYSPACE,
    FAMILY_OUTBOX_KEYSPACE, FAMILY_PENDING_KEYSPACE, FAMILY_PROJECTION_KEYSPACE,
    FAMILY_RECORD_KEYSPACE, GROUP_KEYSPACE, JOB_RESERVATION_KEYSPACE, MANAGED_COPY_KEYSPACE,
    NODE_STATE_KEYSPACE, NODE_SUBJECT_KEYSPACE, OBJECT_METADATA_KEYSPACE, ONBOARDING_KEYSPACE,
    OUTPUT_RECORD_KEYSPACE, PATHS_INDEX_KEYSPACE, PLACEMENT_POLICY_KEYSPACE, PLAN_EXPLAIN_KEYSPACE,
    POLICY_CACHE_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, SYNC_PLACEMENT_KEYSPACE,
    UPLOAD_KEYSPACE, UPLOAD_PART_KEYSPACE, USER_ACCESS_KEYSPACE, WITNESS_DEADLINE_KEYSPACE,
};
use aruna_core::onboarding::OnboardingSecretRecord;
use aruna_core::structs::execution::job::family::{JobFamilyId, JobRecordEnvelope, JobRecordKey};
use aruna_core::structs::execution::job::record_keys::alias_family;
use aruna_core::structs::execution::job::record_rows::{
    ConflictRecord, OutboxEntry, PendingNeed, PendingRecord, ProjectionCache,
};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::node_state::PersistedNodeState;
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId,
};
use aruna_core::structs::placement::node_subject::NodeSubjectRecord;
use aruna_core::structs::placement::policy::attachment::{
    BULK_INTENT_KEYSPACE, BULK_RUN_KEYSPACE, POLICY_MUTATION_KEYSPACE, PolicyBulkRun, PolicyIntent,
    PolicyIntentKey, PolicyMutationRecord,
};
use aruna_core::structs::placement::policy::document::PlacementPolicyDocument;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobLocationKey, BlobVersion, BucketInfo, CurrentVersionPointer, HashIndex,
    ManagedCopyKey, ManagedCopyRecord, UserAccess, VersionKey,
};
use aruna_core::structs::storage::multipart::{
    MultipartObjectKey, MultipartObjectPart, MultipartObjectSummary, MultipartPart,
    MultipartPartKey, MultipartUpload,
};
use aruna_core::structs::storage::vfs::{VfsWatchKey, decode_token};
use aruna_ops_identity::placement::policy::cache::PolicyCacheEntry;
use aruna_ops_work::jobs::lifecycle::witness::arm::{WitnessDeadline, WitnessExplain};
use chrono::{DateTime, Utc};
use craqle::{
    ActorId as CraqleActorId, Dot as CraqleDot, GraphPolicy as CraqleGraphPolicy,
    VectorClock as CraqleVectorClock,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use ulid::Ulid;

use super::present::{
    DecodedField, DecodedValue, EntryOutput, JsonAuthorizationDocument, JsonCacheEntry,
    JsonClockEntry, JsonConfigDocument, JsonCraqleDot, JsonGraphKey, JsonGraphMeta,
    JsonGraphPolicy, JsonGroup, JsonJobReservation, JsonLogKey, JsonPendingPlacement,
    JsonPersistedState, JsonPlacementDocument, JsonQuadKey, JsonRecordEnvelope, JsonRecordKey,
    JsonStored, JsonStoredEntry, JsonStoredOp, JsonUserAccess, JsonVectorClock, family_id_string,
};

const DOT_ENCODING_TAG: u8 = b'D';
const BATCH_ENCODING_TAG: u8 = b'B';
const GRAPH_META_PREFIX: u8 = b'M';
const GRAPH_DIRTY_PREFIX: u8 = b'D';
const GRAPH_REINDEX_PREFIX: u8 = b'R';
const LOG_HEAD_PREFIX: u8 = b'H';
const LOG_BATCH_PREFIX: u8 = b'B';
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct CraqleTermId(pub(super) u128);

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(super) struct CraqleStoredMeta {
    pub(super) policy: CraqleGraphPolicy,
    pub(super) clock: CraqleVectorClock,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(super) enum CraqleQuadOp {
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
    pub(super) ops: Vec<CraqleQuadOp>,
    pub(super) timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CraqleQuadParts {
    pub(super) graph: CraqleTermId,
    pub(super) subject: CraqleTermId,
    pub(super) predicate: CraqleTermId,
    pub(super) object: CraqleTermId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CraqleGraphParts {
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
pub(super) enum CraqleLogParts {
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
        | APPLIED_OPS_KEYSPACE
        | NODE_STATE_KEYSPACE
        | NODE_SUBJECT_KEYSPACE
        | COMPUTE_DEPARTURE_KEYSPACE
        | ONBOARDING_KEYSPACE => decode_utf8_key(key),
        FAMILY_RECORD_KEYSPACE | FAMILY_PENDING_KEYSPACE | FAMILY_OUTBOX_KEYSPACE => {
            decode_record_key(key)
        }
        FAMILY_CONFLICT_KEYSPACE => decode_conflict_key(key),
        FAMILY_ALIAS_KEYSPACE => decode_alias_key(key),
        FAMILY_PROJECTION_KEYSPACE => decode_family_key(key),
        WITNESS_DEADLINE_KEYSPACE => decode_deadline_key(key),
        DEADLINE_INDEX_KEYSPACE => decode_family_key(key),
        PLAN_EXPLAIN_KEYSPACE => decode_explain_key(key),
        JOB_RESERVATION_KEYSPACE => decode_ulid_key(key),
        PLACEMENT_POLICY_KEYSPACE | POLICY_MUTATION_KEYSPACE | BULK_RUN_KEYSPACE => {
            decode_policy_id(keyspace_name, key)
        }
        POLICY_CACHE_KEYSPACE => decode_policy_cache(key),
        BULK_INTENT_KEYSPACE => PolicyIntentKey::from_bytes(key)
            .map(|value| DecodedField::PolicyIntentKey {
                operation_id: value.operation_id.to_string(),
                key: value.key,
            })
            .unwrap_or_else(|_| raw_field(key)),
        OUTPUT_RECORD_KEYSPACE => decode_attempt_key(key),
        SYNC_PLACEMENT_KEYSPACE => raw_field(key),
        UPLOAD_KEYSPACE => decode_ulid_key(key),
        UPLOAD_PART_KEYSPACE => MultipartPartKey::from_bytes(key)
            .map(|value| DecodedField::MultipartPartKey { value })
            .unwrap_or_else(|_| raw_field(key)),
        OBJECT_METADATA_KEYSPACE => MultipartObjectKey::from_bytes(key)
            .map(|value| DecodedField::MultipartObjectKey { value })
            .unwrap_or_else(|_| raw_field(key)),
        DHT_KEYSPACE => decode_dht_key(key),
        BLOB_HEAD_KEYSPACE => decode_head_key(key),
        PATHS_INDEX_KEYSPACE => HashIndex::from_bytes(key)
            .map(|value| DecodedField::HashIndex { value })
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
                    data: JsonConfigDocument(data),
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
        BLOB_HEAD_KEYSPACE if VfsWatchKey::is_watch_key(key) => {
            decode_value_with(value, decode_token, |token| DecodedValue::WatchToken {
                token: token.to_string(),
            })
        }
        BLOB_HEAD_KEYSPACE => decode_value_with(value, CurrentVersionPointer::from_bytes, |data| {
            DecodedValue::CurrentVersionPointer { data }
        }),
        BLOB_LOCATIONS_KEYSPACE => decode_value_with(
            value,
            aruna_core::structs::storage::blob::BackendLocation::from_bytes,
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
        OUTPUT_RECORD_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<JobRecordEnvelope>(bytes),
            |data| DecodedValue::JobOutputRecord {
                data: JsonRecordEnvelope(data),
            },
        ),
        FAMILY_RECORD_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<JobRecordEnvelope>(bytes),
            |data| DecodedValue::JobFamilyRecord {
                data: JsonRecordEnvelope(data),
            },
        ),
        FAMILY_PENDING_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<PendingRecord>(bytes),
            |data| DecodedValue::JobPendingRecord {
                envelope: JsonRecordEnvelope(data.envelope),
                need: pending_need_string(data.need),
                first_seen_ms: data.first_seen_ms,
                attempts: data.attempts,
            },
        ),
        FAMILY_CONFLICT_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<ConflictRecord>(bytes),
            |data| DecodedValue::JobConflictRecord {
                envelope: JsonRecordEnvelope(data.envelope),
                retained: hex::encode(data.retained),
                observed_at_ms: data.observed_at_ms,
                relayed_by: data.relayed_by.map(|node| node.to_string()),
            },
        ),
        FAMILY_ALIAS_KEYSPACE => decode_value_with(value, JobRecordKey::from_bytes, |data| {
            DecodedValue::JobAliasTarget {
                data: JsonRecordKey(data),
            }
        }),
        FAMILY_PROJECTION_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<ProjectionCache>(bytes),
            |data| DecodedValue::JobProjectionCache {
                revision: data.revision,
                stale: data.stale,
                projected: data.projection.is_some(),
            },
        ),
        FAMILY_OUTBOX_KEYSPACE => decode_value_with(
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
        DEADLINE_INDEX_KEYSPACE | WITNESS_DEADLINE_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<WitnessDeadline>(bytes),
            |data| DecodedValue::JobWitnessDeadline { data },
        ),
        PLAN_EXPLAIN_KEYSPACE => decode_value_with(
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
                    data: JsonPlacementDocument(data),
                }
            })
        }
        POLICY_CACHE_KEYSPACE => decode_value_with(value, PolicyCacheEntry::from_bytes, |data| {
            DecodedValue::PolicyCacheEntry {
                data: JsonCacheEntry(data),
            }
        }),
        POLICY_MUTATION_KEYSPACE => {
            decode_value_with(value, PolicyMutationRecord::from_bytes, |data| {
                DecodedValue::PolicyMutationRecord { data }
            })
        }
        BULK_RUN_KEYSPACE => decode_value_with(value, PolicyBulkRun::from_bytes, |data| {
            DecodedValue::PolicyBulkRun { data }
        }),
        BULK_INTENT_KEYSPACE => decode_value_with(value, PolicyIntent::from_bytes, |data| {
            DecodedValue::PolicyIntent { data }
        }),
        UPLOAD_KEYSPACE => decode_value_with(value, MultipartUpload::from_bytes, |data| {
            DecodedValue::MultipartUpload { data }
        }),
        UPLOAD_PART_KEYSPACE => decode_value_with(value, MultipartPart::from_bytes, |data| {
            DecodedValue::MultipartPart { data }
        }),
        OBJECT_METADATA_KEYSPACE => decode_object_metadata(key, value),
        AUTH_KEYSPACE => decode_auth_value(value),
        API_STATE_KEYSPACE => decode_api_state(key, value),
        APPLIED_OPS_KEYSPACE => raw_value(value, Some("document sync applied op".to_string())),
        NODE_STATE_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<PersistedNodeState>(bytes),
            |data| DecodedValue::NodeState {
                data: JsonPersistedState(data),
            },
        ),
        SYNC_PLACEMENT_KEYSPACE => decode_value_with(
            value,
            aruna_ops_identity::sync::shard_placement::decode_placement,
            |data| DecodedValue::PendingDocumentPlacement {
                data: JsonPendingPlacement(data),
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
            value: JsonRecordKey(value),
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
            record: JsonRecordKey(record),
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
        submission_id: aruna_core::structs::execution::job::family::SubmissionId(
            key.get(..32)?.try_into().ok()?,
        ),
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
            data: JsonAuthorizationDocument(data),
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
        REALMS_LIST_KEY => postcard::from_bytes::<HashSet<RealmId>>(value)
            .map(|data| {
                let mut data = data
                    .into_iter()
                    .map(|realm_id| realm_id.to_string())
                    .collect::<Vec<_>>();
                data.sort();
                DecodedValue::TrustedRealmsList { data }
            })
            .unwrap_or_else(|error| raw_value(value, Some(error.to_string()))),
        ADMIN_CLAIMED_KEY => postcard::from_bytes::<bool>(value)
            .map(|data| DecodedValue::RealmAdminClaimed { data })
            .unwrap_or_else(|error| raw_value(value, Some(error.to_string()))),
        _ => raw_value(value, Some("unsupported api_state key".to_string())),
    }
}

fn decode_object_metadata(key: &[u8], value: &[u8]) -> DecodedValue {
    match MultipartObjectKey::from_bytes(key) {
        Ok(MultipartObjectKey::Summary { .. }) => {
            decode_value_with(value, MultipartObjectSummary::from_bytes, |data| {
                DecodedValue::MultipartObjectSummary { data }
            })
        }
        Ok(MultipartObjectKey::Part { .. }) => {
            decode_value_with(value, MultipartObjectPart::from_bytes, |data| {
                DecodedValue::MultipartObjectPart { data }
            })
        }
        Err(error) => raw_value(value, Some(error.to_string())),
    }
}

/// A head keyspace key: an ordinary head, or the watch key of an unpublished name.
fn decode_head_key(key: &[u8]) -> DecodedField {
    if VfsWatchKey::is_watch_key(key) {
        return VfsWatchKey::from_bytes(key)
            .map(|value| DecodedField::WatchKey {
                bucket: value.bucket,
                key: value.key,
            })
            .unwrap_or_else(|_| raw_field(key));
    }
    BlobHeadKey::from_bytes(key)
        .map(|value| DecodedField::BlobHeadKey { value })
        .unwrap_or_else(|_| raw_field(key))
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

fn decode_quad_key(key: &[u8]) -> Result<CraqleQuadParts, String> {
    if key.len() != 64 {
        return Err(format!(
            "invalid craqle quad key: expected 64 bytes, found {}",
            key.len()
        ));
    }
    Ok(CraqleQuadParts {
        graph: decode_term_id(&key[0..16], "craqle quad graph")?,
        subject: decode_term_id(&key[16..32], "craqle quad subject")?,
        predicate: decode_term_id(&key[32..48], "craqle quad predicate")?,
        object: decode_term_id(&key[48..64], "craqle quad object")?,
    })
}

fn decode_graph_key(key: &[u8]) -> Result<CraqleGraphParts, String> {
    match key.first().copied() {
        Some(GRAPH_META_PREFIX) if key.len() == 17 => Ok(CraqleGraphParts::Meta {
            graph: decode_term_id(&key[1..17], "craqle graph meta graph")?,
        }),
        Some(GRAPH_DIRTY_PREFIX) if key.len() == 33 => Ok(CraqleGraphParts::Dirty {
            graph: decode_term_id(&key[1..17], "craqle graph dirty graph")?,
            subject: decode_term_id(&key[17..33], "craqle graph dirty subject")?,
        }),
        Some(GRAPH_REINDEX_PREFIX) if key.len() == 17 => Ok(CraqleGraphParts::Reindex {
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

fn decode_log_key(key: &[u8]) -> Result<CraqleLogParts, String> {
    match key.first().copied() {
        Some(LOG_HEAD_PREFIX) if key.len() == 49 => Ok(CraqleLogParts::Head {
            graph: decode_term_id(&key[1..17], "craqle log head graph")?,
            actor: CraqleActorId::from_bytes(
                key[17..49]
                    .try_into()
                    .map_err(|_| "invalid craqle log head actor".to_string())?,
            ),
        }),
        Some(LOG_BATCH_PREFIX) if key.len() == 57 => Ok(CraqleLogParts::Batch {
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
    let dots = if value.first().copied() == Some(DOT_ENCODING_TAG) {
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
        Ok(CraqleGraphParts::Meta { graph }) => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<CraqleStoredMeta>(bytes),
            |data| DecodedValue::CraqleGraphMeta {
                data: json_graph_meta(graph, data),
            },
        ),
        Ok(CraqleGraphParts::Dirty { .. }) => decode_value_with(
            value,
            |bytes| decode_craqle_u64(bytes, "craqle graph dirty token"),
            |data| DecodedValue::GraphDirtyToken { data },
        ),
        Ok(CraqleGraphParts::Reindex { .. }) => decode_value_with(
            value,
            |bytes| decode_craqle_u64(bytes, "craqle graph reindex token"),
            |data| DecodedValue::GraphReindexToken { data },
        ),
        Err(error) => raw_value(value, Some(error)),
    }
}

fn decode_log_batch(key: &[u8], value: &[u8]) -> Result<JsonStored, String> {
    let CraqleLogParts::Batch { graph, .. } = decode_log_key(key)? else {
        return Err("craqle log batch value requires a batch key".to_string());
    };
    if value.first().copied() != Some(BATCH_ENCODING_TAG) {
        return Err("unsupported craqle log batch encoding".to_string());
    }
    let batch = postcard::from_bytes::<CraqleStoredBatch>(&value[1..])
        .map_err(|error| error.to_string())?;
    Ok(json_stored_batch(graph, batch))
}

fn decode_log_value(key: &[u8], value: &[u8]) -> DecodedValue {
    match decode_log_key(key) {
        Ok(CraqleLogParts::Head { .. }) => decode_value_with(
            value,
            |bytes| decode_craqle_u64(bytes, "craqle log head"),
            |data| DecodedValue::CraqleLogHead { data },
        ),
        Ok(CraqleLogParts::Batch { .. }) => decode_value_with(
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

fn json_quad_key(parts: CraqleQuadParts) -> JsonQuadKey {
    JsonQuadKey {
        graph: term_id_string(parts.graph),
        subject: term_id_string(parts.subject),
        predicate: term_id_string(parts.predicate),
        object: term_id_string(parts.object),
    }
}

fn json_graph_key(parts: CraqleGraphParts) -> JsonGraphKey {
    match parts {
        CraqleGraphParts::Meta { graph } => JsonGraphKey::Meta {
            graph: term_id_string(graph),
        },
        CraqleGraphParts::Dirty { graph, subject } => JsonGraphKey::Dirty {
            graph: term_id_string(graph),
            subject: term_id_string(subject),
        },
        CraqleGraphParts::Reindex { graph } => JsonGraphKey::Reindex {
            graph: term_id_string(graph),
        },
    }
}

fn json_log_key(parts: CraqleLogParts) -> JsonLogKey {
    match parts {
        CraqleLogParts::Head { graph, actor } => JsonLogKey::Head {
            graph: term_id_string(graph),
            actor: actor.to_string(),
        },
        CraqleLogParts::Batch {
            graph,
            actor,
            counter,
        } => JsonLogKey::Batch {
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

fn json_vector_clock(clock: CraqleVectorClock) -> JsonVectorClock {
    JsonVectorClock {
        entries: clock
            .0
            .into_iter()
            .map(|(actor, counter)| JsonClockEntry {
                actor: actor.to_string(),
                counter,
            })
            .collect(),
    }
}

fn json_graph_policy(policy: CraqleGraphPolicy) -> JsonGraphPolicy {
    let mut permission_paths = policy.permission_paths;
    permission_paths.sort();
    permission_paths.dedup();
    JsonGraphPolicy {
        public: policy.public,
        permission_paths,
    }
}

fn json_graph_meta(graph: CraqleTermId, meta: CraqleStoredMeta) -> JsonGraphMeta {
    JsonGraphMeta {
        graph: term_id_string(graph),
        policy: json_graph_policy(meta.policy),
        clock: json_vector_clock(meta.clock),
    }
}

fn json_stored_batch(graph: CraqleTermId, batch: CraqleStoredBatch) -> JsonStored {
    JsonStored {
        graph: term_id_string(graph),
        actor: batch.actor.to_string(),
        counter: batch.counter,
        base_clock: json_vector_clock(batch.base_clock),
        ops: batch
            .ops
            .into_iter()
            .map(|op| match op {
                CraqleQuadOp::Add {
                    subject,
                    predicate,
                    object,
                    dot,
                } => JsonStoredOp::Add {
                    subject: term_id_string(subject),
                    predicate: term_id_string(predicate),
                    object: term_id_string(object),
                    dot: json_craqle_dot(dot),
                },
                CraqleQuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed,
                } => JsonStoredOp::Remove {
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
#[path = "decode_tests.rs"]
mod tests;
