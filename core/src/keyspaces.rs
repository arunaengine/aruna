//! Names the persistent keyspaces the node stores its records in.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub const AUTH_KEYSPACE: &str = "auth";
pub const GROUP_KEYSPACE: &str = "groups";
pub const OWNER_INDEX_KEYSPACE: &str = "group_owner_index";
pub const REALM_CONFIG_KEYSPACE: &str = "realm_config";
pub const METADATA_INDEX_KEYSPACE: &str = "metadata_index";
pub const DOCUMENT_INDEX_KEYSPACE: &str = "metadata_document_index";
pub const IRI_INDEX_KEYSPACE: &str = "metadata_iri_reference_index";
pub const METADATA_HOLDERS_KEYSPACE: &str = "metadata_holders";
/// Time-ordered index of registry records by `updated_at_ms`, for OAI-PMH
/// datestamp enumeration (#320). Written atomically with each registry record.
pub const UPDATED_INDEX_KEYSPACE: &str = "metadata_updated_index";
/// Generation-scoped, timestamp-ordered index of the records an anonymous caller
/// is currently authorized to read, so OAI-PMH enumeration never scans the
/// registry. Maintained out of band; readers re-check authorization per record.
pub const VISIBILITY_INDEX_KEYSPACE: &str = "metadata_visibility_index";
/// The single state row naming the servable visibility-index generation. Absent
/// or not-ready means anonymous enumeration fails closed.
pub const VISIBILITY_STATE_KEYSPACE: &str = "metadata_visibility_state";
pub const METADATA_AUDIT_KEYSPACE: &str = "metadata_audit";
pub const EVENT_LOG_KEYSPACE: &str = "metadata_event_log";
pub const CREATE_ACCEPTANCE_KEYSPACE: &str = "metadata_create_acceptance";
pub const PENDING_PROJECTION_KEYSPACE: &str = "metadata_pending_projection";
pub const DOCUMENT_LIFECYCLE_KEYSPACE: &str = "metadata_document_lifecycle";
pub const GRAPH_LIFECYCLE_KEYSPACE: &str = "metadata_graph_lifecycle";
pub const PRUNE_JOB_KEYSPACE: &str = "metadata_graph_prune_jobs";
pub const MATERIALIZATION_STATUS_KEYSPACE: &str = "metadata_materialization_status";
pub const VALIDATION_STATUS_KEYSPACE: &str = "metadata_profile_validation_status";
pub const RAW_REVISION_KEYSPACE: &str = "metadata_raw_revisions";
pub const RAW_BUDGET_KEYSPACE: &str = "metadata_raw_budgets";
pub const MATERIALIZATION_JOB_KEYSPACE: &str = "metadata_materialization_jobs";
pub const DOCUMENT_JOB_KEYSPACE: &str = "metadata_materialization_document_jobs";
pub const DEAD_LETTER_KEYSPACE: &str = "metadata_materialization_dead_letters";
pub const MATERIALIZATION_PRUNE_KEYSPACE: &str = "metadata_materialization_prunes";
pub const DOCUMENT_STATE_KEYSPACE: &str = "admin_document_state";
pub const DOCUMENT_CONFLICT_KEYSPACE: &str = "admin_document_conflicts";
pub const APPLIED_OPS_KEYSPACE: &str = "document_sync_applied_ops";
pub const SYNC_OUTBOX_KEYSPACE: &str = "document_sync_outbox";
pub const OUTBOX_INDEX_KEYSPACE: &str = "token_revocation_outbox_index";
pub const SYNC_REVISION_KEYSPACE: &str = "document_sync_revisions";
pub const SYNC_CONFLICT_KEYSPACE: &str = "document_sync_conflicts";
/// Durable store for permanently-invalid replicated sync events (#338).
pub const SYNC_QUARANTINE_KEYSPACE: &str = "sync_quarantine";
/// Single-row record/byte accounting for the quarantine store, written in the
/// same batch as every quarantine row write and prune delete.
pub const QUARANTINE_USAGE_KEYSPACE: &str = "sync_quarantine_usage";
pub const SYNC_PLACEMENT_KEYSPACE: &str = "sync_placements";
/// Per-bucket write-admission fence: the highest activation generation a
/// departing holder has closed. A holder-authoritative writer reads it inside
/// its own transaction, so a close conflicts every uncommitted predecessor write.
pub const WRITE_FENCE_KEYSPACE: &str = "placement_write_fence";
/// Immutable placement-policy documents a holder stores, keyed by policy id.
pub const PLACEMENT_POLICY_KEYSPACE: &str = "placement_policies";
/// Node-local policy cache keyed by `(policy_id, digest)`. An id-only key could
/// accept changed bytes under a known id, which policy immutability forbids.
pub const POLICY_CACHE_KEYSPACE: &str = "placement_policy_cache";
pub const SHARD_MANIFEST_KEYSPACE: &str = "shard_manifest";
pub const SHARD_VERIFICATION_KEYSPACE: &str = "shard_verification";
pub const TASK_TIMER_KEYSPACE: &str = "task_timers";
pub const USER_KEYSPACE: &str = "users";
pub const SUBJECT_INDEX_KEYSPACE: &str = "user_subject_index";
pub const SUBJECT_CLAIMS_KEYSPACE: &str = "user_subject_claims";
pub const USER_SESSION_KEYSPACE: &str = "user_sessions";
pub const USER_OWNER_KEYSPACE: &str = "user_session_owner";
pub const USER_VAULT_KEYSPACE: &str = "user_vaults";
pub const ASSISTANT_PROVIDER_KEYSPACE: &str = "assistant_providers";
pub const PROVIDER_OWNER_KEYSPACE: &str = "assistant_provider_owner";
pub const CHAT_HEAD_KEYSPACE: &str = "assistant_chat_heads";
pub const CHAT_TURN_KEYSPACE: &str = "assistant_chat_turns";

// Blob + S3 keyspaces
pub const BLOB_LOCATIONS_KEYSPACE: &str = "blob_locations";
pub const BLOB_CLEANUP_KEYSPACE: &str = "blob_pending_cleanups";
pub const BLOB_RECLAIM_KEYSPACE: &str = "blob_reclaim_candidates";
pub const HIDDEN_RESERVATION_KEYSPACE: &str = "blob_hidden_reservations";
/// Durable evidence of a copy that failed hash/bao verification (§8.2), keyed
/// per (hash, backend) so re-hitting the same corrupt copy overwrites its row.
pub const BLOB_QUARANTINE_KEYSPACE: &str = "blob_quarantine";
/// Local inventory of the logical version copies this node has registered.
/// Written and removed atomically with the operation that exposes a copy
/// locally; it is never evidence about another node's copies.
pub const MANAGED_COPY_KEYSPACE: &str = "managed_copies";
pub const BLOB_HEAD_KEYSPACE: &str = "blob_heads";
pub const BLOB_VERSIONS_KEYSPACE: &str = "blob_versions";
/// Node-local trail of S3 deletions: delete markers, per-version deletes and
/// completed purge jobs. Written inside the transaction that performs them.
pub const DELETE_AUDIT_KEYSPACE: &str = "blob_delete_audit";
pub const PATHS_INDEX_KEYSPACE: &str = "hash_paths_index";
pub const USER_ACCESS_KEYSPACE: &str = "user_access";
pub const ACCESS_OWNER_KEYSPACE: &str = "user_access_owner";
pub const S3_SESSION_KEYSPACE: &str = "s3_sessions";
pub const SESSION_OWNER_KEYSPACE: &str = "s3_session_owner";
pub const SESSION_EXPIRY_KEYSPACE: &str = "s3_session_expiry";
pub const S3_BUCKET_KEYSPACE: &str = "s3_buckets";
pub const RELATIONSHIP_OUT_KEYSPACE: &str = "sync_relationship_out";
pub const RELATIONSHIP_IN_KEYSPACE: &str = "sync_relationship_in";
pub const MIRROR_REPAIR_KEYSPACE: &str = "sync_mirror_repair";
pub const SYNC_REFERENCE_KEYSPACE: &str = "sync_reference_state";
pub const OBJECT_METADATA_KEYSPACE: &str = "s3_multipart_object_metadata";
pub const UPLOAD_KEYSPACE: &str = "s3_multipart_uploads";
pub const UPLOAD_PART_KEYSPACE: &str = "s3_multipart_upload_parts";
/// One active, scope-aware permanent-purge fence per bucket.
pub const PURGE_FENCE_KEYSPACE: &str = "s3_purge_fences";
/// Durable inventory and batch counters for resumable permanent-purge jobs.
pub const PURGE_CHECKPOINT_KEYSPACE: &str = "s3_purge_checkpoints";
pub const REPLICATION_JOB_KEYSPACE: &str = "blob_replication_jobs";
pub const REPLICATION_OBLIGATION_KEYSPACE: &str = "blob_live_replication_obligations";
pub const REFRESH_JOB_KEYSPACE: &str = "reference_metadata_refresh_jobs";
pub const USAGE_STATS_KEYSPACE: &str = "usage_stats";
pub const NODE_STATS_KEYSPACE: &str = "usage_node_stats";
pub const NODE_INFO_KEYSPACE: &str = "node_info";
/// Single-row local placement subject and its generation. Governed writes and
/// internal serves are evaluated against it; a rejoin blocks serving here.
pub const NODE_SUBJECT_KEYSPACE: &str = "node_subject";
pub const NOTIFICATION_INBOX_KEYSPACE: &str = "notification_inbox";
pub const PRUNE_INDEX_KEYSPACE: &str = "notification_inbox_prune_index";
pub const NOTIFICATION_OUTBOX_KEYSPACE: &str = "notification_outbox";
pub const WATCH_SUBSCRIPTIONS_KEYSPACE: &str = "notification_watch_subscriptions";
pub const WATCH_INTEREST_KEYSPACE: &str = "notification_watch_interest";

pub const STORAGE_ROUTING_KEYSPACE: &str = "group_storage_routing";

/// Keyed by backend id alone: the blob adapter resolves a stored
/// `BackendRef::Group` without knowing which group owns it.
pub const STORAGE_BACKEND_KEYSPACE: &str = "group_storage_backend";
pub const BACKEND_SECRET_KEYSPACE: &str = "group_storage_backend_secret";

/// The same records keyed by `group id || backend id`, so routing a write reads
/// one group's backends instead of every tenant's. Written in the same batch or
/// transaction as the id-keyed record it mirrors.
pub const BACKEND_INDEX_KEYSPACE: &str = "group_storage_backend_index";

/// Device-local registrations of the directories this node offers as read-only
/// buckets, keyed by bucket name. Never replicated: the root path is the one
/// detail about the owner's machine that must not leave it.
pub const OFFERED_DIRECTORY_KEYSPACE: &str = "offered_directories";

/// Authoring intents the owner queued on the device while the realm was
/// unreachable, keyed by local draft id. Never replicated: an entry becomes
/// realm state only when the drain forwards it as an ordinary create.
pub const DEVICE_INTAKE_KEYSPACE: &str = "device_intake";

/// The realm-config clock the realm documents this device holds were copied at,
/// keyed by realm id. It is what keeps a later copy from rolling the device
/// back, and it is never replicated.
pub const REALM_MARKER_KEYSPACE: &str = "device_realm_marker";

/// The api urls of the realm's management nodes, as a realm node served them,
/// keyed by realm id. A device holds no peer node-info document, so this is the
/// only address it has for a management-only route.
pub const MANAGEMENT_URL_KEYSPACE: &str = "device_management_urls";

/// The one row a device keeps about its exchange with the realm: when the realm
/// last answered, when the last pass finished, and whether one is in flight.
pub const SYNC_STATE_KEYSPACE: &str = "device_sync_state";

/// The metadata documents this device keeps a local craqle replica of, keyed
/// by document id. Never replicated: it records what the owner selected and
/// how far this device has synced each replica.
pub const DEVICE_REPLICA_KEYSPACE: &str = "device_replica";

/// Device-local bindings of a directory to a realm bucket prefix, keyed by
/// folder id. Never replicated: the root path must not leave the machine.
pub const SYNCED_FOLDER_KEYSPACE: &str = "synced_folders";

/// The merge base of every synced path, keyed by `folder id || relative path`.
/// It is the only evidence a file is still the one the last sync wrote.
pub const SYNC_BASE_KEYSPACE: &str = "sync_bases";

/// Local versions waiting to be pulled by their realm node, keyed by ULID so a
/// forward scan drains in observation order.
pub const SYNC_UPLOAD_KEYSPACE: &str = "sync_upload_outbox";

/// Append-only record of the explicit owner actions that replaced or removed
/// local bytes, keyed by `folder id || action id`.
pub const SYNC_LOG_KEYSPACE: &str = "sync_action_log";

pub const SOURCE_INDEX_KEYSPACE: &str = "source_connector_index";
pub const SOURCE_SECRET_KEYSPACE: &str = "source_connector_secret";

// Repository interop: metadata harvest framework (#442).
pub const CONNECTOR_INDEX_KEYSPACE: &str = "repository_connector_index";
pub const CONNECTOR_SECRET_KEYSPACE: &str = "repository_connector_secret";
pub const HARVEST_SOURCE_KEYSPACE: &str = "harvest_source";
pub const HARVEST_PROVENANCE_KEYSPACE: &str = "harvest_provenance";
/// w3id persistent-identifier mappings, keyed by document id (#442, spec 3.5).
pub const ID_MAPPING_KEYSPACE: &str = "persistent_id_mapping";

// Durable job framework keyspaces (#318).
pub const JOB_KEYSPACE: &str = "jobs";
pub const SCHEDULE_INDEX_KEYSPACE: &str = "job_schedule_index";
pub const JOB_INDEX_KEYSPACE: &str = "job_owner_index";
pub const ACTIVE_USER_KEYSPACE: &str = "job_active_user";
pub const DEDUP_INDEX_KEYSPACE: &str = "job_dedup_index";
pub const RUN_CRATE_KEYSPACE: &str = "job_run_crate";
pub const ATTEMPT_CONTROL_KEYSPACE: &str = "job_attempt_control";
/// Signed immutable output records, keyed by ExecutionId.
pub const OUTPUT_RECORD_KEYSPACE: &str = "job_output_records";
pub const JOB_ENTRY_KEYSPACE: &str = "job_entries";

/// Immutable authentic record envelopes, keyed by `JobRecordKey`. A key is
/// written once: the same digest replays as a no-op and a different digest is
/// retained in the conflict keyspace instead of overwriting it.
pub const FAMILY_RECORD_KEYSPACE: &str = "job_family_records";
/// Bounded records whose predecessor evidence, or whose local holder view, is
/// not available yet. A pending record is never projected or relayed.
pub const FAMILY_PENDING_KEYSPACE: &str = "job_family_pending";
/// Explicit same-key/different-digest evidence, keyed by record key and digest.
/// Quarantined records stay auditable and never enter a projection.
pub const FAMILY_CONFLICT_KEYSPACE: &str = "job_family_conflicts";
/// Alias index: one accepted `JobId` to the request family that admitted it.
pub const FAMILY_ALIAS_KEYSPACE: &str = "job_family_aliases";
/// Per-family projection cache and its bounded revision. Derived state only; it
/// is rebuilt from the immutable records and is never authority.
pub const FAMILY_PROJECTION_KEYSPACE: &str = "job_family_projections";
/// Locally published authentic records awaiting family replication, keyed by
/// record key. Only a replicated-authority record is ever queued here.
pub const FAMILY_OUTBOX_KEYSPACE: &str = "job_family_outbox";
/// Exact local capacity held for one accepted execution, keyed by ExecutionId.
/// The row is written with the signed receipt and released at terminal state.
pub const JOB_RESERVATION_KEYSPACE: &str = "job_reservations";
pub const ADMISSION_QUOTA_KEYSPACE: &str = "job_admission_quota";
/// Persisted witness fallback deadlines, keyed by due time and family, so a
/// later-ranked witness still plans after a restart.
pub const WITNESS_DEADLINE_KEYSPACE: &str = "job_witness_deadlines";
/// Current witness deadline by family; the due-time rows are the scan index.
pub const DEADLINE_INDEX_KEYSPACE: &str = "job_witness_deadline_index";
/// Bounded explain record of the plan a witness stored before it launched.
pub const PLAN_EXPLAIN_KEYSPACE: &str = "job_plan_explains";
pub const ARTIFACT_TOMBSTONE_KEYSPACE: &str = "job_artifact_tombstones";
/// The single row recording what this node could not resolve when it departed.
pub const COMPUTE_DEPARTURE_KEYSPACE: &str = "compute_departure";
pub const JOB_STATE_KEYSPACE: &str = "rocrate_job_state";
pub const ROCRATE_UPLOAD_KEYSPACE: &str = "rocrate_uploads";
pub const UPLOAD_CLEANUP_KEYSPACE: &str = "rocrate_upload_cleanups";
pub const STAGING_STATE_KEYSPACE: &str = "staging_job_state";

pub const BUCKET_STATS_DB: &str = "bucket_stats";

pub const API_STATE_KEYSPACE: &str = "api_state";
pub const NODE_STATE_KEYSPACE: &str = "node_state";
pub const ONBOARDING_KEYSPACE: &str = "onboarding";
pub const DHT_KEYSPACE: &str = "dht_v2";
pub const CRAQLE_TERMS_KEYSPACE: &str = "terms";
pub const CRAQLE_QUADS_KEYSPACE: &str = "quads";
pub const CRAQLE_GRAPHS_KEYSPACE: &str = "graphs";
pub const CRAQLE_LOG_KEYSPACE: &str = "log";

/// Every keyspace this crate defines, so tooling reports a missing one without
/// maintaining its own copy of the list.
pub const KEYSPACE_CATALOG: &[&str] = &[
    AUTH_KEYSPACE,
    GROUP_KEYSPACE,
    OWNER_INDEX_KEYSPACE,
    REALM_CONFIG_KEYSPACE,
    METADATA_INDEX_KEYSPACE,
    DOCUMENT_INDEX_KEYSPACE,
    IRI_INDEX_KEYSPACE,
    METADATA_HOLDERS_KEYSPACE,
    UPDATED_INDEX_KEYSPACE,
    VISIBILITY_INDEX_KEYSPACE,
    VISIBILITY_STATE_KEYSPACE,
    METADATA_AUDIT_KEYSPACE,
    EVENT_LOG_KEYSPACE,
    CREATE_ACCEPTANCE_KEYSPACE,
    PENDING_PROJECTION_KEYSPACE,
    DOCUMENT_LIFECYCLE_KEYSPACE,
    GRAPH_LIFECYCLE_KEYSPACE,
    PRUNE_JOB_KEYSPACE,
    MATERIALIZATION_STATUS_KEYSPACE,
    VALIDATION_STATUS_KEYSPACE,
    RAW_REVISION_KEYSPACE,
    RAW_BUDGET_KEYSPACE,
    MATERIALIZATION_JOB_KEYSPACE,
    DOCUMENT_JOB_KEYSPACE,
    DEAD_LETTER_KEYSPACE,
    MATERIALIZATION_PRUNE_KEYSPACE,
    DOCUMENT_STATE_KEYSPACE,
    DOCUMENT_CONFLICT_KEYSPACE,
    APPLIED_OPS_KEYSPACE,
    SYNC_OUTBOX_KEYSPACE,
    OUTBOX_INDEX_KEYSPACE,
    SYNC_REVISION_KEYSPACE,
    SYNC_CONFLICT_KEYSPACE,
    SYNC_QUARANTINE_KEYSPACE,
    QUARANTINE_USAGE_KEYSPACE,
    SYNC_PLACEMENT_KEYSPACE,
    WRITE_FENCE_KEYSPACE,
    PLACEMENT_POLICY_KEYSPACE,
    POLICY_CACHE_KEYSPACE,
    SHARD_MANIFEST_KEYSPACE,
    SHARD_VERIFICATION_KEYSPACE,
    TASK_TIMER_KEYSPACE,
    USER_KEYSPACE,
    SUBJECT_INDEX_KEYSPACE,
    SUBJECT_CLAIMS_KEYSPACE,
    USER_SESSION_KEYSPACE,
    USER_OWNER_KEYSPACE,
    USER_VAULT_KEYSPACE,
    ASSISTANT_PROVIDER_KEYSPACE,
    PROVIDER_OWNER_KEYSPACE,
    CHAT_HEAD_KEYSPACE,
    CHAT_TURN_KEYSPACE,
    BLOB_LOCATIONS_KEYSPACE,
    BLOB_CLEANUP_KEYSPACE,
    BLOB_RECLAIM_KEYSPACE,
    HIDDEN_RESERVATION_KEYSPACE,
    BLOB_QUARANTINE_KEYSPACE,
    MANAGED_COPY_KEYSPACE,
    BLOB_HEAD_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE,
    DELETE_AUDIT_KEYSPACE,
    PATHS_INDEX_KEYSPACE,
    USER_ACCESS_KEYSPACE,
    ACCESS_OWNER_KEYSPACE,
    S3_SESSION_KEYSPACE,
    SESSION_OWNER_KEYSPACE,
    SESSION_EXPIRY_KEYSPACE,
    S3_BUCKET_KEYSPACE,
    RELATIONSHIP_OUT_KEYSPACE,
    RELATIONSHIP_IN_KEYSPACE,
    MIRROR_REPAIR_KEYSPACE,
    SYNC_REFERENCE_KEYSPACE,
    OBJECT_METADATA_KEYSPACE,
    UPLOAD_KEYSPACE,
    UPLOAD_PART_KEYSPACE,
    PURGE_FENCE_KEYSPACE,
    PURGE_CHECKPOINT_KEYSPACE,
    REPLICATION_JOB_KEYSPACE,
    REPLICATION_OBLIGATION_KEYSPACE,
    REFRESH_JOB_KEYSPACE,
    USAGE_STATS_KEYSPACE,
    NODE_STATS_KEYSPACE,
    NODE_INFO_KEYSPACE,
    NODE_SUBJECT_KEYSPACE,
    NOTIFICATION_INBOX_KEYSPACE,
    PRUNE_INDEX_KEYSPACE,
    NOTIFICATION_OUTBOX_KEYSPACE,
    WATCH_SUBSCRIPTIONS_KEYSPACE,
    WATCH_INTEREST_KEYSPACE,
    STORAGE_ROUTING_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE,
    BACKEND_SECRET_KEYSPACE,
    BACKEND_INDEX_KEYSPACE,
    OFFERED_DIRECTORY_KEYSPACE,
    DEVICE_INTAKE_KEYSPACE,
    REALM_MARKER_KEYSPACE,
    MANAGEMENT_URL_KEYSPACE,
    SYNC_STATE_KEYSPACE,
    DEVICE_REPLICA_KEYSPACE,
    SYNCED_FOLDER_KEYSPACE,
    SYNC_BASE_KEYSPACE,
    SYNC_UPLOAD_KEYSPACE,
    SYNC_LOG_KEYSPACE,
    SOURCE_INDEX_KEYSPACE,
    SOURCE_SECRET_KEYSPACE,
    CONNECTOR_INDEX_KEYSPACE,
    CONNECTOR_SECRET_KEYSPACE,
    HARVEST_SOURCE_KEYSPACE,
    HARVEST_PROVENANCE_KEYSPACE,
    ID_MAPPING_KEYSPACE,
    JOB_KEYSPACE,
    SCHEDULE_INDEX_KEYSPACE,
    JOB_INDEX_KEYSPACE,
    ACTIVE_USER_KEYSPACE,
    DEDUP_INDEX_KEYSPACE,
    RUN_CRATE_KEYSPACE,
    ATTEMPT_CONTROL_KEYSPACE,
    OUTPUT_RECORD_KEYSPACE,
    JOB_ENTRY_KEYSPACE,
    FAMILY_RECORD_KEYSPACE,
    FAMILY_PENDING_KEYSPACE,
    FAMILY_CONFLICT_KEYSPACE,
    FAMILY_ALIAS_KEYSPACE,
    FAMILY_PROJECTION_KEYSPACE,
    FAMILY_OUTBOX_KEYSPACE,
    JOB_RESERVATION_KEYSPACE,
    ADMISSION_QUOTA_KEYSPACE,
    WITNESS_DEADLINE_KEYSPACE,
    DEADLINE_INDEX_KEYSPACE,
    PLAN_EXPLAIN_KEYSPACE,
    ARTIFACT_TOMBSTONE_KEYSPACE,
    COMPUTE_DEPARTURE_KEYSPACE,
    JOB_STATE_KEYSPACE,
    ROCRATE_UPLOAD_KEYSPACE,
    UPLOAD_CLEANUP_KEYSPACE,
    STAGING_STATE_KEYSPACE,
    BUCKET_STATS_DB,
    API_STATE_KEYSPACE,
    NODE_STATE_KEYSPACE,
    ONBOARDING_KEYSPACE,
    DHT_KEYSPACE,
    CRAQLE_TERMS_KEYSPACE,
    CRAQLE_QUADS_KEYSPACE,
    CRAQLE_GRAPHS_KEYSPACE,
    CRAQLE_LOG_KEYSPACE,
];

/// Smallest key strictly greater than every key starting with `prefix`,
/// or `None` if no such key exists (prefix is all `0xFF`).
pub fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for idx in (0..upper.len()).rev() {
        if upper[idx] != u8::MAX {
            upper[idx] = upper[idx].saturating_add(1);
            upper.truncate(idx + 1);
            return Some(upper);
        }
    }

    None
}

/// Cleanup keyspaces record the work a finished transaction still owes, so
/// storage must admit their writes ahead of the ordinary write queue.
pub fn is_cleanup_keyspace(key_space: &str) -> bool {
    key_space == BLOB_CLEANUP_KEYSPACE
}

#[cfg(test)]
mod tests {
    use super::{
        BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, is_cleanup_keyspace, prefix_upper_bound,
    };

    #[test]
    fn classifies_cleanup_keyspace() {
        assert!(is_cleanup_keyspace(BLOB_CLEANUP_KEYSPACE));
        assert!(!is_cleanup_keyspace(BLOB_LOCATIONS_KEYSPACE));
    }

    #[test]
    fn computes_prefix_bound() {
        assert_eq!(prefix_upper_bound(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(prefix_upper_bound(b"ab\xff"), Some(b"ac".to_vec()));
        assert_eq!(prefix_upper_bound(b"\xff\xff"), None);
    }
}
