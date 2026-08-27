pub const AUTH_KEYSPACE: &str = "auth";
pub const GROUP_KEYSPACE: &str = "groups";
pub const GROUP_OWNER_INDEX_KEYSPACE: &str = "group_owner_index";
pub const REALM_CONFIG_KEYSPACE: &str = "realm_config";
pub const METADATA_INDEX_KEYSPACE: &str = "metadata_index";
pub const METADATA_DOCUMENT_INDEX_KEYSPACE: &str = "metadata_document_index";
pub const METADATA_IRI_REFERENCE_INDEX_KEYSPACE: &str = "metadata_iri_reference_index";
pub const METADATA_HOLDERS_KEYSPACE: &str = "metadata_holders";
/// Time-ordered index of registry records by `updated_at_ms`, for OAI-PMH
/// datestamp enumeration (#320). Written atomically with each registry record.
pub const METADATA_UPDATED_INDEX_KEYSPACE: &str = "metadata_updated_index";
/// Generation-scoped, timestamp-ordered index of the records an anonymous caller
/// is currently authorized to read, so OAI-PMH enumeration never scans the
/// registry. Maintained out of band; readers re-check authorization per record.
pub const METADATA_VISIBILITY_INDEX_KEYSPACE: &str = "metadata_visibility_index";
/// The single state row naming the servable visibility-index generation. Absent
/// or not-ready means anonymous enumeration fails closed.
pub const METADATA_VISIBILITY_STATE_KEYSPACE: &str = "metadata_visibility_state";
pub const METADATA_AUDIT_KEYSPACE: &str = "metadata_audit";
pub const METADATA_EVENT_LOG_KEYSPACE: &str = "metadata_event_log";
pub const METADATA_CREATE_ACCEPTANCE_KEYSPACE: &str = "metadata_create_acceptance";
pub const METADATA_PENDING_PROJECTION_KEYSPACE: &str = "metadata_pending_projection";
pub const METADATA_DOCUMENT_LIFECYCLE_KEYSPACE: &str = "metadata_document_lifecycle";
pub const METADATA_GRAPH_LIFECYCLE_KEYSPACE: &str = "metadata_graph_lifecycle";
pub const METADATA_GRAPH_PRUNE_JOB_KEYSPACE: &str = "metadata_graph_prune_jobs";
pub const METADATA_MATERIALIZATION_STATUS_KEYSPACE: &str = "metadata_materialization_status";
pub const METADATA_PROFILE_VALIDATION_STATUS_KEYSPACE: &str = "metadata_profile_validation_status";
pub const METADATA_RAW_REVISION_KEYSPACE: &str = "metadata_raw_revisions";
pub const METADATA_RAW_BUDGET_KEYSPACE: &str = "metadata_raw_budgets";
pub const METADATA_MATERIALIZATION_JOB_KEYSPACE: &str = "metadata_materialization_jobs";
pub const METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE: &str =
    "metadata_materialization_document_jobs";
pub const METADATA_MATERIALIZATION_DEAD_LETTER_KEYSPACE: &str =
    "metadata_materialization_dead_letters";
pub const METADATA_MATERIALIZATION_PRUNE_KEYSPACE: &str = "metadata_materialization_prunes";
pub const ADMIN_DOCUMENT_STATE_KEYSPACE: &str = "admin_document_state";
pub const ADMIN_DOCUMENT_CONFLICT_KEYSPACE: &str = "admin_document_conflicts";
pub const DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE: &str = "document_sync_applied_ops";
pub const DOCUMENT_SYNC_OUTBOX_KEYSPACE: &str = "document_sync_outbox";
pub const TOKEN_REVOCATION_OUTBOX_INDEX_KEYSPACE: &str = "token_revocation_outbox_index";
/// Durable handoff for payloads recovered from a genesis tie-break. An entry
/// survives until every replacement outbox row is committed, so a crash between
/// the topic reset and the re-emission cannot lose an acknowledged write.
pub const DOCUMENT_SYNC_EVICTION_KEYSPACE: &str = "document_sync_evictions";
pub const DOCUMENT_SYNC_REVISION_KEYSPACE: &str = "document_sync_revisions";
pub const DOCUMENT_SYNC_CONFLICT_KEYSPACE: &str = "document_sync_conflicts";
/// Durable store for permanently-invalid replicated sync events (#338).
pub const SYNC_QUARANTINE_KEYSPACE: &str = "sync_quarantine";
/// Single-row record/byte accounting for the quarantine store, written in the
/// same batch as every quarantine row write and prune delete.
pub const SYNC_QUARANTINE_USAGE_KEYSPACE: &str = "sync_quarantine_usage";
pub const SYNC_PLACEMENT_KEYSPACE: &str = "sync_placements";
/// Per-bucket write-admission fence: the highest activation generation a
/// departing holder has closed. A holder-authoritative writer reads it inside
/// its own transaction, so a close conflicts every uncommitted predecessor write.
pub const PLACEMENT_WRITE_FENCE_KEYSPACE: &str = "placement_write_fence";
/// Immutable placement-policy documents a holder stores, keyed by policy id.
pub const PLACEMENT_POLICY_KEYSPACE: &str = "placement_policies";
/// Node-local policy cache keyed by `(policy_id, digest)`. An id-only key could
/// accept changed bytes under a known id, which policy immutability forbids.
pub const PLACEMENT_POLICY_CACHE_KEYSPACE: &str = "placement_policy_cache";
pub const SHARD_MANIFEST_KEYSPACE: &str = "shard_manifest";
pub const SHARD_VERIFICATION_KEYSPACE: &str = "shard_verification";
pub const TASK_TIMER_KEYSPACE: &str = "task_timers";
pub const USER_KEYSPACE: &str = "users";
pub const USER_SUBJECT_INDEX_KEYSPACE: &str = "user_subject_index";
pub const USER_SUBJECT_CLAIMS_KEYSPACE: &str = "user_subject_claims";

// Blob + S3 keyspaces
pub const BLOB_LOCATIONS_KEYSPACE: &str = "blob_locations";
pub const BLOB_CLEANUP_KEYSPACE: &str = "blob_pending_cleanups";
pub const BLOB_RECLAIM_KEYSPACE: &str = "blob_reclaim_candidates";
pub const BLOB_HIDDEN_RESERVATION_KEYSPACE: &str = "blob_hidden_reservations";
/// Durable evidence of a copy that failed hash/bao verification (§8.2), keyed
/// per (hash, backend) so re-hitting the same corrupt copy overwrites its row.
pub const BLOB_QUARANTINE_KEYSPACE: &str = "blob_quarantine";
/// Local inventory of the logical version copies this node has registered.
/// Written and removed atomically with the operation that exposes a copy
/// locally; it is never evidence about another node's copies.
pub const MANAGED_COPY_KEYSPACE: &str = "managed_copies";
pub const BLOB_HEAD_KEYSPACE: &str = "blob_heads";
pub const BLOB_VERSIONS_KEYSPACE: &str = "blob_versions";
pub const HASH_PATHS_INDEX_KEYSPACE: &str = "hash_paths_index";
pub const USER_ACCESS_KEYSPACE: &str = "user_access";
pub const USER_ACCESS_OWNER_KEYSPACE: &str = "user_access_owner";
pub const S3_SESSION_KEYSPACE: &str = "s3_sessions";
pub const S3_SESSION_OWNER_KEYSPACE: &str = "s3_session_owner";
pub const S3_SESSION_EXPIRY_KEYSPACE: &str = "s3_session_expiry";
pub const S3_BUCKET_KEYSPACE: &str = "s3_buckets";
pub const SYNC_RELATIONSHIP_OUT_KEYSPACE: &str = "sync_relationship_out";
pub const SYNC_RELATIONSHIP_IN_KEYSPACE: &str = "sync_relationship_in";
pub const SYNC_MIRROR_REPAIR_KEYSPACE: &str = "sync_mirror_repair";
pub const SYNC_REFERENCE_STATE_KEYSPACE: &str = "sync_reference_state";
pub const S3_MULTIPART_OBJECT_METADATA_KEYSPACE: &str = "s3_multipart_object_metadata";
pub const S3_MULTIPART_UPLOAD_KEYSPACE: &str = "s3_multipart_uploads";
pub const S3_MULTIPART_UPLOAD_PART_KEYSPACE: &str = "s3_multipart_upload_parts";
/// One active, scope-aware permanent-purge fence per bucket.
pub const S3_PURGE_FENCE_KEYSPACE: &str = "s3_purge_fences";
/// Durable inventory and batch counters for resumable permanent-purge jobs.
pub const S3_PURGE_CHECKPOINT_KEYSPACE: &str = "s3_purge_checkpoints";
pub const BLOB_REPLICATION_JOB_KEYSPACE: &str = "blob_replication_jobs";
pub const BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE: &str = "blob_live_replication_obligations";
pub const REFERENCE_METADATA_REFRESH_JOB_KEYSPACE: &str = "reference_metadata_refresh_jobs";
pub const USAGE_STATS_KEYSPACE: &str = "usage_stats";
pub const USAGE_NODE_STATS_KEYSPACE: &str = "usage_node_stats";
pub const NODE_INFO_KEYSPACE: &str = "node_info";
/// Single-row local placement subject and its generation. Governed writes and
/// internal serves are evaluated against it; a rejoin blocks serving here.
pub const NODE_SUBJECT_KEYSPACE: &str = "node_subject";
pub const NOTIFICATION_INBOX_KEYSPACE: &str = "notification_inbox";
pub const NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE: &str = "notification_inbox_prune_index";
pub const NOTIFICATION_OUTBOX_KEYSPACE: &str = "notification_outbox";
pub const NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE: &str = "notification_watch_subscriptions";
pub const NOTIFICATION_WATCH_INTEREST_KEYSPACE: &str = "notification_watch_interest";

pub const GROUP_STORAGE_ROUTING_KEYSPACE: &str = "group_storage_routing";

/// Keyed by backend id alone: the blob adapter resolves a stored
/// `BackendRef::Group` without knowing which group owns it.
pub const GROUP_STORAGE_BACKEND_KEYSPACE: &str = "group_storage_backend";
pub const GROUP_STORAGE_BACKEND_SECRET_KEYSPACE: &str = "group_storage_backend_secret";

/// The same records keyed by `group id || backend id`, so routing a write reads
/// one group's backends instead of every tenant's. Written in the same batch or
/// transaction as the id-keyed record it mirrors.
pub const GROUP_STORAGE_BACKEND_INDEX_KEYSPACE: &str = "group_storage_backend_index";

/// Device-local registrations of the directories this node offers as read-only
/// buckets, keyed by bucket name. Never replicated: the root path is the one
/// fact about the owner's machine that must not leave it.
pub const OFFERED_DIRECTORY_KEYSPACE: &str = "offered_directories";

/// Authoring intents the owner queued on the device while the realm was
/// unreachable, keyed by local draft id. Never replicated: an entry becomes
/// realm state only when the drain forwards it as an ordinary create.
pub const DEVICE_INTAKE_KEYSPACE: &str = "device_intake";

/// The realm-config clock the realm documents this device holds were copied at,
/// keyed by realm id. It is what keeps a later copy from rolling the device
/// back, and it is never replicated.
pub const DEVICE_REALM_MARKER_KEYSPACE: &str = "device_realm_marker";

/// The api urls of the realm's management nodes, as a realm node served them,
/// keyed by realm id. A device holds no peer node-info document, so this is the
/// only address it has for a management-only route.
pub const DEVICE_MANAGEMENT_URL_KEYSPACE: &str = "device_management_urls";

/// The one row a device keeps about its exchange with the realm: when the realm
/// last answered, when the last pass finished, and whether one is in flight.
pub const DEVICE_SYNC_STATE_KEYSPACE: &str = "device_sync_state";

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
pub const SYNC_UPLOAD_OUTBOX_KEYSPACE: &str = "sync_upload_outbox";

/// Append-only record of the explicit owner actions that replaced or removed
/// local bytes, keyed by `folder id || action id`.
pub const SYNC_ACTION_LOG_KEYSPACE: &str = "sync_action_log";

pub const SOURCE_CONNECTOR_INDEX_KEYSPACE: &str = "source_connector_index";
pub const SOURCE_CONNECTOR_SECRET_KEYSPACE: &str = "source_connector_secret";

// Repository interop: metadata harvest framework (#442).
pub const REPOSITORY_CONNECTOR_INDEX_KEYSPACE: &str = "repository_connector_index";
pub const REPOSITORY_CONNECTOR_SECRET_KEYSPACE: &str = "repository_connector_secret";
pub const HARVEST_SOURCE_KEYSPACE: &str = "harvest_source";
pub const HARVEST_PROVENANCE_KEYSPACE: &str = "harvest_provenance";
/// w3id persistent-identifier mappings, keyed by document id (#442, spec 3.5).
pub const PERSISTENT_ID_MAPPING_KEYSPACE: &str = "persistent_id_mapping";

// Durable job framework keyspaces (#318).
pub const JOB_KEYSPACE: &str = "jobs";
pub const JOB_SCHEDULE_INDEX_KEYSPACE: &str = "job_schedule_index";
pub const JOB_OWNER_INDEX_KEYSPACE: &str = "job_owner_index";
pub const JOB_ACTIVE_USER_KEYSPACE: &str = "job_active_user";
pub const JOB_DEDUP_INDEX_KEYSPACE: &str = "job_dedup_index";
pub const JOB_RUN_CRATE_KEYSPACE: &str = "job_run_crate";
pub const JOB_ATTEMPT_CONTROL_KEYSPACE: &str = "job_attempt_control";
/// Signed immutable output records, keyed by ExecutionId.
pub const JOB_OUTPUT_RECORD_KEYSPACE: &str = "job_output_records";
pub const JOB_ENTRY_KEYSPACE: &str = "job_entries";

// Append-only distributed job-record store.
/// Immutable authentic record envelopes, keyed by `JobRecordKey`. A key is
/// written once: the same digest replays as a no-op and a different digest is
/// retained in the conflict keyspace instead of overwriting it.
pub const JOB_FAMILY_RECORD_KEYSPACE: &str = "job_family_records";
/// Bounded records whose predecessor evidence, or whose local holder view, is
/// not available yet. A pending record is never projected or relayed.
pub const JOB_FAMILY_PENDING_KEYSPACE: &str = "job_family_pending";
/// Explicit same-key/different-digest evidence, keyed by record key and digest.
/// Quarantined records stay auditable and never enter a projection.
pub const JOB_FAMILY_CONFLICT_KEYSPACE: &str = "job_family_conflicts";
/// Alias index: one accepted `JobId` to the request family that admitted it.
pub const JOB_FAMILY_ALIAS_KEYSPACE: &str = "job_family_aliases";
/// Per-family projection cache and its bounded revision. Derived state only; it
/// is rebuilt from the immutable records and is never authority.
pub const JOB_FAMILY_PROJECTION_KEYSPACE: &str = "job_family_projections";
/// Locally published authentic records awaiting family replication, keyed by
/// record key. Only a replicated-authority record is ever queued here.
pub const JOB_FAMILY_OUTBOX_KEYSPACE: &str = "job_family_outbox";
/// Exact local capacity held for one accepted execution, keyed by ExecutionId.
/// The row is written with the signed receipt and released at terminal state.
pub const JOB_RESERVATION_KEYSPACE: &str = "job_reservations";
pub const JOB_ADMISSION_QUOTA_KEYSPACE: &str = "job_admission_quota";
/// Persisted witness fallback deadlines, keyed by due time and family, so a
/// later-ranked witness still plans after a restart.
pub const JOB_WITNESS_DEADLINE_KEYSPACE: &str = "job_witness_deadlines";
/// Current witness deadline by family; the due-time rows are the scan index.
pub const JOB_WITNESS_DEADLINE_INDEX_KEYSPACE: &str = "job_witness_deadline_index";
/// Bounded explain record of the plan a witness sealed before it launched.
pub const JOB_PLAN_EXPLAIN_KEYSPACE: &str = "job_plan_explains";
pub const JOB_ARTIFACT_TOMBSTONE_KEYSPACE: &str = "job_artifact_tombstones";
/// The single row recording what this node could not resolve when it departed.
pub const COMPUTE_DEPARTURE_KEYSPACE: &str = "compute_departure";
pub const ROCRATE_JOB_STATE_KEYSPACE: &str = "rocrate_job_state";
pub const ROCRATE_UPLOAD_KEYSPACE: &str = "rocrate_uploads";
pub const ROCRATE_UPLOAD_CLEANUP_KEYSPACE: &str = "rocrate_upload_cleanups";
pub const STAGING_JOB_STATE_KEYSPACE: &str = "staging_job_state";

pub const BUCKET_STATS_DB: &str = "bucket_stats";

pub const API_STATE_KEYSPACE: &str = "api_state";
pub const NODE_STATE_KEYSPACE: &str = "node_state";
pub const ONBOARDING_KEYSPACE: &str = "onboarding";
pub const DHT_KEYSPACE: &str = "dht_v2";
pub const CRAQLE_TERMS_KEYSPACE: &str = "terms";
pub const CRAQLE_QUADS_KEYSPACE: &str = "quads";
pub const CRAQLE_GRAPHS_KEYSPACE: &str = "graphs";
pub const CRAQLE_LOG_KEYSPACE: &str = "log";

/// Cleanup keyspaces record the work a finished transaction still owes, so
/// storage must admit their writes ahead of the ordinary write queue.
pub fn is_cleanup_keyspace(key_space: &str) -> bool {
    key_space == BLOB_CLEANUP_KEYSPACE
}

#[cfg(test)]
mod tests {
    use super::{BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, is_cleanup_keyspace};

    #[test]
    fn classifies_cleanup_keyspace() {
        assert!(is_cleanup_keyspace(BLOB_CLEANUP_KEYSPACE));
        assert!(!is_cleanup_keyspace(BLOB_LOCATIONS_KEYSPACE));
    }
}
