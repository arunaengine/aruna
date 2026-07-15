pub const AUTH_KEYSPACE: &str = "auth";
pub const GROUP_KEYSPACE: &str = "groups";
pub const GROUP_OWNER_INDEX_KEYSPACE: &str = "group_owner_index";
pub const REALM_CONFIG_KEYSPACE: &str = "realm_config";
pub const METADATA_INDEX_KEYSPACE: &str = "metadata_index";
pub const METADATA_DOCUMENT_INDEX_KEYSPACE: &str = "metadata_document_index";
pub const METADATA_IRI_REFERENCE_INDEX_KEYSPACE: &str = "metadata_iri_reference_index";
pub const METADATA_HOLDERS_KEYSPACE: &str = "metadata_holders";
pub const METADATA_AUDIT_KEYSPACE: &str = "metadata_audit";
pub const METADATA_EVENT_LOG_KEYSPACE: &str = "metadata_event_log";
pub const METADATA_CREATE_ACCEPTANCE_KEYSPACE: &str = "metadata_create_acceptance";
pub const METADATA_PENDING_PROJECTION_KEYSPACE: &str = "metadata_pending_projection";
pub const METADATA_DOCUMENT_LIFECYCLE_KEYSPACE: &str = "metadata_document_lifecycle";
pub const METADATA_GRAPH_LIFECYCLE_KEYSPACE: &str = "metadata_graph_lifecycle";
pub const METADATA_GRAPH_PRUNE_JOB_KEYSPACE: &str = "metadata_graph_prune_jobs";
pub const METADATA_MATERIALIZATION_STATUS_KEYSPACE: &str = "metadata_materialization_status";
pub const METADATA_MATERIALIZATION_JOB_KEYSPACE: &str = "metadata_materialization_jobs";
pub const METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE: &str =
    "metadata_materialization_document_jobs";
pub const ADMIN_DOCUMENT_STATE_KEYSPACE: &str = "admin_document_state";
pub const ADMIN_DOCUMENT_CONFLICT_KEYSPACE: &str = "admin_document_conflicts";
pub const DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE: &str = "document_sync_applied_ops";
pub const DOCUMENT_SYNC_OUTBOX_KEYSPACE: &str = "document_sync_outbox";
pub const DOCUMENT_SYNC_REVISION_KEYSPACE: &str = "document_sync_revisions";
pub const DOCUMENT_SYNC_CONFLICT_KEYSPACE: &str = "document_sync_conflicts";
pub const SYNC_PLACEMENT_KEYSPACE: &str = "sync_placements";
pub const SHARD_MANIFEST_KEYSPACE: &str = "shard_manifest";
pub const SHARD_VERIFICATION_KEYSPACE: &str = "shard_verification";
pub const TASK_TIMER_KEYSPACE: &str = "task_timers";
pub const USER_KEYSPACE: &str = "users";
pub const USER_SUBJECT_INDEX_KEYSPACE: &str = "user_subject_index";
pub const USER_SUBJECT_CLAIMS_KEYSPACE: &str = "user_subject_claims";

// Blob + S3 keyspaces
pub const BLOB_LOCATIONS_KEYSPACE: &str = "blob_locations";
pub const BLOB_HEAD_KEYSPACE: &str = "blob_heads";
pub const BLOB_VERSIONS_KEYSPACE: &str = "blob_versions";
pub const HASH_PATHS_INDEX_KEYSPACE: &str = "hash_paths_index";
pub const USER_ACCESS_KEYSPACE: &str = "user_access";
pub const S3_BUCKET_KEYSPACE: &str = "s3_buckets";
pub const S3_BUCKET_REPLICATION_KEYSPACE: &str = "s3_bucket_replication";
pub const S3_MULTIPART_OBJECT_METADATA_KEYSPACE: &str = "s3_multipart_object_metadata";
pub const S3_MULTIPART_UPLOAD_KEYSPACE: &str = "s3_multipart_uploads";
pub const S3_MULTIPART_UPLOAD_PART_KEYSPACE: &str = "s3_multipart_upload_parts";
pub const BLOB_REPLICATION_JOB_KEYSPACE: &str = "blob_replication_jobs";
pub const BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE: &str = "blob_live_replication_obligations";
pub const REFERENCE_METADATA_REFRESH_JOB_KEYSPACE: &str = "reference_metadata_refresh_jobs";
pub const USAGE_STATS_KEYSPACE: &str = "usage_stats";
pub const USAGE_NODE_STATS_KEYSPACE: &str = "usage_node_stats";
pub const NODE_INFO_KEYSPACE: &str = "node_info";
pub const NOTIFICATION_INBOX_KEYSPACE: &str = "notification_inbox";
pub const NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE: &str = "notification_inbox_prune_index";
pub const NOTIFICATION_OUTBOX_KEYSPACE: &str = "notification_outbox";
pub const NOTIFICATION_WATCH_SUBSCRIPTIONS_KEYSPACE: &str = "notification_watch_subscriptions";
pub const NOTIFICATION_WATCH_INTEREST_KEYSPACE: &str = "notification_watch_interest";

pub const SOURCE_CONNECTOR_INDEX_KEYSPACE: &str = "source_connector_index";
pub const SOURCE_CONNECTOR_SECRET_KEYSPACE: &str = "source_connector_secret";

// Durable job framework keyspaces (#318).
pub const JOB_KEYSPACE: &str = "jobs";
pub const JOB_SCHEDULE_INDEX_KEYSPACE: &str = "job_schedule_index";
pub const JOB_OWNER_INDEX_KEYSPACE: &str = "job_owner_index";
pub const JOB_DEDUP_INDEX_KEYSPACE: &str = "job_dedup_index";
pub const JOB_RUN_CRATE_KEYSPACE: &str = "job_run_crate";
pub const JOB_ATTEMPT_CONTROL_KEYSPACE: &str = "job_attempt_control";

pub const BUCKET_STATS_DB: &str = "bucket_stats";

pub const API_STATE_KEYSPACE: &str = "api_state";
pub const NODE_STATE_KEYSPACE: &str = "node_state";
pub const ONBOARDING_KEYSPACE: &str = "onboarding";
pub const DHT_KEYSPACE: &str = "dht";
pub const CRAQLE_TERMS_KEYSPACE: &str = "terms";
pub const CRAQLE_QUADS_KEYSPACE: &str = "quads";
pub const CRAQLE_GRAPHS_KEYSPACE: &str = "graphs";
pub const CRAQLE_LOG_KEYSPACE: &str = "log";
