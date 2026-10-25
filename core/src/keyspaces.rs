//! Names the persistent keyspaces the node stores its records in.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub const AUTH_KEYSPACE: &str = "auth";
pub const GROUP_KEYSPACE: &str = "groups";
pub const GROUP_DELETE_KEYSPACE: &str = "group_deletions";
pub const OWNER_INDEX_KEYSPACE: &str = "group_owner_index";
pub const REALM_CONFIG_KEYSPACE: &str = "realm_config";
pub const METADATA_INDEX_KEYSPACE: &str = "metadata_index";
pub const DOCUMENT_INDEX_KEYSPACE: &str = "metadata_document_index";
pub const IRI_INDEX_KEYSPACE: &str = "metadata_iri_reference_index";
/// Data link postings by identifier, then document: what a materialized document links to.
pub const DATA_LINK_KEYSPACE: &str = "metadata_data_links";
/// The same postings by document first, so a materialization replaces exactly its own rows.
pub const DOCUMENT_LINK_KEYSPACE: &str = "metadata_document_links";
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
/// The origin and encoded size of each logged event, keyed like the event log.
pub const EVENT_SIZE_KEYSPACE: &str = "metadata_event_sizes";
/// One row per metadata checkpoint, keyed by document and event; the last row starts
/// the document's current history window.
pub const METADATA_CHECKPOINT_KEYSPACE: &str = "metadata_checkpoints";
/// Commit messages authors gave for metadata events, keyed by document and event.
pub const COMMIT_MESSAGE_KEYSPACE: &str = "metadata_commit_messages";
/// This node's reused CRDT actor per metadata document.
pub const METADATA_ACTOR_KEYSPACE: &str = "metadata_actors";
/// Replicated Git records of metadata documents, keyed by document and record id.
pub const GIT_RECORD_KEYSPACE: &str = "git_records";
/// Git pack bytes by document and SHA-256; packs stay small because large files use LFS.
pub const GIT_PACK_KEYSPACE: &str = "git_packs";
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
/// Database marks waiting for owner consent on this management node, by request id.
pub const DATABASE_REQUEST_KEYSPACE: &str = "database_requests";
/// Reduced distribution records by set id, materialized from their admin reducer state.
pub const DISTRIBUTION_KEYSPACE: &str = "distribution_records";
/// Requests a set's publisher accepted, by set id and request id, with the revision they made.
pub const DISTRIBUTION_ACCEPT_KEYSPACE: &str = "distribution_accepted";
/// The installed head revision of each set by publisher then set id, for removal cutoffs.
pub const DISTRIBUTION_PUBLISHER_KEYSPACE: &str = "distribution_publishers";
/// Snapshots whose publisher the realm config does not know yet, by publisher and set id.
pub const DISTRIBUTION_PARKED_KEYSPACE: &str = "distribution_parked";
/// The revision number of each parked snapshot, so the bound is counted without the bodies.
pub const PARKED_INDEX_KEYSPACE: &str = "distribution_parked_index";
/// Ordered inventory of each distributed bucket: head and version rows by set id and position.
pub const INVENTORY_KEYSPACE: &str = "distribution_inventory";
/// Open multipart uploads of each distributed bucket by set id, key and upload id.
pub const UPLOAD_INVENTORY_KEYSPACE: &str = "distribution_uploads";
/// Version id to inventory position, by set id and version id.
pub const VERSION_INDEX_KEYSPACE: &str = "distribution_version_index";
/// Versions of a distributed bucket by content identity: set id, identity, version id.
pub const CONTENT_INDEX_KEYSPACE: &str = "distribution_content_index";
/// The installed key ranges of each set by set id and exclusive upper bound.
pub const RANGE_INDEX_KEYSPACE: &str = "distribution_ranges";
/// The accounting row of each range: active generation, rebuild cursors and split state.
pub const RANGE_ACCOUNT_KEYSPACE: &str = "distribution_range_accounts";
/// Delta-merged counter rows of each range by generation, and pending split rows.
pub const RANGE_COUNTER_KEYSPACE: &str = "distribution_range_counters";
/// The bounded key sample of each range and its refill cursor.
pub const KEY_SAMPLE_KEYSPACE: &str = "distribution_key_samples";
/// Spent delegation nonces by expiry second then nonce, kept until they cannot verify anymore.
pub const DELEGATION_NONCE_KEYSPACE: &str = "distribution_delegation_nonces";
/// Sets whose record names a catching-up member, by set id, until their bootstrap is settled.
pub const CATCH_UP_KEYSPACE: &str = "distribution_catch_up";
/// Bootstrap cursors of catching-up members by set id, member id and range id.
pub const BOOTSTRAP_KEYSPACE: &str = "distribution_bootstrap";
/// Copy-on-use attachments by set id and version id: a verified local copy a GET reads.
pub const ATTACHMENT_KEYSPACE: &str = "distribution_attachments";
/// Cache-on-use entries by the logical identity of their version, and the cache totals row.
pub const CACHE_ENTRY_KEYSPACE: &str = "distribution_cache_entries";
/// Cache entries in admission order, by entry ULID, so eviction starts at the oldest.
pub const CACHE_ORDER_KEYSPACE: &str = "distribution_cache_order";
/// Claims on filling one kept copy, so concurrent reads fetch it once.
pub const FILL_CLAIM_KEYSPACE: &str = "distribution_fill_claims";
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
/// Current vault heads a holder keeps, keyed by user and revision id.
pub const VAULT_REVISION_KEYSPACE: &str = "vault_revisions";
/// Revision ids a later save replaced, so a replayed old save never returns as a head.
pub const VAULT_RETIRED_KEYSPACE: &str = "vault_retired";
/// Public key records of users, keyed by user and record id.
pub const USER_KEY_KEYSPACE: &str = "user_keys";
pub const ASSISTANT_PROVIDER_KEYSPACE: &str = "assistant_providers";
pub const PROVIDER_OWNER_KEYSPACE: &str = "assistant_provider_owner";
pub const CHAT_HEAD_KEYSPACE: &str = "assistant_chat_heads";
pub const CHAT_TURN_KEYSPACE: &str = "assistant_chat_turns";

// Blob + S3 keyspaces
pub const BLOB_LOCATIONS_KEYSPACE: &str = "blob_locations";
pub const BLOB_CLEANUP_KEYSPACE: &str = "blob_pending_cleanups";
pub const BLOB_RECLAIM_KEYSPACE: &str = "blob_reclaim_candidates";
/// Node-local progress of re-encoding a bucket's copies after a compression change.
pub const COMPRESSION_MIGRATION_KEYSPACE: &str = "compression_migrations";
/// Buckets whose compression migration is unfinished; the worker scans only these.
pub const COMPRESSION_QUEUE_KEYSPACE: &str = "compression_migration_queue";
/// Moves asked to rename linked files of earlier patterns: the start time of the move record
/// they belong to, keyed by bucket name.
pub const RENAME_MOVE_KEYSPACE: &str = "compression_migration_renames";
/// The files a rename planned to rename, keyed by bucket name: a `RenameTotal`.
pub const RENAME_TOTAL_KEYSPACE: &str = "compression_migration_totals";
/// Where an ending storage move stands in handing its intents over, keyed by bucket name.
pub const MOVE_HANDOVER_KEYSPACE: &str = "compression_migration_handovers";
/// Version rows a migration run finished after its cursor, keyed by bucket name: a
/// `MigrationAhead`.
pub const MIGRATION_AHEAD_KEYSPACE: &str = "compression_migration_ahead";
/// Where the next pass over the blob cleanup queue starts.
pub const CLEANUP_CURSOR_KEYSPACE: &str = "blob_cleanup_cursor";
/// The re-mapping or duplicate cleanup job of each bucket, keyed by bucket name.
pub const LINKED_JOB_KEYSPACE: &str = "linked_storage_jobs";
/// Backend versions of linked files a release deletes, claimed before its last checks and kept
/// as tombstones once deleted; no new reader may name a claimed version.
pub const DELETE_CLAIM_KEYSPACE: &str = "linked_delete_claims";
/// The delete claims by the rule whose file they name, keyed by import id and claim key.
pub const CLAIM_INDEX_KEYSPACE: &str = "linked_delete_claim_rules";
/// Import rows a key rename moved to a key, keyed by the import id, the key's version prefix
/// and the row's path; they go with their rule.
pub const RENAMED_ROW_KEYSPACE: &str = "backend_import_renamed_rows";
/// Node-local progress of checking a bucket's current versions for copies that cannot serve.
pub const STORAGE_CHECK_KEYSPACE: &str = "storage_checks";
/// Buckets whose check is unfinished; the migration worker scans only these.
pub const CHECK_QUEUE_KEYSPACE: &str = "storage_check_queue";
/// Versions a check found broken, keyed by version key so one bucket is one prefix.
pub const BROKEN_VERSION_KEYSPACE: &str = "broken_versions";
pub const HIDDEN_RESERVATION_KEYSPACE: &str = "blob_hidden_reservations";
/// Key generations of encrypted buckets, keyed by bucket id and generation. Public keys only.
pub const BUCKET_KEY_KEYSPACE: &str = "bucket_keys";

pub const ABE_PARAMETERS_KEYSPACE: &str = "abe_parameters";
pub const ABE_EPOCH_KEYSPACE: &str = "abe_epochs";
pub const ABE_ENVELOPE_KEYSPACE: &str = "abe_envelopes";
pub const ABE_VERSION_KEYSPACE: &str = "abe_versions";
pub const ABE_ARCHIVE_KEYSPACE: &str = "abe_archives";
pub const ABE_REQUEST_KEYSPACE: &str = "abe_requests";
pub const ABE_GRANT_KEYSPACE: &str = "abe_grants";
/// Envelopes of open multipart uploads, keyed by upload id. Never served while pending.
pub const ABE_PENDING_KEYSPACE: &str = "abe_pending";
/// Same-bucket copies published while locked, keyed by version. Never served while pending.
pub const ABE_COPY_KEYSPACE: &str = "abe_copies";
/// Key holders of encrypted buckets, keyed by bucket id and user.
pub const BUCKET_HOLDER_KEYSPACE: &str = "bucket_holders";
/// Bucket private keys sealed to user keys, keyed by bucket id, generation and holder.
pub const KEY_COPY_KEYSPACE: &str = "bucket_key_copies";
/// Encryption settings of node-local buckets, keyed by bucket name. No row means off.
pub const BUCKET_ENCRYPTION_KEYSPACE: &str = "bucket_encryption";
/// Encrypted node-local buckets of each group, keyed by group id and bucket name. Values are empty.
pub const GROUP_ENCRYPTED_KEYSPACE: &str = "group_encrypted_buckets";
/// Key state audit of encrypted buckets, keyed by bucket id and time-ordered event id.
pub const BUCKET_AUDIT_KEYSPACE: &str = "bucket_audit";
/// Complete locations of archives whose content hash is unknown, keyed by archive id and backend.
pub const PENDING_LOCATION_KEYSPACE: &str = "pending_locations";
/// The versions that use each Pithos archive, keyed by archive and version.
pub const COPY_OWNER_KEYSPACE: &str = "blob_copy_owners";
/// Node-local progress of moving a bucket's copies to new encryption, keyed by bucket name.
pub const TRANSITION_KEYSPACE: &str = "encryption_transitions";
/// Buckets whose encryption transition is unfinished; the worker scans only these.
pub const TRANSITION_QUEUE_KEYSPACE: &str = "encryption_transition_queue";
/// Old copies a transition still waits to see removed, keyed by bucket and location key.
pub const TRANSITION_CLEANUP_KEYSPACE: &str = "encryption_transition_cleanups";
/// Bucket keys parked jobs wait for: per-job lists and a wake index by key reference.
pub const KEY_WAIT_KEYSPACE: &str = "job_key_waits";
/// When each pending archive was first seen without owners, keyed by archive.
pub const PENDING_RECLAIM_KEYSPACE: &str = "pending_reclaims";
/// The weakened recovery each encrypted bucket's holders were last told about, by bucket id.
pub const BUCKET_RECOVERY_KEYSPACE: &str = "bucket_recovery";
/// Key requests and grants of each S3 credential's token, keyed by access key, a zero byte and
/// the request key. Values are empty.
pub const TOKEN_GRANT_KEYSPACE: &str = "token_grants";
/// The content hash a sender claimed for a received pending archive, keyed by archive.
pub const PENDING_CLAIM_KEYSPACE: &str = "pending_claims";
/// Replication jobs waiting for a locked source key, keyed by key reference and job key.
pub const COPY_WAIT_KEYSPACE: &str = "replication_key_waits";
/// Plaintext copy requests of encrypted sources, keyed by job key or relationship id.
pub const PLAINTEXT_COPY_KEYSPACE: &str = "replication_plaintext";
/// Buckets whose epoch must rise after a lost READ scope, keyed by bucket id. Values are times.
pub const ABE_DUE_KEYSPACE: &str = "abe_epoch_due";
/// Key reissue progress after an epoch raise, keyed by bucket id: epoch, phase and cursor.
pub const ABE_REISSUE_KEYSPACE: &str = "abe_reissues";
/// Scoped re-key progress, keyed by bucket id: prefix, epoch, cursor and count.
pub const ABE_REKEY_KEYSPACE: &str = "abe_rekeys";
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
/// The same trail by object key and purges by group, written with each audit row, so the
/// records about one key read without the rest of the trail.
pub const DELETE_TRAIL_KEYSPACE: &str = "blob_delete_trail";
pub const PATHS_INDEX_KEYSPACE: &str = "hash_paths_index";
pub const USER_ACCESS_KEYSPACE: &str = "user_access";
pub const ACCESS_OWNER_KEYSPACE: &str = "user_access_owner";
pub const S3_SESSION_KEYSPACE: &str = "s3_sessions";
pub const SESSION_OWNER_KEYSPACE: &str = "s3_session_owner";
pub const SESSION_EXPIRY_KEYSPACE: &str = "s3_session_expiry";
pub const S3_BUCKET_KEYSPACE: &str = "s3_buckets";
/// The creation id of each bucket by name, fresh on every creation of the name.
pub const BUCKET_CREATION_KEYSPACE: &str = "s3_bucket_creations";
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
/// Linked-storage files a purge releases once its versions are gone, and its pause marker,
/// keyed by job id.
pub const PURGE_FILES_KEYSPACE: &str = "s3_purge_files";
pub const REPLICATION_JOB_KEYSPACE: &str = "blob_replication_jobs";
pub const REPLICATION_OBLIGATION_KEYSPACE: &str = "blob_live_replication_obligations";
pub const REFRESH_JOB_KEYSPACE: &str = "reference_metadata_refresh_jobs";
/// Due-time index of reference refresh jobs: big-endian due milliseconds, then the job key.
pub const REFRESH_DUE_KEYSPACE: &str = "reference_metadata_refresh_due";
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

/// Node-local backend import rules, keyed by `group id || import id`.
pub const BACKEND_IMPORT_KEYSPACE: &str = "backend_imports";
/// What each import run recorded per backend key, keyed by `import id || backend key`.
pub const IMPORTED_OBJECT_KEYSPACE: &str = "backend_import_objects";
/// The backend version id of bytes at a path of a tenant's folders, keyed by location ulid.
pub const BACKEND_VERSION_KEYSPACE: &str = "backend_versions";
/// What a write to an import folder will be, recorded before its bytes, keyed like its row.
pub const IMPORT_INTENT_KEYSPACE: &str = "backend_import_intents";
/// Earlier write patterns of an import rule that may still name its files, keyed by import id.
pub const IMPORT_PATTERN_KEYSPACE: &str = "backend_import_patterns";
/// Removed import rules whose object rows are not all deleted yet, keyed by import id.
pub const IMPORT_REMOVAL_KEYSPACE: &str = "backend_import_removals";
/// File name discovery rules of import rules, keyed like the rule.
pub const IMPORT_DISCOVERY_KEYSPACE: &str = "backend_import_discovery";

/// Device-local registrations of the directories this node offers as read-only
/// buckets, keyed by bucket name. Never replicated: the root path is the one
/// detail about the owner's machine that must not leave it.
pub const OFFERED_DIRECTORY_KEYSPACE: &str = "offered_directories";

/// Authoring intents the owner queued on the device while the realm was
/// unreachable, keyed by local draft id. Never replicated: an entry becomes
/// realm state only when the drain forwards it as an ordinary create.
pub const DEVICE_INTAKE_KEYSPACE: &str = "device_intake";

/// Issue time of the token that authored each queued draft, keyed like `device_intake`.
/// The drain sends it along, so a holder can apply the owner's token cutoff.
pub const DEVICE_FENCE_KEYSPACE: &str = "device_intake_fence";

/// The realm-config clock the realm documents this device holds were copied at,
/// keyed by realm id. It is what keeps a later copy from rolling the device
/// back, and it is never replicated.
pub const REALM_MARKER_KEYSPACE: &str = "device_realm_marker";

/// The api urls of the realm's management nodes, as a realm node served them,
/// keyed by realm id. A device holds no peer node-info document, so this is the
/// only address it has for a management-only route.
pub const MANAGEMENT_URL_KEYSPACE: &str = "device_management_urls";

/// Realm-document fetches that every realm node refused for realm membership, keyed by realm
/// id. Two without a successful fetch between them report the device as removed.
pub const REALM_REMOVAL_KEYSPACE: &str = "device_realm_removal";

/// The one row a device keeps about its exchange with the realm: when the realm
/// last answered, when the last pass finished, and whether one is in flight.
pub const SYNC_STATE_KEYSPACE: &str = "device_sync_state";

/// The metadata documents this device keeps a local craqle replica of, keyed
/// by document id. Never replicated: it records what the owner selected and
/// how far this device has synced each replica.
pub const DEVICE_REPLICA_KEYSPACE: &str = "device_replica";

/// Bytes of the local graph each device replica installed, keyed like `device_replica`. The
/// graph stays when its replica row goes, so its bytes stay counted against the cache budget.
pub const DEVICE_GRAPH_KEYSPACE: &str = "device_graph_size";

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

/// The realm version a node committed for one device version, keyed by target bucket and
/// key, source device and version, and the delete flag. Written with the version it names.
pub const SYNC_RECEIPT_KEYSPACE: &str = "sync_pull_receipts";

/// Which local file each realm version this device published came from, keyed by realm
/// version id. Never replicated: the path must not leave the machine.
pub const SYNC_ORIGIN_KEYSPACE: &str = "sync_local_origins";

pub const SOURCE_INDEX_KEYSPACE: &str = "source_connector_index";
pub const SOURCE_SECRET_KEYSPACE: &str = "source_connector_secret";
/// One row per reference version bound to a connector, keyed by connector id first.
pub const CONNECTOR_REFERENCE_KEYSPACE: &str = "source_connector_references";
/// Connectors whose stale reference rows a background task prunes, with its position.
pub const CONNECTOR_PRUNE_KEYSPACE: &str = "source_connector_prune_queue";

// Repository interop: metadata harvest framework (#442).
pub const CONNECTOR_INDEX_KEYSPACE: &str = "repository_connector_index";
pub const CONNECTOR_SECRET_KEYSPACE: &str = "repository_connector_secret";
pub const HARVEST_SOURCE_KEYSPACE: &str = "harvest_source";
pub const HARVEST_PROVENANCE_KEYSPACE: &str = "harvest_provenance";
/// w3id persistent-identifier mappings, keyed by document id (#442, spec 3.5).
pub const ID_MAPPING_KEYSPACE: &str = "persistent_id_mapping";
/// Reverse index of secondary identifiers, `kind 0 value 0 endpoint` to document id (#451).
pub const SECONDARY_ID_KEYSPACE: &str = "persistent_id_secondary";
/// Repository links on their owner node, keyed by `document id || link id`.
pub const REPOSITORY_LINK_KEYSPACE: &str = "repository_links";
/// A link's sealed personal token, keyed by link id; removed with the link.
pub const LINK_SECRET_KEYSPACE: &str = "repository_link_secrets";
/// Links per repository connector, `connector id || link id`, so a used connector stays.
pub const LINK_CONNECTOR_KEYSPACE: &str = "repository_link_connectors";
/// Durable push checks per link id, drained by `TaskKey::DrainLinkQueue`.
pub const LINK_QUEUE_KEYSPACE: &str = "repository_link_queue";

// Durable job framework keyspaces (#318).
pub const JOB_KEYSPACE: &str = "jobs";
pub const SCHEDULE_INDEX_KEYSPACE: &str = "job_schedule_index";
pub const JOB_INDEX_KEYSPACE: &str = "job_owner_index";
pub const ACTIVE_USER_KEYSPACE: &str = "job_active_user";
pub const DEDUP_INDEX_KEYSPACE: &str = "job_dedup_index";
pub const RUN_CRATE_KEYSPACE: &str = "job_run_crate";

/// The realm target a device froze for publishing one local run, and the copies it made,
/// keyed like `job_run_crate`. Device-local and never replicated.
pub const RUN_PUBLISH_KEYSPACE: &str = "job_run_publish";
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
/// Directories a staging job still has to list, keyed by job id and queue position.
pub const STAGING_QUEUE_KEYSPACE: &str = "staging_directory_queue";
/// Digests a staging job read from checksum manifests, keyed by job id and source path.
pub const STAGING_MANIFEST_KEYSPACE: &str = "staging_manifest_digests";

pub const BUCKET_STATS_DB: &str = "bucket_stats";
/// Provider buckets below their object limit, keyed by backend, load and bucket name.
pub const BUCKET_FREE_INDEX: &str = "bucket_free_index";

pub const API_STATE_KEYSPACE: &str = "api_state";
pub const NODE_STATE_KEYSPACE: &str = "node_state";
/// Node-local registry publication state: the withdrawals sent since registration was disabled.
pub const FEDERATION_KEYSPACE: &str = "federation_publication";
/// Secrets this node opens by itself, sealed with its node vault key. Never replicated.
pub const NODE_VAULT_KEYSPACE: &str = "node_vault";
/// The one row of `NODE_STATE_KEYSPACE` that holds the node's identity.
pub const NODE_STATE_KEY: &[u8] = b"node_state";
pub const ONBOARDING_KEYSPACE: &str = "onboarding";
pub const DHT_KEYSPACE: &str = "dht_v2";
pub const CRAQLE_TERMS_KEYSPACE: &str = "terms";
pub const CRAQLE_QUADS_KEYSPACE: &str = "quads";
pub const CRAQLE_GRAPHS_KEYSPACE: &str = "graphs";
pub const CRAQLE_LOG_KEYSPACE: &str = "log";
/// The local fence row of each distributed bucket set by set id; publication transactions read it.
pub const DISTRIBUTION_FENCE_KEYSPACE: &str = "distribution_fences";
/// Sets whose record holds open lifecycle work for this node, by set id, until it is settled.
pub const DISTRIBUTION_LIFECYCLE_KEYSPACE: &str = "distribution_lifecycle";
/// Streaming purge cursors of distributed bucket ranges by set id and range id.
pub const DISTRIBUTION_PURGE_KEYSPACE: &str = "distribution_purge";
/// Source roots of a set's external references by set id and root digest, with check markers.
pub const SOURCE_ROOT_KEYSPACE: &str = "distribution_source_roots";
/// IRI index postings by document first, so a materialization replaces exactly its own rows.
pub const DOCUMENT_IRI_KEYSPACE: &str = "metadata_document_iris";
/// Head changes of distributed buckets waiting for the range search indexes, by head position.
pub const SEARCH_FEED_KEYSPACE: &str = "distribution_search_feed";
/// The search index state of each range on this member, by set id and range id.
pub const SEARCH_INDEX_KEYSPACE: &str = "distribution_search_indexes";

/// Every keyspace this crate defines, so tooling reports a missing one without
/// maintaining its own copy of the list.
pub const KEYSPACE_CATALOG: &[&str] = &[
    AUTH_KEYSPACE,
    GROUP_KEYSPACE,
    GROUP_DELETE_KEYSPACE,
    OWNER_INDEX_KEYSPACE,
    REALM_CONFIG_KEYSPACE,
    METADATA_INDEX_KEYSPACE,
    DOCUMENT_INDEX_KEYSPACE,
    IRI_INDEX_KEYSPACE,
    DATA_LINK_KEYSPACE,
    DOCUMENT_LINK_KEYSPACE,
    METADATA_HOLDERS_KEYSPACE,
    UPDATED_INDEX_KEYSPACE,
    VISIBILITY_INDEX_KEYSPACE,
    VISIBILITY_STATE_KEYSPACE,
    METADATA_AUDIT_KEYSPACE,
    EVENT_LOG_KEYSPACE,
    EVENT_SIZE_KEYSPACE,
    METADATA_ACTOR_KEYSPACE,
    METADATA_CHECKPOINT_KEYSPACE,
    COMMIT_MESSAGE_KEYSPACE,
    GIT_RECORD_KEYSPACE,
    GIT_PACK_KEYSPACE,
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
    DATABASE_REQUEST_KEYSPACE,
    DISTRIBUTION_KEYSPACE,
    DISTRIBUTION_ACCEPT_KEYSPACE,
    DISTRIBUTION_PUBLISHER_KEYSPACE,
    DISTRIBUTION_PARKED_KEYSPACE,
    PARKED_INDEX_KEYSPACE,
    INVENTORY_KEYSPACE,
    UPLOAD_INVENTORY_KEYSPACE,
    VERSION_INDEX_KEYSPACE,
    RANGE_INDEX_KEYSPACE,
    RANGE_ACCOUNT_KEYSPACE,
    RANGE_COUNTER_KEYSPACE,
    KEY_SAMPLE_KEYSPACE,
    DELEGATION_NONCE_KEYSPACE,
    CATCH_UP_KEYSPACE,
    BOOTSTRAP_KEYSPACE,
    ATTACHMENT_KEYSPACE,
    CACHE_ENTRY_KEYSPACE,
    CACHE_ORDER_KEYSPACE,
    FILL_CLAIM_KEYSPACE,
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
    VAULT_REVISION_KEYSPACE,
    VAULT_RETIRED_KEYSPACE,
    USER_KEY_KEYSPACE,
    ASSISTANT_PROVIDER_KEYSPACE,
    PROVIDER_OWNER_KEYSPACE,
    CHAT_HEAD_KEYSPACE,
    CHAT_TURN_KEYSPACE,
    BLOB_LOCATIONS_KEYSPACE,
    BLOB_CLEANUP_KEYSPACE,
    BLOB_RECLAIM_KEYSPACE,
    COMPRESSION_MIGRATION_KEYSPACE,
    COMPRESSION_QUEUE_KEYSPACE,
    RENAME_MOVE_KEYSPACE,
    RENAME_TOTAL_KEYSPACE,
    LINKED_JOB_KEYSPACE,
    CLEANUP_CURSOR_KEYSPACE,
    MOVE_HANDOVER_KEYSPACE,
    MIGRATION_AHEAD_KEYSPACE,
    DELETE_CLAIM_KEYSPACE,
    CLAIM_INDEX_KEYSPACE,
    RENAMED_ROW_KEYSPACE,
    STORAGE_CHECK_KEYSPACE,
    CHECK_QUEUE_KEYSPACE,
    BROKEN_VERSION_KEYSPACE,
    ABE_PARAMETERS_KEYSPACE,
    ABE_EPOCH_KEYSPACE,
    ABE_ENVELOPE_KEYSPACE,
    ABE_VERSION_KEYSPACE,
    ABE_ARCHIVE_KEYSPACE,
    ABE_REQUEST_KEYSPACE,
    ABE_GRANT_KEYSPACE,
    ABE_PENDING_KEYSPACE,
    ABE_COPY_KEYSPACE,
    BUCKET_KEY_KEYSPACE,
    BUCKET_HOLDER_KEYSPACE,
    KEY_COPY_KEYSPACE,
    BUCKET_ENCRYPTION_KEYSPACE,
    BUCKET_AUDIT_KEYSPACE,
    PENDING_LOCATION_KEYSPACE,
    COPY_OWNER_KEYSPACE,
    HIDDEN_RESERVATION_KEYSPACE,
    BLOB_QUARANTINE_KEYSPACE,
    MANAGED_COPY_KEYSPACE,
    BLOB_HEAD_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE,
    DELETE_AUDIT_KEYSPACE,
    DELETE_TRAIL_KEYSPACE,
    PATHS_INDEX_KEYSPACE,
    USER_ACCESS_KEYSPACE,
    ACCESS_OWNER_KEYSPACE,
    S3_SESSION_KEYSPACE,
    SESSION_OWNER_KEYSPACE,
    SESSION_EXPIRY_KEYSPACE,
    S3_BUCKET_KEYSPACE,
    BUCKET_CREATION_KEYSPACE,
    RELATIONSHIP_OUT_KEYSPACE,
    RELATIONSHIP_IN_KEYSPACE,
    MIRROR_REPAIR_KEYSPACE,
    SYNC_REFERENCE_KEYSPACE,
    OBJECT_METADATA_KEYSPACE,
    UPLOAD_KEYSPACE,
    UPLOAD_PART_KEYSPACE,
    PURGE_FENCE_KEYSPACE,
    PURGE_CHECKPOINT_KEYSPACE,
    PURGE_FILES_KEYSPACE,
    REPLICATION_JOB_KEYSPACE,
    REPLICATION_OBLIGATION_KEYSPACE,
    REFRESH_JOB_KEYSPACE,
    REFRESH_DUE_KEYSPACE,
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
    BACKEND_IMPORT_KEYSPACE,
    IMPORTED_OBJECT_KEYSPACE,
    BACKEND_VERSION_KEYSPACE,
    IMPORT_INTENT_KEYSPACE,
    IMPORT_PATTERN_KEYSPACE,
    IMPORT_REMOVAL_KEYSPACE,
    IMPORT_DISCOVERY_KEYSPACE,
    OFFERED_DIRECTORY_KEYSPACE,
    DEVICE_INTAKE_KEYSPACE,
    DEVICE_FENCE_KEYSPACE,
    REALM_MARKER_KEYSPACE,
    MANAGEMENT_URL_KEYSPACE,
    REALM_REMOVAL_KEYSPACE,
    SYNC_STATE_KEYSPACE,
    DEVICE_REPLICA_KEYSPACE,
    DEVICE_GRAPH_KEYSPACE,
    SYNCED_FOLDER_KEYSPACE,
    SYNC_BASE_KEYSPACE,
    SYNC_UPLOAD_KEYSPACE,
    SYNC_LOG_KEYSPACE,
    SYNC_RECEIPT_KEYSPACE,
    SYNC_ORIGIN_KEYSPACE,
    SOURCE_INDEX_KEYSPACE,
    SOURCE_SECRET_KEYSPACE,
    CONNECTOR_REFERENCE_KEYSPACE,
    CONNECTOR_PRUNE_KEYSPACE,
    CONNECTOR_INDEX_KEYSPACE,
    CONNECTOR_SECRET_KEYSPACE,
    HARVEST_SOURCE_KEYSPACE,
    HARVEST_PROVENANCE_KEYSPACE,
    ID_MAPPING_KEYSPACE,
    SECONDARY_ID_KEYSPACE,
    REPOSITORY_LINK_KEYSPACE,
    LINK_SECRET_KEYSPACE,
    LINK_CONNECTOR_KEYSPACE,
    LINK_QUEUE_KEYSPACE,
    JOB_KEYSPACE,
    SCHEDULE_INDEX_KEYSPACE,
    JOB_INDEX_KEYSPACE,
    ACTIVE_USER_KEYSPACE,
    DEDUP_INDEX_KEYSPACE,
    RUN_CRATE_KEYSPACE,
    RUN_PUBLISH_KEYSPACE,
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
    STAGING_QUEUE_KEYSPACE,
    STAGING_MANIFEST_KEYSPACE,
    BUCKET_STATS_DB,
    API_STATE_KEYSPACE,
    NODE_STATE_KEYSPACE,
    FEDERATION_KEYSPACE,
    NODE_VAULT_KEYSPACE,
    ONBOARDING_KEYSPACE,
    DHT_KEYSPACE,
    CRAQLE_TERMS_KEYSPACE,
    CRAQLE_QUADS_KEYSPACE,
    CRAQLE_GRAPHS_KEYSPACE,
    CRAQLE_LOG_KEYSPACE,
    TRANSITION_KEYSPACE,
    TRANSITION_QUEUE_KEYSPACE,
    TRANSITION_CLEANUP_KEYSPACE,
    KEY_WAIT_KEYSPACE,
    PENDING_RECLAIM_KEYSPACE,
    BUCKET_RECOVERY_KEYSPACE,
    PENDING_CLAIM_KEYSPACE,
    COPY_WAIT_KEYSPACE,
    PLAINTEXT_COPY_KEYSPACE,
    GROUP_ENCRYPTED_KEYSPACE,
    TOKEN_GRANT_KEYSPACE,
    ABE_DUE_KEYSPACE,
    ABE_REISSUE_KEYSPACE,
    ABE_REKEY_KEYSPACE,
    DISTRIBUTION_FENCE_KEYSPACE,
    DISTRIBUTION_LIFECYCLE_KEYSPACE,
    DISTRIBUTION_PURGE_KEYSPACE,
    SOURCE_ROOT_KEYSPACE,
    BUCKET_FREE_INDEX,
    CONTENT_INDEX_KEYSPACE,
    DOCUMENT_IRI_KEYSPACE,
    SEARCH_FEED_KEYSPACE,
    SEARCH_INDEX_KEYSPACE,
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
