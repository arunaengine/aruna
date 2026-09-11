use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::short_display_id;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
    METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE,
    METADATA_PENDING_PROJECTION_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataCreateEventRecord, MetadataDocumentLifecycleRecord, MetadataError,
    MetadataGraphLifecycleRecord, MetadataQueryResults, MetadataRoCratePage, MetadataSearchHit,
};
use aruna_core::storage_entries::{
    metadata_document_lifecycle_key, metadata_event_log_key, metadata_graph_lifecycle_key,
    metadata_pending_projection_target,
};
use aruna_core::structs::{
    ARUNA_DATA_PREFIX, AuthContext, BlobHeadKey, BlobVersion, BlobVersionState,
    CurrentVersionPointer, MetadataRegistryRecord, PathClaimRecord, Permission, PlacementRef,
    RealmConfigDocument, RealmId, VersionKey, W3idDataIdentifier, blob_bucket_permission_path,
    blob_object_permission_path,
};
use aruna_core::telemetry::record_elapsed_ms;
use aruna_core::types::{GroupId, Key, TxnId, Value};
use aruna_core::{MetaResourceId, NodeId, StructuredId};
use aruna_storage::StorageHandle;
use futures_util::StreamExt;
use futures_util::future::{BoxFuture, FutureExt};
use futures_util::stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{Instrument, Span, debug_span, field, warn};
use ulid::Ulid;

use self::export::export_rocrate_summary_jsonld;
pub use self::export::{export_metadata_rocrate, get_visible_metadata_document};
pub use self::fanout::forwarded_bearer;
pub(crate) use self::fanout::graph_pattern_contains_service;
use self::fanout::{
    MetadataFanoutOperation, MetadataNodeCall, distributed_query_is_union_safe,
    ensure_supported_query_mode, fanout_bearer, metadata_fanout_nodes, metadata_node_call,
    run_metadata_fanout_node,
};
pub(crate) use self::path::local_path_candidates;
pub use self::path::{deduplicate_fanout_nodes, document_replica_query_nodes};
use self::path::{
    forward_path_resolution, load_path_holder, merge_path_views, reduce_path_candidates,
    select_fanout_nodes, select_path_holders, validate_path_candidate,
};
pub(crate) use self::preflight::{discover_realm_nodes, references_preflight_local};
pub use self::preflight::{load_metadata_realm_nodes, load_realm_config};
use self::preflight::{
    preflight_fingerprint, reference_document_title, resolve_graph_reference,
    resolve_preflight_targets,
};
pub(crate) use self::read::{
    can_read_record, ensure_record_readable, filter_live_records, load_record_by_document,
    metadata_read_request,
};
use self::read::{
    check_policy_limit, effective_list_limit, ensure_record_materialized_for_graph_read,
    load_group_records, load_pending_records, merge_pending_metadata_records,
    metadata_record_matches_filters,
};
pub use self::read::{
    query_metadata, query_metadata_document, references_metadata, search_metadata,
};
use super::MetadataAuthToken;
use super::forward::{AuthFailure, ReadDecision, reduce_holder_reads};
use super::handle::{
    METADATA_QUERY_MAX_BYTES, METADATA_QUERY_MAX_RESULT_BYTES, METADATA_QUERY_MAX_ROWS,
    METADATA_REGISTRY_CANDIDATE_LIMIT,
};
use super::protocol::{
    MetadataPathCandidate, MetadataPathResolution, MetadataPathWinner, MetadataReadError,
    MetadataTransportMessage,
};
use super::search_cursor::{
    CursorEnvelopeError, METADATA_SEARCH_MAX_PAGINATION_DEPTH, NodeSearchResult, SearchCursor,
    SearchCursorError, SearchPageCursor, SearchWatermark, SignedCursor, merge_search_hits,
    paginate, query_fingerprint, resume_fetch_limit,
};
use super::summary_cache::summary_cache;
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::permission_rules::GroupPermissionRules;
use crate::blob::resolve_blob_permission_paths::ResolveBlobPermissionPathsOperation;
use crate::driver::{DriverContext, drive};
use crate::groups::list_groups::ListGroupOperation;
use crate::metadata::get_metadata_document::{
    is_metadata_record_materialized_for_graph_read, load_metadata_record_by_document,
};
use crate::metadata::repository::{
    LIST_METADATA_PAGE_SIZE, StorageReadError, iter_registry_effect, parse_registry_iter,
    parse_registry_read, read_registry_by_document_effect,
};
use crate::placement::selector::{
    ROLE_NODE, neg_log2_q48, peer_rank, select_top_peers, selector_hash,
};
use crate::placement::{
    holds_placement, meta_bucket_subject, registry_placement, registry_placement_for,
    registry_strategy, resolve_holders_limit, resolve_shard_holders,
};
use crate::realm::get_realm_config::GetRealmConfigOperation;
use crate::realm::get_realm_nodes::{GetRealmNodesOperation, REALM_DISCOVERY_TIMEOUT};
use crate::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use crate::s3::search_buckets::{BucketSearchHit, SearchBucketsInput, search_local_buckets};
use crate::s3::search_objects::{
    ObjectInventoryHit, ObjectKeyMatch, ObjectSearchNodePage, SearchObjectsInput,
    search_local_objects,
};

mod export;
mod fanout;
mod path;
mod preflight;
mod read;

const DEFAULT_LIST_METADATA_LIMIT: usize = 50;
const MAX_LIST_METADATA_LIMIT: usize = 1_000;
/// Bounds the response payload and the number of RO-Crate summary exports an
/// unauthenticated caller can force per request. The realm-wide registry scan
/// is removed by the cached list path, not by this clamp.
const ANONYMOUS_LIST_METADATA_LIMIT: usize = 100;
/// Splits a targeted lookup from a browse page: the portal pages at 48, a
/// run-crate or preview lookup at 1, and only a browse page pays the estimate.
const METADATA_ESTIMATE_MIN_LIMIT: usize = 24;
// Bounded so a single summary page cannot saturate the craqle read permits.
const METADATA_SUMMARY_FANOUT_LIMIT: usize = 8;
const METADATA_REFERENCES_DEFAULT_LIMIT: usize = 25;
const METADATA_REFERENCES_MAX_LIMIT: usize = 100;
const METADATA_PREFLIGHT_MAX_TARGET_VERSIONS: usize = 128;
const METADATA_PREFLIGHT_SCAN_PAGE_SIZE: usize = 128;
const METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT: usize = 8;
const METADATA_DISTRIBUTED_QUERY_MAX_NODES: usize = 32;
const METADATA_DISTRIBUTED_QUERY_DEADLINE: Duration = Duration::from_secs(12);
const OBJECT_SEARCH_CURSOR_VERSION: u8 = 1;
const OBJECT_SEARCH_CURSOR_SIGNATURE_CONTEXT: &[u8] = b"aruna.object.search.cursor.v1";
const OBJECT_SEARCH_CURSOR_MAX_BYTES: usize = 64 * 1024;
const OBJECT_SEARCH_CURSOR_MAX_KEY_BYTES: usize = 2 * 1024;
#[derive(Debug, Error)]
pub enum MetadataApiError {
    #[error("bad request")]
    BadRequest,
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
    #[error("not found")]
    NotFound,
    #[error("service unavailable")]
    ServiceUnavailable,
    /// The bucket has no usable activation, so the request cannot be routed.
    /// Never absence: no holder was resolved to answer it.
    #[error("placement unavailable: {0}")]
    PlacementUnavailable(crate::placement::PlacementResolveError),
    #[error("{0}")]
    InvalidCursor(String),
    #[error("{0}")]
    Internal(String),
}

/// Order the visible metadata listing is paginated in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataListOrder {
    /// Ascending document id, which is creation order for ULID ids.
    #[default]
    Created,
    /// Descending `updated_at_ms`, tie-broken by descending document id.
    Recent,
}

#[derive(Debug, Clone)]
pub struct ListVisibleMetadataDocumentsRequest {
    pub group_id: Option<GroupId>,
    pub path_prefix: Option<String>,
    pub include_summary: bool,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub order: MetadataListOrder,
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListedMetadataDocument {
    pub record: MetadataRegistryRecord,
    pub rocrate_summary_jsonld: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListVisibleMetadataDocumentsResult {
    pub documents: Vec<ListedMetadataDocument>,
    pub limit: usize,
    pub offset: usize,
    pub total_returned: usize,
    /// Approximate number of matching documents across all pages; group-granular,
    /// so it may over- or under-count glob read rules. `None` when not computed
    /// for a request too small to be a browse page.
    pub total_estimate: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct MetadataPathLookupRequest {
    pub group_id: GroupId,
    pub document_path: String,
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone)]
pub struct MetadataPathLookupResult {
    pub winner: MetadataPathWinner,
    pub conflicts: Vec<Ulid>,
}

#[derive(Debug, Clone)]
pub struct GetVisibleMetadataDocumentRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataRoCrateExportView {
    Full,
    Summary,
    Page,
    Raw,
}

#[derive(Debug, Clone)]
pub struct ExportMetadataRoCrateRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
    pub view: MetadataRoCrateExportView,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExportMetadataRoCrateResult {
    Full {
        record: MetadataRegistryRecord,
        jsonld: String,
    },
    Summary {
        record: MetadataRegistryRecord,
        jsonld: String,
    },
    Page {
        record: MetadataRegistryRecord,
        page: MetadataRoCratePage,
    },
    Raw {
        record: MetadataRegistryRecord,
        raw: crate::metadata::raw_revision::MetadataRawView,
        dataset_digest: Option<[u8; 32]>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataApiQueryMode {
    Local,
    Distributed,
}

#[derive(Debug, Clone)]
pub struct MetadataDocumentQueryRequest {
    pub document_id: Ulid,
    pub auth: Option<AuthContext>,
    pub bearer_token: Option<String>,
    pub query: String,
    pub mode: Option<MetadataApiQueryMode>,
    pub allow_partial: bool,
}

#[derive(Debug, Clone)]
pub struct MetadataQueryRequest {
    pub auth: Option<AuthContext>,
    pub bearer_token: Option<String>,
    pub graph_iris: Option<Vec<String>>,
    pub query: String,
    pub mode: Option<MetadataApiQueryMode>,
    pub target_nodes: Option<Vec<NodeId>>,
    pub allow_partial: bool,
}

#[derive(Debug, Clone)]
pub struct MetadataSearchRequest {
    pub auth: Option<AuthContext>,
    pub bearer_token: Option<String>,
    pub graph_iris: Option<Vec<String>>,
    pub query: String,
    pub conforms_to: Option<String>,
    pub group_id: Option<GroupId>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub mode: Option<MetadataApiQueryMode>,
    pub target_nodes: Option<Vec<NodeId>>,
}

#[derive(Debug, Clone)]
pub struct MetadataQueryExecution {
    pub results: MetadataQueryResults,
    pub fanout_stats: MetadataFanoutStats,
}

#[derive(Debug, Clone)]
pub struct MetadataSearchExecution {
    pub hits: Vec<MetadataSearchHit>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
    pub fanout_stats: MetadataFanoutStats,
}

#[derive(Debug, Clone)]
pub struct BucketSearchRequest {
    pub auth: AuthContext,
    pub bearer_token: Option<String>,
    pub query: String,
    pub limit: usize,
    pub target_nodes: Option<Vec<NodeId>>,
}

#[derive(Debug, Clone)]
pub struct BucketSearchExecution {
    pub hits: Vec<BucketSearchHit>,
    pub fanout_stats: MetadataFanoutStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectSearchQueryMode {
    Local,
    DistributedBestEffort,
    DistributedStrict,
}

impl ObjectSearchQueryMode {
    fn fanout_mode(self) -> MetadataApiQueryMode {
        match self {
            Self::Local => MetadataApiQueryMode::Local,
            Self::DistributedBestEffort | Self::DistributedStrict => {
                MetadataApiQueryMode::Distributed
            }
        }
    }

    fn allow_partial(self) -> bool {
        !matches!(self, Self::DistributedStrict)
    }
}

#[derive(Debug, Clone)]
pub struct ObjectSearchRequest {
    pub auth: AuthContext,
    pub bearer_token: Option<String>,
    pub query: String,
    pub key_match: ObjectKeyMatch,
    pub bucket: Option<String>,
    pub limit: usize,
    pub cursor: Option<String>,
    pub mode: ObjectSearchQueryMode,
    pub target_nodes: Option<Vec<NodeId>>,
}

#[derive(Debug, Clone)]
pub struct ObjectSearchPartitionCoverage {
    pub node_id: NodeId,
    pub observed_at: SystemTime,
    pub truncated: bool,
}

#[derive(Debug, Clone)]
pub struct ObjectSearchExecution {
    pub hits: Vec<ObjectInventoryHit>,
    pub next_cursor: Option<String>,
    pub as_of: SystemTime,
    pub partitions: Vec<ObjectSearchPartitionCoverage>,
    pub fanout_stats: MetadataFanoutStats,
    pub omitted_partitions: usize,
    pub complete: bool,
}

#[derive(Debug, Clone)]
struct ObjectSearchPartitionState {
    node_id: NodeId,
    start_after: Option<Vec<u8>>,
    exhausted: bool,
    observed_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectSearchCursorPartition {
    node_id: [u8; 32],
    start_after: Option<Vec<u8>>,
    exhausted: bool,
    observed_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectSearchCursorPayload {
    as_of: SystemTime,
    partitions: Vec<ObjectSearchCursorPartition>,
    failed_partitions: Vec<[u8; 32]>,
    discovery_failed: bool,
    omitted_partitions: usize,
}

type ObjectSearchCursor = SignedCursor<ObjectSearchCursorPayload>;

impl SignedCursor<ObjectSearchCursorPayload> {
    fn decode(
        raw: &str,
        fingerprint: [u8; 32],
        authorized_signers: &[NodeId],
    ) -> Result<Self, MetadataApiError> {
        if raw.len() > OBJECT_SEARCH_CURSOR_MAX_BYTES {
            return Err(MetadataApiError::InvalidCursor(
                "invalid object search cursor".to_string(),
            ));
        }
        let cursor = Self::decode_envelope(
            raw,
            OBJECT_SEARCH_CURSOR_SIGNATURE_CONTEXT,
            authorized_signers,
            |cursor| {
                if cursor.version != OBJECT_SEARCH_CURSOR_VERSION
                    || cursor.fingerprint != fingerprint
                {
                    Err(CursorEnvelopeError::QueryMismatch)
                } else {
                    Ok(())
                }
            },
        )
        .map_err(|error| match error {
            CursorEnvelopeError::Invalid => {
                MetadataApiError::InvalidCursor("invalid object search cursor".to_string())
            }
            CursorEnvelopeError::QueryMismatch => MetadataApiError::InvalidCursor(
                "object search cursor does not match query".to_string(),
            ),
        })?;
        if cursor.payload.partitions.len() > METADATA_DISTRIBUTED_QUERY_MAX_NODES
            || cursor.payload.failed_partitions.len() > METADATA_DISTRIBUTED_QUERY_MAX_NODES
        {
            return Err(MetadataApiError::InvalidCursor(
                "invalid object search cursor".to_string(),
            ));
        }
        let mut nodes = HashSet::new();
        for partition in &cursor.payload.partitions {
            NodeId::from_bytes(&partition.node_id).map_err(|_| {
                MetadataApiError::InvalidCursor("invalid object search cursor".to_string())
            })?;
            if !nodes.insert(partition.node_id)
                || partition.start_after.as_ref().is_some_and(|key| {
                    key.len() > OBJECT_SEARCH_CURSOR_MAX_KEY_BYTES
                        || BlobHeadKey::from_bytes(key).is_err()
                })
            {
                return Err(MetadataApiError::InvalidCursor(
                    "invalid object search cursor".to_string(),
                ));
            }
        }
        for node_id in &cursor.payload.failed_partitions {
            NodeId::from_bytes(node_id).map_err(|_| {
                MetadataApiError::InvalidCursor("invalid object search cursor".to_string())
            })?;
            if !nodes.insert(*node_id) {
                return Err(MetadataApiError::InvalidCursor(
                    "invalid object search cursor".to_string(),
                ));
            }
        }
        if !cursor
            .payload
            .partitions
            .iter()
            .any(|partition| !partition.exhausted)
        {
            return Err(MetadataApiError::InvalidCursor(
                "exhausted object search cursor".to_string(),
            ));
        }
        Ok(cursor)
    }

    fn partition_states(&self) -> Result<Vec<ObjectSearchPartitionState>, MetadataApiError> {
        self.payload
            .partitions
            .iter()
            .map(|partition| {
                Ok(ObjectSearchPartitionState {
                    node_id: NodeId::from_bytes(&partition.node_id).map_err(|_| {
                        MetadataApiError::InvalidCursor("invalid object search cursor".to_string())
                    })?,
                    start_after: partition.start_after.clone(),
                    exhausted: partition.exhausted,
                    observed_at: partition.observed_at,
                })
            })
            .collect()
    }

    fn failed_nodes(&self) -> Result<Vec<NodeId>, MetadataApiError> {
        self.payload
            .failed_partitions
            .iter()
            .map(|node_id| {
                NodeId::from_bytes(node_id).map_err(|_| {
                    MetadataApiError::InvalidCursor("invalid object search cursor".to_string())
                })
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn new_signed(
        fingerprint: [u8; 32],
        as_of: SystemTime,
        partitions: &[ObjectSearchPartitionState],
        failed_partitions: &[NodeId],
        discovery_failed: bool,
        omitted_partitions: usize,
        signer: NodeId,
        sign: impl FnOnce(&[u8]) -> iroh::Signature,
    ) -> Result<Self, postcard::Error> {
        let partitions: Vec<ObjectSearchCursorPartition> = partitions
            .iter()
            .map(|partition| ObjectSearchCursorPartition {
                node_id: *partition.node_id.as_bytes(),
                start_after: partition.start_after.clone(),
                exhausted: partition.exhausted,
                observed_at: partition.observed_at,
            })
            .collect();
        let failed_partitions: Vec<[u8; 32]> = failed_partitions
            .iter()
            .map(|node_id| *node_id.as_bytes())
            .collect();
        Self::build_signed(
            OBJECT_SEARCH_CURSOR_VERSION,
            OBJECT_SEARCH_CURSOR_SIGNATURE_CONTEXT,
            fingerprint,
            ObjectSearchCursorPayload {
                as_of,
                partitions,
                failed_partitions,
                discovery_failed,
                omitted_partitions,
            },
            signer,
            sign,
        )
    }
}

#[derive(Debug, Clone)]
pub struct MetadataReferencesRequest {
    pub auth: Option<AuthContext>,
    pub iri: String,
    pub predicate: Option<String>,
    pub limit: Option<usize>,
    pub resolve: bool,
}

#[derive(Debug, Clone)]
pub struct MetadataReferenceEntry {
    pub document_id: String,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub predicate: Option<String>,
    pub subject_iris: Vec<String>,
    pub title: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MetadataReferencesExecution {
    pub references: Vec<MetadataReferenceEntry>,
}

#[derive(Debug, Clone)]
pub enum MetadataReferencePreflightTarget {
    ContentW3ids {
        content_w3ids: Vec<String>,
        remove_all_resolvable_locations: bool,
    },
    BucketPrefix {
        bucket: String,
        prefix: Option<String>,
        operation: MetadataPreflightStorageOperation,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataPreflightStorageOperation {
    #[default]
    LatestVersionTombstone,
    AllVersionsPurge,
}

#[derive(Debug, Clone)]
pub struct MetadataReferencePreflightRequest {
    pub auth: AuthContext,
    pub bearer_token: Option<String>,
    pub target: MetadataReferencePreflightTarget,
    pub s3_endpoint: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub mode: Option<MetadataApiQueryMode>,
    pub target_nodes: Option<Vec<NodeId>>,
    pub allow_partial: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPreflightLocation {
    pub node_id: NodeId,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPreflightResolvedTarget {
    pub content_w3id: String,
    pub content_hash: [u8; 32],
    pub queried_iris: Vec<String>,
    pub targeted_versions: Vec<MetadataPreflightLocation>,
    pub removed_locations: Vec<MetadataPreflightLocation>,
    pub remove_all_resolvable_locations: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataReferencePreflightNodeRequest {
    pub targets: Vec<MetadataPreflightResolvedTarget>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPreflightVisibleReference {
    pub content_w3id: String,
    pub document_id: String,
    pub title: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataPreflightIndexState {
    Current,
    Pending,
    Failed,
    Mixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPreflightNodeFreshness {
    pub node_id: NodeId,
    pub index_state: MetadataPreflightIndexState,
    pub oldest_status_updated_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataReferencePreflightNodeTarget {
    pub content_w3id: String,
    pub hidden_references_exist: bool,
    pub resolvable_location_found: bool,
    pub resolvable_location_after_operation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataReferencePreflightNodeExecution {
    pub visible_references: Vec<MetadataPreflightVisibleReference>,
    pub targets: Vec<MetadataReferencePreflightNodeTarget>,
    pub freshness: MetadataPreflightNodeFreshness,
    pub path_style_endpoint_available: bool,
    pub saturated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataReferencePreflightTargetExecution {
    pub content_w3id: String,
    pub targeted_versions: Vec<MetadataPreflightLocation>,
    pub visible_references: Vec<MetadataPreflightVisibleReference>,
    pub hidden_references_exist: bool,
    pub would_remove_last_resolvable_aruna_location: bool,
    pub location_impact_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataPreflightExcludedForm {
    pub form: &'static str,
    pub reason: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataReferencePreflightCoverage {
    pub queried_scope: &'static str,
    pub queried_forms: Vec<&'static str>,
    pub excluded_forms: Vec<MetadataPreflightExcludedForm>,
    pub node_freshness: Vec<MetadataPreflightNodeFreshness>,
    pub target_resolution_complete: bool,
    pub path_style_endpoint_coverage_complete: bool,
    pub realm_coverage_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataReferencePreflightExecution {
    pub targets: Vec<MetadataReferencePreflightTargetExecution>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
    pub nodes_queried: usize,
    pub nodes_failed: usize,
    pub complete: bool,
    pub failed_partitions: Vec<NodeId>,
    pub coverage: MetadataReferencePreflightCoverage,
}

#[derive(Debug, Clone, Default)]
pub struct MetadataFanoutStats {
    pub nodes_queried: usize,
    pub nodes_failed: usize,
    pub failed_partitions: Vec<NodeId>,
    pub discovery_failed: bool,
}

#[derive(Debug)]
struct PathHolderSelection {
    node_id: NodeId,
    shards: Vec<u32>,
}

#[derive(Debug, Clone)]
pub(crate) struct MetadataRealmNodeDiscovery {
    pub(crate) nodes: Vec<NodeId>,
    pub(crate) failed: bool,
}

#[derive(Debug)]
struct MetadataFanoutScope {
    mode: Option<MetadataApiQueryMode>,
    target_nodes: Option<Vec<NodeId>>,
    allow_partial: bool,
    discovery_failed: bool,
    subject: Option<[u8; 32]>,
    deadline: Option<tokio::time::Instant>,
}

impl MetadataFanoutScope {
    fn new(
        mode: Option<MetadataApiQueryMode>,
        target_nodes: Option<Vec<NodeId>>,
        allow_partial: bool,
    ) -> Self {
        Self {
            mode,
            target_nodes,
            allow_partial,
            discovery_failed: false,
            subject: None,
            deadline: None,
        }
    }

    fn with_discovery_failed(mut self, discovery_failed: bool) -> Self {
        self.discovery_failed = discovery_failed;
        self
    }

    fn with_subject(mut self, subject: [u8; 32]) -> Self {
        self.subject = Some(subject);
        self
    }

    fn with_deadline(mut self, deadline: tokio::time::Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataQueryForm {
    Select,
    Ask,
}

pub async fn list_visible_metadata_documents(
    context: &DriverContext,
    realm_id: RealmId,
    request: ListVisibleMetadataDocumentsRequest,
) -> Result<ListVisibleMetadataDocumentsResult, MetadataApiError> {
    let limit = effective_list_limit(request.limit, request.auth.is_none());
    let offset = request.offset.unwrap_or(0);

    let group_ids = check_policy_limit(match request.group_id {
        Some(group_id) => vec![group_id],
        None => drive(
            ListGroupOperation::with_pagination(METADATA_REGISTRY_CANDIDATE_LIMIT + 1, 0),
            context,
        )
        .await
        .map_err(|error| MetadataApiError::Internal(error.to_string()))?
        .into_iter()
        .map(|group| group.group_id)
        .collect(),
    })?;
    // Summary listings and recency listings must show documents whose projection
    // has not landed yet; the pending keyspace is scanned once per request,
    // never once per group.
    let recent = request.order == MetadataListOrder::Recent;
    let mut pending = if request.include_summary || recent {
        load_pending_records(context, request.group_id, METADATA_REGISTRY_CANDIDATE_LIMIT).await?
    } else {
        HashMap::new()
    };

    let mut records = Vec::new();
    for group_id in group_ids {
        let remaining = METADATA_REGISTRY_CANDIDATE_LIMIT.saturating_sub(records.len());
        let mut group_records = load_group_records(context, group_id, remaining).await?;
        if let Some(pending_records) = pending.remove(&group_id) {
            merge_pending_metadata_records(&mut group_records, pending_records);
            if group_records.len() > remaining {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            group_records.sort_by_key(|record| record.document_id);
        }
        records.extend(group_records);
    }
    // Ordering precedes both the estimate scan and the offset window so that
    // pagination and the early exit page the same sequence.
    if recent {
        records.sort_by(|left, right| {
            right
                .updated_at_ms
                .cmp(&left.updated_at_ms)
                .then_with(|| right.document_id.cmp(&left.document_id))
        });
    }

    // One rule collection (a read per distinct group) replaces the per-record
    // permission drives; `record_visible` mirrors `can_read_record` for the
    // same caller and record, so every later check is pure memory.
    let auth = request
        .auth
        .as_ref()
        .filter(|auth| auth.realm_id == realm_id);
    let permissions = GroupPermissionRules::collect(
        context,
        auth,
        records
            .iter()
            .filter(|record| record.realm_id == realm_id)
            .map(|record| record.group_id),
    )
    .await;
    // RBAC/public visibility is additionally constrained by the metadata.read
    // request policies, loaded once per distinct group (fail-closed on error).
    let evaluators = crate::auth::request_policy::PolicyEvaluator::load_bulk(
        context,
        records
            .iter()
            .filter(|record| record.realm_id == realm_id)
            .map(|record| (record.realm_id, record.group_id)),
    )
    .await
    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let policy_auth = request.auth.as_ref();
    let record_visible = |record: &MetadataRegistryRecord| {
        permissions.record_visible(record)
            && evaluators
                .get(&(record.realm_id, record.group_id))
                .is_some_and(|evaluator| {
                    evaluator
                        .evaluate(&metadata_read_request(&record.permission_path, policy_auth))
                        .is_ok()
                })
    };

    let mut total_estimate = None;
    if limit >= METADATA_ESTIMATE_MIN_LIMIT {
        let matching = records
            .iter()
            .filter(|record| {
                metadata_record_matches_filters(record, request.path_prefix.as_deref())
            })
            .filter(|record| record_visible(record))
            .count();
        total_estimate = Some(matching);
    }

    let needed = offset.saturating_add(limit);
    let mut selected = Vec::with_capacity(limit.min(records.len()));
    let mut visible_count = 0usize;
    for record in records {
        if !metadata_record_matches_filters(&record, request.path_prefix.as_deref()) {
            continue;
        }
        if !record_visible(&record) {
            continue;
        }
        visible_count += 1;
        if visible_count > offset {
            selected.push(record);
            if visible_count >= needed {
                break;
            }
        }
    }

    let mut documents = Vec::with_capacity(selected.len());
    if request.include_summary {
        let exports = selected
            .iter()
            .map(|record| async move {
                // Registry cursor advances at event acceptance but graph only at
                // materialization; exporting in between would hand out (and cache)
                // superseded content, so pending documents list without a summary.
                ensure_record_materialized_for_graph_read(context, record).await?;
                export_rocrate_summary_jsonld(context, &record.graph_iri, record.last_event_id)
                    .await
            })
            .collect::<Vec<_>>();
        let summaries = stream::iter(exports)
            .buffered(METADATA_SUMMARY_FANOUT_LIMIT)
            .collect::<Vec<_>>()
            .await;
        for (record, summary) in selected.into_iter().zip(summaries) {
            let rocrate_summary_jsonld = match summary {
                Ok(summary) => Some(summary),
                Err(MetadataApiError::ServiceUnavailable) => None,
                Err(error) => return Err(error),
            };
            documents.push(ListedMetadataDocument {
                record,
                rocrate_summary_jsonld,
            });
        }
    } else {
        documents.extend(selected.into_iter().map(|record| ListedMetadataDocument {
            record,
            rocrate_summary_jsonld: None,
        }));
    }

    let total_returned = documents.len();
    Ok(ListVisibleMetadataDocumentsResult {
        documents,
        limit,
        offset,
        total_returned,
        // Never report fewer than the page already discloses.
        total_estimate: total_estimate.map(|estimate| estimate.max(total_returned)),
    })
}

pub async fn lookup_metadata_path(
    context: &DriverContext,
    realm_id: RealmId,
    request: MetadataPathLookupRequest,
    auth_token: Option<MetadataAuthToken>,
) -> Result<MetadataPathLookupResult, MetadataApiError> {
    let deadline = tokio::time::Instant::now() + METADATA_DISTRIBUTED_QUERY_DEADLINE;
    let normalized = MetadataRegistryRecord::normalize_document_path(&request.document_path);
    if normalized.is_empty() {
        return Err(MetadataApiError::BadRequest);
    }
    if context.net_handle.is_none() {
        return resolve_local_path(context, realm_id, request).await;
    }
    let config = tokio::time::timeout_at(deadline, load_realm_config(context, realm_id))
        .await
        .ok()
        .flatten()
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let local_node = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let trusted_origin = config
        .nodes
        .iter()
        .any(|node| node.node_id == local_node.to_string() && node.kind.is_sync_eligible());
    if !trusted_origin {
        let config_digest = config
            .digest()
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        return forward_path_resolution(
            context,
            realm_id,
            &config,
            request,
            auth_token,
            config_digest,
            deadline,
        )
        .await;
    }
    let strategy = registry_strategy(&config).ok_or(MetadataApiError::ServiceUnavailable)?;
    if strategy.shard_count == 0 {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let config_digest = config
        .digest()
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let shard_count = strategy.shard_count;
    let auth_token = auth_token.or_else(|| request.auth.clone().map(MetadataAuthToken::internal));
    let group_id = request.group_id;
    let auth = request.auth.as_ref();
    let (holders, replica_counts) = select_path_holders(
        &config,
        realm_id,
        group_id,
        &normalized,
        strategy.strategy_id,
        shard_count,
        strategy.replica_count,
        local_node,
        deadline,
    )?;
    let requests = stream::iter(holders.into_iter().map(|selection| {
        let holder = selection.node_id;
        let shards = selection.shards;
        let auth_token = auth_token.clone();
        let normalized = normalized.clone();
        async move {
            let result = if holder == local_node {
                match tokio::time::timeout_at(
                    deadline,
                    local_path_candidates(context, realm_id, group_id, &normalized, auth),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(MetadataApiError::ServiceUnavailable),
                }
            } else {
                load_path_holder(
                    context,
                    group_id,
                    &normalized,
                    holder,
                    auth_token,
                    config_digest,
                    deadline,
                )
                .await
            };
            (holder, shards, result)
        }
    }))
    .buffer_unordered(METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT)
    .collect::<Vec<_>>();
    let responses = tokio::time::timeout_at(deadline, requests)
        .await
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let mut views = Vec::new();
    let mut auth_error = None;
    let mut failed = false;
    let mut only_auth = true;
    let mut candidate_count = 0usize;
    for (_holder, shards, response) in responses {
        match response {
            Ok(returned) => {
                candidate_count = candidate_count.saturating_add(returned.len());
                if candidate_count > METADATA_REGISTRY_CANDIDATE_LIMIT {
                    return Err(MetadataApiError::ServiceUnavailable);
                }
                only_auth = false;
                let mut partitions = shards.iter().map(|_| Vec::new()).collect::<Vec<_>>();
                for candidate in returned {
                    let placement = validate_path_candidate(
                        &config,
                        realm_id,
                        group_id,
                        &normalized,
                        &candidate,
                    )?;
                    let index = shards
                        .binary_search(&placement.shard)
                        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
                    partitions[index].push(candidate);
                }
                views.extend(
                    shards
                        .into_iter()
                        .zip(partitions)
                        .map(|(shard, candidates)| PathShardView { shard, candidates }),
                );
            }
            Err(error @ (MetadataApiError::Unauthorized | MetadataApiError::Forbidden)) => {
                failed = true;
                auth_error.get_or_insert(error);
            }
            Err(_) => {
                failed = true;
                only_auth = false;
            }
        }
    }
    if failed {
        if only_auth && let Some(error) = auth_error {
            return Err(error);
        }
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let candidates = merge_path_views(&replica_counts, views)?;
    reduce_path_candidates(candidates)
}

pub(crate) async fn resolve_local_path(
    context: &DriverContext,
    realm_id: RealmId,
    request: MetadataPathLookupRequest,
) -> Result<MetadataPathLookupResult, MetadataApiError> {
    let normalized = MetadataRegistryRecord::normalize_document_path(&request.document_path);
    if normalized.is_empty() {
        return Err(MetadataApiError::BadRequest);
    }
    let candidates = local_path_candidates(
        context,
        realm_id,
        request.group_id,
        &normalized,
        request.auth.as_ref(),
    )
    .await?;
    reduce_path_candidates(candidates)
}

struct PathShardView {
    shard: u32,
    candidates: Vec<MetadataPathCandidate>,
}

struct ResolvedPreflightTargets {
    targets: Vec<MetadataPreflightResolvedTarget>,
    complete: bool,
}

pub async fn references_preflight(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: MetadataReferencePreflightRequest,
) -> Result<MetadataReferencePreflightExecution, MetadataApiError> {
    let deadline = tokio::time::Instant::now() + METADATA_DISTRIBUTED_QUERY_DEADLINE;
    let MetadataReferencePreflightRequest {
        auth,
        bearer_token,
        target,
        s3_endpoint,
        limit,
        cursor,
        mode,
        mut target_nodes,
        allow_partial,
    } = request;
    if auth.realm_id != realm_id {
        return Err(MetadataApiError::Forbidden);
    }
    let page_size = limit
        .unwrap_or(METADATA_REFERENCES_DEFAULT_LIMIT)
        .clamp(1, METADATA_REFERENCES_MAX_LIMIT);
    let resolved = resolve_preflight_targets(
        context,
        realm_id,
        local_node_id,
        &auth,
        target,
        s3_endpoint.as_deref(),
    )
    .await?;
    let fingerprint = preflight_fingerprint(&resolved.targets, mode);
    let mut cursor_discovery = None;
    let (watermark, resume) = match cursor.as_deref() {
        Some(raw) => {
            let signer_nodes = match mode.unwrap_or(MetadataApiQueryMode::Distributed) {
                MetadataApiQueryMode::Local => vec![local_node_id],
                MetadataApiQueryMode::Distributed => match target_nodes.as_ref() {
                    Some(nodes) => {
                        let mut signers = nodes.clone();
                        signers.push(local_node_id);
                        signers
                    }
                    None => {
                        let discovery = tokio::time::timeout_at(
                            deadline,
                            discover_realm_nodes(context, realm_id, local_node_id),
                        )
                        .await
                        .unwrap_or(MetadataRealmNodeDiscovery {
                            nodes: vec![local_node_id],
                            failed: true,
                        });
                        let mut signers = discovery.nodes.clone();
                        signers.push(local_node_id);
                        let nodes =
                            select_fanout_nodes(&discovery.nodes, local_node_id, &fingerprint);
                        let mut discovery = discovery;
                        discovery.nodes = nodes;
                        cursor_discovery = Some(discovery);
                        signers
                    }
                },
            };
            let cursor = SearchCursor::decode(raw, &signer_nodes)
                .map_err(|error| MetadataApiError::InvalidCursor(error.to_string()))?;
            if cursor.fingerprint != fingerprint {
                return Err(MetadataApiError::InvalidCursor(
                    SearchCursorError::QueryMismatch.to_string(),
                ));
            }
            (
                Some(cursor.payload.watermark.clone()),
                cursor.resume_positions(),
            )
        }
        None => (None, HashMap::new()),
    };
    let discovery_failed = if cursor.is_some() {
        let mut nodes = match target_nodes.as_ref() {
            Some(nodes) => select_fanout_nodes(nodes, local_node_id, &fingerprint),
            None => match mode.unwrap_or(MetadataApiQueryMode::Distributed) {
                MetadataApiQueryMode::Local => vec![local_node_id],
                MetadataApiQueryMode::Distributed => cursor_discovery
                    .as_ref()
                    .map(|discovery| discovery.nodes.clone())
                    .unwrap_or_else(|| vec![local_node_id]),
            },
        };
        for node_id in resume.keys() {
            if !nodes.contains(node_id) {
                nodes.push(*node_id);
            }
        }
        target_nodes = Some(deduplicate_fanout_nodes(nodes));
        cursor_discovery
            .as_ref()
            .is_some_and(|discovery| discovery.failed)
    } else {
        false
    };

    let resume = Arc::new(resume);
    let remote_auth = forwarded_bearer(bearer_token.as_deref())?
        .or_else(|| Some(MetadataAuthToken::internal(auth.clone())));
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let local_call: MetadataNodeCall<MetadataReferencePreflightNodeExecution> = metadata_node_call(
        (
            context.clone(),
            realm_id,
            auth.clone(),
            resolved.targets.clone(),
            s3_endpoint.clone(),
            resume.clone(),
            page_size,
        ),
        |(context, realm_id, auth, targets, endpoint, resume, page_size), node_id| async move {
            let limit = resume_fetch_limit(
                &resume,
                node_id,
                page_size,
                METADATA_SEARCH_MAX_PAGINATION_DEPTH,
            );
            references_preflight_local(
                &context,
                realm_id,
                node_id,
                Some(auth),
                MetadataReferencePreflightNodeRequest { targets, limit },
                endpoint,
            )
            .await
            .map_err(super::forward::read_error)
        },
    );
    let remote_call: MetadataNodeCall<MetadataReferencePreflightNodeExecution> = metadata_node_call(
        (
            handle,
            remote_auth,
            resolved.targets.clone(),
            resume.clone(),
            page_size,
        ),
        |(handle, auth_token, targets, resume, page_size), node_id| async move {
            let limit = resume_fetch_limit(
                &resume,
                node_id,
                page_size,
                METADATA_SEARCH_MAX_PAGINATION_DEPTH,
            );
            handle
                .request_remote_reference_preflight(
                    node_id,
                    auth_token,
                    MetadataReferencePreflightNodeRequest { targets, limit },
                )
                .await
        },
    );
    let (node_parts, fanout_stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        MetadataFanoutScope::new(mode, target_nodes, allow_partial)
            .with_subject(fingerprint)
            .with_discovery_failed(discovery_failed)
            .with_deadline(deadline),
        MetadataFanoutOperation::ReferencePreflight,
        local_call,
        remote_call,
        record_preflight_node_result,
        map_read_error,
    )
    .await?;

    let mut node_results = Vec::new();
    let mut node_freshness = Vec::new();
    let mut hidden = BTreeSet::new();
    let mut locations = BTreeMap::<String, (bool, bool)>::new();
    let mut path_style_endpoint_coverage_complete = true;
    for (node_id, part) in node_parts {
        node_freshness.push(part.freshness.clone());
        path_style_endpoint_coverage_complete &= part.path_style_endpoint_available;
        for target in part.targets {
            if target.hidden_references_exist {
                hidden.insert(target.content_w3id.clone());
            }
            let entry = locations.entry(target.content_w3id).or_default();
            entry.0 |= target.resolvable_location_found;
            entry.1 |= target.resolvable_location_after_operation;
        }
        let hits = part
            .visible_references
            .into_iter()
            .map(|reference| MetadataSearchHit {
                document_id: reference.document_id.clone(),
                group_id: String::new(),
                document_path: String::new(),
                graph_iri: reference.content_w3id,
                subject_iri: reference.document_id,
                score: 0.0,
                title: reference.title,
                snippet: None,
                subject_types: Vec::new(),
            })
            .collect();
        node_results.push(NodeSearchResult {
            node_id,
            hits,
            saturated: part.saturated,
        });
    }
    node_freshness.sort_by_key(|freshness| freshness.node_id.to_string());
    let page = paginate(
        node_results,
        watermark,
        page_size,
        METADATA_SEARCH_MAX_PAGINATION_DEPTH,
    );
    let mut visible_by_target = BTreeMap::<String, Vec<MetadataPreflightVisibleReference>>::new();
    for hit in page.hits {
        visible_by_target
            .entry(hit.graph_iri.clone())
            .or_default()
            .push(MetadataPreflightVisibleReference {
                content_w3id: hit.graph_iri,
                document_id: hit.document_id,
                title: hit.title,
            });
    }
    let index_current = node_freshness
        .iter()
        .all(|freshness| freshness.index_state == MetadataPreflightIndexState::Current);
    let complete = fanout_stats.nodes_failed == 0
        && resolved.complete
        && index_current
        && path_style_endpoint_coverage_complete
        && !page.truncated;
    if !allow_partial && !complete {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let targets = resolved
        .targets
        .into_iter()
        .map(|target| {
            let (found, remaining) = locations
                .remove(&target.content_w3id)
                .unwrap_or((false, false));
            let removes_location =
                target.remove_all_resolvable_locations || !target.removed_locations.is_empty();
            MetadataReferencePreflightTargetExecution {
                visible_references: visible_by_target
                    .remove(&target.content_w3id)
                    .unwrap_or_default(),
                hidden_references_exist: hidden.contains(&target.content_w3id),
                would_remove_last_resolvable_aruna_location: complete
                    && removes_location
                    && found
                    && !remaining,
                location_impact_complete: complete,
                content_w3id: target.content_w3id,
                targeted_versions: target.targeted_versions,
            }
        })
        .collect();
    let next_cursor = match page.next {
        Some(next) => {
            let net = context.net_handle.as_ref().ok_or_else(|| {
                MetadataApiError::Internal(
                    "net handle unavailable for preflight cursor signing".to_string(),
                )
            })?;
            Some(
                SearchCursor::new_signed(
                    fingerprint,
                    next.watermark,
                    next.resume,
                    net.node_id(),
                    |bytes| net.sign(bytes),
                )
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?
                .encode()
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?,
            )
        }
        None => None,
    };
    let distributed =
        mode.unwrap_or(MetadataApiQueryMode::Distributed) == MetadataApiQueryMode::Distributed;
    Ok(MetadataReferencePreflightExecution {
        targets,
        next_cursor,
        truncated: page.truncated,
        nodes_queried: fanout_stats.nodes_queried,
        nodes_failed: fanout_stats.nodes_failed,
        complete,
        failed_partitions: fanout_stats.failed_partitions,
        coverage: MetadataReferencePreflightCoverage {
            queried_scope: if distributed { "realm" } else { "local_node" },
            queried_forms: vec![
                "canonical_content_w3id",
                "legacy_s3_iri",
                "legacy_path_style_http_iri",
            ],
            excluded_forms: vec![
                MetadataPreflightExcludedForm {
                    form: "literal_content_url",
                    reason: "literal objects are not materialized in the NamedNode IRI index",
                },
                MetadataPreflightExcludedForm {
                    form: "imported_relative_identity",
                    reason: "relative imported identities are outside exact absolute-IRI matching",
                },
                MetadataPreflightExcludedForm {
                    form: "imported_external_identity",
                    reason: "external identities without an Aruna content mapping are outside coverage",
                },
            ],
            node_freshness,
            target_resolution_complete: resolved.complete,
            path_style_endpoint_coverage_complete,
            realm_coverage_complete: distributed && complete,
        },
    })
}

fn authorized_realm_nodes(
    config: &RealmConfigDocument,
    nodes: HashSet<NodeId>,
) -> Result<HashSet<NodeId>, ConversionError> {
    let authorized = config
        .sync_eligible_node_ids()?
        .into_iter()
        .collect::<HashSet<_>>();
    Ok(nodes
        .into_iter()
        .filter(|node_id| authorized.contains(node_id))
        .collect())
}
fn map_metadata_event_error(error: MetadataError) -> MetadataApiError {
    match error {
        MetadataError::GraphNotFound => MetadataApiError::ServiceUnavailable,
        other => MetadataApiError::Internal(other.to_string()),
    }
}

fn map_metadata_query_error(error: MetadataError) -> MetadataApiError {
    match error {
        MetadataError::InvalidInput(_) => MetadataApiError::BadRequest,
        other => map_metadata_event_error(other),
    }
}

fn map_read_error(error: MetadataReadError) -> MetadataApiError {
    match error {
        MetadataReadError::Unauthorized => MetadataApiError::Unauthorized,
        MetadataReadError::Forbidden => MetadataApiError::Forbidden,
        MetadataReadError::NotFound | MetadataReadError::Unavailable => {
            MetadataApiError::ServiceUnavailable
        }
    }
}

fn map_metadata_internal_error(error: MetadataError) -> MetadataApiError {
    MetadataApiError::Internal(error.to_string())
}

#[allow(clippy::too_many_arguments)]
async fn run_metadata_fanout<T>(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    scope: MetadataFanoutScope,
    operation: MetadataFanoutOperation,
    local_call: MetadataNodeCall<T>,
    remote_call: MetadataNodeCall<T>,
    record_result: fn(&Span, &Result<T, MetadataReadError>),
    map_local_error: fn(MetadataReadError) -> MetadataApiError,
) -> Result<(Vec<(NodeId, T)>, MetadataFanoutStats), MetadataApiError>
where
    T: Send + 'static,
{
    let span = Span::current();
    let MetadataFanoutScope {
        mode,
        target_nodes,
        allow_partial,
        discovery_failed: scope_discovery_failed,
        subject: request_subject,
        deadline: scope_deadline,
    } = scope;
    let deadline = scope_deadline
        .unwrap_or_else(|| tokio::time::Instant::now() + METADATA_DISTRIBUTED_QUERY_DEADLINE);
    ensure_supported_query_mode(&mode);
    match mode.unwrap_or(MetadataApiQueryMode::Distributed) {
        MetadataApiQueryMode::Local => {
            let result = run_metadata_fanout_node(
                operation,
                local_node_id,
                true,
                deadline,
                local_call,
                remote_call,
                record_result,
                false,
            )
            .await;
            let fanout_stats = MetadataFanoutStats {
                nodes_queried: 1,
                nodes_failed: 0,
                failed_partitions: Vec::new(),
                discovery_failed: false,
            };
            match result {
                Ok(result) => Ok((vec![(local_node_id, result)], fanout_stats)),
                Err(error) => Err(map_local_error(error)),
            }
        }
        MetadataApiQueryMode::Distributed => {
            let discovery = metadata_fanout_nodes(
                context,
                realm_id,
                local_node_id,
                &span,
                target_nodes,
                deadline,
            )
            .await;
            let discovery_failed = scope_discovery_failed || discovery.failed;
            let mut nodes = discovery.nodes;
            if discovery_failed && !allow_partial {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            let failed_partitions = Vec::new();
            let mut omitted_nodes = 0usize;
            if nodes.len() > METADATA_DISTRIBUTED_QUERY_MAX_NODES {
                let mut subject = Vec::with_capacity(32 + operation.label().len() + 32 + 1);
                subject.extend_from_slice(realm_id.as_bytes());
                subject.extend_from_slice(operation.label().as_bytes());
                if let Some(request_subject) = request_subject {
                    subject.extend_from_slice(&request_subject);
                }
                subject.extend_from_slice(local_node_id.as_bytes());
                let selected = select_fanout_nodes(&nodes, local_node_id, &subject);
                omitted_nodes = nodes.len().saturating_sub(selected.len());
                nodes = selected;
            }
            span.record("node_count", nodes.len() as u64);
            let mut fanout_stats = MetadataFanoutStats {
                nodes_queried: nodes.len(),
                nodes_failed: failed_partitions.len()
                    + omitted_nodes
                    + usize::from(discovery_failed),
                failed_partitions,
                discovery_failed,
            };
            let fanout_started = Instant::now();
            let mut node_parts = Vec::new();
            let mut auth_error = None;
            let mut not_found = false;
            let node_order = nodes.clone();
            let mut outstanding = nodes.iter().copied().collect::<HashSet<_>>();
            // Every interactive fanout gets the overall deadline; offline
            // partitions land in failed_partitions and partial-tolerant
            // callers (search) still answer from the reachable nodes.
            let pending =
                stream::iter(nodes.into_iter().enumerate().map(|(node_index, node_id)| {
                    let local_call = local_call.clone();
                    let remote_call = remote_call.clone();
                    async move {
                        let result = run_metadata_fanout_node(
                            operation,
                            node_id,
                            node_id == local_node_id,
                            deadline,
                            local_call,
                            remote_call,
                            record_result,
                            true,
                        )
                        .await;
                        (node_index, node_id, result)
                    }
                }))
                .buffer_unordered(METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT);
            futures_util::pin_mut!(pending);

            loop {
                let next = match tokio::time::timeout_at(deadline, pending.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        fanout_stats
                            .failed_partitions
                            .extend(outstanding.iter().copied());
                        fanout_stats.nodes_failed = fanout_stats.failed_partitions.len()
                            + omitted_nodes
                            + usize::from(fanout_stats.discovery_failed);
                        if !allow_partial && fanout_stats.nodes_failed > 0 {
                            return Err(MetadataApiError::ServiceUnavailable);
                        }
                        break;
                    }
                };
                let Some((node_index, node_id, result)) = next else {
                    break;
                };
                outstanding.remove(&node_id);
                match result {
                    Ok(result) => node_parts.push((node_index, node_id, result)),
                    // A rejected forwarded credential is a failed partition;
                    // local authorization already vouched for the caller.
                    Err(MetadataReadError::Unauthorized) => {
                        fanout_stats.nodes_failed += 1;
                        fanout_stats.failed_partitions.push(node_id);
                        warn!(
                            node_id = ?node_id,
                            operation = operation.label(),
                            "distributed metadata skipped unauthorized node result"
                        );
                    }
                    // An authenticated denial anywhere fails the whole query.
                    Err(error @ MetadataReadError::Forbidden) => {
                        fanout_stats.nodes_failed += 1;
                        fanout_stats.failed_partitions.push(node_id);
                        auth_error.get_or_insert(map_read_error(error));
                    }
                    Err(MetadataReadError::NotFound) => {
                        fanout_stats.nodes_failed += 1;
                        fanout_stats.failed_partitions.push(node_id);
                        not_found = true;
                    }
                    Err(MetadataReadError::Unavailable) => {
                        fanout_stats.nodes_failed += 1;
                        fanout_stats.failed_partitions.push(node_id);
                        warn!(
                            node_id = ?node_id,
                            operation = operation.label(),
                            error = "unavailable",
                            "distributed metadata skipped failed node result"
                        );
                    }
                }
            }

            if let Some(error) = auth_error {
                return Err(error);
            }
            if not_found {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            // Nodes omitted by the fanout cap truncate the answer, so a caller
            // that asked for a complete result must not be told it is complete.
            if !allow_partial && fanout_stats.nodes_failed > 0 {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            node_parts.sort_by_key(|(node_index, _, _)| *node_index);
            fanout_stats.failed_partitions.sort_by_key(|node_id| {
                node_order
                    .iter()
                    .position(|candidate| candidate == node_id)
                    .unwrap_or(usize::MAX)
            });
            aruna_core::telemetry::record_stage("fanout", fanout_started.elapsed());
            Ok((
                node_parts
                    .into_iter()
                    .map(|(_, node_id, result)| (node_id, result))
                    .collect(),
                fanout_stats,
            ))
        }
    }
}

pub async fn search_buckets_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: BucketSearchRequest,
) -> Result<BucketSearchExecution, MetadataApiError> {
    let deadline = tokio::time::Instant::now() + METADATA_DISTRIBUTED_QUERY_DEADLINE;
    let limit = request.limit.clamp(1, 50);
    let subject = query_fingerprint(
        &request.query,
        None,
        Some(MetadataApiQueryMode::Distributed),
        None,
        None,
    );
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let remote_auth_token = fanout_bearer(request.bearer_token.as_deref());
    let local_call: MetadataNodeCall<Vec<BucketSearchHit>> = metadata_node_call(
        (
            context.clone(),
            request.auth,
            realm_id,
            request.query.clone(),
            limit,
        ),
        |(context, auth, realm_id, query, limit), node_id| async move {
            search_local_buckets(
                &context,
                SearchBucketsInput {
                    auth,
                    realm_id,
                    node_id,
                    query,
                    limit,
                    start_after: None,
                },
            )
            .await
            .map_err(|_| MetadataReadError::Unavailable)
        },
    );
    let remote_call: MetadataNodeCall<Vec<BucketSearchHit>> = metadata_node_call(
        (handle, remote_auth_token, request.query, limit),
        |(handle, auth_token, query, limit), node_id| async move {
            handle
                .request_bucket_search(node_id, auth_token, query, limit)
                .await
        },
    );
    let (parts, fanout_stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        MetadataFanoutScope::new(
            Some(MetadataApiQueryMode::Distributed),
            request.target_nodes,
            true,
        )
        .with_subject(subject)
        .with_deadline(deadline),
        MetadataFanoutOperation::BucketSearch,
        local_call,
        remote_call,
        record_bucket_result,
        map_read_error,
    )
    .await?;
    let mut hits = parts
        .into_iter()
        .flat_map(|(_, hits)| hits)
        .collect::<Vec<_>>();
    hits.truncate(limit);
    Ok(BucketSearchExecution { hits, fanout_stats })
}

pub async fn search_objects(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: ObjectSearchRequest,
) -> Result<ObjectSearchExecution, MetadataApiError> {
    if request.query.is_empty() || request.auth.realm_id != realm_id {
        return Err(if request.query.is_empty() {
            MetadataApiError::BadRequest
        } else {
            MetadataApiError::Forbidden
        });
    }
    let deadline = tokio::time::Instant::now() + METADATA_DISTRIBUTED_QUERY_DEADLINE;
    let limit = request
        .limit
        .clamp(1, crate::s3::search_objects::OBJECT_SEARCH_MAX_LIMIT);
    let fingerprint = object_search_fingerprint(
        realm_id,
        &request.query,
        request.key_match,
        request.bucket.as_deref(),
        request.mode,
    );

    let (as_of, mut partitions, mut failed_partitions, discovery_failed, omitted_partitions) =
        match request.cursor.as_deref() {
            Some(raw) => {
                let authorized_signers = match request.mode {
                    ObjectSearchQueryMode::Local => vec![local_node_id],
                    ObjectSearchQueryMode::DistributedBestEffort
                    | ObjectSearchQueryMode::DistributedStrict => {
                        load_realm_config(context, realm_id)
                            .await
                            .ok_or(MetadataApiError::ServiceUnavailable)?
                            .node_ids()
                            .map_err(|_| MetadataApiError::ServiceUnavailable)?
                    }
                };
                let cursor = ObjectSearchCursor::decode(raw, fingerprint, &authorized_signers)?;
                let partitions = cursor.partition_states()?;
                if request.mode == ObjectSearchQueryMode::Local
                    && (partitions.len() != 1 || partitions[0].node_id != local_node_id)
                {
                    return Err(MetadataApiError::InvalidCursor(
                        "invalid local object search cursor".to_string(),
                    ));
                }
                (
                    cursor.payload.as_of,
                    partitions,
                    cursor.failed_nodes()?,
                    cursor.payload.discovery_failed,
                    cursor.payload.omitted_partitions,
                )
            }
            None => {
                let as_of = SystemTime::now();
                let (mut nodes, discovery_failed) =
                    match request.mode {
                        ObjectSearchQueryMode::Local => (vec![local_node_id], false),
                        ObjectSearchQueryMode::DistributedBestEffort
                        | ObjectSearchQueryMode::DistributedStrict => {
                            match request.target_nodes.clone() {
                                Some(nodes) => (deduplicate_fanout_nodes(nodes), false),
                                None => {
                                    let discovery = tokio::time::timeout_at(
                                        deadline,
                                        discover_realm_nodes(context, realm_id, local_node_id),
                                    )
                                    .await
                                    .unwrap_or(MetadataRealmNodeDiscovery {
                                        nodes: vec![local_node_id],
                                        failed: true,
                                    });
                                    (discovery.nodes, discovery.failed)
                                }
                            }
                        }
                    };
                if nodes.is_empty() {
                    return Err(MetadataApiError::ServiceUnavailable);
                }
                let omitted_partitions = if nodes.len() > METADATA_DISTRIBUTED_QUERY_MAX_NODES {
                    let selected = select_fanout_nodes(&nodes, local_node_id, &fingerprint);
                    let omitted = nodes.len().saturating_sub(selected.len());
                    nodes = selected;
                    omitted
                } else {
                    0
                };
                if request.mode == ObjectSearchQueryMode::DistributedStrict
                    && (discovery_failed || omitted_partitions > 0)
                {
                    return Err(MetadataApiError::ServiceUnavailable);
                }
                nodes.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
                (
                    as_of,
                    nodes
                        .into_iter()
                        .map(|node_id| ObjectSearchPartitionState {
                            node_id,
                            start_after: None,
                            exhausted: false,
                            observed_at: None,
                        })
                        .collect(),
                    Vec::new(),
                    discovery_failed,
                    omitted_partitions,
                )
            }
        };

    if request.mode == ObjectSearchQueryMode::DistributedStrict
        && (discovery_failed || omitted_partitions > 0 || !failed_partitions.is_empty())
    {
        return Err(MetadataApiError::ServiceUnavailable);
    }

    let active_nodes = partitions
        .iter()
        .filter(|partition| !partition.exhausted)
        .map(|partition| partition.node_id)
        .collect::<Vec<_>>();
    if active_nodes.is_empty() {
        return Err(MetadataApiError::InvalidCursor(
            "exhausted object search cursor".to_string(),
        ));
    }
    let start_positions = partitions
        .iter()
        .map(|partition| (partition.node_id, partition.start_after.clone()))
        .collect::<HashMap<_, _>>();
    let remote_auth_token = fanout_bearer(request.bearer_token.as_deref());
    let handle = context.metadata_handle.clone();

    let local_call: MetadataNodeCall<ObjectSearchNodePage> = metadata_node_call(
        (
            context.clone(),
            request.auth,
            realm_id,
            request.query.clone(),
            request.key_match,
            request.bucket.clone(),
            limit,
            as_of,
            start_positions.clone(),
        ),
        |(context, auth, realm_id, query, key_match, bucket, limit, as_of, starts), node_id| async move {
            search_local_objects(
                &context,
                SearchObjectsInput {
                    auth,
                    realm_id,
                    node_id,
                    query,
                    key_match,
                    bucket,
                    limit,
                    start_after: starts.get(&node_id).cloned().flatten(),
                    as_of,
                },
            )
            .await
            .map_err(|_| MetadataReadError::Unavailable)
        },
    );
    let remote_call: MetadataNodeCall<ObjectSearchNodePage> = metadata_node_call(
        (
            handle,
            remote_auth_token,
            request.query,
            request.key_match,
            request.bucket,
            limit,
            as_of,
            start_positions,
        ),
        |(handle, auth_token, query, key_match, bucket, limit, as_of, starts), node_id| async move {
            let Some(handle) = handle else {
                return Err(MetadataReadError::Unavailable);
            };
            handle
                .request_object_search(
                    node_id,
                    auth_token,
                    query,
                    key_match,
                    bucket,
                    limit,
                    starts.get(&node_id).cloned().flatten(),
                    as_of,
                )
                .await
        },
    );
    let (parts, mut fanout_stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        MetadataFanoutScope::new(
            Some(request.mode.fanout_mode()),
            Some(active_nodes),
            request.mode.allow_partial(),
        )
        .with_subject(fingerprint)
        .with_discovery_failed(discovery_failed)
        .with_deadline(deadline),
        MetadataFanoutOperation::ObjectSearch,
        local_call,
        remote_call,
        record_object_result,
        map_read_error,
    )
    .await?;

    let newly_failed = fanout_stats.failed_partitions.clone();
    failed_partitions.extend(newly_failed.iter().copied());
    failed_partitions.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    failed_partitions.dedup();
    partitions.retain(|partition| !newly_failed.contains(&partition.node_id));
    partitions
        .sort_unstable_by(|left, right| left.node_id.as_bytes().cmp(right.node_id.as_bytes()));

    let mut pages = parts.into_iter().collect::<HashMap<_, _>>();
    let mut hits = Vec::with_capacity(limit);
    let mut remaining = limit;
    for partition in &mut partitions {
        if partition.exhausted {
            continue;
        }
        let Some(page) = pages.remove(&partition.node_id) else {
            continue;
        };
        partition.observed_at = Some(page.observed_at);
        if remaining == 0 {
            if page.hits.is_empty() && page.next_start_after.is_none() {
                partition.exhausted = true;
            }
            continue;
        }

        let consumed = remaining.min(page.hits.len());
        hits.extend(
            page.hits
                .iter()
                .take(consumed)
                .map(|candidate| candidate.hit.clone()),
        );
        remaining = remaining.saturating_sub(consumed);
        if consumed < page.hits.len() {
            partition.start_after = page
                .hits
                .get(consumed.saturating_sub(1))
                .map(|candidate| candidate.cursor_key.clone());
            partition.exhausted = false;
        } else if consumed > 0 || page.hits.is_empty() {
            partition.start_after = page.next_start_after;
            partition.exhausted = partition.start_after.is_none();
        }
    }

    let next_cursor = if partitions.iter().any(|partition| !partition.exhausted) {
        let net = context.net_handle.as_ref().ok_or_else(|| {
            MetadataApiError::Internal(
                "net handle unavailable for object search cursor signing".to_string(),
            )
        })?;
        Some(
            ObjectSearchCursor::new_signed(
                fingerprint,
                as_of,
                &partitions,
                &failed_partitions,
                discovery_failed,
                omitted_partitions,
                net.node_id(),
                |bytes| net.sign(bytes),
            )
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?
            .encode()
            .map_err(|error| MetadataApiError::Internal(error.to_string()))?,
        )
    } else {
        None
    };
    let coverage = partitions
        .iter()
        .filter_map(|partition| {
            partition
                .observed_at
                .map(|observed_at| ObjectSearchPartitionCoverage {
                    node_id: partition.node_id,
                    observed_at,
                    truncated: !partition.exhausted,
                })
        })
        .collect::<Vec<_>>();

    fanout_stats.failed_partitions = failed_partitions;
    fanout_stats.nodes_failed =
        fanout_stats.failed_partitions.len() + omitted_partitions + usize::from(discovery_failed);
    let complete = fanout_stats.nodes_failed == 0;
    Ok(ObjectSearchExecution {
        hits,
        next_cursor,
        as_of,
        partitions: coverage,
        fanout_stats,
        omitted_partitions,
        complete,
    })
}

fn object_search_fingerprint(
    realm_id: RealmId,
    query: &str,
    key_match: ObjectKeyMatch,
    bucket: Option<&str>,
    mode: ObjectSearchQueryMode,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna.object.search.v1\0");
    hasher.update(realm_id.as_bytes());
    hasher.update(query.as_bytes());
    hasher.update(&[0]);
    hasher.update(&[match key_match {
        ObjectKeyMatch::Substring => 1,
        ObjectKeyMatch::Prefix => 2,
    }]);
    match bucket {
        Some(bucket) => {
            hasher.update(&[1]);
            hasher.update(bucket.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
    hasher.update(&[match mode {
        ObjectSearchQueryMode::Local => 1,
        ObjectSearchQueryMode::DistributedBestEffort => 2,
        ObjectSearchQueryMode::DistributedStrict => 3,
    }]);
    *hasher.finalize().as_bytes()
}

fn record_object_result(span: &Span, result: &Result<ObjectSearchNodePage, MetadataReadError>) {
    match result {
        Ok(page) => {
            span.record("result", "ok");
            span.record("hit_count", page.hits.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

fn record_bucket_result(span: &Span, result: &Result<Vec<BucketSearchHit>, MetadataReadError>) {
    match result {
        Ok(hits) => {
            span.record("result", "ok");
            span.record("hit_count", hits.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

fn record_query_result(span: &Span, result: &Result<MetadataQueryResults, MetadataReadError>) {
    match result {
        Ok(result) => {
            span.record("result", result.kind());
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

fn record_search_node_result(
    span: &Span,
    result: &Result<(Vec<MetadataSearchHit>, usize), MetadataReadError>,
) {
    match result {
        Ok((hits, _)) => {
            span.record("result", "ok");
            span.record("hit_count", hits.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

fn record_preflight_node_result(
    span: &Span,
    result: &Result<MetadataReferencePreflightNodeExecution, MetadataReadError>,
) {
    match result {
        Ok(result) => {
            span.record("result", "ok");
            span.record("hit_count", result.visible_references.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

#[tracing::instrument(
    name = "metadata.operation.query_distributed",
    level = "debug",
    skip(context, auth, bearer_token, query, scope),
    fields(
        mode = ?scope.mode,
        query_len = query.len() as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        node_count = field::Empty,
        discovery_ms = field::Empty,
        elapsed_ms = field::Empty,
        result = field::Empty,
        cache = field::Empty,
    )
)]
#[allow(clippy::too_many_arguments)]
async fn run_query_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: Option<AuthContext>,
    bearer_token: Option<String>,
    graph_iris: Option<Vec<String>>,
    query: String,
    scope: MetadataFanoutScope,
) -> Result<(MetadataQueryResults, MetadataFanoutStats), MetadataApiError> {
    let span = Span::current();
    let total_started = Instant::now();
    let mode = scope.mode.unwrap_or(MetadataApiQueryMode::Distributed);
    let single_dataset_result = mode == MetadataApiQueryMode::Local || graph_iris.is_some();
    if mode == MetadataApiQueryMode::Distributed
        && graph_iris.is_none()
        && !distributed_query_is_union_safe(&query)
    {
        return Err(MetadataApiError::BadRequest);
    }
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let query_form = query_form(&query).ok_or(MetadataApiError::BadRequest)?;
    let select_limit = match query_form {
        MetadataQueryForm::Select => query_select_limit(&query),
        MetadataQueryForm::Ask => None,
    };
    let remote_auth_token = fanout_bearer(bearer_token.as_deref());

    // Remote partitions authorize on the forwarded credential, so entries are
    // partitioned by credential digest. The local invalidation signals only
    // cover the local partition; the TTL bounds remote staleness.
    let cache_stamp = handle.query_cache().stamp(handle.visibility_generation());
    let cache_key = super::query_cache::credential_digest(auth.as_ref(), bearer_token.as_deref())
        .map(|credential| {
            super::query_cache::remote_key(&super::query_cache::RemoteKeyInput {
                distributed: mode == MetadataApiQueryMode::Distributed,
                realm_id,
                credential: &credential,
                graph_iris: graph_iris.as_deref(),
                sparql: &query,
                allow_partial: scope.allow_partial,
                target_nodes: scope.target_nodes.as_deref(),
            })
        })
        .filter(|_| !scope.discovery_failed);
    if let Some(key) = cache_key
        && let Some(cached) = handle.query_cache().get(&key, cache_stamp, Instant::now())
    {
        span.record("cache", "hit");
        span.record("result", cached.results.kind());
        record_elapsed_ms(&span, "elapsed_ms", total_started);
        return Ok((
            (*cached.results).clone(),
            super::query_cache::cached_stats(&cached),
        ));
    }
    span.record("cache", "miss");

    let local_call: MetadataNodeCall<MetadataQueryResults> = metadata_node_call(
        (
            handle.clone(),
            auth.clone(),
            graph_iris.clone(),
            query.clone(),
        ),
        |(handle, auth, graph_iris, query), _| async move {
            handle
                .query_authorized_local(auth, graph_iris, query)
                .await
                .map_err(super::handle::metadata_read_error)
        },
    );
    let remote_call: MetadataNodeCall<MetadataQueryResults> = metadata_node_call(
        (
            handle.clone(),
            remote_auth_token.clone(),
            graph_iris.clone(),
            query.clone(),
        ),
        |(handle, auth_token, graph_iris, query), node_id| async move {
            handle
                .request_remote_query_graphs(node_id, auth_token, graph_iris, query)
                .await
        },
    );
    let (parts, fanout_stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        scope,
        MetadataFanoutOperation::Query,
        local_call,
        remote_call,
        record_query_result,
        map_read_error,
    )
    .await?;

    let parts: Vec<_> = parts.into_iter().map(|(_, result)| result).collect();
    let result = if single_dataset_result {
        match parts.into_iter().next() {
            Some(result) => Ok(result),
            None => aggregate_query_results(Vec::new(), query_form, select_limit),
        }
    } else {
        aggregate_query_results(parts, query_form, select_limit)
    };
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    match &result {
        Ok(results) => {
            span.record("result", results.kind());
            if let Some(key) = cache_key
                && super::query_cache::store_complete(
                    handle.query_cache(),
                    key,
                    results,
                    &fanout_stats,
                    cache_stamp,
                    handle.visibility_generation(),
                    Instant::now(),
                )
            {
                span.record("cache", "stored");
            }
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
    result.map(|results| (results, fanout_stats))
}

#[tracing::instrument(
    name = "metadata.operation.search_distributed",
    level = "debug",
    skip(context, auth, bearer_token, query, resume, watermark, scope),
    fields(
        mode = ?scope.mode,
        query_len = query.len() as u64,
        page_size = page_size as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        node_count = field::Empty,
        discovery_ms = field::Empty,
        elapsed_ms = field::Empty,
        hit_count = field::Empty,
    )
)]
#[allow(clippy::too_many_arguments)]
async fn run_search_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: Option<AuthContext>,
    bearer_token: Option<String>,
    graph_iris: Option<Vec<String>>,
    query: String,
    conforms_to: Option<String>,
    group_id: Option<GroupId>,
    resume: HashMap<NodeId, u32>,
    watermark: Option<SearchWatermark>,
    page_size: usize,
    scope: MetadataFanoutScope,
) -> Result<
    (
        Vec<MetadataSearchHit>,
        Option<SearchPageCursor>,
        bool,
        MetadataFanoutStats,
    ),
    MetadataApiError,
> {
    let span = Span::current();
    let total_started = Instant::now();
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let remote_auth_token = fanout_bearer(bearer_token.as_deref());
    let resume = Arc::new(resume);

    let local_call: MetadataNodeCall<(Vec<MetadataSearchHit>, usize)> = metadata_node_call(
        (
            handle.clone(),
            auth.clone(),
            graph_iris.clone(),
            query.clone(),
            conforms_to.clone(),
            group_id,
            resume.clone(),
            page_size,
        ),
        |(handle, auth, graph_iris, query, conforms_to, group_id, resume, page_size), node_id| async move {
            let limit = resume_fetch_limit(
                &resume,
                node_id,
                page_size,
                METADATA_SEARCH_MAX_PAGINATION_DEPTH,
            );
            let hits = match conforms_to {
                Some(object_iri) => {
                    let mut hits = Vec::new();
                    for object_iri in
                        crate::metadata::profile_validation::equivalent_profile_iris(&object_iri)
                    {
                        hits.extend(
                            handle
                                .search_authorized_local_filtered(
                                    auth.clone(),
                                    graph_iris.clone(),
                                    query.clone(),
                                    limit,
                                    super::iri_index::DCTERMS_CONFORMS_TO_IRI.to_string(),
                                    object_iri,
                                    group_id,
                                )
                                .await
                                .map_err(super::handle::metadata_read_error)?,
                        );
                    }
                    merge_search_hits(hits).into_iter().take(limit).collect()
                }
                None => handle
                    .search_authorized_local(auth, graph_iris, query, limit, group_id)
                    .await
                    .map_err(super::handle::metadata_read_error)?,
            };
            Ok((hits, limit))
        },
    );
    let remote_call: MetadataNodeCall<(Vec<MetadataSearchHit>, usize)> = metadata_node_call(
        (
            handle.clone(),
            remote_auth_token.clone(),
            graph_iris.clone(),
            query.clone(),
            conforms_to,
            group_id,
            resume.clone(),
            page_size,
        ),
        |(handle, auth_token, graph_iris, query, conforms_to, group_id, resume, page_size),
         node_id| async move {
            let limit = resume_fetch_limit(
                &resume,
                node_id,
                page_size,
                METADATA_SEARCH_MAX_PAGINATION_DEPTH,
            );
            let hits = match conforms_to {
                Some(object_iri) => {
                    let mut hits = Vec::new();
                    for object_iri in
                        crate::metadata::profile_validation::equivalent_profile_iris(&object_iri)
                    {
                        hits.extend(
                            handle
                                .request_remote_filtered_search_graphs(
                                    node_id,
                                    auth_token.clone(),
                                    graph_iris.clone(),
                                    query.clone(),
                                    limit,
                                    super::iri_index::DCTERMS_CONFORMS_TO_IRI.to_string(),
                                    object_iri,
                                    group_id,
                                )
                                .await?,
                        );
                    }
                    merge_search_hits(hits).into_iter().take(limit).collect()
                }
                None => {
                    handle
                        .request_remote_search_graphs(
                            node_id, auth_token, graph_iris, query, limit, group_id,
                        )
                        .await?
                }
            };
            Ok((hits, limit))
        },
    );
    let (node_parts, fanout_stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        scope,
        MetadataFanoutOperation::Search,
        local_call,
        remote_call,
        record_search_node_result,
        map_read_error,
    )
    .await?;

    let node_results = node_parts
        .into_iter()
        .map(|(node_id, (hits, requested))| NodeSearchResult {
            node_id,
            saturated: hits.len() >= requested,
            hits,
        })
        .collect();
    let page = paginate(
        node_results,
        watermark,
        page_size,
        METADATA_SEARCH_MAX_PAGINATION_DEPTH,
    );
    span.record("hit_count", page.hits.len() as u64);
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    Ok((page.hits, page.next, page.truncated, fanout_stats))
}

pub fn aggregate_query_results(
    results: Vec<MetadataQueryResults>,
    query_form: MetadataQueryForm,
    select_limit: Option<usize>,
) -> Result<MetadataQueryResults, MetadataApiError> {
    match query_form {
        MetadataQueryForm::Ask => {
            Ok(MetadataQueryResults::Boolean(results.into_iter().any(
                |result| matches!(result, MetadataQueryResults::Boolean(true)),
            )))
        }
        MetadataQueryForm::Select => {
            let mut seen = HashSet::new();
            let mut merged = Vec::new();
            let mut merged_bytes = 32usize;
            let row_limit = select_limit
                .unwrap_or(METADATA_QUERY_MAX_ROWS)
                .min(METADATA_QUERY_MAX_ROWS);
            if row_limit == 0 {
                return Ok(MetadataQueryResults::Solutions(Vec::new()));
            }
            for result in results {
                let MetadataQueryResults::Solutions(rows) = result else {
                    continue;
                };
                for row in rows {
                    let key = serde_json::to_string(&row)
                        .map_err(|err| MetadataApiError::Internal(err.to_string()))?;
                    if seen.insert(key) {
                        merged_bytes = merged_bytes.saturating_add(
                            serde_json::to_vec(&row)
                                .map_err(|err| MetadataApiError::Internal(err.to_string()))?
                                .len()
                                .saturating_add(1),
                        );
                        if merged_bytes > METADATA_QUERY_MAX_RESULT_BYTES {
                            return Err(MetadataApiError::BadRequest);
                        }
                        merged.push(row);
                        if merged.len() >= row_limit {
                            return Ok(MetadataQueryResults::Solutions(merged));
                        }
                    }
                }
            }
            Ok(MetadataQueryResults::Solutions(merged))
        }
    }
}

pub fn query_select_limit(query: &str) -> Option<usize> {
    let parsed = spargebra::SparqlParser::new().parse_query(query).ok()?;
    let spargebra::Query::Select { pattern, .. } = parsed else {
        return None;
    };
    let spargebra::algebra::GraphPattern::Slice { length, .. } = pattern else {
        return None;
    };
    length
}

pub fn query_form(query: &str) -> Option<MetadataQueryForm> {
    match spargebra::SparqlParser::new().parse_query(query).ok()? {
        spargebra::Query::Select { .. } => Some(MetadataQueryForm::Select),
        spargebra::Query::Ask { .. } => Some(MetadataQueryForm::Ask),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::export::raw_identity_matches;
    use super::fanout::ensure_supported_query_form;
    use super::path::{
        reduce_path_response, sanitize_path_winner, select_forward_peers, validate_path_resolution,
    };
    use super::read::ensure_permission;

    use std::collections::BTreeMap;

    use aruna_core::UserId;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::metadata::MetadataCreateEventPayload;
    use aruna_core::storage_entries::{
        metadata_create_event_and_pending_projection_write_entries,
        metadata_document_lifecycle_write_entry, metadata_graph_lifecycle_write_entry,
    };
    use aruna_core::structs::{
        Actor, Group, GroupAuthorizationDocument, PlacementRef, RealmAuthorizationDocument,
        RealmNodeKind, Role,
    };
    use aruna_core::structured_id::{BucketId, PlacementHandle};
    use aruna_core::types::{Key, RoleId};
    use aruna_storage::storage;
    use byteview::ByteView;
    use tempfile::{TempDir, tempdir};

    use crate::metadata::MetadataHandle;

    const TEST_REALM_ID: RealmId = RealmId([7u8; 32]);

    struct MetadataTest {
        context: DriverContext,
        _storage_dir: TempDir,
        _metadata_dir: TempDir,
    }

    fn metadata_test() -> MetadataTest {
        let storage_dir = tempdir().expect("storage dir");
        let metadata_dir = tempdir().expect("metadata dir");
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().expect("storage path"))
                .expect("storage opens");
        let metadata_handle = MetadataHandle::new(
            metadata_dir.path(),
            iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            storage_handle.clone(),
            None,
            None,
            None,
        )
        .expect("metadata handle");
        MetadataTest {
            context: DriverContext {
                storage_handle,
                net_handle: None,
                blob_handle: None,
                metadata_handle: Some(metadata_handle),
                task_handle: None,
                compute_handle: None,
            },
            _storage_dir: storage_dir,
            _metadata_dir: metadata_dir,
        }
    }

    fn public_record(group_id: GroupId, document_id: Ulid) -> MetadataRegistryRecord {
        let event_id = Ulid::generate();
        let document_id = MetaResourceId::from_parts(
            document_id.timestamp_ms(),
            PlacementHandle::new(1).unwrap(),
            BucketId::new(0).unwrap(),
            document_id.0 as u64 & ((1_u64 << 48) - 1),
        )
        .unwrap()
        .as_ulid();
        let document_path = format!("datasets/cached/{document_id}");
        MetadataRegistryRecord {
            realm_id: TEST_REALM_ID,
            group_id,
            document_id,
            document_path: document_path.clone(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &TEST_REALM_ID,
                group_id,
                &document_path,
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: event_id,
            last_event_id: event_id,
        }
    }

    fn summary_request(
        group_id: GroupId,
        include_summary: bool,
    ) -> ListVisibleMetadataDocumentsRequest {
        ListVisibleMetadataDocumentsRequest {
            group_id: Some(group_id),
            path_prefix: None,
            include_summary,
            limit: None,
            offset: None,
            order: MetadataListOrder::default(),
            auth: None,
        }
    }

    // The visibility cache only accepts upserts once it has been filled.
    async fn seed_registry_cache(test: &MetadataTest, record: &MetadataRegistryRecord) {
        seed_policy_docs(test, record.group_id).await;
        let handle = test
            .context
            .metadata_handle
            .as_ref()
            .expect("metadata handle");
        handle
            .list_cached_registry_records_for_group(record.group_id)
            .await
            .expect("registry cache fills");
        handle.upsert_cached_registry_record(record.clone());
    }

    // Policy loading fails closed without realm config and group documents.
    async fn seed_policy_docs(test: &MetadataTest, group_id: GroupId) {
        let owner = UserId::nil(TEST_REALM_ID);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            user_id: owner,
            realm_id: TEST_REALM_ID,
        };
        let config = RealmConfigDocument::default_for_realm(TEST_REALM_ID, Vec::new());
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(TEST_REALM_ID);
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(owner, TEST_REALM_ID, group_id);
        let group = Group {
            display_name: "policy-fixture".to_string(),
            group_id,
            realm_id: TEST_REALM_ID,
            roles: group_auth.roles.keys().copied().collect(),
            owner,
        };
        let writes = [
            (
                aruna_core::keyspaces::REALM_CONFIG_KEYSPACE,
                ByteView::from(*TEST_REALM_ID.as_bytes()),
                config.to_bytes(&actor).expect("config serializes"),
            ),
            (
                AUTH_KEYSPACE,
                ByteView::from(*TEST_REALM_ID.as_bytes()),
                realm_auth.to_bytes(&actor).expect("realm auth serializes"),
            ),
            (
                AUTH_KEYSPACE,
                ByteView::from(group_id.to_bytes().to_vec()),
                group_auth.to_bytes(&actor).expect("group auth serializes"),
            ),
            (
                GROUP_KEYSPACE,
                ByteView::from(group_id.to_bytes().to_vec()),
                group.to_bytes(&actor).expect("group serializes"),
            ),
        ];
        for (key_space, key, value) in writes {
            let existing = test
                .context
                .storage_handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: key_space.to_string(),
                    key: key.clone(),
                    txn_id: None,
                })
                .await;
            if matches!(
                existing,
                Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
            ) {
                continue;
            }
            let event = test
                .context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key,
                    value: ByteView::from(value),
                    txn_id: None,
                })
                .await;
            assert!(matches!(
                event,
                Event::Storage(StorageEvent::WriteResult { .. })
            ));
        }
    }

    async fn write_pending_marker(test: &MetadataTest, record: &MetadataRegistryRecord) {
        seed_policy_docs(test, record.group_id).await;
        let event = MetadataCreateEventRecord {
            event_id: record.last_event_id,
            record: record.clone(),
            user_id: UserId::local(Ulid::generate(), TEST_REALM_ID),
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            payload: MetadataCreateEventPayload::Scaffold {
                name: "Pending".to_string(),
                description: "Projection in flight".to_string(),
                date_published: "2026-01-01".to_string(),
                license: None,
            },
            occurred_at_ms: 1,
        };
        for (key_space, key, value) in
            metadata_create_event_and_pending_projection_write_entries(&event)
                .expect("event encodes")
        {
            match test
                .context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space,
                    key,
                    value,
                    txn_id: None,
                })
                .await
            {
                Event::Storage(StorageEvent::WriteResult { .. }) => {}
                other => panic!("unexpected write event: {other:?}"),
            }
        }
    }

    async fn write_entry(test: &MetadataTest, entry: (String, ByteView, ByteView)) {
        let (key_space, key, value) = entry;
        match test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn filters_graph_delete() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let live = public_record(group_id, Ulid::generate());
        let deleted = public_record(group_id, Ulid::generate());
        let tombstone = MetadataGraphLifecycleRecord::deleted(
            deleted.graph_iri.clone(),
            deleted.realm_id,
            deleted.group_id,
            deleted.document_id,
            2,
        );
        write_entry(
            &test,
            metadata_graph_lifecycle_write_entry(&tombstone).expect("lifecycle entry"),
        )
        .await;

        let records = filter_live_records(&test.context.storage_handle, &[live.clone(), deleted])
            .await
            .expect("lifecycle filter succeeds");
        assert_eq!(records, vec![live]);
    }

    #[tokio::test]
    async fn filters_document_delete() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let deleted = public_record(group_id, Ulid::generate());
        let tombstone = MetadataGraphLifecycleRecord::deleted(
            deleted.graph_iri.clone(),
            deleted.realm_id,
            deleted.group_id,
            deleted.document_id,
            2,
        );
        let lifecycle = aruna_core::metadata::MetadataDocumentLifecycleRecord::Delete {
            event: aruna_core::metadata::MetadataDocumentDeleteRecord {
                event_id: Ulid::generate(),
                tombstone,
                deleted_after_event_id: deleted.last_event_id,
            },
        };
        write_entry(
            &test,
            metadata_document_lifecycle_write_entry(&lifecycle).expect("lifecycle entry"),
        )
        .await;

        let records = filter_live_records(&test.context.storage_handle, &[deleted])
            .await
            .expect("lifecycle filter succeeds");
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn rejects_bad_lifecycle() {
        let test = metadata_test();
        let record = public_record(Ulid::generate(), Ulid::generate());
        write_entry(
            &test,
            (
                METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
                metadata_graph_lifecycle_key(&record.graph_iri),
                ByteView::from(vec![1u8]),
            ),
        )
        .await;

        assert!(
            filter_live_records(&test.context.storage_handle, &[record])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn foreign_lifecycle_rejected() {
        // A well-formed lifecycle entry that belongs to another document must
        // not be allowed to decide this record's visibility.
        let test = metadata_test();
        let graph_record = public_record(Ulid::generate(), Ulid::generate());
        let document_record = public_record(Ulid::generate(), Ulid::generate());
        let stranger = public_record(Ulid::generate(), Ulid::generate());
        let tombstone = MetadataGraphLifecycleRecord::deleted(
            stranger.graph_iri.clone(),
            stranger.realm_id,
            stranger.group_id,
            stranger.document_id,
            2,
        );
        write_entry(
            &test,
            (
                METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
                metadata_graph_lifecycle_key(&graph_record.graph_iri),
                ByteView::from(postcard::to_allocvec(&tombstone).expect("tombstone encodes")),
            ),
        )
        .await;
        let lifecycle = MetadataDocumentLifecycleRecord::Delete {
            event: aruna_core::metadata::MetadataDocumentDeleteRecord {
                event_id: Ulid::generate(),
                tombstone,
                deleted_after_event_id: stranger.last_event_id,
            },
        };
        write_entry(
            &test,
            (
                METADATA_DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
                metadata_document_lifecycle_key(document_record.document_id),
                ByteView::from(postcard::to_allocvec(&lifecycle).expect("lifecycle encodes")),
            ),
        )
        .await;

        assert!(
            filter_live_records(&test.context.storage_handle, &[graph_record])
                .await
                .is_err()
        );
        assert!(
            filter_live_records(&test.context.storage_handle, &[document_record])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn group_scan_capped() {
        // Without the visibility cache the listing scans storage directly, and
        // must refuse rather than hand back a truncated group.
        let test = metadata_test();
        let group_id = Ulid::generate();
        let first = public_record(group_id, Ulid::generate());
        let second = public_record(group_id, Ulid::generate());
        for record in [&first, &second] {
            for entry in aruna_core::storage_entries::metadata_registry_write_entries(record)
                .expect("registry entries encode")
            {
                write_entry(&test, entry).await;
            }
        }
        let context = DriverContext {
            storage_handle: test.context.storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let within = load_group_records(&context, group_id, 2)
            .await
            .expect("group scan succeeds");
        let over = load_group_records(&context, group_id, 1).await;

        assert_eq!(within.len(), 2);
        assert!(matches!(over, Err(MetadataApiError::ServiceUnavailable)));
    }

    #[tokio::test]
    async fn pending_scan_capped() {
        // The pending-projection sweep is bounded by the candidate budget so a
        // large backlog cannot be silently cut short.
        let test = metadata_test();
        let group_id = Ulid::generate();
        let first = public_record(group_id, Ulid::generate());
        let second = public_record(group_id, Ulid::generate());
        write_pending_marker(&test, &first).await;
        write_pending_marker(&test, &second).await;

        let within = load_pending_records(&test.context, Some(group_id), 2)
            .await
            .expect("pending scan succeeds");
        let over = load_pending_records(&test.context, Some(group_id), 1).await;

        assert_eq!(within.get(&group_id).map(Vec::len), Some(2));
        assert!(matches!(over, Err(MetadataApiError::ServiceUnavailable)));
    }

    fn raw_request(document_id: Ulid) -> ExportMetadataRoCrateRequest {
        ExportMetadataRoCrateRequest {
            document_id,
            auth: None,
            view: MetadataRoCrateExportView::Raw,
            limit: None,
            offset: None,
            after: None,
        }
    }

    async fn seed_raw_document(test: &MetadataTest, record: &MetadataRegistryRecord) {
        seed_policy_docs(test, record.group_id).await;
        for entry in aruna_core::storage_entries::metadata_registry_write_entries(record)
            .expect("registry entries encode")
        {
            write_entry(test, entry).await;
        }
        let event = MetadataCreateEventRecord {
            event_id: record.last_event_id,
            record: record.clone(),
            user_id: UserId::local(Ulid::generate(), TEST_REALM_ID),
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            payload: MetadataCreateEventPayload::RoCrate {
                jsonld: "{\"@context\":\"https://w3id.org/ro/crate/1.1/context\",\"@graph\":[]}"
                    .to_string(),
            },
            occurred_at_ms: 1,
        };
        write_entry(
            test,
            aruna_core::storage_entries::metadata_create_event_write_entry(&event)
                .expect("event entry encodes"),
        )
        .await;
    }

    #[tokio::test]
    async fn raw_export_fenced() {
        // The raw export answers from one read snapshot: a document tombstoned
        // after its registry row was written must no longer export.
        let test = metadata_test();
        let record = public_record(Ulid::generate(), Ulid::generate());
        seed_raw_document(&test, &record).await;

        let exported = export_metadata_rocrate(
            &test.context,
            TEST_REALM_ID,
            raw_request(record.document_id),
        )
        .await
        .expect("raw export succeeds");
        assert!(matches!(exported, ExportMetadataRoCrateResult::Raw { .. }));
        let foreign = export_metadata_rocrate(
            &test.context,
            RealmId::from_bytes([9u8; 32]),
            raw_request(record.document_id),
        )
        .await;
        assert!(matches!(foreign, Err(MetadataApiError::NotFound)));

        let tombstone = MetadataGraphLifecycleRecord::deleted(
            record.graph_iri.clone(),
            record.realm_id,
            record.group_id,
            record.document_id,
            2,
        );
        write_entry(
            &test,
            metadata_graph_lifecycle_write_entry(&tombstone).expect("tombstone encodes"),
        )
        .await;

        let fenced = export_metadata_rocrate(
            &test.context,
            TEST_REALM_ID,
            raw_request(record.document_id),
        )
        .await;

        assert!(matches!(fenced, Err(MetadataApiError::NotFound)));
    }

    #[test]
    fn anonymous_limit_clamped() {
        assert_eq!(
            effective_list_limit(None, true),
            DEFAULT_LIST_METADATA_LIMIT
        );
        assert_eq!(
            effective_list_limit(Some(MAX_LIST_METADATA_LIMIT), true),
            ANONYMOUS_LIST_METADATA_LIMIT
        );
        assert_eq!(
            effective_list_limit(Some(MAX_LIST_METADATA_LIMIT), false),
            MAX_LIST_METADATA_LIMIT
        );
        assert_eq!(
            effective_list_limit(Some(usize::MAX), false),
            MAX_LIST_METADATA_LIMIT
        );
        assert_eq!(effective_list_limit(Some(0), true), 1);
    }

    #[test]
    fn policy_scope_limit() {
        let within = (0..METADATA_REGISTRY_CANDIDATE_LIMIT)
            .map(|_| Ulid::generate())
            .collect();
        assert!(check_policy_limit(within).is_ok());

        let over = (0..=METADATA_REGISTRY_CANDIDATE_LIMIT)
            .map(|_| Ulid::generate())
            .collect();
        assert!(matches!(
            check_policy_limit(over),
            Err(MetadataApiError::ServiceUnavailable)
        ));
    }

    #[test]
    fn config_filters_cached() {
        // A cached candidate that realm config no longer lists must disappear
        // from fan-out immediately, without waiting for the snapshot to expire.
        let kept = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
        let removed = iroh::SecretKey::from_bytes(&[12u8; 32]).public();
        let mut config = RealmConfigDocument::new(TEST_REALM_ID, Vec::new(), 2);
        config.ensure_node(kept, RealmNodeKind::Server);

        let authorized = authorized_realm_nodes(&config, HashSet::from([kept, removed]))
            .expect("node ids parse");

        assert_eq!(authorized, HashSet::from([kept]));
    }

    #[test]
    fn peers_are_bounded() {
        let local = iroh::SecretKey::from_bytes(&[255u8; 32]).public();
        let mut config = RealmConfigDocument::new(TEST_REALM_ID, Vec::new(), 2);
        config.ensure_node(local, aruna_core::structs::RealmNodeKind::Server);
        for seed in 1u8..=40 {
            config.ensure_node(
                iroh::SecretKey::from_bytes(&[seed; 32]).public(),
                aruna_core::structs::RealmNodeKind::Server,
            );
        }
        config.nodes.push(aruna_core::structs::RealmNode {
            node_id: "invalid-node".to_string(),
            kind: aruna_core::structs::RealmNodeKind::Server,
        });
        let mut reversed = config.clone();
        reversed.nodes.reverse();
        let first = select_forward_peers(
            &config,
            TEST_REALM_ID,
            Ulid::from_parts(0, 1),
            "datasets/lookup",
            local,
        )
        .expect("peer selection succeeds");
        let second = select_forward_peers(
            &reversed,
            TEST_REALM_ID,
            Ulid::from_parts(0, 1),
            "datasets/lookup",
            local,
        )
        .expect("peer selection succeeds");
        assert_eq!(first.len(), METADATA_DISTRIBUTED_QUERY_MAX_NODES);
        assert_eq!(first, second);
    }

    #[test]
    fn fanout_nodes_bounded() {
        let local = iroh::SecretKey::from_bytes(&[255u8; 32]).public();
        let mut nodes = (1u8..=64)
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
            .collect::<Vec<_>>();
        nodes.push(local);
        let mut reversed = nodes.clone();
        reversed.reverse();
        let first = select_fanout_nodes(&nodes, local, b"metadata-query");
        let second = select_fanout_nodes(&reversed, local, b"metadata-query");

        assert_eq!(first, second);
        assert_eq!(first.len(), METADATA_DISTRIBUTED_QUERY_MAX_NODES);
        assert!(first.contains(&local));
        assert_eq!(first.iter().collect::<HashSet<_>>().len(), first.len());
    }

    fn path_config(
        nodes: u8,
        shard_count: u32,
        replica_count: Option<u32>,
    ) -> (RealmConfigDocument, Ulid) {
        let mut config = RealmConfigDocument::new(TEST_REALM_ID, Vec::new(), 3);
        let strategy = aruna_core::structs::PlacementStrategy {
            strategy_id: Ulid::from_bytes([5u8; 16]),
            name: "metadata-registry".to_string(),
            replica_count,
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy.clone()];
        for seed in 1..=nodes {
            config.ensure_node(
                iroh::SecretKey::from_bytes(&[seed; 32]).public(),
                RealmNodeKind::Server,
            );
        }
        (config, strategy.strategy_id)
    }

    fn holder_deadline() -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_secs(30)
    }

    #[tokio::test]
    async fn shard_counts_match() {
        // The reported replica count per shard is what the merge waits for, so
        // it must equal the holders the selection actually dispatches to.
        let (config, strategy_id) = path_config(6, 8, Some(2));
        let local = iroh::SecretKey::from_bytes(&[1u8; 32]).public();

        let (selections, replica_counts) = select_path_holders(
            &config,
            TEST_REALM_ID,
            Ulid::from_parts(0, 1),
            "datasets/lookup",
            strategy_id,
            8,
            Some(2),
            local,
            holder_deadline(),
        )
        .expect("holder selection succeeds");

        assert_eq!(replica_counts.len(), 8);
        assert!(selections.len() <= METADATA_DISTRIBUTED_QUERY_MAX_NODES);
        for (shard, expected) in replica_counts.iter().copied().enumerate() {
            let dispatched = selections
                .iter()
                .filter(|selection| selection.shards.contains(&(shard as u32)))
                .count();
            assert_eq!(expected, dispatched);
            assert!(expected > 0);
        }
    }

    #[tokio::test]
    async fn everywhere_covers_shards() {
        // An everywhere strategy places every holder on every shard, so each
        // selection must answer for all of them.
        let (config, strategy_id) = path_config(4, 8, None);
        let local = iroh::SecretKey::from_bytes(&[1u8; 32]).public();

        let (selections, replica_counts) = select_path_holders(
            &config,
            TEST_REALM_ID,
            Ulid::from_parts(0, 1),
            "datasets/lookup",
            strategy_id,
            8,
            None,
            local,
            holder_deadline(),
        )
        .expect("holder selection succeeds");

        assert_eq!(selections.len(), 4);
        assert_eq!(replica_counts, vec![4; 8]);
        for selection in &selections {
            assert_eq!(selection.shards, (0..8).collect::<Vec<_>>());
        }
    }

    #[tokio::test]
    async fn capped_shard_rejected() {
        // More shard holders than the fan-out cap leaves shards with nobody to
        // ask; the lookup must fail instead of resolving from the rest.
        let (config, strategy_id) = path_config(200, 64, Some(1));
        let local = iroh::SecretKey::from_bytes(&[1u8; 32]).public();

        let result = select_path_holders(
            &config,
            TEST_REALM_ID,
            Ulid::from_parts(0, 1),
            "datasets/lookup",
            strategy_id,
            64,
            Some(1),
            local,
            holder_deadline(),
        );

        assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
    }

    #[test]
    fn divergent_paths_fail() {
        // Two peers that resolved the same path differently must not be
        // reduced to one of the two answers.
        let winner = sanitize_path_winner(public_record(Ulid::generate(), Ulid::generate()))
            .expect("valid path winner");
        let result = MetadataPathLookupResult {
            winner,
            conflicts: Vec::new(),
        };

        let resolved = reduce_path_response(Some(result.clone()), None, false, false, false)
            .expect("a single agreeing answer resolves");
        assert_eq!(resolved.winner, result.winner);
        assert!(matches!(
            reduce_path_response(Some(result), None, true, false, false),
            Err(MetadataApiError::ServiceUnavailable)
        ));
        assert!(matches!(
            reduce_path_response(None, None, false, true, false),
            Err(MetadataApiError::NotFound)
        ));
        assert!(matches!(
            reduce_path_response(None, None, false, false, false),
            Err(MetadataApiError::ServiceUnavailable)
        ));
    }

    #[test]
    fn path_denial_wins() {
        let record = public_record(Ulid::generate(), Ulid::generate());
        let result = MetadataPathLookupResult {
            winner: sanitize_path_winner(record).expect("valid path winner"),
            conflicts: Vec::new(),
        };

        assert!(matches!(
            reduce_path_response(
                Some(result.clone()),
                Some(MetadataReadError::Forbidden),
                false,
                false,
                false,
            ),
            Err(MetadataApiError::Forbidden)
        ));
        assert!(matches!(
            reduce_path_response(Some(result.clone()), None, false, true, false),
            Err(MetadataApiError::ServiceUnavailable)
        ));

        assert!(matches!(
            reduce_path_response(Some(result), None, false, false, true,),
            Err(MetadataApiError::ServiceUnavailable)
        ));
    }

    #[test]
    fn metadata_read_operation() {
        let request = metadata_read_request("/realm/g/group/meta/document", None);
        assert_eq!(request.operation, "metadata.read");
    }

    #[test]
    fn raw_identity_fence() {
        let record = public_record(Ulid::generate(), Ulid::generate());
        assert!(raw_identity_matches(
            &record,
            TEST_REALM_ID,
            record.document_id
        ));
        assert!(!raw_identity_matches(
            &record,
            RealmId::from_bytes([8; 32]),
            record.document_id
        ));
        assert!(!raw_identity_matches(
            &record,
            TEST_REALM_ID,
            Ulid::generate()
        ));
    }

    // The record lives only in the registry cache and the graph was never
    // projected, so a returned summary can only come from the summary cache.
    #[tokio::test]
    async fn summary_from_cache() {
        let test = metadata_test();
        let record = public_record(Ulid::generate(), Ulid::generate());
        seed_registry_cache(&test, &record).await;
        summary_cache().insert(
            &record.graph_iri,
            record.last_event_id,
            "{\"cached\":true}",
            Instant::now(),
        );

        let result = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(record.group_id, true),
        )
        .await
        .expect("summary listing succeeds");

        assert_eq!(result.documents.len(), 1);
        assert_eq!(
            result.documents[0].rocrate_summary_jsonld.as_deref(),
            Some("{\"cached\":true}")
        );
    }

    #[tokio::test]
    async fn stale_summary_refused() {
        // A cursor advance must fall through to the handle, not serve the entry.
        let test = metadata_test();
        let record = public_record(Ulid::generate(), Ulid::generate());
        seed_registry_cache(&test, &record).await;
        summary_cache().insert(
            &record.graph_iri,
            Ulid::generate(),
            "{\"stale\":true}",
            Instant::now(),
        );

        let result = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(record.group_id, true),
        )
        .await
        .expect("summary listing succeeds");

        assert_eq!(result.documents.len(), 1);
        assert!(result.documents[0].rocrate_summary_jsonld.is_none());
    }

    #[tokio::test]
    async fn pending_summary_listed() {
        let test = metadata_test();
        let record = public_record(Ulid::generate(), Ulid::generate());
        write_pending_marker(&test, &record).await;

        let result = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(record.group_id, true),
        )
        .await
        .expect("summary listing succeeds");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].record.document_id, record.document_id);
        assert!(result.documents[0].rocrate_summary_jsonld.is_none());

        let plain = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(record.group_id, false),
        )
        .await
        .expect("plain listing succeeds");
        assert!(plain.documents.is_empty());
    }

    // The page window must not truncate the estimate, and paging must not move it.
    #[tokio::test]
    async fn estimate_beyond_page() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let seeded = METADATA_ESTIMATE_MIN_LIMIT + 2;
        for _ in 0..seeded {
            seed_registry_cache(&test, &public_record(group_id, Ulid::generate())).await;
        }

        let page = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                limit: Some(METADATA_ESTIMATE_MIN_LIMIT),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(page.documents.len(), METADATA_ESTIMATE_MIN_LIMIT);
        assert_eq!(page.total_returned, METADATA_ESTIMATE_MIN_LIMIT);
        assert_eq!(page.total_estimate, Some(seeded));

        let tail = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                limit: Some(METADATA_ESTIMATE_MIN_LIMIT),
                offset: Some(seeded - 1),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(tail.documents.len(), 1);
        assert_eq!(tail.total_estimate, Some(seeded));
    }

    // A targeted lookup must not pay for the realm-wide estimate scan, and
    // must report the estimate as absent rather than as a truncated count.
    #[tokio::test]
    async fn lookup_omits_estimate() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        for _ in 0..3 {
            seed_registry_cache(&test, &public_record(group_id, Ulid::generate())).await;
        }

        let lookup = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                limit: Some(METADATA_ESTIMATE_MIN_LIMIT - 1),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(lookup.total_returned, 3);
        assert_eq!(lookup.total_estimate, None);

        let browse = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                limit: Some(METADATA_ESTIMATE_MIN_LIMIT),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(browse.total_estimate, Some(3));
    }

    // Anonymous callers collect no rules, so only public records count.
    #[tokio::test]
    async fn estimate_skips_private() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let readable = public_record(group_id, Ulid::generate());
        seed_registry_cache(&test, &readable).await;
        let mut private = public_record(group_id, Ulid::generate());
        private.public = false;
        seed_registry_cache(&test, &private).await;

        let result = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(group_id, false),
        )
        .await
        .expect("listing succeeds");

        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].record.document_id, readable.document_id);
        assert_eq!(result.total_estimate, Some(1));
    }

    #[tokio::test]
    async fn cross_shard_unknown() {
        // Local listings cannot resolve claims that may live on another registry shard.
        let test = metadata_test();
        let group_id = Ulid::generate();
        let mut config = RealmConfigDocument::new(TEST_REALM_ID, Vec::new(), 3);
        config.seed_default_placement();
        let mut first = public_record(group_id, Ulid::generate());
        let first_shard = registry_placement(&config, &first).shard;
        let mut second = loop {
            let candidate = public_record(group_id, Ulid::generate());
            if registry_placement(&config, &candidate).shard != first_shard {
                break candidate;
            }
        };
        second.document_path = first.document_path.clone();
        second.permission_path = MetadataRegistryRecord::permission_path_for(
            &TEST_REALM_ID,
            group_id,
            &second.document_path,
            second.document_id,
        );
        let claims = [&first, &second]
            .into_iter()
            .map(|record| PathClaimRecord {
                document_id: MetaResourceId::from_bytes(record.document_id.to_bytes()).unwrap(),
                establishing_event_id: record.establishing_event_id,
                requested_path: record.document_path.clone(),
            })
            .collect::<Vec<_>>();
        let resolution = aruna_core::structs::resolve_path_claim(&claims).unwrap();
        let winner_id = resolution.winner.document_id.as_ulid();
        let loser_id = resolution.conflicts[0].document_id.as_ulid();
        first.public = first.document_id == loser_id;
        second.public = second.document_id == loser_id;
        seed_registry_cache(&test, &first).await;
        seed_registry_cache(&test, &second).await;

        let listed = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(group_id, false),
        )
        .await
        .unwrap();
        assert_eq!(listed.documents.len(), 1);
        assert_eq!(listed.documents[0].record.document_id, loser_id);
        assert_ne!(winner_id, loser_id);
    }

    #[test]
    fn user_result_opaque() {
        // An unreadable winner still participates and cannot promote a readable loser.
        let group_id = Ulid::generate();
        let first = public_record(group_id, Ulid::generate());
        let mut second = public_record(group_id, Ulid::generate());
        second.document_path = first.document_path.clone();
        let mut candidates = [&first, &second]
            .into_iter()
            .map(|record| MetadataPathCandidate {
                claim: PathClaimRecord {
                    document_id: MetaResourceId::from_bytes(record.document_id.to_bytes()).unwrap(),
                    establishing_event_id: record.establishing_event_id,
                    requested_path: record.document_path.clone(),
                },
                record: Some(record.clone()),
            })
            .collect::<Vec<_>>();
        let claims = candidates
            .iter()
            .map(|candidate| candidate.claim.clone())
            .collect::<Vec<_>>();
        let winner = aruna_core::structs::resolve_path_claim(&claims)
            .unwrap()
            .winner;
        let hidden_id = winner.document_id.to_bytes();
        candidates
            .iter_mut()
            .find(|candidate| candidate.claim == winner)
            .unwrap()
            .record = None;

        let result = reduce_path_candidates(candidates);
        assert!(matches!(result, Err(MetadataApiError::NotFound)));
        let response = MetadataTransportMessage::ForwardedPathResolution {
            result: Err(MetadataReadError::NotFound),
        };
        let encoded = postcard::to_allocvec(&response).unwrap();
        assert!(
            !encoded
                .windows(hidden_id.len())
                .any(|window| window == hidden_id)
        );
    }

    fn path_candidate(record: &MetadataRegistryRecord) -> MetadataPathCandidate {
        MetadataPathCandidate {
            claim: PathClaimRecord {
                document_id: MetaResourceId::from_bytes(record.document_id.to_bytes()).unwrap(),
                establishing_event_id: record.establishing_event_id,
                requested_path: record.document_path.clone(),
            },
            record: Some(record.clone()),
        }
    }

    #[test]
    fn missing_replica_fails() {
        let record = public_record(Ulid::generate(), Ulid::generate());
        let views = vec![PathShardView {
            shard: 0,
            candidates: vec![path_candidate(&record)],
        }];

        assert!(matches!(
            merge_path_views(&[2], views),
            Err(MetadataApiError::ServiceUnavailable)
        ));
    }

    #[test]
    fn stale_replica_fails() {
        let record = public_record(Ulid::generate(), Ulid::generate());
        let views = vec![
            PathShardView {
                shard: 0,
                candidates: vec![path_candidate(&record)],
            },
            PathShardView {
                shard: 0,
                candidates: Vec::new(),
            },
        ];

        assert!(matches!(
            merge_path_views(&[2], views),
            Err(MetadataApiError::ServiceUnavailable)
        ));
    }

    #[test]
    fn divergent_claims_fail() {
        let record = public_record(Ulid::generate(), Ulid::generate());
        let mut divergent = record.clone();
        divergent.establishing_event_id = Ulid::generate();
        let views = vec![
            PathShardView {
                shard: 0,
                candidates: vec![path_candidate(&record)],
            },
            PathShardView {
                shard: 0,
                candidates: vec![path_candidate(&divergent)],
            },
        ];

        assert!(matches!(
            merge_path_views(&[2], views),
            Err(MetadataApiError::ServiceUnavailable)
        ));
    }

    #[test]
    fn divergent_evidence_fails() {
        let record = public_record(Ulid::generate(), Ulid::generate());
        let mut divergent = record.clone();
        divergent.public = false;
        let views = vec![
            PathShardView {
                shard: 0,
                candidates: vec![path_candidate(&record)],
            },
            PathShardView {
                shard: 0,
                candidates: vec![path_candidate(&divergent)],
            },
        ];

        assert!(matches!(
            merge_path_views(&[2], views),
            Err(MetadataApiError::ServiceUnavailable)
        ));
    }

    #[test]
    fn invalid_resolution_fails() {
        let record = public_record(Ulid::generate(), Ulid::generate());
        let mut unrelated = MetadataPathResolution {
            winner: sanitize_path_winner(record.clone()).unwrap(),
            conflicts: Vec::new(),
        };
        unrelated.winner.group_id = Ulid::generate();
        assert!(matches!(
            validate_path_resolution(
                TEST_REALM_ID,
                record.group_id,
                &record.document_path,
                &unrelated,
            ),
            Err(MetadataApiError::ServiceUnavailable)
        ));

        let duplicate = MetadataPathResolution {
            winner: sanitize_path_winner(record.clone()).unwrap(),
            conflicts: vec![record.document_id],
        };
        assert!(matches!(
            validate_path_resolution(
                TEST_REALM_ID,
                record.group_id,
                &record.document_path,
                &duplicate,
            ),
            Err(MetadataApiError::ServiceUnavailable)
        ));
    }

    #[test]
    fn winner_wire_sanitized() {
        let mut record = public_record(Ulid::generate(), Ulid::generate());
        let holder = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
        record.holder_node_ids = vec![holder];
        let permission_path = record.permission_path.clone();
        let winner = sanitize_path_winner(record).unwrap();
        let encoded = postcard::to_allocvec(&winner).unwrap();

        assert!(
            !encoded
                .windows(holder.as_bytes().len())
                .any(|window| window == holder.as_bytes())
        );
        assert!(
            !encoded
                .windows(permission_path.len())
                .any(|window| window == permission_path.as_bytes())
        );
    }

    fn auth_for(user_id: UserId) -> AuthContext {
        AuthContext {
            user_id,
            realm_id: TEST_REALM_ID,
            path_restrictions: None,
            session: None,
        }
    }

    fn user_role(
        user_id: UserId,
        permissions: HashMap<String, Permission>,
    ) -> HashMap<RoleId, Role> {
        let role_id = Ulid::generate();
        HashMap::from([(
            role_id,
            Role {
                role_id,
                name: "listing".to_string(),
                permissions,
                assigned_users: HashSet::from([user_id]),
            },
        )])
    }

    // The rules collection reads both documents; without them a group yields no
    // rules and every non-public record in it stays hidden.
    async fn write_auth_docs(test: &MetadataTest, group_id: GroupId, roles: HashMap<RoleId, Role>) {
        write_policy_docs(test, group_id, roles, Vec::new()).await;
    }

    async fn write_policy_docs(
        test: &MetadataTest,
        group_id: GroupId,
        roles: HashMap<RoleId, Role>,
        policies: Vec<aruna_core::request_policy::RequestPolicy>,
    ) {
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            user_id: UserId::local(Ulid::generate(), TEST_REALM_ID),
            realm_id: TEST_REALM_ID,
        };
        let realm_doc = RealmAuthorizationDocument::new_default_realm_doc(TEST_REALM_ID);
        let group = Group {
            display_name: "Test".to_string(),
            group_id,
            realm_id: TEST_REALM_ID,
            owner: actor.user_id,
            roles: roles.keys().copied().collect(),
        };
        let group_doc = GroupAuthorizationDocument {
            group_id,
            roles,
            policies,
        };
        // The policy evaluator reads the group through GetGroupOperation, which
        // needs the group record as well as the auth doc, and fails closed
        // without the realm config.
        let entries = [
            (
                aruna_core::keyspaces::REALM_CONFIG_KEYSPACE,
                Key::from(*TEST_REALM_ID.as_bytes()),
                RealmConfigDocument::default_for_realm(TEST_REALM_ID, Vec::new())
                    .to_bytes(&actor)
                    .expect("realm config encodes"),
            ),
            (
                AUTH_KEYSPACE,
                Key::from(*TEST_REALM_ID.as_bytes()),
                realm_doc.to_bytes(&actor).expect("realm doc encodes"),
            ),
            (
                AUTH_KEYSPACE,
                Key::from(group_id.to_bytes()),
                group_doc.to_bytes(&actor).expect("group doc encodes"),
            ),
            (
                GROUP_KEYSPACE,
                Key::from(group_id.to_bytes()),
                group.to_bytes(&actor).expect("group encodes"),
            ),
        ];
        for (key_space, key, value) in entries {
            match test
                .context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key,
                    value: value.into(),
                    txn_id: None,
                })
                .await
            {
                Event::Storage(StorageEvent::WriteResult { .. }) => {}
                other => panic!("unexpected write event: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn hidden_ids_match() {
        // Missing, private-denied, and policy-denied public ids must all return
        // NotFound so read-by-id cannot probe existence.
        let test = metadata_test();
        let group_id = Ulid::generate();
        let stranger = UserId::local(Ulid::generate(), TEST_REALM_ID);
        write_policy_docs(
            &test,
            group_id,
            user_role(
                UserId::local(Ulid::generate(), TEST_REALM_ID),
                HashMap::from([(
                    format!("/{TEST_REALM_ID}/g/{group_id}/**"),
                    Permission::WRITE,
                )]),
            ),
            vec![aruna_core::request_policy::RequestPolicy {
                policy_id: Ulid::generate(),
                name: "no-reads".to_string(),
                kind: aruna_core::request_policy::PolicyKind::Deny,
                when: None,
                expression: "permission == 'read'".to_string(),
                enabled: true,
            }],
        )
        .await;

        let public = public_record(group_id, Ulid::generate());
        seed_registry_cache(&test, &public).await;
        let mut private = public_record(group_id, Ulid::generate());
        private.public = false;
        seed_registry_cache(&test, &private).await;
        let missing = public_record(group_id, Ulid::generate());

        for document_id in [public.document_id, private.document_id, missing.document_id] {
            let result = get_visible_metadata_document(
                &test.context,
                TEST_REALM_ID,
                GetVisibleMetadataDocumentRequest {
                    document_id,
                    auth: Some(auth_for(stranger)),
                },
            )
            .await;
            assert!(matches!(result, Err(MetadataApiError::NotFound)));
        }
    }

    #[tokio::test]
    async fn policy_hides_record() {
        // A group deny policy removes one public record from the bulk listing
        // while leaving an allowed public record visible.
        let test = metadata_test();
        let group_id = Ulid::generate();
        let stranger = UserId::local(Ulid::generate(), TEST_REALM_ID);
        let hidden = public_record(group_id, Ulid::generate());
        let visible = public_record(group_id, Ulid::generate());
        write_policy_docs(
            &test,
            group_id,
            HashMap::new(),
            vec![aruna_core::request_policy::RequestPolicy {
                policy_id: Ulid::generate(),
                name: "hide-one".to_string(),
                kind: aruna_core::request_policy::PolicyKind::Deny,
                when: None,
                expression: format!("path.contains('{}')", hidden.document_id),
                enabled: true,
            }],
        )
        .await;
        seed_registry_cache(&test, &hidden).await;
        seed_registry_cache(&test, &visible).await;

        let page = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                auth: Some(auth_for(stranger)),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(listed_ids(&page), vec![visible.document_id]);
        assert_eq!(page.total_estimate, Some(1));
    }

    // A caller who holds no role in the group sees the public records only, and
    // the counts describe the visible set rather than the scanned one.
    #[tokio::test]
    async fn stranger_sees_public() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let stranger = UserId::local(Ulid::generate(), TEST_REALM_ID);
        write_auth_docs(
            &test,
            group_id,
            user_role(
                UserId::local(Ulid::generate(), TEST_REALM_ID),
                HashMap::from([(
                    format!("/{TEST_REALM_ID}/g/{group_id}/**"),
                    Permission::WRITE,
                )]),
            ),
        )
        .await;
        let visible = public_record(group_id, Ulid::generate());
        seed_registry_cache(&test, &visible).await;
        for _ in 0..2 {
            let mut hidden = public_record(group_id, Ulid::generate());
            hidden.public = false;
            seed_registry_cache(&test, &hidden).await;
        }

        let page = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                auth: Some(auth_for(stranger)),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(listed_ids(&page), vec![visible.document_id]);
        assert_eq!(page.total_returned, 1);
        assert_eq!(page.total_estimate, Some(1));

        let beyond = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                offset: Some(1),
                auth: Some(auth_for(stranger)),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert!(beyond.documents.is_empty());
        assert_eq!(beyond.total_returned, 0);
        assert_eq!(beyond.total_estimate, Some(1));
    }

    // An unauthenticated caller must not inherit a member's grants.
    #[tokio::test]
    async fn anonymous_sees_public() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let member = UserId::local(Ulid::generate(), TEST_REALM_ID);
        write_auth_docs(
            &test,
            group_id,
            user_role(
                member,
                HashMap::from([(
                    format!("/{TEST_REALM_ID}/g/{group_id}/meta/**"),
                    Permission::READ,
                )]),
            ),
        )
        .await;
        let visible = public_record(group_id, Ulid::generate());
        seed_registry_cache(&test, &visible).await;
        let mut hidden = public_record(group_id, Ulid::generate());
        hidden.public = false;
        seed_registry_cache(&test, &hidden).await;

        let anonymous = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(group_id, false),
        )
        .await
        .expect("listing succeeds");
        assert_eq!(listed_ids(&anonymous), vec![visible.document_id]);
        assert_eq!(anonymous.total_estimate, Some(1));

        let signed = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                auth: Some(auth_for(member)),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(signed.total_returned, 2);
        assert_eq!(signed.total_estimate, Some(2));
    }

    #[tokio::test]
    async fn foreign_policy_identity() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let foreign_realm = RealmId([8u8; 32]);
        let foreign_user = UserId::local(Ulid::generate(), foreign_realm);
        let record = public_record(group_id, Ulid::generate());
        write_policy_docs(
            &test,
            group_id,
            HashMap::new(),
            vec![aruna_core::request_policy::RequestPolicy {
                policy_id: Ulid::generate(),
                name: "foreign-user".to_string(),
                kind: aruna_core::request_policy::PolicyKind::Require,
                when: None,
                expression: format!("user == '{foreign_user}'"),
                enabled: true,
            }],
        )
        .await;
        seed_registry_cache(&test, &record).await;
        let auth = AuthContext {
            user_id: foreign_user,
            realm_id: foreign_realm,
            path_restrictions: None,
            session: None,
        };

        let listed = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                auth: Some(auth.clone()),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(listed_ids(&listed), vec![record.document_id]);

        let candidates = local_path_candidates(
            &test.context,
            TEST_REALM_ID,
            group_id,
            &record.document_path,
            Some(&auth),
        )
        .await
        .expect("path lookup succeeds");
        assert_eq!(candidates.len(), 1);
        assert!(candidates[0].record.is_some());
    }

    // A per-document DENY inside a group-wide grant: the estimate must decide
    // each document, not reuse one representative answer for the whole group.
    #[tokio::test]
    async fn estimate_counts_exact() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let member = UserId::local(Ulid::generate(), TEST_REALM_ID);
        let mut allowed = public_record(group_id, Ulid::generate());
        allowed.public = false;
        let mut denied = public_record(group_id, Ulid::generate());
        denied.public = false;
        write_auth_docs(
            &test,
            group_id,
            user_role(
                member,
                HashMap::from([
                    (
                        format!("/{TEST_REALM_ID}/g/{group_id}/meta/**"),
                        Permission::READ,
                    ),
                    (denied.permission_path.clone(), Permission::DENY),
                ]),
            ),
        )
        .await;
        seed_registry_cache(&test, &allowed).await;
        seed_registry_cache(&test, &denied).await;

        let page = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                auth: Some(auth_for(member)),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(listed_ids(&page), vec![allowed.document_id]);
        assert_eq!(page.total_estimate, Some(1));

        // A targeted lookup still reports no estimate for the same caller.
        let lookup = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                limit: Some(METADATA_ESTIMATE_MIN_LIMIT - 1),
                auth: Some(auth_for(member)),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(lookup.total_returned, 1);
        assert_eq!(lookup.total_estimate, None);
    }

    // path_prefix must scope the estimate to the same set the page came from.
    #[tokio::test]
    async fn estimate_honours_prefix() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        for _ in 0..2 {
            seed_registry_cache(&test, &public_record(group_id, Ulid::generate())).await;
        }
        let mut other = public_record(group_id, Ulid::generate());
        other.document_path = "other/excluded".to_string();
        seed_registry_cache(&test, &other).await;

        let result = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                path_prefix: Some("datasets".to_string()),
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");

        assert_eq!(result.total_returned, 2);
        assert_eq!(result.total_estimate, Some(2));
    }

    // Update stamps deliberately disagree with the ascending document ids.
    async fn seed_timed_records(
        test: &MetadataTest,
        group_id: GroupId,
    ) -> Vec<MetadataRegistryRecord> {
        let mut records = Vec::new();
        for updated_at_ms in [10u64, 30, 20] {
            let mut record = public_record(group_id, Ulid::generate());
            record.updated_at_ms = updated_at_ms;
            seed_registry_cache(test, &record).await;
            records.push(record);
        }
        records
    }

    fn listed_ids(result: &ListVisibleMetadataDocumentsResult) -> Vec<Ulid> {
        result
            .documents
            .iter()
            .map(|document| document.record.document_id)
            .collect()
    }

    // Recency ordering must precede the offset window so pages walk it too.
    #[tokio::test]
    async fn orders_recent_first() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let records = seed_timed_records(&test, group_id).await;

        let page = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                order: MetadataListOrder::Recent,
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(
            listed_ids(&page),
            vec![
                records[1].document_id,
                records[2].document_id,
                records[0].document_id
            ]
        );

        let second = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            ListVisibleMetadataDocumentsRequest {
                limit: Some(1),
                offset: Some(1),
                order: MetadataListOrder::Recent,
                ..summary_request(group_id, false)
            },
        )
        .await
        .expect("listing succeeds");
        assert_eq!(listed_ids(&second), vec![records[2].document_id]);
    }

    // The default page stays in ascending document id order.
    #[tokio::test]
    async fn default_keeps_created() {
        let test = metadata_test();
        let group_id = Ulid::generate();
        let records = seed_timed_records(&test, group_id).await;

        let page = list_visible_metadata_documents(
            &test.context,
            TEST_REALM_ID,
            summary_request(group_id, false),
        )
        .await
        .expect("listing succeeds");

        let mut expected = records
            .iter()
            .map(|record| record.document_id)
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(listed_ids(&page), expected);
    }

    #[test]
    fn deduplicates_select_rows_from_multiple_nodes() {
        let results = aggregate_query_results(
            vec![
                MetadataQueryResults::Solutions(vec![
                    BTreeMap::from([(String::from("s"), String::from("<urn:a>"))]),
                    BTreeMap::from([(String::from("s"), String::from("<urn:b>"))]),
                ]),
                MetadataQueryResults::Solutions(vec![BTreeMap::from([(
                    String::from("s"),
                    String::from("<urn:a>"),
                )])]),
            ],
            MetadataQueryForm::Select,
            None,
        )
        .unwrap();

        let MetadataQueryResults::Solutions(rows) = results else {
            panic!("expected solutions");
        };
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn reapplies_select_limit_after_distributed_merge() {
        let results = aggregate_query_results(
            vec![
                MetadataQueryResults::Solutions(vec![
                    BTreeMap::from([(String::from("s"), String::from("<urn:a>"))]),
                    BTreeMap::from([(String::from("s"), String::from("<urn:b>"))]),
                ]),
                MetadataQueryResults::Solutions(vec![
                    BTreeMap::from([(String::from("s"), String::from("<urn:c>"))]),
                    BTreeMap::from([(String::from("s"), String::from("<urn:d>"))]),
                ]),
            ],
            MetadataQueryForm::Select,
            Some(3),
        )
        .unwrap();

        let MetadataQueryResults::Solutions(rows) = results else {
            panic!("expected solutions");
        };
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn query_select_limit_reads_outermost_limit_only() {
        assert_eq!(
            query_select_limit("SELECT ?s WHERE { ?s ?p ?o } LIMIT 5"),
            Some(5)
        );
        assert_eq!(
            query_select_limit("SELECT ?s WHERE { ?s ?p ?o } LIMIT 7 OFFSET 3"),
            Some(7)
        );
        assert_eq!(query_select_limit("SELECT ?s WHERE { ?s ?p ?o }"), None);
        assert_eq!(
            query_select_limit(
                "SELECT ?s WHERE { { SELECT ?s WHERE { ?s ?p ?o } LIMIT 5 } ?s ?p ?o }"
            ),
            None
        );
        assert_eq!(query_select_limit("ASK WHERE { ?s ?p ?o }"), None);
        assert_eq!(query_select_limit("not sparql"), None);
    }

    #[test]
    fn query_form_accepts_single_line_declarations() {
        assert_eq!(
            query_form("PREFIX ex: <https://example.org/> SELECT ?s WHERE { ?s ?p ?o }").unwrap(),
            MetadataQueryForm::Select
        );
        assert_eq!(
            query_form("BASE <https://example.org/> ASK WHERE { ?s ?p ?o }").unwrap(),
            MetadataQueryForm::Ask
        );
        assert_eq!(query_form("CONSTRUCT WHERE { ?s ?p ?o }"), None);
    }

    #[test]
    fn query_validation_rejects_updates_and_service() {
        assert!(ensure_supported_query_form("SELECT ?s WHERE { ?s ?p ?o }").is_ok());
        assert!(ensure_supported_query_form("ASK WHERE { ?s ?p ?o }").is_ok());
        assert!(ensure_supported_query_form("INSERT DATA { <urn:s> <urn:p> <urn:o> }").is_err());
        assert!(
            ensure_supported_query_form(
                "SELECT ?s WHERE { SERVICE <https://example.org/sparql> { ?s ?p ?o } }"
            )
            .is_err()
        );
        assert!(
            ensure_supported_query_form(
                "ASK WHERE { FILTER EXISTS { SERVICE SILENT ?endpoint { ?s ?p ?o } } }"
            )
            .is_err()
        );
    }

    #[test]
    fn distributed_query_validation_accepts_only_union_safe_forms() {
        assert!(distributed_query_is_union_safe("ASK WHERE { ?s ?p ?o }"));
        assert!(!distributed_query_is_union_safe(
            "ASK WHERE { ?s ?p ?o . ?s ?p2 ?o2 }"
        ));
        assert!(distributed_query_is_union_safe(
            "SELECT DISTINCT ?s WHERE { ?s ?p ?o } LIMIT 10"
        ));
        assert!(!distributed_query_is_union_safe(
            "SELECT ?s WHERE { ?s ?p ?o }"
        ));
        assert!(!distributed_query_is_union_safe(
            "SELECT DISTINCT ?s WHERE { ?s ?p ?o . ?s ?p2 ?o2 }"
        ));
        assert!(!distributed_query_is_union_safe(
            "SELECT DISTINCT ?s WHERE { ?s ?p ?o } OFFSET 1"
        ));
        assert!(!distributed_query_is_union_safe(
            "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }"
        ));
    }

    #[test]
    fn query_validation_enforces_byte_and_row_bounds() {
        assert!(ensure_supported_query_form(&" ".repeat(METADATA_QUERY_MAX_BYTES + 1)).is_err());
        assert!(
            ensure_supported_query_form(&format!(
                "SELECT ?s WHERE {{ ?s ?p ?o }} LIMIT {}",
                METADATA_QUERY_MAX_ROWS + 1
            ))
            .is_err()
        );
    }

    // Fan-out follows the live holders of the stored bucket; the event-time
    // holder stamp on the record is ignored, and no config means local only.
    #[test]
    fn query_fans_out_to_holders() {
        let local_node_id = iroh::SecretKey::from_bytes(&[21u8; 32]).public();
        let remote_node_id = iroh::SecretKey::from_bytes(&[22u8; 32]).public();
        let stale_node_id = iroh::SecretKey::from_bytes(&[23u8; 32]).public();
        let realm_id = RealmId([3u8; 32]);
        let document_id = Ulid::generate();
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
        config.seed_default_placement();
        config.ensure_node(local_node_id, aruna_core::structs::RealmNodeKind::Server);
        config.ensure_node(remote_node_id, aruna_core::structs::RealmNodeKind::Server);
        let strategy = config
            .strategy(&config.default_strategy_id.expect("default strategy"))
            .expect("default strategy resolves");
        let placement = crate::placement::choose_origin_bucket(
            &config,
            strategy,
            local_node_id,
            &document_id.to_bytes(),
        )
        .expect("origin holds a bucket");

        let record = MetadataRegistryRecord {
            realm_id,
            group_id: Ulid::generate(),
            document_id,
            document_path: "datasets/query-targets".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: "/metadata/query-targets".to_string(),
            placement,
            holder_node_ids: vec![stale_node_id],
            created_at_ms: 0,
            updated_at_ms: 0,
            establishing_event_id: Ulid::nil(),
            last_event_id: Ulid::nil(),
        };

        let nodes = document_replica_query_nodes(Some(&config), &record, local_node_id);
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&local_node_id) && nodes.contains(&remote_node_id));
        assert!(!nodes.contains(&stale_node_id));

        assert_eq!(
            document_replica_query_nodes(None, &record, local_node_id),
            vec![local_node_id]
        );
    }

    #[test]
    fn fanout_filters_nodes() {
        let server = iroh::SecretKey::from_bytes(&[24u8; 32]).public();
        let user = iroh::SecretKey::from_bytes(&[25u8; 32]).public();
        let unknown = iroh::SecretKey::from_bytes(&[26u8; 32]).public();
        let realm_id = RealmId([4u8; 32]);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
        config.ensure_node(server, RealmNodeKind::Server);
        config.ensure_node(
            user,
            RealmNodeKind::User {
                owner: UserId::nil(realm_id),
            },
        );

        let nodes = authorized_realm_nodes(&config, HashSet::from([server, user, unknown]))
            .expect("valid node ids");

        assert_eq!(nodes, HashSet::from([server]));
    }

    #[test]
    fn deduplicate_fanout_nodes_preserves_first_seen_order() {
        let first = iroh::SecretKey::from_bytes(&[31u8; 32]).public();
        let second = iroh::SecretKey::from_bytes(&[32u8; 32]).public();
        let third = iroh::SecretKey::from_bytes(&[33u8; 32]).public();

        assert_eq!(
            deduplicate_fanout_nodes(vec![first, second, first, third, second]),
            vec![first, second, third]
        );
    }

    #[test]
    fn rejects_cursor_tampering() {
        let secret = iroh::SecretKey::from_bytes(&[34u8; 32]);
        let signer = secret.public();
        let receiver = iroh::SecretKey::from_bytes(&[36u8; 32]).public();
        let fingerprint = [35u8; 32];
        let mut cursor = ObjectSearchCursor::new_signed(
            fingerprint,
            SystemTime::UNIX_EPOCH,
            &[ObjectSearchPartitionState {
                node_id: signer,
                start_after: None,
                exhausted: false,
                observed_at: Some(SystemTime::UNIX_EPOCH),
            }],
            &[],
            false,
            0,
            signer,
            |bytes| secret.sign(bytes),
        )
        .expect("object search cursor signs");

        assert!(
            ObjectSearchCursor::decode(&cursor.encode().unwrap(), fingerprint, &[receiver, signer])
                .is_ok()
        );
        assert!(matches!(
            ObjectSearchCursor::decode(&cursor.encode().unwrap(), fingerprint, &[receiver]),
            Err(MetadataApiError::InvalidCursor(_))
        ));
        cursor.payload.omitted_partitions = 1;
        assert!(matches!(
            ObjectSearchCursor::decode(&cursor.encode().unwrap(), fingerprint, &[signer]),
            Err(MetadataApiError::InvalidCursor(_))
        ));
    }

    #[tokio::test]
    async fn bucket_fanout_partial() {
        let directory = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let local = iroh::SecretKey::from_bytes(&[41u8; 32]).public();
        let healthy = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
        let failed = iroh::SecretKey::from_bytes(&[43u8; 32]).public();
        let local_call: MetadataNodeCall<usize> =
            metadata_node_call((), |(), _| async move { Ok(1) });
        let remote_call: MetadataNodeCall<usize> =
            metadata_node_call(failed, |failed, node_id| async move {
                if node_id == failed {
                    Err(MetadataReadError::Unavailable)
                } else {
                    Ok(2)
                }
            });

        let (parts, stats) = run_metadata_fanout(
            &context,
            RealmId::from_bytes([9u8; 32]),
            local,
            MetadataFanoutScope::new(
                Some(MetadataApiQueryMode::Distributed),
                Some(vec![local, healthy, failed]),
                true,
            ),
            MetadataFanoutOperation::BucketSearch,
            local_call,
            remote_call,
            |_, _| {},
            map_read_error,
        )
        .await
        .unwrap();

        assert_eq!(parts, vec![(local, 1), (healthy, 2)]);
        assert_eq!(stats.nodes_queried, 3);
        assert_eq!(stats.nodes_failed, 1);
        assert_eq!(stats.failed_partitions, vec![failed]);
    }

    #[tokio::test]
    async fn object_fanout_reports_partial_partitions() {
        let directory = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let local = iroh::SecretKey::from_bytes(&[81u8; 32]).public();
        let healthy = iroh::SecretKey::from_bytes(&[82u8; 32]).public();
        let failed = iroh::SecretKey::from_bytes(&[83u8; 32]).public();
        let local_call: MetadataNodeCall<usize> =
            metadata_node_call((), |(), _| async move { Ok(1) });
        let remote_call: MetadataNodeCall<usize> =
            metadata_node_call(failed, |failed, node_id| async move {
                if node_id == failed {
                    Err(MetadataReadError::Unavailable)
                } else {
                    Ok(2)
                }
            });

        let (parts, stats) = run_metadata_fanout(
            &context,
            RealmId::from_bytes([19u8; 32]),
            local,
            MetadataFanoutScope::new(
                Some(MetadataApiQueryMode::Distributed),
                Some(vec![local, healthy, failed]),
                ObjectSearchQueryMode::DistributedBestEffort.allow_partial(),
            ),
            MetadataFanoutOperation::ObjectSearch,
            local_call,
            remote_call,
            |_, _| {},
            map_read_error,
        )
        .await
        .unwrap();

        assert_eq!(parts, vec![(local, 1), (healthy, 2)]);
        assert_eq!(stats.nodes_queried, 3);
        assert_eq!(stats.nodes_failed, 1);
        assert_eq!(stats.failed_partitions, vec![failed]);
    }

    #[tokio::test]
    async fn object_fanout_strict_fails_instead_of_downgrading() {
        let directory = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let local = iroh::SecretKey::from_bytes(&[84u8; 32]).public();
        let failed = iroh::SecretKey::from_bytes(&[85u8; 32]).public();
        let local_call: MetadataNodeCall<usize> =
            metadata_node_call((), |(), _| async move { Ok(1) });
        let remote_call: MetadataNodeCall<usize> =
            metadata_node_call(
                (),
                |(), _| async move { Err(MetadataReadError::Unavailable) },
            );

        let result = run_metadata_fanout(
            &context,
            RealmId::from_bytes([20u8; 32]),
            local,
            MetadataFanoutScope::new(
                Some(MetadataApiQueryMode::Distributed),
                Some(vec![local, failed]),
                ObjectSearchQueryMode::DistributedStrict.allow_partial(),
            ),
            MetadataFanoutOperation::ObjectSearch,
            local_call,
            remote_call,
            |_, _| {},
            map_read_error,
        )
        .await;

        assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
    }

    #[tokio::test]
    async fn preflight_fanout_reports_partial_and_strict_fails() {
        let directory = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let local = iroh::SecretKey::from_bytes(&[61u8; 32]).public();
        let healthy = iroh::SecretKey::from_bytes(&[62u8; 32]).public();
        let failed = iroh::SecretKey::from_bytes(&[63u8; 32]).public();
        let result_for = |node_id| MetadataReferencePreflightNodeExecution {
            visible_references: Vec::new(),
            targets: Vec::new(),
            freshness: MetadataPreflightNodeFreshness {
                node_id,
                index_state: MetadataPreflightIndexState::Current,
                oldest_status_updated_at_ms: None,
            },
            path_style_endpoint_available: true,
            saturated: false,
        };
        let local_call: MetadataNodeCall<MetadataReferencePreflightNodeExecution> =
            metadata_node_call(
                (),
                move |(), node_id| async move { Ok(result_for(node_id)) },
            );
        let remote_call: MetadataNodeCall<MetadataReferencePreflightNodeExecution> =
            metadata_node_call(failed, move |failed, node_id| async move {
                if node_id == failed {
                    Err(MetadataReadError::Unavailable)
                } else {
                    Ok(result_for(node_id))
                }
            });

        let (parts, stats) = run_metadata_fanout(
            &context,
            RealmId::from_bytes([19u8; 32]),
            local,
            MetadataFanoutScope::new(
                Some(MetadataApiQueryMode::Distributed),
                Some(vec![local, healthy, failed]),
                true,
            ),
            MetadataFanoutOperation::ReferencePreflight,
            local_call.clone(),
            remote_call.clone(),
            record_preflight_node_result,
            map_read_error,
        )
        .await
        .unwrap();

        assert_eq!(parts.len(), 2);
        assert_eq!(stats.nodes_queried, 3);
        assert_eq!(stats.nodes_failed, 1);
        assert_eq!(stats.failed_partitions, vec![failed]);

        let strict = run_metadata_fanout(
            &context,
            RealmId::from_bytes([19u8; 32]),
            local,
            MetadataFanoutScope::new(
                Some(MetadataApiQueryMode::Distributed),
                Some(vec![local, healthy, failed]),
                false,
            ),
            MetadataFanoutOperation::ReferencePreflight,
            local_call,
            remote_call,
            record_preflight_node_result,
            map_read_error,
        )
        .await;

        assert!(matches!(strict, Err(MetadataApiError::ServiceUnavailable)));
    }

    #[test]
    fn preflight_cursor_pagination_is_stable() {
        let secret = iroh::SecretKey::from_bytes(&[64u8; 32]);
        let node_id = secret.public();
        let hash = [65u8; 32];
        let content_w3id = format!("{ARUNA_DATA_PREFIX}{}", hex::encode(hash));
        let targets = vec![MetadataPreflightResolvedTarget {
            content_w3id: content_w3id.clone(),
            content_hash: hash,
            queried_iris: vec![content_w3id.clone()],
            targeted_versions: Vec::new(),
            removed_locations: Vec::new(),
            remove_all_resolvable_locations: false,
        }];
        let fingerprint = preflight_fingerprint(&targets, Some(MetadataApiQueryMode::Local));
        let hits = (0..3)
            .map(|index| MetadataSearchHit {
                document_id: format!("document-{index}"),
                group_id: String::new(),
                document_path: String::new(),
                graph_iri: content_w3id.clone(),
                subject_iri: format!("document-{index}"),
                score: 0.0,
                title: format!("Document {index}"),
                snippet: None,
                subject_types: Vec::new(),
            })
            .collect::<Vec<_>>();
        let mut watermark = None;
        let mut returned = Vec::new();

        for depth in 1..=3 {
            let page = paginate(
                vec![NodeSearchResult {
                    node_id,
                    hits: hits[..depth].to_vec(),
                    saturated: depth < hits.len(),
                }],
                watermark,
                1,
                METADATA_SEARCH_MAX_PAGINATION_DEPTH,
            );
            returned.push(page.hits[0].document_id.clone());
            watermark = page.next.map(|next| {
                let cursor = SearchCursor::new_signed(
                    fingerprint,
                    next.watermark,
                    next.resume,
                    node_id,
                    |bytes| secret.sign(bytes),
                )
                .expect("search cursor signs");
                let decoded = SearchCursor::decode(&cursor.encode().unwrap(), &[node_id]).unwrap();
                assert_eq!(decoded.fingerprint, fingerprint);
                decoded.payload.watermark
            });
        }

        assert_eq!(returned, vec!["document-0", "document-1", "document-2"]);
        assert!(watermark.is_none());
    }

    #[tokio::test]
    async fn bucket_denial_wins() {
        let directory = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let local = iroh::SecretKey::from_bytes(&[49u8; 32]).public();
        let denied = iroh::SecretKey::from_bytes(&[50u8; 32]).public();
        let local_call: MetadataNodeCall<Vec<BucketSearchHit>> =
            metadata_node_call((), |(), _| async move { Ok(Vec::new()) });
        let remote_call: MetadataNodeCall<Vec<BucketSearchHit>> =
            metadata_node_call(denied, |denied, node_id| async move {
                if node_id == denied {
                    Err(MetadataReadError::Forbidden)
                } else {
                    Ok(Vec::new())
                }
            });

        let result = run_metadata_fanout(
            &context,
            RealmId::from_bytes([12u8; 32]),
            local,
            MetadataFanoutScope::new(
                Some(MetadataApiQueryMode::Distributed),
                Some(vec![local, denied]),
                true,
            ),
            MetadataFanoutOperation::BucketSearch,
            local_call,
            remote_call,
            |_, _| {},
            map_read_error,
        )
        .await;

        assert!(matches!(result, Err(MetadataApiError::Forbidden)));
    }

    #[tokio::test]
    async fn fanout_missing_fails() {
        let directory = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let local = iroh::SecretKey::from_bytes(&[47u8; 32]).public();
        let stale = iroh::SecretKey::from_bytes(&[48u8; 32]).public();
        let local_call: MetadataNodeCall<usize> =
            metadata_node_call((), |(), _| async move { Ok(1) });
        let remote_call: MetadataNodeCall<usize> =
            metadata_node_call(stale, |stale, node_id| async move {
                if node_id == stale {
                    Err(MetadataReadError::NotFound)
                } else {
                    Ok(2)
                }
            });

        let result = run_metadata_fanout(
            &context,
            RealmId::from_bytes([11u8; 32]),
            local,
            MetadataFanoutScope::new(
                Some(MetadataApiQueryMode::Distributed),
                Some(vec![local, stale]),
                true,
            ),
            MetadataFanoutOperation::Search,
            local_call,
            remote_call,
            |_, _| {},
            map_read_error,
        )
        .await;

        assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
    }

    #[tokio::test(start_paused = true)]
    async fn search_deadline_partial() {
        // A node that never answers must not stall search fanout: the overall
        // deadline fails its partition and the reachable nodes still answer.
        let directory = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let local = iroh::SecretKey::from_bytes(&[44u8; 32]).public();
        let healthy = iroh::SecretKey::from_bytes(&[45u8; 32]).public();
        let hanging = iroh::SecretKey::from_bytes(&[46u8; 32]).public();
        let local_call: MetadataNodeCall<usize> =
            metadata_node_call((), |(), _| async move { Ok(1) });
        let remote_call: MetadataNodeCall<usize> =
            metadata_node_call(hanging, |hanging, node_id| async move {
                if node_id == hanging {
                    std::future::pending::<Result<usize, MetadataReadError>>().await
                } else {
                    Ok(2)
                }
            });

        let (parts, stats) = run_metadata_fanout(
            &context,
            RealmId::from_bytes([10u8; 32]),
            local,
            MetadataFanoutScope::new(
                Some(MetadataApiQueryMode::Distributed),
                Some(vec![local, healthy, hanging]),
                true,
            ),
            MetadataFanoutOperation::Search,
            local_call,
            remote_call,
            |_, _| {},
            map_read_error,
        )
        .await
        .unwrap();

        assert_eq!(parts, vec![(local, 1), (healthy, 2)]);
        assert_eq!(stats.nodes_queried, 3);
        assert_eq!(stats.nodes_failed, 1);
        assert_eq!(stats.failed_partitions, vec![hanging]);
    }

    #[tokio::test]
    async fn capped_fanout_incomplete() {
        // More realm nodes than the fanout cap truncates the node set, which a
        // caller that refused partial results must not receive as complete.
        let directory = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: storage::FjallStorage::open(directory.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let nodes = (0..=METADATA_DISTRIBUTED_QUERY_MAX_NODES)
            .map(|index| iroh::SecretKey::from_bytes(&[60 + index as u8; 32]).public())
            .collect::<Vec<_>>();
        let local = nodes[0];
        let local_call: MetadataNodeCall<usize> =
            metadata_node_call((), |(), _| async move { Ok(1) });
        let remote_call: MetadataNodeCall<usize> =
            metadata_node_call((), |(), _| async move { Ok(2) });

        let result = run_metadata_fanout(
            &context,
            RealmId::from_bytes([13u8; 32]),
            local,
            MetadataFanoutScope::new(Some(MetadataApiQueryMode::Distributed), Some(nodes), false),
            MetadataFanoutOperation::Search,
            local_call,
            remote_call,
            |_, _| {},
            map_read_error,
        )
        .await;

        assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
    }

    #[tokio::test]
    async fn write_policy_denies() {
        // The policy must be evaluated with the permission the caller asked for,
        // so a write-deny policy cannot be bypassed by a fixed read request.
        let test = metadata_test();
        let group_id = Ulid::generate();
        let user = UserId::local(Ulid::generate(), TEST_REALM_ID);
        let path = format!("/{TEST_REALM_ID}/g/{group_id}/data/object");
        write_policy_docs(
            &test,
            group_id,
            user_role(
                user,
                HashMap::from([(
                    format!("/{TEST_REALM_ID}/g/{group_id}/**"),
                    Permission::WRITE,
                )]),
            ),
            vec![aruna_core::request_policy::RequestPolicy {
                policy_id: Ulid::generate(),
                name: "no-writes".to_string(),
                kind: aruna_core::request_policy::PolicyKind::Deny,
                when: None,
                expression: "permission == 'write'".to_string(),
                enabled: true,
            }],
        )
        .await;

        let denied = ensure_permission(
            &test.context,
            TEST_REALM_ID,
            auth_for(user),
            group_id,
            path.clone(),
            Permission::WRITE,
            None,
        )
        .await;
        // The same role allows the read, so the denial comes from the policy.
        let allowed = ensure_permission(
            &test.context,
            TEST_REALM_ID,
            auth_for(user),
            group_id,
            path,
            Permission::READ,
            None,
        )
        .await;

        assert!(matches!(denied, Err(MetadataApiError::Forbidden)));
        assert!(allowed.is_ok());
    }

    #[test]
    fn bearer_limits() {
        assert!(matches!(
            forwarded_bearer(Some(&"x".repeat(4096))),
            Ok(Some(MetadataAuthToken::Bearer(_)))
        ));
        assert!(matches!(
            forwarded_bearer(Some(&"x".repeat(4097))),
            Err(MetadataApiError::BadRequest)
        ));
        assert!(fanout_bearer(Some(&"x".repeat(4097))).is_none());
        assert!(matches!(forwarded_bearer(None), Ok(None)));
    }
}
