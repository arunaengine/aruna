use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::short_display_id;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, METADATA_DOCUMENT_LIFECYCLE_KEYSPACE,
    METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE,
    METADATA_PENDING_PROJECTION_KEYSPACE,
};
use aruna_core::metadata::{
    GraphLifecycleRecord, MetadataError, MetadataEventRecord, MetadataLifecycleRecord,
    MetadataQueryResults, MetadataRoCratePage, MetadataSearchHit,
};
use aruna_core::storage_entries::{
    document_lifecycle_key, event_log_key, graph_lifecycle_key, pending_projection_target,
};
use aruna_core::structs::storage::replication::{ARUNA_DATA_PREFIX, W3idIdentifier};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer, VersionKey,
    bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::PathClaimRecord;
use aruna_core::structs::placement::placement_record::PlacementRef;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::telemetry::record_elapsed_ms;
use aruna_core::types::{GroupId, Key, TxnId, Value};
use aruna_core::{MetaResourceId, NodeId};
use aruna_storage::StorageHandle;
use futures_util::future::BoxFuture;
use futures_util::stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{Span, debug_span, field, warn};
use ulid::Ulid;

pub use self::distributed::{aggregate_query_results, query_form, query_select_limit};
use self::distributed::{object_search_fingerprint, record_object_result, record_preflight_node};
use self::export::export_summary_jsonld;
pub use self::export::{export_metadata_rocrate, get_visible_document};
pub use self::fanout::forwarded_bearer;
pub(crate) use self::fanout::pattern_contains_service;
pub use self::fanout::search_buckets_distributed;
use self::fanout::{
    MetadataFanoutOperation, MetadataNodeCall, fanout_bearer, metadata_node_call, query_union_safe,
    run_metadata_fanout,
};
#[cfg(test)]
use self::list::{
    ANONYMOUS_LIST_METADATA_LIMIT, DEFAULT_LIST_METADATA_LIMIT, MAX_LIST_METADATA_LIMIT,
    METADATA_ESTIMATE_MIN_LIMIT, effective_list_limit,
};
use self::list::{
    check_policy_limit, load_group_records, load_pending_records, merge_pending_records,
};
pub(crate) use self::path::local_path_candidates;
use self::path::select_fanout_nodes;
pub use self::path::{deduplicate_fanout_nodes, replica_query_nodes};
pub(crate) use self::preflight::{discover_realm_nodes, references_preflight_local};
pub use self::preflight::{load_realm_config, load_realm_nodes};
use self::preflight::{reference_document_title, resolve_graph_reference};
use self::read::ensure_record_materialized;
pub(crate) use self::read::{
    can_read_record, ensure_record_readable, filter_live_records, load_live_record,
    metadata_read_request,
};
pub use self::read::{query_metadata, query_metadata_document, references_metadata};
use super::AuthToken;
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
use crate::blob::permission_paths::ResolvePathsOperation;
use crate::driver::{DriverContext, drive};
use crate::groups::list_groups::ListGroupOperation;
use crate::metadata::get_document::{load_document_record, record_materialized_read};
use crate::metadata::repository::{
    LIST_METADATA_PAGE_SIZE, StorageReadError, iter_registry_effect, parse_registry_iter,
    parse_registry_read, read_document_registry,
};
use crate::placement::selector::{
    ROLE_NODE, neg_log2_q48, peer_rank, select_top_peers, selector_hash,
};
use crate::placement::{
    holds_placement, meta_bucket_subject, registry_placement, registry_placement_for,
    registry_strategy, resolve_holders_limit, resolve_shard_holders,
};
use crate::realm::get_config::GetConfigOperation;
use crate::realm::get_nodes::{GetNodesOperation, REALM_DISCOVERY_TIMEOUT};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::bucket::search::{BucketSearchHit, SearchBucketsInput, search_local_buckets};
use crate::s3::object::search::{
    ObjectInventoryHit, ObjectKeyMatch, SearchNodePage, SearchObjectsInput, search_local_objects,
};

mod distributed;
mod export;
mod fanout;
mod list;
mod path;
mod preflight;
mod read;
mod search;

pub use self::distributed::MetadataQueryForm;
pub use self::export::{
    ExportMetadataRequest, ExportMetadataResult, GetVisibleRequest, RoCrateExportView,
};
use self::fanout::MetadataFanoutScope;
pub use self::fanout::MetadataFanoutStats;
pub(crate) use self::fanout::RealmNodeDiscovery;
pub use self::list::{
    ListVisibleRequest, ListVisibleResult, ListedMetadataDocument, MetadataListOrder,
    list_visible_documents,
};
pub(crate) use self::path::resolve_local_path;
pub use self::path::{MetadataLookupRequest, MetadataLookupResult, lookup_metadata_path};
#[cfg(test)]
use self::path::{PathShardView, merge_path_views, reduce_path_candidates, select_path_holders};
pub use self::preflight::{
    MetadataExcludedForm, MetadataIndexState, MetadataNodeFreshness, MetadataPreflightLocation,
    MetadataResolvedTarget, MetadataStorageOperation, MetadataVisibleReference, ReferenceCoverage,
    ReferenceExecution, ReferenceNodeExecution, ReferenceNodeRequest, ReferenceNodeTarget,
    ReferenceRequest, ReferenceTarget, ReferenceTargetExecution, references_preflight,
};
#[cfg(test)]
use self::preflight::{authorized_realm_nodes, preflight_fingerprint};
pub use self::read::{
    ApiQueryMode, DocumentQueryRequest, MetadataQueryExecution, MetadataQueryRequest,
    MetadataReferenceEntry, MetadataReferencesExecution, MetadataReferencesRequest,
};
pub use self::search::{
    BucketSearchExecution, BucketSearchRequest, MetadataSearchExecution, MetadataSearchRequest,
    ObjectExecution, ObjectPartitionCoverage, ObjectQueryMode, SearchQueryRequest, search_metadata,
    search_objects,
};
#[cfg(test)]
use self::search::{ObjectCursor, ObjectPartitionState};

const METADATA_REFERENCES_DEFAULT_LIMIT: usize = 25;

const METADATA_REFERENCES_MAX_LIMIT: usize = 100;

const METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT: usize = 8;

const METADATA_DISTRIBUTED_QUERY_MAX_NODES: usize = 32;

const METADATA_DISTRIBUTED_QUERY_DEADLINE: Duration = Duration::from_secs(12);

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

fn map_event_error(error: MetadataError) -> MetadataApiError {
    match error {
        MetadataError::GraphNotFound => MetadataApiError::ServiceUnavailable,
        other => MetadataApiError::Internal(other.to_string()),
    }
}

fn map_query_error(error: MetadataError) -> MetadataApiError {
    match error {
        MetadataError::InvalidInput(_) => MetadataApiError::BadRequest,
        other => map_event_error(other),
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

fn map_internal_error(error: MetadataError) -> MetadataApiError {
    MetadataApiError::Internal(error.to_string())
}

#[cfg(test)]
mod tests;
