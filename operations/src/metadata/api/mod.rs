//! Wires the metadata read API submodules together and maps their errors to one API error.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::short_display_id;
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, EVENT_LOG_KEYSPACE, PENDING_PROJECTION_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataError, MetadataEventRecord, MetadataQueryResults, MetadataRoCratePage,
    MetadataSearchHit,
};
use aruna_core::storage_entries::{event_log_key, pending_projection_target};
use aruna_core::structs::PathClaimRecord;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, VersionKey, bucket_permission_path,
    object_permission_path,
};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::replication::{ARUNA_DATA_PREFIX, W3idIdentifier};
use aruna_core::telemetry::record_elapsed_ms;
use aruna_core::types::{GroupId, Key, TxnId, Value};
use aruna_core::{MetaResourceId, NodeId};
use futures_util::future::BoxFuture;
use futures_util::stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{Span, debug_span, field, warn};
use ulid::Ulid;

use self::distributed_types::{
    object_search_fingerprint, record_object_result, record_preflight_node,
};

use self::fanout_types::forwarded_bearer;

use self::fanout_types::{MetadataFanoutOperation, MetadataNodeCall};
use self::fanout_types::{fanout_bearer, metadata_node_call, query_union_safe};
#[cfg(test)]
use self::list_types::{
    ANONYMOUS_METADATA_LIMIT, ESTIMATE_MIN_LIMIT, LIST_METADATA_LIMIT, MAX_METADATA_LIMIT,
    effective_list_limit,
};
use self::list_types::{check_policy_limit, merge_pending_records};

use self::path_types::select_fanout_nodes;
use self::path_types::{deduplicate_fanout_nodes, replica_query_nodes};

use self::read_types::metadata_read_request;
use super::protocol::{
    MetadataPathCandidate, MetadataPathResolution, MetadataPathWinner, MetadataReadError,
    MetadataTransportMessage,
};
use super::search_cursor::{
    CursorEnvelopeError, MAX_PAGINATION_DEPTH, NodeSearchResult, SearchCursor, SearchCursorError,
    SearchPageCursor, SearchWatermark, SignedCursor, merge_search_hits, paginate,
    query_fingerprint, resume_fetch_limit,
};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::permission_rules::GroupPermissionRules;
use crate::blob::permission_paths::ResolvePathsOperation;
use crate::groups::list_groups::ListGroupOperation;
use crate::metadata::repository::{
    LIST_METADATA_SIZE, StorageReadError, iter_registry_effect, parse_registry_iter,
};
use crate::realm::get_nodes::{GetNodesOperation, REALM_DISCOVERY_TIMEOUT};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::bucket::search::{BucketPage, BucketSearchHit, SearchBucketsInput};
use crate::s3::object::search::{
    ObjectInventoryHit, ObjectKeyMatch, SearchNodePage, SearchObjectsInput,
};
use aruna_core::metadata::AuthToken;
use aruna_core::metadata::{MAX_RESULT_BYTES, QUERY_MAX_ROWS, REGISTRY_CANDIDATE_LIMIT};
use aruna_core::placement::selector::{
    ROLE_NODE, neg_log2_q48, peer_rank, select_top_peers, selector_hash,
};
use aruna_core::placement::{
    holds_placement, meta_bucket_subject, registry_placement, registry_placement_for,
    registry_strategy, resolve_holders_limit, resolve_shard_holders,
};

mod bucket_page;
mod distributed;
pub mod distributed_types;
pub mod export;
pub mod export_types;
pub mod fanout;
pub mod fanout_types;
mod head_versions;
mod join;
pub mod links;
pub mod list;
pub mod list_types;
pub mod path;
pub mod path_types;
mod prefix_scan;
pub mod preflight;
pub mod preflight_types;
pub mod read;
pub(crate) mod read_access;
pub mod read_types;
pub(crate) mod readable;
pub mod search;
pub mod search_types;

use self::fanout_types::MetadataFanoutScope;
use self::fanout_types::MetadataFanoutStats;
use self::fanout_types::RealmNodeDiscovery;

#[cfg(test)]
use self::path_types::PathShardView;

use self::preflight_types::ReferenceNodeExecution;
#[cfg(test)]
use self::preflight_types::{authorized_realm_nodes, preflight_fingerprint};
use self::read_types::{ApiQueryMode, MetadataReferenceEntry, MetadataReferencesRequest};

use self::search_types::{BucketSearchExecution, BucketSearchRequest, ObjectQueryMode};
#[cfg(test)]
use self::search_types::{ObjectCursor, ObjectPartitionState, PendingBucket};

const REFERENCES_LIMIT: usize = 25;

const REFERENCES_MAX_LIMIT: usize = 100;

const QUERY_FANOUT_LIMIT: usize = 8;

const QUERY_MAX_NODES: usize = 32;

const DISTRIBUTED_QUERY_DEADLINE: Duration = Duration::from_secs(12);

#[derive(Debug, Error, PartialEq)]
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
    /// A strict search found ranges it could not search.
    #[error("{0}")]
    SearchIncomplete(String),
    /// The bucket has no usable activation, so the request cannot be routed.
    /// Never absence: no holder was resolved to answer it.
    #[error("placement unavailable: {0}")]
    PlacementUnavailable(aruna_core::placement::PlacementResolveError),
    #[error("{0}")]
    InvalidCursor(String),
    /// The query is valid for the client but cannot be run as asked.
    #[error("{0}")]
    InvalidQuery(String),
    #[error("{0}")]
    Internal(String),
    /// Even the smallest page of the answer is larger than the asked byte bound.
    #[error("the answer is larger than the byte bound")]
    TooLarge,
}

/// A runner failure leaves the request unanswered, so the caller may retry.
impl From<aruna_core::calls::IoError> for MetadataApiError {
    fn from(_: aruna_core::calls::IoError) -> Self {
        Self::ServiceUnavailable
    }
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
        MetadataReadError::TooLarge => MetadataApiError::TooLarge,
    }
}

#[cfg(test)]
mod tests;
