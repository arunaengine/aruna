//! Owns the metadata handle: its caches, peer state and node keys, beside the graph store.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::format::Compression;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
#[cfg(feature = "test-hooks")]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::metadata::{MetadataError, REGISTRY_CANDIDATE_LIMIT};
use aruna_core::structs::SyncRelationship;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use aruna_graph::GraphHandle;
use aruna_net::NetHandle;
use aruna_storage::StorageHandle;
use aruna_storage::key_index::RangeIndexes;
use ulid::Ulid;

use super::contact::PeerContacts;
use super::materialization_queue::metadata_graph_fence;
use super::query_cache::MetadataQueryCache;
use crate::auth::bearer_token::{
    DecodeTokenOperation, IssuerKeyCache, IssuerTrust, RevocationCheck, TokenChecks, claims_auth,
};
use crate::auth::error::ArunaBearerError;
use crate::driver::{DriverContext, drive};
use crate::s3::bucket::create::{CreateBucketError, CreateBucketOperation};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::auth::TokenClaims;

mod effects;
pub use self::effects::{export_rocrate_jsonld, send_metadata_effect};
pub(crate) use self::effects::{export_summary_jsonld, metadata_read_error};
pub use self::lifecycle::{list_cached_group, list_cached_records, warm_caches};
pub(crate) use self::lifecycle::{list_group_records, prune_if_deleted};
pub use self::query::query_authorized_local;
pub use self::search::search_authorized_local;
pub(crate) use self::search::search_local_filtered;
pub(crate) use self::transport::transport_message_kind;
mod ingress;
mod lifecycle;
mod peer_auth;
mod query;
mod search;
mod sync;
mod transport;

const METADATA_IO_TIMEOUT: Duration = Duration::from_secs(15);
/// How long a sender waits for a member's answer to a reference page.
const PAGE_ANSWER_TIMEOUT: Duration = Duration::from_secs(120);
const METADATA_CHUNK_SIZE: usize = 64 * 1024;
const METADATA_ENVELOPE_BYTES: u64 = 16 * 1024 * 1024;
const GRAPH_SYNC_ATTEMPTS: usize = 3;
const GRAPH_SYNC_AFTER: Duration = Duration::from_millis(250);
pub(crate) const QUERY_MAX_BYTES: usize = 64 * 1024;
pub(crate) const METADATA_QUERY_DEADLINE: Duration = Duration::from_secs(10);
const VISIBILITY_CACHE_TTL: Duration = Duration::from_secs(30);

fn sync_identity_matches(left: &SyncRelationship, right: &SyncRelationship) -> bool {
    left.id == right.id
        && left.source == right.source
        && left.target == right.target
        && left.mode == right.mode
        && left.replicate_deletes == right.replicate_deletes
        && left.created_by == right.created_by
        && left.created_at == right.created_at
}

/// The sender of a mirror rollback, as the transport authenticated it.
#[derive(Clone, Copy, Debug)]
struct RollbackRequest {
    peer: NodeId,
    local_node: NodeId,
    /// The token was node-vouched, never a user bearer.
    vouched: bool,
}

impl RollbackRequest {
    /// Only the source peer of this exact incoming mirror may roll it back without WRITE.
    fn allows(&self, stored: &SyncRelationship, request: &SyncRelationship) -> bool {
        self.vouched
            && request.source.node_id == self.peer
            && request.target.node_id == self.local_node
            && sync_identity_matches(stored, request)
    }
}

fn valid_sync_request(
    relationship: &SyncRelationship,
    source_bucket: &str,
    target_bucket: &str,
    realm_id: RealmId,
    user_id: UserId,
    delete: bool,
) -> bool {
    !source_bucket.is_empty()
        && !target_bucket.is_empty()
        && relationship.source.key_prefix() != Some("")
        && relationship.target.key_prefix() != Some("")
        && relationship.source.realm_id == realm_id
        && relationship.target.realm_id == realm_id
        && relationship.created_by == user_id
        && (delete || (!source_bucket.starts_with("ws-") && !target_bucket.starts_with("ws-")))
}

async fn create_sync_bucket(
    context: &DriverContext,
    bucket: &str,
    group_id: GroupId,
    relationship: &SyncRelationship,
) -> Result<(), CreateBucketError> {
    drive(
        CreateBucketOperation::new(
            bucket.to_string(),
            BucketInfo {
                group_id,
                created_at: relationship.created_at,
                created_by: relationship.created_by,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
                distribution: None,
            },
        ),
        context,
    )
    .await?;
    Ok(())
}

/// The metadata handle and the graph store of `context`.
fn handles(context: &DriverContext) -> Result<(&MetadataHandle, &GraphHandle), MetadataError> {
    match (&context.metadata_handle, &context.graph_handle) {
        (Some(metadata), Some(graph)) => Ok((metadata, graph)),
        _ => Err(MetadataError::HandleMissing),
    }
}

/// The directory below the metadata storage path that holds the range search indexes.
const RANGE_INDEX_DIR: &str = "range-indexes";

#[derive(Clone)]
pub struct MetadataHandle {
    inner: Arc<MetadataInner>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MetadataRequestDelivery {
    DefinitelyNotSent,
    PossiblySent,
}

#[derive(Debug)]
pub(crate) enum WritePeerError {
    Unauthorized,
    Unavailable(MetadataError),
}

#[derive(Debug)]
pub(crate) struct MetadataRequestError {
    delivery: MetadataRequestDelivery,
    error: MetadataError,
}

impl MetadataRequestError {
    fn definitely_not_sent(error: MetadataError) -> Self {
        Self {
            delivery: MetadataRequestDelivery::DefinitelyNotSent,
            error,
        }
    }

    pub(crate) fn possibly_sent(error: MetadataError) -> Self {
        Self {
            delivery: MetadataRequestDelivery::PossiblySent,
            error,
        }
    }

    pub(crate) fn delivery(&self) -> MetadataRequestDelivery {
        self.delivery
    }

    pub(crate) fn into_metadata_error(self) -> MetadataError {
        self.error
    }
}

impl std::fmt::Display for MetadataRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

#[derive(Clone, Debug, Default)]
pub struct MetadataHandleOptions {}

struct MetadataInner {
    storage_handle: StorageHandle,
    auth_validation: AuthValidationState,
    net_handle: Option<NetHandle>,
    visibility_cache: MetadataVisibilityCache,
    query_cache: MetadataQueryCache,
    inbound_frame_bytes: Arc<tokio::sync::Semaphore>,
    peer_contacts: PeerContacts,
    sync_pulls: crate::metadata::device_pull::PullRegistry,
    #[cfg(feature = "test-hooks")]
    drop_ack: AtomicBool,
    #[cfg(feature = "test-hooks")]
    repair_hold: AtomicBool,
}

#[derive(Clone)]
struct AuthValidationState {
    storage_handle: StorageHandle,
    /// Realm this node serves; without one there is no replicated revocation
    /// set to consult, and this node serves no remote metadata peers either.
    realm_id: Option<RealmId>,
    issuer_keys: Arc<IssuerKeyCache>,
}

impl AuthValidationState {
    fn new(storage_handle: StorageHandle, realm_id: Option<RealmId>) -> Self {
        Self {
            storage_handle,
            realm_id,
            issuer_keys: Arc::new(IssuerKeyCache::new()),
        }
    }

    /// Verifies `token`. Without realm state this node holds no revocation authority at all;
    /// once it has any, the token's own issuing realm answers, unless `blind` skips revocation.
    async fn decode(&self, token: &str, blind: bool) -> Result<TokenClaims, ArunaBearerError> {
        let revocation = match (blind, self.realm_id) {
            (false, Some(_)) => RevocationCheck::Realm,
            _ => RevocationCheck::Skipped,
        };
        let checks = TokenChecks {
            trust: IssuerTrust::Stored,
            revocation,
            linked_realm: self.realm_id,
            keys: Some(self.issuer_keys.clone()),
        };
        let context = DriverContext::storage_only(self.storage_handle.clone());
        drive(DecodeTokenOperation::new(token, checks), &context).await
    }

    async fn validate(&self, token: &str) -> Result<AuthContext, ArunaBearerError> {
        claims_auth(self.decode(token, false).await?)
    }
}

struct MetadataVisibilityCache {
    registry: Mutex<Option<RegistryCacheEntry>>,
    registry_fill: Arc<tokio::sync::Mutex<()>>,
    lifecycle_deleted: Mutex<HashMap<String, LifecycleDeletedEntry>>,
    generation: AtomicU64,
}

struct RegistryCacheEntry {
    records: BTreeMap<Ulid, MetadataRegistryRecord>,
    snapshot: Option<Arc<Vec<MetadataRegistryRecord>>>,
    group_snapshots: HashMap<GroupId, Arc<Vec<MetadataRegistryRecord>>>,
    expires_at: Instant,
}

impl RegistryCacheEntry {
    fn snapshot(&mut self) -> Option<Arc<Vec<MetadataRegistryRecord>>> {
        if self.records.len() > REGISTRY_CANDIDATE_LIMIT {
            return None;
        }
        Some(
            self.snapshot
                .get_or_insert_with(|| Arc::new(self.records.values().cloned().collect()))
                .clone(),
        )
    }

    fn group_snapshot(&mut self, group_id: GroupId) -> Option<Arc<Vec<MetadataRegistryRecord>>> {
        if let Some(records) = self.group_snapshots.get(&group_id) {
            return (records.len() <= REGISTRY_CANDIDATE_LIMIT).then(|| records.clone());
        }
        let mut group_records = Vec::new();
        for record in self
            .records
            .values()
            .filter(|record| record.group_id == group_id)
        {
            if group_records.len() == REGISTRY_CANDIDATE_LIMIT {
                return None;
            }
            group_records.push(record.clone());
        }
        let records = Arc::new(group_records);
        self.group_snapshots.insert(group_id, records.clone());
        Some(records)
    }
}

struct LifecycleDeletedEntry {
    deleted: bool,
    expires_at: Instant,
}

struct VisibilityFillResult {
    records: Arc<Vec<MetadataRegistryRecord>>,
    store_accepted: bool,
}

struct LifecycleVisibilityRefresh {
    deleted_graphs: HashSet<String>,
    store_accepted: bool,
}

impl std::fmt::Debug for MetadataHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataHandle").finish_non_exhaustive()
    }
}

impl MetadataHandle {
    /// A handle whose range indexes live under `path`.
    pub fn new(
        path: impl AsRef<Path>,
        storage_handle: StorageHandle,
        net_handle: Option<NetHandle>,
        _options: MetadataHandleOptions,
    ) -> Self {
        // Range indexes sit beside the metadata store, so only a node with metadata keeps them.
        storage_handle
            .install_range_indexes(RangeIndexes::new(path.as_ref().join(RANGE_INDEX_DIR)));
        Self {
            inner: Arc::new(MetadataInner {
                auth_validation: AuthValidationState::new(
                    storage_handle.clone(),
                    net_handle.as_ref().map(|net| *net.realm_id()),
                ),
                storage_handle,
                net_handle,
                visibility_cache: MetadataVisibilityCache::new(),
                query_cache: MetadataQueryCache::new(),
                inbound_frame_bytes: Arc::new(tokio::sync::Semaphore::new(
                    super::protocol::INBOUND_FRAME_BYTES,
                )),
                peer_contacts: PeerContacts::default(),
                sync_pulls: Default::default(),
                #[cfg(feature = "test-hooks")]
                drop_ack: AtomicBool::new(false),
                #[cfg(feature = "test-hooks")]
                repair_hold: AtomicBool::new(false),
            }),
        }
    }

    /// Test hook: the next sync mirror create reports a timeout after the target stored it.
    #[cfg(feature = "test-hooks")]
    pub fn drop_create_ack(&self) {
        self.inner
            .drop_ack
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Test hook: sync mirror repair on this node waits until `release_repair`.
    #[cfg(feature = "test-hooks")]
    pub fn hold_repair(&self) {
        self.inner
            .repair_hold
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(feature = "test-hooks")]
    pub fn release_repair(&self) {
        self.inner
            .repair_hold
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn repair_held(&self) -> bool {
        self.inner
            .repair_hold
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(feature = "test-hooks")]
    fn take_dropped_ack(&self) -> bool {
        self.inner
            .drop_ack
            .swap(false, std::sync::atomic::Ordering::SeqCst)
    }

    /// The device pulls this node is running.
    pub(crate) fn sync_pulls(&self) -> &crate::metadata::device_pull::PullRegistry {
        &self.inner.sync_pulls
    }

    pub fn cache_registry_record(&self, record: MetadataRegistryRecord) {
        self.inner
            .visibility_cache
            .upsert_registry_records(std::slice::from_ref(&record));
    }

    pub fn cache_registry_records(&self, records: &[MetadataRegistryRecord]) {
        self.inner.visibility_cache.upsert_registry_records(records);
    }

    pub(crate) fn remove_cached_record(&self, document_id: Ulid) {
        self.inner
            .visibility_cache
            .remove_registry_record(document_id);
    }

    pub(in crate::metadata) fn query_cache(&self) -> &MetadataQueryCache {
        &self.inner.query_cache
    }

    pub(crate) fn visibility_generation(&self) -> u64 {
        self.inner.visibility_cache.current_generation()
    }

    /// Test hook: marks all visibility cache entries as expired so the next
    /// read exercises the stale-serving + background-refill path.
    #[doc(hidden)]
    pub fn expire_visibility_caches(&self) {
        self.inner.visibility_cache.expire_now();
    }
}

#[cfg(test)]
mod tests;
