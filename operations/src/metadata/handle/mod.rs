use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::auth::TRUSTED_REALMS_LIST_KEY;
use aruna_core::effects::{Effect, IterStart, StorageEffect, StoragePriority};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    API_STATE_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE, METADATA_INDEX_KEYSPACE,
    REALM_CONFIG_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataBatch, MetadataBatchSource, MetadataCreateCrateRequest, MetadataDot, MetadataEffect,
    MetadataError, MetadataEvent, MetadataGraphLifecycleRecord, MetadataGraphPolicy,
    MetadataQuadOp, MetadataQueryResults, MetadataRequestDurability, MetadataRoCratePage,
    MetadataSearchHit, MetadataUpsertEntityRequest, MetadataValidationViolation,
};
use aruna_core::structs::{
    AuthContext, BucketInfo, MetadataRegistryRecord, Permission, RealmConfigDocument, RealmId,
    SyncRelationship, TokenClaims, blob_bucket_permission_path,
};
use aruna_core::telemetry::{duration_ms, record_duration_ms, record_elapsed_ms};
use aruna_core::types::{GroupId, UserId};
use aruna_core::util::unix_timestamp_millis;
use aruna_net::NetHandle;
use aruna_net::streams::{BiStream, RecvStream};
use aruna_storage::{FjallPersistPolicy, StorageHandle};
use async_trait::async_trait;
use byteview::ByteView;
use craqle::{
    Action as CraqleAction, ActorId, AllowAllAuthorizer, AuthorizationError as CraqleAuthError,
    Authorizer as CraqleAuthorizer, Batch, CraqleError, CraqleFjallPersistMode,
    CraqleIrokleOptions, CraqleNode, CraqleOptions, CraqleRequestDurability, CrateViolation,
    CreateCrateRequest, CreateEntityRequest, DescribeRequest, GraphId, GraphPolicy,
    GraphSearchRequest, PatchEntityRequest, RoCrateError, SearchRequest, SearchStorage, vocab,
};
use futures_util::FutureExt;
use jsonwebtoken::DecodingKey;
use oxrdf::{BlankNode, Dataset, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use serde::de::DeserializeOwned;
use serde_json::Value;
use spareval::{CancellationToken, QueryEvaluator};
use spargebra::{Query, SparqlParser};
use tokio::io::AsyncRead;
use tokio::time::{sleep, timeout, timeout_at};
use tracing::{Instrument, Span, debug, debug_span, field, warn};
use ulid::Ulid;

use self::engine::{
    config_digest_matches, craqle_create_request, craqle_fjall_persist_mode, craqle_graph_policy,
    craqle_request_durability, document_sync_peer_id, effect_graph_iri,
    flush_document_sync_journal, flush_metadata_persistence, graph_ids, metadata_batch_from_craqle,
    metadata_effect_defers_persist, metadata_effect_kind, metadata_effect_persists_document_sync,
    metadata_error_from_craqle, metadata_event_kind, metadata_graph_policy_from_craqle,
    metadata_rocrate_page_from_craqle, plan_batch, record_craqle_call_result, record_error,
    record_metadata_query_result_counts, record_metadata_result,
    schedule_deferred_metadata_persist, to_craqle_batch, upsert_contextual_entity,
    upsert_data_entity, warn_if_slow_metadata_backend,
};
use self::lifecycle::{
    effect_rejects_deleted_graph, fill_visibility_caches, graph_lifecycle_deleted,
    list_group_records, list_local_registry_records, list_local_registry_records_for_group,
    metadata_effect_mutates_graph, metadata_graph_deleted,
};
use self::query::{query_local_graphs, snapshot_iri_references};
use self::search::{
    AllowedGraphAuthorizer, clamp_remote_search_graph_limit, describe_hit_properties,
    list_visible_graphs, search_local_graphs, select_authorized_graphs,
};
use self::transport::{
    close_stream, close_stream_at, drain_request_stream, drain_stream_at, metadata_body_limit,
    read_budget, send_export_request, send_request, write_body_at, write_message_at,
    write_stream_body, write_transport_message,
};
use super::contact::PeerContacts;
use super::materialization_queue::metadata_graph_fence;
use super::profile_cache::ProfileCache;
use super::profile_shacl::{
    ProfileShaclEngine, ProfileShaclError, ProfileShaclReport, ProfileShapes,
};
use super::protocol::{
    MetadataAuthToken, MetadataReadError, MetadataTransportMessage, encode_message, frame_class,
    read_message, read_message_budget, read_message_cap, response_cap, write_encoded_message,
    write_message,
};
use super::query_cache::{
    CachedQuery, LocalScopeKind, MetadataQueryCache, ScopeDigest, graphs_digest, local_key,
};
use super::repository::{
    StorageReadError, iter_registry_effect, parse_graph_lifecycle_read, parse_registry_iter,
    read_graph_lifecycle_effect,
};
use super::search_cursor::{METADATA_SEARCH_MAX_PAGINATION_DEPTH, compare_hits};
use super::search_enrichment::{hit_snippet, hit_title, hit_types};
use super::summary_cache::summary_cache;
use crate::auth::bearer_token::{
    ArunaBearerTokenError, ArunaBearerTokenValidationState, IssuerKeyCache,
    decode_aruna_bearer_token, realm_token_revoked, validate_aruna_bearer_token,
};
use crate::auth::permission_rules::GroupPermissionRules;
use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::{DriverContext, drive};
use crate::realm::peer_trust::{PeerTrust, RealmPeerError, ensure_peer_trust};
use crate::s3::create_bucket::{CreateBucketError, CreateBucketOperation};
use crate::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use crate::s3::search_buckets::{BucketSearchHit, SearchBucketsInput, search_local_buckets};
use crate::s3::search_objects::{
    ObjectKeyMatch, ObjectSearchNodePage, SearchObjectsInput, search_local_objects,
};
use crate::sync::sync_mirror_repair::RECONCILE_GRACE;
use crate::sync::sync_relationship::{
    DeleteSyncRelationshipOperation, GetSyncRelationshipOperation, StoreSyncRelationshipOperation,
    SyncRelationshipDirection, SyncRelationshipError, remove_outgoing_relationship,
};

mod engine;
pub(crate) use self::engine::metadata_read_error;
pub(crate) use self::engine::transport_message_kind;
mod ingress;
mod lifecycle;
mod query;
mod search;
mod transport;

const METADATA_IO_TIMEOUT: Duration = Duration::from_secs(15);
const METADATA_CHUNK_SIZE: usize = 64 * 1024;
const METADATA_ENVELOPE_BYTES: u64 = 16 * 1024 * 1024;
const SYNC_MIRROR_REQUEST_TIMEOUT: Duration =
    RECONCILE_GRACE.saturating_sub(Duration::from_secs(10));
const METADATA_GRAPH_SYNC_ATTEMPTS: usize = 3;
const METADATA_GRAPH_SYNC_RETRY_AFTER: Duration = Duration::from_millis(250);
const SLOW_METADATA_BACKEND_THRESHOLD: Duration = Duration::from_millis(100);
// Craqle rebuilds a describe context per hit and Aruna maps one document per
// graph, so search enrichment overlaps instead of memoizing; the craqle read
// semaphore, not this task cap, is the real concurrency limit.
const METADATA_ENRICH_TASKS: usize = 8;
pub(crate) const METADATA_QUERY_MAX_BYTES: usize = 64 * 1024;
pub(crate) const METADATA_QUERY_MAX_ROWS: usize = 10_000;
pub(crate) const METADATA_QUERY_MAX_RESULT_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const METADATA_QUERY_DEADLINE: Duration = Duration::from_secs(10);
const METADATA_QUERY_COMMON_PREFIXES: &str = "\
PREFIX schema: <http://schema.org/>\n\
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n\
PREFIX fts: <urn:craqle:fts:>\n";
// Unbiased per-call-kind craqle latency histograms; every backend call is
// recorded, not just the ones above the slow-call threshold.
static CRAQLE_LATENCY: LazyLock<aruna_core::telemetry::LatencyAggregator> =
    LazyLock::new(|| aruna_core::telemetry::LatencyAggregator::new("craqle"));
const METADATA_VISIBILITY_CACHE_TTL: Duration = Duration::from_secs(30);
pub(crate) const METADATA_REGISTRY_CANDIDATE_LIMIT: usize = 1024;

fn sync_identity_matches(left: &SyncRelationship, right: &SyncRelationship) -> bool {
    left.id == right.id
        && left.source == right.source
        && left.target == right.target
        && left.mode == right.mode
        && left.replicate_deletes == right.replicate_deletes
        && left.created_by == right.created_by
        && left.created_at == right.created_at
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
            },
        ),
        context,
    )
    .await?
    .transpose()?
    .ok_or(CreateBucketError::CreateBucketFailed)?;
    Ok(())
}

#[derive(Clone)]
pub struct MetadataHandle {
    inner: Arc<MetadataInner>,
    /// Lane for this handle's own storage reads. Background drains use the bulk
    /// lane so their lifecycle reads stay off the foreground sync path.
    storage_priority: StoragePriority,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MetadataRequestDelivery {
    DefinitelyNotSent,
    PossiblySent,
}

#[derive(Debug)]
pub(crate) enum MetadataWritePeerError {
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

    pub(super) fn possibly_sent(error: MetadataError) -> Self {
        Self {
            delivery: MetadataRequestDelivery::PossiblySent,
            error,
        }
    }

    pub(crate) fn delivery(&self) -> MetadataRequestDelivery {
        self.delivery
    }

    fn into_metadata_error(self) -> MetadataError {
        self.error
    }
}

impl std::fmt::Display for MetadataRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetadataHandleOptions {
    pub search_storage: MetadataSearchStorage,
    pub document_sync_persist_policy: FjallPersistPolicy,
    /// Size of the craqle mutation and read permit pools. Defaults to the
    /// host parallelism; set explicitly when cgroup limits make
    /// `available_parallelism` unrepresentative.
    pub backend_pool_size: Option<usize>,
    /// Test/maintenance fault injection. Tagged writes fail closed while an
    /// untagged crate continues through ordinary structural validation.
    pub profile_validation_disabled: bool,
}

impl MetadataHandleOptions {
    pub fn with_search_storage(mut self, search_storage: MetadataSearchStorage) -> Self {
        self.search_storage = search_storage;
        self
    }

    pub fn with_document_sync_persist_policy(mut self, persist_policy: FjallPersistPolicy) -> Self {
        self.document_sync_persist_policy = persist_policy;
        self
    }

    pub fn with_backend_pool_size(mut self, backend_pool_size: usize) -> Self {
        self.backend_pool_size = Some(backend_pool_size.max(1));
        self
    }

    pub fn with_profile_validation_disabled(mut self, disabled: bool) -> Self {
        self.profile_validation_disabled = disabled;
        self
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MetadataSearchStorage {
    #[default]
    Disk,
    Memory,
}

impl From<MetadataSearchStorage> for SearchStorage {
    fn from(search_storage: MetadataSearchStorage) -> Self {
        match search_storage {
            MetadataSearchStorage::Disk => SearchStorage::Disk,
            MetadataSearchStorage::Memory => SearchStorage::Memory,
        }
    }
}

struct MetadataInner {
    node: Arc<CraqleNode>,
    storage_handle: StorageHandle,
    auth_validation: MetadataAuthValidationState,
    net_handle: Option<NetHandle>,
    document_sync_db: Option<fjall::OptimisticTxDatabase>,
    document_sync_persist_policy: FjallPersistPolicy,
    visibility_cache: MetadataVisibilityCache,
    query_cache: MetadataQueryCache,
    profile_cache: ProfileCache,
    craqle_permits: Arc<tokio::sync::Semaphore>,
    craqle_read_permits: Arc<tokio::sync::Semaphore>,
    inbound_frame_bytes: Arc<tokio::sync::Semaphore>,
    deferred_persist_requested: AtomicBool,
    deferred_persist_running: AtomicBool,
    profile_validation_disabled: bool,
    profile_shacl: Option<Arc<ProfileShaclEngine>>,
    peer_contacts: PeerContacts,
}

#[derive(Clone)]
struct MetadataAuthValidationState {
    storage_handle: StorageHandle,
    /// Realm this node serves; without one there is no replicated revocation
    /// set to consult, and this node serves no remote metadata peers either.
    realm_id: Option<RealmId>,
    issuer_keys: Arc<IssuerKeyCache>,
}

impl MetadataAuthValidationState {
    fn new(storage_handle: StorageHandle, realm_id: Option<RealmId>) -> Self {
        Self {
            storage_handle,
            realm_id,
            issuer_keys: Arc::new(IssuerKeyCache::new()),
        }
    }
}

#[async_trait]
impl ArunaBearerTokenValidationState for MetadataAuthValidationState {
    async fn is_token_revoked(
        &self,
        realm_id: &RealmId,
        token_hash: &str,
    ) -> Result<bool, ArunaBearerTokenError> {
        // Without realm state this node holds no revocation authority at all;
        // once it has any, the token's own issuing realm answers.
        match self.realm_id {
            Some(_) => realm_token_revoked(&self.storage_handle, *realm_id, token_hash).await,
            None => Ok(false),
        }
    }

    async fn is_trusted_realm(&self, realm_id: &RealmId) -> bool {
        match load_metadata_auth_state::<HashSet<RealmId>>(
            &self.storage_handle,
            TRUSTED_REALMS_LIST_KEY,
        )
        .await
        {
            Ok(trusted) => trusted.contains(realm_id),
            Err(error) => {
                warn!(error = %error, "Failed to read metadata trusted realms state");
                false
            }
        }
    }

    async fn issuer_decoding_key(
        &self,
        issuer_pubkey: &str,
    ) -> Result<DecodingKey, ArunaBearerTokenError> {
        self.issuer_keys.get_or_insert(issuer_pubkey).await
    }
}

struct RevocationBlindValidation<'a>(&'a MetadataAuthValidationState);

#[async_trait]
impl ArunaBearerTokenValidationState for RevocationBlindValidation<'_> {
    async fn is_token_revoked(
        &self,
        _realm_id: &RealmId,
        _token_hash: &str,
    ) -> Result<bool, ArunaBearerTokenError> {
        Ok(false)
    }

    async fn is_trusted_realm(&self, realm_id: &RealmId) -> bool {
        self.0.is_trusted_realm(realm_id).await
    }

    async fn issuer_decoding_key(
        &self,
        issuer_pubkey: &str,
    ) -> Result<DecodingKey, ArunaBearerTokenError> {
        self.0.issuer_decoding_key(issuer_pubkey).await
    }
}

struct MetadataVisibilityCache {
    registry: Mutex<Option<RegistryCacheEntry>>,
    registry_fill: Arc<tokio::sync::Mutex<()>>,
    lifecycle_deleted: Mutex<HashMap<String, LifecycleDeletedCacheEntry>>,
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
        if self.records.len() > METADATA_REGISTRY_CANDIDATE_LIMIT {
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
            return (records.len() <= METADATA_REGISTRY_CANDIDATE_LIMIT).then(|| records.clone());
        }
        let mut group_records = Vec::new();
        for record in self
            .records
            .values()
            .filter(|record| record.group_id == group_id)
        {
            if group_records.len() == METADATA_REGISTRY_CANDIDATE_LIMIT {
                return None;
            }
            group_records.push(record.clone());
        }
        let records = Arc::new(group_records);
        self.group_snapshots.insert(group_id, records.clone());
        Some(records)
    }
}

struct LifecycleDeletedCacheEntry {
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
    pub fn new(
        path: impl AsRef<Path>,
        node_id: NodeId,
        storage_handle: StorageHandle,
        net_handle: Option<NetHandle>,
        document_sync_node: Option<irokle::Irokle<irokle::FjallStorage>>,
        document_sync_db: Option<fjall::OptimisticTxDatabase>,
    ) -> Result<Self, MetadataError> {
        Self::new_with_options(
            path,
            node_id,
            storage_handle,
            net_handle,
            document_sync_node,
            document_sync_db,
            MetadataHandleOptions::default(),
        )
    }

    pub fn new_with_options(
        path: impl AsRef<Path>,
        node_id: NodeId,
        storage_handle: StorageHandle,
        net_handle: Option<NetHandle>,
        document_sync_node: Option<irokle::Irokle<irokle::FjallStorage>>,
        document_sync_db: Option<fjall::OptimisticTxDatabase>,
        metadata_options: MetadataHandleOptions,
    ) -> Result<Self, MetadataError> {
        let path = path.as_ref();
        let actor = ActorId::from_bytes(*node_id.as_bytes());
        let options = CraqleOptions::new()
            .with_actor(actor)
            .with_search_storage(metadata_options.search_storage.into())
            .with_graph_store_persist_mode(craqle_fjall_persist_mode(
                metadata_options.document_sync_persist_policy,
            ));
        let options = match document_sync_node {
            Some(document_sync_node) => {
                options.with_irokle(document_sync_node, CraqleIrokleOptions::new())
            }
            None => options,
        };
        let node = CraqleNode::open_with_options(path, options)
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        let profile_shacl = if metadata_options.profile_validation_disabled {
            None
        } else {
            Some(Arc::new(
                ProfileShaclEngine::open(&path.join("profile-validation"))
                    .map_err(|error| MetadataError::Backend(error.to_string()))?,
            ))
        };
        let pool_size = metadata_options.backend_pool_size.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|cores| cores.get())
                .unwrap_or(4)
                .max(4)
        });
        Ok(Self {
            inner: Arc::new(MetadataInner {
                node: Arc::new(node),
                auth_validation: MetadataAuthValidationState::new(
                    storage_handle.clone(),
                    net_handle.as_ref().map(|net| *net.realm_id()),
                ),
                storage_handle,
                net_handle,
                document_sync_db,
                document_sync_persist_policy: metadata_options.document_sync_persist_policy,
                visibility_cache: MetadataVisibilityCache::new(),
                query_cache: MetadataQueryCache::new(),
                profile_cache: ProfileCache::new(),
                craqle_permits: Arc::new(tokio::sync::Semaphore::new(pool_size)),
                craqle_read_permits: Arc::new(tokio::sync::Semaphore::new(pool_size)),
                inbound_frame_bytes: Arc::new(tokio::sync::Semaphore::new(
                    super::protocol::METADATA_INBOUND_FRAME_BYTES,
                )),
                deferred_persist_requested: AtomicBool::new(false),
                deferred_persist_running: AtomicBool::new(false),
                profile_validation_disabled: metadata_options.profile_validation_disabled,
                profile_shacl,
                peer_contacts: PeerContacts::default(),
            }),
            storage_priority: StoragePriority::Foreground,
        })
    }

    /// A handle whose own storage reads dispatch on the bulk lane.
    pub fn bulk(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            storage_priority: StoragePriority::Bulk,
        }
    }

    fn lifecycle_storage(&self) -> StorageHandle {
        match self.storage_priority {
            StoragePriority::Foreground => self.inner.storage_handle.clone(),
            StoragePriority::Bulk => self.inner.storage_handle.bulk(),
        }
    }

    pub fn upsert_cached_registry_record(&self, record: MetadataRegistryRecord) {
        self.inner
            .visibility_cache
            .upsert_registry_records(std::slice::from_ref(&record));
    }

    pub fn upsert_cached_registry_records(&self, records: &[MetadataRegistryRecord]) {
        self.inner.visibility_cache.upsert_registry_records(records);
    }

    pub(crate) fn upsert_cached_at(&self, record: MetadataRegistryRecord, generation: u64) {
        self.inner
            .visibility_cache
            .upsert_at(std::slice::from_ref(&record), Some(generation));
    }

    pub fn remove_cached_registry_record(&self, document_id: Ulid) {
        self.inner
            .visibility_cache
            .remove_registry_record(document_id);
    }

    pub async fn list_cached_registry_records(
        &self,
    ) -> Result<Arc<Vec<MetadataRegistryRecord>>, MetadataError> {
        list_local_registry_records(self.inner.clone()).await
    }

    pub async fn list_cached_registry_records_for_group(
        &self,
        group_id: GroupId,
    ) -> Result<Arc<Vec<MetadataRegistryRecord>>, MetadataError> {
        list_local_registry_records_for_group(self.inner.clone(), group_id).await
    }

    pub async fn list_group_records(
        &self,
        group_id: GroupId,
        limit: usize,
    ) -> Result<Arc<Vec<MetadataRegistryRecord>>, MetadataError> {
        list_group_records(self.inner.clone(), group_id, limit).await
    }

    pub(crate) async fn snapshot_iri_references(
        &self,
        graph_iri: String,
    ) -> Result<Vec<(String, String, String)>, MetadataError> {
        let inner = self.inner.clone();
        let _permit = inner.craqle_read_permits.clone().acquire_owned().await.ok();
        tokio::task::spawn_blocking(move || {
            snapshot_iri_references(&inner.node, &GraphId::new(&graph_iri))
        })
        .await
        .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
    }

    /// Best-effort properties of a document's root subject (its graph IRI), for
    /// title enrichment. Returns empty on any backend error so a lookup never
    /// fails on a pending or raced projection.
    pub(crate) async fn describe_root_properties(&self, graph_iri: String) -> Vec<(String, Term)> {
        let inner = self.inner.clone();
        let _permit = inner.craqle_read_permits.clone().acquire_owned().await.ok();
        tokio::task::spawn_blocking(move || {
            let authorizer = AllowedGraphAuthorizer {
                graph_iris: HashSet::from([graph_iri.clone()]),
            };
            describe_hit_properties(&inner.node, &authorizer, &graph_iri, &graph_iri)
        })
        .await
        .unwrap_or_default()
    }

    pub(super) fn query_cache(&self) -> &MetadataQueryCache {
        &self.inner.query_cache
    }

    pub(super) fn profile_cache(&self) -> &ProfileCache {
        &self.inner.profile_cache
    }

    /// Profile revisions this node fetched for validation since start. A
    /// second validation of the same revision must not raise it.
    pub fn profile_loads(&self) -> u64 {
        self.inner.profile_cache.loads()
    }

    pub(crate) fn visibility_generation(&self) -> u64 {
        self.inner.visibility_cache.current_generation()
    }

    pub(crate) fn profile_validation_available(&self) -> bool {
        !self.inner.profile_validation_disabled
    }

    /// Structural RO-Crate findings for a candidate that carries no Profile
    /// tag. Nothing is committed: the document is only prepared.
    pub(crate) async fn preview_crate_structure(
        &self,
        jsonld: String,
    ) -> Result<Vec<CrateViolation>, ProfileShaclError> {
        self.run_profile_shacl(move |engine| engine.structural(&jsonld))
            .await
    }

    /// Evaluates one candidate against a registered Profile's shapes.
    pub(crate) async fn evaluate_profile_shapes(
        &self,
        profile: ProfileShapes,
        jsonld: String,
    ) -> Result<ProfileShaclReport, ProfileShaclError> {
        self.run_profile_shacl(move |engine| engine.evaluate(&profile, &jsonld))
            .await
    }

    async fn run_profile_shacl<T, F>(&self, work: F) -> Result<T, ProfileShaclError>
    where
        T: Send + 'static,
        F: FnOnce(&ProfileShaclEngine) -> Result<T, ProfileShaclError> + Send + 'static,
    {
        let Some(engine) = self.inner.profile_shacl.clone() else {
            return Err(ProfileShaclError::Unavailable {
                message: "the profile evaluator is unavailable; retry or remove the Profile tag"
                    .to_string(),
            });
        };
        let _permit = self.inner.craqle_permits.clone().acquire_owned().await.ok();
        tokio::task::spawn_blocking(move || work(engine.as_ref()))
            .await
            .map_err(|error| ProfileShaclError::Unavailable {
                message: error.to_string(),
            })?
    }

    /// Test hook: marks all visibility cache entries as expired so the next
    /// read exercises the stale-serving + background-refill path.
    #[doc(hidden)]
    pub fn expire_visibility_caches(&self) {
        self.inner.visibility_cache.expire_now();
    }

    /// Primes the visibility cache and craqle query indexes so the first
    /// query after boot finds everything warm.
    pub async fn warm_caches(&self) -> Result<(), MetadataError> {
        let node = self.inner.node.clone();
        tokio::task::spawn_blocking(move || node.ensure_query_indexes())
            .await
            .map_err(|error| MetadataError::TaskJoin(error.to_string()))?;
        if self.inner.visibility_cache.registry_records_any().is_none() {
            let _fill = self
                .inner
                .visibility_cache
                .registry_fill
                .clone()
                .lock_owned()
                .await;
            if self.inner.visibility_cache.registry_records_any().is_none() {
                fill_visibility_caches(&self.inner).await?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(
        name = "metadata.remote.inbound",
        level = "debug",
        skip(self, stream),
        fields(
            peer = ?peer,
            request = field::Empty,
            response = field::Empty,
            read_ms = field::Empty,
            process_ms = field::Empty,
            drain_ms = field::Empty,
            write_ms = field::Empty,
            elapsed_ms = field::Empty,
        )
    )]
    pub async fn handle_inbound_stream(
        &self,
        context: &Arc<DriverContext>,
        mut stream: BiStream,
        peer: NodeId,
        metadata_bytes: u64,
    ) -> Result<(), MetadataError> {
        let total_started = Instant::now();
        let audit_deadline =
            tokio::time::Instant::now() + Duration::from_secs(super::audit::AUDIT_DEADLINE_SECS);
        let read_started = Instant::now();
        let (message, frame_budget) =
            read_budget(&mut stream.1, &self.inner.inbound_frame_bytes).await?;
        let is_audit = matches!(&message, MetadataTransportMessage::ForwardAuditPage { .. });
        let span = Span::current();
        record_elapsed_ms(&span, "read_ms", read_started);
        span.record("request", transport_message_kind(&message));

        let process_started = Instant::now();
        let mut response_body = None;
        // Debug poll frames reserve stack for every arm at once, so each awaiting
        // arm is boxed to keep only the active one on the stack.
        let response = match message {
            MetadataTransportMessage::QueryGraphs {
                auth_token,
                graph_iris,
                sparql,
            } => {
                Box::pin(async {
                    match self.authorize_read_peer(peer, auth_token, false).await {
                        Ok(auth_context) => {
                            match query_local_graphs(
                                self.inner.clone(),
                                auth_context,
                                graph_iris,
                                sparql,
                            )
                            .await
                            {
                                Ok(results) => MetadataTransportMessage::QueryResults {
                                    result: Ok(results),
                                },
                                Err(error) => MetadataTransportMessage::QueryResults {
                                    result: Err(metadata_read_error(error)),
                                },
                            }
                        }
                        Err(error) => MetadataTransportMessage::QueryResults { result: Err(error) },
                    }
                })
                .await
            }
            MetadataTransportMessage::SearchGraphs {
                auth_token,
                graph_iris,
                query,
                limit,
                group_id,
            } => {
                Box::pin(async {
                    match self.authorize_read_peer(peer, auth_token, false).await {
                        Ok(auth_context) => match search_local_graphs(
                            self.inner.clone(),
                            auth_context,
                            graph_iris,
                            query,
                            clamp_remote_search_graph_limit(limit),
                            group_id,
                            None,
                        )
                        .await
                        {
                            Ok(hits) => {
                                MetadataTransportMessage::SearchResults { result: Ok(hits) }
                            }
                            Err(error) => MetadataTransportMessage::SearchResults {
                                result: Err(metadata_read_error(error)),
                            },
                        },
                        Err(error) => {
                            MetadataTransportMessage::SearchResults { result: Err(error) }
                        }
                    }
                })
                .await
            }
            MetadataTransportMessage::FilteredSearchGraphs {
                auth_token,
                graph_iris,
                query,
                limit,
                predicate_iri,
                object_iri,
                group_id,
            } => {
                Box::pin(async {
                    match self.authorize_read_peer(peer, auth_token, false).await {
                        Ok(auth_context) => match search_local_graphs(
                            self.inner.clone(),
                            auth_context,
                            graph_iris,
                            query,
                            clamp_remote_search_graph_limit(limit),
                            group_id,
                            Some((predicate_iri, object_iri)),
                        )
                        .await
                        {
                            Ok(hits) => {
                                MetadataTransportMessage::SearchResults { result: Ok(hits) }
                            }
                            Err(error) => MetadataTransportMessage::SearchResults {
                                result: Err(metadata_read_error(error)),
                            },
                        },
                        Err(error) => {
                            MetadataTransportMessage::SearchResults { result: Err(error) }
                        }
                    }
                })
                .await
            }
            MetadataTransportMessage::ReferencePreflight {
                auth_token,
                request,
            } => {
                Box::pin(async {
                    let result = match self.authorize_read_peer(peer, auth_token, false).await {
                        Ok(auth) => match context.net_handle.as_ref() {
                            Some(net) => {
                                let endpoint = crate::node::node_info::read_node_info_document(
                                    &context.storage_handle,
                                    net.node_id(),
                                )
                                .await
                                .ok()
                                .flatten()
                                .and_then(|document| document.urls.s3);
                                super::api::references_preflight_local(
                                    context.as_ref(),
                                    *net.realm_id(),
                                    net.node_id(),
                                    auth,
                                    *request,
                                    endpoint,
                                )
                                .await
                                .map(Box::new)
                                .map_err(super::forward::read_error)
                            }
                            None => Err(MetadataReadError::Unavailable),
                        },
                        Err(error) => Err(error),
                    };
                    MetadataTransportMessage::ReferencePreflightResults { result }
                })
                .await
            }
            MetadataTransportMessage::SearchBuckets {
                auth_token,
                query,
                limit,
            } => {
                Box::pin(async {
                    match bucket_search_auth(
                        &self.inner.auth_validation,
                        &self.inner.storage_handle,
                        peer,
                        self.inner.net_handle.as_ref().map(|net| *net.realm_id()),
                        auth_token,
                    )
                    .await
                    {
                        Ok(auth) => match self.inner.net_handle.as_ref() {
                            Some(net_handle) => match search_local_buckets(
                                context.as_ref(),
                                SearchBucketsInput {
                                    auth,
                                    realm_id: *net_handle.realm_id(),
                                    node_id: net_handle.node_id(),
                                    query,
                                    limit,
                                    start_after: None,
                                },
                            )
                            .await
                            {
                                Ok(hits) => MetadataTransportMessage::BucketSearchResults {
                                    result: Ok(hits),
                                },
                                Err(_) => MetadataTransportMessage::BucketSearchResults {
                                    result: Err(MetadataReadError::Unavailable),
                                },
                            },
                            None => MetadataTransportMessage::BucketSearchResults {
                                result: Err(MetadataReadError::Unavailable),
                            },
                        },
                        Err(error) => {
                            MetadataTransportMessage::BucketSearchResults { result: Err(error) }
                        }
                    }
                })
                .await
            }
            MetadataTransportMessage::SearchObjects {
                auth_token,
                query,
                key_match,
                bucket,
                limit,
                start_after,
                as_of,
            } => {
                Box::pin(async {
                    match bucket_search_auth(
                        &self.inner.auth_validation,
                        &self.inner.storage_handle,
                        peer,
                        self.inner.net_handle.as_ref().map(|net| *net.realm_id()),
                        auth_token,
                    )
                    .await
                    {
                        Ok(auth) => match self.inner.net_handle.as_ref() {
                            Some(net_handle) => match search_local_objects(
                                context.as_ref(),
                                SearchObjectsInput {
                                    auth,
                                    realm_id: *net_handle.realm_id(),
                                    node_id: net_handle.node_id(),
                                    query,
                                    key_match,
                                    bucket,
                                    limit,
                                    start_after,
                                    as_of,
                                },
                            )
                            .await
                            {
                                Ok(page) => MetadataTransportMessage::ObjectSearchResults {
                                    result: Ok(page),
                                },
                                Err(_) => MetadataTransportMessage::ObjectSearchResults {
                                    result: Err(MetadataReadError::Unavailable),
                                },
                            },
                            None => MetadataTransportMessage::ObjectSearchResults {
                                result: Err(MetadataReadError::Unavailable),
                            },
                        },
                        Err(error) => {
                            MetadataTransportMessage::ObjectSearchResults { result: Err(error) }
                        }
                    }
                })
                .await
            }
            MetadataTransportMessage::CreateSyncMirror {
                auth_token,
                source_group_id,
                relationship,
                extras,
            } => {
                Box::pin(async {
                    self.apply_sync_mirror(
                        context,
                        peer,
                        auth_token,
                        *relationship,
                        Some(source_group_id),
                        false,
                        extras,
                    )
                    .await
                })
                .await
            }
            MetadataTransportMessage::DeleteSyncMirror {
                auth_token,
                relationship,
                extras,
            } => {
                Box::pin(async {
                    self.apply_sync_mirror(
                        context,
                        peer,
                        auth_token,
                        *relationship,
                        None,
                        true,
                        extras,
                    )
                    .await
                })
                .await
            }
            query @ MetadataTransportMessage::QueryDocument { .. } => {
                Box::pin(async {
                    let result = super::forward::apply_document_query(context, peer, query).await;
                    MetadataTransportMessage::DocumentQueryResults { result }
                })
                .await
            }
            MetadataTransportMessage::ForwardPathLookup {
                auth_token,
                group_id,
                document_path,
                config_digest,
            } => {
                Box::pin(async {
                    let result = match self.authorize_read_peer(peer, auth_token, true).await {
                        Ok(auth) => match context.net_handle.as_ref() {
                            Some(net) => {
                                let realm_id = *net.realm_id();
                                if !config_digest_matches(
                                    context.as_ref(),
                                    realm_id,
                                    &config_digest,
                                )
                                .await
                                {
                                    Err(MetadataReadError::Unavailable)
                                } else {
                                    let result = super::api::local_path_candidates(
                                        context.as_ref(),
                                        realm_id,
                                        group_id,
                                        &document_path,
                                        auth.as_ref(),
                                    )
                                    .await
                                    .map_err(super::forward::read_error);
                                    if !config_digest_matches(
                                        context.as_ref(),
                                        realm_id,
                                        &config_digest,
                                    )
                                    .await
                                    {
                                        Err(MetadataReadError::Unavailable)
                                    } else {
                                        result
                                    }
                                }
                            }
                            None => Err(MetadataReadError::Unavailable),
                        },
                        Err(error) => Err(error),
                    };
                    MetadataTransportMessage::ForwardedPathLookup { result }
                })
                .await
            }
            MetadataTransportMessage::ForwardPathResolution {
                auth_token,
                group_id,
                document_path,
                config_digest,
            } => {
                Box::pin(async {
                    let result = match self.authorize_read_peer(peer, auth_token, false).await {
                        Ok(auth) => match context.net_handle.as_ref() {
                            Some(net) => {
                                let realm_id = *net.realm_id();
                                if !config_digest_matches(
                                    context.as_ref(),
                                    realm_id,
                                    &config_digest,
                                )
                                .await
                                {
                                    Err(MetadataReadError::Unavailable)
                                } else {
                                    let result = super::api::resolve_local_path(
                                        context.as_ref(),
                                        realm_id,
                                        super::api::MetadataPathLookupRequest {
                                            group_id,
                                            document_path,
                                            auth,
                                        },
                                    )
                                    .await
                                    .map(|result| {
                                        Box::new(super::protocol::MetadataPathResolution {
                                            winner: result.winner,
                                            conflicts: result.conflicts,
                                        })
                                    })
                                    .map_err(super::forward::read_error);
                                    if !config_digest_matches(
                                        context.as_ref(),
                                        realm_id,
                                        &config_digest,
                                    )
                                    .await
                                    {
                                        Err(MetadataReadError::Unavailable)
                                    } else {
                                        result
                                    }
                                }
                            }
                            None => Err(MetadataReadError::Unavailable),
                        },
                        Err(error) => Err(error),
                    };
                    MetadataTransportMessage::ForwardedPathResolution { result }
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardExportDocument { .. } => {
                Box::pin(async {
                    match super::forward::apply_forwarded_export(
                        context,
                        peer,
                        forward,
                        metadata_bytes,
                    )
                    .await
                    {
                        Ok((export, metadata_bytes)) => match postcard::to_allocvec(&export) {
                            Ok(bytes) => {
                                let length = bytes.len() as u64;
                                if length > metadata_body_limit(metadata_bytes) {
                                    MetadataTransportMessage::ForwardedExport {
                                        result: Err(MetadataReadError::Unavailable),
                                    }
                                } else {
                                    response_body = Some(bytes);
                                    MetadataTransportMessage::ForwardedExport { result: Ok(length) }
                                }
                            }
                            Err(_) => MetadataTransportMessage::ForwardedExport {
                                result: Err(MetadataReadError::Unavailable),
                            },
                        },
                        Err(error) => {
                            MetadataTransportMessage::ForwardedExport { result: Err(error) }
                        }
                    }
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardExportProfile { .. } => {
                Box::pin(async {
                    match super::forward::apply_forwarded_profile(
                        context,
                        peer,
                        forward,
                        metadata_bytes,
                    )
                    .await
                    {
                        Ok((export, metadata_bytes)) => match postcard::to_allocvec(&export) {
                            Ok(bytes) => {
                                let length = bytes.len() as u64;
                                if length > metadata_body_limit(metadata_bytes) {
                                    MetadataTransportMessage::ForwardedExport {
                                        result: Err(MetadataReadError::Unavailable),
                                    }
                                } else {
                                    response_body = Some(bytes);
                                    MetadataTransportMessage::ForwardedExport { result: Ok(length) }
                                }
                            }
                            Err(_) => MetadataTransportMessage::ForwardedExport {
                                result: Err(MetadataReadError::Unavailable),
                            },
                        },
                        Err(error) => {
                            MetadataTransportMessage::ForwardedExport { result: Err(error) }
                        }
                    }
                })
                .await
            }
            forward @ (MetadataTransportMessage::ForwardCreateDocument { .. }
            | MetadataTransportMessage::ForwardUpdateDocument { .. }
            | MetadataTransportMessage::ForwardDeleteDocument { .. }
            | MetadataTransportMessage::ForwardReadDocument { .. }
            | MetadataTransportMessage::ForwardProfileValidationStatus { .. }) => {
                Box::pin(async {
                    super::forward::apply_forwarded_write(context, peer, forward).await
                })
                .await
            }
            MetadataTransportMessage::ForwardAuditPage { request } => {
                Box::pin(async {
                    super::audit::serve_local_audit(context, peer, request, audit_deadline).await
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardTokenRevocation { .. } => {
                Box::pin(async { super::forward::apply_token_revoke(context, peer, forward).await })
                    .await
            }
            forward @ MetadataTransportMessage::ForwardPersistentId { .. } => {
                Box::pin(async {
                    super::forward::apply_forwarded_pid(context, peer, forward).await
                })
                .await
            }
            MetadataTransportMessage::ForwardPlacementPolicy { policy_ref } => {
                Box::pin(async {
                    crate::placement::policy::serve_local_policy(context, peer, policy_ref).await
                })
                .await
            }
            record @ (MetadataTransportMessage::ForwardJobRecord { .. }
            | MetadataTransportMessage::ForwardJobRecordPage { .. }) => {
                Box::pin(async {
                    crate::jobs::records::serve_job_record(context, peer, record).await
                })
                .await
            }
            MetadataTransportMessage::ForwardLaunchOffer { launch } => {
                Box::pin(async {
                    crate::jobs::records::serve_launch_offer(context, peer, *launch).await
                })
                .await
            }
            MetadataTransportMessage::ForwardJobSubmission {
                auth_token,
                submission_id,
                request,
            } => {
                Box::pin(async {
                    crate::jobs::lifecycle::ingress::serve_submission(
                        context,
                        peer,
                        auth_token,
                        submission_id,
                        *request,
                    )
                    .await
                })
                .await
            }
            forward @ MetadataTransportMessage::ForwardCreatePlacementPolicy { .. } => {
                Box::pin(async {
                    crate::placement::policy::apply_forwarded_policy(context, peer, forward).await
                })
                .await
            }
            pull @ MetadataTransportMessage::ForwardSyncPull { .. } => {
                Box::pin(async { super::device_pull::serve_sync_pull(context, peer, pull).await })
                    .await
            }
            listing @ MetadataTransportMessage::ForwardListVersions { .. } => {
                Box::pin(async {
                    super::device_pull::serve_list_versions(context, peer, listing).await
                })
                .await
            }
            create @ MetadataTransportMessage::ForwardCreateBucket { .. } => {
                Box::pin(async { super::forward::apply_bucket_create(context, peer, create).await })
                    .await
            }
            fetch @ MetadataTransportMessage::FetchRealmDocuments { .. } => {
                Box::pin(async {
                    super::forward::serve_realm_documents(context, peer, fetch).await
                })
                .await
            }
            fetch @ MetadataTransportMessage::FetchGraphState { .. } => {
                Box::pin(async { super::forward::serve_graph_state(context, peer, fetch).await })
                    .await
            }
            forward @ MetadataTransportMessage::ForwardApplyBatch { .. } => {
                Box::pin(async { super::forward::apply_device_batch(context, peer, forward).await })
                    .await
            }
            forward @ MetadataTransportMessage::ForwardAdminEvent { .. } => {
                Box::pin(async { super::forward::apply_admin_relay(context, peer, forward).await })
                    .await
            }
            forward @ MetadataTransportMessage::ForwardGroupCreate { .. } => {
                Box::pin(async { super::forward::apply_group_create(context, peer, forward).await })
                    .await
            }
            MetadataTransportMessage::QueryResults { .. }
            | MetadataTransportMessage::SearchResults { .. }
            | MetadataTransportMessage::BucketSearchResults { .. }
            | MetadataTransportMessage::ObjectSearchResults { .. }
            | MetadataTransportMessage::SyncMirrorCreated
            | MetadataTransportMessage::SyncMirrorDeleted
            | MetadataTransportMessage::ForwardedRecord { .. }
            | MetadataTransportMessage::ForwardedRead { .. }
            | MetadataTransportMessage::ForwardedPathLookup { .. }
            | MetadataTransportMessage::ForwardedPathResolution { .. }
            | MetadataTransportMessage::ForwardedWriteDenied { .. }
            | MetadataTransportMessage::ForwardedWriteNotFound
            | MetadataTransportMessage::ForwardedWriteUnavailable
            | MetadataTransportMessage::ForwardedDelete
            | MetadataTransportMessage::ForwardedUpdateInvalidInput { .. }
            | MetadataTransportMessage::ForwardedExport { .. }
            | MetadataTransportMessage::DocumentQueryResults { .. }
            | MetadataTransportMessage::ForwardedAuditPage { .. }
            | MetadataTransportMessage::ForwardedTokenRevoked
            | MetadataTransportMessage::ForwardedTokenRevocationCapacity
            | MetadataTransportMessage::ForwardedMetadataHistoryCapacity
            | MetadataTransportMessage::ForwardedPersistentId { .. }
            | MetadataTransportMessage::ForwardedPlacementPolicy { .. }
            | MetadataTransportMessage::ForwardedPlacementPolicyCreated { .. }
            | MetadataTransportMessage::ForwardedJobRecord { .. }
            | MetadataTransportMessage::ForwardedJobRecordPage { .. }
            | MetadataTransportMessage::ForwardedLaunchOffer { .. }
            | MetadataTransportMessage::ForwardedJobSubmission { .. }
            | MetadataTransportMessage::ForwardedProfileValidation { .. }
            | MetadataTransportMessage::ForwardedProfileValidationStatus { .. }
            | MetadataTransportMessage::ReferencePreflightResults { .. }
            | MetadataTransportMessage::ForwardedAdminEventQueued
            | MetadataTransportMessage::ForwardedGroupCreated { .. }
            | MetadataTransportMessage::ForwardedGroupCreateConflict { .. }
            | MetadataTransportMessage::ForwardedSyncPull { .. }
            | MetadataTransportMessage::ForwardedVersions { .. }
            | MetadataTransportMessage::ForwardedBucketCreated { .. }
            | MetadataTransportMessage::FetchedRealmDocuments { .. }
            | MetadataTransportMessage::FetchedGraphState { .. }
            | MetadataTransportMessage::ForwardedApplyBatch { .. }
            | MetadataTransportMessage::Reject(_) => {
                MetadataTransportMessage::Reject("unexpected metadata control message".to_string())
            }
        };
        record_elapsed_ms(&span, "process_ms", process_started);

        let drain_started = Instant::now();
        let drain_result = if is_audit {
            drain_stream_at(&mut stream.1, audit_deadline).await
        } else {
            drain_request_stream(&mut stream).await
        };
        if let Err(error) = drain_result {
            if is_audit {
                close_stream_at(&mut stream, audit_deadline);
            }
            return Err(error);
        }
        record_elapsed_ms(&span, "drain_ms", drain_started);

        let write_started = Instant::now();
        let response_written = if is_audit {
            write_message_at(&mut stream, &response, audit_deadline)
                .await
                .is_ok()
        } else {
            write_transport_message(&mut stream, &response)
                .await
                .is_ok()
        };
        if response_written && let Some(body) = response_body {
            if is_audit {
                let _ = write_body_at(&mut stream, &body, audit_deadline).await;
            } else {
                let _ = write_stream_body(&mut stream, &body).await;
            }
        }
        record_elapsed_ms(&span, "write_ms", write_started);
        if is_audit {
            close_stream_at(&mut stream, audit_deadline);
        } else {
            close_stream(&mut stream).await;
        }
        drop(frame_budget);
        record_elapsed_ms(&span, "elapsed_ms", total_started);
        span.record("response", transport_message_kind(&response));
        Ok(())
    }

    pub(crate) async fn request_export(
        &self,
        node_id: NodeId,
        message: MetadataTransportMessage,
    ) -> Result<
        Result<super::api::ExportMetadataRoCrateResult, MetadataReadError>,
        MetadataRequestError,
    > {
        send_export_request(&self.inner, node_id, message).await
    }

    #[tracing::instrument(
        name = "metadata.query.local_authorized",
        level = "debug",
        skip(self, auth_context, sparql),
        fields(
            query_len = sparql.len() as u64,
            graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        )
    )]
    pub async fn query_authorized_local(
        &self,
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        sparql: String,
    ) -> Result<MetadataQueryResults, MetadataError> {
        query_local_graphs(self.inner.clone(), auth_context, graph_iris, sparql).await
    }

    #[tracing::instrument(
        name = "metadata.search.local_authorized",
        level = "debug",
        skip(self, auth_context, query),
        fields(
            query_len = query.len() as u64,
            limit = limit as u64,
            graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        )
    )]
    pub async fn search_authorized_local(
        &self,
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
        group_id: Option<GroupId>,
    ) -> Result<Vec<MetadataSearchHit>, MetadataError> {
        search_local_graphs(
            self.inner.clone(),
            auth_context,
            graph_iris,
            query,
            limit,
            group_id,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn search_authorized_local_filtered(
        &self,
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
        predicate_iri: String,
        object_iri: String,
        group_id: Option<GroupId>,
    ) -> Result<Vec<MetadataSearchHit>, MetadataError> {
        search_local_graphs(
            self.inner.clone(),
            auth_context,
            graph_iris,
            query,
            limit,
            group_id,
            Some((predicate_iri, object_iri)),
        )
        .await
    }

    pub async fn export_rocrate_jsonld(&self, graph_iri: String) -> Result<String, MetadataError> {
        match self
            .send_metadata_effect(MetadataEffect::ExportRoCrate { graph_iri })
            .await
        {
            Event::Metadata(MetadataEvent::RoCrateExportResult { jsonld, .. }) => Ok(jsonld),
            Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error),
            other => Err(MetadataError::Backend(format!(
                "unexpected metadata export event: {other:?}"
            ))),
        }
    }

    pub async fn export_rocrate_summary_jsonld(
        &self,
        graph_iri: String,
    ) -> Result<String, MetadataError> {
        match self
            .send_metadata_effect(MetadataEffect::ExportRoCrateSummary { graph_iri })
            .await
        {
            Event::Metadata(MetadataEvent::RoCrateSummaryResult { jsonld, .. }) => Ok(jsonld),
            Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error),
            other => Err(MetadataError::Backend(format!(
                "unexpected metadata summary event: {other:?}"
            ))),
        }
    }

    pub async fn export_rocrate_page(
        &self,
        graph_iri: String,
        limit: usize,
        offset: Option<usize>,
        after: Option<String>,
    ) -> Result<MetadataRoCratePage, MetadataError> {
        match self
            .send_metadata_effect(MetadataEffect::ExportRoCratePage {
                graph_iri,
                limit,
                offset,
                after,
            })
            .await
        {
            Event::Metadata(MetadataEvent::RoCratePageResult { page, .. }) => Ok(page),
            Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error),
            other => Err(MetadataError::Backend(format!(
                "unexpected metadata page event: {other:?}"
            ))),
        }
    }

    pub async fn flush_search_updates(&self) -> Result<(), MetadataError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.node.flush_search_updates())
            .await
            .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
            .map_err(metadata_error_from_craqle)
    }

    pub async fn flush_persistence(&self) -> Result<(), MetadataError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || flush_metadata_persistence(&inner, "shutdown", None))
            .await
            .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
    }

    #[tracing::instrument(
        name = "metadata.query.remote",
        level = "debug",
        skip(self, auth_token, sparql),
        fields(
            peer = ?node_id,
            query_len = sparql.len() as u64,
            graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
            elapsed_ms = field::Empty,
            result = field::Empty,
            row_count = field::Empty,
            triple_count = field::Empty,
        )
        )]
    pub async fn request_remote_query_graphs(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        graph_iris: Option<Vec<String>>,
        sparql: String,
    ) -> Result<MetadataQueryResults, MetadataReadError> {
        let started = Instant::now();
        let span = Span::current();
        let result = match send_remote_metadata_request(
            &self.inner,
            &span,
            node_id,
            MetadataTransportMessage::QueryGraphs {
                auth_token,
                graph_iris,
                sparql,
            },
        )
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::QueryResults { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        };
        record_elapsed_ms(&span, "elapsed_ms", started);
        match &result {
            Ok(results) => {
                span.record("result", results.kind());
                record_metadata_query_result_counts(&span, results);
            }
            Err(error) => record_error(&span, &format!("{error:?}")),
        }
        result
    }

    pub(crate) async fn request_document_query(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        config_digest: [u8; 32],
        document_id: Ulid,
        sparql: String,
    ) -> Result<MetadataQueryResults, MetadataReadError> {
        match send_remote_metadata_request(
            &self.inner,
            &Span::current(),
            node_id,
            MetadataTransportMessage::QueryDocument {
                auth_token,
                config_digest,
                document_id,
                sparql,
            },
        )
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::DocumentQueryResults { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        }
    }

    #[tracing::instrument(
        name = "metadata.search.remote",
        level = "debug",
        skip(self, auth_token, query),
        fields(
            peer = ?node_id,
            query_len = query.len() as u64,
            limit = limit as u64,
            graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
            elapsed_ms = field::Empty,
            result = field::Empty,
            hit_count = field::Empty,
        )
    )]
    #[allow(clippy::too_many_arguments)]
    pub async fn request_remote_search_graphs(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
        group_id: Option<GroupId>,
    ) -> Result<Vec<MetadataSearchHit>, MetadataReadError> {
        self.request_remote_search_graphs_with_filter(
            node_id, auth_token, graph_iris, query, limit, group_id, None,
        )
        .await
    }

    #[tracing::instrument(
        name = "metadata.bucket_search.remote",
        level = "debug",
        skip(self, auth_token, query),
        fields(
            peer = ?node_id,
            query_len = query.len() as u64,
            limit = limit as u64,
            elapsed_ms = field::Empty,
            result = field::Empty,
            hit_count = field::Empty,
        )
    )]
    pub async fn request_bucket_search(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        query: String,
        limit: usize,
    ) -> Result<Vec<BucketSearchHit>, MetadataReadError> {
        let started = Instant::now();
        let span = Span::current();
        let result = match send_remote_metadata_request(
            &self.inner,
            &span,
            node_id,
            MetadataTransportMessage::SearchBuckets {
                auth_token,
                query,
                limit,
            },
        )
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::BucketSearchResults { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        };
        record_elapsed_ms(&span, "elapsed_ms", started);
        match &result {
            Ok(hits) => {
                span.record("result", "ok");
                span.record("hit_count", hits.len() as u64);
            }
            Err(error) => record_error(&span, &format!("{error:?}")),
        }
        result
    }

    #[tracing::instrument(
        name = "metadata.object_search.remote",
        level = "debug",
        skip(self, auth_token, query, start_after),
        fields(
            peer = ?node_id,
            query_len = query.len() as u64,
            limit = limit as u64,
            elapsed_ms = field::Empty,
            result = field::Empty,
            hit_count = field::Empty,
        )
    )]
    #[allow(clippy::too_many_arguments)]
    pub async fn request_object_search(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        query: String,
        key_match: ObjectKeyMatch,
        bucket: Option<String>,
        limit: usize,
        start_after: Option<Vec<u8>>,
        as_of: SystemTime,
    ) -> Result<ObjectSearchNodePage, MetadataReadError> {
        let started = Instant::now();
        let span = Span::current();
        let result = match send_remote_metadata_request(
            &self.inner,
            &span,
            node_id,
            MetadataTransportMessage::SearchObjects {
                auth_token,
                query,
                key_match,
                bucket,
                limit,
                start_after,
                as_of,
            },
        )
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::ObjectSearchResults { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        };
        record_elapsed_ms(&span, "elapsed_ms", started);
        match &result {
            Ok(page) => {
                span.record("result", "ok");
                span.record("hit_count", page.hits.len() as u64);
            }
            Err(error) => record_error(&span, &format!("{error:?}")),
        }
        result
    }

    pub async fn request_sync_create(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        source_group_id: GroupId,
        relationship: SyncRelationship,
        extras: PolicyRequestExtras,
    ) -> Result<(), MetadataError> {
        match with_sync_timeout(send_remote_metadata_request(
            &self.inner,
            &Span::current(),
            node_id,
            MetadataTransportMessage::CreateSyncMirror {
                auth_token,
                source_group_id,
                relationship: Box::new(relationship),
                extras,
            },
        ))
        .await?
        {
            MetadataTransportMessage::SyncMirrorCreated => Ok(()),
            MetadataTransportMessage::Reject(error) => Err(MetadataError::Backend(error)),
            other => Err(MetadataError::Backend(format!(
                "unexpected sync mirror response: {other:?}"
            ))),
        }
    }

    pub async fn request_sync_delete(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        relationship: SyncRelationship,
        extras: PolicyRequestExtras,
    ) -> Result<(), MetadataError> {
        match with_sync_timeout(send_remote_metadata_request(
            &self.inner,
            &Span::current(),
            node_id,
            MetadataTransportMessage::DeleteSyncMirror {
                auth_token,
                relationship: Box::new(relationship),
                extras,
            },
        ))
        .await?
        {
            MetadataTransportMessage::SyncMirrorDeleted => Ok(()),
            MetadataTransportMessage::Reject(error) => Err(MetadataError::Backend(error)),
            other => Err(MetadataError::Backend(format!(
                "unexpected sync mirror response: {other:?}"
            ))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn request_remote_filtered_search_graphs(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
        predicate_iri: String,
        object_iri: String,
        group_id: Option<GroupId>,
    ) -> Result<Vec<MetadataSearchHit>, MetadataReadError> {
        self.request_remote_search_graphs_with_filter(
            node_id,
            auth_token,
            graph_iris,
            query,
            limit,
            group_id,
            Some((predicate_iri, object_iri)),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn request_remote_search_graphs_with_filter(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
        group_id: Option<GroupId>,
        iri_filter: Option<(String, String)>,
    ) -> Result<Vec<MetadataSearchHit>, MetadataReadError> {
        let started = Instant::now();
        let span = Span::current();
        let message = match iri_filter {
            Some((predicate_iri, object_iri)) => MetadataTransportMessage::FilteredSearchGraphs {
                auth_token,
                graph_iris,
                query,
                limit,
                predicate_iri,
                object_iri,
                group_id,
            },
            None => MetadataTransportMessage::SearchGraphs {
                auth_token,
                graph_iris,
                query,
                limit,
                group_id,
            },
        };
        let result = match send_remote_metadata_request(&self.inner, &span, node_id, message)
            .await
            .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::SearchResults { result } => result,
            _ => Err(MetadataReadError::Unavailable),
        };
        record_elapsed_ms(&span, "elapsed_ms", started);
        match &result {
            Ok(hits) => {
                span.record("result", "ok");
                span.record("hit_count", hits.len() as u64);
            }
            Err(error) => record_error(&span, &format!("{error:?}")),
        }
        result
    }

    pub async fn request_remote_reference_preflight(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        request: super::api::MetadataReferencePreflightNodeRequest,
    ) -> Result<super::api::MetadataReferencePreflightNodeExecution, MetadataReadError> {
        match send_remote_metadata_request(
            &self.inner,
            &Span::current(),
            node_id,
            MetadataTransportMessage::ReferencePreflight {
                auth_token,
                request: Box::new(request),
            },
        )
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        {
            MetadataTransportMessage::ReferencePreflightResults { result } => {
                result.map(|result| *result)
            }
            _ => Err(MetadataReadError::Unavailable),
        }
    }
}

async fn remote_metadata_auth_context<S>(
    state: &S,
    auth_token: Option<MetadataAuthToken>,
) -> Result<Option<AuthContext>, MetadataError>
where
    S: ArunaBearerTokenValidationState + ?Sized,
{
    let Some(auth_token) = auth_token else {
        return Ok(None);
    };
    let MetadataAuthToken::Bearer(token) = auth_token else {
        return Err(MetadataError::Backend(
            "internal metadata auth requires the remote peer gate".to_string(),
        ));
    };
    validate_aruna_bearer_token(state, token.as_str())
        .await
        .map(Some)
        .map_err(|error| MetadataError::Backend(format!("invalid metadata auth token: {error}")))
}

async fn bucket_search_auth<S>(
    state: &S,
    storage_handle: &StorageHandle,
    peer: NodeId,
    local_realm_id: Option<RealmId>,
    auth_token: Option<MetadataAuthToken>,
) -> Result<AuthContext, MetadataReadError>
where
    S: ArunaBearerTokenValidationState + ?Sized,
{
    let Some(MetadataAuthToken::Bearer(token)) = auth_token else {
        return Err(MetadataReadError::Unauthorized);
    };
    let auth = validate_aruna_bearer_token(state, token.as_str())
        .await
        .map_err(|_| MetadataReadError::Unauthorized)?;
    let Some(local_realm_id) = local_realm_id else {
        return Err(MetadataReadError::Unavailable);
    };
    if auth.realm_id != local_realm_id {
        return Err(MetadataReadError::Forbidden);
    }
    ensure_remote_metadata_peer_is_configured_for_realm(
        storage_handle,
        peer,
        auth.realm_id,
        PeerTrust::Member,
    )
    .await
    .map_err(|_| MetadataReadError::Unavailable)?;
    Ok(auth)
}

async fn authorize_remote_metadata_peer<S>(
    state: &S,
    storage_handle: &StorageHandle,
    peer: NodeId,
    local_realm_id: Option<RealmId>,
    auth_token: Option<MetadataAuthToken>,
    allow_internal: bool,
) -> Result<Option<AuthContext>, MetadataError>
where
    S: ArunaBearerTokenValidationState + ?Sized,
{
    let internal_auth = matches!(&auth_token, Some(MetadataAuthToken::Internal(_)));
    let auth_context = match auth_token {
        Some(MetadataAuthToken::Internal(auth)) if allow_internal => {
            let local_realm_id = local_realm_id.ok_or_else(|| {
                MetadataError::InvalidInput(
                    "internal metadata auth requires a local serving realm".to_string(),
                )
            })?;
            if auth.realm_id != local_realm_id {
                return Err(MetadataError::InvalidInput(format!(
                    "internal metadata auth realm `{}` does not match local realm `{local_realm_id}`",
                    auth.realm_id
                )));
            }
            if auth.user_id.realm_id != auth.realm_id {
                return Err(MetadataError::InvalidInput(format!(
                    "internal metadata auth user realm `{}` does not match token realm `{}`",
                    auth.user_id.realm_id, auth.realm_id
                )));
            }
            Some(auth)
        }
        Some(MetadataAuthToken::Internal(_)) => {
            return Err(MetadataError::Backend(
                "internal metadata auth is limited to forwarded requests".to_string(),
            ));
        }
        auth_token => remote_metadata_auth_context(state, auth_token).await?,
    };
    // Authenticated metadata requests are bound to the token's realm. Anonymous
    // requests can only read public metadata, but still must come from a peer
    // configured in this node's local serving realm.
    let peer_realm_id = match auth_context.as_ref().map(|auth| auth.realm_id) {
        Some(realm_id) => realm_id,
        None => local_realm_id.ok_or_else(|| {
            MetadataError::InvalidInput(
                "remote metadata anonymous peer gate requires a local serving realm".to_string(),
            )
        })?,
    };
    // Internal auth is node-vouched, so the gate is told which user the peer
    // vouches for: an owner-bound device passes only for its own owner.
    let trust = match internal_auth {
        true => PeerTrust::Vouched(auth_context.as_ref().map(|auth| auth.user_id)),
        false => PeerTrust::Member,
    };
    ensure_remote_metadata_peer_is_configured_for_realm(storage_handle, peer, peer_realm_id, trust)
        .await?;
    Ok(auth_context)
}

async fn ensure_remote_metadata_peer_is_configured_for_realm(
    storage_handle: &StorageHandle,
    peer: NodeId,
    realm_id: RealmId,
    trust: PeerTrust,
) -> Result<(), MetadataError> {
    match storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(realm_id.as_bytes().to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => {
            let document = RealmConfigDocument::from_bytes(&bytes)
                .map_err(|error| MetadataError::Backend(error.to_string()))?;
            ensure_peer_trust(&document, peer, realm_id, trust)
                .map_err(|error| {
                    MetadataError::InvalidInput(match error {
                        RealmPeerError::RealmMismatch { configured, .. } => format!(
                            "realm config `{configured}` does not match remote metadata realm `{realm_id}`"
                        ),
                        RealmPeerError::NotConfigured { .. } => format!(
                            "remote metadata peer `{peer}` is not configured in realm `{realm_id}`"
                        ),
                        RealmPeerError::NotTrusted { .. } => format!(
                            "remote metadata peer `{peer}` is not trusted for internal auth in realm `{realm_id}`"
                        ),
                    })
                })
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            Err(MetadataError::InvalidInput(format!(
                "remote metadata peer `{peer}` is not configured in realm `{realm_id}`"
            )))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(MetadataError::Storage(error)),
        other => Err(MetadataError::Backend(format!(
            "unexpected realm config read result for `{realm_id}`: {other:?}"
        ))),
    }
}

async fn send_remote_metadata_request(
    inner: &MetadataInner,
    span: &Span,
    node_id: NodeId,
    message: MetadataTransportMessage,
) -> Result<MetadataTransportMessage, MetadataRequestError> {
    let Some(net_handle) = inner.net_handle.clone() else {
        record_error(span, "metadata net handle missing");
        return Err(MetadataRequestError::definitely_not_sent(
            MetadataError::HandleMissing,
        ));
    };

    send_request(&net_handle, node_id, message).await
}

async fn with_sync_timeout<T>(
    request: impl std::future::Future<Output = Result<T, MetadataRequestError>>,
) -> Result<T, MetadataError> {
    timeout(SYNC_MIRROR_REQUEST_TIMEOUT, request)
        .await
        .map_err(|_| MetadataError::Backend("sync mirror request timed out".to_string()))?
        .map_err(MetadataRequestError::into_metadata_error)
}

async fn load_metadata_auth_state<T>(
    storage_handle: &StorageHandle,
    key: &[u8],
) -> Result<T, MetadataError>
where
    T: DeserializeOwned + Default,
{
    match storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: API_STATE_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => {
            postcard::from_bytes(&bytes).map_err(|error| MetadataError::Backend(error.to_string()))
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(T::default()),
        Event::Storage(StorageEvent::Error { error }) => Err(MetadataError::Storage(error)),
        other => Err(MetadataError::Backend(format!(
            "unexpected metadata auth state read result: {other:?}"
        ))),
    }
}

#[async_trait]
impl Handle for MetadataHandle {
    async fn send_effect(&self, effect: Effect) -> Event {
        match effect {
            Effect::Metadata(metadata_effect) => self.send_metadata_effect(metadata_effect).await,
            _ => Event::Metadata(MetadataEvent::Error {
                graph_iri: None,
                error: MetadataError::InvalidEffect,
            }),
        }
    }
}

fn handle_effect(inner: Arc<MetadataInner>, effect: MetadataEffect) -> MetadataEvent {
    let effect_name = metadata_effect_kind(&effect);
    let auth = AllowAllAuthorizer;
    let graph_iri = effect_graph_iri(&effect);
    let needs_existing_graph = matches!(
        effect,
        MetadataEffect::ExportRoCrate { .. }
            | MetadataEffect::ExportRoCrateSummary { .. }
            | MetadataEffect::ExportRoCratePage { .. }
            | MetadataEffect::PlanBatch { .. }
    );
    let persist_document_sync_after_success = metadata_effect_persists_document_sync(&effect);
    let deferred_persist_after_success = metadata_effect_defers_persist(&effect);
    let node = inner.node.clone();
    let effect_span = debug_span!(
        "metadata.backend.effect",
        effect = effect_name,
        graph_iri = graph_iri.as_deref().unwrap_or("<none>"),
        elapsed_ms = field::Empty,
        result = field::Empty,
    );
    let effect_started = Instant::now();
    let result = effect_span.in_scope(|| match effect {
        MetadataEffect::ValidateCreateCrate { request } => {
            let graph_iri = request.graph_iri.clone();
            let call_span = debug_span!(
                "metadata.backend.craqle.validate_create_crate",
                graph_iri = %graph_iri,
                name_len = request.name.len() as u64,
                description_len = request.description.len() as u64,
                public = request.policy.public,
                permission_path_count = request.policy.permission_paths.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.validate_create_crate(&auth, craqle_create_request(request)))
                .map(|_| MetadataEvent::ValidationResult {
                    graph_iri: graph_iri.clone(),
                });
            record_metadata_result(
                &call_span,
                "validate_create_crate",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::ValidateRoCrate { request } => {
            let graph_iri = request.graph_iri.clone();
            let policy = request.policy;
            let jsonld = request.jsonld;
            let call_span = debug_span!(
                "metadata.backend.craqle.validate_rocrate",
                graph_iri = %graph_iri,
                jsonld_len = jsonld.len() as u64,
                public = policy.public,
                permission_path_count = policy.permission_paths.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| {
                    node.validate_rocrate_document_checked_with_policy(
                        &auth,
                        GraphId::new(&graph_iri),
                        &jsonld,
                        craqle_graph_policy(policy),
                    )
                })
                .map(|_| MetadataEvent::ValidationResult {
                    graph_iri: graph_iri.clone(),
                });
            record_metadata_result(
                &call_span,
                "validate_rocrate",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::CreateCrate { request } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.create_crate",
                graph_iri = %request.graph_iri,
                name_len = request.name.len() as u64,
                description_len = request.description.len() as u64,
                public = request.policy.public,
                permission_path_count = request.policy.permission_paths.len() as u64,
                durability = ?request.durability,
                elapsed_ms = field::Empty,
                result = field::Empty,
                batch_ops = field::Empty,
            );
            let started = Instant::now();
            let durability = request.durability;
            let actor = request.deterministic_actor.map(ActorId::from_bytes);
            let result = call_span.in_scope(|| {
                node.create_crate_with_durability_as(
                    &auth,
                    craqle_create_request(request.clone()),
                    craqle_request_durability(durability),
                    actor,
                )
            });
            record_craqle_call_result(
                &call_span,
                "create_crate",
                Some(&request.graph_iri),
                started,
                &result,
            );
            if let Ok(batch) = &result {
                call_span.record("batch_ops", batch.ops.len() as u64);
            }
            result.map(|batch| MetadataEvent::CreateCrateResult {
                graph_iri: request.graph_iri,
                batch: metadata_batch_from_craqle(batch),
            })
        }
        MetadataEffect::ApplyRoCrate { request } => {
            let graph_iri = request.graph_iri.clone();
            let policy = request.policy;
            let jsonld = request.jsonld;
            let durability = request.durability;
            let actor = request.deterministic_actor.map(ActorId::from_bytes);
            let call_span = debug_span!(
                "metadata.backend.craqle.apply_rocrate",
                graph_iri = %graph_iri,
                jsonld_len = jsonld.len() as u64,
                public = policy.public,
                permission_path_count = policy.permission_paths.len() as u64,
                durability = ?durability,
                elapsed_ms = field::Empty,
                result = field::Empty,
                batch_ops = field::Empty,
            );
            let started = Instant::now();
            let result = call_span.in_scope(|| {
                node.apply_rocrate_document_checked_with_policy_and_durability_as(
                    &auth,
                    GraphId::new(&graph_iri),
                    &jsonld,
                    craqle_graph_policy(policy),
                    craqle_request_durability(durability),
                    actor,
                )
            });
            record_craqle_call_result(
                &call_span,
                "apply_rocrate",
                Some(&graph_iri),
                started,
                &result,
            );
            if let Ok(batch) = &result {
                call_span.record("batch_ops", batch.ops.len() as u64);
            }
            result.map(|batch| MetadataEvent::ApplyRoCrateResult {
                graph_iri,
                batch: metadata_batch_from_craqle(batch),
            })
        }
        MetadataEffect::UpsertDataEntity { request } => {
            let graph_iri = request.graph_iri.clone();
            let durability = request.durability;
            let call_span = debug_span!(
                "metadata.backend.craqle.upsert_data_entity",
                graph_iri = %graph_iri,
                jsonld_len = request.jsonld.len() as u64,
                durability = ?durability,
                elapsed_ms = field::Empty,
                result = field::Empty,
                batch_ops = field::Empty,
            );
            let started = Instant::now();
            let result = call_span.in_scope(|| upsert_data_entity(&node, &auth, request));
            let converted = result.map(|batch| {
                call_span.record("batch_ops", batch.ops.len() as u64);
                MetadataEvent::EntityUpsertResult {
                    graph_iri: batch.graph_iri.clone(),
                    batch,
                }
            });
            record_metadata_result(
                &call_span,
                "upsert_data_entity",
                Some(&graph_iri),
                started,
                &converted,
            );
            converted
        }
        MetadataEffect::UpsertContextualEntity { request } => {
            let graph_iri = request.graph_iri.clone();
            let durability = request.durability;
            let call_span = debug_span!(
                "metadata.backend.craqle.upsert_contextual_entity",
                graph_iri = %graph_iri,
                jsonld_len = request.jsonld.len() as u64,
                durability = ?durability,
                elapsed_ms = field::Empty,
                result = field::Empty,
                batch_ops = field::Empty,
            );
            let started = Instant::now();
            let result = call_span.in_scope(|| upsert_contextual_entity(&node, &auth, request));
            let converted = result.map(|batch| {
                call_span.record("batch_ops", batch.ops.len() as u64);
                MetadataEvent::EntityUpsertResult {
                    graph_iri: batch.graph_iri.clone(),
                    batch,
                }
            });
            record_metadata_result(
                &call_span,
                "upsert_contextual_entity",
                Some(&graph_iri),
                started,
                &converted,
            );
            converted
        }
        MetadataEffect::SetGraphPolicy { graph_iri, policy } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.set_graph_policy",
                graph_iri = %graph_iri,
                public = policy.public,
                permission_path_count = policy.permission_paths.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| {
                    node.set_graph_policy(
                        &auth,
                        &GraphId::new(&graph_iri),
                        craqle_graph_policy(policy),
                    )
                })
                .map(|_| MetadataEvent::GraphPolicySet {
                    graph_iri: graph_iri.clone(),
                });
            record_metadata_result(
                &call_span,
                "set_graph_policy",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::AddGraphPeer { graph_iri, node_id } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.add_graph_peer",
                graph_iri = %graph_iri,
                peer = ?node_id,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| {
                    node.add_irokle_peer(&GraphId::new(&graph_iri), document_sync_peer_id(node_id))
                })
                .map(|_| MetadataEvent::GraphPeerAdded {
                    graph_iri: graph_iri.clone(),
                    node_id,
                });
            record_metadata_result(
                &call_span,
                "add_graph_peer",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::GetGraphPolicy { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.get_graph_policy",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.graph_policy(&GraphId::new(&graph_iri)))
                .map(|policy| MetadataEvent::GraphPolicyResult {
                    graph_iri: graph_iri.clone(),
                    policy: metadata_graph_policy_from_craqle(policy),
                });
            record_metadata_result(
                &call_span,
                "get_graph_policy",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::ExportRoCrate { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.export_rocrate",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
                jsonld_len = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.export_rocrate(&auth, &GraphId::new(&graph_iri)))
                .map(|jsonld| {
                    call_span.record("jsonld_len", jsonld.len() as u64);
                    MetadataEvent::RoCrateExportResult {
                        graph_iri: graph_iri.clone(),
                        jsonld,
                    }
                });
            record_metadata_result(
                &call_span,
                "export_rocrate",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::ExportRoCrateSummary { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.export_rocrate_summary",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
                jsonld_len = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.export_rocrate_summary(&auth, &GraphId::new(&graph_iri)))
                .map(|jsonld| {
                    call_span.record("jsonld_len", jsonld.len() as u64);
                    MetadataEvent::RoCrateSummaryResult {
                        graph_iri: graph_iri.clone(),
                        jsonld,
                    }
                });
            record_metadata_result(
                &call_span,
                "export_rocrate_summary",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::ExportRoCratePage {
            graph_iri,
            offset,
            after,
            limit,
        } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.export_rocrate_page",
                graph_iri = %graph_iri,
                offset = offset.unwrap_or(0) as u64,
                after_present = after.is_some(),
                limit = limit as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
                returned_data_entities = field::Empty,
                total_data_entities = field::Empty,
            );
            let started = Instant::now();
            let graph = GraphId::new(&graph_iri);
            let page = call_span.in_scope(|| {
                if let Some(after) = after.as_deref() {
                    node.export_rocrate_page_after(&auth, &graph, Some(after), limit)
                } else {
                    node.export_rocrate_page(&auth, &graph, offset.unwrap_or(0), limit)
                }
            });
            let result = page.map(|page| {
                call_span.record("returned_data_entities", page.returned_data_entities as u64);
                call_span.record("total_data_entities", page.total_data_entities as u64);
                MetadataEvent::RoCratePageResult {
                    graph_iri: graph_iri.clone(),
                    page: metadata_rocrate_page_from_craqle(page),
                }
            });
            record_metadata_result(
                &call_span,
                "export_rocrate_page",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::SearchGraphs { .. }
        | MetadataEffect::QueryGraphs { .. }
        | MetadataEffect::SyncGraphBestEffort { .. } => {
            unreachable!("handled asynchronously")
        }
        MetadataEffect::DeleteGraph { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.delete_graph",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.delete_graph(&auth, &GraphId::new(&graph_iri)))
                .map(|_| MetadataEvent::GraphDeleted {
                    graph_iri: graph_iri.clone(),
                });
            record_metadata_result(
                &call_span,
                "delete_graph",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::ListGraphs => {
            let call_span = debug_span!(
                "metadata.backend.craqle.list_graphs",
                elapsed_ms = field::Empty,
                result = field::Empty,
                graph_count = field::Empty,
            );
            let started = Instant::now();
            let result = call_span.in_scope(|| node.graphs()).map(|graphs| {
                call_span.record("graph_count", graphs.len() as u64);
                MetadataEvent::GraphListResult {
                    graph_iris: graphs
                        .into_iter()
                        .map(|graph| graph.as_str().to_string())
                        .collect(),
                }
            });
            record_metadata_result(&call_span, "list_graphs", None, started, &result);
            result
        }
        MetadataEffect::ContainsGraph { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.contains_graph",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
                exists = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.contains_graph(&GraphId::new(&graph_iri)))
                .map(|exists| {
                    call_span.record("exists", exists);
                    MetadataEvent::ContainsGraphResult {
                        graph_iri: graph_iri.clone(),
                        exists,
                    }
                });
            record_metadata_result(
                &call_span,
                "contains_graph",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        // Device replicas
        MetadataEffect::GraphSnapshot { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.graph_snapshot",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
                quad_count = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.graph_snapshot(&GraphId::new(&graph_iri)))
                .map(|snapshot| {
                    call_span.record("quad_count", snapshot.quads.len() as u64);
                    MetadataEvent::GraphSnapshotResult {
                        graph_iri: graph_iri.clone(),
                        snapshot: Box::new(snapshot),
                    }
                });
            record_metadata_result(
                &call_span,
                "graph_snapshot",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::InstallSnapshot {
            graph_iri,
            snapshot,
        } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.install_snapshot",
                graph_iri = %graph_iri,
                quad_count = snapshot.quads.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
                applied = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.install_graph_snapshot(&snapshot))
                .map(|merged| {
                    call_span.record("applied", merged.applied);
                    MetadataEvent::SnapshotInstalled {
                        graph_iri: graph_iri.clone(),
                        applied: merged.applied,
                    }
                });
            record_metadata_result(
                &call_span,
                "install_snapshot",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        // OR-Set metadata graphs
        MetadataEffect::PlanBatch {
            graph_iri,
            actor,
            source,
        } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.plan_batch",
                graph_iri = %graph_iri,
                jsonld_len = source.jsonld().len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
                batch_ops = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| plan_batch(&node, &auth, &graph_iri, actor, &source))
                .map(|batch| {
                    call_span.record("batch_ops", batch.ops.len() as u64);
                    MetadataEvent::BatchPlanned {
                        graph_iri: graph_iri.clone(),
                        batch,
                    }
                });
            record_metadata_result(&call_span, "plan_batch", Some(&graph_iri), started, &result);
            result
        }
        MetadataEffect::MergeBatch { graph_iri, batch } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.merge_batch",
                graph_iri = %graph_iri,
                batch_ops = batch.ops.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
                applied = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| to_craqle_batch(&batch).and_then(|batch| node.merge_batch(&batch)))
                .map(|merged| {
                    call_span.record("applied", merged.applied);
                    MetadataEvent::BatchMerged {
                        graph_iri: graph_iri.clone(),
                        applied: merged.applied,
                    }
                });
            record_metadata_result(
                &call_span,
                "merge_batch",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
    });

    let persist_error = if persist_document_sync_after_success && result.is_ok() {
        flush_document_sync_journal(&inner, effect_name, graph_iri.as_deref()).err()
    } else {
        None
    };
    if result.is_ok() && persist_error.is_none() && deferred_persist_after_success {
        schedule_deferred_metadata_persist(inner.clone(), effect_name, graph_iri.clone());
    }
    record_elapsed_ms(&effect_span, "elapsed_ms", effect_started);
    let event = match (result, persist_error) {
        (_, Some(error)) => MetadataEvent::Error { graph_iri, error },
        (Ok(event), None) => event,
        (Err(error), None) => {
            // Craqle has no public typed missing-graph error, so probe graph
            // existence to distinguish a materialization that has not caught up
            // (retryable, 503) from a genuine backend failure.
            let error = if needs_existing_graph
                && graph_iri
                    .as_deref()
                    .is_some_and(|iri| matches!(node.contains_graph(&GraphId::new(iri)), Ok(false)))
            {
                MetadataError::GraphNotFound
            } else {
                metadata_error_from_craqle(error)
            };
            MetadataEvent::Error { graph_iri, error }
        }
    };
    effect_span.record("result", metadata_event_kind(&event));
    if let MetadataEvent::Error { error, .. } = &event {
        record_error(&effect_span, &error.to_string());
    }
    event
}

#[cfg(test)]
mod tests {
    use super::lifecycle::registry_records_for_group;
    use super::search::{GraphVisibilityScope, LifecycleVisibility, registry_record_for_graph};
    use super::search::{
        HitDescribe, ScopeAuthorizer, describe_hits_parallel, filter_candidate_records,
        select_visible_records,
    };
    use super::*;
    use aruna_core::auth::bearer_token_hash;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::metadata::MetadataApplyRoCrateRequest;
    use aruna_core::storage_entries::metadata_graph_lifecycle_key;
    use aruna_core::structs::{
        ArunaArn, PathRestriction, PlacementRef, RealmNodeKind, SyncMode, SyncState,
        SyncStatusSnapshot, TokenRevocation,
    };
    use aruna_storage::FjallStorage;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::EncodePrivateKey;
    use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde::Serialize;
    use tempfile::{TempDir, tempdir};
    use tokio::io::AsyncWriteExt;

    const ROCRATE_12: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/rocrate/roundtrip-1.2.json"
    ));
    const ROCRATE_13: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/rocrate/roundtrip-1.3.json"
    ));

    mod auth;
    mod effect;
    mod lifecycle;
    mod query;
    mod search;
    mod sync;
    mod visibility;

    // Permissive on purpose: craqle's stored policy must not sway the decision.
}
