use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::auth::TRUSTED_REALMS_LIST_KEY;
use aruna_core::effects::StoragePriority;
use aruna_core::events::Event;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent, MetadataRoCratePage};
use aruna_core::structs::{BucketInfo, MetadataRegistryRecord, RealmId, SyncRelationship};
use aruna_core::types::{GroupId, UserId};
use aruna_net::NetHandle;
use aruna_storage::{FjallPersistPolicy, StorageHandle};
use async_trait::async_trait;
use craqle::{
    ActorId, CraqleIrokleOptions, CraqleNode, CraqleOptions, CrateViolation, GraphId, SearchStorage,
};
use jsonwebtoken::DecodingKey;
use oxrdf::Term;
use tracing::warn;
use ulid::Ulid;

use self::entity_convert::{error_from_craqle, fjall_persist_mode};
use self::lifecycle::{
    fill_visibility_caches, list_group_records, list_local_group, list_local_records,
};
use self::peer_auth::load_auth_state;
use self::persist::flush_metadata_persistence;
use self::query::snapshot_iri_references;
use self::search::{AllowedGraphAuthorizer, describe_hit_properties};
use super::contact::PeerContacts;
use super::materialization_queue::metadata_graph_fence;
use super::profile_cache::ProfileCache;
use super::profile_shacl::{
    ProfileShaclEngine, ProfileShaclError, ProfileShaclReport, ProfileShapes,
};
use super::query_cache::MetadataQueryCache;
use super::summary_cache::summary_cache;
use crate::auth::bearer_token::{
    ArunaBearerTokenError, ArunaBearerTokenValidationState, IssuerKeyCache, realm_token_revoked,
};
use crate::driver::{DriverContext, drive};
use crate::s3::create_bucket::{CreateBucketError, CreateBucketOperation};
use crate::sync::mirror_repair::RECONCILE_GRACE;

mod effects;
pub(crate) use self::effects::metadata_read_error;
pub(crate) use self::transport::transport_message_kind;
mod entity_convert;
mod ingress;
mod lifecycle;
mod peer_auth;
mod persist;
mod query;
mod search;
mod sync;
mod transport;

const METADATA_IO_TIMEOUT: Duration = Duration::from_secs(15);
const METADATA_CHUNK_SIZE: usize = 64 * 1024;
const METADATA_ENVELOPE_BYTES: u64 = 16 * 1024 * 1024;
const SYNC_MIRROR_REQUEST_TIMEOUT: Duration =
    RECONCILE_GRACE.saturating_sub(Duration::from_secs(10));
const METADATA_GRAPH_SYNC_ATTEMPTS: usize = 3;
const METADATA_GRAPH_SYNC_RETRY_AFTER: Duration = Duration::from_millis(250);
const SLOW_METADATA_BACKEND_THRESHOLD: Duration = Duration::from_millis(100);
// Craqle rebuilds a describe context per hit, so search enrichment overlaps
// instead of memoizing; the craqle read semaphore is the real concurrency cap.
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

    pub fn with_sync_policy(mut self, persist_policy: FjallPersistPolicy) -> Self {
        self.document_sync_persist_policy = persist_policy;
        self
    }

    pub fn with_pool_size(mut self, backend_pool_size: usize) -> Self {
        self.backend_pool_size = Some(backend_pool_size.max(1));
        self
    }

    pub fn with_validation_disabled(mut self, disabled: bool) -> Self {
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
        match load_auth_state::<HashSet<RealmId>>(&self.storage_handle, TRUSTED_REALMS_LIST_KEY)
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
            .with_graph_store_persist_mode(fjall_persist_mode(
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

    pub fn cache_registry_record(&self, record: MetadataRegistryRecord) {
        self.inner
            .visibility_cache
            .upsert_registry_records(std::slice::from_ref(&record));
    }

    pub fn cache_registry_records(&self, records: &[MetadataRegistryRecord]) {
        self.inner.visibility_cache.upsert_registry_records(records);
    }

    pub(crate) fn upsert_cached_at(&self, record: MetadataRegistryRecord, generation: u64) {
        self.inner
            .visibility_cache
            .upsert_at(std::slice::from_ref(&record), Some(generation));
    }

    pub fn remove_cached_record(&self, document_id: Ulid) {
        self.inner
            .visibility_cache
            .remove_registry_record(document_id);
    }

    pub async fn list_cached_records(
        &self,
    ) -> Result<Arc<Vec<MetadataRegistryRecord>>, MetadataError> {
        list_local_records(self.inner.clone()).await
    }

    pub async fn list_cached_group(
        &self,
        group_id: GroupId,
    ) -> Result<Arc<Vec<MetadataRegistryRecord>>, MetadataError> {
        list_local_group(self.inner.clone(), group_id).await
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

    pub async fn export_summary_jsonld(&self, graph_iri: String) -> Result<String, MetadataError> {
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
            .map_err(error_from_craqle)
    }

    pub async fn flush_persistence(&self) -> Result<(), MetadataError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || flush_metadata_persistence(&inner, "shutdown", None))
            .await
            .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
    }
}

#[cfg(test)]
mod tests;
