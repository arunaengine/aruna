use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::auth::{TOKEN_REVOCATION_LIST_KEY, TRUSTED_REALMS_LIST_KEY};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    API_STATE_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE, REALM_CONFIG_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataBatch, MetadataCreateCrateRequest, MetadataDot, MetadataEffect, MetadataError,
    MetadataEvent, MetadataGraphLifecycleRecord, MetadataGraphPolicy, MetadataQuadOp,
    MetadataQueryResults, MetadataRequestDurability, MetadataRoCratePage, MetadataSearchHit,
    MetadataUpsertEntityRequest,
};
use aruna_core::storage_entries::metadata_graph_lifecycle_key;
use aruna_core::structs::{
    AuthContext, MetadataRegistryRecord, Permission, RealmConfigDocument, RealmId,
};
use aruna_core::telemetry::duration_ms;
use aruna_core::types::GroupId;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use aruna_storage::{FjallPersistPolicy, StorageHandle};
use async_trait::async_trait;
use byteview::ByteView;
use craqle::{
    Action as CraqleAction, ActorId, AllowAllAuthorizer, AuthorizationError as CraqleAuthError,
    Authorizer as CraqleAuthorizer, Batch, CraqleError, CraqleIrokleOptions, CraqleNode,
    CraqleOptions, CraqleRequestDurability, CreateCrateRequest, CreateEntityRequest, GraphId,
    GraphPolicy, QueryResults, RoCrateError, SearchStorage, vocab,
};
use jsonwebtoken::DecodingKey;
use oxrdf::{BlankNode, Literal, NamedNode, Term};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use tokio::sync::RwLock;
use tokio::time::{sleep, timeout};
use tracing::{Instrument, Span, debug_span, field, warn};
use ulid::Ulid;

use super::protocol::{MetadataAuthToken, MetadataTransportMessage, read_message, write_message};
use super::repository::{REGISTRY_FILL_PAGE_SIZE, iter_all_registry_effect, parse_registry_iter};
use crate::auth::{
    ArunaBearerTokenError, ArunaBearerTokenValidationState, decoding_key_from_base64_public_key,
    validate_aruna_bearer_token,
};
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::list_groups::ListGroupOperation;

const METADATA_IO_TIMEOUT: Duration = Duration::from_secs(15);
const METADATA_GRAPH_SYNC_ATTEMPTS: usize = 3;
const METADATA_GRAPH_SYNC_RETRY_AFTER: Duration = Duration::from_millis(250);
const SLOW_METADATA_BACKEND_THRESHOLD: Duration = Duration::from_millis(100);
// Unbiased per-call-kind craqle latency histograms; every backend call is
// recorded, not just the ones above the slow-call threshold.
static CRAQLE_LATENCY: LazyLock<aruna_core::telemetry::LatencyAggregator> =
    LazyLock::new(|| aruna_core::telemetry::LatencyAggregator::new("craqle"));
const METADATA_VISIBILITY_CACHE_TTL: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct MetadataHandle {
    inner: Arc<MetadataInner>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetadataHandleOptions {
    pub search_storage: MetadataSearchStorage,
    pub irokle_persist_policy: FjallPersistPolicy,
    /// Size of the craqle mutation and read permit pools. Defaults to the
    /// host parallelism; set explicitly when cgroup limits make
    /// `available_parallelism` unrepresentative.
    pub backend_pool_size: Option<usize>,
}

impl MetadataHandleOptions {
    pub fn with_search_storage(mut self, search_storage: MetadataSearchStorage) -> Self {
        self.search_storage = search_storage;
        self
    }

    pub fn with_irokle_persist_policy(mut self, persist_policy: FjallPersistPolicy) -> Self {
        self.irokle_persist_policy = persist_policy;
        self
    }

    pub fn with_backend_pool_size(mut self, backend_pool_size: usize) -> Self {
        self.backend_pool_size = Some(backend_pool_size.max(1));
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
    irokle_db: Option<fjall::OptimisticTxDatabase>,
    irokle_persist_policy: FjallPersistPolicy,
    visibility_cache: MetadataVisibilityCache,
    craqle_permits: Arc<tokio::sync::Semaphore>,
    craqle_read_permits: Arc<tokio::sync::Semaphore>,
    deferred_persist_requested: AtomicBool,
    deferred_persist_running: AtomicBool,
}

#[derive(Clone)]
struct MetadataAuthValidationState {
    storage_handle: StorageHandle,
    issuer_keys: Arc<RwLock<HashMap<String, DecodingKey>>>,
}

impl MetadataAuthValidationState {
    fn new(storage_handle: StorageHandle) -> Self {
        Self {
            storage_handle,
            issuer_keys: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ArunaBearerTokenValidationState for MetadataAuthValidationState {
    async fn is_bearer_token_revoked(&self, token_hash: &str) -> bool {
        match load_metadata_auth_state::<HashSet<String>>(
            &self.storage_handle,
            TOKEN_REVOCATION_LIST_KEY,
        )
        .await
        {
            Ok(revoked) => revoked.contains(token_hash),
            Err(error) => {
                warn!(error = %error, "Failed to read metadata token revocation state");
                true
            }
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

    async fn decoding_key_for_issuer(
        &self,
        issuer_pubkey: &str,
    ) -> Result<DecodingKey, ArunaBearerTokenError> {
        let read_lock = self.issuer_keys.read().await;
        let key = read_lock.get(issuer_pubkey).cloned();
        drop(read_lock);
        if let Some(key) = key {
            return Ok(key);
        }

        let decoding_key = decoding_key_from_base64_public_key(issuer_pubkey)?;
        self.issuer_keys
            .write()
            .await
            .insert(issuer_pubkey.to_string(), decoding_key.clone());
        Ok(decoding_key)
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
    fn snapshot(&mut self) -> Arc<Vec<MetadataRegistryRecord>> {
        self.snapshot
            .get_or_insert_with(|| Arc::new(self.records.values().cloned().collect()))
            .clone()
    }

    fn group_snapshot(&mut self, group_id: GroupId) -> Arc<Vec<MetadataRegistryRecord>> {
        if let Some(records) = self.group_snapshots.get(&group_id) {
            return records.clone();
        }
        let records = Arc::new(
            self.records
                .values()
                .filter(|record| record.group_id == group_id)
                .cloned()
                .collect::<Vec<_>>(),
        );
        self.group_snapshots.insert(group_id, records.clone());
        records
    }
}

struct LifecycleDeletedCacheEntry {
    deleted: bool,
    expires_at: Instant,
}

struct MetadataGraphDeletedRead {
    deleted: bool,
    cache_hit: bool,
}

impl MetadataVisibilityCache {
    fn new() -> Self {
        Self {
            registry: Mutex::new(None),
            registry_fill: Arc::new(tokio::sync::Mutex::new(())),
            lifecycle_deleted: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
        }
    }

    fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn advance_generation(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    fn registry_records(&self) -> Option<Arc<Vec<MetadataRegistryRecord>>> {
        match self.registry_records_any() {
            Some((records, true)) => Some(records),
            _ => None,
        }
    }

    // Expired entries are kept so readers can be served stale data while a
    // background refill replaces the entry; the bool flags freshness.
    fn registry_records_any(&self) -> Option<(Arc<Vec<MetadataRegistryRecord>>, bool)> {
        let now = Instant::now();
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        registry
            .as_mut()
            .map(|entry| (entry.snapshot(), entry.expires_at > now))
    }

    fn registry_records_for_group_any(
        &self,
        group_id: GroupId,
    ) -> Option<(Arc<Vec<MetadataRegistryRecord>>, bool)> {
        let now = Instant::now();
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        registry
            .as_mut()
            .map(|entry| (entry.group_snapshot(group_id), entry.expires_at > now))
    }

    #[cfg(test)]
    fn store_registry_records(&self, records: Arc<Vec<MetadataRegistryRecord>>) {
        let map: BTreeMap<_, _> = records
            .iter()
            .map(|record| (record.document_id, record.clone()))
            .collect();
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        *registry = Some(RegistryCacheEntry {
            records: map,
            snapshot: Some(records),
            group_snapshots: HashMap::new(),
            expires_at: Instant::now() + METADATA_VISIBILITY_CACHE_TTL,
        });
    }

    fn store_visibility_fill(
        &self,
        records: Arc<Vec<MetadataRegistryRecord>>,
        lifecycle_entries: Vec<(String, bool)>,
        fill_generation: u64,
    ) -> bool {
        if self.current_generation() != fill_generation {
            return false;
        }
        let map: BTreeMap<_, _> = records
            .iter()
            .map(|record| (record.document_id, record.clone()))
            .collect();
        let now = Instant::now();
        let expires_at = now + METADATA_VISIBILITY_CACHE_TTL;
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        let mut lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        if self.current_generation() != fill_generation {
            return false;
        }
        for (graph_iri, deleted) in lifecycle_entries {
            lifecycle.insert(
                graph_iri,
                LifecycleDeletedCacheEntry {
                    deleted,
                    expires_at,
                },
            );
        }
        lifecycle.retain(|_, entry| entry.expires_at > now);
        *registry = Some(RegistryCacheEntry {
            records: map,
            snapshot: Some(records),
            group_snapshots: HashMap::new(),
            expires_at,
        });
        true
    }

    #[cfg(test)]
    fn lifecycle_deleted(&self, graph_iri: &str) -> Option<bool> {
        match self.lifecycle_deleted_any(graph_iri) {
            Some((deleted, true)) => Some(deleted),
            _ => None,
        }
    }

    fn lifecycle_deleted_any(&self, graph_iri: &str) -> Option<(bool, bool)> {
        let now = Instant::now();
        let lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        lifecycle
            .get(graph_iri)
            .map(|entry| (entry.deleted, entry.expires_at > now))
    }

    fn store_lifecycle_deleted(&self, graph_iri: String, deleted: bool) {
        let mut lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        self.advance_generation();
        lifecycle.insert(
            graph_iri,
            LifecycleDeletedCacheEntry {
                deleted,
                expires_at: Instant::now() + METADATA_VISIBILITY_CACHE_TTL,
            },
        );
    }

    // Bulk refresh after a registry fill: re-stamps every supplied graph and
    // drops expired leftovers (graphs no longer in the registry) so the map
    // stays bounded.
    #[cfg(test)]
    fn refresh_lifecycle_deleted(&self, entries: impl IntoIterator<Item = (String, bool)>) {
        let now = Instant::now();
        let expires_at = now + METADATA_VISIBILITY_CACHE_TTL;
        let mut lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        for (graph_iri, deleted) in entries {
            lifecycle.insert(
                graph_iri,
                LifecycleDeletedCacheEntry {
                    deleted,
                    expires_at,
                },
            );
        }
        lifecycle.retain(|_, entry| entry.expires_at > now);
    }

    fn refresh_lifecycle_deleted_if_current(
        &self,
        entries: impl IntoIterator<Item = (String, bool)>,
        fill_generation: u64,
    ) -> bool {
        if self.current_generation() != fill_generation {
            return false;
        }
        let entries = entries.into_iter().collect::<Vec<_>>();
        let now = Instant::now();
        let expires_at = now + METADATA_VISIBILITY_CACHE_TTL;
        let mut lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        if self.current_generation() != fill_generation {
            return false;
        }
        for (graph_iri, deleted) in entries {
            lifecycle.insert(
                graph_iri,
                LifecycleDeletedCacheEntry {
                    deleted,
                    expires_at,
                },
            );
        }
        lifecycle.retain(|_, entry| entry.expires_at > now);
        true
    }

    // Incremental maintenance keeps the cached registry usable under writes;
    // entries never outlive their fill TTL, so a missed update converges to
    // storage truth within one TTL via the periodic refill.
    fn upsert_registry_records(&self, updates: &[MetadataRegistryRecord]) {
        if updates.is_empty() {
            return;
        }
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        self.advance_generation();
        let Some(entry) = registry.as_mut() else {
            return;
        };
        let mut touched_groups = HashSet::new();
        for update in updates {
            entry.records.insert(update.document_id, update.clone());
            touched_groups.insert(update.group_id);
        }
        for group_id in touched_groups {
            entry.group_snapshots.remove(&group_id);
        }
        entry.snapshot = None;
    }

    fn remove_registry_record(&self, document_id: Ulid) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        self.advance_generation();
        let Some(entry) = registry.as_mut() else {
            return;
        };
        if let Some(removed) = entry.records.remove(&document_id) {
            entry.group_snapshots.remove(&removed.group_id);
            entry.snapshot = None;
        }
    }

    fn remove_registry_records_by_graph(&self, graph_iri: &str) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        self.advance_generation();
        let Some(entry) = registry.as_mut() else {
            return;
        };
        let removed_groups = entry
            .records
            .values()
            .filter(|record| record.graph_iri == graph_iri)
            .map(|record| record.group_id)
            .collect::<HashSet<_>>();
        if removed_groups.is_empty() {
            return;
        }
        entry
            .records
            .retain(|_, record| record.graph_iri != graph_iri);
        for group_id in removed_groups {
            entry.group_snapshots.remove(&group_id);
        }
        entry.snapshot = None;
    }

    fn remove_lifecycle_entry(&self, graph_iri: &str) {
        let mut lifecycle = self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner());
        self.advance_generation();
        lifecycle.remove(graph_iri);
    }

    fn expire_now(&self) {
        let expired = Instant::now() - Duration::from_secs(1);
        if let Some(entry) = self
            .registry
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .as_mut()
        {
            entry.expires_at = expired;
        }
        for entry in self
            .lifecycle_deleted
            .lock()
            .unwrap_or_else(|lock| lock.into_inner())
            .values_mut()
        {
            entry.expires_at = expired;
        }
    }
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
        irokle_node: Option<irokle::Irokle<irokle::FjallStorage>>,
        irokle_db: Option<fjall::OptimisticTxDatabase>,
    ) -> Result<Self, MetadataError> {
        Self::new_with_options(
            path,
            node_id,
            storage_handle,
            net_handle,
            irokle_node,
            irokle_db,
            MetadataHandleOptions::default(),
        )
    }

    pub fn new_with_options(
        path: impl AsRef<Path>,
        node_id: NodeId,
        storage_handle: StorageHandle,
        net_handle: Option<NetHandle>,
        irokle_node: Option<irokle::Irokle<irokle::FjallStorage>>,
        irokle_db: Option<fjall::OptimisticTxDatabase>,
        metadata_options: MetadataHandleOptions,
    ) -> Result<Self, MetadataError> {
        let actor = ActorId::from_bytes(*node_id.as_bytes());
        let options = CraqleOptions::new()
            .with_actor(actor)
            .with_search_storage(metadata_options.search_storage.into());
        let options = match irokle_node {
            Some(irokle_node) => options.with_irokle(irokle_node, CraqleIrokleOptions::new()),
            None => options,
        };
        let node = CraqleNode::open_with_options(path, options)
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        let pool_size = metadata_options.backend_pool_size.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|cores| cores.get())
                .unwrap_or(4)
                .max(4)
        });
        Ok(Self {
            inner: Arc::new(MetadataInner {
                node: Arc::new(node),
                auth_validation: MetadataAuthValidationState::new(storage_handle.clone()),
                storage_handle,
                net_handle,
                irokle_db,
                irokle_persist_policy: metadata_options.irokle_persist_policy,
                visibility_cache: MetadataVisibilityCache::new(),
                craqle_permits: Arc::new(tokio::sync::Semaphore::new(pool_size)),
                craqle_read_permits: Arc::new(tokio::sync::Semaphore::new(pool_size)),
                deferred_persist_requested: AtomicBool::new(false),
                deferred_persist_running: AtomicBool::new(false),
            }),
        })
    }

    pub fn upsert_visible_registry_record(&self, record: MetadataRegistryRecord) {
        self.inner
            .visibility_cache
            .upsert_registry_records(std::slice::from_ref(&record));
    }

    pub fn upsert_visible_registry_records(&self, records: &[MetadataRegistryRecord]) {
        self.inner.visibility_cache.upsert_registry_records(records);
    }

    pub fn remove_visible_registry_record(&self, document_id: Ulid) {
        self.inner
            .visibility_cache
            .remove_registry_record(document_id);
    }

    pub async fn list_visible_registry_records(
        &self,
    ) -> Result<Arc<Vec<MetadataRegistryRecord>>, MetadataError> {
        list_local_registry_records(self.inner.clone()).await
    }

    pub async fn list_visible_registry_records_for_group(
        &self,
        group_id: GroupId,
    ) -> Result<Arc<Vec<MetadataRegistryRecord>>, MetadataError> {
        list_local_registry_records_for_group(self.inner.clone(), group_id).await
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

    pub async fn send_metadata_effect(&self, effect: MetadataEffect) -> Event {
        let started = Instant::now();
        let event = self.send_metadata_effect_inner(effect).await;
        aruna_core::telemetry::record_stage("craqle", started.elapsed());
        event
    }

    async fn send_metadata_effect_inner(&self, effect: MetadataEffect) -> Event {
        let effect_name = metadata_effect_kind(&effect);
        let graph_iri = effect_graph_iri(&effect);
        if let MetadataEffect::DeleteGraph { graph_iri } = &effect {
            self.inner
                .visibility_cache
                .remove_registry_records_by_graph(graph_iri);
            self.inner
                .visibility_cache
                .remove_lifecycle_entry(graph_iri);
        }
        if let Some(graph_iri) = graph_iri.as_deref()
            && !metadata_effect_skips_lifecycle_read(&effect)
        {
            let span = debug_span!(
                "metadata.graph_lifecycle.read_before_effect",
                effect = effect_name,
                graph_iri,
                deleted = field::Empty,
                elapsed_ms = field::Empty,
            );
            let started = Instant::now();
            let result = metadata_graph_deleted(self.inner.clone(), graph_iri)
                .instrument(span.clone())
                .await;
            match result {
                Ok(read) if read.deleted => {
                    span.record("deleted", true);
                    record_elapsed(&span, "elapsed_ms", started);
                    match &effect {
                        MetadataEffect::DeleteGraph { .. } => {}
                        MetadataEffect::SyncGraphBestEffort { graph_iri, peers } => {
                            return Event::Metadata(MetadataEvent::GraphSyncScheduled {
                                graph_iri: graph_iri.clone(),
                                peers: peers.clone(),
                            });
                        }
                        MetadataEffect::ContainsGraph { graph_iri } => {
                            return Event::Metadata(MetadataEvent::ContainsGraphResult {
                                graph_iri: graph_iri.clone(),
                                exists: false,
                            });
                        }
                        _ if effect_rejects_deleted_graph(&effect) => {
                            return Event::Metadata(MetadataEvent::Error {
                                graph_iri: Some(graph_iri.to_string()),
                                error: MetadataError::InvalidInput(format!(
                                    "metadata graph `{graph_iri}` is deleted"
                                )),
                            });
                        }
                        _ => {}
                    }
                }
                Ok(_) => {
                    span.record("deleted", false);
                    record_elapsed(&span, "elapsed_ms", started);
                }
                Err(error) => {
                    record_error(&span, &error.to_string());
                    record_elapsed(&span, "elapsed_ms", started);
                    return Event::Metadata(MetadataEvent::Error {
                        graph_iri: Some(graph_iri.to_string()),
                        error,
                    });
                }
            }
        }
        match effect {
            MetadataEffect::SyncGraphBestEffort { graph_iri, peers } => {
                Event::Metadata(self.sync_graph_best_effort(graph_iri, peers).await)
            }
            MetadataEffect::QueryGraphs {
                auth_context,
                graph_iris,
                sparql,
            } => Event::Metadata(
                match self
                    .query_authorized_local(auth_context, graph_iris, sparql)
                    .await
                {
                    Ok(results) => MetadataEvent::QueryResult { results },
                    Err(error) => MetadataEvent::Error {
                        graph_iri: None,
                        error,
                    },
                },
            ),
            MetadataEffect::SearchGraphs {
                auth_context,
                graph_iris,
                query,
                limit,
            } => Event::Metadata(
                match self
                    .search_authorized_local(auth_context, graph_iris, query, limit)
                    .await
                {
                    Ok(hits) => MetadataEvent::SearchResult { hits },
                    Err(error) => MetadataEvent::Error {
                        graph_iri: None,
                        error,
                    },
                },
            ),
            MetadataEffect::ListGraphs => {
                Event::Metadata(match list_visible_graphs(self.inner.clone()).await {
                    Ok(graph_iris) => MetadataEvent::GraphListResult { graph_iris },
                    Err(error) => MetadataEvent::Error {
                        graph_iri: None,
                        error,
                    },
                })
            }
            other => {
                let inner = self.inner.clone();
                let span = debug_span!(
                    "metadata.backend.blocking_task",
                    effect = metadata_effect_kind(&other),
                    graph_iri = graph_iri.as_deref().unwrap_or("<none>"),
                    elapsed_ms = field::Empty,
                    result = field::Empty,
                );
                let blocking_span = span.clone();
                let started = Instant::now();
                // Heavy mutations and cheap reads queue on separate pools so
                // trivial reads never wait behind long materializations.
                let permits = if metadata_effect_mutates_graph(&other) {
                    self.inner.craqle_permits.clone()
                } else {
                    self.inner.craqle_read_permits.clone()
                };
                let _permit = permits.acquire_owned().await.ok();
                let metadata_event = match tokio::task::spawn_blocking(move || {
                    blocking_span.in_scope(|| handle_effect(inner, other))
                })
                .await
                {
                    Ok(event) => event,
                    Err(error) => {
                        record_error(&span, &error.to_string());
                        MetadataEvent::Error {
                            graph_iri,
                            error: MetadataError::TaskJoin(error.to_string()),
                        }
                    }
                };
                record_elapsed(&span, "elapsed_ms", started);
                span.record("result", metadata_event_kind(&metadata_event));
                Event::Metadata(metadata_event)
            }
        }
    }

    pub async fn reconcile_irokle(&self) -> Result<usize, MetadataError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.node.reconcile_irokle())
            .await
            .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
            .map_err(|error| MetadataError::Backend(error.to_string()))
    }

    pub async fn prune_graph_if_deleted(&self, graph_iri: String) -> Result<bool, MetadataError> {
        if !graph_lifecycle_deleted(self.inner.storage_handle.clone(), &graph_iri).await? {
            return Ok(false);
        }
        self.inner
            .visibility_cache
            .remove_registry_records_by_graph(&graph_iri);
        self.inner
            .visibility_cache
            .store_lifecycle_deleted(graph_iri.clone(), true);
        if !contains_local_graph(self.inner.node.clone(), graph_iri.clone()).await? {
            return Ok(true);
        }
        delete_local_graph(self.inner.node.clone(), graph_iri).await?;
        Ok(true)
    }

    pub async fn prune_deleted_graphs(&self) -> Result<usize, MetadataError> {
        let inner = self.inner.clone();
        let graphs = tokio::task::spawn_blocking(move || inner.node.graphs())
            .await
            .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        let mut pruned = 0usize;
        for graph in graphs {
            if self
                .prune_graph_if_deleted(graph.as_str().to_string())
                .await?
            {
                pruned += 1;
            }
        }
        Ok(pruned)
    }

    async fn sync_graph_best_effort(
        &self,
        graph_iri: String,
        mut peers: Vec<NodeId>,
    ) -> MetadataEvent {
        if let Some(net_handle) = self.inner.net_handle.as_ref() {
            peers.retain(|peer| *peer != net_handle.node_id());
        }
        peers.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        peers.dedup();
        if peers.is_empty() {
            return MetadataEvent::GraphSyncScheduled { graph_iri, peers };
        }

        let inner = self.inner.clone();
        let task_graph_iri = graph_iri.clone();
        let task_peers = peers.clone();
        tokio::spawn(async move {
            for attempt in 1..=METADATA_GRAPH_SYNC_ATTEMPTS {
                match sync_graph_once(inner.clone(), task_graph_iri.clone(), task_peers.clone())
                    .await
                {
                    Ok(()) => return,
                    Err(error) => {
                        warn!(
                            graph_iri = %task_graph_iri,
                            attempt,
                            attempts = METADATA_GRAPH_SYNC_ATTEMPTS,
                            error = ?error,
                            "Metadata graph sync attempt failed"
                        );
                        if attempt < METADATA_GRAPH_SYNC_ATTEMPTS {
                            sleep(METADATA_GRAPH_SYNC_RETRY_AFTER).await;
                        }
                    }
                }
            }

            warn!(
                graph_iri = %task_graph_iri,
                peer_count = task_peers.len(),
                "Metadata graph sync retries exhausted"
            );
        });

        MetadataEvent::GraphSyncScheduled { graph_iri, peers }
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
        mut stream: BiStream,
        peer: NodeId,
    ) -> Result<(), MetadataError> {
        let total_started = Instant::now();
        let read_started = Instant::now();
        let message = read_transport_message(&mut stream).await?;
        let span = Span::current();
        record_elapsed(&span, "read_ms", read_started);
        span.record("request", metadata_transport_message_kind(&message));

        let process_started = Instant::now();
        let response = match message {
            MetadataTransportMessage::QueryGraphs {
                auth_token,
                graph_iris,
                sparql,
            } => match authorize_remote_metadata_peer(
                &self.inner.auth_validation,
                &self.inner.storage_handle,
                peer,
                auth_token,
            )
            .await
            {
                Ok(auth_context) => match query_local_graphs(
                    self.inner.clone(),
                    Some(auth_context),
                    graph_iris,
                    sparql,
                )
                .await
                {
                    Ok(results) => MetadataTransportMessage::QueryResults { results },
                    Err(error) => MetadataTransportMessage::Reject(error.to_string()),
                },
                Err(error) => MetadataTransportMessage::Reject(error.to_string()),
            },
            MetadataTransportMessage::SearchGraphs {
                auth_token,
                graph_iris,
                query,
                limit,
            } => match authorize_remote_metadata_peer(
                &self.inner.auth_validation,
                &self.inner.storage_handle,
                peer,
                auth_token,
            )
            .await
            {
                Ok(auth_context) => match search_local_graphs(
                    self.inner.clone(),
                    Some(auth_context),
                    graph_iris,
                    query,
                    limit,
                )
                .await
                {
                    Ok(hits) => MetadataTransportMessage::SearchResults { hits },
                    Err(error) => MetadataTransportMessage::Reject(error.to_string()),
                },
                Err(error) => MetadataTransportMessage::Reject(error.to_string()),
            },
            MetadataTransportMessage::QueryResults { .. }
            | MetadataTransportMessage::SearchResults { .. }
            | MetadataTransportMessage::Reject(_) => {
                MetadataTransportMessage::Reject("unexpected metadata control message".to_string())
            }
        };
        record_elapsed(&span, "process_ms", process_started);

        let drain_started = Instant::now();
        drain_request_stream(&mut stream).await?;
        record_elapsed(&span, "drain_ms", drain_started);

        let write_started = Instant::now();
        let _ = write_transport_message(&mut stream, &response).await;
        record_elapsed(&span, "write_ms", write_started);
        close_stream(&mut stream).await;
        record_elapsed(&span, "elapsed_ms", total_started);
        span.record("response", metadata_transport_message_kind(&response));
        Ok(())
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
    ) -> Result<Vec<MetadataSearchHit>, MetadataError> {
        search_local_graphs(self.inner.clone(), auth_context, graph_iris, query, limit).await
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
    ) -> Result<MetadataQueryResults, MetadataError> {
        let started = Instant::now();
        let span = Span::current();
        let Some(net_handle) = self.inner.net_handle.clone() else {
            record_error(&span, "metadata net handle missing");
            return Err(MetadataError::HandleMissing);
        };
        let result = match send_request(
            &net_handle,
            node_id,
            MetadataTransportMessage::QueryGraphs {
                auth_token,
                graph_iris,
                sparql,
            },
        )
        .await?
        {
            MetadataTransportMessage::QueryResults { results } => Ok(results),
            MetadataTransportMessage::Reject(error) => Err(MetadataError::Backend(error)),
            other => Err(MetadataError::Backend(format!(
                "unexpected metadata query response: {other:?}"
            ))),
        };
        record_elapsed(&span, "elapsed_ms", started);
        match &result {
            Ok(results) => {
                span.record("result", metadata_query_result_kind(results));
                record_metadata_query_result_counts(&span, results);
            }
            Err(error) => record_error(&span, &error.to_string()),
        }
        result
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
    pub async fn request_remote_search_graphs(
        &self,
        node_id: NodeId,
        auth_token: Option<MetadataAuthToken>,
        graph_iris: Option<Vec<String>>,
        query: String,
        limit: usize,
    ) -> Result<Vec<MetadataSearchHit>, MetadataError> {
        let started = Instant::now();
        let span = Span::current();
        let Some(net_handle) = self.inner.net_handle.clone() else {
            record_error(&span, "metadata net handle missing");
            return Err(MetadataError::HandleMissing);
        };
        let result = match send_request(
            &net_handle,
            node_id,
            MetadataTransportMessage::SearchGraphs {
                auth_token,
                graph_iris,
                query,
                limit,
            },
        )
        .await?
        {
            MetadataTransportMessage::SearchResults { hits } => Ok(hits),
            MetadataTransportMessage::Reject(error) => Err(MetadataError::Backend(error)),
            other => Err(MetadataError::Backend(format!(
                "unexpected metadata search response: {other:?}"
            ))),
        };
        record_elapsed(&span, "elapsed_ms", started);
        match &result {
            Ok(hits) => {
                span.record("result", "ok");
                span.record("hit_count", hits.len() as u64);
            }
            Err(error) => record_error(&span, &error.to_string()),
        }
        result
    }
}

async fn remote_metadata_auth_context<S>(
    state: &S,
    auth_token: Option<MetadataAuthToken>,
) -> Result<AuthContext, MetadataError>
where
    S: ArunaBearerTokenValidationState + ?Sized,
{
    let Some(auth_token) = auth_token else {
        return Err(MetadataError::Backend(
            "missing metadata auth token".to_string(),
        ));
    };
    let MetadataAuthToken::Bearer(token) = auth_token;
    validate_aruna_bearer_token(state, token.as_str())
        .await
        .map_err(|error| MetadataError::Backend(format!("invalid metadata auth token: {error}")))
}

async fn authorize_remote_metadata_peer<S>(
    state: &S,
    storage_handle: &StorageHandle,
    peer: NodeId,
    auth_token: Option<MetadataAuthToken>,
) -> Result<AuthContext, MetadataError>
where
    S: ArunaBearerTokenValidationState + ?Sized,
{
    let auth_context = remote_metadata_auth_context(state, auth_token).await?;
    ensure_remote_metadata_peer_is_configured_for_realm(
        storage_handle,
        peer,
        auth_context.realm_id,
    )
    .await?;
    Ok(auth_context)
}

async fn ensure_remote_metadata_peer_is_configured_for_realm(
    storage_handle: &StorageHandle,
    peer: NodeId,
    realm_id: RealmId,
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
            if document.has_node(peer) {
                Ok(())
            } else {
                Err(MetadataError::InvalidInput(format!(
                    "remote metadata peer `{peer}` is not configured in realm `{realm_id}`"
                )))
            }
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            Err(MetadataError::InvalidInput(format!(
                "remote metadata peer `{peer}` is not configured in realm `{realm_id}`"
            )))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(MetadataError::Backend(format!(
            "realm config read failed for `{realm_id}`: {error}"
        ))),
        other => Err(MetadataError::Backend(format!(
            "unexpected realm config read result for `{realm_id}`: {other:?}"
        ))),
    }
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
        Event::Storage(StorageEvent::Error { error }) => {
            Err(MetadataError::Backend(error.to_string()))
        }
        other => Err(MetadataError::Backend(format!(
            "unexpected metadata auth state read result: {other:?}"
        ))),
    }
}

async fn graph_lifecycle_record(
    storage_handle: StorageHandle,
    graph_iri: &str,
) -> Result<Option<MetadataGraphLifecycleRecord>, MetadataError> {
    match storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            key: metadata_graph_lifecycle_key(graph_iri),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                postcard::from_bytes(&bytes)
                    .map_err(|error| MetadataError::Backend(error.to_string()))
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(MetadataError::Backend(error.to_string()))
        }
        other => Err(MetadataError::Backend(format!(
            "unexpected metadata graph lifecycle read result: {other:?}"
        ))),
    }
}

async fn metadata_graph_deleted(
    inner: Arc<MetadataInner>,
    graph_iri: &str,
) -> Result<MetadataGraphDeletedRead, MetadataError> {
    if let Some((true, _)) = inner.visibility_cache.lifecycle_deleted_any(graph_iri) {
        return Ok(MetadataGraphDeletedRead {
            deleted: true,
            cache_hit: true,
        });
    }

    let deleted = graph_lifecycle_deleted(inner.storage_handle.clone(), graph_iri).await?;
    inner
        .visibility_cache
        .store_lifecycle_deleted(graph_iri.to_string(), deleted);
    Ok(MetadataGraphDeletedRead {
        deleted,
        cache_hit: false,
    })
}

async fn graph_lifecycle_deleted(
    storage_handle: StorageHandle,
    graph_iri: &str,
) -> Result<bool, MetadataError> {
    Ok(graph_lifecycle_record(storage_handle, graph_iri)
        .await?
        .map(|record| record.is_deleted())
        .unwrap_or(false))
}

async fn delete_local_graph(node: Arc<CraqleNode>, graph_iri: String) -> Result<(), MetadataError> {
    tokio::task::spawn_blocking(move || node.delete_graph_unchecked(&GraphId::new(&graph_iri)))
        .await
        .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
        .map_err(metadata_error_from_craqle)
}

async fn contains_local_graph(
    node: Arc<CraqleNode>,
    graph_iri: String,
) -> Result<bool, MetadataError> {
    tokio::task::spawn_blocking(move || node.contains_graph(&GraphId::new(&graph_iri)))
        .await
        .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
        .map_err(metadata_error_from_craqle)
}

fn metadata_effect_mutates_graph(effect: &MetadataEffect) -> bool {
    matches!(
        effect,
        MetadataEffect::CreateCrate { .. }
            | MetadataEffect::ApplyRoCrate { .. }
            | MetadataEffect::UpsertDataEntity { .. }
            | MetadataEffect::UpsertContextualEntity { .. }
            | MetadataEffect::SetGraphPolicy { .. }
            | MetadataEffect::AddGraphPeer { .. }
            | MetadataEffect::DeleteGraph { .. }
    )
}

fn effect_rejects_deleted_graph(effect: &MetadataEffect) -> bool {
    matches!(
        effect,
        MetadataEffect::ValidateCreateCrate { .. }
            | MetadataEffect::ValidateRoCrate { .. }
            | MetadataEffect::CreateCrate { .. }
            | MetadataEffect::ApplyRoCrate { .. }
            | MetadataEffect::UpsertDataEntity { .. }
            | MetadataEffect::UpsertContextualEntity { .. }
            | MetadataEffect::SetGraphPolicy { .. }
            | MetadataEffect::AddGraphPeer { .. }
            | MetadataEffect::GetGraphPolicy { .. }
            | MetadataEffect::ExportRoCrate { .. }
            | MetadataEffect::ExportRoCrateSummary { .. }
            | MetadataEffect::ExportRoCratePage { .. }
    )
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

#[tracing::instrument(
    name = "metadata.graph_sync.once",
    level = "debug",
    skip(inner),
    fields(
        graph_iri = %graph_iri,
        peer_count = peers.len() as u64,
        local_peer_setup_ms = field::Empty,
        network_sync_ms = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
async fn sync_graph_once(
    inner: Arc<MetadataInner>,
    graph_iri: String,
    peers: Vec<NodeId>,
) -> Result<(), MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();
    if peers.is_empty() {
        return Ok(());
    }
    if graph_lifecycle_deleted(inner.storage_handle.clone(), &graph_iri).await? {
        return Ok(());
    }
    let net_handle = inner
        .net_handle
        .clone()
        .ok_or(MetadataError::HandleMissing)?;
    let node = inner.node.clone();
    let graph_iri_for_blocking = graph_iri.clone();
    let peers_for_blocking = peers.clone();
    let setup_span = debug_span!(
        "metadata.backend.craqle.ensure_irokle_topic",
        graph_iri = %graph_iri_for_blocking,
        peer_count = peers_for_blocking.len() as u64,
        elapsed_ms = field::Empty,
        result = field::Empty,
    );
    let blocking_span = setup_span.clone();
    let setup_started = Instant::now();
    let topic_id = tokio::task::spawn_blocking(move || {
        blocking_span.in_scope(|| {
            let graph = GraphId::new(&graph_iri_for_blocking);
            for peer in peers_for_blocking {
                node.add_irokle_peer(&graph, irokle_peer_id(peer))?;
            }
            node.ensure_irokle_topic(&graph)
        })
    })
    .await
    .map_err(|error| MetadataError::TaskJoin(error.to_string()))?;
    record_elapsed(&setup_span, "elapsed_ms", setup_started);
    record_elapsed(&span, "local_peer_setup_ms", setup_started);
    match &topic_id {
        Ok(_) => {
            setup_span.record("result", "ok");
        }
        Err(error) => record_error(&setup_span, &error.to_string()),
    }
    let topic_id = topic_id.map_err(metadata_error_from_craqle)?;

    let sync_started = Instant::now();
    net_handle
        .sync_irokle_topic_with_peers(topic_id, peers)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    record_elapsed(&span, "network_sync_ms", sync_started);
    record_elapsed(&span, "elapsed_ms", total_started);
    Ok(())
}

fn handle_effect(inner: Arc<MetadataInner>, effect: MetadataEffect) -> MetadataEvent {
    let effect_name = metadata_effect_kind(&effect);
    let auth = AllowAllAuthorizer;
    let graph_iri = effect_graph_iri(&effect);
    let reads_existing_graph = matches!(
        effect,
        MetadataEffect::ExportRoCrate { .. }
            | MetadataEffect::ExportRoCrateSummary { .. }
            | MetadataEffect::ExportRoCratePage { .. }
    );
    let persist_irokle_after_success = metadata_effect_persists_irokle(&effect);
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
            // Event-log materialization (deterministic actor + WAL-durable
            // request) replays payloads validated at the origin.
            let prevalidated =
                actor.is_some() && durability == MetadataRequestDurability::WalAlreadyDurable;
            let result = call_span.in_scope(|| {
                if prevalidated {
                    node.create_crate_prevalidated_with_durability_as(
                        &auth,
                        craqle_create_request(request.clone()),
                        craqle_request_durability(durability),
                        actor,
                    )
                } else {
                    node.create_crate_with_durability_as(
                        &auth,
                        craqle_create_request(request.clone()),
                        craqle_request_durability(durability),
                        actor,
                    )
                }
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
            let prevalidated =
                actor.is_some() && durability == MetadataRequestDurability::WalAlreadyDurable;
            let result = call_span.in_scope(|| {
                if prevalidated {
                    node.apply_rocrate_document_prevalidated_with_policy_and_durability_as(
                        &auth,
                        GraphId::new(&graph_iri),
                        &jsonld,
                        craqle_graph_policy(policy),
                        craqle_request_durability(durability),
                        actor,
                    )
                } else {
                    node.apply_rocrate_document_checked_with_policy_and_durability_as(
                        &auth,
                        GraphId::new(&graph_iri),
                        &jsonld,
                        craqle_graph_policy(policy),
                        craqle_request_durability(durability),
                        actor,
                    )
                }
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
                    node.import_graph_policy(&GraphId::new(&graph_iri), craqle_graph_policy(policy))
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
                    node.add_irokle_peer(&GraphId::new(&graph_iri), irokle_peer_id(node_id))
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
                .in_scope(|| node.delete_graph_unchecked(&GraphId::new(&graph_iri)))
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
    });

    let persist_error = if persist_irokle_after_success && result.is_ok() {
        flush_irokle_journal(&inner, effect_name, graph_iri.as_deref()).err()
    } else {
        None
    };
    if result.is_ok() && persist_error.is_none() && deferred_persist_after_success {
        schedule_deferred_metadata_persist(inner.clone(), effect_name, graph_iri.clone());
    }
    record_elapsed(&effect_span, "elapsed_ms", effect_started);
    let event = match (result, persist_error) {
        (_, Some(error)) => MetadataEvent::Error { graph_iri, error },
        (Ok(event), None) => event,
        (Err(error), None) => {
            // Craqle has no public typed missing-graph error on the export
            // path, so probe graph existence to distinguish a pending
            // materialization from a genuine backend failure.
            let error = if reads_existing_graph
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

fn flush_irokle_journal(
    inner: &MetadataInner,
    effect_name: &'static str,
    graph_iri: Option<&str>,
) -> Result<(), MetadataError> {
    let Some(db) = &inner.irokle_db else {
        return Ok(());
    };
    let span = debug_span!(
        "metadata.backend.irokle.flush",
        effect = effect_name,
        graph_iri = graph_iri.unwrap_or("<none>"),
        mode = inner.irokle_persist_policy.label(),
        elapsed_ms = field::Empty,
        result = field::Empty,
    );
    let started = Instant::now();
    let result = span.in_scope(|| db.persist(inner.irokle_persist_policy.as_fjall()));
    record_elapsed(&span, "elapsed_ms", started);
    match result {
        Ok(()) => {
            span.record("result", "ok");
            Ok(())
        }
        Err(error) => {
            record_error(&span, &error.to_string());
            Err(MetadataError::Backend(format!(
                "failed to flush irokle journal: {error}"
            )))
        }
    }
}

fn flush_metadata_persistence(
    inner: &MetadataInner,
    effect_name: &'static str,
    graph_iri: Option<&str>,
) -> Result<(), MetadataError> {
    inner
        .node
        .persist_fjall()
        .map_err(metadata_error_from_craqle)?;
    flush_irokle_journal(inner, effect_name, graph_iri)
}

fn metadata_effect_persists_irokle(effect: &MetadataEffect) -> bool {
    match effect {
        MetadataEffect::ValidateCreateCrate { .. } | MetadataEffect::ValidateRoCrate { .. } => {
            false
        }
        MetadataEffect::CreateCrate { request } => {
            metadata_request_persists_irokle(request.durability)
        }
        MetadataEffect::ApplyRoCrate { request } => {
            metadata_request_persists_irokle(request.durability)
        }
        MetadataEffect::UpsertDataEntity { request }
        | MetadataEffect::UpsertContextualEntity { request } => {
            metadata_request_persists_irokle(request.durability)
        }
        MetadataEffect::SetGraphPolicy { .. }
        | MetadataEffect::AddGraphPeer { .. }
        | MetadataEffect::DeleteGraph { .. } => true,
        MetadataEffect::SyncGraphBestEffort { .. }
        | MetadataEffect::QueryGraphs { .. }
        | MetadataEffect::SearchGraphs { .. }
        | MetadataEffect::GetGraphPolicy { .. }
        | MetadataEffect::ExportRoCrate { .. }
        | MetadataEffect::ExportRoCrateSummary { .. }
        | MetadataEffect::ExportRoCratePage { .. }
        | MetadataEffect::ListGraphs
        | MetadataEffect::ContainsGraph { .. } => false,
    }
}

fn metadata_effect_defers_persist(effect: &MetadataEffect) -> bool {
    match effect {
        MetadataEffect::CreateCrate { request } => {
            request.durability == MetadataRequestDurability::WalAlreadyDurable
        }
        MetadataEffect::ApplyRoCrate { request } => {
            request.durability == MetadataRequestDurability::WalAlreadyDurable
        }
        MetadataEffect::UpsertDataEntity { request }
        | MetadataEffect::UpsertContextualEntity { request } => {
            request.durability == MetadataRequestDurability::WalAlreadyDurable
        }
        _ => false,
    }
}

fn metadata_effect_skips_lifecycle_read(effect: &MetadataEffect) -> bool {
    match effect {
        MetadataEffect::ValidateCreateCrate { .. } | MetadataEffect::ValidateRoCrate { .. } => true,
        MetadataEffect::CreateCrate { request } => {
            request.durability == MetadataRequestDurability::WalAlreadyDurable
        }
        MetadataEffect::ApplyRoCrate { request } => {
            request.durability == MetadataRequestDurability::WalAlreadyDurable
        }
        MetadataEffect::UpsertDataEntity { request }
        | MetadataEffect::UpsertContextualEntity { request } => {
            request.durability == MetadataRequestDurability::WalAlreadyDurable
        }
        _ => false,
    }
}

fn schedule_deferred_metadata_persist(
    inner: Arc<MetadataInner>,
    effect_name: &'static str,
    graph_iri: Option<String>,
) {
    inner
        .deferred_persist_requested
        .store(true, Ordering::Release);
    if inner.deferred_persist_running.swap(true, Ordering::AcqRel) {
        return;
    }

    let worker_inner = inner.clone();
    let worker_graph_iri = graph_iri.clone();
    let spawn_result = thread::Builder::new()
        .name("metadata-deferred-persist".to_string())
        .spawn(move || {
            loop {
                while worker_inner
                    .deferred_persist_requested
                    .swap(false, Ordering::AcqRel)
                {
                    run_deferred_metadata_flush(
                        &worker_inner,
                        effect_name,
                        worker_graph_iri.as_deref(),
                    );
                }

                worker_inner
                    .deferred_persist_running
                    .store(false, Ordering::Release);
                if !worker_inner
                    .deferred_persist_requested
                    .load(Ordering::Acquire)
                {
                    break;
                }
                if worker_inner
                    .deferred_persist_running
                    .swap(true, Ordering::AcqRel)
                {
                    break;
                }
            }
        });

    if let Err(error) = spawn_result {
        inner
            .deferred_persist_running
            .store(false, Ordering::Release);
        warn!(
            event = "metadata.backend.deferred_persist.spawn_failed",
            effect = effect_name,
            error = %error,
            "Failed to spawn deferred metadata persist"
        );
    }
}

fn run_deferred_metadata_flush(
    inner: &MetadataInner,
    effect_name: &'static str,
    graph_iri: Option<&str>,
) {
    let span = debug_span!(
        "metadata.backend.deferred_persist",
        effect = effect_name,
        graph_iri = graph_iri.unwrap_or("<none>"),
        elapsed_ms = field::Empty,
        result = field::Empty,
    );
    let started = Instant::now();
    let result = span.in_scope(|| flush_metadata_persistence(inner, effect_name, graph_iri));
    record_elapsed(&span, "elapsed_ms", started);
    match result {
        Ok(()) => {
            span.record("result", "ok");
        }
        Err(error) => {
            record_error(&span, &error.to_string());
            warn!(
                event = "metadata.backend.deferred_persist.failed",
                effect = effect_name,
                graph_iri = graph_iri.unwrap_or("<none>"),
                error = %error,
                "Deferred metadata persist failed"
            );
        }
    }
}

fn metadata_request_persists_irokle(durability: MetadataRequestDurability) -> bool {
    matches!(durability, MetadataRequestDurability::Durable)
}

fn upsert_data_entity(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    request: MetadataUpsertEntityRequest,
) -> Result<MetadataBatch, CraqleError> {
    let graph = GraphId::new(&request.graph_iri);
    if request.durability == MetadataRequestDurability::WalAlreadyDurable {
        return upsert_entity_via_rocrate_document(
            node,
            auth,
            graph,
            request,
            EntityUpsertKind::Data,
        );
    }
    let entity_request = craqle_entity_request(&graph, &request.jsonld)?;
    node.add_data_entity_with(auth, entity_request)
        .map(metadata_batch_from_craqle)
}

fn upsert_contextual_entity(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    request: MetadataUpsertEntityRequest,
) -> Result<MetadataBatch, CraqleError> {
    let graph = GraphId::new(&request.graph_iri);
    if request.durability == MetadataRequestDurability::WalAlreadyDurable {
        return upsert_entity_via_rocrate_document(
            node,
            auth,
            graph,
            request,
            EntityUpsertKind::Contextual,
        );
    }
    let entity_request = craqle_entity_request(&graph, &request.jsonld)?;
    node.add_contextual_entity_with(auth, entity_request)
        .map(metadata_batch_from_craqle)
}

#[derive(Clone, Copy)]
enum EntityUpsertKind {
    Data,
    Contextual,
}

fn upsert_entity_via_rocrate_document(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    graph: GraphId,
    request: MetadataUpsertEntityRequest,
    kind: EntityUpsertKind,
) -> Result<MetadataBatch, CraqleError> {
    let actor = request.deterministic_actor.map(ActorId::from_bytes);
    let entity_request = craqle_entity_request(&graph, &request.jsonld)?;
    let jsonld = graph_snapshot_jsonld(node, &graph)?;
    let merged_jsonld = merge_entity_into_rocrate_jsonld(
        &graph,
        &jsonld,
        &request.jsonld,
        &entity_request.entity_id,
        kind,
    )?;
    let policy = node.graph_policy(&graph)?;
    node.apply_rocrate_document_prevalidated_with_policy_and_durability_as(
        auth,
        graph,
        &merged_jsonld,
        policy,
        craqle_request_durability(request.durability),
        actor,
    )
    .map(metadata_batch_from_craqle)
}

fn graph_snapshot_jsonld(node: &CraqleNode, graph: &GraphId) -> Result<String, CraqleError> {
    let snapshot = node.graph_snapshot(graph)?;
    let mut by_subject = BTreeMap::<String, Map<String, Value>>::new();
    for quad in snapshot.quads {
        let subject_id = encoded_subject_id(&quad.subject)?;
        let predicate = quad.predicate.to_named_node().ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::UnsupportedTerm(quad.predicate.0.clone()))
        })?;
        let object = encoded_object_value(&quad.object)?;
        let entry = by_subject.entry(subject_id.clone()).or_insert_with(|| {
            Map::from_iter([("@id".to_string(), Value::String(subject_id.clone()))])
        });
        insert_jsonld_property(entry, predicate.as_str().to_string(), object);
    }

    let mut document = Map::new();
    document.insert(
        "@graph".to_string(),
        Value::Array(by_subject.into_values().map(Value::Object).collect()),
    );
    serde_json::to_string(&Value::Object(document))
        .map_err(|error| CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(error.to_string())))
}

fn encoded_subject_id(term: &craqle::EncodedTerm) -> Result<String, CraqleError> {
    match term.to_term() {
        Some(Term::NamedNode(node)) => Ok(node.as_str().to_string()),
        Some(Term::BlankNode(node)) => Ok(format!("_:{}", node.as_str())),
        _ => Err(CraqleError::RoCrate(RoCrateError::UnsupportedTerm(
            term.0.clone(),
        ))),
    }
}

fn encoded_object_value(term: &craqle::EncodedTerm) -> Result<Value, CraqleError> {
    match term.to_term() {
        Some(Term::NamedNode(node)) => Ok(entity_reference_value(node.as_str())),
        Some(Term::BlankNode(node)) => Ok(entity_reference_value(&format!("_:{}", node.as_str()))),
        Some(Term::Literal(literal)) => Ok(literal_value(literal)),
        _ => Err(CraqleError::RoCrate(RoCrateError::UnsupportedTerm(
            term.0.clone(),
        ))),
    }
}

fn literal_value(literal: Literal) -> Value {
    if let Some(language) = literal.language() {
        return Value::Object(Map::from_iter([
            (
                "@value".to_string(),
                Value::String(literal.value().to_string()),
            ),
            ("@language".to_string(), Value::String(language.to_string())),
        ]));
    }
    let datatype = literal.datatype();
    if datatype.as_str() != "http://www.w3.org/2001/XMLSchema#string" {
        return Value::Object(Map::from_iter([
            (
                "@value".to_string(),
                Value::String(literal.value().to_string()),
            ),
            (
                "@type".to_string(),
                Value::String(datatype.as_str().to_string()),
            ),
        ]));
    }
    Value::String(literal.value().to_string())
}

fn insert_jsonld_property(entry: &mut Map<String, Value>, key: String, value: Value) {
    match entry.get_mut(&key) {
        Some(existing) if existing == &value => {}
        Some(Value::Array(values)) => {
            if !values.contains(&value) {
                values.push(value);
            }
        }
        Some(existing) => {
            let old = existing.clone();
            *existing = Value::Array(vec![old, value]);
        }
        None => {
            entry.insert(key, value);
        }
    }
}

fn merge_entity_into_rocrate_jsonld(
    graph: &GraphId,
    rocrate_jsonld: &str,
    entity_jsonld: &str,
    entity_id: &str,
    kind: EntityUpsertKind,
) -> Result<String, CraqleError> {
    let mut rocrate: Value = serde_json::from_str(rocrate_jsonld).map_err(|error| {
        CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(error.to_string()))
    })?;
    let entity: Value = serde_json::from_str(entity_jsonld).map_err(|error| {
        CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(error.to_string()))
    })?;
    if !entity.is_object() {
        return Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
            "entity payload must be a JSON object".to_string(),
        )));
    }

    let graph_entries = rocrate_graph_entries_mut(&mut rocrate)?;
    match graph_entries
        .iter()
        .position(|entry| graph_entry_id(entry).as_deref() == Some(entity_id))
    {
        Some(index) => graph_entries[index] = entity,
        None => graph_entries.push(entity),
    }
    if matches!(kind, EntityUpsertKind::Data) {
        ensure_root_has_part(graph_entries, graph, entity_id)?;
    }

    serde_json::to_string(&rocrate)
        .map_err(|error| CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(error.to_string())))
}

fn rocrate_graph_entries_mut(rocrate: &mut Value) -> Result<&mut Vec<Value>, CraqleError> {
    let object = rocrate.as_object_mut().ok_or_else(|| {
        CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
            "top-level JSON-LD document must be an object".to_string(),
        ))
    })?;
    let graph_key = if object.contains_key("@graph") {
        "@graph"
    } else {
        "graph"
    };
    object
        .get_mut(graph_key)
        .and_then(Value::as_array_mut)
        .ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "RO-Crate import requires a top-level `@graph` array".to_string(),
            ))
        })
}

fn graph_entry_id(entry: &Value) -> Option<String> {
    entry
        .as_object()?
        .get("@id")
        .or_else(|| entry.as_object()?.get("id"))
        .and_then(Value::as_str)
        .map(normalize_entity_id)
}

fn ensure_root_has_part(
    graph_entries: &mut [Value],
    graph: &GraphId,
    entity_id: &str,
) -> Result<(), CraqleError> {
    let root_id = graph.as_str();
    let Some(root) = graph_entries
        .iter_mut()
        .find(|entry| graph_entry_id(entry).as_deref() == Some(root_id))
        .and_then(Value::as_object_mut)
    else {
        return Err(CraqleError::RoCrate(RoCrateError::InvalidGraph(format!(
            "root entity missing for graph `{root_id}`"
        ))));
    };
    let has_part_key = has_part_key(root);
    let reference = entity_reference_value(entity_id);
    match root.get_mut(has_part_key) {
        Some(value) if entity_reference_contains(value, entity_id) => {}
        Some(Value::Array(values)) => values.push(reference),
        Some(value) => {
            let existing = value.clone();
            *value = Value::Array(vec![existing, reference]);
        }
        None => {
            root.insert(has_part_key.to_string(), Value::Array(vec![reference]));
        }
    }
    Ok(())
}

fn has_part_key(root: &Map<String, Value>) -> &'static str {
    if root.contains_key("hasPart") {
        "hasPart"
    } else if root.contains_key("schema:hasPart") {
        "schema:hasPart"
    } else if root.contains_key("http://schema.org/hasPart") {
        "http://schema.org/hasPart"
    } else if root.contains_key("https://schema.org/hasPart") {
        "https://schema.org/hasPart"
    } else {
        "hasPart"
    }
}

fn entity_reference_value(entity_id: &str) -> Value {
    Value::Object(Map::from_iter([(
        "@id".to_string(),
        Value::String(entity_id.to_string()),
    )]))
}

fn entity_reference_contains(value: &Value, entity_id: &str) -> bool {
    match value {
        Value::Array(values) => values
            .iter()
            .any(|value| entity_reference_contains(value, entity_id)),
        Value::Object(object) => {
            object
                .get("@id")
                .or_else(|| object.get("id"))
                .and_then(Value::as_str)
                .map(normalize_entity_id)
                .as_deref()
                == Some(entity_id)
        }
        _ => false,
    }
}

fn craqle_entity_request(
    graph: &GraphId,
    jsonld: &str,
) -> Result<CreateEntityRequest, CraqleError> {
    let value: Value = serde_json::from_str(jsonld).map_err(|error| {
        CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(error.to_string()))
    })?;
    let object = value.as_object().ok_or_else(|| {
        CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
            "entity payload must be a JSON object".to_string(),
        ))
    })?;
    if object.contains_key("@graph") || object.contains_key("graph") {
        return Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
            "entity payload must not contain `@graph`; send a single JSON-LD entity object"
                .to_string(),
        )));
    }

    let entity_id = entity_identifier(object)?;
    let mut entity_types = entity_types(object)?;
    let entity_type = entity_types.remove(0);
    let name = entity_name(object)?;
    let mut additional_triples = Vec::new();
    for extra_type in entity_types {
        additional_triples.push((vocab::rdf_type(), class_term(&extra_type)?));
    }

    for (property, property_value) in object {
        if matches!(
            property.as_str(),
            "@context" | "@id" | "id" | "@type" | "type" | "name"
        ) {
            continue;
        }
        let property = normalize_property(property);
        let predicate = property_named_node(&property)?;
        for object in property_value_terms(&property, property_value)? {
            additional_triples.push((predicate.clone(), object));
        }
    }

    Ok(CreateEntityRequest {
        graph: graph.clone(),
        entity_id,
        entity_type,
        name,
        additional_triples,
    })
}

fn entity_identifier(object: &serde_json::Map<String, Value>) -> Result<String, CraqleError> {
    object
        .get("@id")
        .or_else(|| object.get("id"))
        .and_then(Value::as_str)
        .map(normalize_entity_id)
        .ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "entity payload must define string `@id`".to_string(),
            ))
        })
}

fn entity_types(object: &serde_json::Map<String, Value>) -> Result<Vec<String>, CraqleError> {
    let value = object
        .get("@type")
        .or_else(|| object.get("type"))
        .ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "entity payload must define `@type`".to_string(),
            ))
        })?;
    let mut types = Vec::new();
    match value {
        Value::String(value) => types.push(value.clone()),
        Value::Array(values) => {
            for value in values {
                let Some(value) = value.as_str() else {
                    return Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                        "entity `@type` arrays must contain only strings".to_string(),
                    )));
                };
                types.push(value.to_string());
            }
        }
        _ => {
            return Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "entity `@type` must be a string or array of strings".to_string(),
            )));
        }
    }
    if types.is_empty() {
        return Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
            "entity `@type` must not be empty".to_string(),
        )));
    }
    Ok(types)
}

fn entity_name(object: &serde_json::Map<String, Value>) -> Result<String, CraqleError> {
    object
        .get("name")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "entity payload must define string `name`".to_string(),
            ))
        })
}

fn property_named_node(property: &str) -> Result<NamedNode, CraqleError> {
    match property {
        "@type" | "type" => Ok(vocab::rdf_type()),
        "name" => Ok(vocab::schema_name()),
        "description" => Ok(vocab::schema_description()),
        "keywords" => Ok(vocab::schema_keywords()),
        "datePublished" => Ok(vocab::schema_date_published()),
        "license" => Ok(vocab::schema_license()),
        "about" => Ok(vocab::schema_about()),
        "conformsTo" => Ok(vocab::schema_conforms_to()),
        other if other.contains("://") => Ok(NamedNode::new_unchecked(other)),
        other if other.contains(':') => expand_known_compact_iri(other),
        other => Ok(NamedNode::new_unchecked(format!(
            "http://schema.org/{}",
            normalize_term(other)
        ))),
    }
}

fn property_value_terms(property: &str, value: &Value) -> Result<Vec<Term>, CraqleError> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Bool(boolean) => Ok(vec![Term::Literal(Literal::new_typed_literal(
            boolean.to_string(),
            NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
        ))]),
        Value::Number(number) => Ok(vec![number_literal(number)]),
        Value::String(text) => {
            let mapped = normalize_entity_id(text);
            let value = if property_expects_identifier(property) {
                mapped.as_str()
            } else {
                text
            };
            Ok(vec![property_value_term(property, value)?])
        }
        Value::Array(values) => {
            let mut objects = Vec::new();
            for entry in values {
                objects.extend(property_value_terms(property, entry)?);
            }
            Ok(objects)
        }
        Value::Object(object) if is_reference_object(object) => {
            let id = object
                .get("@id")
                .or_else(|| object.get("id"))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(format!(
                        "property `{property}` reference object is missing string `@id`"
                    )))
                })?;
            Ok(vec![reference_term(&normalize_entity_id(id))?])
        }
        Value::Object(object) if is_value_object(object) => Ok(vec![value_object_term(object)?]),
        Value::Object(_) => Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
            format!(
                "property `{property}` contains an inline nested object; nested entities must be separate top-level entities referenced by `@id`"
            ),
        ))),
    }
}

fn property_value_term(property: &str, value: &str) -> Result<Term, CraqleError> {
    match property {
        "@type" | "type" => class_term(value),
        "license" | "about" | "conformsTo" => {
            if looks_like_identifier(value) {
                reference_term(value)
            } else {
                Ok(Term::Literal(Literal::new_simple_literal(value)))
            }
        }
        _ => Ok(Term::Literal(Literal::new_simple_literal(value))),
    }
}

fn class_term(value: &str) -> Result<Term, CraqleError> {
    let iri = if value.starts_with("http://") || value.starts_with("https://") {
        value.to_string()
    } else if value.contains(':') {
        expand_known_compact_iri(value)?.as_str().to_string()
    } else {
        format!("http://schema.org/{}", normalize_term(value))
    };
    Ok(Term::NamedNode(NamedNode::new_unchecked(iri)))
}

fn reference_term(value: &str) -> Result<Term, CraqleError> {
    if let Some(value) = value.strip_prefix("_:") {
        Ok(Term::BlankNode(BlankNode::new_unchecked(value)))
    } else if value.starts_with("./")
        || value.starts_with("../")
        || value.starts_with('#')
        || value.contains("://")
    {
        Ok(Term::NamedNode(NamedNode::new_unchecked(value)))
    } else if value.contains(':') {
        Ok(Term::NamedNode(expand_known_compact_iri(value)?))
    } else {
        Err(CraqleError::RoCrate(RoCrateError::UnsupportedTerm(
            value.to_string(),
        )))
    }
}

fn number_literal(number: &serde_json::Number) -> Term {
    let datatype = if number.as_i64().is_some() || number.as_u64().is_some() {
        "http://www.w3.org/2001/XMLSchema#integer"
    } else {
        "http://www.w3.org/2001/XMLSchema#double"
    };
    Term::Literal(Literal::new_typed_literal(
        number.to_string(),
        NamedNode::new_unchecked(datatype),
    ))
}

fn value_object_term(object: &serde_json::Map<String, Value>) -> Result<Term, CraqleError> {
    let value = object
        .get("@value")
        .or_else(|| object.get("value"))
        .ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "value object missing `@value`".to_string(),
            ))
        })?;
    let language = object
        .get("@language")
        .or_else(|| object.get("language"))
        .and_then(Value::as_str);
    let datatype = object
        .get("@type")
        .or_else(|| object.get("type"))
        .and_then(Value::as_str);

    match value {
        Value::String(text) => {
            if let Some(language) = language {
                Ok(Term::Literal(
                    Literal::new_language_tagged_literal_unchecked(text, language),
                ))
            } else if let Some(datatype) = datatype {
                Ok(Term::Literal(Literal::new_typed_literal(
                    text.clone(),
                    datatype_named_node(datatype)?,
                )))
            } else {
                Ok(Term::Literal(Literal::new_simple_literal(text)))
            }
        }
        Value::Bool(boolean) => Ok(Term::Literal(Literal::new_typed_literal(
            boolean.to_string(),
            datatype
                .map(datatype_named_node)
                .transpose()?
                .unwrap_or_else(|| {
                    NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean")
                }),
        ))),
        Value::Number(number) => Ok(Term::Literal(Literal::new_typed_literal(
            number.to_string(),
            datatype
                .map(datatype_named_node)
                .transpose()?
                .unwrap_or_else(|| {
                    if number.as_i64().is_some() || number.as_u64().is_some() {
                        NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer")
                    } else {
                        NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#double")
                    }
                }),
        ))),
        Value::Null => Ok(Term::Literal(Literal::new_simple_literal(""))),
        Value::Array(_) | Value::Object(_) => Err(CraqleError::RoCrate(
            RoCrateError::UnsupportedJsonLd("value object `@value` must be scalar".to_string()),
        )),
    }
}

fn datatype_named_node(datatype: &str) -> Result<NamedNode, CraqleError> {
    if datatype.starts_with("http://") || datatype.starts_with("https://") {
        Ok(NamedNode::new_unchecked(datatype))
    } else {
        expand_known_compact_iri(datatype)
    }
}

fn expand_known_compact_iri(value: &str) -> Result<NamedNode, CraqleError> {
    if let Some(local) = value.strip_prefix("schema:") {
        Ok(NamedNode::new_unchecked(format!(
            "http://schema.org/{local}"
        )))
    } else if let Some(local) = value.strip_prefix("rdf:") {
        Ok(NamedNode::new_unchecked(format!(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#{local}"
        )))
    } else if let Some(local) = value.strip_prefix("rdfs:") {
        Ok(NamedNode::new_unchecked(format!(
            "http://www.w3.org/2000/01/rdf-schema#{local}"
        )))
    } else {
        Err(CraqleError::RoCrate(RoCrateError::UnsupportedTerm(
            value.to_string(),
        )))
    }
}

fn normalize_property(property: &str) -> String {
    property
        .strip_prefix("schema:")
        .or_else(|| property.strip_prefix("http://schema.org/"))
        .or_else(|| property.strip_prefix("https://schema.org/"))
        .map(str::to_string)
        .unwrap_or_else(|| property.to_string())
}

fn normalize_term(term: &str) -> String {
    normalize_property(term)
}

fn normalize_entity_id(id: &str) -> String {
    if id == "ro-crate-metadata.json"
        || id.starts_with("./")
        || id.starts_with("../")
        || id.starts_with('#')
        || id.starts_with("_:")
        || id.contains("://")
        || (id.contains(':') && !id.contains('/'))
    {
        id.to_string()
    } else {
        format!("./{id}")
    }
}

fn property_expects_identifier(property: &str) -> bool {
    matches!(property, "license" | "about" | "conformsTo")
}

fn is_reference_object(object: &serde_json::Map<String, Value>) -> bool {
    let has_identifier = object.contains_key("@id") || object.contains_key("id");
    has_identifier
        && object
            .keys()
            .all(|key| matches!(key.as_str(), "@id" | "id" | "@type" | "type"))
}

fn is_value_object(object: &serde_json::Map<String, Value>) -> bool {
    let has_value = object.contains_key("@value") || object.contains_key("value");
    has_value
        && object.keys().all(|key| {
            matches!(
                key.as_str(),
                "@value" | "value" | "@type" | "type" | "@language" | "language"
            )
        })
}

fn looks_like_identifier(value: &str) -> bool {
    value.starts_with("./")
        || value.starts_with("../")
        || value.starts_with('#')
        || value.starts_with("_:")
        || value.contains("://")
        || (value.contains(':') && !value.contains(' '))
}

fn metadata_error_from_craqle(error: CraqleError) -> MetadataError {
    match error {
        CraqleError::RoCrate(rocrate_error) => match rocrate_error {
            RoCrateError::InvalidGraph(_)
            | RoCrateError::EntityNotFound(_)
            | RoCrateError::UnsupportedJsonLd(_)
            | RoCrateError::UnsupportedTerm(_)
            | RoCrateError::InvalidBatch(_) => {
                MetadataError::InvalidInput(rocrate_error.to_string())
            }
            other => MetadataError::Backend(other.to_string()),
        },
        CraqleError::SyncInputRejected(message) => MetadataError::InvalidInput(message),
        CraqleError::MultiGraphUpdateUnsupported => {
            MetadataError::InvalidInput("unsupported update across multiple graphs".to_string())
        }
        CraqleError::Update(craqle::UpdateError::ValidationFailed(violations)) => {
            MetadataError::InvalidInput(format!("validation failed: {violations:?}"))
        }
        other => MetadataError::Backend(other.to_string()),
    }
}

fn record_duration(span: &Span, field: &'static str, duration: Duration) {
    span.record(field, duration_ms(duration));
}

fn record_elapsed(span: &Span, field: &'static str, started: Instant) {
    record_duration(span, field, started.elapsed());
}

fn record_error(span: &Span, error: &str) {
    span.record("result", "error");
    span.record("error", field::display(error));
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_description", field::display(error));
}

fn warn_if_slow_metadata_backend(
    operation: &'static str,
    graph_iri: Option<&str>,
    duration: Duration,
) {
    CRAQLE_LATENCY.record(operation, duration);
    if duration >= SLOW_METADATA_BACKEND_THRESHOLD {
        warn!(
            event = "metadata.backend.slow_call",
            operation,
            graph_iri = graph_iri.unwrap_or("<none>"),
            duration_ms = duration_ms(duration),
            threshold_ms = duration_ms(SLOW_METADATA_BACKEND_THRESHOLD),
            "Slow metadata backend call"
        );
    }
}

fn record_craqle_call_result<T>(
    span: &Span,
    operation: &'static str,
    graph_iri: Option<&str>,
    started: Instant,
    result: &Result<T, CraqleError>,
) {
    let duration = started.elapsed();
    record_duration(span, "elapsed_ms", duration);
    match result {
        Ok(_) => {
            span.record("result", "ok");
            span.record("otel.status_code", "OK");
        }
        Err(error) => record_error(span, &error.to_string()),
    }
    warn_if_slow_metadata_backend(operation, graph_iri, duration);
}

fn record_metadata_result(
    span: &Span,
    operation: &'static str,
    graph_iri: Option<&str>,
    started: Instant,
    result: &Result<MetadataEvent, CraqleError>,
) {
    record_craqle_call_result(span, operation, graph_iri, started, result);
}

fn metadata_effect_kind(effect: &MetadataEffect) -> &'static str {
    match effect {
        MetadataEffect::ValidateCreateCrate { .. } => "validate_create_crate",
        MetadataEffect::ValidateRoCrate { .. } => "validate_rocrate",
        MetadataEffect::CreateCrate { .. } => "create_crate",
        MetadataEffect::ApplyRoCrate { .. } => "apply_rocrate",
        MetadataEffect::UpsertDataEntity { .. } => "upsert_data_entity",
        MetadataEffect::UpsertContextualEntity { .. } => "upsert_contextual_entity",
        MetadataEffect::SetGraphPolicy { .. } => "set_graph_policy",
        MetadataEffect::AddGraphPeer { .. } => "add_graph_peer",
        MetadataEffect::SyncGraphBestEffort { .. } => "sync_graph_best_effort",
        MetadataEffect::GetGraphPolicy { .. } => "get_graph_policy",
        MetadataEffect::ExportRoCrate { .. } => "export_rocrate",
        MetadataEffect::ExportRoCrateSummary { .. } => "export_rocrate_summary",
        MetadataEffect::ExportRoCratePage { .. } => "export_rocrate_page",
        MetadataEffect::SearchGraphs { .. } => "search_graphs",
        MetadataEffect::QueryGraphs { .. } => "query_graphs",
        MetadataEffect::DeleteGraph { .. } => "delete_graph",
        MetadataEffect::ListGraphs => "list_graphs",
        MetadataEffect::ContainsGraph { .. } => "contains_graph",
    }
}

fn metadata_event_kind(event: &MetadataEvent) -> &'static str {
    match event {
        MetadataEvent::ValidationResult { .. } => "validation_result",
        MetadataEvent::CreateCrateResult { .. } => "create_crate_result",
        MetadataEvent::ApplyRoCrateResult { .. } => "apply_rocrate_result",
        MetadataEvent::EntityUpsertResult { .. } => "entity_upsert_result",
        MetadataEvent::GraphPolicySet { .. } => "graph_policy_set",
        MetadataEvent::GraphPeerAdded { .. } => "graph_peer_added",
        MetadataEvent::GraphSyncScheduled { .. } => "graph_sync_scheduled",
        MetadataEvent::GraphPolicyResult { .. } => "graph_policy_result",
        MetadataEvent::RoCrateExportResult { .. } => "rocrate_export_result",
        MetadataEvent::RoCrateSummaryResult { .. } => "rocrate_summary_result",
        MetadataEvent::RoCratePageResult { .. } => "rocrate_page_result",
        MetadataEvent::SearchResult { .. } => "search_result",
        MetadataEvent::QueryResult { .. } => "query_result",
        MetadataEvent::GraphDeleted { .. } => "graph_deleted",
        MetadataEvent::GraphListResult { .. } => "graph_list_result",
        MetadataEvent::ContainsGraphResult { .. } => "contains_graph_result",
        MetadataEvent::Error { .. } => "error",
    }
}

fn metadata_query_result_kind(results: &MetadataQueryResults) -> &'static str {
    match results {
        MetadataQueryResults::Solutions(_) => "solutions",
        MetadataQueryResults::Boolean(_) => "boolean",
        MetadataQueryResults::Graph(_) => "graph",
    }
}

fn record_metadata_query_result_counts(span: &Span, results: &MetadataQueryResults) {
    match results {
        MetadataQueryResults::Solutions(rows) => {
            span.record("row_count", rows.len() as u64);
        }
        MetadataQueryResults::Boolean(_) => {
            span.record("row_count", 1u64);
        }
        MetadataQueryResults::Graph(triples) => {
            span.record("triple_count", triples.len() as u64);
        }
    }
}

fn metadata_transport_message_kind(message: &MetadataTransportMessage) -> &'static str {
    match message {
        MetadataTransportMessage::QueryGraphs { .. } => "query_graphs",
        MetadataTransportMessage::QueryResults { .. } => "query_results",
        MetadataTransportMessage::SearchGraphs { .. } => "search_graphs",
        MetadataTransportMessage::SearchResults { .. } => "search_results",
        MetadataTransportMessage::Reject(_) => "reject",
    }
}

fn effect_graph_iri(effect: &MetadataEffect) -> Option<String> {
    match effect {
        MetadataEffect::ValidateCreateCrate { request } => Some(request.graph_iri.clone()),
        MetadataEffect::ValidateRoCrate { request } => Some(request.graph_iri.clone()),
        MetadataEffect::CreateCrate { request } => Some(request.graph_iri.clone()),
        MetadataEffect::ApplyRoCrate { request } => Some(request.graph_iri.clone()),
        MetadataEffect::UpsertDataEntity { request }
        | MetadataEffect::UpsertContextualEntity { request } => Some(request.graph_iri.clone()),
        MetadataEffect::SetGraphPolicy { graph_iri, .. }
        | MetadataEffect::AddGraphPeer { graph_iri, .. }
        | MetadataEffect::SyncGraphBestEffort { graph_iri, .. }
        | MetadataEffect::GetGraphPolicy { graph_iri }
        | MetadataEffect::ExportRoCrate { graph_iri }
        | MetadataEffect::ExportRoCrateSummary { graph_iri }
        | MetadataEffect::DeleteGraph { graph_iri }
        | MetadataEffect::ContainsGraph { graph_iri } => Some(graph_iri.clone()),
        MetadataEffect::ExportRoCratePage { graph_iri, .. } => Some(graph_iri.clone()),
        MetadataEffect::SearchGraphs { graph_iris, .. } => graph_iris
            .as_ref()
            .and_then(|graph_iris| graph_iris.first().cloned()),
        MetadataEffect::QueryGraphs { graph_iris, .. } => graph_iris
            .as_ref()
            .and_then(|graph_iris| graph_iris.first().cloned()),
        MetadataEffect::ListGraphs => None,
    }
}

fn graph_ids(graph_iris: &[String]) -> Vec<GraphId> {
    graph_iris
        .iter()
        .map(|graph_iri| GraphId::new(graph_iri))
        .collect()
}

fn craqle_create_request(request: MetadataCreateCrateRequest) -> CreateCrateRequest {
    CreateCrateRequest::new(
        GraphId::new(&request.graph_iri),
        request.name,
        request.description,
        request.date_published,
        request.license,
        craqle_graph_policy(request.policy),
    )
}

fn craqle_request_durability(durability: MetadataRequestDurability) -> CraqleRequestDurability {
    match durability {
        MetadataRequestDurability::Durable => CraqleRequestDurability::Durable,
        MetadataRequestDurability::WalAlreadyDurable => CraqleRequestDurability::WalAlreadyDurable,
    }
}

fn craqle_graph_policy(policy: MetadataGraphPolicy) -> GraphPolicy {
    GraphPolicy {
        public: policy.public,
        permission_paths: policy.permission_paths,
    }
}

fn irokle_peer_id(node_id: NodeId) -> irokle::PeerId {
    irokle::PeerId::from_bytes(*node_id.as_bytes())
}

fn metadata_graph_policy_from_craqle(policy: GraphPolicy) -> MetadataGraphPolicy {
    MetadataGraphPolicy {
        public: policy.public,
        permission_paths: policy.permission_paths,
    }
}

fn metadata_query_results_from_craqle(results: QueryResults) -> MetadataQueryResults {
    match results {
        QueryResults::Solutions(rows) => MetadataQueryResults::Solutions(
            rows.into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|(key, value)| (key, value.0))
                        .collect::<BTreeMap<_, _>>()
                })
                .collect(),
        ),
        QueryResults::Boolean(value) => MetadataQueryResults::Boolean(value),
        QueryResults::Graph(triples) => MetadataQueryResults::Graph(
            triples
                .into_iter()
                .map(|(subject, predicate, object)| (subject.0, predicate.0, object.0))
                .collect(),
        ),
    }
}

fn metadata_dot_from_craqle(dot: craqle::Dot) -> MetadataDot {
    MetadataDot {
        actor: *dot.actor.as_bytes(),
        counter: dot.counter,
    }
}

fn metadata_batch_from_craqle(batch: Batch) -> MetadataBatch {
    MetadataBatch {
        graph_iri: batch.graph.as_str().to_string(),
        actor: *batch.actor.as_bytes(),
        counter: batch.counter,
        base_clock: batch.base_clock,
        ops: batch
            .ops
            .into_iter()
            .map(|op| match op {
                craqle::QuadOp::Add {
                    subject,
                    predicate,
                    object,
                    dot,
                } => MetadataQuadOp::Add {
                    subject: subject.0,
                    predicate: predicate.0,
                    object: object.0,
                    dot: metadata_dot_from_craqle(dot),
                },
                craqle::QuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed,
                } => MetadataQuadOp::Remove {
                    subject: subject.0,
                    predicate: predicate.0,
                    object: object.0,
                    witnessed,
                },
            })
            .collect(),
        timestamp_millis: batch.timestamp.timestamp_millis(),
    }
}

fn metadata_rocrate_page_from_craqle(page: craqle::RoCratePage) -> MetadataRoCratePage {
    MetadataRoCratePage {
        jsonld: page.jsonld,
        total_data_entities: page.total_data_entities,
        returned_data_entities: page.returned_data_entities,
        next_offset: page.next_offset,
        next_cursor: page.next_cursor,
    }
}

fn metadata_search_hit_from_craqle(
    hit: craqle::SearchHit,
    record: &MetadataRegistryRecord,
) -> MetadataSearchHit {
    MetadataSearchHit {
        document_id: record.document_id.to_string(),
        group_id: record.group_id.to_string(),
        document_path: record.document_path.clone(),
        graph_iri: hit.graph_id,
        subject_iri: hit.subject_iri,
        score: hit.score,
    }
}

#[tracing::instrument(
    name = "metadata.registry.list_local",
    level = "debug",
    skip(inner),
    fields(
        cache_hit = field::Empty,
        stale = field::Empty,
        record_count = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
async fn list_local_registry_records(
    inner: Arc<MetadataInner>,
) -> Result<Arc<Vec<MetadataRegistryRecord>>, MetadataError> {
    let started = Instant::now();
    let span = Span::current();
    match inner.visibility_cache.registry_records_any() {
        Some((records, fresh)) => {
            if !fresh {
                spawn_visibility_cache_refill(inner.clone());
            }
            span.record("cache_hit", true);
            span.record("stale", !fresh);
            span.record("record_count", records.len() as u64);
            record_elapsed(&span, "elapsed_ms", started);
            Ok(records)
        }
        None => {
            // Cold start only: block until the first fill completes.
            let _fill = inner
                .visibility_cache
                .registry_fill
                .clone()
                .lock_owned()
                .await;
            if let Some((records, true)) = inner.visibility_cache.registry_records_any() {
                span.record("cache_hit", true);
                span.record("stale", false);
                span.record("record_count", records.len() as u64);
                record_elapsed(&span, "elapsed_ms", started);
                return Ok(records);
            }
            span.record("cache_hit", false);
            let records = fill_visibility_caches(&inner).await?;
            span.record("record_count", records.len() as u64);
            record_elapsed(&span, "elapsed_ms", started);
            Ok(records)
        }
    }
}

#[tracing::instrument(
    name = "metadata.registry.list_local_group",
    level = "debug",
    skip(inner),
    fields(
        group_id = %group_id,
        cache_hit = field::Empty,
        stale = field::Empty,
        record_count = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
async fn list_local_registry_records_for_group(
    inner: Arc<MetadataInner>,
    group_id: GroupId,
) -> Result<Arc<Vec<MetadataRegistryRecord>>, MetadataError> {
    let started = Instant::now();
    let span = Span::current();
    match inner
        .visibility_cache
        .registry_records_for_group_any(group_id)
    {
        Some((records, fresh)) => {
            if !fresh {
                spawn_visibility_cache_refill(inner.clone());
            }
            span.record("cache_hit", true);
            span.record("stale", !fresh);
            span.record("record_count", records.len() as u64);
            record_elapsed(&span, "elapsed_ms", started);
            Ok(records)
        }
        None => {
            let _fill = inner
                .visibility_cache
                .registry_fill
                .clone()
                .lock_owned()
                .await;
            if let Some((records, true)) = inner
                .visibility_cache
                .registry_records_for_group_any(group_id)
            {
                span.record("cache_hit", true);
                span.record("stale", false);
                span.record("record_count", records.len() as u64);
                record_elapsed(&span, "elapsed_ms", started);
                return Ok(records);
            }
            span.record("cache_hit", false);
            fill_visibility_caches(&inner).await?;
            let records = inner
                .visibility_cache
                .registry_records_for_group_any(group_id)
                .map(|(records, _)| records)
                .unwrap_or_else(|| Arc::new(Vec::new()));
            span.record("record_count", records.len() as u64);
            record_elapsed(&span, "elapsed_ms", started);
            Ok(records)
        }
    }
}

// Single-flight background refill; readers keep being served the stale entry
// until the new Arc is swapped in.
fn spawn_visibility_cache_refill(inner: Arc<MetadataInner>) {
    let Ok(guard) = inner
        .visibility_cache
        .registry_fill
        .clone()
        .try_lock_owned()
    else {
        return;
    };
    tokio::spawn(async move {
        let _guard = guard;
        if let Some((_, true)) = inner.visibility_cache.registry_records_any() {
            return;
        }
        if let Err(error) = fill_visibility_caches(&inner).await {
            warn!(
                event = "metadata.visibility.refill_failed",
                error = %error,
                "Background metadata visibility cache refill failed; serving stale entries"
            );
        }
    });
}

#[tracing::instrument(
    name = "metadata.visibility.fill",
    level = "debug",
    skip(inner),
    fields(
        registry_pages = field::Empty,
        lifecycle_pages = field::Empty,
        record_count = field::Empty,
        deleted_count = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
async fn fill_visibility_caches(
    inner: &Arc<MetadataInner>,
) -> Result<Arc<Vec<MetadataRegistryRecord>>, MetadataError> {
    let started = Instant::now();
    let span = Span::current();
    let fill_generation = inner.visibility_cache.current_generation();

    let mut records = Vec::new();
    let mut start_after = None;
    let mut registry_pages = 0usize;
    loop {
        let event = inner
            .storage_handle
            .send_effect(iter_all_registry_effect(start_after, None))
            .await;
        let (mut page, next_start_after) = parse_registry_iter(event).map_err(|error| {
            MetadataError::Backend(format!("metadata registry iteration failed: {error:?}"))
        })?;
        registry_pages += 1;
        records.append(&mut page);
        match next_start_after {
            Some(cursor) => start_after = Some(cursor),
            None => break,
        }
    }
    span.record("registry_pages", registry_pages as u64);
    span.record("record_count", records.len() as u64);
    // The registry keyspace iterates in (group, document) order; snapshot
    // consumers binary-search by document id (registry_record_for_graph).
    records.sort_unstable_by_key(|record| record.document_id);

    // Lifecycle records are deletion tombstones, so one keyspace sweep
    // refreshes the deleted-state of every registry graph without per-graph
    // point reads.
    let (deleted_graphs, lifecycle_pages) = list_deleted_graph_iris(inner).await?;
    span.record("lifecycle_pages", lifecycle_pages as u64);
    span.record("deleted_count", deleted_graphs.len() as u64);

    let lifecycle_entries = records
        .iter()
        .map(|record| {
            (
                record.graph_iri.clone(),
                deleted_graphs.contains(&record.graph_iri),
            )
        })
        .collect::<Vec<_>>();
    records.retain(|record| !deleted_graphs.contains(&record.graph_iri));
    let records = Arc::new(records);
    inner.visibility_cache.store_visibility_fill(
        records.clone(),
        lifecycle_entries,
        fill_generation,
    );
    record_elapsed(&span, "elapsed_ms", started);
    Ok(records)
}

async fn list_deleted_graph_iris(
    inner: &Arc<MetadataInner>,
) -> Result<(HashSet<String>, usize), MetadataError> {
    let mut deleted = HashSet::new();
    let mut start_after = None;
    let mut pages = 0usize;
    loop {
        let event = inner
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Iter {
                key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.map(IterStart::After),
                limit: REGISTRY_FILL_PAGE_SIZE,
                txn_id: None,
            }))
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(MetadataError::Backend(format!(
                    "metadata graph lifecycle iteration failed: {error:?}"
                )));
            }
            other => {
                return Err(MetadataError::Backend(format!(
                    "unexpected metadata graph lifecycle iteration result: {other:?}"
                )));
            }
        };
        pages += 1;
        for (_, value) in values {
            let record: MetadataGraphLifecycleRecord = postcard::from_bytes(&value)
                .map_err(|error| MetadataError::Backend(error.to_string()))?;
            if record.is_deleted() {
                deleted.insert(record.graph_iri);
            }
        }
        match next_start_after {
            Some(cursor) => start_after = Some(cursor),
            None => break,
        }
    }
    Ok((deleted, pages))
}

#[tracing::instrument(
    name = "metadata.query.local",
    level = "debug",
    skip(inner, auth_context, sparql),
    fields(
        query_len = sparql.len() as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        registry_records = field::Empty,
        authorized_graphs = field::Empty,
        registry_ms = field::Empty,
        authorization_ms = field::Empty,
        craqle_query_ms = field::Empty,
        elapsed_ms = field::Empty,
        result = field::Empty,
        row_count = field::Empty,
        triple_count = field::Empty,
    )
)]
async fn query_local_graphs(
    inner: Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    sparql: String,
) -> Result<MetadataQueryResults, MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();

    let registry_started = Instant::now();
    let records = list_local_registry_records(inner.clone()).await?;
    record_elapsed(&span, "registry_ms", registry_started);
    span.record("registry_records", records.len() as u64);

    let authorization_started = Instant::now();
    // Document-scoped queries keep the eager per-record selection; the
    // all-metadata path defers per-graph visibility to query evaluation.
    let scope = match graph_iris {
        Some(graph_iris) => LocalReadScope::Eager(
            select_authorized_graphs(inner.clone(), auth_context, records, Some(graph_iris))
                .await?,
        ),
        None => LocalReadScope::Lazy(
            resolve_graph_visibility_scope(&inner, auth_context, records).await?,
        ),
    };
    record_elapsed(&span, "authorization_ms", authorization_started);
    let lazy = match &scope {
        LocalReadScope::Eager(allowed) => {
            span.record("authorized_graphs", allowed.len() as u64);
            false
        }
        LocalReadScope::Lazy(scope) => {
            span.record("readable_groups", scope.readable_groups.len() as u64);
            true
        }
    };

    let query_span = debug_span!(
        "metadata.backend.craqle.query_graphs",
        lazy,
        graph_count = field::Empty,
        query_len = sparql.len() as u64,
        elapsed_ms = field::Empty,
        result = field::Empty,
        row_count = field::Empty,
        triple_count = field::Empty,
    );
    if let LocalReadScope::Eager(allowed) = &scope {
        query_span.record("graph_count", allowed.len() as u64);
    }
    let blocking_span = query_span.clone();
    let query_started = Instant::now();
    // Queries are reads: take from the read pool so they never queue behind
    // long-running materializations holding the mutation permits.
    let _permit = inner.craqle_read_permits.clone().acquire_owned().await.ok();
    let result = match tokio::task::spawn_blocking(move || {
        blocking_span.in_scope(|| {
            match scope {
                LocalReadScope::Eager(allowed) => {
                    inner.node.query_graphs(&graph_ids(&allowed), &sparql)
                }
                LocalReadScope::Lazy(scope) => inner.node.query_graphs_with(
                    |graph| scope.graph_visible(&inner.visibility_cache, graph.as_str()),
                    &sparql,
                ),
            }
            .map(metadata_query_results_from_craqle)
            .map_err(|error| MetadataError::Backend(error.to_string()))
        })
    })
    .await
    {
        Ok(result) => result,
        Err(error) => Err(MetadataError::TaskJoin(error.to_string())),
    };
    let query_elapsed = query_started.elapsed();
    record_duration(&query_span, "elapsed_ms", query_elapsed);
    record_duration(&span, "craqle_query_ms", query_elapsed);
    match &result {
        Ok(results) => {
            query_span.record("result", metadata_query_result_kind(results));
            span.record("result", metadata_query_result_kind(results));
            record_metadata_query_result_counts(&query_span, results);
            record_metadata_query_result_counts(&span, results);
        }
        Err(error) => {
            record_error(&query_span, &error.to_string());
            record_error(&span, &error.to_string());
        }
    }
    warn_if_slow_metadata_backend("query_graphs", None, query_elapsed);
    record_elapsed(&span, "elapsed_ms", total_started);
    result
}

#[tracing::instrument(
    name = "metadata.search.local",
    level = "debug",
    skip(inner, auth_context, query),
    fields(
        query_len = query.len() as u64,
        limit = limit as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        registry_records = field::Empty,
        authorized_graphs = field::Empty,
        registry_ms = field::Empty,
        authorization_ms = field::Empty,
        craqle_search_ms = field::Empty,
        elapsed_ms = field::Empty,
        result = field::Empty,
        hit_count = field::Empty,
    )
)]
async fn search_local_graphs(
    inner: Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    query: String,
    limit: usize,
) -> Result<Vec<MetadataSearchHit>, MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();

    let registry_started = Instant::now();
    let records = list_local_registry_records(inner.clone()).await?;
    record_elapsed(&span, "registry_ms", registry_started);
    span.record("registry_records", records.len() as u64);

    let authorization_started = Instant::now();
    let allowed_records =
        select_authorized_records(inner.clone(), auth_context, records, graph_iris).await?;
    record_elapsed(&span, "authorization_ms", authorization_started);
    span.record("authorized_graphs", allowed_records.len() as u64);

    if limit == 0 || allowed_records.is_empty() {
        span.record("result", "ok");
        span.record("hit_count", 0u64);
        record_elapsed(&span, "elapsed_ms", total_started);
        return Ok(Vec::new());
    }

    let by_graph: HashMap<_, _> = allowed_records
        .into_iter()
        .map(|record| (record.graph_iri.clone(), record))
        .collect();
    let allowed_graphs = by_graph.keys().cloned().collect::<HashSet<_>>();

    let search_span = debug_span!(
        "metadata.backend.craqle.search",
        lazy = false,
        graph_count = allowed_graphs.len() as u64,
        query_len = query.len() as u64,
        limit = limit as u64,
        elapsed_ms = field::Empty,
        result = field::Empty,
        hit_count = field::Empty,
    );
    let blocking_span = search_span.clone();
    let search_started = Instant::now();
    let _permit = inner.craqle_read_permits.clone().acquire_owned().await.ok();
    let result = match tokio::task::spawn_blocking(move || {
        blocking_span.in_scope(|| {
            let authorizer = AllowedGraphAuthorizer {
                graph_iris: allowed_graphs,
            };
            // Craqle currently lacks a public multi-graph filtered search API;
            // this is the earliest available authorization hook before its
            // public search limit, with the Aruna mapping below kept defensive.
            let hits = inner
                .node
                .search(&authorizer, &query, limit)
                .map_err(|error| MetadataError::Backend(error.to_string()))?;
            let mut visible = hits
                .into_iter()
                .filter_map(|hit| by_graph.get(&hit.graph_id).map(|record| (hit, record)))
                .map(|(hit, record)| metadata_search_hit_from_craqle(hit, record))
                .collect::<Vec<_>>();
            visible.truncate(limit);
            Ok(visible)
        })
    })
    .await
    {
        Ok(result) => result,
        Err(error) => Err(MetadataError::TaskJoin(error.to_string())),
    };
    let search_elapsed = search_started.elapsed();
    record_duration(&search_span, "elapsed_ms", search_elapsed);
    record_duration(&span, "craqle_search_ms", search_elapsed);
    match &result {
        Ok(hits) => {
            search_span.record("result", "ok");
            span.record("result", "ok");
            search_span.record("hit_count", hits.len() as u64);
            span.record("hit_count", hits.len() as u64);
        }
        Err(error) => {
            record_error(&search_span, &error.to_string());
            record_error(&span, &error.to_string());
        }
    }
    warn_if_slow_metadata_backend("search", None, search_elapsed);
    record_elapsed(&span, "elapsed_ms", total_started);
    result
}

struct AllowedGraphAuthorizer {
    graph_iris: HashSet<String>,
}

impl CraqleAuthorizer for AllowedGraphAuthorizer {
    fn authorize(
        &self,
        graph: &GraphId,
        _policy: &GraphPolicy,
        action: CraqleAction,
    ) -> Result<(), CraqleAuthError> {
        if matches!(action, CraqleAction::Read) && self.graph_iris.contains(graph.as_str()) {
            return Ok(());
        }

        Err(CraqleAuthError::PermissionDenied {
            action,
            graph: graph.as_str().to_string(),
        })
    }
}

async fn list_visible_graphs(inner: Arc<MetadataInner>) -> Result<Vec<String>, MetadataError> {
    let graphs = tokio::task::spawn_blocking({
        let inner = inner.clone();
        move || inner.node.graphs()
    })
    .await
    .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
    .map_err(|error| MetadataError::Backend(error.to_string()))?;

    let mut visible = Vec::with_capacity(graphs.len());
    for graph in graphs {
        let graph_iri = graph.as_str().to_string();
        if !metadata_graph_deleted(inner.clone(), &graph_iri)
            .await?
            .deleted
        {
            visible.push(graph_iri);
        }
    }
    Ok(visible)
}

async fn select_authorized_graphs(
    inner: Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    records: Arc<Vec<MetadataRegistryRecord>>,
    graph_filter: Option<Vec<String>>,
) -> Result<Vec<String>, MetadataError> {
    Ok(
        select_authorized_records(inner, auth_context, records, graph_filter)
            .await?
            .into_iter()
            .map(|record| record.graph_iri)
            .collect(),
    )
}

#[tracing::instrument(
    name = "metadata.authorization.select_records",
    level = "debug",
    skip(inner, auth_context, records, graph_filter),
    fields(
        record_count = records.len() as u64,
        graph_filter_count = graph_filter.as_ref().map_or(0, Vec::len) as u64,
        visible_count = field::Empty,
        deleted_count = field::Empty,
        filtered_count = field::Empty,
        lifecycle_cache_hits = field::Empty,
        lifecycle_cache_misses = field::Empty,
        lifecycle_reads = field::Empty,
        public_count = field::Empty,
        private_checked_count = field::Empty,
        denied_count = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
async fn select_authorized_records(
    inner: Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    records: Arc<Vec<MetadataRegistryRecord>>,
    graph_filter: Option<Vec<String>>,
) -> Result<Vec<MetadataRegistryRecord>, MetadataError> {
    let span = Span::current();
    let started = Instant::now();
    let allowed_graphs = graph_filter.map(|graphs| graphs.into_iter().collect::<HashSet<_>>());
    let mut visible = Vec::new();
    let mut deleted_count = 0usize;
    let mut filtered_count = 0usize;
    let mut lifecycle_cache_hits = 0usize;
    let mut lifecycle_cache_misses = 0usize;
    let mut public_count = 0usize;
    let mut private_checked_count = 0usize;
    let mut denied_count = 0usize;
    for record in records.iter() {
        if let Some(filter) = allowed_graphs.as_ref()
            && !filter.contains(&record.graph_iri)
        {
            filtered_count += 1;
            continue;
        }
        let deleted = metadata_graph_deleted(inner.clone(), &record.graph_iri).await?;
        if deleted.cache_hit {
            lifecycle_cache_hits += 1;
        } else {
            lifecycle_cache_misses += 1;
        }
        if deleted.deleted {
            deleted_count += 1;
            continue;
        }
        if record.public {
            public_count += 1;
        } else {
            private_checked_count += 1;
        }
        if can_read_record_locally(inner.storage_handle.clone(), auth_context.clone(), record)
            .await?
        {
            visible.push(record.clone());
        } else {
            denied_count += 1;
        }
    }
    span.record("visible_count", visible.len() as u64);
    span.record("deleted_count", deleted_count as u64);
    span.record("filtered_count", filtered_count as u64);
    span.record("lifecycle_cache_hits", lifecycle_cache_hits as u64);
    span.record("lifecycle_cache_misses", lifecycle_cache_misses as u64);
    span.record("lifecycle_reads", lifecycle_cache_misses as u64);
    span.record("public_count", public_count as u64);
    span.record("private_checked_count", private_checked_count as u64);
    span.record("denied_count", denied_count as u64);
    record_elapsed(&span, "elapsed_ms", started);
    Ok(visible)
}

async fn can_read_record_locally(
    storage_handle: StorageHandle,
    auth_context: Option<AuthContext>,
    record: &MetadataRegistryRecord,
) -> Result<bool, MetadataError> {
    if record.public {
        return Ok(true);
    }
    let Some(auth_context) = auth_context else {
        return Ok(false);
    };
    if auth_context.realm_id != record.realm_id {
        return Ok(false);
    }

    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
    };
    drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context,
            path: record.permission_path.clone(),
            required_permission: Permission::READ,
        }),
        &context,
    )
    .await
    .map_err(|error| MetadataError::Backend(error.to_string()))
}

// All-metadata reads defer per-graph authorization to evaluation time: the
// scope is resolved once per query (O(caller's groups)) and the per-graph
// decision is a cheap synchronous lookup that craqle memoizes per query.
enum LocalReadScope<T> {
    Eager(T),
    Lazy(GraphVisibilityScope),
}

struct GraphVisibilityScope {
    records: Arc<Vec<MetadataRegistryRecord>>,
    auth_realm: Option<RealmId>,
    readable_groups: HashSet<GroupId>,
}

impl GraphVisibilityScope {
    fn record_for_graph(&self, graph_iri: &str) -> Option<&MetadataRegistryRecord> {
        registry_record_for_graph(&self.records, graph_iri)
    }

    fn record_visible(
        &self,
        visibility_cache: &MetadataVisibilityCache,
        record: &MetadataRegistryRecord,
    ) -> bool {
        if matches!(
            visibility_cache.lifecycle_deleted_any(&record.graph_iri),
            Some((true, _))
        ) {
            return false;
        }
        record.public
            || (self.auth_realm == Some(record.realm_id)
                && self.readable_groups.contains(&record.group_id))
    }

    // Graphs without a registry record stay invisible (fail closed).
    fn graph_visible(&self, visibility_cache: &MetadataVisibilityCache, graph_iri: &str) -> bool {
        self.record_for_graph(graph_iri)
            .is_some_and(|record| self.record_visible(visibility_cache, record))
    }
}

// Canonical graph IRIs embed the document id (graph_iri_for), enabling an
// O(log n) lookup in the document-id-ordered snapshot; non-canonical IRIs
// fall back to a scan.
fn registry_record_for_graph<'a>(
    records: &'a [MetadataRegistryRecord],
    graph_iri: &str,
) -> Option<&'a MetadataRegistryRecord> {
    if let Some(document_id) = graph_iri
        .rsplit('/')
        .next()
        .and_then(|tail| Ulid::from_string(tail).ok())
        && let Ok(index) = records.binary_search_by(|record| record.document_id.cmp(&document_id))
        && records[index].graph_iri == graph_iri
    {
        return Some(&records[index]);
    }
    records.iter().find(|record| record.graph_iri == graph_iri)
}

async fn resolve_graph_visibility_scope(
    inner: &Arc<MetadataInner>,
    auth_context: Option<AuthContext>,
    records: Arc<Vec<MetadataRegistryRecord>>,
) -> Result<GraphVisibilityScope, MetadataError> {
    refresh_lifecycle_visibility_for_records(inner, &records).await?;
    let auth_realm = auth_context.as_ref().map(|auth| auth.realm_id);
    let mut readable_groups = HashSet::new();
    if let Some(auth_context) = auth_context {
        let context = DriverContext {
            storage_handle: inner.storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };
        let groups = drive(ListGroupOperation::new(), &context)
            .await
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        for group in groups {
            if group.realm_id != auth_context.realm_id {
                continue;
            }
            let readable = drive(
                CheckPermissionsOperation::new(CheckPermissionsConfig {
                    auth_context: auth_context.clone(),
                    path: format!("/{}/g/{}/meta/**", group.realm_id, group.group_id),
                    required_permission: Permission::READ,
                }),
                &context,
            )
            .await
            .unwrap_or(false);
            if readable {
                readable_groups.insert(group.group_id);
            }
        }
    }
    Ok(GraphVisibilityScope {
        records,
        auth_realm,
        readable_groups,
    })
}

async fn refresh_lifecycle_visibility_for_records(
    inner: &Arc<MetadataInner>,
    records: &[MetadataRegistryRecord],
) -> Result<(), MetadataError> {
    let fill_generation = inner.visibility_cache.current_generation();
    let (deleted_graphs, _) = list_deleted_graph_iris(inner).await?;
    inner.visibility_cache.refresh_lifecycle_deleted_if_current(
        records.iter().map(|record| {
            (
                record.graph_iri.clone(),
                deleted_graphs.contains(&record.graph_iri),
            )
        }),
        fill_generation,
    );
    Ok(())
}

#[tracing::instrument(
    name = "metadata.remote.request",
    level = "debug",
    skip(net_handle, message),
    fields(
        peer = ?node_id,
        request = metadata_transport_message_kind(&message),
        response = field::Empty,
        open_stream_ms = field::Empty,
        write_ms = field::Empty,
        finish_ms = field::Empty,
        read_ms = field::Empty,
        close_ms = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
async fn send_request(
    net_handle: &NetHandle,
    node_id: NodeId,
    message: MetadataTransportMessage,
) -> Result<MetadataTransportMessage, MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();

    let open_started = Instant::now();
    let mut stream = net_handle
        .open_stream(node_id, Alpn::Metadata)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    record_elapsed(&span, "open_stream_ms", open_started);

    let write_started = Instant::now();
    write_transport_message(&mut stream, &message).await?;
    record_elapsed(&span, "write_ms", write_started);

    let finish_started = Instant::now();
    stream
        .0
        .finish()
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    record_elapsed(&span, "finish_ms", finish_started);

    let read_started = Instant::now();
    let response = read_transport_message(&mut stream).await?;
    record_elapsed(&span, "read_ms", read_started);

    let close_started = Instant::now();
    close_stream(&mut stream).await;
    record_elapsed(&span, "close_ms", close_started);
    record_elapsed(&span, "elapsed_ms", total_started);
    span.record("response", metadata_transport_message_kind(&response));
    Ok(response)
}

async fn write_transport_message(
    stream: &mut BiStream,
    message: &MetadataTransportMessage,
) -> Result<(), MetadataError> {
    let result: Result<Result<(), String>, tokio::time::error::Elapsed> =
        timeout(METADATA_IO_TIMEOUT, write_message(stream, message)).await;
    result
        .map_err(|_| MetadataError::Backend("timed out writing metadata message".to_string()))?
        .map_err(MetadataError::Backend)
}

async fn read_transport_message(
    stream: &mut BiStream,
) -> Result<MetadataTransportMessage, MetadataError> {
    let result: Result<Result<MetadataTransportMessage, String>, tokio::time::error::Elapsed> =
        timeout(METADATA_IO_TIMEOUT, read_message(stream)).await;
    result
        .map_err(|_| MetadataError::Backend("timed out waiting for metadata message".to_string()))?
        .map_err(MetadataError::Backend)
}

async fn close_stream(stream: &mut BiStream) {
    let _ = stream.0.finish();
    let _ = stream.1.stop(0u32.into());
}

async fn drain_request_stream(stream: &mut BiStream) -> Result<(), MetadataError> {
    timeout(METADATA_IO_TIMEOUT, stream.1.read_to_end(1))
        .await
        .map_err(|_| {
            MetadataError::Backend("timed out draining metadata request stream".to_string())
        })?
        .map(|_| ())
        .map_err(|error| MetadataError::Backend(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::auth::{TOKEN_REVOCATION_LIST_KEY, TRUSTED_REALMS_LIST_KEY, bearer_token_hash};
    use aruna_core::keyspaces::{API_STATE_KEYSPACE, REALM_CONFIG_KEYSPACE};
    use aruna_core::structs::{
        PathRestriction, RealmConfigDocument, RealmId, RealmNodeKind, TokenClaims,
    };
    use aruna_storage::{FjallStorage, StorageHandle};
    use byteview::ByteView;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::EncodePrivateKey;
    use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde::Serialize;
    use tempfile::{TempDir, tempdir};

    #[test]
    fn metadata_handle_options_default_to_buffered_irokle_persist() {
        let options = MetadataHandleOptions::default();

        assert_eq!(options.irokle_persist_policy, FjallPersistPolicy::Buffer);
    }

    #[test]
    fn metadata_handle_options_can_set_irokle_persist_policy() {
        let options = MetadataHandleOptions::default()
            .with_search_storage(MetadataSearchStorage::Memory)
            .with_irokle_persist_policy(FjallPersistPolicy::SyncAll);

        assert_eq!(options.search_storage, MetadataSearchStorage::Memory);
        assert_eq!(options.irokle_persist_policy, FjallPersistPolicy::SyncAll);
    }

    #[tokio::test]
    async fn flush_persistence_succeeds_without_irokle_database() {
        let (_storage_dir, storage) = auth_storage();
        let metadata_dir = tempdir().expect("metadata dir");
        let metadata_handle = MetadataHandle::new_with_options(
            metadata_dir.path(),
            node_id_from_seed(1),
            storage,
            None,
            None,
            None,
            MetadataHandleOptions::default().with_search_storage(MetadataSearchStorage::Memory),
        )
        .expect("metadata handle opens");

        metadata_handle
            .flush_persistence()
            .await
            .expect("metadata persistence flushes");
    }

    #[tokio::test]
    async fn flush_persistence_succeeds_with_configured_irokle_database() {
        let (_storage_dir, storage) = auth_storage();
        let metadata_dir = tempdir().expect("metadata dir");
        let irokle_dir = tempdir().expect("irokle dir");
        let irokle_db =
            fjall::OptimisticTxDatabase::builder(irokle_dir.path().to_str().expect("irokle path"))
                .manual_journal_persist(true)
                .open()
                .expect("irokle db opens");
        let metadata_handle = MetadataHandle::new_with_options(
            metadata_dir.path(),
            node_id_from_seed(2),
            storage,
            None,
            None,
            Some(irokle_db),
            MetadataHandleOptions::default()
                .with_search_storage(MetadataSearchStorage::Memory)
                .with_irokle_persist_policy(FjallPersistPolicy::SyncAll),
        )
        .expect("metadata handle opens");

        metadata_handle
            .flush_persistence()
            .await
            .expect("metadata and irokle persistence flush");
    }

    #[tokio::test]
    async fn remote_metadata_peer_gate_accepts_valid_peer_in_auth_realm() {
        let (realm_signing_key, realm_id, user_id) = realm_fixture();
        let token = sign_token(&realm_signing_key, &token_claims(realm_id, user_id));
        let (_dir, storage) = auth_storage();
        let configured_peer = node_id_from_seed(12);
        persist_auth_state(
            &storage,
            TRUSTED_REALMS_LIST_KEY,
            &HashSet::from([realm_id]),
        )
        .await;
        persist_realm_config(&storage, realm_id, &[configured_peer]).await;
        let state = MetadataAuthValidationState::new(storage.clone());

        let auth = authorize_remote_metadata_peer(
            &state,
            &storage,
            configured_peer,
            Some(MetadataAuthToken::bearer(token).unwrap()),
        )
        .await
        .expect("configured peer accepted");

        assert_eq!(auth.user_id, user_id);
        assert_eq!(auth.realm_id, realm_id);
    }

    #[tokio::test]
    async fn remote_metadata_peer_gate_rejects_peer_from_wrong_realm() {
        let (realm_signing_key, realm_id, user_id) = realm_fixture();
        let token = sign_token(&realm_signing_key, &token_claims(realm_id, user_id));
        let (_dir, storage) = auth_storage();
        let wrong_realm_id = RealmId([21u8; 32]);
        let wrong_realm_peer = node_id_from_seed(22);
        let auth_realm_peer = node_id_from_seed(23);
        persist_auth_state(
            &storage,
            TRUSTED_REALMS_LIST_KEY,
            &HashSet::from([realm_id]),
        )
        .await;
        persist_realm_config(&storage, wrong_realm_id, &[wrong_realm_peer]).await;
        persist_realm_config(&storage, realm_id, &[auth_realm_peer]).await;
        let state = MetadataAuthValidationState::new(storage.clone());

        let error = authorize_remote_metadata_peer(
            &state,
            &storage,
            wrong_realm_peer,
            Some(MetadataAuthToken::bearer(token).unwrap()),
        )
        .await
        .expect_err("wrong realm peer rejected");

        assert_eq!(
            error,
            MetadataError::InvalidInput(format!(
                "remote metadata peer `{wrong_realm_peer}` is not configured in realm `{realm_id}`"
            ))
        );
    }

    #[tokio::test]
    async fn remote_metadata_auth_validates_token_into_auth_context() {
        let (realm_signing_key, realm_id, user_id) = realm_fixture();
        let token = sign_token(&realm_signing_key, &token_claims(realm_id, user_id));
        let (_dir, storage) = auth_storage();
        persist_auth_state(
            &storage,
            TRUSTED_REALMS_LIST_KEY,
            &HashSet::from([realm_id]),
        )
        .await;
        let state = MetadataAuthValidationState::new(storage);

        let auth =
            remote_metadata_auth_context(&state, Some(MetadataAuthToken::bearer(token).unwrap()))
                .await
                .unwrap();

        assert_eq!(auth.user_id, user_id);
        assert_eq!(auth.realm_id, realm_id);
    }

    #[tokio::test]
    async fn remote_metadata_auth_preserves_path_restrictions() {
        let (realm_signing_key, realm_id, user_id) = realm_fixture();
        let restrictions = vec![PathRestriction {
            pattern: format!("/{realm_id}/g/{}/meta/**", Ulid::new()),
            permission: Permission::READ,
        }];
        let mut claims = token_claims(realm_id, user_id);
        claims.restrictions = Some(restrictions.clone());
        let token = sign_token(&realm_signing_key, &claims);
        let (_dir, storage) = auth_storage();
        persist_auth_state(
            &storage,
            TRUSTED_REALMS_LIST_KEY,
            &HashSet::from([realm_id]),
        )
        .await;
        let state = MetadataAuthValidationState::new(storage);

        let auth =
            remote_metadata_auth_context(&state, Some(MetadataAuthToken::bearer(token).unwrap()))
                .await
                .unwrap();

        assert_eq!(auth.user_id, user_id);
        assert_eq!(auth.realm_id, realm_id);
        assert_eq!(auth.path_restrictions, Some(restrictions));
    }

    #[tokio::test]
    async fn remote_metadata_auth_rejects_revoked_untrusted_and_invalid_tokens() {
        let (realm_signing_key, realm_id, user_id) = realm_fixture();
        let token = sign_token(&realm_signing_key, &token_claims(realm_id, user_id));

        let (_revoked_dir, revoked_storage) = auth_storage();
        persist_auth_state(
            &revoked_storage,
            TRUSTED_REALMS_LIST_KEY,
            &HashSet::from([realm_id]),
        )
        .await;
        persist_auth_state(
            &revoked_storage,
            TOKEN_REVOCATION_LIST_KEY,
            &HashSet::from([bearer_token_hash(&token)]),
        )
        .await;
        let revoked_state = MetadataAuthValidationState::new(revoked_storage);
        assert_metadata_auth_rejected(&revoked_state, &token, "Token is revoked").await;

        let (_untrusted_dir, untrusted_storage) = auth_storage();
        let untrusted_state = MetadataAuthValidationState::new(untrusted_storage);
        assert_metadata_auth_rejected(&untrusted_state, &token, "Realm is not trusted").await;

        let (_invalid_dir, invalid_storage) = auth_storage();
        let invalid_state = MetadataAuthValidationState::new(invalid_storage);
        assert_metadata_auth_rejected(&invalid_state, "not-a-jwt", "invalid metadata auth token")
            .await;
    }

    #[tokio::test]
    async fn remote_metadata_auth_rejects_missing_token() {
        let (_dir, storage) = auth_storage();
        let state = MetadataAuthValidationState::new(storage);

        assert_eq!(
            remote_metadata_auth_context(&state, None)
                .await
                .expect_err("missing token rejected"),
            MetadataError::Backend("missing metadata auth token".to_string())
        );
    }

    async fn assert_metadata_auth_rejected(
        state: &MetadataAuthValidationState,
        token: &str,
        expected: &str,
    ) {
        let error =
            remote_metadata_auth_context(state, Some(MetadataAuthToken::bearer(token).unwrap()))
                .await
                .unwrap_err();

        match error {
            MetadataError::Backend(message) => assert!(
                message.contains(expected),
                "expected {message:?} to contain {expected:?}"
            ),
            other => panic!("unexpected metadata auth error: {other:?}"),
        }
    }

    fn auth_storage() -> (TempDir, StorageHandle) {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        (dir, storage)
    }

    async fn persist_auth_state<T: Serialize>(storage: &StorageHandle, key: &[u8], value: &T) {
        let bytes = postcard::to_allocvec(value).expect("auth state serializes");
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: API_STATE_KEYSPACE.to_string(),
                key: ByteView::from(key),
                value: ByteView::from(bytes),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected auth state write result: {other:?}"),
        }
    }

    async fn persist_realm_config(storage: &StorageHandle, realm_id: RealmId, node_ids: &[NodeId]) {
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        for node_id in node_ids {
            config.ensure_node(*node_id, RealmNodeKind::Server);
        }
        let bytes = postcard::to_allocvec(&config).expect("realm config serializes");

        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: (*realm_id.as_bytes()).into(),
                value: ByteView::from(bytes),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected realm config write result: {other:?}"),
        }
    }

    fn realm_fixture() -> (SigningKey, RealmId, UserId) {
        let signing_key = signing_key();
        let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
        let user_id = UserId::local(Ulid::new(), realm_id);
        (signing_key, realm_id, user_id)
    }

    fn signing_key() -> SigningKey {
        let mut rng = jsonwebtoken::signature::rand_core::OsRng;
        SigningKey::generate(&mut rng)
    }

    fn node_id_from_seed(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn token_claims(realm_id: RealmId, user_id: UserId) -> TokenClaims {
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        TokenClaims {
            sub: user_id.to_string(),
            iss: realm_id.to_string(),
            iat: now,
            exp: now + 600,
            jti: Ulid::new().to_string(),
            restrictions: None,
            issuer_pubkey: None,
            delegation_signature: None,
        }
    }

    fn sign_token(signing_key: &SigningKey, claims: &TokenClaims) -> String {
        let key_pem = signing_key.to_pkcs8_pem(LineEnding::LF).unwrap();
        encode(
            &Header::new(Algorithm::EdDSA),
            claims,
            &EncodingKey::from_ed_pem(key_pem.as_bytes()).unwrap(),
        )
        .unwrap()
    }

    fn registry_record(document_path: &str) -> MetadataRegistryRecord {
        let document_id = Ulid::new();
        MetadataRegistryRecord {
            realm_id: RealmId([7u8; 32]),
            group_id: Ulid::new(),
            document_id,
            document_path: document_path.to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: format!("/metadata/{document_path}"),
            holder_node_ids: Vec::new(),
            created_at_ms: 0,
            updated_at_ms: 0,
            last_event_id: Ulid::nil(),
        }
    }

    fn filled_cache(records: Vec<MetadataRegistryRecord>) -> MetadataVisibilityCache {
        let cache = MetadataVisibilityCache::new();
        cache.store_registry_records(Arc::new(records));
        cache
    }

    #[test]
    fn upsert_replaces_existing_record_and_appends_new_ones() {
        let mut existing = registry_record("datasets/a");
        let cache = filled_cache(vec![existing.clone()]);

        existing.public = false;
        existing.updated_at_ms = 42;
        let added = registry_record("datasets/b");
        cache.upsert_registry_records(&[existing.clone(), added.clone()]);

        let records = cache.registry_records().expect("cache entry");
        assert_eq!(records.len(), 2);
        let updated = records
            .iter()
            .find(|record| record.document_id == existing.document_id)
            .expect("updated record");
        assert!(!updated.public);
        assert_eq!(updated.updated_at_ms, 42);
        assert!(
            records
                .iter()
                .any(|record| record.document_id == added.document_id)
        );
    }

    #[test]
    fn upsert_without_filled_cache_is_noop_until_refill() {
        let cache = MetadataVisibilityCache::new();
        cache.upsert_registry_records(&[registry_record("datasets/a")]);
        assert!(cache.registry_records().is_none());
    }

    #[test]
    fn remove_by_document_and_graph_drop_records() {
        let by_document = registry_record("datasets/a");
        let by_graph = registry_record("datasets/b");
        let kept = registry_record("datasets/c");
        let cache = filled_cache(vec![by_document.clone(), by_graph.clone(), kept.clone()]);

        cache.remove_registry_record(by_document.document_id);
        cache.remove_registry_records_by_graph(&by_graph.graph_iri);

        let records = cache.registry_records().expect("cache entry");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].document_id, kept.document_id);
    }

    #[test]
    fn group_snapshots_are_scoped_and_invalidate_per_group() {
        let group_a = Ulid::new();
        let group_b = Ulid::new();
        let mut record_a = registry_record("datasets/a");
        record_a.group_id = group_a;
        let mut record_b = registry_record("datasets/b");
        record_b.group_id = group_b;
        let cache = filled_cache(vec![record_a.clone(), record_b.clone()]);

        let (listed_a, fresh) = cache
            .registry_records_for_group_any(group_a)
            .expect("group A snapshot exists");
        assert!(fresh);
        assert_eq!(listed_a.as_ref(), &vec![record_a.clone()]);

        let mut added_b = registry_record("datasets/b2");
        added_b.group_id = group_b;
        cache.upsert_registry_records(std::slice::from_ref(&added_b));
        let (listed_a_again, _) = cache
            .registry_records_for_group_any(group_a)
            .expect("group A snapshot still exists");
        assert!(Arc::ptr_eq(&listed_a, &listed_a_again));

        let mut added_a = registry_record("datasets/a2");
        added_a.group_id = group_a;
        cache.upsert_registry_records(std::slice::from_ref(&added_a));
        let (listed_a_after, _) = cache
            .registry_records_for_group_any(group_a)
            .expect("group A snapshot refreshes");
        assert_eq!(listed_a_after.len(), 2);
        assert!(!Arc::ptr_eq(&listed_a, &listed_a_after));
    }

    #[test]
    fn upsert_does_not_extend_expiry_or_resurrect_expired_entries() {
        let cache = filled_cache(vec![registry_record("datasets/a")]);
        {
            let mut registry = cache.registry.lock().unwrap();
            registry.as_mut().expect("cache entry").expires_at =
                Instant::now() - Duration::from_secs(1);
        }

        cache.upsert_registry_records(&[registry_record("datasets/b")]);

        assert!(cache.registry_records().is_none());
    }

    #[test]
    fn lifecycle_entry_removal_forces_storage_reread() {
        let cache = MetadataVisibilityCache::new();
        cache.store_lifecycle_deleted("urn:graph:a".to_string(), false);
        assert_eq!(cache.lifecycle_deleted("urn:graph:a"), Some(false));

        cache.remove_lifecycle_entry("urn:graph:a");
        assert_eq!(cache.lifecycle_deleted("urn:graph:a"), None);
    }

    #[test]
    fn expired_registry_entry_is_served_stale_not_dropped() {
        let record = registry_record("datasets/a");
        let cache = filled_cache(vec![record.clone()]);
        cache.expire_now();

        assert!(cache.registry_records().is_none());
        let (records, fresh) = cache.registry_records_any().expect("stale entry kept");
        assert!(!fresh);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].document_id, record.document_id);

        cache.store_registry_records(Arc::new(vec![record.clone()]));
        let (_, fresh) = cache.registry_records_any().expect("fresh entry");
        assert!(fresh);
        assert!(cache.registry_records().is_some());
    }

    #[test]
    fn background_visibility_fill_does_not_overwrite_newer_upsert() {
        let mut stale_record = registry_record("datasets/a");
        let cache = filled_cache(vec![stale_record.clone()]);
        let fill_generation = cache.current_generation();

        let mut updated_record = stale_record.clone();
        updated_record.public = false;
        updated_record.updated_at_ms = 42;
        cache.upsert_registry_records(std::slice::from_ref(&updated_record));

        stale_record.updated_at_ms = 1;
        assert!(!cache.store_visibility_fill(
            Arc::new(vec![stale_record]),
            Vec::new(),
            fill_generation,
        ));
        let records = cache.registry_records().expect("cache entry");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].document_id, updated_record.document_id);
        assert!(!records[0].public);
        assert_eq!(records[0].updated_at_ms, 42);
    }

    #[test]
    fn background_visibility_fill_does_not_resurrect_removed_document() {
        let removed = registry_record("datasets/removed");
        let kept = registry_record("datasets/kept");
        let cache = filled_cache(vec![removed.clone(), kept.clone()]);
        let fill_generation = cache.current_generation();

        cache.remove_registry_record(removed.document_id);

        assert!(!cache.store_visibility_fill(
            Arc::new(vec![removed.clone(), kept.clone()]),
            Vec::new(),
            fill_generation,
        ));
        let records = cache.registry_records().expect("cache entry");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].document_id, kept.document_id);
        assert!(
            !records
                .iter()
                .any(|record| record.document_id == removed.document_id)
        );
    }

    #[test]
    fn background_visibility_fill_does_not_clear_newer_lifecycle_tombstone() {
        let record = registry_record("datasets/deleted");
        let cache = filled_cache(vec![record.clone()]);
        let fill_generation = cache.current_generation();

        cache.store_lifecycle_deleted(record.graph_iri.clone(), true);

        assert!(!cache.store_visibility_fill(
            Arc::new(vec![record.clone()]),
            vec![(record.graph_iri.clone(), false)],
            fill_generation,
        ));
        assert_eq!(cache.lifecycle_deleted(&record.graph_iri), Some(true));
    }

    #[test]
    fn expired_lifecycle_entry_is_served_stale_not_dropped() {
        let cache = MetadataVisibilityCache::new();
        cache.store_lifecycle_deleted("urn:graph:a".to_string(), true);
        cache.expire_now();

        assert_eq!(cache.lifecycle_deleted("urn:graph:a"), None);
        assert_eq!(
            cache.lifecycle_deleted_any("urn:graph:a"),
            Some((true, false))
        );
    }

    #[test]
    fn registry_record_lookup_parses_iri_and_falls_back_to_scan() {
        let mut records: Vec<_> = (0..4)
            .map(|index| registry_record(&format!("datasets/{index}")))
            .collect();
        let mut custom = registry_record("datasets/custom");
        custom.graph_iri = "https://example.org/custom-graph".to_string();
        records.push(custom.clone());
        records.sort_unstable_by_key(|record| record.document_id);

        for record in &records {
            let found =
                registry_record_for_graph(&records, &record.graph_iri).expect("record found");
            assert_eq!(found.document_id, record.document_id);
        }
        assert!(
            registry_record_for_graph(
                &records,
                &MetadataRegistryRecord::graph_iri_for(Ulid::new())
            )
            .is_none()
        );
        assert!(registry_record_for_graph(&records, "https://example.org/missing").is_none());
    }

    #[test]
    fn visibility_scope_enforces_public_group_and_lifecycle_rules() {
        let realm = RealmId([7u8; 32]);
        let mut public_record = registry_record("datasets/public");
        public_record.public = true;
        let mut private_record = registry_record("datasets/private");
        private_record.public = false;
        let mut deleted_record = registry_record("datasets/deleted");
        deleted_record.public = true;
        let mut records = vec![
            public_record.clone(),
            private_record.clone(),
            deleted_record.clone(),
        ];
        records.sort_unstable_by_key(|record| record.document_id);

        let cache = MetadataVisibilityCache::new();
        cache.store_lifecycle_deleted(deleted_record.graph_iri.clone(), true);

        let anonymous = GraphVisibilityScope {
            records: Arc::new(records.clone()),
            auth_realm: None,
            readable_groups: HashSet::new(),
        };
        assert!(anonymous.graph_visible(&cache, &public_record.graph_iri));
        assert!(!anonymous.graph_visible(&cache, &private_record.graph_iri));
        assert!(!anonymous.graph_visible(&cache, &deleted_record.graph_iri));
        assert!(
            !anonymous.graph_visible(&cache, &MetadataRegistryRecord::graph_iri_for(Ulid::new()))
        );

        let member = GraphVisibilityScope {
            records: Arc::new(records.clone()),
            auth_realm: Some(realm),
            readable_groups: HashSet::from([private_record.group_id]),
        };
        assert!(member.graph_visible(&cache, &public_record.graph_iri));
        assert!(member.graph_visible(&cache, &private_record.graph_iri));
        assert!(!member.graph_visible(&cache, &deleted_record.graph_iri));

        let wrong_realm = GraphVisibilityScope {
            records: Arc::new(records),
            auth_realm: Some(RealmId([8u8; 32])),
            readable_groups: HashSet::from([private_record.group_id]),
        };
        assert!(wrong_realm.graph_visible(&cache, &public_record.graph_iri));
        assert!(!wrong_realm.graph_visible(&cache, &private_record.graph_iri));
    }

    #[test]
    fn lifecycle_refresh_restamps_entries_and_prunes_expired_leftovers() {
        let cache = MetadataVisibilityCache::new();
        cache.store_lifecycle_deleted("urn:graph:kept".to_string(), true);
        cache.store_lifecycle_deleted("urn:graph:gone".to_string(), false);
        cache.expire_now();
        cache.store_lifecycle_deleted("urn:graph:fresh".to_string(), false);

        cache.refresh_lifecycle_deleted(vec![("urn:graph:kept".to_string(), false)]);

        assert_eq!(cache.lifecycle_deleted("urn:graph:kept"), Some(false));
        assert_eq!(cache.lifecycle_deleted_any("urn:graph:gone"), None);
        assert_eq!(cache.lifecycle_deleted("urn:graph:fresh"), Some(false));
    }
}
