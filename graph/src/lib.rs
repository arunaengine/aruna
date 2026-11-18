//! Owns the craqle metadata graph store: effects, queries, searches, persistence and Profile checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::metadata::{MetadataError, MetadataGraphPolicy};
use aruna_core::profile_shapes::ProfileShaclError;
use craqle::{
    ActorId, AllowAllAuthorizer, CraqleError, CraqleIrokleOptions, CraqleNode, CraqleOptions,
    GraphId, SearchStorage,
};
use tokio::sync::{OwnedMutexGuard, Semaphore};

use crate::convert::{error_from_craqle, fjall_persist_mode, irokle_peer_id, policy_from_craqle};
use crate::persist::flush_metadata_persistence;

mod convert;
mod persist;
mod profiles;
mod read;
mod run;
mod shacl;
mod summaries;

pub use crate::read::{Properties, ReadScope};
pub use crate::shacl::ProfileShaclEngine;

/// Store calls at or above this duration are logged as slow.
const SLOW_CALL: Duration = Duration::from_millis(100);
// Unbiased per-call-kind craqle latency histograms; every backend call is
// recorded, not just the ones above the slow-call threshold.
static CRAQLE_LATENCY: LazyLock<aruna_core::telemetry::LatencyAggregator> =
    LazyLock::new(|| aruna_core::telemetry::LatencyAggregator::new("craqle"));

/// Where craqle keeps its full-text search index.
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

/// How the graph store is opened.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphOptions {
    pub search_storage: MetadataSearchStorage,
    /// Flush every durable write with `SyncAll` instead of buffering it.
    pub sync_all: bool,
    /// Size of the craqle mutation and read permit pools. Defaults to the
    /// host parallelism; set explicitly when cgroup limits make
    /// `available_parallelism` unrepresentative.
    pub backend_pool_size: Option<usize>,
    /// Test/maintenance fault injection. Tagged writes fail closed while an
    /// untagged crate continues through ordinary structural validation.
    pub profile_validation_disabled: bool,
}

impl GraphOptions {
    pub fn with_search_storage(mut self, search_storage: MetadataSearchStorage) -> Self {
        self.search_storage = search_storage;
        self
    }

    pub fn with_sync_all(mut self, sync_all: bool) -> Self {
        self.sync_all = sync_all;
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

/// The metadata graph store of this node. Clones share one store.
#[derive(Clone)]
pub struct GraphHandle {
    inner: Arc<GraphInner>,
}

struct GraphInner {
    node: CraqleNode,
    document_sync_db: Option<fjall::OptimisticTxDatabase>,
    sync_all: bool,
    write_permits: Arc<Semaphore>,
    read_permits: Arc<Semaphore>,
    deferred_persist_requested: AtomicBool,
    deferred_persist_running: AtomicBool,
    profile_shacl: Option<Arc<ProfileShaclEngine>>,
    /// RO-Crate summaries by graph and document cursor.
    summaries: crate::summaries::MetadataSummaryCache,
    /// Shape sources of the Profile revisions this node fetched.
    profiles: crate::profiles::ProfileCache,
    /// Striped holds that serialize this node's writes to one document.
    document_holds: [Arc<tokio::sync::Mutex<()>>; DOCUMENT_HOLDS],
}

/// Stripes of the document write holds.
const DOCUMENT_HOLDS: usize = 64;

impl std::fmt::Debug for GraphHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphHandle").finish_non_exhaustive()
    }
}

impl GraphHandle {
    /// Opens the store under `path`, writing as `node_id` and syncing through
    /// `document_sync_node` when one is given.
    pub fn open(
        path: impl AsRef<Path>,
        node_id: NodeId,
        document_sync_node: Option<irokle::Irokle<irokle::FjallStorage>>,
        document_sync_db: Option<fjall::OptimisticTxDatabase>,
        options: GraphOptions,
    ) -> Result<Self, MetadataError> {
        let path = path.as_ref();
        let craqle_options = CraqleOptions::new()
            .with_actor(ActorId::from_bytes(*node_id.as_bytes()))
            .with_search_storage(options.search_storage.into())
            .with_graph_store_persist_mode(fjall_persist_mode(options.sync_all));
        let craqle_options = match document_sync_node {
            Some(document_sync_node) => {
                craqle_options.with_irokle(document_sync_node, CraqleIrokleOptions::new())
            }
            None => craqle_options,
        };
        let node = CraqleNode::open_with_options(path, craqle_options)
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        let profile_shacl = if options.profile_validation_disabled {
            None
        } else {
            Some(Arc::new(
                ProfileShaclEngine::open(&path.join("profile-validation"))
                    .map_err(|error| MetadataError::Backend(error.to_string()))?,
            ))
        };
        let pool_size = options.backend_pool_size.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|cores| cores.get())
                .unwrap_or(4)
                .max(4)
        });
        Ok(Self {
            inner: Arc::new(GraphInner {
                node,
                document_sync_db,
                sync_all: options.sync_all,
                write_permits: Arc::new(Semaphore::new(pool_size)),
                read_permits: Arc::new(Semaphore::new(pool_size)),
                deferred_persist_requested: AtomicBool::new(false),
                deferred_persist_running: AtomicBool::new(false),
                profile_shacl,
                profiles: crate::profiles::ProfileCache::new(),
                summaries: crate::summaries::MetadataSummaryCache::new(
                    crate::summaries::SUMMARY_CACHE_ENTRIES,
                    crate::summaries::SUMMARY_CACHE_BYTES,
                    crate::summaries::SUMMARY_CACHE_TTL,
                ),
                document_holds: std::array::from_fn(|_| Arc::default()),
            }),
        })
    }

    /// Profile revisions this node fetched for validation since start. A second validation of
    /// the same revision must not raise it.
    pub fn profile_loads(&self) -> u64 {
        self.inner.profiles.loads()
    }

    /// Waits for the write hold of one document; the caller keeps it until it drops the guard.
    pub async fn hold_document(&self, document_id: ulid::Ulid) -> OwnedMutexGuard<()> {
        let stripe = usize::from(document_id.to_bytes()[15]) % DOCUMENT_HOLDS;
        self.inner.document_holds[stripe].clone().lock_owned().await
    }

    /// The store's Fjall database, for consistent read snapshots such as live backups.
    pub fn store_database(&self) -> fjall::Database {
        self.inner.node.store_database()
    }

    /// Whether `graph_iri` holds a graph.
    pub async fn contains_graph(&self, graph_iri: String) -> Result<bool, MetadataError> {
        self.blocking(move |node| {
            node.contains_graph(&GraphId::new(&graph_iri))
                .map_err(Box::new)
        })
        .await
    }

    /// Removes the local copy of one graph.
    pub async fn delete_graph(&self, graph_iri: String) -> Result<(), MetadataError> {
        self.blocking(move |node| {
            node.delete_graph(&AllowAllAuthorizer, &GraphId::new(&graph_iri))
                .map_err(Box::new)
        })
        .await
    }

    /// The IRIs of every graph in the store.
    pub async fn graphs(&self) -> Result<Vec<String>, MetadataError> {
        let inner = self.inner.clone();
        let graphs = tokio::task::spawn_blocking(move || inner.node.graphs().map_err(Box::new))
            .await
            .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        Ok(graphs
            .iter()
            .map(|graph| graph.as_str().to_string())
            .collect())
    }

    /// The stored policy of every graph, by graph IRI.
    pub async fn graph_policies(
        &self,
    ) -> Result<BTreeMap<String, MetadataGraphPolicy>, MetadataError> {
        self.blocking(|node| {
            node.graphs()?
                .into_iter()
                .map(|graph| {
                    let policy = node.graph_policy(&graph)?;
                    Ok((graph.as_str().to_string(), policy_from_craqle(policy)))
                })
                .collect::<Result<BTreeMap<_, _>, Box<CraqleError>>>()
        })
        .await
    }

    /// Applies document sync updates that arrived from peers; returns the changed graphs.
    pub async fn reconcile(&self) -> Result<Vec<String>, MetadataError> {
        let inner = self.inner.clone();
        let applied =
            tokio::task::spawn_blocking(move || inner.node.reconcile_irokle().map_err(Box::new))
                .await
                .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
                .map_err(|error| MetadataError::Backend(error.to_string()))?;
        // Peer sync invalidates summaries because their cursor may precede newly applied content.
        let applied: Vec<String> = applied
            .iter()
            .map(|graph| graph.as_str().to_string())
            .collect();
        for graph in &applied {
            self.inner.summaries.remove(graph);
        }
        Ok(applied)
    }

    /// The document sync topic of one graph, bound or derived.
    pub async fn graph_topic(&self, graph_iri: String) -> Result<irokle::TopicId, MetadataError> {
        self.blocking(move |node| {
            node.bind_or_derive_irokle_topic(&GraphId::new(&graph_iri))
                .map_err(Box::new)
        })
        .await
    }

    /// Binds one graph to its document sync topic, when the sync store knows one.
    pub async fn bind_topic(
        &self,
        graph_iri: String,
    ) -> Result<Option<irokle::TopicId>, MetadataError> {
        self.blocking(move |node| {
            node.bind_irokle_topic(&GraphId::new(&graph_iri))
                .map_err(Box::new)
        })
        .await
    }

    /// Creates the document sync topic of one graph with `members`.
    pub async fn mint_topic(
        &self,
        graph_iri: String,
        members: BTreeSet<NodeId>,
    ) -> Result<irokle::TopicId, MetadataError> {
        let members = members.into_iter().map(irokle_peer_id).collect();
        self.blocking(move |node| {
            node.mint_irokle_topic(&GraphId::new(&graph_iri), members)
                .map_err(Box::new)
        })
        .await
    }

    /// Adds `peers` to the document sync topic of one graph.
    pub async fn add_topic_peers(
        &self,
        graph_iri: String,
        peers: Vec<NodeId>,
    ) -> Result<(), MetadataError> {
        self.blocking(move |node| {
            let graph = GraphId::new(&graph_iri);
            for peer in peers {
                node.add_irokle_peer(&graph, irokle_peer_id(peer))?;
            }
            Ok(())
        })
        .await
    }

    /// Makes every pending search index update visible.
    pub async fn flush_search_updates(&self) -> Result<(), MetadataError> {
        self.blocking(|node| node.flush_search_updates().map_err(Box::new))
            .await
    }

    /// Writes the store and the document sync journal to disk.
    pub async fn flush_persistence(&self) -> Result<(), MetadataError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || flush_metadata_persistence(&inner, "shutdown", None))
            .await
            .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
    }

    /// Builds the query indexes so the first query after boot finds them warm.
    pub async fn ensure_query_indexes(&self) -> Result<(), MetadataError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.node.ensure_query_indexes())
            .await
            .map_err(|error| MetadataError::TaskJoin(error.to_string()))
    }

    pub(crate) async fn run_profile_shacl<T, F>(&self, work: F) -> Result<T, ProfileShaclError>
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
        let _permit = self.inner.write_permits.clone().acquire_owned().await.ok();
        tokio::task::spawn_blocking(move || work(engine.as_ref()))
            .await
            .map_err(|error| ProfileShaclError::Unavailable {
                message: error.to_string(),
            })?
    }

    async fn blocking<T, F>(&self, work: F) -> Result<T, MetadataError>
    where
        T: Send + 'static,
        F: FnOnce(&CraqleNode) -> Result<T, Box<CraqleError>> + Send + 'static,
    {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || work(&inner.node))
            .await
            .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
            .map_err(|error| error_from_craqle(*error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use craqle::CraqleFjallPersistMode;

    fn node_id(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn memory_options() -> GraphOptions {
        GraphOptions::default().with_search_storage(MetadataSearchStorage::Memory)
    }

    #[test]
    fn maps_violations() {
        let error = error_from_craqle(CraqleError::Update(craqle::UpdateError::ValidationFailed(
            vec![craqle::CrateViolation {
                code: "missing_root_data_entity",
                message: "missing root".to_string(),
                pointer: "/@graph".to_string(),
                entity_id: Some("./".to_string()),
            }],
        )));

        let MetadataError::Validation(violations) = error else {
            panic!("expected structured metadata validation error");
        };
        assert_eq!(violations[0].code, "missing_root_data_entity");
        assert_eq!(violations[0].pointer, "/@graph");
        assert_eq!(violations[0].entity_id.as_deref(), Some("./"));
    }

    #[test]
    fn maps_io_failures() {
        // Infrastructure must stay distinguishable from a rejected payload: the
        // materialization queue retries the former forever and parks the latter.
        let error = error_from_craqle(CraqleError::Io(std::io::Error::other("disk")));

        assert!(matches!(error, MetadataError::Persist(_)));
    }

    #[test]
    fn maps_cancelled_work() {
        // Cancelled or budget-limited work clears on retry, so it must not be parked.
        let error = error_from_craqle(CraqleError::QueryCancelled);

        assert!(matches!(error, MetadataError::Persist(_)));
    }

    #[test]
    fn options_default_buffered() {
        let options = GraphOptions::default();

        assert!(!options.sync_all);
        assert_eq!(options.search_storage, MetadataSearchStorage::Disk);
    }

    // The graph was never written, so a returned summary can only come from the summary cache;
    // a cursor advance must fall through to the store.
    #[tokio::test]
    async fn summary_follows_cursor() {
        let dir = tempfile::tempdir().expect("graph dir");
        let graph = GraphHandle::open(dir.path(), node_id(1), None, None, memory_options())
            .expect("graph opens");
        let graph_iri = "https://w3id.org/aruna/summary".to_string();
        let cursor = ulid::Ulid::from_parts(1, 1);
        let at = |cursor| aruna_core::metadata::MetadataEffect::ExportSummaryAt {
            graph_iri: graph_iri.clone(),
            cursor,
        };
        (graph.inner.summaries).insert(
            &graph_iri,
            cursor,
            "{\"cached\":true}",
            std::time::Instant::now(),
        );
        assert_eq!(
            graph.run(at(cursor)).await,
            aruna_core::metadata::MetadataEvent::RoCrateSummaryResult {
                graph_iri: graph_iri.clone(),
                jsonld: "{\"cached\":true}".to_string(),
            }
        );
        let stale = graph.run(at(ulid::Ulid::from_parts(2, 2))).await;
        assert!(!matches!(
            stale,
            aruna_core::metadata::MetadataEvent::RoCrateSummaryResult { .. }
        ));
    }

    #[tokio::test]
    async fn flush_without_sync() {
        let dir = tempfile::tempdir().expect("graph dir");
        let graph = GraphHandle::open(dir.path(), node_id(1), None, None, memory_options())
            .expect("graph store opens");

        assert_eq!(
            graph.inner.node.graph_store_persist_mode(),
            CraqleFjallPersistMode::Buffer
        );
        graph
            .flush_persistence()
            .await
            .expect("graph persistence flushes");
    }

    #[tokio::test]
    async fn flush_with_sync() {
        let dir = tempfile::tempdir().expect("graph dir");
        let sync_dir = tempfile::tempdir().expect("document sync dir");
        let sync_db = fjall::OptimisticTxDatabase::builder(
            sync_dir.path().to_str().expect("document sync path"),
        )
        .manual_journal_persist(true)
        .open()
        .expect("document sync db opens");
        let graph = GraphHandle::open(
            dir.path(),
            node_id(2),
            None,
            Some(sync_db),
            memory_options().with_sync_all(true),
        )
        .expect("graph store opens");

        assert_eq!(
            graph.inner.node.graph_store_persist_mode(),
            CraqleFjallPersistMode::SyncAll
        );
        graph
            .flush_persistence()
            .await
            .expect("graph and document sync persistence flush");
    }
}
