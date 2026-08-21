use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::audit::AuditPageBatch;
use aruna_core::effects::{DhtEffect, DhtGetOptions, NetEffect, StreamEffect};
use aruna_core::errors::{DhtError, StreamError};
use aruna_core::events::{
    DhtEntry, DhtEvent, JobControlEvent, JobRecordEvent, LaunchOfferEvent, NetEvent,
    PolicyFetchEvent, PolicySignEvent, StreamEvent,
};
use aruna_core::id::{DhtKeyId, NodeId, hex_prefix};
use aruna_core::structs::RealmId;
use parking_lot::Mutex;
#[cfg(test)]
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, trace, warn};

use crate::{DhtHandle, DocumentSyncService};

/// A snapshot younger than this answers without any DHT work.
const PRESENCE_FRESH: Duration = Duration::from_secs(10);
/// Beyond this age the snapshot is not served at all and the caller waits for a
/// bounded cold lookup.
const PRESENCE_MAX_STALE: Duration = Duration::from_secs(60);
/// A stale reader gives up this long before its own deadline, so a refresh that
/// cannot finish still yields the snapshot instead of an expired read.
const PRESENCE_ANSWER_MARGIN: Duration = Duration::from_millis(250);

/// Everything a net effect needs besides the effect itself.
pub struct NetEffectContext {
    pub dht: Arc<DhtHandle>,
    pub document_sync: Arc<DocumentSyncService>,
    pub presence: RealmPresenceCache,
    pub tasks: TaskTracker,
    pub shutdown: CancellationToken,
    #[cfg(test)]
    pub(crate) refresh_probe: Option<Arc<RefreshProbe>>,
}

#[cfg(test)]
pub(crate) struct RefreshProbe {
    started: Notify,
    release: Notify,
    starts: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl RefreshProbe {
    fn new() -> Self {
        Self {
            started: Notify::new(),
            release: Notify::new(),
            starts: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

#[derive(Debug, Clone)]
struct PresenceSnapshot {
    values: Vec<DhtEntry>,
    observed: Instant,
}

/// Per-`NetHandle` realm-presence snapshots. Cached entries are discovery
/// candidates: callers still filter them through the current realm config, and
/// they never mark a peer connected or grant authority.
#[derive(Clone, Default)]
pub struct RealmPresenceCache {
    snapshots: Arc<Mutex<HashMap<RealmId, PresenceSnapshot>>>,
    /// Token per in-flight refresh, cancelled when it settles so every stale
    /// reader can join the one refresh instead of starting its own. Locked
    /// before `snapshots` whenever both are needed.
    refreshing: Arc<Mutex<HashMap<RealmId, CancellationToken>>>,
}

impl std::fmt::Debug for RealmPresenceCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealmPresenceCache")
            .field("realms", &self.snapshots.lock().len())
            .finish()
    }
}

#[derive(Debug)]
enum PresenceServe {
    Fresh(Vec<DhtEntry>),
    Stale {
        values: Vec<DhtEntry>,
        /// True for the reader that has to start the refresh.
        refresh: bool,
        /// Cancelled once that refresh settled.
        settled: CancellationToken,
    },
    Cold,
}

impl RealmPresenceCache {
    fn serve(&self, realm_id: RealmId, now: Instant) -> PresenceServe {
        let values = {
            let snapshots = self.snapshots.lock();
            let Some(snapshot) = snapshots.get(&realm_id) else {
                return PresenceServe::Cold;
            };
            let age = now.saturating_duration_since(snapshot.observed);
            if age <= PRESENCE_FRESH {
                return PresenceServe::Fresh(snapshot.values.clone());
            }
            if age > PRESENCE_MAX_STALE {
                return PresenceServe::Cold;
            }
            snapshot.values.clone()
        };

        self.claim_slot(realm_id, now, values)
    }

    /// Joins or opens the single-flight slot for a stale read. The in-flight
    /// refresh may have stored and released since that read, so freshness is
    /// re-checked under this lock instead of starting a redundant refresh.
    fn claim_slot(&self, realm_id: RealmId, now: Instant, values: Vec<DhtEntry>) -> PresenceServe {
        let mut refreshing = self.refreshing.lock();
        if let Some(fresh) = self.fresh(realm_id, now) {
            return PresenceServe::Fresh(fresh);
        }
        match refreshing.get(&realm_id) {
            Some(settled) => PresenceServe::Stale {
                values,
                refresh: false,
                settled: settled.clone(),
            },
            None => {
                let settled = CancellationToken::new();
                refreshing.insert(realm_id, settled.clone());
                PresenceServe::Stale {
                    values,
                    refresh: true,
                    settled,
                }
            }
        }
    }

    /// Side-effect free freshness check: a reader that waited for a refresh
    /// must not claim the next single-flight slot while re-reading.
    fn fresh(&self, realm_id: RealmId, now: Instant) -> Option<Vec<DhtEntry>> {
        let snapshots = self.snapshots.lock();
        let snapshot = snapshots.get(&realm_id)?;
        (now.saturating_duration_since(snapshot.observed) <= PRESENCE_FRESH)
            .then(|| snapshot.values.clone())
    }

    /// An empty answer holds no candidate to serve, so caching it would only
    /// hide a peer that announces during the freshness window.
    fn store(&self, realm_id: RealmId, values: Vec<DhtEntry>, now: Instant) {
        if values.is_empty() {
            self.snapshots.lock().remove(&realm_id);
            return;
        }
        self.snapshots.lock().insert(
            realm_id,
            PresenceSnapshot {
                values,
                observed: now,
            },
        );
    }

    fn release(&self, realm_id: RealmId) {
        if let Some(settled) = self.refreshing.lock().remove(&realm_id) {
            settled.cancel();
        }
    }
}

#[tracing::instrument(
    name = "net.effect",
    level = "debug",
    skip(ctx, effect),
    fields(effect = net_effect_kind(&effect))
)]
pub async fn handle_net_effect(ctx: &NetEffectContext, effect: NetEffect) -> NetEvent {
    let document_sync = ctx.document_sync.as_ref();
    match effect {
        NetEffect::Dht(dht_effect) => handle_dht_effect(ctx, dht_effect).await,
        NetEffect::DocumentSync(document_sync_effect) => match document_sync_effect {
            aruna_core::DocumentSyncEffect::PublishDocuments { documents, peers } => {
                NetEvent::DocumentSync(document_sync.publish_documents(documents, peers).await)
            }
            aruna_core::DocumentSyncEffect::SyncDocument { topic, peers } => {
                NetEvent::DocumentSync(document_sync.sync_document_event(topic, peers).await)
            }
            aruna_core::DocumentSyncEffect::SyncDocuments { topics, peers } => {
                NetEvent::DocumentSync(document_sync.sync_documents_event(topics, peers).await)
            }
        },
        NetEffect::Stream(stream_effect) => handle_stream_effect(stream_effect).await,
        // Job-control and audit effects are executed by the operations runner,
        // which holds the driver context; they never reach this handler.
        NetEffect::JobControl(_) => NetEvent::JobControl(JobControlEvent::Unavailable(
            "job-control effect must be dispatched by the operations runner".to_string(),
        )),
        NetEffect::AuditPage(audit) => {
            NetEvent::AuditPages(audit_fallback(audit.nodes, audit.request.limit))
        }
        // Policy, job-record, and launch protocols live in the operations
        // runner too; reaching this handler is a wiring bug, never a success.
        NetEffect::PolicyFetch(_) => NetEvent::PolicyFetch(PolicyFetchEvent::Unavailable(
            "policy fetch must be dispatched by the operations runner".to_string(),
        )),
        NetEffect::JobRecord(_) => NetEvent::JobRecord(JobRecordEvent::Unavailable(
            "job-record replication must be dispatched by the operations runner".to_string(),
        )),
        NetEffect::LaunchOffer(_) => NetEvent::LaunchOffer(LaunchOfferEvent::Unavailable(
            "launch offer must be dispatched by the operations runner".to_string(),
        )),
        NetEffect::PolicySign(_) => NetEvent::PolicySign(PolicySignEvent::Unavailable(
            "policy publication signing must be dispatched by the operations runner".to_string(),
        )),
    }
}

fn audit_fallback(nodes: Vec<NodeId>, limit: usize) -> AuditPageBatch {
    let mut batch = AuditPageBatch::with_limit(limit);
    // Every node of a fallback batch is missing; mark_missing bounds the set and
    // rolls the excess into missing_overflow, so nothing is silently dropped.
    for node in nodes {
        batch.mark_missing(node);
    }
    batch
}

#[tracing::instrument(
    name = "net.effect.dht",
    level = "debug",
    skip(ctx, effect),
    fields(effect = dht_effect_kind(&effect))
)]
async fn handle_dht_effect(ctx: &NetEffectContext, effect: DhtEffect) -> NetEvent {
    let dht = ctx.dht.as_ref();
    match effect {
        DhtEffect::Put {
            key,
            realm_id,
            value,
            ttl,
        } => {
            trace!(
                event = "dht.put.started",
                key = %hex_prefix(key.as_bytes()),
                realm_id = %realm_id,
                value_len = value.len(),
                ttl_secs = ttl.as_secs(),
                "Starting DHT put"
            );
            match dht.put(&key, realm_id, value, ttl).await {
                Ok(stats) => NetEvent::Dht(DhtEvent::PutComplete {
                    key,
                    remote_attempt_count: stats.remote_attempt_count,
                    remote_store_count: stats.remote_store_count,
                }),
                Err(error) => {
                    warn!(key = %hex_prefix(key.as_bytes()), error = %error, "DHT put failed");
                    NetEvent::Dht(DhtEvent::Error {
                        error: DhtError::StoreFailed(error.to_string()),
                    })
                }
            }
        }
        DhtEffect::Get {
            key,
            realm_filter,
            options,
        } => {
            trace!(
                event = "dht.get.started",
                key = %hex_prefix(key.as_bytes()),
                realm_id = ?realm_filter,
                deadline_ms = options.deadline.as_millis(),
                "Starting DHT get"
            );
            if let Some(realm_id) = options.presence {
                return serve_presence(ctx, realm_id, key, realm_filter, options).await;
            }
            run_get(dht, key, realm_filter, options).await
        }
    }
}

async fn run_get(
    dht: &DhtHandle,
    key: DhtKeyId,
    realm_filter: Option<RealmId>,
    options: DhtGetOptions,
) -> NetEvent {
    match dht.get(&key, realm_filter, options).await {
        Ok(values) => NetEvent::Dht(DhtEvent::GetResult {
            key,
            values,
            stale: false,
        }),
        Err(error) => {
            warn!(key = %hex_prefix(key.as_bytes()), error = %error, "DHT get failed");
            NetEvent::Dht(DhtEvent::Error {
                error: DhtError::Other(error.to_string()),
            })
        }
    }
}

/// Serves realm presence from a young snapshot. A stale one starts a single
/// refresh that every stale reader awaits inside its own deadline, so a peer
/// is reported unseen only when that refresh cannot answer in time.
async fn serve_presence(
    ctx: &NetEffectContext,
    realm_id: RealmId,
    key: DhtKeyId,
    realm_filter: Option<RealmId>,
    options: DhtGetOptions,
) -> NetEvent {
    match ctx.presence.serve(realm_id, Instant::now()) {
        PresenceServe::Fresh(values) => NetEvent::Dht(DhtEvent::GetResult {
            key,
            values,
            stale: false,
        }),
        PresenceServe::Stale {
            values,
            refresh,
            settled,
        } => {
            if refresh {
                spawn_refresh(ctx, realm_id, key, realm_filter, options);
            }
            let wait = options.deadline.saturating_sub(PRESENCE_ANSWER_MARGIN);
            if tokio::time::timeout(wait, settled.cancelled())
                .await
                .is_ok()
                && let Some(values) = ctx.presence.fresh(realm_id, Instant::now())
            {
                return NetEvent::Dht(DhtEvent::GetResult {
                    key,
                    values,
                    stale: false,
                });
            }
            NetEvent::Dht(DhtEvent::GetResult {
                key,
                values,
                stale: true,
            })
        }
        PresenceServe::Cold => {
            let event = run_get(ctx.dht.as_ref(), key, realm_filter, options).await;
            if let NetEvent::Dht(DhtEvent::GetResult { values, .. }) = &event {
                ctx.presence.store(realm_id, values.clone(), Instant::now());
            }
            event
        }
    }
}

fn spawn_refresh(
    ctx: &NetEffectContext,
    realm_id: RealmId,
    key: DhtKeyId,
    realm_filter: Option<RealmId>,
    options: DhtGetOptions,
) {
    if ctx.tasks.is_closed() || ctx.shutdown.is_cancelled() {
        ctx.presence.release(realm_id);
        return;
    }

    let dht = ctx.dht.clone();
    #[cfg(test)]
    let refresh_probe = ctx.refresh_probe.clone();
    spawn_refresh_task(
        &ctx.presence,
        &ctx.tasks,
        &ctx.shutdown,
        realm_id,
        move |shutdown| async move {
            #[cfg(test)]
            if let Some(probe) = refresh_probe {
                probe
                    .starts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                probe.started.notify_one();
                tokio::select! {
                    _ = shutdown.cancelled() => return None,
                    _ = probe.release.notified() => {}
                }
            }
            tokio::select! {
                _ = shutdown.cancelled() => None,
                result = dht.get(&key, realm_filter, options) => {
                    Some(result.map_err(|error| error.to_string()))
                }
            }
        },
    );
}

fn spawn_refresh_task<F, Fut>(
    presence: &RealmPresenceCache,
    tasks: &TaskTracker,
    shutdown: &CancellationToken,
    realm_id: RealmId,
    refresh: F,
) where
    F: FnOnce(CancellationToken) -> Fut + Send + 'static,
    Fut: Future<Output = Option<Result<Vec<DhtEntry>, String>>> + Send + 'static,
{
    let presence = presence.clone();
    let shutdown = shutdown.clone();
    tasks.spawn(async move {
        // Released on drop, so a panicking refresh cannot park stale readers
        // behind a slot that is never handed back.
        let _slot = RefreshSlot {
            presence: presence.clone(),
            realm_id,
        };
        match refresh(shutdown).await {
            Some(Ok(values)) => presence.store(realm_id, values, Instant::now()),
            Some(Err(error)) => {
                debug!(error = %error, "realm presence refresh failed; keeping the previous snapshot")
            }
            None => {}
        }
    });
}

struct RefreshSlot {
    presence: RealmPresenceCache,
    realm_id: RealmId,
}

impl Drop for RefreshSlot {
    fn drop(&mut self) {
        self.presence.release(self.realm_id);
    }
}

#[tracing::instrument(
    name = "net.effect.stream",
    level = "debug",
    skip(effect),
    fields(effect = stream_effect_kind(&effect))
)]
async fn handle_stream_effect(effect: StreamEffect) -> NetEvent {
    match effect {
        StreamEffect::Open { node_id, .. } => NetEvent::Stream(StreamEvent::Error {
            stream_id: 0,
            error: StreamError::Other(format!(
                "Stream effects are unsupported; call NetHandle::open_stream for node {node_id}"
            )),
        }),
        StreamEffect::Close { stream_id } => NetEvent::Stream(StreamEvent::Error {
            stream_id,
            error: StreamError::Other(
                "Stream effects are unsupported without stream registry".to_string(),
            ),
        }),
    }
}

fn net_effect_kind(effect: &NetEffect) -> &'static str {
    match effect {
        NetEffect::Dht(_) => "dht",
        NetEffect::DocumentSync(_) => "document_sync",
        NetEffect::Stream(_) => "stream",
        NetEffect::JobControl(_) => "job_control",
        NetEffect::AuditPage(_) => "audit_page",
        NetEffect::PolicyFetch(_) => "policy_fetch",
        NetEffect::JobRecord(_) => "job_record",
        NetEffect::LaunchOffer(_) => "launch_offer",
        NetEffect::PolicySign(_) => "policy_sign",
    }
}

fn dht_effect_kind(effect: &DhtEffect) -> &'static str {
    match effect {
        DhtEffect::Put { .. } => "put",
        DhtEffect::Get { .. } => "get",
    }
}

fn stream_effect_kind(effect: &StreamEffect) -> &'static str {
    match effect {
        StreamEffect::Open { .. } => "open",
        StreamEffect::Close { .. } => "close",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::audit::MAX_AUDIT_PEERS;
    use aruna_core::effects::{DhtEffect, DhtGetOptions, NetEffect};
    use aruna_core::events::{DhtEvent, NetEvent};
    use aruna_core::id::NodeId;
    use aruna_core::keys::realm_presence_key;
    use aruna_storage::FjallStorage;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    async fn handler_context(seed: u8) -> (crate::NetHandle, NetEffectContext, TempDir, RealmId) {
        let directory = tempfile::tempdir().expect("test storage directory");
        let storage = FjallStorage::open(directory.path().to_str().expect("test path"))
            .expect("test storage");
        let realm_id = RealmId::from_bytes([seed; 32]);
        let handle = crate::NetHandle::new(
            crate::NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("test bind address"),
                secret_key: Some(iroh::SecretKey::from_bytes(&[seed; 32])),
                realm_id,
                discovery_method: crate::DiscoveryMethod::None,
                relay_method: crate::RelayMethod::None,
                ..crate::NetConfig::default()
            },
            storage,
        )
        .await
        .expect("test net handle");
        let context = NetEffectContext {
            dht: handle.inner.dht.clone(),
            document_sync: handle.inner.document_sync.clone(),
            presence: RealmPresenceCache::default(),
            tasks: TaskTracker::new(),
            shutdown: CancellationToken::new(),
            refresh_probe: None,
        };
        (handle, context, directory, realm_id)
    }

    async fn handler_pair(
        seed: u8,
    ) -> (
        crate::NetHandle,
        crate::NetHandle,
        NetEffectContext,
        TempDir,
        TempDir,
        RealmId,
    ) {
        let first_directory = tempfile::tempdir().expect("first storage directory");
        let second_directory = tempfile::tempdir().expect("second storage directory");
        let first_storage =
            FjallStorage::open(first_directory.path().to_str().expect("first storage path"))
                .expect("first storage");
        let second_storage = FjallStorage::open(
            second_directory
                .path()
                .to_str()
                .expect("second storage path"),
        )
        .expect("second storage");
        let realm_id = RealmId::from_bytes([seed; 32]);
        let first_secret = iroh::SecretKey::from_bytes(&[seed; 32]);
        let second_secret = iroh::SecretKey::from_bytes(&[seed + 1; 32]);
        let second_id = second_secret.public();
        let first = crate::NetHandle::new(
            crate::NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("first bind address"),
                secret_key: Some(first_secret),
                realm_id,
                peer_nodes: vec![second_id],
                discovery_method: crate::DiscoveryMethod::None,
                relay_method: crate::RelayMethod::None,
                ..crate::NetConfig::default()
            },
            first_storage,
        )
        .await
        .expect("first net handle");
        let second = crate::NetHandle::new(
            crate::NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("second bind address"),
                secret_key: Some(second_secret),
                realm_id,
                peer_nodes: vec![first.node_id()],
                discovery_method: crate::DiscoveryMethod::None,
                relay_method: crate::RelayMethod::None,
                ..crate::NetConfig::default()
            },
            second_storage,
        )
        .await
        .expect("second net handle");
        first.add_peer_addr(second.endpoint_addr()).await;
        second.add_peer_addr(first.endpoint_addr()).await;
        let context = NetEffectContext {
            dht: first.inner.dht.clone(),
            document_sync: first.inner.document_sync.clone(),
            presence: RealmPresenceCache::default(),
            tasks: TaskTracker::new(),
            shutdown: CancellationToken::new(),
            refresh_probe: None,
        };
        (
            first,
            second,
            context,
            first_directory,
            second_directory,
            realm_id,
        )
    }

    fn presence_effect(realm_id: RealmId) -> NetEffect {
        NetEffect::Dht(DhtEffect::Get {
            key: realm_presence_key(&realm_id),
            realm_filter: Some(realm_id),
            options: DhtGetOptions::presence(Duration::from_secs(4), realm_id),
        })
    }

    fn stale_entry(realm_id: RealmId, seed: u8) -> DhtEntry {
        make_entry(seed, realm_id)
    }

    async fn finish_tasks(context: &NetEffectContext) {
        context.tasks.close();
        context.tasks.wait().await;
    }

    fn make_node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn make_entry(seed: u8, realm_id: RealmId) -> DhtEntry {
        DhtEntry {
            node_id: make_node(seed),
            realm_id,
            value: Vec::new(),
            expires_at: 0,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn serves_fresh() {
        let cache = RealmPresenceCache::default();
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let start = Instant::now();
        cache.store(realm_id, vec![make_entry(1, realm_id)], start);

        assert!(matches!(
            cache.serve(realm_id, start + Duration::from_secs(5)),
            PresenceServe::Fresh(values) if values.len() == 1
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_once() {
        // A stale snapshot serves every caller but starts one refresh.
        let cache = RealmPresenceCache::default();
        let realm_id = RealmId::from_bytes([2u8; 32]);
        let start = Instant::now();
        cache.store(realm_id, vec![make_entry(2, realm_id)], start);
        let stale = start + Duration::from_secs(20);

        assert!(matches!(
            cache.serve(realm_id, stale),
            PresenceServe::Stale { refresh: true, .. }
        ));
        assert!(matches!(
            cache.serve(realm_id, stale),
            PresenceServe::Stale { refresh: false, .. }
        ));

        cache.release(realm_id);
        assert!(matches!(
            cache.serve(realm_id, stale),
            PresenceServe::Stale { refresh: true, .. }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_success() {
        let cache = RealmPresenceCache::default();
        let tasks = TaskTracker::new();
        let shutdown = CancellationToken::new();
        let realm_id = RealmId::from_bytes([5u8; 32]);
        let start = Instant::now();
        cache.store(realm_id, vec![make_entry(5, realm_id)], start);
        tokio::time::advance(Duration::from_secs(20)).await;
        assert!(matches!(
            cache.serve(realm_id, Instant::now()),
            PresenceServe::Stale { refresh: true, .. }
        ));

        spawn_refresh_task(&cache, &tasks, &shutdown, realm_id, move |_| async move {
            Some(Ok(vec![make_entry(6, realm_id)]))
        });
        tasks.close();
        tasks.wait().await;

        assert!(matches!(
            cache.serve(realm_id, Instant::now()),
            PresenceServe::Fresh(values) if values[0].node_id == make_node(6)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_empty() {
        let cache = RealmPresenceCache::default();
        let tasks = TaskTracker::new();
        let shutdown = CancellationToken::new();
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let start = Instant::now();
        cache.store(realm_id, vec![make_entry(6, realm_id)], start);
        tokio::time::advance(Duration::from_secs(20)).await;
        assert!(matches!(
            cache.serve(realm_id, Instant::now()),
            PresenceServe::Stale { refresh: true, .. }
        ));

        spawn_refresh_task(&cache, &tasks, &shutdown, realm_id, |_| async {
            Some(Ok(Vec::new()))
        });
        tasks.close();
        tasks.wait().await;

        assert!(matches!(
            cache.serve(realm_id, Instant::now()),
            PresenceServe::Cold
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_failure() {
        let cache = RealmPresenceCache::default();
        let tasks = TaskTracker::new();
        let shutdown = CancellationToken::new();
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let start = Instant::now();
        cache.store(realm_id, vec![make_entry(7, realm_id)], start);
        tokio::time::advance(Duration::from_secs(20)).await;
        assert!(matches!(
            cache.serve(realm_id, Instant::now()),
            PresenceServe::Stale { refresh: true, .. }
        ));

        spawn_refresh_task(&cache, &tasks, &shutdown, realm_id, |_| async {
            Some(Err("offline".to_string()))
        });
        tasks.close();
        tasks.wait().await;

        assert!(matches!(
            cache.serve(realm_id, Instant::now()),
            PresenceServe::Stale { refresh: true, values, .. } if values[0].node_id == make_node(7)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_cancel() {
        let cache = RealmPresenceCache::default();
        let tasks = TaskTracker::new();
        let shutdown = CancellationToken::new();
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let start = Instant::now();
        cache.store(realm_id, vec![make_entry(8, realm_id)], start);
        tokio::time::advance(Duration::from_secs(20)).await;
        assert!(matches!(
            cache.serve(realm_id, Instant::now()),
            PresenceServe::Stale { refresh: true, .. }
        ));

        spawn_refresh_task(&cache, &tasks, &shutdown, realm_id, |shutdown| async move {
            shutdown.cancelled().await;
            None
        });
        shutdown.cancel();
        tasks.close();
        tasks.wait().await;

        assert!(matches!(
            cache.serve(realm_id, Instant::now()),
            PresenceServe::Stale { refresh: true, .. }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_panic() {
        // A panicking refresh must still hand the single-flight slot back.
        let cache = RealmPresenceCache::default();
        let tasks = TaskTracker::new();
        let shutdown = CancellationToken::new();
        let realm_id = RealmId::from_bytes([12u8; 32]);
        cache.store(realm_id, vec![make_entry(12, realm_id)], Instant::now());
        tokio::time::advance(Duration::from_secs(20)).await;
        assert!(matches!(
            cache.serve(realm_id, Instant::now()),
            PresenceServe::Stale { refresh: true, .. }
        ));

        spawn_refresh_task(&cache, &tasks, &shutdown, realm_id, |_| async {
            panic!("refresh panicked")
        });
        tasks.close();
        tasks.wait().await;

        assert!(matches!(
            cache.serve(realm_id, Instant::now()),
            PresenceServe::Stale { refresh: true, .. }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn recheck_before_claim() {
        // The refresh stores and releases between the stale read and the slot.
        let cache = RealmPresenceCache::default();
        let realm_id = RealmId::from_bytes([13u8; 32]);
        let start = Instant::now();
        cache.store(realm_id, vec![make_entry(13, realm_id)], start);
        let stale_now = start + Duration::from_secs(20);
        let stale_values = vec![make_entry(13, realm_id)];

        cache.store(realm_id, vec![make_entry(14, realm_id)], stale_now);
        let served = cache.claim_slot(realm_id, stale_now, stale_values);

        assert!(matches!(
            served,
            PresenceServe::Fresh(values) if values[0].node_id == make_node(14)
        ));
        assert!(cache.refreshing.lock().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_single_flight() {
        let cache = RealmPresenceCache::default();
        let tasks = TaskTracker::new();
        let shutdown = CancellationToken::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let start = Instant::now();
        cache.store(realm_id, vec![make_entry(9, realm_id)], start);
        tokio::time::advance(Duration::from_secs(20)).await;
        let stale = Instant::now();
        let first = cache.serve(realm_id, stale);
        assert!(matches!(&first, PresenceServe::Stale { refresh: true, .. }));
        let second = cache.serve(realm_id, stale);
        assert!(matches!(
            &second,
            PresenceServe::Stale { refresh: false, .. }
        ));

        let calls_for_task = calls.clone();
        if matches!(&first, PresenceServe::Stale { refresh: true, .. }) {
            spawn_refresh_task(&cache, &tasks, &shutdown, realm_id, move |_| async move {
                calls_for_task.fetch_add(1, Ordering::SeqCst);
                Some(Ok(vec![make_entry(10, realm_id)]))
            });
        }
        tasks.close();
        tasks.wait().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn refreshes_through_handler() {
        let (handle, context, _directory, realm_id) = handler_context(11).await;
        let key = realm_presence_key(&realm_id);
        let put = handle_net_effect(
            &context,
            NetEffect::Dht(DhtEffect::Put {
                key,
                realm_id,
                value: Vec::new(),
                ttl: Duration::from_secs(300),
            }),
        )
        .await;
        assert!(matches!(put, NetEvent::Dht(DhtEvent::PutComplete { .. })));

        context.presence.store(
            realm_id,
            vec![stale_entry(realm_id, 12)],
            Instant::now() - Duration::from_secs(20),
        );
        // The stale reader waits for the refresh instead of flapping the peer
        // to unseen for a whole freshness window.
        let event = handle_net_effect(&context, presence_effect(realm_id)).await;
        assert!(matches!(
            event,
            NetEvent::Dht(DhtEvent::GetResult {
                stale: false,
                values,
                ..
            }) if values.iter().any(|entry| entry.node_id == handle.node_id())
        ));

        finish_tasks(&context).await;
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn empty_refresh_handler() {
        let (handle, peer, context, _first_directory, _second_directory, realm_id) =
            handler_pair(13).await;
        context.presence.store(
            realm_id,
            vec![stale_entry(realm_id, 13)],
            Instant::now() - Duration::from_secs(20),
        );

        let event = handle_net_effect(&context, presence_effect(realm_id)).await;
        assert!(matches!(
            event,
            NetEvent::Dht(DhtEvent::GetResult {
                stale: true,
                values,
                ..
            }) if values.len() == 1
        ));
        finish_tasks(&context).await;
        assert!(matches!(
            context.presence.serve(realm_id, Instant::now()),
            PresenceServe::Cold
        ));
        handle.shutdown().await;
        peer.shutdown().await;
    }

    #[tokio::test]
    async fn failed_refresh_handler() {
        let (handle, context, _directory, realm_id) = handler_context(14).await;
        context.presence.store(
            realm_id,
            vec![stale_entry(realm_id, 14)],
            Instant::now() - Duration::from_secs(20),
        );
        handle.shutdown().await;

        let event = handle_net_effect(&context, presence_effect(realm_id)).await;
        assert!(matches!(
            event,
            NetEvent::Dht(DhtEvent::GetResult {
                stale: true,
                values,
                ..
            }) if values.len() == 1
        ));
        finish_tasks(&context).await;
        assert!(matches!(
            context.presence.serve(realm_id, Instant::now()),
            PresenceServe::Stale { refresh: true, values, .. } if values.len() == 1
        ));
    }

    #[tokio::test]
    async fn cancelled_refresh_handler() {
        let (handle, context, _directory, realm_id) = handler_context(15).await;
        context.presence.store(
            realm_id,
            vec![stale_entry(realm_id, 15)],
            Instant::now() - Duration::from_secs(20),
        );
        context.shutdown.cancel();

        let event = handle_net_effect(&context, presence_effect(realm_id)).await;
        assert!(matches!(
            event,
            NetEvent::Dht(DhtEvent::GetResult { stale: true, .. })
        ));
        assert!(matches!(
            context.presence.serve(realm_id, Instant::now()),
            PresenceServe::Stale { refresh: true, .. }
        ));
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn single_flight_handler() {
        // Concurrent stale readers join one refresh and both answer fresh.
        let (handle, mut context, _directory, realm_id) = handler_context(16).await;
        let probe = Arc::new(RefreshProbe::new());
        context.refresh_probe = Some(probe.clone());
        let put = handle_net_effect(
            &context,
            NetEffect::Dht(DhtEffect::Put {
                key: realm_presence_key(&realm_id),
                realm_id,
                value: Vec::new(),
                ttl: Duration::from_secs(300),
            }),
        )
        .await;
        assert!(matches!(put, NetEvent::Dht(DhtEvent::PutComplete { .. })));
        context.presence.store(
            realm_id,
            vec![stale_entry(realm_id, 16)],
            Instant::now() - Duration::from_secs(20),
        );

        let releaser = {
            let probe = probe.clone();
            tokio::spawn(async move {
                probe.started.notified().await;
                probe.release.notify_one();
            })
        };
        let (first, second) = tokio::join!(
            handle_net_effect(&context, presence_effect(realm_id)),
            handle_net_effect(&context, presence_effect(realm_id)),
        );
        releaser.await.expect("releaser joins");

        for event in [first, second] {
            assert!(matches!(
                event,
                NetEvent::Dht(DhtEvent::GetResult {
                    stale: false,
                    values,
                    ..
                }) if values.iter().any(|entry| entry.node_id == handle.node_id())
            ));
        }
        assert_eq!(probe.starts.load(Ordering::SeqCst), 1);
        finish_tasks(&context).await;
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn deadline_keeps_stale() {
        // A refresh that outlives the caller's deadline still answers from the
        // snapshot rather than reporting the realm as unseen.
        let (handle, mut context, _directory, realm_id) = handler_context(19).await;
        let probe = Arc::new(RefreshProbe::new());
        context.refresh_probe = Some(probe.clone());
        context.presence.store(
            realm_id,
            vec![stale_entry(realm_id, 19)],
            Instant::now() - Duration::from_secs(20),
        );

        let event = handle_net_effect(
            &context,
            NetEffect::Dht(DhtEffect::Get {
                key: realm_presence_key(&realm_id),
                realm_filter: Some(realm_id),
                options: DhtGetOptions::presence(Duration::from_millis(300), realm_id),
            }),
        )
        .await;
        assert!(matches!(
            event,
            NetEvent::Dht(DhtEvent::GetResult {
                stale: true,
                values,
                ..
            }) if values[0].node_id == make_node(19)
        ));

        context.shutdown.cancel();
        finish_tasks(&context).await;
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn closed_tracker_handler() {
        let (handle, context, _directory, realm_id) = handler_context(17).await;
        context.presence.store(
            realm_id,
            vec![stale_entry(realm_id, 17)],
            Instant::now() - Duration::from_secs(20),
        );
        context.tasks.close();

        let event = handle_net_effect(&context, presence_effect(realm_id)).await;
        assert!(matches!(
            event,
            NetEvent::Dht(DhtEvent::GetResult { stale: true, .. })
        ));
        assert!(matches!(
            context.presence.serve(realm_id, Instant::now()),
            PresenceServe::Stale { refresh: true, .. }
        ));
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn expired_presence_handler() {
        let (handle, peer, context, _first_directory, _second_directory, realm_id) =
            handler_pair(18).await;
        context.presence.store(
            realm_id,
            vec![stale_entry(realm_id, 18)],
            Instant::now() - PRESENCE_MAX_STALE - Duration::from_secs(1),
        );

        let event = handle_net_effect(&context, presence_effect(realm_id)).await;
        assert!(matches!(
            event,
            NetEvent::Dht(DhtEvent::GetResult {
                stale: false,
                values,
                ..
            }) if values.is_empty()
        ));
        finish_tasks(&context).await;
        assert!(matches!(
            context.presence.serve(realm_id, Instant::now()),
            PresenceServe::Cold
        ));
        handle.shutdown().await;
        peer.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn expires_when_old() {
        let cache = RealmPresenceCache::default();
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let start = Instant::now();
        cache.store(realm_id, vec![make_entry(3, realm_id)], start);

        assert!(matches!(
            cache.serve(
                realm_id,
                start + PRESENCE_MAX_STALE + Duration::from_secs(1)
            ),
            PresenceServe::Cold
        ));
        assert!(matches!(
            cache.serve(RealmId::from_bytes([9u8; 32]), start),
            PresenceServe::Cold
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn skips_empty() {
        // Caching "nobody announced yet" would delay discovery of a peer that
        // announces inside the freshness window.
        let cache = RealmPresenceCache::default();
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let start = Instant::now();
        cache.store(realm_id, Vec::new(), start);
        assert!(matches!(cache.serve(realm_id, start), PresenceServe::Cold));

        cache.store(realm_id, vec![make_entry(4, realm_id)], start);
        cache.store(realm_id, Vec::new(), start);
        assert!(matches!(cache.serve(realm_id, start), PresenceServe::Cold));
    }

    #[test]
    fn bounds_overflow() {
        // A fallback batch has no completed node, so an oversized list must
        // report the bounded prefix as missing instead of an empty page.
        let nodes = (1u8..=u8::try_from(MAX_AUDIT_PEERS + 1).unwrap())
            .map(make_node)
            .collect();
        let batch = audit_fallback(nodes, usize::MAX);

        assert!(batch.records.is_empty());
        assert!(batch.completed_nodes.is_empty());
        assert_eq!(batch.missing_nodes.len(), MAX_AUDIT_PEERS);
        assert_eq!(batch.missing_overflow, 1);
    }

    #[test]
    fn keeps_unique() {
        let first = make_node(1);
        let second = make_node(2);
        let batch = audit_fallback(vec![second, first, first], usize::MAX);

        let mut expected = vec![first, second];
        expected.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        assert_eq!(
            batch.missing_nodes.into_iter().collect::<Vec<_>>(),
            expected
        );
        assert_eq!(batch.missing_overflow, 0);
    }
}
