use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use aruna_core::audit::AuditPageBatch;
use aruna_core::effects::{DhtEffect, DhtGetOptions, NetEffect, StreamEffect};
use aruna_core::errors::{DhtError, StreamError};
use aruna_core::events::{DhtEntry, DhtEvent, JobControlEvent, NetEvent, StreamEvent};
use aruna_core::id::{DhtKeyId, NodeId, hex_prefix};
use aruna_core::structs::RealmId;
use parking_lot::Mutex;
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

/// Everything a net effect needs besides the effect itself.
pub struct NetEffectContext {
    pub dht: Arc<DhtHandle>,
    pub document_sync: Arc<DocumentSyncService>,
    pub presence: RealmPresenceCache,
    pub tasks: TaskTracker,
    pub shutdown: CancellationToken,
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
    refreshing: Arc<Mutex<HashSet<RealmId>>>,
}

impl std::fmt::Debug for RealmPresenceCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealmPresenceCache")
            .field("realms", &self.snapshots.lock().len())
            .finish()
    }
}

#[derive(Debug, PartialEq)]
enum PresenceServe {
    Fresh(Vec<DhtEntry>),
    Stale {
        values: Vec<DhtEntry>,
        refresh: bool,
    },
    Cold,
}

impl RealmPresenceCache {
    fn serve(&self, realm_id: RealmId, now: Instant) -> PresenceServe {
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
        PresenceServe::Stale {
            values: snapshot.values.clone(),
            refresh: self.refreshing.lock().insert(realm_id),
        }
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
        self.refreshing.lock().remove(&realm_id);
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

/// Serves realm presence from the snapshot when it is young enough, refreshing
/// a stale one exactly once in the background instead of blocking the caller.
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
        PresenceServe::Stale { values, refresh } => {
            if refresh {
                spawn_refresh(ctx, realm_id, key, realm_filter, options);
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
    let presence = ctx.presence.clone();
    let shutdown = ctx.shutdown.clone();
    ctx.tasks.spawn(async move {
        let refreshed = tokio::select! {
            _ = shutdown.cancelled() => None,
            result = dht.get(&key, realm_filter, options) => Some(result),
        };
        match refreshed {
            Some(Ok(values)) => presence.store(realm_id, values, Instant::now()),
            Some(Err(error)) => {
                debug!(error = %error, "realm presence refresh failed; keeping the previous snapshot")
            }
            None => {}
        }
        presence.release(realm_id);
    });
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
    use aruna_core::id::NodeId;

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
    async fn expires_when_old() {
        let cache = RealmPresenceCache::default();
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let start = Instant::now();
        cache.store(realm_id, vec![make_entry(3, realm_id)], start);

        assert_eq!(
            cache.serve(
                realm_id,
                start + PRESENCE_MAX_STALE + Duration::from_secs(1)
            ),
            PresenceServe::Cold
        );
        assert_eq!(
            cache.serve(RealmId::from_bytes([9u8; 32]), start),
            PresenceServe::Cold
        );
    }

    #[tokio::test(start_paused = true)]
    async fn skips_empty() {
        // Caching "nobody announced yet" would delay discovery of a peer that
        // announces inside the freshness window.
        let cache = RealmPresenceCache::default();
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let start = Instant::now();
        cache.store(realm_id, Vec::new(), start);
        assert_eq!(cache.serve(realm_id, start), PresenceServe::Cold);

        cache.store(realm_id, vec![make_entry(4, realm_id)], start);
        cache.store(realm_id, Vec::new(), start);
        assert_eq!(cache.serve(realm_id, start), PresenceServe::Cold);
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
