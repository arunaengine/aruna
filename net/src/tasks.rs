//! Background task ownership and the loops the network starts at construction.
//!
//! `BackgroundTasks` is the single owner of every spawned loop. The
//! constructor starts services first and tasks last, so nothing fallible runs
//! after the first spawn; a failure before that point has only the endpoint and
//! DHT driver to release, which the constructor handles explicitly. Shutdown
//! drains inbound handlers first and then joins through this owner.

use std::sync::Arc;

use aruna_core::alpn::Alpn;
use aruna_core::id::NodeId;
use crossfire::TrySendError;
use iroh::Endpoint;
use iroh::endpoint::{RecvStream, SendStream};
use parking_lot::RwLock;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{Instrument, warn};

use crate::DhtHandle;
use crate::EffectHandle;
use crate::InboundEventHandler;
use crate::dht::driver::InboundSender;
use crate::document_sync::DocumentSyncService;
use crate::effect_handlers::{self, NetEffectContext};
use crate::streams;

pub(crate) const MAX_INBOUND_APP_STREAM_HANDLERS: usize = 1024;

/// One owner for every background loop: joining happens in one place and the
/// handles live exactly as long as `NetInner` does.
#[derive(Default)]
pub(crate) struct BackgroundTasks {
    handles: Vec<JoinHandle<()>>,
}

impl BackgroundTasks {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn push(&mut self, handle: JoinHandle<()>) {
        self.handles.push(handle);
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// Joins every loop, newest first. The set is empty afterwards.
    pub(crate) async fn join_all(&mut self) {
        while let Some(handle) = self.handles.pop() {
            let _ = handle.await;
        }
    }
}

/// Serializes accepted net effects onto the effect handlers. Each accepted
/// effect runs in its own tracked task, so the effect completion boundary is
/// the tracker plus the dispatcher, not the dispatcher alone. A tracker close
/// only lets `wait` finish; it does not reject later insertions, so callers
/// must join the dispatcher before treating the tracker as the final boundary.
pub(crate) fn spawn_effect_dispatch(
    mut effect_rx: mpsc::Receiver<EffectHandle>,
    effect_context: Arc<NetEffectContext>,
    effect_tasks: TaskTracker,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                maybe_effect = effect_rx.recv() => {
                    let Some((effect, response_tx, span)) = maybe_effect else { break };
                    let context = effect_context.clone();
                    effect_tasks.spawn(async move {
                        let event = effect_handlers::handle_net_effect(&context, effect).await;
                        let _ = response_tx.send(event);
                    }.instrument(span));
                }
            }
        }
    })
}

/// Forwards inbound DHT streams into the DHT driver's bounded inbox.
pub(crate) fn spawn_dht_inbound_forwarder(
    mut dht_rx: mpsc::Receiver<(SendStream, RecvStream, NodeId)>,
    dht_inbound_tx: InboundSender,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some((send, recv, peer_id)) = dht_rx.recv().await {
            match dht_inbound_tx.try_send((send, recv, peer_id)) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    warn!(node_id = %peer_id, "Dropping inbound DHT stream: queue full");
                }
                Err(TrySendError::Disconnected(_)) => break,
            }
        }
    })
}

/// Applies admission to inbound app streams and hands them to the registered
/// handler. The handler limit and per-device limit are both checked before any
/// task is spawned.
pub(crate) fn spawn_inbound_stream_dispatch(
    mut stream_rx: mpsc::Receiver<(Alpn, streams::BiStream, NodeId)>,
    dht: Arc<DhtHandle>,
    inbound_handler: Arc<RwLock<Option<Arc<dyn InboundEventHandler>>>>,
    inbound_admission: streams::InboundAdmission,
    inbound_tasks: TaskTracker,
    inbound_stream_handlers: Arc<Semaphore>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some((alpn, stream, peer_id)) = stream_rx.recv().await {
            if let Err(err) = dht.add_peer(peer_id) {
                warn!(
                    node_id = %peer_id,
                    error = %err,
                    "Failed to add inbound stream peer to routing queue"
                );
            }
            let handler = inbound_handler.read().clone();
            if let Some(handler) = handler {
                let Ok(permit) = inbound_stream_handlers.clone().try_acquire_owned() else {
                    warn!(
                        node_id = %peer_id,
                        alpn = %alpn,
                        "Dropping inbound stream: handler limit reached"
                    );
                    continue;
                };
                // A user device is bounded by the realm's published limits
                // rather than responsibilities it does not carry.
                let device_permit = match inbound_admission.admit_stream(peer_id) {
                    Ok(permit) => permit,
                    Err(refusal) => {
                        warn!(
                            node_id = %peer_id,
                            alpn = %alpn,
                            refusal = ?refusal,
                            "Dropping inbound stream: device is over its realm limit"
                        );
                        continue;
                    }
                };
                inbound_tasks.spawn(async move {
                    let _permit = permit;
                    let _device_permit = device_permit;
                    handler.handle_incoming_stream(alpn, stream, peer_id).await;
                });
            } else {
                warn!(node_id = %peer_id, "Dropping inbound stream without registered handler");
            }
        }
    })
}

/// Runs the endpoint accept loop under its own cancellation token so admission
/// can stop before the endpoint is torn down.
pub(crate) fn spawn_accept_loop(
    endpoint: Endpoint,
    dht_tx: mpsc::Sender<(SendStream, RecvStream, NodeId)>,
    stream_tx: mpsc::Sender<(Alpn, streams::BiStream, NodeId)>,
    document_sync: Arc<DocumentSyncService>,
    inbound_admission: streams::InboundAdmission,
    accept_shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        streams::run_accept_loop(
            endpoint,
            dht_tx,
            stream_tx,
            document_sync,
            inbound_admission,
            accept_shutdown,
        )
        .await;
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::{DhtEffect, DhtGetOptions, NetEffect};
    use aruna_core::keys::realm_presence_key;
    use aruna_core::structs::RealmId;
    use aruna_storage::FjallStorage;
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tracing::Span;

    async fn effect_context(
        seed: u8,
    ) -> (crate::NetHandle, Arc<NetEffectContext>, tempfile::TempDir) {
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
        let context = Arc::new(NetEffectContext {
            dht: handle.inner.dht.clone(),
            document_sync: handle.inner.document_sync.clone(),
            presence: effect_handlers::RealmPresenceCache::default(),
            tasks: TaskTracker::new(),
            shutdown: CancellationToken::new(),
            refresh_probe: None,
        });
        (handle, context, directory)
    }

    fn presence_effect_for(seed: u8) -> NetEffect {
        let realm_id = RealmId::from_bytes([seed; 32]);
        NetEffect::Dht(DhtEffect::Get {
            key: realm_presence_key(&realm_id),
            realm_filter: Some(realm_id),
            options: DhtGetOptions::presence(Duration::from_secs(1), realm_id),
        })
    }

    // An accepted effect future is part of the completion boundary: after the
    // dispatcher joins, the tracker still owns the running child and waits for
    // it and its response before shutdown returns.
    #[tokio::test]
    async fn accepted_effect_cannot_escape_the_completion_boundary() {
        let (_handle, context, _directory) = effect_context(0x61).await;
        let effect_tasks = TaskTracker::new();
        let (effect_tx, effect_rx) = mpsc::channel::<EffectHandle>(8);
        let shutdown = CancellationToken::new();
        let dispatcher =
            spawn_effect_dispatch(effect_rx, context, effect_tasks.clone(), shutdown.clone());

        let (response_tx, response_rx) = oneshot::channel();
        effect_tx
            .send((presence_effect_for(0x61), response_tx, Span::current()))
            .await
            .expect("the dispatcher is accepting effects");
        for _ in 0..100 {
            if !effect_tasks.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            !effect_tasks.is_empty(),
            "the accepted effect must be tracked"
        );

        // Closing admission and joining the dispatcher is the insertion barrier.
        shutdown.cancel();
        dispatcher.await.expect("the dispatcher joins");

        assert!(
            !effect_tasks.is_empty(),
            "accepted work must not escape the dispatcher join"
        );
        // Admission close only lets `wait` finish; the tracked child is still
        // awaited to completion by the boundary.
        effect_tasks.close();
        tokio::time::timeout(Duration::from_secs(30), effect_tasks.wait())
            .await
            .expect("the tracked effect must complete before the boundary");
        assert!(
            response_rx.await.is_ok(),
            "the completed effect must deliver its response"
        );
    }

    // After the dispatcher stops, the effect channel is closed: a later send is
    // rejected instead of being accepted into an unowned task.
    #[tokio::test]
    async fn new_effects_are_rejected_after_the_dispatcher_stops() {
        let (_handle, context, _directory) = effect_context(0x62).await;
        let (effect_tx, effect_rx) = mpsc::channel::<EffectHandle>(1);
        let shutdown = CancellationToken::new();
        let dispatcher =
            spawn_effect_dispatch(effect_rx, context, TaskTracker::new(), shutdown.clone());

        shutdown.cancel();
        dispatcher.await.expect("the dispatcher joins");

        let (response_tx, _response_rx) = oneshot::channel();
        assert!(
            effect_tx
                .send((presence_effect_for(0x62), response_tx, Span::current()))
                .await
                .is_err(),
            "the closed admission must reject a new effect"
        );
    }
}
