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

/// Serializes accepted net effects onto the effect handlers. Each effect runs
/// in its own task so one slow lookup cannot serialize the rest.
pub(crate) fn spawn_effect_dispatch(
    mut effect_rx: mpsc::Receiver<EffectHandle>,
    effect_context: Arc<NetEffectContext>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                maybe_effect = effect_rx.recv() => {
                    let Some((effect, response_tx, span)) = maybe_effect else { break };
                    let context = effect_context.clone();
                    tokio::spawn(async move {
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
