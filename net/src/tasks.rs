//! Background task ownership and the loops the network starts at construction.
//! `BackgroundTasks` owns every spawned loop; the constructor starts services
//! first and tasks last, so no fallible step runs after the first spawn.

use std::future::Future;
use std::sync::Arc;

use aruna_core::alpn::Alpn;
use aruna_core::effects::NetEffect;
use aruna_core::events::NetEvent;
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

pub(crate) const MAX_STREAM_HANDLERS: usize = 1024;

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

    /// Joins every loop, newest first. A handle leaves the owner only after its
    /// task was observed finished, so a join future dropped by an outer
    /// deadline stays resumable instead of detaching the task it had popped.
    pub(crate) async fn join_all(&mut self) {
        while let Some(handle) = self.handles.last_mut() {
            if !handle.is_finished() {
                let _ = (&mut *handle).await;
            }
            self.handles.pop();
        }
    }
}

/// Executes one accepted net effect and returns its response event. Production
/// routes through [`effect_handlers::handle_net_effect`]; tests supply
/// controlled start, release, and completion signals instead of real I/O.
pub(crate) trait EffectExecutor: Send + Sync + 'static {
    fn execute(&self, effect: NetEffect) -> impl Future<Output = NetEvent> + Send;
}

impl EffectExecutor for NetEffectContext {
    async fn execute(&self, effect: NetEffect) -> NetEvent {
        effect_handlers::handle_net_effect(self, effect).await
    }
}

/// Serializes accepted net effects onto the executor. Cancellation still spawns
/// buffered effects under the tracker (accepted work settles by completion, not
/// drops), so callers join the dispatcher before trusting the tracker's wait.
pub(crate) fn spawn_effect_dispatch<E: EffectExecutor>(
    mut effect_rx: mpsc::Receiver<EffectHandle>,
    executor: Arc<E>,
    effect_tasks: TaskTracker,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    // `recv` yields `None` only once send capacity reserved
                    // before `close` settles, so it waits out held permits.
                    effect_rx.close();
                    while let Some((effect, response_tx, span)) = effect_rx.recv().await {
                        let executor = executor.clone();
                        effect_tasks.spawn(async move {
                            let event = executor.execute(effect).await;
                            let _ = response_tx.send(event);
                        }.instrument(span));
                    }
                    break;
                }
                maybe_effect = effect_rx.recv() => {
                    let Some((effect, response_tx, span)) = maybe_effect else { break };
                    let executor = executor.clone();
                    effect_tasks.spawn(async move {
                        let event = executor.execute(effect).await;
                        let _ = response_tx.send(event);
                    }.instrument(span));
                }
            }
        }
    })
}

/// Forwards inbound DHT streams into the DHT driver's bounded inbox.
pub(crate) fn spawn_dht_forwarder(
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
pub(crate) fn spawn_stream_dispatch(
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
    use aruna_core::structs::identity::realm::RealmId;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tracing::Span;

    /// Deterministic executor: reports each start, blocks until released, and
    /// counts completions. None of the dispatcher ownership assertions need a
    /// runtime endpoint, storage, or a timing guess.
    struct ControlledExecutor {
        started: mpsc::UnboundedSender<()>,
        release: Arc<Semaphore>,
        completed: Arc<AtomicUsize>,
    }

    impl EffectExecutor for ControlledExecutor {
        async fn execute(&self, _effect: NetEffect) -> NetEvent {
            let _ = self.started.send(());
            let _ = self.release.acquire().await;
            self.completed.fetch_add(1, Ordering::SeqCst);
            NetEvent::Error(aruna_core::events::NetError::ChannelClosed)
        }
    }

    fn controlled_executor() -> (
        Arc<ControlledExecutor>,
        mpsc::UnboundedReceiver<()>,
        Arc<Semaphore>,
        Arc<AtomicUsize>,
    ) {
        let (started_tx, started_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Semaphore::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(ControlledExecutor {
                started: started_tx,
                release: release.clone(),
                completed: completed.clone(),
            }),
            started_rx,
            release,
            completed,
        )
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
    async fn accepted_stays_tracked() {
        let (executor, mut started, release, completed) = controlled_executor();
        let effect_tasks = TaskTracker::new();
        let (effect_tx, effect_rx) = mpsc::channel::<EffectHandle>(8);
        let shutdown = CancellationToken::new();
        let dispatcher =
            spawn_effect_dispatch(effect_rx, executor, effect_tasks.clone(), shutdown.clone());

        let (response_tx, response_rx) = oneshot::channel();
        effect_tx
            .send((presence_effect_for(0x61), response_tx, Span::current()))
            .await
            .expect("the dispatcher is accepting effects");
        started.recv().await.expect("the accepted effect started");
        assert_eq!(effect_tasks.len(), 1, "the accepted effect is tracked");

        // Cancelling admission and joining the dispatcher is the insertion
        // barrier; the running child stays owned by the tracker.
        shutdown.cancel();
        dispatcher.await.expect("the dispatcher joins");

        assert_eq!(
            effect_tasks.len(),
            1,
            "accepted work must not escape the dispatcher join"
        );
        release.add_permits(1);
        effect_tasks.close();
        effect_tasks.wait().await;
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        assert!(
            response_rx.await.is_ok(),
            "the completed effect must deliver its response"
        );
    }

    // Effects already buffered when the dispatcher is cancelled are settled by
    // being spawned under the tracker, not dropped with the receiver.
    #[tokio::test]
    async fn queued_effects_settled() {
        let (executor, mut started, release, completed) = controlled_executor();
        let effect_tasks = TaskTracker::new();
        let (effect_tx, effect_rx) = mpsc::channel::<EffectHandle>(2);
        for seed in [0x62, 0x63] {
            let (response_tx, _response_rx) = oneshot::channel();
            effect_tx
                .send((presence_effect_for(seed), response_tx, Span::current()))
                .await
                .expect("the channel buffers the queued effect");
        }

        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let dispatcher =
            spawn_effect_dispatch(effect_rx, executor, effect_tasks.clone(), shutdown.clone());
        dispatcher.await.expect("the dispatcher joins");

        assert_eq!(
            effect_tasks.len(),
            2,
            "every buffered effect must be spawned before the receiver drops"
        );
        for _ in 0..2 {
            started.recv().await.expect("the queued effect started");
        }
        release.add_permits(2);
        effect_tasks.close();
        effect_tasks.wait().await;
        assert_eq!(completed.load(Ordering::SeqCst), 2);
    }

    // Capacity reserved before cancellation is accepted work: the dispatcher
    // must not finish while a held permit can still publish, even past a budget.
    #[tokio::test(start_paused = true)]
    async fn held_reservation_settles() {
        let (executor, mut started, release, completed) = controlled_executor();
        let effect_tasks = TaskTracker::new();
        let (effect_tx, effect_rx) = mpsc::channel::<EffectHandle>(2);
        let permit = effect_tx
            .reserve()
            .await
            .expect("capacity is reserved before cancellation");
        let shutdown = CancellationToken::new();
        let dispatcher =
            spawn_effect_dispatch(effect_rx, executor, effect_tasks.clone(), shutdown.clone());

        shutdown.cancel();
        // The queue is empty, so only the held permit keeps the dispatcher open.
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(
            !dispatcher.is_finished(),
            "a held reservation must keep the dispatcher from completing"
        );

        let (response_tx, response_rx) = oneshot::channel();
        permit.send((presence_effect_for(0x65), response_tx, Span::current()));
        dispatcher
            .await
            .expect("the dispatcher joins once the reservation settles");

        assert_eq!(
            effect_tasks.len(),
            1,
            "the reserved effect must be spawned before the dispatcher exits"
        );
        started.recv().await.expect("the reserved effect started");
        release.add_permits(1);
        effect_tasks.close();
        effect_tasks.wait().await;
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        assert!(
            response_rx.await.is_ok(),
            "the reserved effect must deliver its response"
        );
    }

    // Releasing the reservation without sending settles the boundary too: the
    // dispatcher completes with no accepted work behind it.
    #[tokio::test(start_paused = true)]
    async fn released_reservation_settles() {
        let (executor, _started, _release, completed) = controlled_executor();
        let effect_tasks = TaskTracker::new();
        let (effect_tx, effect_rx) = mpsc::channel::<EffectHandle>(1);
        let permit = effect_tx
            .reserve()
            .await
            .expect("capacity is reserved before cancellation");
        let shutdown = CancellationToken::new();
        let dispatcher =
            spawn_effect_dispatch(effect_rx, executor, effect_tasks.clone(), shutdown.clone());

        shutdown.cancel();
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(
            !dispatcher.is_finished(),
            "a held reservation must keep the dispatcher from completing"
        );

        drop(permit);
        dispatcher
            .await
            .expect("the dispatcher joins once the reservation drops");

        assert_eq!(effect_tasks.len(), 0, "no effect was accepted");
        effect_tasks.close();
        effect_tasks.wait().await;
        assert_eq!(completed.load(Ordering::SeqCst), 0);
    }

    // After the dispatcher stops, the effect channel is closed: a later send is
    // rejected instead of being accepted into an unowned task.
    #[tokio::test]
    async fn late_effects_rejected() {
        let (executor, _started, _release, _completed) = controlled_executor();
        let (effect_tx, effect_rx) = mpsc::channel::<EffectHandle>(1);
        let shutdown = CancellationToken::new();
        let dispatcher =
            spawn_effect_dispatch(effect_rx, executor, TaskTracker::new(), shutdown.clone());

        shutdown.cancel();
        dispatcher.await.expect("the dispatcher joins");

        let (response_tx, _response_rx) = oneshot::channel();
        assert!(
            effect_tx
                .send((presence_effect_for(0x64), response_tx, Span::current()))
                .await
                .is_err(),
            "the closed admission must reject a new effect"
        );
    }
}
