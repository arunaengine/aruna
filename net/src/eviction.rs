//! Eviction maintenance beside the document-sync service it maintains.
//!
//! Irokle journals evictions before reset; this module converts journalled
//! entries into durable outbox rows and releases an entry only once its
//! replacement records commit. Progress happens on handler registration and
//! on new evictions, with a bounded retry timer for an otherwise idle node.
//! The flush decision (which entries may be released) stays in
//! `flush_evicted_documents`; the loop owns the waits and the receiver.

use std::mem;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::InboundEventHandler;
use crate::document_sync::{DocumentSyncService, PendingEviction};

/// How often a journalled eviction whose outbox rows are not yet committed is
/// retried. Progress also happens on handler registration and on any new
/// eviction, so this only bounds an otherwise idle retry.
const EVICTION_RETRY_INTERVAL: Duration = Duration::from_secs(15);

/// Hands journalled evictions to the registered handler and releases only the
/// entries whose replacement records are durable. Everything else stays pending,
/// so a missing handler, a failed write, or a shutdown loses nothing.
pub(crate) async fn flush_evicted_documents(
    inbound_handler: &Arc<RwLock<Option<Arc<dyn InboundEventHandler>>>>,
    document_sync: &Arc<DocumentSyncService>,
    pending: &mut Vec<PendingEviction>,
) -> bool {
    if pending.is_empty() {
        return true;
    }

    let handler = inbound_handler.read().clone();
    let Some(handler) = handler else {
        return false;
    };

    let mut retained = Vec::new();
    let mut flushed = true;
    for entry in mem::take(pending) {
        if !handler
            .handle_evicted_documents(entry.documents.clone())
            .await
        {
            flushed = false;
            retained.push(entry);
            continue;
        }
        if let Err(error) = document_sync.clear_eviction(entry.key).await {
            // The rows are durable and their ids are stable, so a repeat is a
            // no-op; keep the entry rather than risk losing it.
            warn!(%error, "Failed to release a drained eviction journal entry");
            flushed = false;
            retained.push(entry);
        }
    }
    *pending = retained;
    flushed
}

/// Runs the eviction journal conversion until `shutdown` is cancelled, then
/// flushes once more and leaves anything uncommitted journalled for restart.
pub(crate) fn spawn_eviction_maintenance(
    document_sync: Arc<DocumentSyncService>,
    inbound_handler: Arc<RwLock<Option<Arc<dyn InboundEventHandler>>>>,
    inbound_handler_registered: Arc<Notify>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let Some(mut eviction_rx) = document_sync.take_eviction_receiver() else {
            return;
        };
        let mut pending = match document_sync.pending_evictions().await {
            Ok(pending) => pending,
            Err(error) => {
                warn!(%error, "Failed to read the eviction journal on startup");
                Vec::new()
            }
        };
        let mut retry = tokio::time::interval(EVICTION_RETRY_INTERVAL);
        retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    while let Ok(eviction) = eviction_rx.try_recv() {
                        pending.extend(document_sync.consume_eviction(eviction).await);
                    }
                    if !flush_evicted_documents(
                        &inbound_handler,
                        &document_sync,
                        &mut pending,
                    )
                    .await
                    {
                        // Never dropped: the journal outlives this process.
                        warn!(
                            count = pending.len(),
                            "Leaving evicted document sync payloads journalled for restart"
                        );
                    }
                    break;
                },
                _ = inbound_handler_registered.notified(), if !pending.is_empty() => {
                    let _ = flush_evicted_documents(
                        &inbound_handler,
                        &document_sync,
                        &mut pending,
                    )
                    .await;
                },
                _ = retry.tick(), if !pending.is_empty() => {
                    let _ = flush_evicted_documents(
                        &inbound_handler,
                        &document_sync,
                        &mut pending,
                    )
                    .await;
                },
                maybe_eviction = eviction_rx.recv() => {
                    let Some(eviction) = maybe_eviction else { break };
                    pending.extend(document_sync.consume_eviction(eviction).await);
                    if pending.is_empty() {
                        continue;
                    }
                    if !flush_evicted_documents(
                        &inbound_handler,
                        &document_sync,
                        &mut pending,
                    )
                    .await
                    {
                        warn!(
                            count = pending.len(),
                            "Retrying journalled eviction payloads until their outbox rows commit"
                        );
                    }
                }
            }
        }
    })
}
