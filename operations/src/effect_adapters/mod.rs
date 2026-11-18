//! Routes each effect to the adapter family that performs it, one module per family.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod audit;
mod blob;
mod compute;
mod git;
pub(crate) mod job;
mod metadata;
mod net;
mod session;
pub(crate) mod shard;
mod storage;
mod task;

use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult, NetEvent, SubOperationEvent};
use aruna_core::key_index::{IndexError, IndexEvent};
use aruna_core::outbound::{
    ConnectionError, OutboundEffect, OutboundError, OutboundEvent, RangeAnswer,
};
use tracing::{debug, trace, warn};

use crate::driver::{DriverContext, MAX_SUBOP_DEPTH};
use aruna_core::task::TaskEvent;
use std::sync::{Arc, LazyLock};
use tokio::sync::Semaphore;

/// Children that run beside their caller at once; a caller finding no slot starts none.
const BACKGROUND_CHILDREN: usize = 8;

static BACKGROUND_SLOTS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(BACKGROUND_CHILDREN)));

#[tracing::instrument(
    name = "operation.effect",
    level = "debug",
    skip(effect, context),
    fields(depth, effect = effect_kind(&effect))
)]
pub(crate) async fn dispatch_effect(
    effect: Effect,
    context: &DriverContext,
    depth: usize,
) -> Event {
    dispatch_effect_until(effect, context, depth, None).await
}

/// Dispatches one effect; the boxed future keeps the recursion through sub operations sized.
pub(crate) fn dispatch_effect_until(
    effect: Effect,
    context: &DriverContext,
    depth: usize,
    deadline: Option<tokio::time::Instant>,
) -> futures_util::future::BoxFuture<'_, Event> {
    Box::pin(dispatch_unboxed(effect, context, depth, deadline))
}

async fn dispatch_unboxed(
    effect: Effect,
    context: &DriverContext,
    depth: usize,
    deadline: Option<tokio::time::Instant>,
) -> Event {
    let effect_name = effect_kind(&effect);
    if depth == 0 {
        debug!(
            effect = effect_name,
            "Dispatching top-level operation effect"
        );
    }
    trace!(
        event = "operation.effect.dispatch",
        depth,
        effect = effect_name,
        "Dispatching operation effect"
    );

    let event = match effect {
        Effect::Blob(blob_effect) => blob::dispatch_blob(blob_effect, context).await,
        Effect::StagingSource(staging_source_effect) => {
            blob::dispatch_staging_source(staging_source_effect, context).await
        }
        Effect::LocalFile(local_file_effect) => {
            blob::dispatch_local_file(local_file_effect, context).await
        }
        Effect::Storage(storage_effect) => storage::dispatch_storage(storage_effect, context).await,
        // Audit fan-out runs its frame I/O here for the same reason.
        Effect::Net(NetEffect::AuditPage(audit)) => {
            Box::pin(audit::dispatch_audit_page(*audit, context, deadline)).await
        }
        Effect::Net(NetEffect::Heartbeat(effect)) => {
            Box::pin(net::dispatch_heartbeat(*effect, context)).await
        }
        // A plugin artifact chunk travels over the metadata transport, which the runner holds.
        Effect::Net(NetEffect::ArtifactStore(store)) => {
            let stored = match context.metadata_handle.as_ref() {
                Some(handle) => (handle
                    .request_store_artifact(store.node_id, store.chunk)
                    .await)
                    .map_err(|error| format!("{error:?}")),
                None => Err("no metadata transport on this node".to_string()),
            };
            Event::Net(NetEvent::ArtifactStored(stored))
        }
        // The metadata transport lives in the metadata handle the runner holds.
        Effect::Net(NetEffect::MetadataRequest(request)) => {
            Box::pin(metadata::dispatch_request(*request, context)).await
        }
        // Token validation state lives in the metadata handle the runner holds.
        Effect::Net(NetEffect::WritePeerAuth(request)) => {
            Box::pin(metadata::dispatch_peer_auth(*request, context)).await
        }
        // The routed export reads through the handles the runner holds.
        Effect::Net(NetEffect::DatasetSummary(request)) => {
            metadata::dispatch_summary(*request, context).await
        }
        Effect::Net(NetEffect::Shard(effect)) => {
            Box::pin(shard::dispatch_shard(*effect, context)).await
        }
        Effect::Net(NetEffect::RevocationClaims(token)) => {
            Box::pin(metadata::dispatch_revocation_claims(*token, context)).await
        }
        // Notification frames and inbox wakes need the net handle the runner holds.
        Effect::Net(NetEffect::Notification(request)) => {
            Event::Net(NetEvent::Notification(match context.net_handle.as_ref() {
                Some(net_handle) => {
                    let answer = net_handle.notification_request(request.node, &request.message);
                    Box::pin(answer).await.map(Box::new)
                }
                None => Err("the notification transport is unavailable".to_string()),
            }))
        }
        Effect::Net(NetEffect::WakeInbox(recipients)) => {
            if let Some(net_handle) = context.net_handle.as_ref() {
                for recipient in recipients {
                    net_handle.notify_inbox_activity(recipient);
                }
            }
            Event::Net(NetEvent::InboxWoken)
        }
        Effect::Net(NetEffect::DashboardChanged) => {
            context.notify_dashboard_change();
            Event::Net(NetEvent::Recorded)
        }
        Effect::Net(NetEffect::LocalNode) => {
            let node = context.net_handle.as_ref().map(|net| net.node_id());
            Event::Net(NetEvent::LocalNode(node))
        }
        Effect::Net(net_effect) => net::dispatch_net(net_effect, context).await,
        Effect::Outbound(OutboundEffect::ListRange(listing))
            if context.outbound_handle.is_some() =>
        {
            net::dispatch_listing(*listing, context).await
        }
        Effect::Outbound(effect) => match context.outbound_handle.as_ref() {
            Some(outbound) => Event::Outbound(Box::pin(outbound.run(effect)).await),
            None => Event::Outbound(outbound_unavailable(&effect)),
        },
        Effect::Metadata(metadata_effect) => {
            metadata::dispatch_metadata(metadata_effect, context).await
        }
        Effect::SubOperation(sub_operation) => {
            if depth >= MAX_SUBOP_DEPTH {
                Event::SubOperation(SubOperationEvent::DepthLimitExceeded {
                    max_depth: MAX_SUBOP_DEPTH,
                })
            } else {
                // Keep the child owned by this future so cancellation cannot detach it.
                crate::driver::drive_suboperation(sub_operation, context, depth + 1, deadline).await
            }
        }
        Effect::Background(child) => {
            let slot = BACKGROUND_SLOTS.clone().try_acquire_owned().ok();
            let started = slot.is_some();
            if let Some(slot) = slot {
                let context = context.clone();
                tokio::spawn(async move {
                    let _slot = slot;
                    dispatch_effect(Effect::SubOperation(child), &context, 0).await;
                });
            }
            Event::Task(TaskEvent::BackgroundStarted { started })
        }
        Effect::Task(task_effect) => task::dispatch_task(task_effect, context).await,
        Effect::Index(index_effect) => Event::Index(match context.storage_handle.range_indexes() {
            Some(indexes) => indexes.execute(*index_effect).await,
            None => IndexEvent::Error(IndexError::Unavailable),
        }),
        Effect::Search() => {
            warn!(
                depth,
                effect = effect_name,
                "Search effect is not handled by driver yet"
            );
            Event::Search()
        }
        Effect::Stream() => {
            warn!(
                depth,
                effect = effect_name,
                "Top-level stream effect is not handled by driver yet"
            );
            Event::Stream()
        }
        Effect::Delay(after) => {
            tokio::time::sleep(after).await;
            Event::Delayed
        }
        // The effect loop runs groups itself; a group never reaches a single dispatch.
        Effect::Group(group) => {
            warn!(depth, "Effect group reached a single dispatch");
            let results = group.members.iter().map(|_| GroupResult::Refused).collect();
            Event::Group(GroupEvent::Finished { results })
        }
        Effect::Session(effect) => {
            Event::Session(Box::new(session::dispatch_session(*effect, context).await))
        }
        Effect::Compute(effect) => {
            Event::Compute(Box::new(compute::dispatch_compute(*effect, context).await))
        }
        Effect::Git(call) => Event::Git(Box::new(git::dispatch_git(*call, context).await)),
        Effect::Job(effect) => Box::pin(job::dispatch_job(effect, context, depth, deadline)).await,
    };

    trace!(
        event = "operation.effect.result",
        depth,
        effect = effect_name,
        result = event.label(),
        "Received operation event"
    );
    if depth == 0 {
        debug!(
            effect = effect_name,
            result = event.label(),
            "Received top-level operation event"
        );
    }

    event
}

pub(crate) fn effect_kind(effect: &Effect) -> &'static str {
    match effect {
        Effect::Blob(_) => "blob",
        Effect::StagingSource(_) => "staging_source",
        Effect::LocalFile(_) => "local_file",
        Effect::Storage(_) => "storage",
        Effect::Net(_) => "net",
        Effect::Metadata(_) => "metadata",
        Effect::SubOperation(_) => "suboperation",
        Effect::Background(_) => "background",
        Effect::Task(_) => "task",
        Effect::Search() => "search",
        Effect::Stream() => "stream",
        Effect::Index(_) => "index",
        Effect::Group(_) => "group",
        Effect::Session(_) => "session",
        Effect::Compute(_) => "compute",
        Effect::Outbound(_) => "outbound",
        Effect::Delay(_) => "delay",
        Effect::Job(_) => "job",
        Effect::Git(_) => "git",
    }
}

/// The answer to an outbound effect on a node without the outbound backend.
fn outbound_unavailable(effect: &OutboundEffect) -> OutboundEvent {
    let error = OutboundError::Retryable("the outbound backend is unavailable".to_string());
    match effect {
        OutboundEffect::Post(_) => OutboundEvent::Posted { result: Err(error) },
        OutboundEffect::Mail(_) => OutboundEvent::MailSent { result: Err(error) },
        OutboundEffect::Backoff(_) => OutboundEvent::Backoff { stored: None },
        OutboundEffect::Request(_) => OutboundEvent::Answered { result: Err(error) },
        OutboundEffect::RefreshMail(_) => OutboundEvent::MailRefreshed,
        OutboundEffect::ListRange(_) => OutboundEvent::RangeListed(Box::new(RangeAnswer::Silent)),
        OutboundEffect::Connection(_) => OutboundEvent::ConnectionAnswered {
            result: Err(ConnectionError::NotSent(
                "this node sends no connection requests".to_string(),
            )),
        },
    }
}
