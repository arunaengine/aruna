//! Routes each effect to the adapter family that performs it, one module per family.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod audit;
mod blob;
mod metadata;
mod net;
pub mod routing;
mod session;
mod storage;
mod task;

use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult, NetEvent, SubOperationEvent};
use aruna_core::outbound::{OutboundEffect, OutboundError, OutboundEvent};
use aruna_core::key_index::{IndexError, IndexEvent};
use tracing::{debug, trace, warn};

use crate::driver::{DriverContext, MAX_SUBOP_DEPTH};

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

pub(crate) async fn dispatch_effect_until(
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
        // Policy fetch resolves its holders in the operation and runs only the
        // holder round-trips here.
        Effect::Net(NetEffect::PolicyFetch(fetch)) => Event::Net(NetEvent::PolicyFetch(
            Box::pin(crate::placement::policy::fetch_policy(context, *fetch)).await,
        )),
        // Publication signing needs this node's key, which only the handle holds.
        Effect::Net(NetEffect::PolicySign(claim)) => Event::Net(NetEvent::PolicySign(
            crate::placement::policy::sign_publication(context, *claim),
        )),
        Effect::Net(NetEffect::VaultFetch(fetch)) => Event::Net(NetEvent::VaultFetch(
            Box::pin(crate::users::vault_route::fetch_vault(context, *fetch)).await,
        )),
        Effect::Net(NetEffect::VaultSign(records)) => Event::Net(NetEvent::VaultSign(
            crate::users::vault_route::sign_records(context, *records),
        )),
        // Release crate reads and holder requests need the runner's handles.
        Effect::Net(NetEffect::Release(effect)) => Event::Net(NetEvent::Release(
            Box::pin(crate::s3::bucket::release::publish::dispatch_release(
                context, *effect,
            ))
            .await,
        )),
        // Job-record replication and launch offers resolve their holders in the
        // operation and run only the holder round-trips here.
        Effect::Net(NetEffect::JobRecord(record)) => Event::Net(NetEvent::JobRecord(
            Box::pin(crate::jobs::records::transport::dispatch_record(
                context, *record,
            ))
            .await,
        )),
        Effect::Net(NetEffect::LaunchOffer(offer)) => Event::Net(NetEvent::LaunchOffer(
            Box::pin(crate::jobs::records::transport::dispatch_offer(
                context, *offer,
            ))
            .await,
        )),
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
        Effect::Net(net_effect) => net::dispatch_net(net_effect, context).await,
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
        // The effect loop runs groups itself; a group never reaches a single dispatch.
        Effect::Group(group) => {
            warn!(depth, "Effect group reached a single dispatch");
            let results = group.members.iter().map(|_| GroupResult::Refused).collect();
            Event::Group(GroupEvent::Finished { results })
        }
        Effect::Session(effect) => {
            Event::Session(Box::new(session::dispatch_session(*effect, context).await))
        }
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

fn effect_kind(effect: &Effect) -> &'static str {
    match effect {
        Effect::Blob(_) => "blob",
        Effect::StagingSource(_) => "staging_source",
        Effect::LocalFile(_) => "local_file",
        Effect::Storage(_) => "storage",
        Effect::Net(_) => "net",
        Effect::Metadata(_) => "metadata",
        Effect::SubOperation(_) => "suboperation",
        Effect::Task(_) => "task",
        Effect::Search() => "search",
        Effect::Stream() => "stream",
        Effect::Index(_) => "index",
        Effect::Group(_) => "group",
        Effect::Session(_) => "session",
        Effect::Outbound(_) => "outbound",
    }
}

/// The answer to an outbound effect on a node without the outbound backend.
fn outbound_unavailable(effect: &OutboundEffect) -> OutboundEvent {
    let error = OutboundError::Retryable("the outbound backend is unavailable".to_string());
    match effect {
        OutboundEffect::Post(_) => OutboundEvent::Posted { result: Err(error) },
        OutboundEffect::Mail(_) => OutboundEvent::MailSent { result: Err(error) },
    }
}
