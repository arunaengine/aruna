//! Routes each effect to the adapter family that performs it, one module per family.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod blob;
mod compute;
mod git;
pub mod job;
mod metadata;
mod net;
pub mod queue_lag;
mod session;
pub(crate) mod shard;
mod storage;
pub mod task;

use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult, NetEvent, SubOperationEvent};
use aruna_core::key_index::{IndexError, IndexEvent};
use aruna_core::member_read::MemberAnswer;
use aruna_core::operation::SubOperation;
use aruna_core::outbound::{
    ConnectionError, OutboundEffect, OutboundError, OutboundEvent, RangeAnswer,
};
use aruna_core::plugins::{MemoryPermit, PluginEffect, PluginEvent};
use tracing::{debug, trace, warn};

use crate::driver::{DriverContext, MAX_SUBOP_DEPTH};
use aruna_core::task::TaskEvent;
use std::sync::{Arc, LazyLock};
use tokio::sync::Semaphore;

/// Children that run beside their caller at once; a caller finding no slot starts none.
const BACKGROUND_CHILDREN: usize = 8;

static BACKGROUND_SLOTS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(BACKGROUND_CHILDREN)));

/// Starts `child` beside its caller while a slot is free; `keep` lives as long as the child.
fn start_background<K: Send + 'static>(
    child: Box<dyn SubOperation>,
    context: &DriverContext,
    keep: K,
) -> bool {
    let Ok(slot) = BACKGROUND_SLOTS.clone().try_acquire_owned() else {
        return false;
    };
    let context = context.clone();
    tokio::spawn(async move {
        let _held = (slot, keep);
        dispatch_effect(Effect::SubOperation(child), &context, 0).await;
    });
    true
}

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
        Effect::Net(NetEffect::Heartbeat(effect)) => {
            Box::pin(net::dispatch_heartbeat(*effect, context)).await
        }
        // Metadata requests travel as plain frames over the net handle.
        Effect::Net(NetEffect::MetadataRequest(request)) => {
            Box::pin(metadata::dispatch_request(*request, context)).await
        }
        // Peer contacts live on the net handle the runner holds.
        Effect::Net(NetEffect::PeerContact(peer)) => net::note_contact(peer, context),
        Effect::Net(NetEffect::Shard(effect)) => {
            Box::pin(shard::dispatch_shard(*effect, context)).await
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
        Effect::Net(NetEffect::PeerContacts { peers }) => {
            let seen = (context.net_handle.as_ref())
                .map(|net| net.peer_contacts().last_seen_of(&peers))
                .unwrap_or_default();
            Event::Net(NetEvent::PeerContacts(seen))
        }
        Effect::Net(NetEffect::LocalNode) => {
            let node = context.net_handle.as_ref().map(|net| net.node_id());
            Event::Net(NetEvent::LocalNode(node))
        }
        Effect::Net(NetEffect::LocalRealm) => {
            let realm = context.net_handle.as_ref().map(|net| *net.realm_id());
            Event::Net(NetEvent::LocalRealm(realm))
        }
        Effect::Net(NetEffect::IssuerKey { key }) => {
            let checked =
                (context.net_handle.as_ref()).is_some_and(|net| net.issuer_keys().check(key));
            Event::Net(NetEvent::IssuerKey(checked))
        }
        Effect::Net(net_effect) => net::dispatch_net(net_effect, context).await,
        Effect::Outbound(OutboundEffect::ListRange(listing)) => {
            net::dispatch_listing(*listing, context).await
        }
        Effect::Outbound(OutboundEffect::MemberFetch(request)) => {
            net::dispatch_fetch(*request, context).await
        }
        Effect::Outbound(effect) => match context.outbound_handle.as_ref() {
            Some(outbound) => Event::Outbound(Box::pin(outbound.run(effect)).await),
            None => Event::Outbound(outbound_unavailable(&effect)),
        },
        Effect::Plugin(effect) => Event::Plugin(plugin_state(effect)),
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
            let started = start_background(child, context, ());
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
        Effect::Git(call) => Event::Git(Box::new(
            Box::pin(git::dispatch_git(*call, context, depth, deadline)).await,
        )),
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
        Effect::Plugin(_) => "plugin",
    }
}

/// Answers a plugin effect from the plugin host's node-wide state.
fn plugin_state(effect: PluginEffect) -> PluginEvent {
    use aruna_plugins::host_state;
    match effect {
        PluginEffect::TakeCall { install_id, now_ms } => {
            PluginEvent::Taken(host_state::take_call(u128::from(install_id), now_ms))
        }
        PluginEffect::TakeHook { install_id, now_ms } => {
            PluginEvent::Taken(host_state::take_hook(u128::from(install_id), now_ms))
        }
        PluginEffect::Reserve { bytes } => PluginEvent::Reserved(
            host_state::reserve(bytes)
                .map(|mib| MemoryPermit::new(mib, move || host_state::release(mib))),
        ),
        PluginEffect::FirstSeen {
            named,
            unnamed,
            now_ms,
        } => {
            let keys = |users: Vec<aruna_core::user::id::UserId>| -> Vec<String> {
                users.iter().map(ToString::to_string).collect()
            };
            PluginEvent::Seen(host_state::first_seen(&keys(named), &keys(unnamed), now_ms))
        }
        PluginEffect::HeldValue {
            run_id,
            holder,
            slot,
        } => {
            let held = host_state::held_value((run_id.into(), holder.into()), &slot);
            PluginEvent::Held(held.map(ulid::Ulid::from))
        }
        PluginEffect::HoldValue {
            run_id,
            holder,
            slot,
            value_id,
            fields,
        } => {
            let fields = fields.map(|fields| fields.expose().to_vec());
            let holder = (run_id.into(), holder.into());
            let held = host_state::hold_value(holder, &slot, value_id.into(), fields);
            PluginEvent::Fields(held.map(aruna_core::compute::SecretBytes::new))
        }
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
        // Both checks need no backend, so they answer as on a node with one.
        OutboundEffect::CheckSmtp(settings) => OutboundEvent::SmtpChecked {
            result: aruna_outbound::mail::check_smtp(settings),
        },
        OutboundEffect::CheckAddress(address) => OutboundEvent::AddressChecked {
            valid: aruna_outbound::mail::valid_address(address),
        },
        OutboundEffect::ListRange(_) => OutboundEvent::RangeListed(Box::new(RangeAnswer::Silent)),
        OutboundEffect::MemberFetch(_) => {
            OutboundEvent::MemberAnswered(Box::new(MemberAnswer::Silent))
        }
        OutboundEffect::Connection(_) => OutboundEvent::ConnectionAnswered {
            result: Err(ConnectionError::NotSent(
                "this node sends no connection requests".to_string(),
            )),
        },
    }
}
