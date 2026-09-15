//! Effect adapters: one visible dispatch outline, one module per effect family.
//! The runner owns execution and transaction ownership; this module only routes
//! each effect to the adapter that performs it.

mod audit;
mod blob;
mod metadata;
mod net;
pub mod routing;
mod storage;
mod task;

use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent, SubOperationEvent};
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
        // Job-control routing runs its frame I/O here, where the runner holds
        // the context; the net crate never sees this effect.
        Effect::Net(NetEffect::JobControl(job_control)) => {
            Box::pin(net::dispatch_job_control(*job_control, context)).await
        }
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
        // Job-record replication and launch offers resolve their holders in the
        // operation and run only the holder round-trips here.
        Effect::Net(NetEffect::JobRecord(record)) => Event::Net(NetEvent::JobRecord(
            Box::pin(crate::jobs::records::dispatch_record(context, *record)).await,
        )),
        Effect::Net(NetEffect::LaunchOffer(offer)) => Event::Net(NetEvent::LaunchOffer(
            Box::pin(crate::jobs::records::dispatch_offer(context, *offer)).await,
        )),
        Effect::Net(net_effect) => net::dispatch_net(net_effect, context).await,
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
    };

    trace!(
        event = "operation.effect.result",
        depth,
        effect = effect_name,
        result = event_kind(&event),
        "Received operation event"
    );
    if depth == 0 {
        debug!(
            effect = effect_name,
            result = event_kind(&event),
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
    }
}

fn event_kind(event: &Event) -> &'static str {
    match event {
        Event::Blob(_) => "blob",
        Event::StagingSource(_) => "staging_source",
        Event::LocalFile(_) => "local_file",
        Event::Storage(_) => "storage",
        Event::Net(_) => "net",
        Event::Metadata(_) => "metadata",
        Event::SubOperation(_) => "suboperation",
        Event::Task(_) => "task",
        Event::Search() => "search",
        Event::Stream() => "stream",
    }
}
