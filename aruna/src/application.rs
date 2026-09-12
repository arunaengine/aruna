//! The ordered node lifecycle.
//!
//! `run_node` acquires resources, prepares the realm, binds listeners, starts
//! background work, supervises ingress, and then runs the ordered shutdown.

use std::sync::Arc;

use aruna_api::monitoring::Readiness;
use aruna_core::shutdown::Shutdown;
use aruna_operations::device::wipe as device_wipe;
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_tasks::TaskHandle;

use crate::shutdown::{NodeShutdown, arm_signal_exit, shutdown_grace_env, wait_for_signal};
use crate::startup;
use crate::startup::background::{Background, start as start_background};
use crate::startup::listeners::{ServerBindings, bind as bind_servers, device_wipe_armed};
use crate::startup::listeners::{portal_exit, s3_exit};
use crate::startup::resources::NodeResources;

/// What the node process accomplished, mapped to one exit behavior by the
/// process entry.
#[derive(Debug, PartialEq, Eq)]
pub enum ProcessOutcome {
    /// The node drained and stopped normally.
    Stopped,
    /// The operator asked to stop while startup was still acquiring/binding;
    /// no readiness was announced and no background work started.
    StartupCancelled,
    /// Ingress failed before any signal; the node shut down because of it.
    ServerFailure(String),
    /// The owner's wipe erased everything this node stores.
    WipeComplete,
    /// The owner's wipe could not erase every stored root or backend.
    WipeIncomplete,
}

impl ProcessOutcome {
    /// `None` means a normal zero exit; `Some` is the code to exit with.
    pub fn exit_code(&self) -> Option<i32> {
        match self {
            Self::Stopped | Self::StartupCancelled => None,
            Self::ServerFailure(_) => Some(1),
            Self::WipeComplete => Some(device_wipe::WIPED_EXIT_CODE),
            Self::WipeIncomplete => Some(device_wipe::WIPE_INCOMPLETE_EXIT_CODE),
        }
    }

    /// The message to report before exiting, when there is one.
    pub fn failure(&self) -> Option<&str> {
        match self {
            Self::ServerFailure(message) => Some(message),
            Self::Stopped | Self::StartupCancelled | Self::WipeComplete | Self::WipeIncomplete => {
                None
            }
        }
    }
}

/// A long-lived server whose unexpected exit the node supervises.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Service {
    Rest,
    S3,
    Portal,
    Ops,
}

/// How an unexpected server exit affects the node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceExit {
    /// The node stops serving and begins the shutdown sequence.
    StopsNode,
    /// The server only reports its own exit; the node keeps serving.
    ReportedOnly,
}

/// The named supervision policy per long-lived server. The ops listener is
/// deliberately `ReportedOnly`: it must answer `/readyz` and `/healthz` through
/// the entire drain, its task logs an unexpected exit, and the shutdown
/// sequence aborts it at the very end. Changing a policy here is an explicit
/// behavior change, not a side effect of moving code.
pub fn supervision(service: Service) -> ServiceExit {
    match service {
        Service::Rest | Service::S3 | Service::Portal => ServiceExit::StopsNode,
        Service::Ops => ServiceExit::ReportedOnly,
    }
}

/// A finished signal task means the operator asked to stop. Checking it between
/// startup stages keeps cancellation from waiting behind later stages.
fn startup_stop_requested(signal: &tokio::task::JoinHandle<()>) -> bool {
    signal.is_finished()
}

pub async fn run_node() -> Result<ProcessOutcome, Box<dyn std::error::Error>> {
    // Install signal handling before readiness or background work can start.
    let mut signal = tokio::spawn(wait_for_signal());

    let NodeResources {
        config,
        driver_ctx,
        net_handle,
        shutdown,
        metrics,
        readiness,
        recovery,
        jobs_runtime,
        task_handle,
        task_queues,
        usage_counters_rebuilt,
        ops_handle,
    } = startup::resources::acquire().await?;

    // A stop requested while resources were still being acquired must not run
    // realm preparation against a node that is already being torn down.
    if startup_stop_requested(&signal) {
        return Ok(stop_during_startup(
            shutdown,
            readiness,
            None,
            task_handle,
            jobs_runtime,
            &driver_ctx,
            ops_handle,
        )
        .await);
    }

    let core_announcement = startup::realm::prepare(&config, &driver_ctx, &net_handle).await?;

    if startup_stop_requested(&signal) {
        return Ok(stop_during_startup(
            shutdown,
            readiness,
            None,
            task_handle,
            jobs_runtime,
            &driver_ctx,
            ops_handle,
        )
        .await);
    }

    let bindings = bind_servers(
        config,
        driver_ctx.clone(),
        jobs_runtime.clone(),
        metrics.clone(),
        &shutdown,
    )
    .await?;

    // The operator asked to stop before admissions opened: tear down the
    // bound listeners and acquired resources without announcing readiness.
    if startup_stop_requested(&signal) {
        return Ok(stop_during_startup(
            shutdown,
            readiness,
            Some(bindings),
            task_handle,
            jobs_runtime,
            &driver_ctx,
            ops_handle,
        )
        .await);
    }

    let ServerBindings {
        rest_handle,
        s3_handle,
        mut portal_handle,
        realm_id,
        node_id,
        is_initial_boot,
        device_wipe,
    } = bindings;

    start_background(Background {
        realm_id,
        node_id,
        is_initial_boot,
        driver_ctx: driver_ctx.clone(),
        shutdown: shutdown.clone(),
        readiness: readiness.clone(),
        recovery: recovery.clone(),
        jobs_runtime: jobs_runtime.clone(),
        task_handle: task_handle.clone(),
        task_queues,
        usage_counters_rebuilt,
        core_announcement,
    })
    .await;

    let mut rest_handle = Some(rest_handle);
    let mut s3_handle = s3_handle;

    // A server that returns before shutdown was requested has failed: the node
    // is no longer serving, so it must not exit as success.
    let mut failure: Option<String> = None;
    tokio::select! {
        message = s3_exit(s3_handle.as_mut()) => {
            s3_handle = None;
            failure = Some(message);
        }
        result = rest_handle.as_mut().expect("rest server handle is present") => {
            rest_handle = None;
            failure = Some(match result {
                Ok(Ok(())) => "REST server stopped unexpectedly".to_string(),
                Ok(Err(error)) => format!("REST server failed: {error}"),
                Err(error) => format!("REST server panicked: {error}"),
            });
        }
        message = portal_exit(portal_handle.as_mut()) => {
            portal_handle = None;
            failure = Some(message);
        }
        _ = device_wipe_armed(device_wipe.as_ref()) => {}
        _ = &mut signal => {}
    }

    if let Some(failure) = failure.as_ref() {
        tracing::error!(error = %failure, "Shutting down after a server failure");
    }

    // A second termination signal means "stop now". After a server failure no
    // signal has arrived yet, so wait for the first before arming escalation.
    if failure.is_some() {
        tokio::spawn(async move {
            let _ = signal.await;
            let _ = arm_signal_exit().await;
        });
    } else {
        arm_signal_exit();
    }

    NodeShutdown {
        shutdown,
        readiness,
        rest: rest_handle,
        s3: s3_handle,
        portal: portal_handle,
        task_handle,
        jobs_runtime,
        net_handle: driver_ctx.net_handle.clone(),
        metadata_handle: driver_ctx.metadata_handle.clone(),
        blob_handle: driver_ctx.blob_handle.clone(),
        storage_handle: driver_ctx.storage_handle.clone(),
        ops: Some(ops_handle),
        grace: shutdown_grace_env(),
    }
    .run()
    .await;

    // The stores keep their files open until the shutdown sequence finished, so
    // the owner's wipe erases the roots here.
    if let Some(wipe) = device_wipe.filter(|wipe| wipe.is_armed()) {
        let failed = device_wipe::purge(wipe.roots());
        // Only complete erasure reports wiped status. Remaining paths or unsupported stores
        // use a different exit code so supervisors do not treat the device as erased.
        return Ok(if failed.is_empty() && wipe.unsupported().is_empty() {
            tracing::info!("Wiped this device on its owner's request");
            ProcessOutcome::WipeComplete
        } else {
            tracing::error!(
                paths = failed.len(),
                backends = wipe.unsupported().join(","),
                "The device wipe did not erase everything this node stores"
            );
            ProcessOutcome::WipeIncomplete
        });
    }

    Ok(match failure {
        Some(failure) => ProcessOutcome::ServerFailure(failure),
        None => ProcessOutcome::Stopped,
    })
}

/// Runs the ordered teardown for everything acquired before background work
/// started, including bound listeners when they already exist, and reports the
/// stop as a startup cancellation rather than a ready shutdown.
async fn stop_during_startup(
    shutdown: Shutdown,
    readiness: Readiness,
    bindings: Option<ServerBindings>,
    task_handle: TaskHandle,
    jobs_runtime: Arc<JobsRuntime>,
    driver_ctx: &DriverContext,
    ops_handle: tokio::task::JoinHandle<()>,
) -> ProcessOutcome {
    let (rest, s3, portal) = match bindings {
        Some(ServerBindings {
            rest_handle,
            s3_handle,
            portal_handle,
            ..
        }) => (Some(rest_handle), s3_handle, portal_handle),
        None => (None, None, None),
    };
    NodeShutdown {
        shutdown,
        readiness,
        rest,
        s3,
        portal,
        task_handle,
        jobs_runtime,
        net_handle: driver_ctx.net_handle.clone(),
        metadata_handle: driver_ctx.metadata_handle.clone(),
        blob_handle: driver_ctx.blob_handle.clone(),
        storage_handle: driver_ctx.storage_handle.clone(),
        ops: Some(ops_handle),
        grace: shutdown_grace_env(),
    }
    .run()
    .await;
    ProcessOutcome::StartupCancelled
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    #[test]
    fn supervision_policies_are_named() {
        assert_eq!(supervision(Service::Rest), ServiceExit::StopsNode);
        assert_eq!(supervision(Service::S3), ServiceExit::StopsNode);
        assert_eq!(supervision(Service::Portal), ServiceExit::StopsNode);
        assert_eq!(supervision(Service::Ops), ServiceExit::ReportedOnly);
    }

    #[test]
    fn outcomes_map_to_exit_codes() {
        assert_eq!(ProcessOutcome::Stopped.exit_code(), None);
        assert_eq!(ProcessOutcome::StartupCancelled.exit_code(), None);
        assert_eq!(
            ProcessOutcome::ServerFailure("rest stopped".to_string()).exit_code(),
            Some(1)
        );
        assert_eq!(
            ProcessOutcome::WipeComplete.exit_code(),
            Some(device_wipe::WIPED_EXIT_CODE)
        );
        assert_eq!(
            ProcessOutcome::WipeIncomplete.exit_code(),
            Some(device_wipe::WIPE_INCOMPLETE_EXIT_CODE)
        );
    }

    #[test]
    fn only_server_failure_reports_a_message() {
        assert_eq!(ProcessOutcome::Stopped.failure(), None);
        assert_eq!(ProcessOutcome::StartupCancelled.failure(), None);
        assert_eq!(ProcessOutcome::WipeComplete.failure(), None);
        assert_eq!(ProcessOutcome::WipeIncomplete.failure(), None);
        assert_eq!(
            ProcessOutcome::ServerFailure("s3 stopped".to_string()).failure(),
            Some("s3 stopped")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A signal that already arrived must stop startup; a running one must not.
    // This is the checkpoint predicate without any signal or wall-clock wait.
    #[tokio::test]
    async fn a_finished_signal_task_requests_stop() {
        let running = tokio::spawn(std::future::pending::<()>());
        assert!(!startup_stop_requested(&running));

        let finished = tokio::spawn(async {});
        tokio::task::yield_now().await;
        assert!(startup_stop_requested(&finished));
    }
}
