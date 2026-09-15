//! The ordered node lifecycle: acquire resources, prepare the realm, bind
//! listeners, start background work, supervise ingress, then run the ordered
//! shutdown (`run_node`).

use aruna_operations::device::wipe as device_wipe;

use crate::shutdown::{
    NodeShutdown, ShutdownOutcome, arm_signal_exit, shutdown_grace_env, wait_for_signal,
};
use crate::startup;
use crate::startup::background::{Background, BackgroundOutcome, start as start_background};
use crate::startup::listeners::{ServerBindings, bind as bind_servers, device_wipe_armed};
use crate::startup::listeners::{portal_exit, s3_exit, session_s3_exit};
use crate::startup::resources::NodeResources;

/// What the node process accomplished, mapped to one exit behavior by the
/// process entry.
#[derive(Debug, PartialEq, Eq)]
pub enum ProcessOutcome {
    /// The node drained and stopped normally.
    Stopped,
    /// The node stopped, but the shutdown sequence left an owner or a
    /// persistence step unresolved; the stores were not verified clean. Also
    /// the outcome of an accepted startup cancellation whose cleanup was
    /// incomplete.
    StoppedIncomplete,
    /// The operator asked to stop before startup completed; no later phase
    /// was admitted and the ordered cleanup released every acquired owner. A
    /// stop whose cleanup left an owner unresolved maps to
    /// [`ProcessOutcome::StoppedIncomplete`] instead.
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
            Self::ServerFailure(_) | Self::StoppedIncomplete => Some(1),
            Self::WipeComplete => Some(device_wipe::WIPED_EXIT_CODE),
            Self::WipeIncomplete => Some(device_wipe::INCOMPLETE_EXIT_CODE),
        }
    }

    /// The message to report before exiting, when there is one.
    pub fn failure(&self) -> Option<&str> {
        match self {
            Self::ServerFailure(message) => Some(message),
            Self::Stopped
            | Self::StoppedIncomplete
            | Self::StartupCancelled
            | Self::WipeComplete
            | Self::WipeIncomplete => None,
        }
    }
}

/// A long-lived server whose unexpected exit the node supervises.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Service {
    Rest,
    S3,
    Portal,
    /// The optional session bridge listener; only sessions lose their endpoint.
    SessionS3,
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

/// The named supervision policy per long-lived server. `ReportedOnly` (ops,
/// session bridge) logs an unexpected exit and keeps serving; the ops listener
/// must answer `/readyz` through the drain. Changing a policy is behavior.
pub fn supervision(service: Service) -> ServiceExit {
    match service {
        Service::Rest | Service::S3 | Service::Portal => ServiceExit::StopsNode,
        Service::SessionS3 | Service::Ops => ServiceExit::ReportedOnly,
    }
}

/// Applies the policy to one observed exit: `Some` stops the node with that
/// message, `None` reports it and keeps serving.
fn exit_effect(service: Service, message: &str) -> Option<&str> {
    match supervision(service) {
        ServiceExit::StopsNode => Some(message),
        ServiceExit::ReportedOnly => None,
    }
}

/// Owns the process signal tasks for one `run_node`: the first-signal waiter
/// that bridges into the startup stop token, and the second-signal escalation
/// armed for every drain. `finish` runs on every exit path.
struct SignalTasks {
    stop: tokio_util::sync::CancellationToken,
    first: Option<tokio::task::JoinHandle<()>>,
    escalation: Option<tokio::task::JoinHandle<()>>,
    escalation_action: Option<EscalationAction>,
}

/// The second-signal action the escalation task owns; tests substitute a
/// controllable future for the OS signal wait and the process exit.
type EscalationAction = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>;

impl SignalTasks {
    /// Installs the OS signal handlers before readiness or background work. One
    /// token carries "stop accepted" to every startup boundary, so a stop during
    /// preparation does not wait behind the next fallible stage.
    fn install() -> Self {
        Self::with_first(wait_for_signal())
    }

    /// The same lifecycle with a controllable first signal, so tests need no
    /// OS signal or runtime timing.
    fn with_first(first_signal: impl std::future::Future<Output = ()> + Send + 'static) -> Self {
        Self::with_signals(first_signal, arm_signal_exit())
    }

    /// The same lifecycle with a controllable escalation action, so a test can
    /// drive the active second-signal path without exiting its runner.
    fn with_signals(
        first_signal: impl std::future::Future<Output = ()> + Send + 'static,
        escalation_action: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Self {
        let stop = tokio_util::sync::CancellationToken::new();
        let bridge = stop.clone();
        let first = tokio::spawn(async move {
            first_signal.await;
            bridge.cancel();
        });
        Self {
            stop,
            first: Some(first),
            escalation: None,
            escalation_action: Some(Box::pin(escalation_action)),
        }
    }

    fn stop(&self) -> tokio_util::sync::CancellationToken {
        self.stop.clone()
    }

    /// Applies the second-signal policy consistently across every drain; the
    /// spawned task owns the escalation action directly, so aborting it
    /// leaves no nested task behind.
    fn arm_escalation(&mut self) {
        if self.escalation.is_some() {
            return;
        }
        let Some(action) = self.escalation_action.take() else {
            return;
        };
        let stop = self.stop.clone();
        self.escalation = Some(tokio::spawn(async move {
            stop.cancelled().await;
            action.await;
        }));
    }

    /// Finishes both signal tasks on successful shutdown, acquisition failure,
    /// preparation failure, and accepted startup cancellation.
    async fn finish(&mut self) {
        if let Some(escalation) = self.escalation.take() {
            escalation.abort();
            let _ = escalation.await;
        }
        self.escalation_action = None;
        if let Some(first) = self.first.take() {
            first.abort();
            let _ = first.await;
        }
    }
}

pub async fn run_node() -> Result<ProcessOutcome, Box<dyn std::error::Error>> {
    let mut signals = SignalTasks::install();
    let outcome = run_node_owned(&mut signals).await;
    signals.finish().await;
    outcome
}

async fn run_node_owned(
    signals: &mut SignalTasks,
) -> Result<ProcessOutcome, Box<dyn std::error::Error>> {
    let stop_token = signals.stop();

    // One acquired owner stays whole through realm preparation, listener
    // binding, and background startup. Every failure and accepted cancellation
    // releases exactly this owner; only a completed handoff takes it apart.
    let resources = match startup::resources::acquire(&stop_token).await {
        Ok(Some(resources)) => resources,
        Ok(None) => return Ok(ProcessOutcome::StartupCancelled),
        Err(error) => return Err(error),
    };

    // A stop requested while resources were still being acquired must not run
    // realm preparation against a node that is already being torn down.
    if stop_token.is_cancelled() {
        let cleanup = release_unready(signals, resources, None).await;
        return Ok(cancellation_outcome(&cleanup));
    }

    let core_announcement = match startup::realm::prepare(
        &resources.config,
        &resources.driver_ctx,
        &resources.net_handle,
        &stop_token,
    )
    .await
    {
        Ok(Some(core_announcement)) => core_announcement,
        Ok(None) => {
            // The stop was accepted between preparation phases; release the
            // acquired subset without continuing startup.
            let cleanup = release_unready(signals, resources, None).await;
            return Ok(cancellation_outcome(&cleanup));
        }
        Err(error) => {
            // Release the acquired subset, then report the initiating failure.
            release_unready(signals, resources, None).await;
            return Err(error);
        }
    };

    if stop_token.is_cancelled() {
        let cleanup = release_unready(signals, resources, None).await;
        return Ok(cancellation_outcome(&cleanup));
    }

    let bindings = match bind_servers(
        &resources.config,
        resources.session_s3,
        resources.driver_ctx.clone(),
        resources.jobs_runtime.clone(),
        resources.metrics.clone(),
        &resources.shutdown,
    )
    .await
    {
        Ok(bindings) => bindings,
        Err(error) => {
            // `bind` already aborted any listener it started; release the
            // acquired resources and report the initiating failure.
            release_unready(signals, resources, None).await;
            return Err(error);
        }
    };

    // The operator asked to stop before admissions opened: tear down the
    // bound listeners and acquired resources without announcing readiness.
    if stop_token.is_cancelled() {
        let cleanup = release_unready(signals, resources, Some(bindings)).await;
        return Ok(cancellation_outcome(&cleanup));
    }

    // Background startup applies the same required-versus-optional service
    // policy as steady-state supervision, so an optional listener exit never
    // abandons required recovery phases; a required failure is retained.
    let mut failure: Option<String> = None;
    match start_background(
        Background {
            realm_id: bindings.realm_id,
            node_id: bindings.node_id,
            is_initial_boot: bindings.is_initial_boot,
            driver_ctx: resources.driver_ctx.clone(),
            shutdown: resources.shutdown.clone(),
            readiness: resources.readiness.clone(),
            recovery: resources.recovery.clone(),
            jobs_runtime: resources.jobs_runtime.clone(),
            task_handle: resources.task_handle.clone(),
            task_queues: resources.task_queues.clone(),
            usage_counters_rebuilt: resources.usage_counters_rebuilt,
            core_announcement,
        },
        &stop_token,
        || observed_listener_exit(&bindings),
        |_| {},
    )
    .await
    {
        BackgroundOutcome::Started => {}
        BackgroundOutcome::Cancelled => {
            let cleanup = release_unready(signals, resources, Some(bindings)).await;
            return Ok(cancellation_outcome(&cleanup));
        }
        BackgroundOutcome::RequiredListenerFailed(service) => {
            failure = Some(format!(
                "Required listener {service:?} exited during background startup"
            ));
        }
    }

    let ServerBindings {
        rest_handle,
        s3_handle,
        mut portal_handle,
        mut session_s3_handle,
        realm_id: _,
        node_id: _,
        is_initial_boot: _,
        device_wipe,
    } = bindings;

    let mut rest_handle = Some(rest_handle);
    let mut s3_handle = s3_handle;

    // Every long-lived server exit is classified by the one named policy: a
    // `StopsNode` exit ends serving with its message, a `ReportedOnly` exit is
    // logged and the node keeps waiting on the remaining servers.
    loop {
        let observed = tokio::select! {
            message = s3_exit(s3_handle.as_mut()) => {
                Some((Service::S3, message))
            }
            result = rest_handle.as_mut().expect("rest server handle is present") => {
                rest_handle = None;
                Some((Service::Rest, match result {
                    Ok(Ok(())) => "REST server stopped unexpectedly".to_string(),
                    Ok(Err(error)) => format!("REST server failed: {error}"),
                    Err(error) => format!("REST server panicked: {error}"),
                }))
            }
            message = portal_exit(portal_handle.as_mut()) => {
                portal_handle = None;
                Some((Service::Portal, message))
            }
            message = session_s3_exit(session_s3_handle.as_mut()) => {
                Some((Service::SessionS3, message))
            }
            _ = device_wipe_armed(device_wipe.as_ref()) => None,
            _ = stop_token.cancelled() => None,
        };
        let Some((service, message)) = observed else {
            break;
        };
        match exit_effect(service, &message) {
            Some(_) => {
                failure = Some(message);
                break;
            }
            None => {
                tracing::warn!(
                    service = ?service,
                    message = %message,
                    "Server exited; the node keeps serving"
                );
            }
        }
    }

    if let Some(failure) = failure.as_ref() {
        tracing::error!(error = %failure, "Shutting down after a server failure");
    }

    // A second termination signal means "stop now". After a server failure no
    // signal has arrived yet, so the owner waits for the first before arming
    // escalation; after a first signal it arms immediately.
    signals.arm_escalation();

    let shutdown_outcome = NodeShutdown {
        shutdown: resources.shutdown,
        readiness: resources.readiness,
        rest: rest_handle,
        s3: s3_handle,
        portal: portal_handle,
        session_s3: session_s3_handle,
        monitoring: Some(resources.monitoring),
        task_handle: resources.task_handle,
        jobs_runtime: resources.jobs_runtime,
        net_handle: resources.driver_ctx.net_handle.clone(),
        metadata_handle: resources.driver_ctx.metadata_handle.clone(),
        blob_handle: resources.driver_ctx.blob_handle.clone(),
        storage_handle: resources.driver_ctx.storage_handle.clone(),
        ops: Some(resources.ops_handle),
        grace: shutdown_grace_env(),
    }
    .run()
    .await;
    let shutdown_complete = shutdown_outcome.complete();

    // The stores keep their files open until the shutdown sequence finished, so
    // the owner's wipe erases the roots here, and only a completed sequence may
    // reach the destructive purge.
    if let Some(wipe) = device_wipe.filter(|wipe| wipe.is_armed()) {
        return Ok(decide_wipe(&wipe, shutdown_complete, device_wipe::purge));
    }

    Ok(match (failure, shutdown_complete) {
        (Some(failure), _) => ProcessOutcome::ServerFailure(failure),
        (None, true) => ProcessOutcome::Stopped,
        (None, false) => ProcessOutcome::StoppedIncomplete,
    })
}

/// Decides the armed wipe: an unfinished shutdown declines the destructive
/// purge and leaves the armed intent in place, so no unfinished owner is erased
/// behind. A completed sequence purges exactly once and classifies the result.
fn decide_wipe(
    wipe: &device_wipe::DeviceWipe,
    shutdown_complete: bool,
    purge: impl FnOnce(&[std::path::PathBuf]) -> Vec<std::path::PathBuf>,
) -> ProcessOutcome {
    if !shutdown_complete {
        tracing::error!(
            backends = wipe.unsupported().join(","),
            "The device wipe was declined because the shutdown sequence did not complete"
        );
        return ProcessOutcome::WipeIncomplete;
    }
    // Only complete erasure reports wiped status. Remaining paths or unsupported stores
    // use a different exit code so supervisors do not treat the device as erased.
    let failed = purge(wipe.roots());
    if wipe_succeeded(shutdown_complete, failed.len(), wipe.unsupported().len()) {
        tracing::info!("Wiped this device on its owner's request");
        ProcessOutcome::WipeComplete
    } else {
        tracing::error!(
            paths = failed.len(),
            backends = wipe.unsupported().join(","),
            shutdown_complete,
            "The device wipe did not erase everything this node stores"
        );
        ProcessOutcome::WipeIncomplete
    }
}

/// Only a sequence that released every owner and reported its persistence
/// steps can claim the purge erased the device; unfinished owners may still
/// write behind the removed roots.
fn wipe_succeeded(shutdown_complete: bool, failed_paths: usize, unsupported: usize) -> bool {
    shutdown_complete && failed_paths == 0 && unsupported == 0
}

/// Observes a finished ingress listener between startup phases and classifies
/// it under the named policy. Required listeners are checked before the
/// optional session bridge, so a dead required server is never masked.
fn observed_listener_exit(bindings: &ServerBindings) -> Option<(Service, ServiceExit)> {
    if bindings.rest_handle.is_finished() {
        Some((Service::Rest, supervision(Service::Rest)))
    } else if bindings
        .s3_handle
        .as_ref()
        .is_some_and(aruna_api::s3::server::S3ServerHandle::is_finished)
    {
        Some((Service::S3, supervision(Service::S3)))
    } else if bindings
        .portal_handle
        .as_ref()
        .is_some_and(|handle| handle.is_finished())
    {
        Some((Service::Portal, supervision(Service::Portal)))
    } else if bindings
        .session_s3_handle
        .as_ref()
        .is_some_and(aruna_api::s3::server::S3ServerHandle::is_finished)
    {
        Some((Service::SessionS3, supervision(Service::SessionS3)))
    } else {
        None
    }
}

/// The one mapping from accepted-stop cleanup to process outcome: a sequence
/// that released every owner is a clean cancellation; an incomplete one leaves
/// an owner or persistence step unresolved and must exit nonzero.
fn cancellation_outcome(cleanup: &ShutdownOutcome) -> ProcessOutcome {
    if cleanup.complete() {
        ProcessOutcome::StartupCancelled
    } else {
        ProcessOutcome::StoppedIncomplete
    }
}

/// Runs the ordered teardown for everything acquired before background work,
/// including bound listeners. The caller decides whether the stop is a
/// cancellation or a failure; every early drain keeps the second-signal policy.
/// Returns what the sequence accomplished so a cancellation site can report a
/// complete release and an incomplete one distinctly.
async fn release_unready(
    signals: &mut SignalTasks,
    resources: NodeResources,
    bindings: Option<ServerBindings>,
) -> ShutdownOutcome {
    signals.arm_escalation();
    let NodeResources {
        config: _,
        driver_ctx,
        net_handle: _,
        shutdown,
        metrics: _,
        readiness,
        recovery: _,
        jobs_runtime,
        task_handle,
        task_queues: _,
        usage_counters_rebuilt: _,
        session_s3: _,
        monitoring,
        ops_handle,
    } = resources;
    let (rest, s3, portal, session_s3) = match bindings {
        Some(ServerBindings {
            rest_handle,
            s3_handle,
            portal_handle,
            session_s3_handle,
            ..
        }) => (
            Some(rest_handle),
            s3_handle,
            portal_handle,
            session_s3_handle,
        ),
        None => (None, None, None, None),
    };
    NodeShutdown {
        shutdown,
        readiness,
        rest,
        s3,
        portal,
        session_s3,
        monitoring: Some(monitoring),
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
    .await
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    #[test]
    fn supervision_policies() {
        assert_eq!(supervision(Service::Rest), ServiceExit::StopsNode);
        assert_eq!(supervision(Service::S3), ServiceExit::StopsNode);
        assert_eq!(supervision(Service::Portal), ServiceExit::StopsNode);
        assert_eq!(supervision(Service::SessionS3), ServiceExit::ReportedOnly);
        assert_eq!(supervision(Service::Ops), ServiceExit::ReportedOnly);
    }

    // The real exit path consumes the policy: `StopsNode` decisions carry the
    // observed message, `ReportedOnly` decisions keep the node serving.
    #[test]
    fn exit_follows_policy() {
        for service in [Service::Rest, Service::S3, Service::Portal] {
            assert_eq!(
                exit_effect(service, "observed exit"),
                Some("observed exit"),
                "{service:?} must stop the node"
            );
        }
        for service in [Service::SessionS3, Service::Ops] {
            assert_eq!(
                exit_effect(service, "observed exit"),
                None,
                "{service:?} must only be reported"
            );
        }
    }

    #[test]
    fn outcome_exit_codes() {
        assert_eq!(ProcessOutcome::Stopped.exit_code(), None);
        assert_eq!(ProcessOutcome::StartupCancelled.exit_code(), None);
        assert_eq!(
            ProcessOutcome::ServerFailure("rest stopped".to_string()).exit_code(),
            Some(1)
        );
        assert_eq!(ProcessOutcome::StoppedIncomplete.exit_code(), Some(1));
        assert_eq!(
            ProcessOutcome::WipeComplete.exit_code(),
            Some(device_wipe::WIPED_EXIT_CODE)
        );
        assert_eq!(
            ProcessOutcome::WipeIncomplete.exit_code(),
            Some(device_wipe::INCOMPLETE_EXIT_CODE)
        );
    }

    #[test]
    fn server_failure_message() {
        assert_eq!(ProcessOutcome::Stopped.failure(), None);
        assert_eq!(ProcessOutcome::StartupCancelled.failure(), None);
        assert_eq!(ProcessOutcome::WipeComplete.failure(), None);
        assert_eq!(ProcessOutcome::WipeIncomplete.failure(), None);
        assert_eq!(
            ProcessOutcome::ServerFailure("s3 stopped".to_string()).failure(),
            Some("s3 stopped")
        );
    }

    // A wipe may only claim success when the sequence released every owner and
    // the purge left nothing behind; an unfinished owner can still write.
    #[test]
    fn complete_wipe_required() {
        assert!(wipe_succeeded(true, 0, 0));
        assert!(!wipe_succeeded(false, 0, 0));
        assert!(!wipe_succeeded(true, 1, 0));
        assert!(!wipe_succeeded(true, 0, 1));
    }

    /// A purge boundary that records every invocation and answers with a fixed
    /// failure list, so the decision is exercised without touching a filesystem.
    #[derive(Default)]
    struct RecordingPurge {
        calls: std::cell::RefCell<Vec<Vec<std::path::PathBuf>>>,
        failures: Vec<std::path::PathBuf>,
    }

    impl RecordingPurge {
        fn purge(&self, roots: &[std::path::PathBuf]) -> Vec<std::path::PathBuf> {
            self.calls.borrow_mut().push(roots.to_vec());
            self.failures.clone()
        }
    }

    // An unfinished shutdown must not reach the destructive call: the recorder
    // stays empty, the roots are untouched, and the armed intent survives.
    #[test]
    fn wipe_declined_incomplete() {
        let roots = vec![
            std::path::PathBuf::from("/tmp/aruna-wipe-root-a"),
            std::path::PathBuf::from("/tmp/aruna-wipe-root-b"),
        ];
        let wipe = device_wipe::DeviceWipe::new(roots, Vec::new());
        wipe.arm();

        let purge = RecordingPurge::default();
        let outcome = decide_wipe(&wipe, false, |passed| purge.purge(passed));

        assert_eq!(outcome, ProcessOutcome::WipeIncomplete);
        assert!(
            purge.calls.borrow().is_empty(),
            "an unfinished shutdown must not purge the wipe roots"
        );
        assert!(wipe.is_armed(), "a declined wipe keeps its armed intent");
    }

    // After a completed sequence the purge runs exactly once with the wipe's
    // roots, and the result distinguishes complete erasure from path failures
    // and unsupported stores.
    #[test]
    fn wipe_classified_complete() {
        let roots = vec![
            std::path::PathBuf::from("/tmp/aruna-wipe-root-a"),
            std::path::PathBuf::from("/tmp/aruna-wipe-root-b"),
        ];
        let wipe = device_wipe::DeviceWipe::new(roots.clone(), Vec::new());

        let purge = RecordingPurge::default();
        let outcome = decide_wipe(&wipe, true, |passed| purge.purge(passed));
        assert_eq!(outcome, ProcessOutcome::WipeComplete);
        assert_eq!(
            purge.calls.borrow().as_slice(),
            std::slice::from_ref(&roots),
            "the purge must see the wipe's roots exactly once"
        );

        let purge = RecordingPurge {
            failures: vec![roots[0].clone()],
            ..RecordingPurge::default()
        };
        let outcome = decide_wipe(&wipe, true, |passed| purge.purge(passed));
        assert_eq!(outcome, ProcessOutcome::WipeIncomplete);
        assert_eq!(purge.calls.borrow().len(), 1);

        let unsupported = device_wipe::DeviceWipe::new(roots, vec!["object-store".to_string()]);
        let purge = RecordingPurge::default();
        let outcome = decide_wipe(&unsupported, true, |passed| purge.purge(passed));
        assert_eq!(outcome, ProcessOutcome::WipeIncomplete);
        assert_eq!(purge.calls.borrow().len(), 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::oneshot;

    /// A controllable first signal: it resolves when the test fires it and
    /// records its own abort, so no OS signal or wall-clock wait is involved.
    struct ControlledSignal {
        fired: oneshot::Receiver<()>,
        aborted: Arc<AtomicBool>,
    }

    impl ControlledSignal {
        fn new(aborted: Arc<AtomicBool>) -> (Self, oneshot::Sender<()>) {
            let (fire_tx, fired) = oneshot::channel();
            (Self { fired, aborted }, fire_tx)
        }
    }

    impl std::future::Future for ControlledSignal {
        type Output = ();

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            match std::future::Future::poll(std::pin::Pin::new(&mut self.fired), cx) {
                std::task::Poll::Ready(_) => std::task::Poll::Ready(()),
                std::task::Poll::Pending => std::task::Poll::Pending,
            }
        }
    }

    impl Drop for ControlledSignal {
        fn drop(&mut self) {
            self.aborted.store(true, Ordering::SeqCst);
        }
    }

    /// A controllable escalation action replacing the OS second-signal wait
    /// and process exit; it reports arming and completion and records a real
    /// drop, so no test needs a signal or can exit its runner.
    struct ControlledEscalation {
        fire: Option<oneshot::Sender<()>>,
        armed: Option<oneshot::Receiver<()>>,
        completed: Option<oneshot::Receiver<()>>,
        dropped: Arc<AtomicBool>,
    }

    impl ControlledEscalation {
        fn new() -> Self {
            Self {
                fire: None,
                armed: None,
                completed: None,
                dropped: Arc::new(AtomicBool::new(false)),
            }
        }

        fn action(&mut self) -> impl std::future::Future<Output = ()> + Send + 'static {
            let (fire, fired) = oneshot::channel();
            let (armed, armed_rx) = oneshot::channel();
            let (completed, completed_rx) = oneshot::channel();
            self.fire = Some(fire);
            self.armed = Some(armed_rx);
            self.completed = Some(completed_rx);
            let dropped = self.dropped.clone();
            async move {
                let _dropped = DropRecorder(dropped);
                let _ = armed.send(());
                let _ = fired.await;
                let _ = completed.send(());
            }
        }

        async fn wait_until_armed(&mut self) {
            self.armed
                .take()
                .expect("the escalation action was created")
                .await
                .expect("the escalation action armed");
        }

        async fn wait_until_completed(&mut self) {
            self.completed
                .take()
                .expect("the escalation action was created")
                .await
                .expect("the escalation action completed");
        }

        fn fire_second(&mut self) {
            self.fire
                .take()
                .expect("the escalation action is armed")
                .send(())
                .expect("the escalation action still waits for its second signal");
        }
    }

    /// Sets its flag when the wrapped future drops, so an aborted escalation
    /// cannot leave a detached task holding the action.
    struct DropRecorder(Arc<AtomicBool>);

    impl Drop for DropRecorder {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    // The owner observes the first signal through the stop token rather than
    // consuming its task handle, so `finish` remains the single consumer.
    #[tokio::test]
    async fn signal_cleanup() {
        let aborted = Arc::new(AtomicBool::new(false));
        let (signal, fire) = ControlledSignal::new(aborted.clone());
        let mut escalation = ControlledEscalation::new();
        let action = escalation.action();
        let mut signals = SignalTasks::with_signals(signal, action);
        let stop = signals.stop();
        assert!(!stop.is_cancelled());

        fire.send(())
            .expect("the bridge is waiting for the first signal");
        stop.cancelled().await;
        assert!(stop.is_cancelled());

        signals.finish().await;
        assert!(signals.first.is_none());
        assert!(signals.escalation.is_none());
        assert!(signals.escalation_action.is_none());
        assert!(aborted.load(Ordering::SeqCst));
    }

    // Finishing without a first signal aborts the waiter and drops the
    // never-armed escalation action once.
    #[tokio::test]
    async fn finish_aborts_signals() {
        let aborted = Arc::new(AtomicBool::new(false));
        let (signal, _fire) = ControlledSignal::new(aborted.clone());
        let mut escalation = ControlledEscalation::new();
        let action = escalation.action();
        let mut signals = SignalTasks::with_signals(signal, action);
        signals.arm_escalation();
        assert!(signals.escalation.is_some());

        signals.finish().await;

        assert!(aborted.load(Ordering::SeqCst));
        assert!(signals.first.is_none());
        assert!(signals.escalation.is_none());
        assert!(signals.escalation_action.is_none());
    }

    // Acquisition cleanup arms escalation after the stop was accepted, and
    // finishing then really drops the waiting action instead of detaching it.
    #[tokio::test]
    async fn finish_aborts_escalation() {
        let (signal, fire) = ControlledSignal::new(Arc::new(AtomicBool::new(false)));
        let mut escalation = ControlledEscalation::new();
        let action = escalation.action();
        let mut signals = SignalTasks::with_signals(signal, action);
        let stop = signals.stop();

        fire.send(())
            .expect("the bridge is waiting for the first signal");
        stop.cancelled().await;
        signals.arm_escalation();
        escalation.wait_until_armed().await;
        assert!(
            !signals
                .escalation
                .as_ref()
                .expect("the escalation task is retained")
                .is_finished()
        );

        signals.finish().await;

        assert!(signals.first.is_none());
        assert!(signals.escalation.is_none());
        assert!(
            escalation.dropped.load(Ordering::SeqCst),
            "finish must drop the escalation action, not only its wrapper"
        );
    }

    // The active path: an already-accepted stop lets the second signal end the
    // escalation action, and finishing still consumes each task once.
    #[tokio::test]
    async fn escalation_completes() {
        let (signal, fire) = ControlledSignal::new(Arc::new(AtomicBool::new(false)));
        let mut escalation = ControlledEscalation::new();
        let action = escalation.action();
        let mut signals = SignalTasks::with_signals(signal, action);
        let stop = signals.stop();

        fire.send(())
            .expect("the bridge is waiting for the first signal");
        stop.cancelled().await;
        signals.arm_escalation();
        escalation.wait_until_armed().await;

        escalation.fire_second();
        escalation.wait_until_completed().await;

        signals.finish().await;
        assert!(signals.first.is_none());
        assert!(signals.escalation.is_none());
        assert!(signals.escalation_action.is_none());
    }

    // Repeated application runs share one runtime, so each owner must clear
    // both of its signal tasks before the next run installs its own.
    #[tokio::test]
    async fn runs_clear_signals() {
        for run in 0..2 {
            let aborted = Arc::new(AtomicBool::new(false));
            let (signal, fire) = ControlledSignal::new(aborted.clone());
            let mut escalation = ControlledEscalation::new();
            let action = escalation.action();
            let dropped = escalation.dropped.clone();
            let mut signals = SignalTasks::with_signals(signal, action);

            fire.send(())
                .expect("the bridge is waiting for the first signal");
            signals.stop().cancelled().await;
            signals.arm_escalation();
            escalation.wait_until_armed().await;
            signals.finish().await;

            assert!(signals.first.is_none(), "run {run} retained the first task");
            assert!(
                signals.escalation.is_none(),
                "run {run} retained the second task"
            );
            assert!(
                aborted.load(Ordering::SeqCst),
                "run {run} left the first task running"
            );
            assert!(
                dropped.load(Ordering::SeqCst),
                "run {run} left the second task running"
            );
        }
    }

    /// The same minimal owner set as the shutdown-module helper, so the
    /// mapping test runs a real `NodeShutdown` sequence.
    fn node_shutdown(
        shutdown: aruna_core::shutdown::Shutdown,
        storage_handle: aruna_storage::StorageHandle,
        grace: std::time::Duration,
    ) -> NodeShutdown {
        NodeShutdown {
            shutdown,
            readiness: aruna_api::monitoring::Readiness::new(),
            rest: None,
            s3: None,
            portal: None,
            session_s3: None,
            monitoring: None,
            task_handle: aruna_tasks::TaskHandle::new(),
            jobs_runtime: aruna_operations::jobs::runtime::JobsRuntime::new(),
            net_handle: None,
            metadata_handle: None,
            blob_handle: None,
            storage_handle,
            ops: None,
            grace,
        }
    }

    // The acquisition, realm-preparation, listener-binding, and
    // background-start cancellations all map through `cancellation_outcome`; a
    // real complete sequence is the clean cancellation and a real incomplete
    // one is the nonzero incomplete stop, so no accepted stop can report
    // success after a cleanup that left an owner unresolved.
    #[tokio::test]
    async fn cancellation_requires_cleanup() {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage_handle =
            aruna_storage::FjallStorage::open(dir.path().to_str().expect("utf8 path"))
                .expect("storage opens");

        let complete = node_shutdown(
            aruna_core::shutdown::Shutdown::new(),
            storage_handle.clone(),
            crate::shutdown::MIN_SHUTDOWN_GRACE,
        )
        .run()
        .await;
        assert!(complete.complete(), "an idle sequence must complete");
        assert_eq!(
            cancellation_outcome(&complete),
            ProcessOutcome::StartupCancelled
        );

        let shutdown = aruna_core::shutdown::Shutdown::new();
        shutdown.spawn(std::future::pending());
        let incomplete =
            node_shutdown(shutdown, storage_handle, std::time::Duration::from_millis(200))
                .run()
                .await;
        assert!(
            !incomplete.background_drained,
            "a pending child must leave the sequence incomplete: {incomplete:?}"
        );
        assert_eq!(
            cancellation_outcome(&incomplete),
            ProcessOutcome::StoppedIncomplete
        );
    }

    // The real purge boundary: an unfinished shutdown leaves the files in
    // place, and the completed sequence then erases the root contents.
    #[test]
    fn wipe_roots_retained() {
        let root = tempfile::tempdir().expect("temp dir");
        std::fs::write(root.path().join("object"), b"data").expect("fixture file");
        let wipe = device_wipe::DeviceWipe::new(vec![root.path().to_path_buf()], Vec::new());

        assert_eq!(
            decide_wipe(&wipe, false, device_wipe::purge),
            ProcessOutcome::WipeIncomplete
        );
        assert!(
            root.path().join("object").exists(),
            "an unfinished shutdown must not erase the roots"
        );

        assert_eq!(
            decide_wipe(&wipe, true, device_wipe::purge),
            ProcessOutcome::WipeComplete
        );
        assert!(
            !root.path().join("object").exists(),
            "the completed wipe must erase the root contents"
        );
        assert!(root.path().is_dir(), "the wipe keeps the root itself");
    }
}
