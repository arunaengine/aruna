//! Tracks connection and stream progress for the S3 listener and cancels stalled work.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::warn;

/// The idle bound a connection falls back to when no configured one was
/// installed.
pub(super) const CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_secs(20);
/// Body bytes between two progress units. Small frames must not hide a stream
/// that never reaches a unit, so they accumulate until one is crossed.
pub(super) const STREAM_PROGRESS_BYTES: usize = 1024;

/// One measurable progress counter with a cancellation broadcast. A connection
/// activity watches transport I/O, a stream activity one request body or
/// response, and a deadline activity only carries a timer cancellation.
#[derive(Default)]
pub(super) struct ConnectionActivity {
    generation: AtomicU64,
    progress: AtomicUsize,
    cancelled: AtomicBool,
    stopped: AtomicBool,
    requested: AtomicBool,
    active: AtomicUsize,
    notify: Notify,
    // `None` keeps the built-in idle bound; the listener installs the configured one.
    idle_timeout: Option<Duration>,
}

impl ConnectionActivity {
    pub(super) fn with_idle(idle_timeout: Duration) -> Self {
        Self {
            idle_timeout: Some(idle_timeout),
            ..Self::default()
        }
    }

    pub(super) fn touch(&self) {
        if self.is_operable() {
            self.generation.fetch_add(1, Ordering::AcqRel);
            self.notify.notify_waiters();
        }
    }

    pub(super) fn record_progress(&self, bytes: usize) {
        if bytes == 0 || !self.is_operable() {
            return;
        }

        let mut progress = self.progress.load(Ordering::Acquire);
        loop {
            let (next, crossed) = accumulate_progress(progress, bytes);
            match self.progress.compare_exchange_weak(
                progress,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if crossed {
                        self.touch();
                    }
                    return;
                }
                Err(updated) => progress = updated,
            }
        }
    }

    pub(super) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub(super) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.notify.notify_waiters();
    }

    pub(super) fn mark_request(&self) {
        self.requested.store(true, Ordering::Release);
        self.touch();
    }

    pub(super) fn begin_request(&self) {
        self.active.fetch_add(1, Ordering::AcqRel);
        self.touch();
    }

    fn end_request(&self) {
        let _previous = self
            .active
            .try_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                Some(active.saturating_sub(1))
            });
        self.touch();
    }

    pub(super) fn has_request(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(super) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub(super) fn active_requests(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    pub(super) async fn wait_cancelled(&self) {
        while !self.is_cancelled() {
            let notified = self.notify.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }

    pub(super) async fn wait_done(&self) {
        while !self.is_cancelled() && !self.is_stopped() {
            let notified = self.notify.notified();
            if self.is_cancelled() || self.is_stopped() {
                return;
            }
            notified.await;
        }
    }

    pub(super) async fn wait_idle(&self) -> bool {
        loop {
            if self.is_cancelled() || self.is_stopped() {
                return false;
            }
            let generation = self.generation.load(Ordering::Acquire);
            let notified = self.notify.notified();
            if generation != self.generation.load(Ordering::Acquire) {
                continue;
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(self.idle_timeout.unwrap_or(CONNECTION_IDLE_TIMEOUT)) => {
                    if generation == self.generation.load(Ordering::Acquire)
                        && !self.is_cancelled()
                        && !self.is_stopped()
                        && self.active_requests() == 0
                    {
                        self.cancel();
                        return true;
                    }
                }
            }
        }
    }

    fn is_operable(&self) -> bool {
        !self.is_cancelled() && !self.is_stopped()
    }
}

/// Releases the active-request accounting and stops the total deadline when
/// dropped. It is owned by whichever lifetime holder serves the response, so a
/// returned header cannot end the request early.
pub(super) struct ActiveRequestGuard {
    activity: Arc<ConnectionActivity>,
    deadline: Arc<ConnectionActivity>,
}

impl ActiveRequestGuard {
    pub(super) fn new(
        activity: Arc<ConnectionActivity>,
        deadline: Arc<ConnectionActivity>,
    ) -> Self {
        Self { activity, deadline }
    }
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.activity.end_request();
        self.deadline.stop();
    }
}

/// Accumulates stream bytes toward one progress unit. The first value is the
/// carry into the next unit and the second whether the unit was crossed.
pub(super) fn accumulate_progress(current: usize, bytes: usize) -> (usize, bool) {
    let total = current.saturating_add(bytes);
    if total >= STREAM_PROGRESS_BYTES {
        (0, true)
    } else {
        (total, false)
    }
}

/// The total lifetime deadline fires only while the request is neither
/// finished nor already cancelled.
pub(super) fn should_expire_total(activity: &ConnectionActivity) -> bool {
    !activity.is_cancelled() && !activity.is_stopped()
}

/// An unread request body is watched for idleness; a body that already ended
/// has no stall left to observe.
pub(super) fn should_watch_idle(body_end: bool) -> bool {
    !body_end
}

/// A response is cancelled when its idle watch fired or the total deadline
/// expired.
pub(super) fn should_cancel_response(idle: bool, deadline: &ConnectionActivity) -> bool {
    idle || deadline.is_cancelled()
}

/// Async adapter for the total-lifetime timer: cancels the returned activity
/// once the lifetime elapses while the request is still running.
pub(super) fn spawn_total_deadline(lifetime: Duration) -> Arc<ConnectionActivity> {
    let activity = Arc::new(ConnectionActivity::default());
    let timer_activity = activity.clone();
    let deadline = tokio::time::Instant::now() + lifetime;
    tokio::spawn(async move {
        tokio::select! {
            _ = timer_activity.wait_done() => {}
            _ = tokio::time::sleep_until(deadline) => {
                if should_expire_total(&timer_activity) {
                    // The cancelled request returns without a completion
                    // record, so this is its only trace.
                    warn!(
                        event = "s3.request.lifetime_expired",
                        timeout_ms = lifetime.as_millis() as u64,
                        "Cancelling an S3 request that outlived its stream lifetime"
                    );
                    timer_activity.cancel();
                }
            }
        }
    });
    activity
}

/// Async adapter for the request-body idle watch: touches the stream once and
/// cancels it when no progress arrives within the idle bound.
pub(super) fn spawn_stream_idle(activity: &Arc<ConnectionActivity>) {
    activity.touch();
    let idle_activity = activity.clone();
    tokio::spawn(async move {
        idle_activity.wait_idle().await;
    });
}

/// Async adapter for a streaming response: cancels the connection when the
/// response goes idle or the total deadline expires.
pub(super) fn spawn_response_watch(
    connection: Arc<ConnectionActivity>,
    idle: Arc<ConnectionActivity>,
    deadline: Arc<ConnectionActivity>,
) {
    tokio::spawn(async move {
        let cancel = tokio::select! {
            idle = idle.wait_idle() => should_cancel_response(idle, &deadline),
            _ = deadline.wait_done() => should_cancel_response(false, &deadline),
        };
        if cancel {
            connection.cancel();
        }
    });
}

/// Accepts until `connection` finishes, the first request goes unanswered past
/// `initial`, or the connection sits idle.
pub(super) async fn run_connection<F>(
    activity: Arc<ConnectionActivity>,
    connection: F,
    initial: Duration,
) where
    F: Future + Send,
{
    let mut connection = Box::pin(connection);
    let initial = tokio::time::sleep(initial);
    tokio::pin!(initial);

    tokio::select! {
        result = &mut connection => {
            let _ = result;
        }
        _ = &mut initial, if !activity.has_request() => {
            activity.cancel();
        }
        _ = activity.wait_idle() => {
            activity.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::server::{INITIAL_REQUEST_TIMEOUT, STREAM_LIFETIME_TIMEOUT};
    use std::sync::atomic::Ordering;

    #[tokio::test(start_paused = true)]
    async fn idle_connection_closes() {
        let activity = Arc::new(ConnectionActivity::default());
        let task = tokio::spawn(run_connection(
            activity,
            async { std::future::pending::<hyper::Result<()>>().await },
            INITIAL_REQUEST_TIMEOUT,
        ));
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        tokio::time::advance(CONNECTION_IDLE_TIMEOUT).await;
        tokio::task::yield_now().await;
        assert!(task.is_finished());
        task.await.expect("idle task joins");
    }

    #[tokio::test(start_paused = true)]
    async fn configured_idle_applies() {
        // A connection given a longer idle budget must outlive the default one.
        let idle = CONNECTION_IDLE_TIMEOUT * 3;
        let activity = Arc::new(ConnectionActivity::with_idle(idle));
        let task = tokio::spawn(run_connection(
            activity,
            async { std::future::pending::<hyper::Result<()>>().await },
            INITIAL_REQUEST_TIMEOUT * 3,
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(CONNECTION_IDLE_TIMEOUT).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        tokio::time::advance(idle).await;
        tokio::task::yield_now().await;
        assert!(task.is_finished());
        task.await.expect("idle task joins");
    }

    #[tokio::test(start_paused = true)]
    async fn unrequested_closes() {
        let activity = Arc::new(ConnectionActivity::default());
        let task = tokio::spawn(run_connection(
            activity,
            async { std::future::pending::<hyper::Result<()>>().await },
            INITIAL_REQUEST_TIMEOUT,
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(INITIAL_REQUEST_TIMEOUT).await;
        tokio::task::yield_now().await;
        assert!(task.is_finished());
        task.await.expect("unrequested task joins");
    }

    #[tokio::test(start_paused = true)]
    async fn progress_keeps_alive() {
        let activity = Arc::new(ConnectionActivity::default());
        activity.mark_request();
        let marker = activity.clone();
        let task = tokio::spawn(run_connection(
            activity,
            async move {
                tokio::time::sleep(CONNECTION_IDLE_TIMEOUT - Duration::from_secs(1)).await;
                marker.touch();
                tokio::time::sleep(CONNECTION_IDLE_TIMEOUT - Duration::from_secs(1)).await;
                Ok::<(), hyper::Error>(())
            },
            INITIAL_REQUEST_TIMEOUT,
        ));
        tokio::task::yield_now().await;
        tokio::time::advance(CONNECTION_IDLE_TIMEOUT - Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        tokio::time::advance(CONNECTION_IDLE_TIMEOUT - Duration::from_secs(1)).await;
        task.await.expect("request task joins");
    }

    #[tokio::test(start_paused = true)]
    async fn stream_progress_survives() {
        let activity = Arc::new(ConnectionActivity::default());
        let task_activity = activity.clone();
        let watcher = tokio::spawn(async move { task_activity.wait_idle().await });
        tokio::task::yield_now().await;
        tokio::time::advance(CONNECTION_IDLE_TIMEOUT - Duration::from_secs(1)).await;
        activity.touch();
        tokio::task::yield_now().await;
        assert!(!activity.is_cancelled());
        tokio::time::advance(CONNECTION_IDLE_TIMEOUT - Duration::from_secs(1)).await;
        assert!(!activity.is_cancelled());
        activity.stop();
        watcher.await.expect("stream watcher joins");
    }

    #[tokio::test(start_paused = true)]
    async fn byte_trickle_closes() {
        let activity = Arc::new(ConnectionActivity::default());
        let task_activity = activity.clone();
        let watcher = tokio::spawn(async move { task_activity.wait_idle().await });
        tokio::task::yield_now().await;
        tokio::time::advance(CONNECTION_IDLE_TIMEOUT - Duration::from_secs(1)).await;
        activity.record_progress(1);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(activity.is_cancelled());
        watcher.await.expect("stream task joins");
    }

    #[tokio::test(start_paused = true)]
    async fn small_frames_progress() {
        let activity = Arc::new(ConnectionActivity::default());
        let watcher = {
            let task = activity.clone();
            tokio::spawn(async move { task.wait_idle().await })
        };
        tokio::task::yield_now().await;
        let half = STREAM_PROGRESS_BYTES / 2;
        activity.record_progress(half);
        tokio::time::advance(CONNECTION_IDLE_TIMEOUT / 2).await;
        activity.record_progress(STREAM_PROGRESS_BYTES - half);
        tokio::task::yield_now().await;
        assert!(!activity.is_cancelled());
        tokio::time::advance(CONNECTION_IDLE_TIMEOUT - Duration::from_secs(1)).await;
        assert!(!activity.is_cancelled());
        activity.stop();
        watcher.await.expect("stream task joins");
    }

    #[tokio::test(start_paused = true)]
    async fn large_frame_bound() {
        let activity = Arc::new(ConnectionActivity::default());
        let watcher = {
            let task = activity.clone();
            tokio::spawn(async move { task.wait_idle().await })
        };
        tokio::task::yield_now().await;
        activity.record_progress(STREAM_PROGRESS_BYTES * 2);
        tokio::task::yield_now().await;
        tokio::time::advance(CONNECTION_IDLE_TIMEOUT - Duration::from_secs(1)).await;
        activity.record_progress(1);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(activity.is_cancelled());
        watcher.await.expect("stream task joins");
    }

    #[tokio::test(start_paused = true)]
    async fn handler_survives_idle() {
        let activity = Arc::new(ConnectionActivity::default());
        activity.mark_request();
        activity.begin_request();
        let watcher = {
            let task = activity.clone();
            tokio::spawn(async move { task.wait_idle().await })
        };
        tokio::time::advance(CONNECTION_IDLE_TIMEOUT * 2).await;
        assert!(!activity.is_cancelled());
        activity.end_request();
        activity.stop();
        watcher.await.expect("idle watcher joins");
    }

    #[tokio::test(start_paused = true)]
    async fn request_deadline_expires() {
        let activity = Arc::new(ConnectionActivity::default());
        activity.mark_request();
        activity.begin_request();
        let deadline_activity = spawn_total_deadline(STREAM_LIFETIME_TIMEOUT);
        let stream_activity = Arc::new(ConnectionActivity::default());
        spawn_stream_idle(&stream_activity);
        let control_limit = Arc::new(tokio::sync::Semaphore::new(1));
        let control_permit = control_limit
            .clone()
            .try_acquire_owned()
            .expect("control permit");
        let egress_limit = Arc::new(tokio::sync::Semaphore::new(1));
        let egress_permit = egress_limit
            .clone()
            .try_acquire_owned()
            .expect("egress permit");
        let active = ActiveRequestGuard::new(activity.clone(), deadline_activity.clone());
        let request_activity = stream_activity.clone();
        let request_deadline = deadline_activity.clone();
        let request = tokio::spawn(async move {
            let _active = active;
            let _control_permit = control_permit;
            let _egress_permit = egress_permit;
            tokio::select! {
                _ = request_activity.wait_cancelled() => {}
                _ = request_deadline.wait_cancelled() => request_activity.stop(),
            }
        });
        tokio::task::yield_now().await;
        let tick = Duration::from_secs(19);
        for _ in 0..94 {
            tokio::time::advance(tick).await;
            stream_activity.touch();
            tokio::task::yield_now().await;
        }
        assert!(!stream_activity.is_cancelled());
        tokio::time::advance(STREAM_LIFETIME_TIMEOUT - Duration::from_secs(19 * 94)).await;
        request.await.expect("request task joins");
        assert!(deadline_activity.is_cancelled());
        assert!(stream_activity.is_stopped());
        assert_eq!(control_limit.available_permits(), 1);
        assert_eq!(egress_limit.available_permits(), 1);
        assert_eq!(activity.active_requests(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn sibling_not_refresh() {
        let stalled = Arc::new(ConnectionActivity::default());
        let sibling = Arc::new(ConnectionActivity::default());
        let stalled_task = {
            let task = stalled.clone();
            tokio::spawn(async move { task.wait_idle().await })
        };
        tokio::task::yield_now().await;
        sibling.touch();
        tokio::time::advance(CONNECTION_IDLE_TIMEOUT).await;
        tokio::task::yield_now().await;
        assert!(stalled.is_cancelled());
        assert!(!sibling.is_cancelled());
        stalled_task.await.expect("stream task joins");
    }

    #[test]
    fn progress_crosses_units() {
        assert_eq!(accumulate_progress(0, 1), (1, false));
        assert_eq!(accumulate_progress(STREAM_PROGRESS_BYTES - 1, 1), (0, true));
        assert_eq!(accumulate_progress(STREAM_PROGRESS_BYTES, 1), (0, true));
        assert_eq!(accumulate_progress(STREAM_PROGRESS_BYTES * 2, 1), (0, true));
    }

    #[test]
    fn deadline_skips_finished() {
        let running = ConnectionActivity::default();
        assert!(should_expire_total(&running));
        running.cancel();
        assert!(!should_expire_total(&running));
        let stopped = ConnectionActivity::default();
        stopped.stop();
        assert!(!should_expire_total(&stopped));
    }

    #[test]
    fn ended_body_unwatched() {
        assert!(!should_watch_idle(true));
        assert!(should_watch_idle(false));
    }

    #[test]
    fn response_cancel_policy() {
        let deadline = ConnectionActivity::default();
        assert!(should_cancel_response(true, &deadline));
        assert!(!should_cancel_response(false, &deadline));
        deadline.cancel();
        assert!(should_cancel_response(false, &deadline));
    }

    #[tokio::test]
    async fn drops_pending_guard() {
        let activity = Arc::new(ConnectionActivity::default());
        activity.begin_request();
        let deadline = Arc::new(ConnectionActivity::default());
        let ready = Arc::new(Notify::new());
        let task_activity = activity.clone();
        let task_deadline = deadline.clone();
        let task_ready = ready.clone();
        let task = tokio::spawn(async move {
            let _guard = ActiveRequestGuard::new(task_activity, task_deadline);
            task_ready.notify_one();
            std::future::pending::<()>().await;
        });
        ready.notified().await;
        task.abort();
        let _ = task.await;
        assert_eq!(activity.active.load(Ordering::Acquire), 0);
        assert!(deadline.is_stopped());
    }
}
