//! Request and response bodies of the S3 listener. Request bodies report
//! progress (and capture a bounded DeleteObjects prefix); response bodies own a
//! [`ResponseLifetime`] released once, on completion, error, or drop.

use super::activity::{ActiveRequestGuard, ConnectionActivity};
use crate::rate_limit::LocalLease;
use futures_core::future::BoxFuture;
use hyper::body::Incoming;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::OwnedSemaphorePermit;

/// Upper bound for a DeleteObjects request body; anything larger is rejected
/// before it is parsed.
pub(super) const DELETE_MAX_BODY: usize = 2 * 1024 * 1024;
/// Independent phase permitting concurrent DeleteObjects body aggregation; an
/// oversized request holds no permit.
pub(super) const DELETE_CAPTURE_LIMIT: usize = 16;

pub(super) fn touch_frame(
    activity: &ConnectionActivity,
    frame: &hyper::body::Frame<hyper::body::Bytes>,
) {
    if let Some(data) = frame.data_ref().filter(|data| !data.is_empty()) {
        activity.record_progress(data.len());
    } else if frame.is_trailers() {
        activity.touch();
    }
}

#[derive(Default)]
struct DeleteObjectsState {
    bytes: Vec<u8>,
    exceeded: bool,
}

/// Captures the bounded prefix of a DeleteObjects body. The same state is
/// cloned into the request extensions before the handler reads it.
#[derive(Clone, Default)]
pub(crate) struct DeleteObjectsBody(Arc<Mutex<DeleteObjectsState>>);

impl DeleteObjectsBody {
    fn append(&self, data: &[u8]) -> usize {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let remaining = DELETE_MAX_BODY.saturating_sub(state.bytes.len());
        let copied = data.len().min(remaining);
        state.bytes.extend_from_slice(&data[..copied]);
        state.exceeded |= copied < data.len();
        copied
    }

    pub(crate) fn take_bytes(&self) -> Vec<u8> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::take(&mut state.bytes)
    }

    pub(crate) fn exceeded(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .exceeded
    }
}

/// Wraps the transport body with the capture or progress behaviour the
/// classification selected. A `None` capture only tracks progress.
pub(super) fn wrap_request_body(
    body: Incoming,
    connection: &Arc<ConnectionActivity>,
    stream: &Arc<ConnectionActivity>,
    capture: Option<DeleteObjectsBody>,
) -> s3s::Body {
    match capture {
        Some(captured) => s3s::Body::http_body_unsync(CaptureObjectsBody {
            inner: Box::pin(body),
            captured,
            activity: stream.clone(),
            connection_activity: connection.clone(),
            ended: false,
        }),
        None => s3s::Body::http_body_unsync(TrackRequestBody {
            inner: Box::pin(body),
            activity: stream.clone(),
            connection_activity: connection.clone(),
            ended: false,
        }),
    }
}

struct CaptureObjectsBody {
    inner: Pin<Box<Incoming>>,
    captured: DeleteObjectsBody,
    activity: Arc<ConnectionActivity>,
    connection_activity: Arc<ConnectionActivity>,
    ended: bool,
}

impl hyper::body::Body for CaptureObjectsBody {
    type Data = hyper::body::Bytes;
    type Error = s3s::StdError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                touch_frame(&self.activity, &frame);
                touch_frame(&self.connection_activity, &frame);
                if frame.is_trailers() {
                    self.ended = true;
                    self.activity.stop();
                    return Poll::Ready(Some(Ok(frame)));
                }
                let Some(data) = frame.data_ref() else {
                    return Poll::Ready(Some(Ok(frame)));
                };
                let copied = self.captured.append(data);
                if copied < data.len() {
                    self.ended = true;
                    self.activity.stop();
                    return Poll::Ready(Some(Err(Box::new(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "DeleteObjects request body exceeds 2 MiB",
                    )))));
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(None) => {
                self.ended = true;
                self.activity.stop();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                self.ended = true;
                self.activity.stop();
                Poll::Ready(Some(Err(Box::new(error))))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.ended || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        let mut hint = self.inner.size_hint();
        if hint
            .upper()
            .is_none_or(|upper| upper > DELETE_MAX_BODY as u64)
        {
            hint.set_upper(DELETE_MAX_BODY as u64);
        }
        hint
    }
}

impl Drop for CaptureObjectsBody {
    fn drop(&mut self) {
        self.activity.stop();
    }
}

struct TrackRequestBody {
    inner: Pin<Box<Incoming>>,
    activity: Arc<ConnectionActivity>,
    connection_activity: Arc<ConnectionActivity>,
    ended: bool,
}

impl hyper::body::Body for TrackRequestBody {
    type Data = hyper::body::Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                touch_frame(&self.activity, &frame);
                touch_frame(&self.connection_activity, &frame);
                if frame.is_trailers() {
                    self.ended = true;
                    self.activity.stop();
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(None) => {
                self.ended = true;
                self.activity.stop();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                self.ended = true;
                self.activity.stop();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.ended || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for TrackRequestBody {
    fn drop(&mut self) {
        self.activity.stop();
    }
}

/// Everything owned until the response stream finishes: the egress permit, the
/// active-request accounting and the local rate-limit lease. It attaches to
/// whichever body serves the response, so headers alone release nothing.
pub(super) struct ResponseLifetime {
    egress: Option<OwnedSemaphorePermit>,
    active: Option<ActiveRequestGuard>,
    lease: Option<LocalLease>,
}

impl ResponseLifetime {
    pub(super) fn new(active: Option<ActiveRequestGuard>, lease: LocalLease) -> Self {
        Self {
            egress: None,
            active,
            lease: Some(lease),
        }
    }

    pub(super) fn with_egress(mut self, permit: Option<OwnedSemaphorePermit>) -> Self {
        self.egress = permit;
        self
    }

    fn finish(&mut self) {
        self.active.take();
        self.lease.take();
        self.egress.take();
    }
}

impl Drop for ResponseLifetime {
    fn drop(&mut self) {
        self.finish();
    }
}

pub(super) struct ResponseBody {
    inner: Pin<Box<s3s::Body>>,
    lifetime: Option<ResponseLifetime>,
    activity: Arc<ConnectionActivity>,
    response_activity: Arc<ConnectionActivity>,
    cancellation: BoxFuture<'static, ()>,
    ended: bool,
}

impl ResponseBody {
    pub(super) fn new(
        inner: s3s::Body,
        activity: Arc<ConnectionActivity>,
        response_activity: Arc<ConnectionActivity>,
        lifetime: ResponseLifetime,
    ) -> Self {
        let connection_activity = activity.clone();
        let body_activity = response_activity.clone();
        let cancellation = Box::pin(async move {
            tokio::select! {
                _ = connection_activity.wait_cancelled() => {}
                _ = body_activity.wait_cancelled() => {}
            }
        });
        Self {
            inner: Box::pin(inner),
            lifetime: Some(lifetime),
            activity,
            response_activity,
            cancellation,
            ended: false,
        }
    }

    fn finish(&mut self) {
        self.lifetime.take();
    }
}

impl hyper::body::Body for ResponseBody {
    type Data = hyper::body::Bytes;
    type Error = s3s::StdError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        if self.cancellation.as_mut().poll(cx).is_ready() {
            self.ended = true;
            self.finish();
            if self.activity.is_cancelled() {
                self.response_activity.stop();
                return Poll::Ready(Some(Err(Box::new(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "S3 connection became idle",
                )))));
            }
            self.response_activity.stop();
            return Poll::Ready(Some(Err(Box::new(io::Error::new(
                io::ErrorKind::TimedOut,
                "S3 response became idle",
            )))));
        }
        if self.activity.is_cancelled() {
            self.ended = true;
            self.finish();
            self.response_activity.stop();
            return Poll::Ready(Some(Err(Box::new(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "S3 connection became idle",
            )))));
        }
        if self.response_activity.is_cancelled() {
            self.ended = true;
            self.finish();
            self.response_activity.stop();
            return Poll::Ready(Some(Err(Box::new(io::Error::new(
                io::ErrorKind::TimedOut,
                "S3 response became idle",
            )))));
        }
        match self.inner.as_mut().poll_frame(cx) {
            Poll::Ready(None) => {
                self.ended = true;
                self.finish();
                self.response_activity.stop();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                self.ended = true;
                self.finish();
                self.response_activity.stop();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(frame))) => {
                touch_frame(&self.activity, &frame);
                touch_frame(&self.response_activity, &frame);
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.ended || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for ResponseBody {
    fn drop(&mut self) {
        self.finish();
        self.response_activity.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::LocalPermit;
    use bytes::Bytes;
    use std::task::Waker;
    use std::time::Duration;
    use tokio::sync::{Notify, Semaphore};

    /// A body that stays pending until the test signals it, so the response can
    /// be observed open without a live client.
    struct ControlledBody {
        ready: Option<tokio::sync::oneshot::Receiver<()>>,
        failed: bool,
        finished: bool,
    }

    impl hyper::body::Body for ControlledBody {
        type Data = hyper::body::Bytes;
        type Error = s3s::StdError;

        fn poll_frame(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
            let this = self.get_mut();
            if this.finished {
                return Poll::Ready(None);
            }
            if this.ready.is_some() {
                let signalled = match this.ready.as_mut() {
                    Some(ready) => match Pin::new(ready).poll(cx) {
                        Poll::Ready(_) => true,
                        Poll::Pending => return Poll::Pending,
                    },
                    None => false,
                };
                if signalled {
                    this.ready = None;
                    if this.failed {
                        return Poll::Ready(Some(Err(Box::new(io::Error::other(
                            "controlled failure",
                        )))));
                    }
                    return Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_static(
                        b"body",
                    )))));
                }
            }
            this.finished = true;
            Poll::Ready(None)
        }
    }

    fn controlled(ready: tokio::sync::oneshot::Receiver<()>, failed: bool) -> s3s::Body {
        s3s::Body::http_body_unsync(ControlledBody {
            ready: Some(ready),
            failed,
            finished: false,
        })
    }

    type FramePoll<B> = Poll<
        Option<
            Result<
                hyper::body::Frame<<B as hyper::body::Body>::Data>,
                <B as hyper::body::Body>::Error,
            >,
        >,
    >;

    /// Polls a body exactly once, returning the first poll result without
    /// waiting.
    fn poll_once<B: hyper::body::Body + Unpin>(body: &mut B) -> FramePoll<B> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        hyper::body::Body::poll_frame(Pin::new(body), &mut context)
    }

    fn lifetime_owners(
        activity: &Arc<ConnectionActivity>,
        deadline: &Arc<ConnectionActivity>,
        lease: LocalLease,
    ) -> (ResponseLifetime, Arc<Semaphore>, Arc<Semaphore>) {
        let permit_limit = Arc::new(Semaphore::new(1));
        let egress = permit_limit
            .clone()
            .try_acquire_owned()
            .expect("egress permit");
        let local_limit = Arc::new(Semaphore::new(1));
        assert!(
            lease.hold(LocalPermit::test_permit(local_limit.clone())),
            "local permit installs"
        );
        let active = ActiveRequestGuard::new(activity.clone(), deadline.clone());
        let lifetime = ResponseLifetime::new(Some(active), lease).with_egress(Some(egress));
        (lifetime, permit_limit, local_limit)
    }

    #[tokio::test]
    async fn body_holds_lifetime() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let activity = Arc::new(ConnectionActivity::default());
        activity.mark_request();
        activity.begin_request();
        let deadline = Arc::new(ConnectionActivity::default());
        let response_activity = Arc::new(ConnectionActivity::default());
        response_activity.touch();
        let (lifetime, egress_limit, local_limit) =
            lifetime_owners(&activity, &deadline, LocalLease::default());
        let mut body = ResponseBody::new(
            controlled(receiver, false),
            activity.clone(),
            response_activity,
            lifetime,
        );

        // The response is open but its headers already returned: every owner
        // must still be held.
        assert!(
            matches!(poll_once(&mut body), Poll::Pending),
            "controlled body stays open"
        );
        assert_eq!(egress_limit.available_permits(), 0);
        assert_eq!(local_limit.available_permits(), 0);
        assert_eq!(activity.active_requests(), 1);
        assert!(!deadline.is_stopped());

        // Completing the stream releases each owner exactly once.
        let _ = sender.send(());
        assert!(matches!(poll_once(&mut body), Poll::Ready(Some(Ok(_)))));
        assert!(matches!(poll_once(&mut body), Poll::Ready(None)));
        assert_eq!(egress_limit.available_permits(), 1);
        assert_eq!(local_limit.available_permits(), 1);
        assert_eq!(activity.active_requests(), 0);
        assert!(deadline.is_stopped());
        drop(body);
        assert_eq!(egress_limit.available_permits(), 1);
        assert_eq!(local_limit.available_permits(), 1);
        assert_eq!(activity.active_requests(), 0);
    }

    #[tokio::test]
    async fn error_releases_lifetime() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let activity = Arc::new(ConnectionActivity::default());
        activity.begin_request();
        let deadline = Arc::new(ConnectionActivity::default());
        let response_activity = Arc::new(ConnectionActivity::default());
        response_activity.touch();
        let (lifetime, egress_limit, local_limit) =
            lifetime_owners(&activity, &deadline, LocalLease::default());
        let mut body = ResponseBody::new(
            controlled(receiver, true),
            activity.clone(),
            response_activity,
            lifetime,
        );

        let _ = sender.send(());
        assert!(matches!(poll_once(&mut body), Poll::Ready(Some(Err(_)))));
        assert_eq!(egress_limit.available_permits(), 1);
        assert_eq!(local_limit.available_permits(), 1);
        assert_eq!(activity.active_requests(), 0);
        assert!(deadline.is_stopped());
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_response_closes() {
        let activity = Arc::new(ConnectionActivity::default());
        activity.mark_request();
        activity.begin_request();
        let response_activity = Arc::new(ConnectionActivity::default());
        response_activity.touch();
        let limit = Arc::new(Semaphore::new(1));
        let permit = limit.clone().try_acquire_owned().expect("permit");
        let deadline_activity = Arc::new(ConnectionActivity::default());
        let active = ActiveRequestGuard::new(activity.clone(), deadline_activity);
        let body = ResponseBody::new(
            s3s::Body::empty(),
            activity.clone(),
            response_activity.clone(),
            ResponseLifetime::new(Some(active), LocalLease::default()).with_egress(Some(permit)),
        );
        let ready = Arc::new(Notify::new());
        let task_ready = ready.clone();
        let idle_activity = response_activity.clone();
        let idle_connection = activity.clone();
        let watcher = tokio::spawn(async move {
            task_ready.notify_one();
            if idle_activity.wait_idle().await {
                idle_connection.cancel();
            }
        });
        let connection_activity = activity.clone();
        let connection = tokio::spawn(async move {
            let _body = body;
            connection_activity.wait_cancelled().await;
        });
        ready.notified().await;
        tokio::time::advance(crate::s3::server::activity::CONNECTION_IDLE_TIMEOUT).await;
        watcher.await.expect("response watcher joins");
        assert!(activity.is_cancelled());
        connection.await.expect("connection task joins");
        assert_eq!(limit.available_permits(), 1);
        assert_eq!(activity.active_requests(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn response_deadline_expires() {
        let activity = Arc::new(ConnectionActivity::default());
        activity.mark_request();
        activity.begin_request();
        let deadline_activity = super::super::activity::spawn_total_deadline(
            crate::s3::server::STREAM_LIFETIME_TIMEOUT,
        );
        let response_activity = Arc::new(ConnectionActivity::default());
        response_activity.touch();
        let limit = Arc::new(Semaphore::new(1));
        let permit = limit.clone().try_acquire_owned().expect("permit");
        let active = ActiveRequestGuard::new(activity.clone(), deadline_activity.clone());
        let body = ResponseBody::new(
            s3s::Body::empty(),
            activity.clone(),
            response_activity.clone(),
            ResponseLifetime::new(Some(active), LocalLease::default()).with_egress(Some(permit)),
        );
        let sibling = Arc::new(ConnectionActivity::default());
        sibling.mark_request();
        let idle_activity = response_activity.clone();
        let idle_connection = activity.clone();
        let deadline_watch = deadline_activity.clone();
        super::super::activity::spawn_response_watch(
            idle_connection.clone(),
            idle_activity,
            deadline_watch,
        );
        let connection_activity = activity.clone();
        let connection = tokio::spawn(async move {
            let _body = body;
            connection_activity.wait_cancelled().await;
        });
        tokio::task::yield_now().await;
        let tick = Duration::from_secs(19);
        for _ in 0..94 {
            tokio::time::advance(tick).await;
            response_activity.touch();
            tokio::task::yield_now().await;
        }
        assert!(!response_activity.is_cancelled());
        assert!(!activity.is_cancelled());
        tokio::time::advance(
            crate::s3::server::STREAM_LIFETIME_TIMEOUT - Duration::from_secs(19 * 94),
        )
        .await;
        connection.await.expect("connection task joins");
        assert!(activity.is_cancelled());
        assert!(!sibling.is_cancelled());
        assert_eq!(limit.available_permits(), 1);
        assert_eq!(activity.active_requests(), 0);
    }

    #[test]
    fn caps_delete_body() {
        let body = DeleteObjectsBody::default();
        assert_eq!(body.append(&vec![0; DELETE_MAX_BODY]), DELETE_MAX_BODY);
        assert_eq!(body.append(b"overflow"), 0);
        assert!(body.exceeded());
        assert_eq!(body.take_bytes().len(), DELETE_MAX_BODY);
    }

    #[test]
    fn holds_response_permit() {
        let limit = Arc::new(Semaphore::new(1));
        let permit = limit.clone().try_acquire_owned().expect("permit");
        let local_limit = Arc::new(Semaphore::new(1));
        let local_permit = LocalPermit::test_permit(local_limit.clone());
        let lease = LocalLease::default();
        assert!(lease.hold(local_permit));
        let activity = Arc::new(ConnectionActivity::default());
        activity.begin_request();
        let deadline = Arc::new(ConnectionActivity::default());
        let active = ActiveRequestGuard::new(activity.clone(), deadline);
        let stream_activity = Arc::new(ConnectionActivity::default());
        let body = ResponseBody::new(
            s3s::Body::empty(),
            activity.clone(),
            stream_activity,
            ResponseLifetime::new(Some(active), lease).with_egress(Some(permit)),
        );
        assert_eq!(limit.available_permits(), 0);
        assert_eq!(local_limit.available_permits(), 0);
        assert_eq!(activity.active_requests(), 1);
        drop(body);
        assert_eq!(limit.available_permits(), 1);
        assert_eq!(local_limit.available_permits(), 1);
        assert_eq!(activity.active_requests(), 0);
    }
}
