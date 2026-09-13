//! Streaming response for a slow CompleteMultipartUpload.
//!
//! AWS answers a completion that takes longer than a normal response with the
//! 200 head first, whitespace while it works and the XML document last. The
//! body is wrapped by [`super::body::ResponseBody`], which keeps the response
//! lifetime attached and touches both activity watchers on every filler, so
//! the long wait cannot be mistaken for a stalled stream.

use super::activity::STREAM_PROGRESS_BYTES;
use bytes::Bytes;
use futures_core::future::BoxFuture;
use http::header;
use s3s::HttpError;
use s3s::HttpResponse;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tracing::warn;

/// A completion that answers faster than this is a plain response; only a slow
/// one streams, so small uploads are untouched.
pub(super) const KEEPALIVE_AFTER: Duration = Duration::from_secs(5);
/// Cadence of the whitespace filler while a completion is still running.
pub(super) const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// Whitespace between the prologue and the result. It is a full progress unit so
/// the response's own idle watch counts it and proxies flush it rather than
/// buffering single bytes.
const KEEPALIVE_FILLER: [u8; STREAM_PROGRESS_BYTES] = [b' '; STREAM_PROGRESS_BYTES];
/// A completion result is a handful of elements; anything larger is a bug.
const KEEPALIVE_BODY_LIMIT: usize = 64 * 1024;
const XML_PROLOGUE: &[u8] = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
const KEEPALIVE_ERROR_BODY: &[u8] =
    b"<Error><Code>InternalError</Code><Message>The multipart completion did not produce a result.</Message></Error>";

/// Waits for the handler. `None` means a completion is still running after the
/// keepalive window, so the caller answers with a streamed body; the handler is
/// borrowed, not dropped, and keeps running behind it.
pub(super) async fn await_handler(
    handler: &mut BoxFuture<'static, Result<HttpResponse, HttpError>>,
    keepalive: bool,
) -> Option<Result<HttpResponse, HttpError>> {
    if !keepalive {
        return Some(handler.await);
    }
    tokio::time::timeout(KEEPALIVE_AFTER, handler).await.ok()
}

pub(super) enum HandlerOutcome {
    Done(Result<HttpResponse, HttpError>),
    Keepalive,
    Aborted,
    TimedOut,
}

/// Drops the handler's status and headers: the 200 head is already on the wire,
/// so only its body still matters. An S3 error body is XML `<Error>` already,
/// which is exactly how AWS reports a late CompleteMultipartUpload failure.
async fn completion_body(handler: BoxFuture<'static, Result<HttpResponse, HttpError>>) -> Bytes {
    let mut body = match handler.await {
        Ok(response) => response.into_body(),
        Err(error) => {
            warn!(error = ?error, "CompleteMultipartUpload failed after its response head");
            return Bytes::from_static(KEEPALIVE_ERROR_BODY);
        }
    };
    match body.store_all_limited(KEEPALIVE_BODY_LIMIT).await {
        Ok(bytes) => strip_xml_prologue(bytes),
        Err(error) => {
            warn!(error = ?error, "CompleteMultipartUpload body could not be read");
            Bytes::from_static(KEEPALIVE_ERROR_BODY)
        }
    }
}

/// The prologue was already sent before the filler; a second one in the middle
/// of the document would make the response invalid XML.
fn strip_xml_prologue(bytes: Bytes) -> Bytes {
    if !bytes.starts_with(b"<?xml") {
        return bytes;
    }
    match bytes.windows(2).position(|window| window == b"?>") {
        Some(end) => bytes.slice(end + 2..),
        None => bytes,
    }
}

pub(super) fn keepalive_response(
    handler: BoxFuture<'static, Result<HttpResponse, HttpError>>,
) -> HttpResponse {
    let mut response = http::Response::new(s3s::Body::http_body_unsync(KeepaliveBody {
        body: Box::pin(completion_body(handler)),
        tick: Box::pin(tokio::time::sleep(KEEPALIVE_INTERVAL)),
        prologue: false,
        ended: false,
    }));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/xml"),
    );
    response
}

/// Holds the connection open with whitespace while the completion runs, then
/// appends the real document.
struct KeepaliveBody {
    body: BoxFuture<'static, Bytes>,
    tick: Pin<Box<tokio::time::Sleep>>,
    prologue: bool,
    ended: bool,
}

impl hyper::body::Body for KeepaliveBody {
    type Data = hyper::body::Bytes;
    type Error = s3s::StdError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        if !self.prologue {
            self.prologue = true;
            return Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_static(
                XML_PROLOGUE,
            )))));
        }
        if let Poll::Ready(bytes) = self.body.as_mut().poll(cx) {
            self.ended = true;
            return Poll::Ready(Some(Ok(hyper::body::Frame::data(bytes))));
        }
        if self.tick.as_mut().poll(cx).is_ready() {
            self.tick = Box::pin(tokio::time::sleep(KEEPALIVE_INTERVAL));
            return Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_static(
                &KEEPALIVE_FILLER,
            )))));
        }
        Poll::Pending
    }

    fn is_end_stream(&self) -> bool {
        self.ended
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    fn slow_handler(
        ready: oneshot::Receiver<()>,
    ) -> BoxFuture<'static, Result<HttpResponse, HttpError>> {
        Box::pin(async move {
            let _ = ready.await;
            Ok(http::Response::new(s3s::Body::from(
                b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUploadResult/>"
                    .to_vec(),
            )))
        })
    }

    async fn next_frame(body: &mut KeepaliveBody) -> Option<Bytes> {
        std::future::poll_fn(|cx| hyper::body::Body::poll_frame(Pin::new(&mut *body), cx))
            .await
            .map(|frame| frame.expect("frame").into_data().expect("data"))
    }

    // A completion still running after the keepalive window is handed back so it
    // can be answered with a streamed body; a fast one answers directly.
    #[tokio::test(start_paused = true)]
    async fn defers_slow_handler() {
        let (sender, receiver) = oneshot::channel();
        let mut slow = slow_handler(receiver);
        assert!(await_handler(&mut slow, true).await.is_none());

        let _ = sender.send(());
        assert!(await_handler(&mut slow, true).await.is_some());

        let (sender, receiver) = oneshot::channel();
        let _ = sender.send(());
        let mut fast = slow_handler(receiver);
        assert!(await_handler(&mut fast, true).await.is_some());
    }

    // The stream is the XML prologue, whitespace while the completion runs, then
    // the document without a second prologue.
    #[tokio::test(start_paused = true)]
    async fn keepalive_streams_filler() {
        let (sender, receiver) = oneshot::channel();
        let mut body = KeepaliveBody {
            body: Box::pin(completion_body(slow_handler(receiver))),
            tick: Box::pin(tokio::time::sleep(KEEPALIVE_INTERVAL)),
            prologue: false,
            ended: false,
        };

        assert_eq!(next_frame(&mut body).await.unwrap(), XML_PROLOGUE);
        let filler = next_frame(&mut body).await.unwrap();
        assert_eq!(filler.len(), STREAM_PROGRESS_BYTES);
        assert!(filler.iter().all(|byte| *byte == b' '));

        let _ = sender.send(());
        let document = next_frame(&mut body).await.unwrap();
        assert_eq!(
            document,
            Bytes::from_static(b"<CompleteMultipartUploadResult/>")
        );
        assert!(next_frame(&mut body).await.is_none());
    }

    // A failure after the head is reported as an XML error body, the way AWS
    // reports a late CompleteMultipartUpload failure.
    #[tokio::test]
    async fn reports_late_failure() {
        let handler: BoxFuture<'static, Result<HttpResponse, HttpError>> =
            Box::pin(async { Err(crate::s3::server::response::connection_error()) });
        assert_eq!(
            completion_body(handler).await,
            Bytes::from_static(KEEPALIVE_ERROR_BODY)
        );
    }

    #[test]
    fn keepalive_distinct_from_idle() {
        // The keepalive cadence is independent of the connection idle bound and
        // its filler is one full stream progress unit.
        assert_eq!(KEEPALIVE_AFTER, Duration::from_secs(5));
        assert_eq!(KEEPALIVE_INTERVAL, Duration::from_secs(10));
        assert_ne!(
            KEEPALIVE_INTERVAL,
            crate::s3::server::activity::CONNECTION_IDLE_TIMEOUT
        );
        assert_eq!(KEEPALIVE_FILLER.len(), STREAM_PROGRESS_BYTES);
    }
}
