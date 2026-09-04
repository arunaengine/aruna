use super::auth::AuthProvider;
use super::cors::{
    build_preflight_forbidden_response, build_preflight_response, inject_actual_cors_headers,
    match_actual_rule, match_preflight_rule, parse_requested_headers,
};
use super::s3_service::ArunaS3Service;
use crate::cors::CorsConfig;
use crate::error::S3ServerError;
use crate::rate_limit::{LocalKey, LocalLease};
use crate::s3::util::bucket_name_reason;
use crate::telemetry::{RequestCancelGuard, emit_request_completed, make_request_span};
use aruna_core::NodeId;
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::metrics::{NodeMetrics, RequestLabels, RouteLabels, method_label};
use aruna_core::structs::{BucketCorsConfiguration, RealmId, RoCrateLimits};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use bytes::Bytes;
use futures_core::future::BoxFuture;
use http::{Method, Request, StatusCode, header};
use hyper::body::Incoming;
use hyper::service::Service;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use percent_encoding::percent_decode_str;
use s3s::HttpError;
use s3s::HttpResponse;
use s3s::host::{S3Host, SingleDomain};
use s3s::s3_error;
use s3s::service::S3Service;
use s3s::service::S3ServiceBuilder;
use s3s::validation::AwsNameValidation;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{Instrument, error, info, trace, warn};

const INITIAL_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_secs(20);
const STREAM_LIFETIME_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const STREAM_PROGRESS_BYTES: usize = 1024;
/// A completion that answers faster than this is a plain response; only a slow
/// one streams, so small uploads are untouched.
const KEEPALIVE_AFTER: Duration = Duration::from_secs(5);
/// Cadence of the whitespace filler while a completion is still running.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// Whitespace between the prologue and the result. It is a full progress unit so
/// the response's own idle watch counts it and proxies flush it rather than
/// buffering single bytes.
const KEEPALIVE_FILLER: [u8; STREAM_PROGRESS_BYTES] = [b' '; STREAM_PROGRESS_BYTES];
/// A completion result is a handful of elements; anything larger is a bug.
const KEEPALIVE_BODY_LIMIT: usize = 64 * 1024;
const XML_PROLOGUE: &[u8] = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
const KEEPALIVE_ERROR_BODY: &[u8] =
    b"<Error><Code>InternalError</Code><Message>The multipart completion did not produce a result.</Message></Error>";
const DELETE_OBJECTS_MAX_BODY: usize = 2 * 1024 * 1024;
const DELETE_CAPTURE_LIMIT: usize = 16;
const EGRESS_LIMIT: usize = 256;
const CONTROL_EGRESS_LIMIT: usize = 64;
const CONTROL_REQUEST_LIMIT: usize = 64;

/// Concurrent S3 connections served at once; connections at capacity are
/// dropped so a flood cannot spawn unbounded connection tasks.
pub const DEFAULT_S3_MAX_CONNECTIONS: usize = 1_024;
/// Concurrent S3 requests processed at once, acquired before the expensive
/// s3s parse/body/storage work.
pub const DEFAULT_S3_MAX_CONCURRENT_REQUESTS: usize = 512;

/// Listener deadlines of the S3 plane: how long a connection may stay silent
/// before its first request, how long a request or response may make no I/O
/// progress, and the total lifetime of one streamed request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct S3ServerTimeouts {
    pub initial_request: Duration,
    pub connection_idle: Duration,
    pub stream_lifetime: Duration,
}

impl Default for S3ServerTimeouts {
    fn default() -> Self {
        Self {
            initial_request: INITIAL_REQUEST_TIMEOUT,
            connection_idle: CONNECTION_IDLE_TIMEOUT,
            stream_lifetime: STREAM_LIFETIME_TIMEOUT,
        }
    }
}

fn touch_frame(activity: &ConnectionActivity, frame: &hyper::body::Frame<hyper::body::Bytes>) {
    if let Some(data) = frame.data_ref().filter(|data| !data.is_empty()) {
        activity.record_progress(data.len());
    } else if frame.is_trailers() {
        activity.touch();
    }
}

/// Carries the resolved S3 operation name from the access check back to the
/// wrapper so request metrics can be labelled by operation. The wrapper inserts
/// it into the request extensions (which survive `s3s` routing) and keeps a
/// clone; [`crate::s3::auth::AuthProvider::check`] fills it in once the
/// operation is known.
#[derive(Clone)]
pub struct S3OpLabel(Arc<OnceLock<String>>);

#[derive(Default)]
struct DeleteObjectsState {
    bytes: Vec<u8>,
    exceeded: bool,
}

#[derive(Clone, Default)]
pub(crate) struct DeleteObjectsBody(Arc<Mutex<DeleteObjectsState>>);

impl DeleteObjectsBody {
    fn append(&self, data: &[u8]) -> usize {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let remaining = DELETE_OBJECTS_MAX_BODY.saturating_sub(state.bytes.len());
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

struct CaptureDeleteObjectsBody {
    inner: Pin<Box<Incoming>>,
    captured: DeleteObjectsBody,
    activity: Arc<ConnectionActivity>,
    connection_activity: Arc<ConnectionActivity>,
    ended: bool,
}

impl hyper::body::Body for CaptureDeleteObjectsBody {
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
            .is_none_or(|upper| upper > DELETE_OBJECTS_MAX_BODY as u64)
        {
            hint.set_upper(DELETE_OBJECTS_MAX_BODY as u64);
        }
        hint
    }
}

impl Drop for CaptureDeleteObjectsBody {
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

impl S3OpLabel {
    fn new() -> Self {
        Self(Arc::new(OnceLock::new()))
    }

    pub fn set(&self, name: &str) {
        let _ = self.0.set(name.to_string());
    }

    fn resolved(&self) -> &str {
        self.0.get().map_or("unknown", String::as_str)
    }
}

fn record_s3_request(
    metrics: &NodeMetrics,
    method: &Method,
    code: u16,
    op: &str,
    elapsed: Duration,
) {
    metrics
        .http_requests
        .get_or_create(&RequestLabels {
            interface: "s3",
            method: method_label(method.as_str()),
            code,
        })
        .inc();
    metrics
        .http_request_duration
        .get_or_create(&RouteLabels {
            interface: "s3",
            op: op.to_string(),
        })
        .observe(elapsed.as_secs_f64());
}

fn control_capacity(max_requests: usize) -> usize {
    let max_requests = max_requests.max(1);
    (max_requests / 4).clamp(1, CONTROL_REQUEST_LIMIT)
}

fn bulk_capacity(max_requests: usize) -> usize {
    let max_requests = max_requests.max(1);
    max_requests - control_capacity(max_requests)
}

fn query_has_any(uri: &http::Uri, names: &[&str]) -> bool {
    uri.query().is_some_and(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .any(|(key, _)| names.iter().any(|name| key.as_ref() == *name))
    })
}

fn query_value(uri: &http::Uri, name: &str, expected: &str) -> bool {
    uri.query().is_some_and(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .any(|(key, value)| key.as_ref() == name && value.as_ref() == expected)
    })
}

fn request_path(host: Option<&str>, path: &str, domain: &str) -> Option<s3s::path::S3Path> {
    let path = percent_decode_str(path).decode_utf8().ok()?;
    if let Some(host) = host
        && host.parse::<SocketAddr>().is_err()
        && host.parse::<IpAddr>().is_err()
    {
        let s3_host = SingleDomain::new(domain).ok()?;
        let virtual_host = s3_host.parse_host_header(host).ok()?;
        return s3s::path::parse_virtual_hosted_style(virtual_host.bucket(), path.as_ref()).ok();
    }

    s3s::path::parse_path_style(path.as_ref()).ok()
}

fn is_multipart(headers: &http::HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("multipart/form-data"))
}

fn is_bulk_request(
    method: &Method,
    host: Option<&str>,
    path: &str,
    domain: &str,
    uri: &http::Uri,
    headers: &http::HeaderMap,
) -> bool {
    // s3s resolves the operation later, so admission mirrors only data-heavy routes.
    let parsed_path = request_path(host, path, domain);
    let object = parsed_path
        .as_ref()
        .is_some_and(|path| path.as_object().is_some_and(|(_, key)| !key.is_empty()));
    let bucket = parsed_path
        .as_ref()
        .is_some_and(|path| path.as_bucket().is_some());
    let root = parsed_path.as_ref().is_some_and(s3s::path::S3Path::is_root);
    match method.as_str() {
        "GET" => {
            root || (bucket
                && !query_has_any(
                    uri,
                    &[
                        "analytics",
                        "intelligent-tiering",
                        "inventory",
                        "metrics",
                        "session",
                        "accelerate",
                        "acl",
                        "cors",
                        "encryption",
                        "lifecycle",
                        "location",
                        "logging",
                        "metadataTable",
                        "notification",
                        "ownershipControls",
                        "policy",
                        "policyStatus",
                        "replication",
                        "requestPayment",
                        "tagging",
                        "versioning",
                        "website",
                        "object-lock",
                        "publicAccessBlock",
                    ],
                ))
                || (object
                    && !query_has_any(
                        uri,
                        &[
                            "attributes",
                            "acl",
                            "legal-hold",
                            "retention",
                            "tagging",
                            "torrent",
                            "uploadId",
                        ],
                    ))
        }
        "PUT" => object && !query_has_any(uri, &["acl", "legal-hold", "retention", "tagging"]),
        "DELETE" => object && !query_has_any(uri, &["tagging"]),
        "POST" => {
            let select =
                object && query_has_any(uri, &["select"]) && query_value(uri, "select-type", "2");
            let control = if select {
                false
            } else if object {
                query_has_any(uri, &["uploads", "restore"])
            } else {
                query_has_any(uri, &["metadataTable"])
            };
            !control
                && (select
                    || (object && query_has_any(uri, &["uploadId"]))
                    || (bucket && query_has_any(uri, &["delete"]))
                    || ((object || bucket) && is_multipart(headers)))
        }
        _ => false,
    }
}

pub struct S3Server {
    address: String,
    s3service: S3Service,
    aruna_service: ArunaS3Service,
    realm_id: RealmId,
    node_id: NodeId,
    cors: CorsConfig,
    domain: String,
    driver_ctx: Arc<DriverContext>,
    metrics: Arc<NodeMetrics>,
    rate_limits: Arc<crate::rate_limit::ApiRateLimits>,
    encryption_key: CredentialEncryptionKey,
    connection_limit: Arc<Semaphore>,
    control_limit: Arc<Semaphore>,
    bulk_limit: Arc<Semaphore>,
    read_limit: Arc<Semaphore>,
    mutation_limit: Arc<Semaphore>,
    capture_limit: Arc<Semaphore>,
    trusted_proxies: Arc<Vec<ipnet::IpNet>>,
    timeouts: S3ServerTimeouts,
}

#[derive(Default)]
struct ConnectionActivity {
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
    fn with_idle(idle_timeout: Duration) -> Self {
        Self {
            idle_timeout: Some(idle_timeout),
            ..Self::default()
        }
    }

    fn touch(&self) {
        if !self.cancelled.load(Ordering::Acquire) && !self.stopped.load(Ordering::Acquire) {
            self.generation.fetch_add(1, Ordering::AcqRel);
            self.notify.notify_waiters();
        }
    }

    fn record_progress(&self, bytes: usize) {
        if bytes == 0
            || self.cancelled.load(Ordering::Acquire)
            || self.stopped.load(Ordering::Acquire)
        {
            return;
        }

        let mut progress = self.progress.load(Ordering::Acquire);
        loop {
            let total = progress.saturating_add(bytes);
            let next = if total >= STREAM_PROGRESS_BYTES {
                0
            } else {
                total
            };
            match self.progress.compare_exchange_weak(
                progress,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if total >= STREAM_PROGRESS_BYTES {
                        self.touch();
                    }
                    return;
                }
                Err(updated) => progress = updated,
            }
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.notify.notify_waiters();
    }

    fn mark_request(&self) {
        self.requested.store(true, Ordering::Release);
        self.touch();
    }

    fn begin_request(&self) {
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

    fn has_request(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    async fn wait_cancelled(&self) {
        while !self.is_cancelled() {
            let notified = self.notify.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }

    async fn wait_done(&self) {
        while !self.is_cancelled() && !self.stopped.load(Ordering::Acquire) {
            let notified = self.notify.notified();
            if self.is_cancelled() || self.stopped.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    async fn wait_idle(&self) -> bool {
        loop {
            if self.is_cancelled() || self.stopped.load(Ordering::Acquire) {
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
                        && !self.stopped.load(Ordering::Acquire)
                        && self.active.load(Ordering::Acquire) == 0
                    {
                        self.cancel();
                        return true;
                    }
                }
            }
        }
    }
}

struct ActiveRequestGuard {
    activity: Arc<ConnectionActivity>,
    deadline: Arc<ConnectionActivity>,
}

impl ActiveRequestGuard {
    fn new(activity: Arc<ConnectionActivity>, deadline: Arc<ConnectionActivity>) -> Self {
        Self { activity, deadline }
    }
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        self.activity.end_request();
        self.deadline.stop();
    }
}

/// Waits for the handler. `None` means a completion is still running after the
/// keepalive window, so the caller answers with a streamed body; the handler is
/// borrowed, not dropped, and keeps running behind it.
async fn await_handler(
    handler: &mut BoxFuture<'static, Result<HttpResponse, HttpError>>,
    keepalive: bool,
) -> Option<Result<HttpResponse, HttpError>> {
    if !keepalive {
        return Some(handler.await);
    }
    tokio::time::timeout(KEEPALIVE_AFTER, handler).await.ok()
}

enum HandlerOutcome {
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

fn keepalive_response(
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

struct ResponseBody {
    inner: Pin<Box<s3s::Body>>,
    permit: Option<OwnedSemaphorePermit>,
    activity: Arc<ConnectionActivity>,
    response_activity: Arc<ConnectionActivity>,
    cancellation: BoxFuture<'static, ()>,
    active: Option<ActiveRequestGuard>,
    lease: Option<LocalLease>,
    ended: bool,
}

impl ResponseBody {
    fn new(
        inner: s3s::Body,
        permit: Option<OwnedSemaphorePermit>,
        activity: Arc<ConnectionActivity>,
        response_activity: Arc<ConnectionActivity>,
        active: ActiveRequestGuard,
        lease: LocalLease,
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
            permit,
            activity,
            response_activity,
            cancellation,
            active: Some(active),
            lease: Some(lease),
            ended: false,
        }
    }

    fn finish(&mut self) {
        self.active.take();
        self.lease.take();
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
            self.permit.take();
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
            self.permit.take();
            self.response_activity.stop();
            return Poll::Ready(Some(Err(Box::new(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "S3 connection became idle",
            )))));
        }
        if self.response_activity.is_cancelled() {
            self.ended = true;
            self.finish();
            self.permit.take();
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
                self.permit.take();
                self.response_activity.stop();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                self.ended = true;
                self.finish();
                self.permit.take();
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

#[derive(Clone)]
pub struct WrappingService {
    shared: S3Service, // Aruna specific implementation of S3 trait
    cors: CorsConfig,
    domain: String,
    driver_ctx: Arc<DriverContext>,
    metrics: Arc<NodeMetrics>,
    // The accepted connection's peer, stamped into every request it carries.
    peer_ip: Option<std::net::IpAddr>,
    // Shared with the access hook: the IP bucket is charged here, the
    // per-principal bucket after authentication.
    rate_limits: Arc<crate::rate_limit::ApiRateLimits>,
    // Held while control request parsing and handler work are in progress.
    control_limit: Arc<Semaphore>,
    // Held while bulk request parsing and handler work are in progress.
    bulk_limit: Arc<Semaphore>,
    // Bulk responses use an independent lane from controls and metadata.
    read_limit: Arc<Semaphore>,
    // Control responses retain their bounded admission reserve.
    mutation_limit: Arc<Semaphore>,
    // Bounds concurrent DeleteObjects body aggregation.
    capture_limit: Arc<Semaphore>,
    // Cancels request and response futures when the connection has no I/O progress.
    activity: Option<Arc<ConnectionActivity>>,
    // Proxies whose forwarded client address may be charged instead of the peer.
    trusted_proxies: Arc<Vec<ipnet::IpNet>>,
    timeouts: S3ServerTimeouts,
}

fn build_s3_service(
    aruna_service: &ArunaS3Service,
    domain: &str,
    auth: AuthProvider,
) -> Result<S3Service, S3ServerError> {
    let mut builder = S3ServiceBuilder::new(aruna_service.clone());
    builder.set_host(SingleDomain::new(domain)?);
    builder.set_auth(auth.clone());
    builder.set_access(auth);
    builder.set_validation(AwsNameValidation::new());
    Ok(builder.build())
}

impl S3Server {
    #[tracing::instrument(
        level = "trace",
        skip(address, hostname, driver_ctx, encryption_key, metrics)
    )]
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        address: impl Into<String> + Copy,
        hostname: impl Into<String>,
        driver_ctx: Arc<DriverContext>,
        realm_id: RealmId,
        node_id: NodeId,
        encryption_key: CredentialEncryptionKey,
        rocrate_limits: RoCrateLimits,
        cors: CorsConfig,
        metrics: Arc<NodeMetrics>,
    ) -> Result<Self, S3ServerError> {
        let aruna_service = ArunaS3Service::new(driver_ctx.clone(), realm_id, node_id)
            .await
            .with_rocrate_limits(rocrate_limits);
        let hostname = hostname.into();

        let rate_limits = Arc::new(crate::rate_limit::ApiRateLimits::default());
        let service = build_s3_service(
            &aruna_service,
            &hostname,
            AuthProvider {
                driver_ctx: driver_ctx.clone(),
                realm_id,
                node_id,
                encryption_key: encryption_key.clone(),
                rate_limits: rate_limits.clone(),
            },
        )?;

        Ok(Self {
            address: address.into(),
            s3service: service,
            aruna_service,
            realm_id,
            node_id,
            cors,
            domain: hostname,
            driver_ctx,
            metrics,
            rate_limits,
            encryption_key,
            connection_limit: Arc::new(Semaphore::new(DEFAULT_S3_MAX_CONNECTIONS)),
            control_limit: Arc::new(Semaphore::new(control_capacity(
                DEFAULT_S3_MAX_CONCURRENT_REQUESTS,
            ))),
            bulk_limit: Arc::new(Semaphore::new(bulk_capacity(
                DEFAULT_S3_MAX_CONCURRENT_REQUESTS,
            ))),
            read_limit: Arc::new(Semaphore::new(
                bulk_capacity(DEFAULT_S3_MAX_CONCURRENT_REQUESTS).min(EGRESS_LIMIT),
            )),
            mutation_limit: Arc::new(Semaphore::new(
                control_capacity(DEFAULT_S3_MAX_CONCURRENT_REQUESTS).min(CONTROL_EGRESS_LIMIT),
            )),
            capture_limit: Arc::new(Semaphore::new(DELETE_CAPTURE_LIMIT)),
            trusted_proxies: Arc::new(Vec::new()),
            timeouts: S3ServerTimeouts::default(),
        })
    }

    /// Installs operator-configured listener deadlines; the defaults apply when
    /// this is not called.
    pub fn with_timeouts(mut self, timeouts: S3ServerTimeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Installs operator-configured concurrency ceilings; the control lane
    /// floors at one and the bulk lane receives the remaining capacity. A
    /// one-request budget therefore admits controls but not bulk data.
    pub fn with_concurrency_limits(mut self, max_connections: usize, max_requests: usize) -> Self {
        self.connection_limit = Arc::new(Semaphore::new(max_connections.max(1)));
        self.control_limit = Arc::new(Semaphore::new(control_capacity(max_requests)));
        self.bulk_limit = Arc::new(Semaphore::new(bulk_capacity(max_requests)));
        self.read_limit = Arc::new(Semaphore::new(
            bulk_capacity(max_requests).min(EGRESS_LIMIT),
        ));
        self.mutation_limit = Arc::new(Semaphore::new(
            control_capacity(max_requests).min(CONTROL_EGRESS_LIMIT),
        ));
        self.capture_limit = Arc::new(Semaphore::new(max_requests.clamp(1, DELETE_CAPTURE_LIMIT)));
        self
    }

    /// Installs operator-configured token-bucket quotas. Both the per-IP limiter
    /// on the transport boundary and the per-principal limiter in the access hook
    /// share the one Arc, so a rebuilt access hook applies the same quotas.
    pub fn with_rate_limits(
        mut self,
        limits: crate::rate_limit::ApiRateLimits,
    ) -> Result<Self, S3ServerError> {
        let rate_limits = Arc::new(limits);
        self.s3service = build_s3_service(
            &self.aruna_service,
            &self.domain,
            AuthProvider {
                driver_ctx: self.driver_ctx.clone(),
                realm_id: self.realm_id,
                node_id: self.node_id,
                encryption_key: self.encryption_key.clone(),
                rate_limits: rate_limits.clone(),
            },
        )?;
        self.rate_limits = rate_limits;
        Ok(self)
    }

    /// Installs the reverse proxies whose `x-forwarded-for` client address is
    /// charged instead of the transport peer, matching the REST limiter.
    pub fn with_trusted_proxies(mut self, proxies: Vec<ipnet::IpNet>) -> Self {
        self.trusted_proxies = Arc::new(proxies);
        self
    }

    /// Accepts until `shutdown` is cancelled. Connection tasks are tracked, so
    /// the returned handle only resolves once every request has finished.
    pub fn run_with_listener(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(SocketAddr, JoinHandle<()>), S3ServerError> {
        let local_addr = listener.local_addr()?;
        let connection_limit = self.connection_limit.clone();
        let timeouts = self.timeouts;
        let service = WrappingService {
            shared: self.s3service,
            cors: self.cors,
            domain: self.domain,
            driver_ctx: self.driver_ctx,
            metrics: self.metrics,
            peer_ip: None,
            rate_limits: self.rate_limits,
            control_limit: self.control_limit,
            bulk_limit: self.bulk_limit,
            read_limit: self.read_limit,
            mutation_limit: self.mutation_limit,
            capture_limit: self.capture_limit,
            activity: None,
            trusted_proxies: self.trusted_proxies,
            timeouts,
        };
        let mut connection = ConnBuilder::new(TokioExecutor::new()).http1_only();
        connection
            .http1()
            .timer(hyper_util::rt::TokioTimer::new())
            .header_read_timeout(timeouts.initial_request);
        let connections = TaskTracker::new();
        let abort_connections = CancellationToken::new();

        let server = async move {
            let _abort_connections = abort_connections.clone().drop_guard();
            loop {
                let (socket, peer) = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    accepted = listener.accept() => match accepted {
                        Ok(ok) => ok,
                        Err(err) => {
                            error!("error accepting connection: {err}");
                            continue;
                        }
                    },
                };
                // Bound concurrent connections without retaining sockets at capacity.
                let permit = match connection_limit.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(TryAcquireError::NoPermits) => {
                        drop(socket);
                        continue;
                    }
                    Err(TryAcquireError::Closed) => break,
                };
                let mut service = service.clone();
                service.peer_ip = Some(peer.ip());
                let activity = Arc::new(ConnectionActivity::with_idle(timeouts.connection_idle));
                service.activity = Some(activity.clone());
                let builder = connection.clone();
                let connection_shutdown = shutdown.clone();
                let connection_abort = abort_connections.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    let conn = builder.serve_connection(TokioIo::new(socket), service);
                    let mut conn = std::pin::pin!(conn);
                    tokio::select! {
                        biased;
                        _ = connection_abort.cancelled() => {}
                        _ = run_connection(activity.clone(), conn.as_mut(), timeouts.initial_request) => {}
                        _ = connection_shutdown.cancelled() => {
                            // Finish the request being served, then close the
                            // connection instead of waiting out its keep-alive.
                            conn.as_mut().graceful_shutdown();
                            tokio::select! {
                                biased;
                                _ = connection_abort.cancelled() => {}
                                _ = run_connection(activity, conn.as_mut(), timeouts.initial_request) => {}
                            }
                        }
                    }
                });
            }

            connections.close();
            let in_flight = connections.len();
            if in_flight > 0 {
                info!(in_flight, "Draining in-flight S3 connections");
            }
            connections.wait().await;
        };

        let task = tokio::spawn(server);
        info!("server is running at http://{local_addr}");

        Ok((local_addr, task))
    }

    #[tracing::instrument(level = "trace", skip(self, shutdown))]
    pub async fn run(self, shutdown: CancellationToken) -> Result<JoinHandle<()>, S3ServerError> {
        let listener = TcpListener::bind(&self.address).await?;
        let (_, task) = self.run_with_listener(listener, shutdown)?;
        Ok(task)
    }
}

impl Service<Request<Incoming>> for WrappingService {
    type Response = HttpResponse;

    type Error = HttpError;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let (mut parts, body) = req.into_parts();
        let method = parts.method.clone();
        let path = parts.uri.path().to_string();
        // The access check fills this in with the resolved operation name; it
        // reaches the check through the request extensions.
        let op_label = S3OpLabel::new();
        parts.extensions.insert(op_label.clone());
        let span = make_request_span("s3", &parts.headers, &method, &path);
        let started = Instant::now();
        {
            let _guard = span.enter();
            trace!(
                event = "request.received",
                protocol = "s3",
                method = %method,
                path = %path,
                "Received S3 request"
            );
        }
        let host = parts
            .headers
            .get(header::HOST)
            .and_then(|value| value.to_str().ok());
        let bucket = extract_bucket_name(host, &path, &self.domain);
        // s3s rejects a malformed bucket at path parse without a message, which
        // reaches clients as "UnknownError"; answer the violated rule instead.
        let invalid_bucket = bucket.as_deref().and_then(bucket_name_reason);
        let origin_header = parts.headers.get(header::ORIGIN).cloned();
        let origin = origin_header
            .as_ref()
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let requested_method = parts
            .headers
            .get(header::ACCESS_CONTROL_REQUEST_METHOD)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let requested_headers_value = parts
            .headers
            .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
            .cloned();
        let requested_headers = requested_headers_value
            .as_ref()
            .and_then(|value| value.to_str().ok())
            .map(parse_requested_headers)
            .unwrap_or_default();
        let delete_objects = method == Method::POST
            && parts.uri.query().is_some_and(|query| {
                url::form_urlencoded::parse(query.as_bytes()).any(|(name, _)| name == "delete")
            });
        let oversized_delete = delete_objects
            && parts
                .headers
                .get(header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .is_some_and(|length| length > DELETE_OBJECTS_MAX_BODY as u64);
        // A completion is the one S3 call that can legitimately run for minutes,
        // and its client sends no bytes while it waits.
        let complete_multipart = method == Method::POST && query_has_any(&parts.uri, &["uploadId"]);
        let bulk_request = is_bulk_request(
            &method,
            host,
            &path,
            &self.domain,
            &parts.uri,
            &parts.headers,
        );
        let captured = (!oversized_delete && delete_objects).then(DeleteObjectsBody::default);
        let timeouts = self.timeouts;
        let stream_activity = Arc::new(ConnectionActivity::with_idle(timeouts.connection_idle));
        let connection_activity = self
            .activity
            .clone()
            .unwrap_or_else(|| Arc::new(ConnectionActivity::with_idle(timeouts.connection_idle)));
        connection_activity.mark_request();
        parts.extensions.insert(connection_activity.clone());
        parts.extensions.insert(stream_activity.clone());
        let body_end = hyper::body::Body::is_end_stream(&body);
        let body = if delete_objects {
            let captured = captured.clone().unwrap_or_default();
            parts.extensions.insert(captured.clone());
            s3s::Body::http_body_unsync(CaptureDeleteObjectsBody {
                inner: Box::pin(body),
                captured,
                activity: stream_activity.clone(),
                connection_activity: connection_activity.clone(),
                ended: false,
            })
        } else {
            s3s::Body::http_body_unsync(TrackRequestBody {
                inner: Box::pin(body),
                activity: stream_activity.clone(),
                connection_activity: connection_activity.clone(),
                ended: false,
            })
        };
        // Resolved before the parts move into the s3s request: behind a trusted
        // proxy the forwarded client is charged, never the shared proxy address.
        let charged_ip = self
            .peer_ip
            .map(|peer| crate::forwarded::client_ip(&self.trusted_proxies, peer, &parts.headers));
        let mut s3s_request = s3s::HttpRequest::from_parts(parts, body);
        let shared = self.shared.clone();
        let cors = self.cors.clone();
        let driver_ctx = self.driver_ctx.clone();
        let metrics = self.metrics.clone();
        let rate_limits = self.rate_limits.clone();
        let admission_limit = if bulk_request {
            self.bulk_limit.clone()
        } else {
            self.control_limit.clone()
        };
        // Bulk responses use an independent lane from controls and metadata.
        let egress_limit = if bulk_request {
            self.read_limit.clone()
        } else {
            self.mutation_limit.clone()
        };
        let capture_limit = self.capture_limit.clone();
        let activity = connection_activity;
        let mut cancel_guard = RequestCancelGuard::new(span.clone(), &method, &path);
        let inner = Box::pin(async move {
            // Charge the transport IP before CORS, signature checks, credential
            // reads, or body handling, so unsigned or invalid requests cannot
            // bypass admission.
            if let Some(charged_ip) = charged_ip
                && let Err(retry_after) = rate_limits.check_ip(charged_ip)
            {
                let response = slow_down_response(retry_after);
                let code = response.status().as_u16();
                emit_request_completed(&span, "s3", code, started);
                record_s3_request(&metrics, &method, code, "rate_limited", started.elapsed());
                return Ok(response);
            }
            // Bound concurrent request processing before the expensive s3s
            // parse, body handling, and storage work.
            let request_permit = match admission_limit.try_acquire_owned() {
                Ok(permit) => permit,
                Err(TryAcquireError::NoPermits | TryAcquireError::Closed) => {
                    // Dropping the request lets the transport drain or close
                    // bodies without desynchronizing the next request.
                    drop(s3s_request);
                    let response = slow_down_response(1);
                    let code = response.status().as_u16();
                    emit_request_completed(&span, "s3", code, started);
                    record_s3_request(
                        &metrics,
                        &method,
                        code,
                        "admission_limited",
                        started.elapsed(),
                    );
                    return Ok(response);
                }
            };
            if oversized_delete {
                drop(s3s_request);
                let response = oversized_delete_response()?;
                let code = response.status().as_u16();
                emit_request_completed(&span, "s3", code, started);
                record_s3_request(&metrics, &method, code, "body_limited", started.elapsed());
                return Ok(response);
            }
            let local_lease = LocalLease::default();
            if let Some(charged_ip) = charged_ip {
                let permit = match rate_limits.try_acquire_local(LocalKey::Ip(charged_ip)) {
                    Some(permit) => permit,
                    None => {
                        drop(request_permit);
                        drop(s3s_request);
                        let response = slow_down_response(1);
                        let code = response.status().as_u16();
                        emit_request_completed(&span, "s3", code, started);
                        record_s3_request(
                            &metrics,
                            &method,
                            code,
                            "local_limited",
                            started.elapsed(),
                        );
                        return Ok(response);
                    }
                };
                if !local_lease.hold(permit) {
                    drop(request_permit);
                    drop(s3s_request);
                    let response = slow_down_response(1);
                    let code = response.status().as_u16();
                    emit_request_completed(&span, "s3", code, started);
                    record_s3_request(&metrics, &method, code, "local_limited", started.elapsed());
                    return Ok(response);
                }
            }
            s3s_request.extensions_mut().insert(local_lease.clone());
            activity.begin_request();
            let deadline_activity = Arc::new(ConnectionActivity::default());
            let stream_lifetime = timeouts.stream_lifetime;
            let deadline = tokio::time::Instant::now() + stream_lifetime;
            let deadline_task_activity = deadline_activity.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = deadline_task_activity.wait_done() => {}
                    _ = tokio::time::sleep_until(deadline) => {
                        if !deadline_task_activity.is_cancelled()
                            && !deadline_task_activity.stopped.load(Ordering::Acquire)
                        {
                            // The cancelled request returns without a completion
                            // record, so this is its only trace.
                            warn!(
                                event = "s3.request.lifetime_expired",
                                timeout_ms = stream_lifetime.as_millis() as u64,
                                "Cancelling an S3 request that outlived its stream lifetime"
                            );
                            deadline_task_activity.cancel();
                        }
                    }
                }
            });
            let active_guard = ActiveRequestGuard::new(activity.clone(), deadline_activity.clone());
            let bucket_cors = if origin_header.is_some() {
                let cors_result = tokio::select! {
                    result = load_bucket_cors_config(driver_ctx, bucket) => result,
                    _ = activity.wait_cancelled() => {
                        drop(request_permit);
                        drop(s3s_request);
                        return Err(connection_error());
                    }
                    _ = deadline_activity.wait_cancelled() => {
                        drop(request_permit);
                        drop(s3s_request);
                        return stream_timeout_response();
                    }
                };
                match cors_result {
                    Ok(bucket_cors) => bucket_cors,
                    Err(error) => {
                        span.record("status_code", 500);
                        record_s3_request(&metrics, &method, 500, "unknown", started.elapsed());
                        let _guard = span.enter();
                        error!(
                            event = "request.failed",
                            protocol = "s3",
                            latency_ms = started.elapsed().as_millis() as u64,
                            error = ?error,
                            "Failed to query bucket CORS configuration"
                        );
                        return Err(HttpError::new(error.into()));
                    }
                }
            } else {
                None
            };

            if deadline_activity.is_cancelled() {
                drop(request_permit);
                drop(s3s_request);
                return stream_timeout_response();
            }

            // Answer CORS preflight before s3s signature validation: an unsigned
            // OPTIONS request must not fail with 403.
            if method == Method::OPTIONS
                && let Some(origin_header) = origin_header.as_ref()
            {
                let bucket_rule = bucket_cors.as_ref().and_then(|config| {
                    requested_method.as_deref().and_then(|requested_method| {
                        match_preflight_rule(
                            config,
                            origin.as_deref().unwrap_or_default(),
                            requested_method,
                            &requested_headers,
                        )
                    })
                });
                // A stored bucket configuration EXTENDS cross-origin access for
                // additional origins; the node's own allowlist (its portals)
                // stays authoritative. Without this fallback a bucket whose
                // stored rules omit e.g. PUT locks the portal out of every
                // write on it - including the PutBucketCors call that would
                // repair the stored rules.
                let response = if let Some(matched_rule) = bucket_rule {
                    build_preflight_response(matched_rule)
                } else if let Some(cors_headers) =
                    cors.s3_preflight_headers(origin_header, requested_headers_value.as_ref())
                {
                    let mut response = http::Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .body(s3s::Body::empty())
                        .expect("static response must build");
                    response.headers_mut().extend(cors_headers);
                    response
                } else if bucket_cors.is_some() {
                    build_preflight_forbidden_response()
                } else {
                    http::Response::builder()
                        .status(StatusCode::NO_CONTENT)
                        .body(s3s::Body::empty())
                        .expect("static response must build")
                };
                let code = response.status().as_u16();
                emit_request_completed(&span, "s3", code, started);
                record_s3_request(&metrics, &method, code, "cors_preflight", started.elapsed());
                return Ok(response);
            }

            if let Some(reason) = invalid_bucket {
                drop(request_permit);
                drop(s3s_request);
                let response = invalid_bucket_response(reason)?;
                let code = response.status().as_u16();
                emit_request_completed(&span, "s3", code, started);
                record_s3_request(&metrics, &method, code, "invalid_bucket", started.elapsed());
                return Ok(response);
            }

            let mut capture_permit = if delete_objects {
                match capture_limit.try_acquire_owned() {
                    Ok(permit) => Some(permit),
                    Err(TryAcquireError::NoPermits | TryAcquireError::Closed) => {
                        drop(request_permit);
                        drop(s3s_request);
                        let response = slow_down_response(1);
                        let code = response.status().as_u16();
                        emit_request_completed(&span, "s3", code, started);
                        record_s3_request(
                            &metrics,
                            &method,
                            code,
                            "capture_limited",
                            started.elapsed(),
                        );
                        return Ok(response);
                    }
                }
            } else {
                None
            };
            if body_end {
                stream_activity.stop();
            } else {
                stream_activity.touch();
                let idle_task_activity = stream_activity.clone();
                tokio::spawn(async move {
                    idle_task_activity.wait_idle().await;
                });
            }

            let mut handler: BoxFuture<'static, Result<HttpResponse, HttpError>> =
                Box::pin(async move { shared.call(s3s_request).await }.instrument(span.clone()));
            let outcome = tokio::select! {
                result = await_handler(&mut handler, complete_multipart) => match result {
                    Some(result) => HandlerOutcome::Done(result),
                    None => HandlerOutcome::Keepalive,
                },
                _ = activity.wait_cancelled() => HandlerOutcome::Aborted,
                _ = deadline_activity.wait_cancelled() => HandlerOutcome::TimedOut,
                _ = stream_activity.wait_cancelled() => HandlerOutcome::TimedOut,
            };
            let result = match outcome {
                HandlerOutcome::Done(result) => result,
                // AWS answers a slow CompleteMultipartUpload the same way: the
                // 200 head first, whitespace while it works, the XML last.
                HandlerOutcome::Keepalive => Ok(keepalive_response(handler)),
                HandlerOutcome::Aborted => {
                    drop(request_permit);
                    drop(capture_permit.take());
                    stream_activity.stop();
                    return Err(connection_error());
                }
                HandlerOutcome::TimedOut => {
                    drop(request_permit);
                    drop(capture_permit.take());
                    stream_activity.stop();
                    return stream_timeout_response();
                }
            };
            if deadline_activity.is_cancelled() {
                drop(request_permit);
                drop(capture_permit.take());
                stream_activity.stop();
                return stream_timeout_response();
            }
            if stream_activity.is_cancelled() {
                drop(request_permit);
                drop(capture_permit.take());
                stream_activity.stop();
                return stream_timeout_response();
            }
            let mut result = match result {
                Ok(response) => {
                    let egress_permit = match egress_limit.try_acquire_owned() {
                        Ok(permit) => Some(permit),
                        Err(TryAcquireError::NoPermits | TryAcquireError::Closed)
                            if method != Method::GET && method != Method::HEAD =>
                        {
                            // The handler already ran: a durable mutation's small
                            // ack must not be dropped for a streaming permit.
                            None
                        }
                        Err(TryAcquireError::NoPermits | TryAcquireError::Closed) => {
                            drop(response);
                            drop(request_permit);
                            drop(capture_permit.take());
                            drop(local_lease);
                            stream_activity.stop();
                            drop(active_guard);
                            let response = slow_down_response(1);
                            let code = response.status().as_u16();
                            emit_request_completed(&span, "s3", code, started);
                            record_s3_request(
                                &metrics,
                                &method,
                                code,
                                "egress_limited",
                                started.elapsed(),
                            );
                            return Ok(response);
                        }
                    };
                    drop(request_permit);
                    drop(capture_permit.take());
                    stream_activity.stop();
                    activity.touch();
                    let response_activity =
                        Arc::new(ConnectionActivity::with_idle(timeouts.connection_idle));
                    response_activity.touch();
                    let idle_task_activity = response_activity.clone();
                    let response_connection = activity.clone();
                    let response_deadline = deadline_activity.clone();
                    tokio::spawn(async move {
                        tokio::select! {
                            idle = idle_task_activity.wait_idle() => {
                                if idle {
                                    response_connection.cancel();
                                }
                            }
                            _ = response_deadline.wait_done() => {
                                if response_deadline.is_cancelled() {
                                    response_connection.cancel();
                                }
                            }
                        }
                    });
                    Ok(response.map(|body| {
                        s3s::Body::http_body_unsync(ResponseBody::new(
                            body,
                            egress_permit,
                            activity.clone(),
                            response_activity,
                            active_guard,
                            local_lease,
                        ))
                    }))
                }
                Err(error) => {
                    drop(request_permit);
                    drop(capture_permit.take());
                    drop(local_lease);
                    stream_activity.stop();
                    Err(error)
                }
            };
            if captured.as_ref().is_some_and(DeleteObjectsBody::exceeded) {
                stream_activity.stop();
                result = oversized_delete_response();
            }
            if let Ok(response) = &mut result {
                let bucket_rule = bucket_cors.as_ref().and_then(|config| {
                    origin
                        .as_deref()
                        .and_then(|origin| match_actual_rule(config, origin, &method))
                });
                if let Some(matched_rule) = bucket_rule {
                    inject_actual_cors_headers(response, matched_rule);
                } else {
                    // Same fallback as the preflight: allowlisted origins keep
                    // readable responses on buckets whose stored rules do not
                    // cover this request.
                    cors.apply_s3_response_headers(origin_header.as_ref(), response.headers_mut());
                }
            }
            let op = op_label.resolved();
            match &result {
                Ok(response) => {
                    let code = response.status().as_u16();
                    emit_request_completed(&span, "s3", code, started);
                    record_s3_request(&metrics, &method, code, op, started.elapsed());
                }
                Err(error) => {
                    span.record("status_code", 500);
                    record_s3_request(&metrics, &method, 500, op, started.elapsed());
                    let _guard = span.enter();
                    error!(
                        event = "request.failed",
                        protocol = "s3",
                        latency_ms = started.elapsed().as_millis() as u64,
                        error = ?error,
                        "S3 request failed"
                    );
                }
            }
            result
        });
        // A request future dropped mid-flight leaves no completion record, which
        // is exactly how a cancelled completion used to go unnoticed.
        Box::pin(async move {
            let result = inner.await;
            cancel_guard.disarm();
            result
        })
    }
}

async fn run_connection<F>(activity: Arc<ConnectionActivity>, connection: F, initial: Duration)
where
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

fn connection_error() -> HttpError {
    HttpError::new(Box::new(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "S3 connection became idle",
    )))
}

fn stream_timeout_response() -> Result<HttpResponse, HttpError> {
    s3_error!(RequestTimeout, "S3 request made no progress")
        .to_http_response()
        .map_err(|error| HttpError::new(Box::new(error)))
}

fn oversized_delete_response() -> Result<HttpResponse, HttpError> {
    s3_error!(
        MaxMessageLengthExceeded,
        "DeleteObjects request body exceeds 2 MiB"
    )
    .to_http_response()
    .map_err(|error| HttpError::new(Box::new(error)))
}

fn invalid_bucket_response(reason: &'static str) -> Result<HttpResponse, HttpError> {
    s3_error!(InvalidBucketName, "{}", reason)
        .to_http_response()
        .map_err(|error| HttpError::new(Box::new(error)))
}

fn slow_down_response(retry_after: u64) -> HttpResponse {
    const SLOW_DOWN_BODY: &[u8] = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SlowDown</Code><Message>Reduce your request rate.</Message></Error>";
    let mut response = http::Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .body(s3s::Body::from(SLOW_DOWN_BODY.to_vec()))
        .expect("static response must build");
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/xml"),
    );
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, header::HeaderValue::from(retry_after));
    response
}

fn extract_bucket_name(host: Option<&str>, path: &str, domain: &str) -> Option<String> {
    if let Some(host) = host
        && let Some(bucket) = virtual_hosted_bucket(host, domain)
    {
        return Some(bucket);
    }

    path.trim_start_matches('/')
        .split('/')
        .find(|segment| !segment.is_empty())
        .map(str::to_owned)
}

fn virtual_hosted_bucket(host: &str, domain: &str) -> Option<String> {
    let host = host.split(':').next().unwrap_or(host);
    let domain = domain.split(':').next().unwrap_or(domain);
    let prefix = host.strip_suffix(domain)?.strip_suffix('.')?;
    (!prefix.is_empty()).then(|| prefix.to_owned())
}

async fn load_bucket_cors_config(
    driver_ctx: Arc<DriverContext>,
    bucket: Option<String>,
) -> Result<Option<BucketCorsConfiguration>, GetBucketInfoError> {
    let Some(bucket) = bucket else {
        return Ok(None);
    };

    match drive(GetBucketInfoOperation::new(bucket), driver_ctx.as_ref())
        .await
        .and_then(|result| result.transpose())
    {
        Ok(Some(bucket_info)) => Ok(bucket_info.cors_configuration),
        Ok(None) | Err(GetBucketInfoError::NotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slow_handler(
        ready: tokio::sync::oneshot::Receiver<()>,
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
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let mut slow = slow_handler(receiver);
        assert!(await_handler(&mut slow, true).await.is_none());

        let _ = sender.send(());
        assert!(await_handler(&mut slow, true).await.is_some());

        let (sender, receiver) = tokio::sync::oneshot::channel();
        let _ = sender.send(());
        let mut fast = slow_handler(receiver);
        assert!(await_handler(&mut fast, true).await.is_some());
    }

    // The stream is the XML prologue, whitespace while the completion runs, then
    // the document without a second prologue.
    #[tokio::test(start_paused = true)]
    async fn keepalive_streams_filler() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
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
            Box::pin(async { Err(connection_error()) });
        assert_eq!(
            completion_body(handler).await,
            Bytes::from_static(KEEPALIVE_ERROR_BODY)
        );
    }

    #[test]
    fn refuses_full_connection() {
        let limit = Arc::new(Semaphore::new(1));
        let permit = limit.clone().try_acquire_owned().expect("first permit");
        assert!(matches!(
            limit.clone().try_acquire_owned(),
            Err(TryAcquireError::NoPermits)
        ));
        drop(permit);
        limit.close();
        assert!(matches!(
            limit.try_acquire_owned(),
            Err(TryAcquireError::Closed)
        ));
    }

    #[test]
    fn slows_full_request() {
        let limit = Arc::new(Semaphore::new(1));
        let permit = limit.clone().try_acquire_owned().expect("first permit");
        let response = match limit.clone().try_acquire_owned() {
            Ok(_) => panic!("request unexpectedly admitted"),
            Err(TryAcquireError::NoPermits | TryAcquireError::Closed) => slow_down_response(1),
        };
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
        drop(permit);
    }

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

    #[test]
    fn default_timeouts_unchanged() {
        // Making the deadlines configurable must not move the shipped defaults.
        let timeouts = S3ServerTimeouts::default();
        assert_eq!(timeouts.initial_request, Duration::from_secs(10));
        assert_eq!(timeouts.connection_idle, Duration::from_secs(20));
        assert_eq!(timeouts.stream_lifetime, Duration::from_secs(30 * 60));
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
            Some(permit),
            activity.clone(),
            response_activity.clone(),
            active,
            LocalLease::default(),
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
        tokio::time::advance(CONNECTION_IDLE_TIMEOUT).await;
        watcher.await.expect("response watcher joins");
        assert!(activity.is_cancelled());
        connection.await.expect("connection task joins");
        assert_eq!(limit.available_permits(), 1);
        assert_eq!(activity.active.load(Ordering::Acquire), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn request_deadline_expires() {
        let activity = Arc::new(ConnectionActivity::default());
        activity.mark_request();
        activity.begin_request();
        let deadline_activity = Arc::new(ConnectionActivity::default());
        let deadline = tokio::time::Instant::now() + STREAM_LIFETIME_TIMEOUT;
        let timer_activity = deadline_activity.clone();
        let ready = Arc::new(Notify::new());
        let timer_ready = ready.clone();
        let timer = tokio::spawn(async move {
            timer_ready.notify_one();
            tokio::select! {
                _ = timer_activity.wait_done() => {}
                _ = tokio::time::sleep_until(deadline) => timer_activity.cancel(),
            }
        });
        let stream_activity = Arc::new(ConnectionActivity::default());
        stream_activity.touch();
        let idle_activity = stream_activity.clone();
        let idle = tokio::spawn(async move {
            idle_activity.wait_idle().await;
        });
        let control_limit = Arc::new(Semaphore::new(1));
        let control_permit = control_limit
            .clone()
            .try_acquire_owned()
            .expect("control permit");
        let egress_limit = Arc::new(Semaphore::new(1));
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
        ready.notified().await;
        tokio::task::yield_now().await;
        let tick = Duration::from_secs(19);
        for _ in 0..94 {
            tokio::time::advance(tick).await;
            stream_activity.touch();
            tokio::task::yield_now().await;
        }
        assert!(!stream_activity.is_cancelled());
        tokio::time::advance(STREAM_LIFETIME_TIMEOUT - Duration::from_secs(19 * 94)).await;
        timer.await.expect("deadline timer joins");
        idle.await.expect("idle watcher joins");
        request.await.expect("request task joins");
        assert!(deadline_activity.is_cancelled());
        assert!(stream_activity.stopped.load(Ordering::Acquire));
        assert_eq!(control_limit.available_permits(), 1);
        assert_eq!(egress_limit.available_permits(), 1);
        assert_eq!(activity.active.load(Ordering::Acquire), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn response_deadline_expires() {
        let activity = Arc::new(ConnectionActivity::default());
        activity.mark_request();
        activity.begin_request();
        let deadline_activity = Arc::new(ConnectionActivity::default());
        let deadline = tokio::time::Instant::now() + STREAM_LIFETIME_TIMEOUT;
        let timer_activity = deadline_activity.clone();
        let ready = Arc::new(Notify::new());
        let timer_ready = ready.clone();
        let timer = tokio::spawn(async move {
            timer_ready.notify_one();
            tokio::select! {
                _ = timer_activity.wait_done() => {}
                _ = tokio::time::sleep_until(deadline) => timer_activity.cancel(),
            }
        });
        let response_activity = Arc::new(ConnectionActivity::default());
        response_activity.touch();
        let limit = Arc::new(Semaphore::new(1));
        let permit = limit.clone().try_acquire_owned().expect("permit");
        let active = ActiveRequestGuard::new(activity.clone(), deadline_activity.clone());
        let body = ResponseBody::new(
            s3s::Body::empty(),
            Some(permit),
            activity.clone(),
            response_activity.clone(),
            active,
            LocalLease::default(),
        );
        let sibling = Arc::new(ConnectionActivity::default());
        sibling.mark_request();
        let idle_activity = response_activity.clone();
        let idle_connection = activity.clone();
        let response_deadline = deadline_activity.clone();
        let watcher = tokio::spawn(async move {
            tokio::select! {
                idle = idle_activity.wait_idle() => {
                    if idle {
                        idle_connection.cancel();
                    }
                }
                _ = response_deadline.wait_done() => {
                    if response_deadline.is_cancelled() {
                        idle_connection.cancel();
                    }
                }
            }
        });
        let connection_activity = activity.clone();
        let connection = tokio::spawn(async move {
            let _body = body;
            connection_activity.wait_cancelled().await;
        });
        ready.notified().await;
        tokio::task::yield_now().await;
        let tick = Duration::from_secs(19);
        for _ in 0..94 {
            tokio::time::advance(tick).await;
            response_activity.touch();
            tokio::task::yield_now().await;
        }
        assert!(!response_activity.is_cancelled());
        assert!(!activity.is_cancelled());
        tokio::time::advance(STREAM_LIFETIME_TIMEOUT - Duration::from_secs(19 * 94)).await;
        timer.await.expect("deadline timer joins");
        watcher.await.expect("response watcher joins");
        connection.await.expect("connection task joins");
        assert!(activity.is_cancelled());
        assert!(!sibling.is_cancelled());
        assert_eq!(limit.available_permits(), 1);
        assert_eq!(activity.active.load(Ordering::Acquire), 0);
    }

    #[test]
    fn caps_delete_body() {
        let body = DeleteObjectsBody::default();
        assert_eq!(
            body.append(&vec![0; DELETE_OBJECTS_MAX_BODY]),
            DELETE_OBJECTS_MAX_BODY
        );
        assert_eq!(body.append(b"overflow"), 0);
        assert!(body.exceeded());
        assert_eq!(body.take_bytes().len(), DELETE_OBJECTS_MAX_BODY);
    }

    #[test]
    fn holds_response_permit() {
        let limit = Arc::new(Semaphore::new(1));
        let permit = limit.clone().try_acquire_owned().expect("permit");
        let local_limit = Arc::new(Semaphore::new(1));
        let local_permit = crate::rate_limit::LocalPermit::test_permit(local_limit.clone());
        let lease = LocalLease::default();
        assert!(lease.hold(local_permit));
        let activity = Arc::new(ConnectionActivity::default());
        activity.begin_request();
        let deadline = Arc::new(ConnectionActivity::default());
        let active = ActiveRequestGuard::new(activity.clone(), deadline);
        let stream_activity = Arc::new(ConnectionActivity::default());
        let body = ResponseBody::new(
            s3s::Body::empty(),
            Some(permit),
            activity.clone(),
            stream_activity,
            active,
            lease,
        );
        assert_eq!(limit.available_permits(), 0);
        assert_eq!(local_limit.available_permits(), 0);
        assert_eq!(activity.active.load(Ordering::Acquire), 1);
        drop(body);
        assert_eq!(limit.available_permits(), 1);
        assert_eq!(local_limit.available_permits(), 1);
        assert_eq!(activity.active.load(Ordering::Acquire), 0);
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
        assert!(deadline.stopped.load(Ordering::Acquire));
    }

    #[test]
    fn releases_request_permit() {
        let request_limit = Arc::new(Semaphore::new(1));
        let egress_limit = Arc::new(Semaphore::new(1));
        let request_permit = request_limit
            .clone()
            .try_acquire_owned()
            .expect("request permit");
        let egress_permit = egress_limit
            .clone()
            .try_acquire_owned()
            .expect("egress permit");
        drop(request_permit);
        assert_eq!(request_limit.available_permits(), 1);
        assert_eq!(egress_limit.available_permits(), 0);
        drop(egress_permit);
    }

    #[test]
    fn reserves_control_lane() {
        for max_requests in [0, 1, 2, 128] {
            let normalized = max_requests.max(1);
            assert_eq!(
                control_capacity(max_requests) + bulk_capacity(max_requests),
                normalized
            );
            assert!(
                control_capacity(max_requests).min(CONTROL_EGRESS_LIMIT)
                    + bulk_capacity(max_requests).min(EGRESS_LIMIT)
                    <= normalized
            );
            assert!(control_capacity(max_requests) >= 1);
        }
        assert_eq!(bulk_capacity(1), 0);
        assert_eq!(bulk_capacity(2), 1);

        let control = Arc::new(Semaphore::new(control_capacity(4)));
        let bulk = Arc::new(Semaphore::new(bulk_capacity(4)));
        let bulk_permits = (0..bulk_capacity(4))
            .map(|_| bulk.clone().try_acquire_owned().expect("bulk permit"))
            .collect::<Vec<_>>();
        assert!(bulk.clone().try_acquire_owned().is_err());
        let control_permit = control.clone().try_acquire_owned().expect("control permit");
        assert_eq!(control.available_permits(), control_capacity(4) - 1);
        drop(control_permit);
        assert_eq!(control.available_permits(), control_capacity(4));
        drop(bulk_permits);
        assert_eq!(bulk.available_permits(), bulk_capacity(4));

        let control_egress = Arc::new(Semaphore::new(
            control_capacity(4).min(CONTROL_EGRESS_LIMIT),
        ));
        let bulk_egress = Arc::new(Semaphore::new(bulk_capacity(4).min(EGRESS_LIMIT)));
        let bulk_egress_permits = (0..bulk_capacity(4).min(EGRESS_LIMIT))
            .map(|_| {
                bulk_egress
                    .clone()
                    .try_acquire_owned()
                    .expect("bulk egress permit")
            })
            .collect::<Vec<_>>();
        assert!(bulk_egress.clone().try_acquire_owned().is_err());
        let control_egress_permit = control_egress
            .clone()
            .try_acquire_owned()
            .expect("control egress permit");
        drop(control_egress_permit);
        drop(bulk_egress_permits);
    }

    #[test]
    fn classifies_bulk_routes() {
        let headers = http::HeaderMap::new();
        assert!(is_bulk_request(
            &Method::GET,
            None,
            "/",
            "s3.example",
            &http::Uri::from_static("/"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::GET,
            None,
            "/bucket",
            "s3.example",
            &http::Uri::from_static("/bucket"),
            &headers,
        ));
        for query in ["list-type=2", "versions", "uploads"] {
            let uri = format!("/bucket?{query}").parse().expect("list URI");
            assert!(is_bulk_request(
                &Method::GET,
                None,
                "/bucket",
                "s3.example",
                &uri,
                &headers,
            ));
        }
        for query in ["location", "replication", "versioning"] {
            let uri = format!("/bucket?{query}").parse().expect("config URI");
            assert!(!is_bulk_request(
                &Method::GET,
                None,
                "/bucket",
                "s3.example",
                &uri,
                &headers,
            ));
        }
        assert!(is_bulk_request(
            &Method::PUT,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::PUT,
            None,
            "/bucket/%2F",
            "s3.example",
            &http::Uri::from_static("/bucket/%2F"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::PUT,
            None,
            "/bucket//",
            "s3.example",
            &http::Uri::from_static("/bucket//"),
            &headers,
        ));
        assert!(!is_bulk_request(
            &Method::PUT,
            None,
            "/bucket/",
            "s3.example",
            &http::Uri::from_static("/bucket/"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::PUT,
            Some("bucket.s3.example"),
            "/key",
            "s3.example",
            &http::Uri::from_static("/key?partNumber=1&uploadId=upload"),
            &headers,
        ));
        assert!(!is_bulk_request(
            &Method::PUT,
            Some("bucket.s3.example"),
            "/",
            "s3.example",
            &http::Uri::from_static("/"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::PUT,
            Some("bucket.s3.example"),
            "/%2F",
            "s3.example",
            &http::Uri::from_static("/%2F"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::POST,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key?uploadId=upload"),
            &headers,
        ));
        assert!(!is_bulk_request(
            &Method::POST,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key?uploads"),
            &headers,
        ));
        assert!(!is_bulk_request(
            &Method::POST,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key?uploads&uploadId=upload"),
            &headers,
        ));
        let mut multipart = http::HeaderMap::new();
        multipart.insert(
            header::CONTENT_TYPE,
            http::HeaderValue::from_static("Multipart/Form-Data; boundary=x"),
        );
        assert!(is_bulk_request(
            &Method::POST,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key"),
            &multipart,
        ));
        assert!(!is_bulk_request(
            &Method::PUT,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key?tagging"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::GET,
            None,
            "/bucket",
            "s3.example",
            &http::Uri::from_static("/bucket"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::DELETE,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key"),
            &headers,
        ));
        assert!(is_bulk_request(
            &Method::DELETE,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key?uploadId=upload"),
            &headers,
        ));
        assert!(!is_bulk_request(
            &Method::DELETE,
            None,
            "/bucket/key",
            "s3.example",
            &http::Uri::from_static("/bucket/key?tagging&uploadId=upload"),
            &headers,
        ));
        assert!(!is_bulk_request(
            &Method::DELETE,
            None,
            "/bucket",
            "s3.example",
            &http::Uri::from_static("/bucket"),
            &headers,
        ));
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
    fn limits_capture_budget() {
        let limit = Arc::new(Semaphore::new(DELETE_CAPTURE_LIMIT));
        let permits = (0..DELETE_CAPTURE_LIMIT)
            .map(|_| limit.clone().try_acquire_owned().expect("capture permit"))
            .collect::<Vec<_>>();
        assert!(matches!(
            limit.clone().try_acquire_owned(),
            Err(TryAcquireError::NoPermits)
        ));
        drop(permits);
        assert_eq!(limit.available_permits(), DELETE_CAPTURE_LIMIT);
    }

    #[test]
    fn accepts_http1_only() {
        let builder = ConnBuilder::new(TokioExecutor::new()).http1_only();
        assert!(builder.is_http1_available());
        assert!(!builder.is_http2_available());
    }

    #[tokio::test]
    async fn charges_forwarded_client() {
        // Behind a trusted proxy every client gets its own bucket; charging the
        // proxy would let one caller throttle every other caller of that proxy.
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::storage::FjallStorage::open(dir.path().to_str().unwrap())
            .expect("storage opens");
        let driver_ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("local addr");
        let server = S3Server::new(
            "127.0.0.1:0",
            format!("localhost:{}", address.port()),
            driver_ctx,
            RealmId([5u8; 32]),
            iroh::SecretKey::generate().public(),
            CredentialEncryptionKey::random(),
            RoCrateLimits::default(),
            CorsConfig::default(),
            Arc::new(NodeMetrics::new()),
        )
        .await
        .expect("s3 server builds")
        .with_trusted_proxies(vec!["127.0.0.1/32".parse().expect("valid proxy net")])
        .with_rate_limits(crate::rate_limit::ApiRateLimits::for_test(1))
        .expect("rate limits install");
        let (_bound, task) = server
            .run_with_listener(listener, CancellationToken::new())
            .expect("server runs");

        let client = reqwest::Client::new();
        let charge = async |forwarded: &str| {
            client
                .get(format!("http://127.0.0.1:{}/bucket/key", address.port()))
                .header("x-forwarded-for", forwarded)
                .send()
                .await
                .expect("request completes")
                .status()
        };
        assert_ne!(
            charge("198.51.100.1").await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            charge("198.51.100.1").await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        // A second client behind the same proxy still has its own budget.
        assert_ne!(
            charge("198.51.100.2").await,
            StatusCode::SERVICE_UNAVAILABLE
        );

        task.abort();
    }

    #[test]
    fn extracts_bucket_name_from_path_style_request() {
        assert_eq!(
            extract_bucket_name(
                Some("s3.example.com"),
                "/bucket-name/object.txt",
                "s3.example.com"
            ),
            Some("bucket-name".to_string())
        );
        assert_eq!(
            extract_bucket_name(Some("s3.example.com"), "/bucket-name", "s3.example.com"),
            Some("bucket-name".to_string())
        );
        assert_eq!(
            extract_bucket_name(None, "/bucket-name", "s3.example.com"),
            Some("bucket-name".to_string())
        );
        assert_eq!(
            extract_bucket_name(Some("s3.example.com"), "/", "s3.example.com"),
            None
        );
    }

    #[test]
    fn extracts_bucket_name_from_virtual_hosted_request() {
        assert_eq!(
            extract_bucket_name(
                Some("bucket-name.s3.example.com"),
                "/object.txt",
                "s3.example.com"
            ),
            Some("bucket-name".to_string())
        );
        assert_eq!(
            extract_bucket_name(
                Some("bucket-name.s3.example.com:9000"),
                "/object.txt",
                "s3.example.com:9000"
            ),
            Some("bucket-name".to_string())
        );
    }

    #[tokio::test]
    async fn applies_configured_limits() {
        // A nondefault burst of one must reject the second request, proving S3
        // uses the configured quota rather than the default.
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let node_id = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        let server = S3Server::new(
            "127.0.0.1:0",
            "localhost".to_string(),
            driver_ctx,
            RealmId([9u8; 32]),
            node_id,
            CredentialEncryptionKey::random(),
            Default::default(),
            crate::cors::CorsConfig::default(),
            Arc::new(NodeMetrics::new()),
        )
        .await
        .unwrap()
        .with_rate_limits(crate::rate_limit::ApiRateLimits::new(60, 1, 60, 1))
        .unwrap();

        let ip = std::net::IpAddr::from([127, 0, 0, 1]);
        assert!(server.rate_limits.check_ip(ip).is_ok());
        assert!(server.rate_limits.check_ip(ip).is_err());
    }
}
