//! The S3 listener: connection acceptance, the staged request pipeline, and
//! response-lifetime ownership. Responding bodies keep their permits and
//! accounting until the stream ends, so headers never release a request early.

mod activity;
mod body;
mod classification;
mod keepalive;
mod response;

pub(crate) use body::DeleteObjectsBody;

use self::activity::{
    ActiveRequestGuard, ConnectionActivity, should_watch_idle, spawn_response_watch,
    spawn_stream_idle, spawn_total_deadline,
};
use self::classification::RequestClassification;
use self::keepalive::{HandlerOutcome, await_handler, keepalive_response};
use self::response::{
    apply_response_cors, connection_error, invalid_bucket_response, oversized_delete_response,
    preflight_response, slow_down_response, stream_timeout_response,
};
use super::auth::AuthProvider;
use super::service::ArunaS3Service;
use crate::cors::CorsConfig;
use crate::error::S3ServerError;
use crate::rate_limit::{LocalKey, LocalLease};
use crate::telemetry::{RequestCancelGuard, emit_request_completed, make_request_span};
use aruna_core::NodeId;
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::metrics::{NodeMetrics, RequestLabels, RouteLabels, method_label};
use aruna_core::structs::storage::blob::BucketCorsConfiguration;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use futures_core::future::BoxFuture;
use http::{Method, Request};
use hyper::body::Incoming;
use hyper::service::Service;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::HttpError;
use s3s::HttpResponse;
use s3s::host::SingleDomain;
use s3s::service::S3Service;
use s3s::service::S3ServiceBuilder;
use s3s::validation::AwsNameValidation;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{Instrument, error, info, trace};

const INITIAL_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const STREAM_LIFETIME_TIMEOUT: Duration = Duration::from_secs(30 * 60);
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
            connection_idle: activity::CONNECTION_IDLE_TIMEOUT,
            stream_lifetime: STREAM_LIFETIME_TIMEOUT,
        }
    }
}

/// Shares the resolved S3 operation between access checks and request metrics.
/// Request extensions preserve it across `s3s` routing.
#[derive(Clone)]
pub struct S3OpLabel(Arc<OnceLock<String>>);

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

/// Tracing and metrics state of one request. Every early answer and the final
/// result go through it, so completion records cannot drift apart.
struct RequestTrace {
    span: tracing::Span,
    started: Instant,
    method: Method,
    metrics: Arc<NodeMetrics>,
}

impl RequestTrace {
    fn begin(
        classification: &RequestClassification,
        headers: &http::HeaderMap,
        metrics: Arc<NodeMetrics>,
    ) -> Self {
        let span = make_request_span("s3", headers, &classification.method, &classification.path);
        {
            let _guard = span.enter();
            trace!(
                event = "request.received",
                protocol = "s3",
                method = %classification.method,
                path = %classification.path,
                "Received S3 request"
            );
        }
        Self {
            span,
            started: Instant::now(),
            method: classification.method.clone(),
            metrics,
        }
    }

    /// Emits the completion record for a finished response and returns it
    /// unchanged.
    fn respond(&self, op: &str, response: HttpResponse) -> Result<HttpResponse, HttpError> {
        let code = response.status().as_u16();
        emit_request_completed(&self.span, "s3", code, self.started);
        record_s3_request(
            &self.metrics,
            &self.method,
            code,
            op,
            self.started.elapsed(),
        );
        Ok(response)
    }

    /// Records a failed request with a 500 completion and returns the error.
    fn fail(
        &self,
        op: &str,
        message: &'static str,
        error: HttpError,
    ) -> Result<HttpResponse, HttpError> {
        self.span.record("status_code", 500);
        record_s3_request(&self.metrics, &self.method, 500, op, self.started.elapsed());
        let _guard = self.span.enter();
        error!(
            event = "request.failed",
            protocol = "s3",
            latency_ms = self.started.elapsed().as_millis() as u64,
            error = ?error,
            "{}",
            message
        );
        Err(error)
    }
}

/// One classified request with every owner its execution needs. Created by
/// [`Service::call`] after classification, tracing, activity setup and body
/// wrapping, then consumed by [`PreparedRequest::run`].
struct PreparedRequest {
    service: WrappingService,
    classification: RequestClassification,
    trace: RequestTrace,
    op_label: S3OpLabel,
    /// Present until the handler starts; dropping it before then releases the
    /// request body.
    request: Option<s3s::HttpRequest>,
    capture: Option<DeleteObjectsBody>,
    admission: Option<OwnedSemaphorePermit>,
    capture_permit: Option<OwnedSemaphorePermit>,
    lease: LocalLease,
    connection: Arc<ConnectionActivity>,
    stream: Arc<ConnectionActivity>,
    active: Option<ActiveRequestGuard>,
    body_end: bool,
    charged_ip: Option<IpAddr>,
    admission_limit: Arc<Semaphore>,
    egress_limit: Arc<Semaphore>,
    capture_limit: Arc<Semaphore>,
}

impl PreparedRequest {
    /// Runs the request stages in execution order: IP quota, admission,
    /// oversized-body rejection, lease, accounting and deadline, CORS,
    /// preflight, bucket validation, capture, idle watch, execution, response.
    async fn run(mut self) -> Result<HttpResponse, HttpError> {
        // Stage: charge the transport IP before any validation or body handling.
        if let Some(charged_ip) = self.charged_ip
            && let Err(retry_after) = self.service.rate_limits.check_ip(charged_ip)
        {
            self.request = None;
            let response = slow_down_response(retry_after);
            return self.trace.respond("rate_limited", response);
        }

        // Stage: bound concurrent request processing before the expensive s3s
        // parse, body handling, and storage work.
        match self.admission_limit.clone().try_acquire_owned() {
            Ok(permit) => self.admission = Some(permit),
            Err(TryAcquireError::NoPermits | TryAcquireError::Closed) => {
                // Dropping the request lets the transport drain or close bodies
                // without desynchronizing the next request.
                self.request = None;
                let response = slow_down_response(1);
                return self.trace.respond("admission_limited", response);
            }
        }

        // Stage: a body already known oversized from Content-Length is rejected
        // before it is read.
        if self.classification.oversized_delete {
            self.request = None;
            let response = oversized_delete_response()?;
            return self.trace.respond("body_limited", response);
        }

        // Stage: hold the local per-IP lease for the request lifetime. A lease
        // is shared with the access hook, which installs the credential permit.
        if let Some(charged_ip) = self.charged_ip {
            let permit = match self
                .service
                .rate_limits
                .try_acquire_local(LocalKey::Ip(charged_ip))
            {
                Some(permit) => permit,
                None => return Ok(self.local_limited_response()),
            };
            if !self.lease.hold(permit) {
                return Ok(self.local_limited_response());
            }
        }
        self.request
            .as_mut()
            .expect("request is present before the handler runs")
            .extensions_mut()
            .insert(self.lease.clone());

        self.connection.begin_request();
        let deadline_activity = spawn_total_deadline(self.service.timeouts.stream_lifetime);
        self.active = Some(ActiveRequestGuard::new(
            self.connection.clone(),
            deadline_activity.clone(),
        ));

        let bucket_cors = if self.classification.origin_header.is_some() {
            let driver_ctx = self.service.driver_ctx.clone();
            let bucket = self.classification.bucket.clone();
            let connection = self.connection.clone();
            let deadline = deadline_activity.clone();
            let cors_result = tokio::select! {
                result = load_bucket_cors(driver_ctx, bucket) => result,
                _ = connection.wait_cancelled() => {
                    self.request = None;
                    self.admission = None;
                    return Err(connection_error());
                }
                _ = deadline.wait_cancelled() => {
                    self.request = None;
                    self.admission = None;
                    return stream_timeout_response();
                }
            };
            match cors_result {
                Ok(bucket_cors) => bucket_cors,
                Err(error) => {
                    return self.trace.fail(
                        "unknown",
                        "Failed to query bucket CORS configuration",
                        HttpError::new(error.into()),
                    );
                }
            }
        } else {
            None
        };

        if deadline_activity.is_cancelled() {
            self.request = None;
            self.admission = None;
            return stream_timeout_response();
        }

        // Stage: answer CORS preflight before s3s signature validation, so an
        // unsigned OPTIONS request cannot fail with 403.
        if let Some(response) = preflight_response(
            &self.classification,
            bucket_cors.as_ref(),
            &self.service.cors,
        ) {
            return self.trace.respond("cors_preflight", response);
        }

        // Stage: reject a malformed bucket name with the violated rule instead
        // of s3s's message-less "UnknownError".
        if let Some(reason) = self.classification.invalid_bucket {
            self.request = None;
            self.admission = None;
            let response = invalid_bucket_response(reason)?;
            return self.trace.respond("invalid_bucket", response);
        }

        if self.classification.delete_objects {
            match self.capture_limit.clone().try_acquire_owned() {
                Ok(permit) => self.capture_permit = Some(permit),
                Err(TryAcquireError::NoPermits | TryAcquireError::Closed) => {
                    self.request = None;
                    self.admission = None;
                    let response = slow_down_response(1);
                    return self.trace.respond("capture_limited", response);
                }
            }
        }

        // Stage: watch the request body for idleness; an ended body has nothing
        // left to protect.
        if should_watch_idle(self.body_end) {
            spawn_stream_idle(&self.stream);
        } else {
            self.stream.stop();
        }

        let shared = self.service.shared.clone();
        let span = self.trace.span.clone();
        let request = self
            .request
            .take()
            .expect("request is present before the handler runs");
        let mut handler: BoxFuture<'static, Result<HttpResponse, HttpError>> =
            Box::pin(async move { shared.call(request).await }.instrument(span));
        let connection = self.connection.clone();
        let stream = self.stream.clone();
        let deadline = deadline_activity.clone();
        let outcome = tokio::select! {
            result = await_handler(&mut handler, self.classification.complete_multipart) => match result {
                Some(result) => HandlerOutcome::Done(result),
                None => HandlerOutcome::Keepalive,
            },
            _ = connection.wait_cancelled() => HandlerOutcome::Aborted,
            _ = deadline.wait_cancelled() => HandlerOutcome::TimedOut,
            _ = stream.wait_cancelled() => HandlerOutcome::TimedOut,
        };

        self.finish(outcome, handler, bucket_cors, deadline_activity)
            .await
    }

    fn local_limited_response(&mut self) -> HttpResponse {
        self.request = None;
        self.admission = None;
        slow_down_response(1)
    }

    /// Releases the admission and capture permits and stops the request stream
    /// watch. The active-request accounting and the lease stay owned by
    /// whichever response lifetime holder is built next.
    fn finish_request(&mut self) {
        self.admission = None;
        self.capture_permit = None;
        self.stream.stop();
    }

    async fn finish(
        mut self,
        outcome: HandlerOutcome,
        handler: BoxFuture<'static, Result<HttpResponse, HttpError>>,
        bucket_cors: Option<BucketCorsConfiguration>,
        deadline_activity: Arc<ConnectionActivity>,
    ) -> Result<HttpResponse, HttpError> {
        let connection = self.connection.clone();
        let stream = self.stream.clone();
        let result = match outcome {
            HandlerOutcome::Done(result) => result,
            // AWS answers a slow CompleteMultipartUpload the same way: the 200
            // head first, whitespace while it works, the XML last. The body is
            // wrapped below, keeping the response lifetime on the completion.
            HandlerOutcome::Keepalive => Ok(keepalive_response(handler)),
            HandlerOutcome::Aborted => {
                self.finish_request();
                return Err(connection_error());
            }
            HandlerOutcome::TimedOut => {
                self.finish_request();
                return stream_timeout_response();
            }
        };
        if deadline_activity.is_cancelled() {
            self.finish_request();
            return stream_timeout_response();
        }
        if stream.is_cancelled() {
            self.finish_request();
            return stream_timeout_response();
        }

        let mut result = match result {
            Ok(response) => {
                let egress_permit = match self.egress_limit.clone().try_acquire_owned() {
                    Ok(permit) => Some(permit),
                    Err(TryAcquireError::NoPermits | TryAcquireError::Closed)
                        if self.classification.method != Method::GET
                            && self.classification.method != Method::HEAD =>
                    {
                        // The handler already ran: a durable mutation's small
                        // ack must not be dropped for a streaming permit.
                        None
                    }
                    Err(TryAcquireError::NoPermits | TryAcquireError::Closed) => {
                        drop(response);
                        self.finish_request();
                        self.active = None;
                        let response = slow_down_response(1);
                        return self.trace.respond("egress_limited", response);
                    }
                };
                self.finish_request();
                self.connection.touch();
                // Bulk responses use an independent lane from controls and metadata.
                let response_activity = Arc::new(ConnectionActivity::with_idle(
                    self.service.timeouts.connection_idle,
                ));
                response_activity.touch();
                spawn_response_watch(
                    connection,
                    response_activity.clone(),
                    deadline_activity.clone(),
                );
                let lifetime = body::ResponseLifetime::new(self.active.take(), self.lease)
                    .with_egress(egress_permit);
                Ok(response.map(|body| {
                    s3s::Body::http_body_unsync(body::ResponseBody::new(
                        body,
                        self.connection.clone(),
                        response_activity,
                        lifetime,
                    ))
                }))
            }
            Err(error) => {
                self.finish_request();
                Err(error)
            }
        };

        if self
            .capture
            .as_ref()
            .is_some_and(DeleteObjectsBody::exceeded)
        {
            stream.stop();
            result = oversized_delete_response();
        }

        if let Ok(response) = &mut result {
            apply_response_cors(
                response,
                &self.classification,
                bucket_cors.as_ref(),
                &self.service.cors,
            );
        }

        let op = self.op_label.resolved();
        match result {
            Ok(response) => self.trace.respond(op, response),
            Err(error) => self.trace.fail(op, "S3 request failed", error),
        }
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
            capture_limit: Arc::new(Semaphore::new(body::DELETE_CAPTURE_LIMIT)),
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
        self.capture_limit = Arc::new(Semaphore::new(
            max_requests.clamp(1, body::DELETE_CAPTURE_LIMIT),
        ));
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

    /// Accepts until `shutdown` is cancelled. Connection tasks are tracked by
    /// the returned handle, so both graceful and forced shutdown can await
    /// every child's release instead of detaching it with the accept task.
    pub fn run_with_listener(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(SocketAddr, S3ServerHandle), S3ServerError> {
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
        let connection_tracker = connections.clone();
        let abort_connections = CancellationToken::new();
        let server_abort = abort_connections.clone();

        let server = async move {
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
                let connection_abort = server_abort.clone();
                connection_tracker.spawn(async move {
                    let _permit = permit;
                    let conn = builder.serve_connection(TokioIo::new(socket), service);
                    let mut conn = std::pin::pin!(conn);
                    tokio::select! {
                        biased;
                        _ = connection_abort.cancelled() => {}
                        _ = activity::run_connection(activity.clone(), conn.as_mut(), timeouts.initial_request) => {}
                        _ = connection_shutdown.cancelled() => {
                            // Finish the request being served, then close the
                            // connection instead of waiting out its keep-alive.
                            conn.as_mut().graceful_shutdown();
                            tokio::select! {
                                biased;
                                _ = connection_abort.cancelled() => {}
                                _ = activity::run_connection(activity, conn.as_mut(), timeouts.initial_request) => {}
                            }
                        }
                    }
                });
            }
        };

        let task = tokio::spawn(server);
        info!("server is running at http://{local_addr}");

        Ok((
            local_addr,
            S3ServerHandle {
                task: Some(task),
                connections,
                abort_connections,
            },
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, shutdown))]
    pub async fn run(self, shutdown: CancellationToken) -> Result<S3ServerHandle, S3ServerError> {
        let listener = TcpListener::bind(&self.address).await?;
        let (_, handle) = self.run_with_listener(listener, shutdown)?;
        Ok(handle)
    }
}

/// Completion boundary for one bound S3 server. The accept task ends with the
/// accept loop while connection children stay owned here, so a forced shutdown
/// awaits their release instead of detaching them with the aborted future.
pub struct S3ServerHandle {
    /// `None` once `exit` consumed the accept task's result.
    task: Option<JoinHandle<()>>,
    connections: TaskTracker,
    abort_connections: CancellationToken,
}

impl S3ServerHandle {
    pub fn is_finished(&self) -> bool {
        self.task
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
    }

    /// Stops accepting and asks every connection to close; does not wait.
    pub fn abort(&self) {
        self.abort_connections.cancel();
        if let Some(task) = self.task.as_ref() {
            task.abort();
        }
    }

    /// Resolves when the accept loop exits, for supervision selects. The accept
    /// result is consumed exactly once; a later call pends, and the handle keeps
    /// owning the connection children for the wait that follows.
    pub async fn exit(&mut self, label: &str) -> String {
        let Some(task) = self.task.as_mut() else {
            return std::future::pending().await;
        };
        let result = (&mut *task).await;
        self.task = None;
        match result {
            Ok(()) => format!("{label} server stopped unexpectedly"),
            Err(error) if error.is_cancelled() => format!("{label} server aborted"),
            Err(error) => format!("{label} server panicked: {error}"),
        }
    }

    /// Waits for the accept loop and every connection task to release while the
    /// handle stays owned. Dropping this future (a phase deadline) leaves the
    /// owner able to abort and await the remaining work.
    pub async fn wait_until_released(&mut self) {
        if let Some(task) = self.task.as_mut() {
            let _ = (&mut *task).await;
        }
        self.task = None;
        self.connections.close();
        let in_flight = self.connections.len();
        if in_flight > 0 {
            info!(in_flight, "Draining in-flight S3 connections");
        }
        self.connections.wait().await;
    }

    /// Waits for the accept loop and every connection task to release.
    pub async fn wait(mut self) {
        self.wait_until_released().await;
    }
}

impl Service<Request<Incoming>> for WrappingService {
    type Response = HttpResponse;

    type Error = HttpError;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let (mut parts, body) = req.into_parts();
        // Stage: classification. Route, bucket, CORS preconditions and body
        // shape are derived before anything is parsed or stored.
        let classification = RequestClassification::classify(&parts, &self.domain);
        // Stage: tracing and metrics. One label carrier travels through s3s
        // routing so the access hook can name the resolved operation.
        let op_label = S3OpLabel::new();
        parts.extensions.insert(op_label.clone());
        let trace = RequestTrace::begin(&classification, &parts.headers, self.metrics.clone());
        // Stage: activity and deadlines. Independent owners for the connection,
        // the request stream and the total lifetime; registered before any I/O.
        let stream_activity =
            Arc::new(ConnectionActivity::with_idle(self.timeouts.connection_idle));
        let connection_activity = self.activity.clone().unwrap_or_else(|| {
            Arc::new(ConnectionActivity::with_idle(self.timeouts.connection_idle))
        });
        connection_activity.mark_request();
        parts.extensions.insert(connection_activity.clone());
        parts.extensions.insert(stream_activity.clone());
        // Stage: body handling. DeleteObjects captures a bounded prefix; every
        // other body only reports progress.
        let body_end = hyper::body::Body::is_end_stream(&body);
        let capture = (!classification.oversized_delete && classification.delete_objects)
            .then(DeleteObjectsBody::default);
        let body = if classification.delete_objects {
            let captured = capture.clone().unwrap_or_default();
            parts.extensions.insert(captured.clone());
            body::wrap_request_body(body, &connection_activity, &stream_activity, Some(captured))
        } else {
            body::wrap_request_body(body, &connection_activity, &stream_activity, None)
        };
        // Stage: lanes. Bulk data takes the bulk admission and read lanes;
        // everything else the control admission and mutation lanes.
        let (admission_limit, egress_limit) = if classification.bulk_request {
            (self.bulk_limit.clone(), self.read_limit.clone())
        } else {
            (self.control_limit.clone(), self.mutation_limit.clone())
        };
        // Behind a trusted proxy the forwarded client is charged, never the
        // shared proxy address.
        let charged_ip = self
            .peer_ip
            .map(|peer| crate::forwarded::client_ip(&self.trusted_proxies, peer, &parts.headers));
        let request = s3s::HttpRequest::from_parts(parts, body);
        let mut cancel_guard = RequestCancelGuard::new(
            trace.span.clone(),
            &classification.method,
            &classification.path,
        );
        let prepared = PreparedRequest {
            service: self.clone(),
            classification,
            trace,
            op_label,
            request: Some(request),
            capture,
            admission: None,
            capture_permit: None,
            lease: LocalLease::default(),
            connection: connection_activity,
            stream: stream_activity,
            active: None,
            body_end,
            charged_ip,
            admission_limit,
            egress_limit,
            capture_limit: self.capture_limit.clone(),
        };
        // A request future dropped mid-flight leaves no completion record, which
        // is exactly how a cancelled completion used to go unnoticed.
        Box::pin(async move {
            let result = prepared.run().await;
            cancel_guard.disarm();
            result
        })
    }
}

async fn load_bucket_cors(
    driver_ctx: Arc<DriverContext>,
    bucket: Option<String>,
) -> Result<Option<BucketCorsConfiguration>, GetBucketError> {
    let Some(bucket) = bucket else {
        return Ok(None);
    };

    match drive(GetBucketOperation::new(bucket), driver_ctx.as_ref()).await {
        Ok(bucket_info) => Ok(bucket_info.cors_configuration),
        Err(GetBucketError::NotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn default_timeouts_unchanged() {
        // Making the deadlines configurable must not move the shipped defaults.
        let timeouts = S3ServerTimeouts::default();
        assert_eq!(timeouts.initial_request, Duration::from_secs(10));
        assert_eq!(timeouts.connection_idle, Duration::from_secs(20));
        assert_eq!(timeouts.stream_lifetime, Duration::from_secs(30 * 60));
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
    fn limits_capture_budget() {
        let limit = Arc::new(Semaphore::new(body::DELETE_CAPTURE_LIMIT));
        let permits = (0..body::DELETE_CAPTURE_LIMIT)
            .map(|_| limit.clone().try_acquire_owned().expect("capture permit"))
            .collect::<Vec<_>>();
        assert!(matches!(
            limit.clone().try_acquire_owned(),
            Err(TryAcquireError::NoPermits)
        ));
        drop(permits);
        assert_eq!(limit.available_permits(), body::DELETE_CAPTURE_LIMIT);
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
            http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            charge("198.51.100.1").await,
            http::StatusCode::SERVICE_UNAVAILABLE
        );
        // A second client behind the same proxy still has its own budget.
        assert_ne!(
            charge("198.51.100.2").await,
            http::StatusCode::SERVICE_UNAVAILABLE
        );

        task.abort();
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

    // The handle reports how its accept loop ended, including cancellation and
    // panic, so supervision selects keep their named failure messages.
    #[tokio::test]
    async fn exit_reports_cause() {
        let handle = |task| S3ServerHandle {
            task: Some(task),
            connections: TaskTracker::new(),
            abort_connections: CancellationToken::new(),
        };

        let mut stopped = handle(tokio::spawn(async {}));
        assert_eq!(stopped.exit("S3").await, "S3 server stopped unexpectedly");
        assert!(stopped.is_finished());

        let mut aborted = handle(tokio::spawn(std::future::pending::<()>()));
        aborted.task.as_mut().expect("accept task retained").abort();
        assert_eq!(aborted.exit("S3").await, "S3 server aborted");

        let mut panicked = handle(tokio::spawn(async { panic!("boom") }));
        assert!(panicked.exit("S3").await.contains("S3 server panicked"));
    }

    // A forced abort must still own every accepted connection: `wait` resolves
    // only after the connection children released, not merely after the accept
    // task was cancelled.
    #[tokio::test]
    async fn abort_awaits_connections() {
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::storage::FjallStorage::open(dir.path().to_str().unwrap())
            .expect("test storage");
        let driver_ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let node_id = iroh::SecretKey::from_bytes(&[0x56; 32]).public();
        let server = S3Server::new(
            "127.0.0.1:0",
            "localhost".to_string(),
            driver_ctx,
            RealmId([0x56; 32]),
            node_id,
            CredentialEncryptionKey::random(),
            Default::default(),
            crate::cors::CorsConfig::default(),
            Arc::new(NodeMetrics::new()),
        )
        .await
        .expect("s3 server builds");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (_bound, handle) = server
            .run_with_listener(listener, CancellationToken::new())
            .expect("server runs");

        // One accepted connection with a partial request keeps a connection
        // child active; the short wait is the OS boundary, not a schedule guess.
        let mut socket = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect");
        socket
            .write_all(b"GET /bucket/key HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .expect("partial request");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        handle.abort();
        tokio::time::timeout(std::time::Duration::from_secs(5), handle.wait())
            .await
            .expect("a forced abort must await every connection child");
    }

    // An expired phase deadline drops the wait future but not the owner: the
    // retained handle still aborts and awaits the active connection child.
    #[tokio::test(start_paused = true)]
    async fn wait_keeps_owner() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::storage::FjallStorage::open(dir.path().to_str().unwrap())
            .expect("test storage");
        let driver_ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let node_id = iroh::SecretKey::from_bytes(&[0x57; 32]).public();
        let server = S3Server::new(
            "127.0.0.1:0",
            "localhost".to_string(),
            driver_ctx,
            RealmId([0x57; 32]),
            node_id,
            CredentialEncryptionKey::random(),
            Default::default(),
            crate::cors::CorsConfig::default(),
            Arc::new(NodeMetrics::new()),
        )
        .await
        .expect("s3 server builds");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (_bound, mut handle) = server
            .run_with_listener(listener, CancellationToken::new())
            .expect("server runs");

        // A request with an unfinished body keeps a connection child busy.
        let mut socket = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect");
        socket
            .write_all(
                b"PUT /bucket/key HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nab",
            )
            .await
            .expect("partial body");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // The accept loop is still running, so this wait stays pending and the
        // zero budget drops it exactly like an expired phase deadline.
        assert!(
            tokio::time::timeout(std::time::Duration::ZERO, handle.wait_until_released())
                .await
                .is_err(),
            "the wait must still be pending when its deadline expires"
        );

        handle.abort();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handle.wait_until_released(),
        )
        .await
        .expect("forced cleanup must await the connection child");

        let mut buf = [0u8; 1];
        let closed =
            tokio::time::timeout(std::time::Duration::from_secs(1), socket.read(&mut buf)).await;
        assert!(
            matches!(closed, Ok(Ok(0)) | Ok(Err(_))),
            "the connection child must have released the client, got {closed:?}"
        );
    }
}
