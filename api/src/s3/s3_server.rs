use super::auth::AuthProvider;
use super::cors::{
    build_preflight_forbidden_response, build_preflight_response, inject_actual_cors_headers,
    match_actual_rule, match_preflight_rule, parse_requested_headers,
};
use super::s3_service::ArunaS3Service;
use crate::cors::CorsConfig;
use crate::error::S3ServerError;
use crate::telemetry::{emit_request_completed, make_request_span};
use aruna_core::NodeId;
use aruna_core::metrics::{NodeMetrics, RequestLabels, RouteLabels, method_label};
use aruna_core::structs::{BucketCorsConfiguration, RealmId, RoCrateLimits};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use futures_core::future::BoxFuture;
use http::{Method, Request, StatusCode, header};
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
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::{Instrument, error, info, trace};

/// Concurrent S3 connections served at once; further connections wait for a
/// slot so a flood cannot spawn unbounded connection tasks.
pub const DEFAULT_S3_MAX_CONNECTIONS: usize = 1_024;
/// Concurrent S3 requests processed at once, acquired before the expensive
/// s3s parse/body/storage work.
pub const DEFAULT_S3_MAX_CONCURRENT_REQUESTS: usize = 512;

/// Carries the resolved S3 operation name from the access check back to the
/// wrapper so request metrics can be labelled by operation. The wrapper inserts
/// it into the request extensions (which survive `s3s` routing) and keeps a
/// clone; [`crate::s3::auth::AuthProvider::check`] fills it in once the
/// operation is known.
#[derive(Clone)]
pub struct S3OpLabel(Arc<OnceLock<String>>);

#[derive(Clone, Default)]
pub(crate) struct DeleteObjectsBody(Arc<Mutex<Vec<u8>>>);

impl DeleteObjectsBody {
    pub(crate) fn bytes(&self) -> Vec<u8> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

struct CaptureDeleteObjectsBody {
    inner: Pin<Box<Incoming>>,
    captured: DeleteObjectsBody,
}

impl hyper::body::Body for CaptureDeleteObjectsBody {
    type Data = hyper::body::Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        match self.inner.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    self.captured
                        .0
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .extend_from_slice(data);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
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
    connection_limit: Arc<Semaphore>,
    request_limit: Arc<Semaphore>,
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
    // Held across each request so concurrent expensive processing is bounded.
    request_limit: Arc<Semaphore>,
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
    #[tracing::instrument(level = "trace", skip(address, hostname, driver_ctx, metrics))]
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        address: impl Into<String> + Copy,
        hostname: impl Into<String>,
        driver_ctx: Arc<DriverContext>,
        realm_id: RealmId,
        node_id: NodeId,
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
            connection_limit: Arc::new(Semaphore::new(DEFAULT_S3_MAX_CONNECTIONS)),
            request_limit: Arc::new(Semaphore::new(DEFAULT_S3_MAX_CONCURRENT_REQUESTS)),
        })
    }

    /// Installs operator-configured concurrency ceilings; each floors at one so
    /// a permit is always available.
    pub fn with_concurrency_limits(mut self, max_connections: usize, max_requests: usize) -> Self {
        self.connection_limit = Arc::new(Semaphore::new(max_connections.max(1)));
        self.request_limit = Arc::new(Semaphore::new(max_requests.max(1)));
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
                rate_limits: rate_limits.clone(),
            },
        )?;
        self.rate_limits = rate_limits;
        Ok(self)
    }

    pub fn run_with_listener(
        self,
        listener: TcpListener,
    ) -> Result<(SocketAddr, JoinHandle<()>), S3ServerError> {
        let local_addr = listener.local_addr()?;
        let connection_limit = self.connection_limit.clone();
        let service = WrappingService {
            shared: self.s3service,
            cors: self.cors,
            domain: self.domain,
            driver_ctx: self.driver_ctx,
            metrics: self.metrics,
            peer_ip: None,
            rate_limits: self.rate_limits,
            request_limit: self.request_limit,
        };
        let connection = ConnBuilder::new(TokioExecutor::new());

        let server = async move {
            loop {
                let (socket, peer) = match listener.accept().await {
                    Ok(ok) => ok,
                    Err(err) => {
                        error!("error accepting connection: {err}");
                        continue;
                    }
                };
                // Bound concurrent connections: wait for a slot before spawning,
                // and hold the permit for the connection's whole lifetime.
                let permit = match connection_limit.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => break,
                };
                let mut service = service.clone();
                service.peer_ip = Some(peer.ip());
                let conn = connection.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let _ = conn.serve_connection(TokioIo::new(socket), service).await;
                });
            }
        };

        let task = tokio::spawn(server);
        info!("server is running at http://{local_addr}");

        Ok((local_addr, task))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn run(self) -> Result<JoinHandle<()>, S3ServerError> {
        let listener = TcpListener::bind(&self.address).await?;
        let (_, task) = self.run_with_listener(listener)?;
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
        let body = if delete_objects {
            let captured = DeleteObjectsBody::default();
            parts.extensions.insert(captured.clone());
            s3s::Body::http_body_unsync(CaptureDeleteObjectsBody {
                inner: Box::pin(body),
                captured,
            })
        } else {
            body.into()
        };
        let s3s_request = s3s::HttpRequest::from_parts(parts, body);
        let shared = self.shared.clone();
        let cors = self.cors.clone();
        let driver_ctx = self.driver_ctx.clone();
        let metrics = self.metrics.clone();
        let rate_limits = self.rate_limits.clone();
        let peer_ip = self.peer_ip;
        let request_limit = self.request_limit.clone();
        Box::pin(async move {
            // Charge the transport IP before CORS, signature checks, credential
            // reads, or body handling, so unsigned or invalid requests cannot
            // bypass admission.
            if let Some(peer_ip) = peer_ip
                && let Err(retry_after) = rate_limits.check_ip(peer_ip)
            {
                let response = slow_down_response(retry_after);
                let code = response.status().as_u16();
                emit_request_completed(&span, "s3", code, started);
                record_s3_request(&metrics, &method, code, "rate_limited", started.elapsed());
                return Ok(response);
            }
            // Bound concurrent request processing before the expensive s3s
            // parse, body handling, and storage work.
            let _request_permit = request_limit.acquire_owned().await.ok();
            let bucket_cors = if origin_header.is_some() {
                match load_bucket_cors_config(driver_ctx, bucket).await {
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

            let mut result = shared.call(s3s_request).instrument(span.clone()).await;
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
        })
    }
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
