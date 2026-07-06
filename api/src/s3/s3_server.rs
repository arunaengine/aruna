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
use aruna_core::metrics::{NodeMetrics, RequestLabels, RouteLabels};
use aruna_core::structs::{BucketCorsConfiguration, RealmId};
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
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::{Instrument, error, info, trace};

/// Carries the resolved S3 operation name from the access check back to the
/// wrapper so request metrics can be labelled by operation. The wrapper inserts
/// it into the request extensions (which survive `s3s` routing) and keeps a
/// clone; [`crate::s3::auth::AuthProvider::check`] fills it in once the
/// operation is known.
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
            method: method.to_string(),
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
    cors: CorsConfig,
    domain: String,
    driver_ctx: Arc<DriverContext>,
    metrics: Arc<NodeMetrics>,
}

#[derive(Clone)]
pub struct WrappingService {
    shared: S3Service, // Aruna specific implementation of S3 trait
    cors: CorsConfig,
    domain: String,
    driver_ctx: Arc<DriverContext>,
    metrics: Arc<NodeMetrics>,
}

impl S3Server {
    #[tracing::instrument(level = "trace", skip(address, hostname, driver_ctx, metrics))]
    pub async fn new(
        address: impl Into<String> + Copy,
        hostname: impl Into<String>,
        driver_ctx: Arc<DriverContext>,
        realm_id: RealmId,
        node_id: NodeId,
        cors: CorsConfig,
        metrics: Arc<NodeMetrics>,
    ) -> Result<Self, S3ServerError> {
        let s3service = ArunaS3Service::new(driver_ctx.clone(), realm_id, node_id).await;
        let hostname = hostname.into();

        let auth = AuthProvider {
            driver_ctx: driver_ctx.clone(),
            realm_id,
            node_id,
        };

        let service = {
            let mut b = S3ServiceBuilder::new(s3service.clone());
            b.set_host(SingleDomain::new(&hostname)?);
            b.set_auth(auth.clone());
            b.set_access(auth);
            b.set_validation(AwsNameValidation::new());
            b.build()
        };

        Ok(Self {
            address: address.into(),
            s3service: service,
            cors,
            domain: hostname,
            driver_ctx,
            metrics,
        })
    }

    pub fn run_with_listener(
        self,
        listener: TcpListener,
    ) -> Result<(SocketAddr, JoinHandle<()>), S3ServerError> {
        let local_addr = listener.local_addr()?;
        let service = WrappingService {
            shared: self.s3service,
            cors: self.cors,
            domain: self.domain,
            driver_ctx: self.driver_ctx,
            metrics: self.metrics,
        };
        let connection = ConnBuilder::new(TokioExecutor::new());

        let server = async move {
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(ok) => ok,
                    Err(err) => {
                        error!("error accepting connection: {err}");
                        continue;
                    }
                };
                let service = service.clone();
                let conn = connection.clone();
                tokio::spawn(async move {
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
        let s3s_request = s3s::HttpRequest::from_parts(parts, body.into());
        let shared = self.shared.clone();
        let cors = self.cors.clone();
        let driver_ctx = self.driver_ctx.clone();
        let metrics = self.metrics.clone();
        Box::pin(async move {
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
                if let Some(config) = bucket_cors.as_ref() {
                    let response = requested_method.as_deref().map_or_else(
                        build_preflight_forbidden_response,
                        |requested_method| match match_preflight_rule(
                            config,
                            origin.as_deref().unwrap_or_default(),
                            requested_method,
                            &requested_headers,
                        ) {
                            Some(matched_rule) => build_preflight_response(matched_rule),
                            None => build_preflight_forbidden_response(),
                        },
                    );
                    let code = response.status().as_u16();
                    emit_request_completed(&span, "s3", code, started);
                    record_s3_request(&metrics, &method, code, "cors_preflight", started.elapsed());
                    return Ok(response);
                }

                let mut response = http::Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .body(s3s::Body::empty())
                    .expect("static response must build");
                if let Some(cors_headers) =
                    cors.s3_preflight_headers(origin_header, requested_headers_value.as_ref())
                {
                    response.headers_mut().extend(cors_headers);
                }
                let code = response.status().as_u16();
                emit_request_completed(&span, "s3", code, started);
                record_s3_request(&metrics, &method, code, "cors_preflight", started.elapsed());
                return Ok(response);
            }

            let mut result = shared.call(s3s_request).instrument(span.clone()).await;
            if let Ok(response) = &mut result {
                if let Some(config) = bucket_cors.as_ref() {
                    if let Some(origin) = origin.as_deref()
                        && let Some(matched_rule) = match_actual_rule(config, origin, &method)
                    {
                        inject_actual_cors_headers(response, matched_rule);
                    }
                } else {
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
}
