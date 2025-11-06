use super::auth::AuthProvider;
use super::s3service::ArunaS3Service;
use crate::caching::cache;
use crate::data_backends::storage_backend::StorageBackend;
use crate::CONFIG;
use crate::CORS_REGEX;
use anyhow::Result;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use http::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    ORIGIN, VARY,
};
use http::{HeaderValue, Method, StatusCode};
use hyper::service::Service;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::header::HOST;
use s3s::host::SingleDomain;
use s3s::s3_error;
use s3s::service::{S3Service, S3ServiceBuilder, SharedS3Service};
use s3s::Body;
use s3s::S3Error;
use std::convert::Infallible;
use std::future::{ready, Ready};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

pub struct S3Server {
    s3service: S3Service,
    address: String,
    inner_raw: ArunaS3Service,
}

#[derive(Clone)]
pub struct WrappingService {
    shared: SharedS3Service,
    inner: ArunaS3Service,
}

impl S3Server {
    #[tracing::instrument(level = "trace", skip(address, hostname, backend, cache))]
    pub async fn new(
        address: impl Into<String> + Copy,
        hostname: impl Into<String>,
        backend: Arc<Box<dyn StorageBackend>>,
        cache: Arc<cache::Cache>,
    ) -> Result<Self> {
        let s3service = ArunaS3Service::new(backend, cache.clone())
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                tonic::Status::unauthenticated(e.to_string())
            })?;
        let auth_provider = AuthProvider::new(cache).await;
        let service = {
            let mut b = S3ServiceBuilder::new(s3service.clone());
            b.set_host(SingleDomain::new(&hostname.into())?);
            b.set_access(auth_provider.clone());
            b.set_auth(auth_provider);
            b.build()
        };

        Ok(Self {
            s3service: service,
            address: address.into(),
            inner_raw: s3service,
        })
    }
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn run(self) -> Result<()> {
        // Run server
        let listener = TcpListener::bind(&self.address).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            tonic::Status::unauthenticated(e.to_string())
        })?;

        let local_addr = listener.local_addr()?;

        let service = WrappingService {
            shared: self.s3service.into_shared(),
            inner: self.inner_raw.clone(),
        };

        let connection = ConnBuilder::new(TokioExecutor::new());

        let server = async move {
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(ok) => ok,
                    Err(err) => {
                        tracing::error!("error accepting connection: {err}");
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

        let _task = tokio::spawn(server);
        info!("server is running at http://{local_addr}");

        Ok(())
    }
}

impl Service<hyper::Request<hyper::body::Incoming>> for WrappingService {
    type Response = hyper::Response<Body>;

    type Error = S3Error;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    #[tracing::instrument(level = "trace", skip(self, req))]
    fn call(&self, req: hyper::Request<hyper::body::Incoming>) -> Self::Future {
        // Check if response gets CORS header pass
        let mut origin_exception = false;
        if let Some(origin) = req.headers().get("Origin") {
            if let Some(cors_regex) = &*CORS_REGEX {
                match origin.to_str() {
                    Ok(origin) => {
                        origin_exception = cors_regex.is_match(origin);
                    }
                    Err(err) => {
                        error!("Invalid Origin Header: {err}");
                    }
                }
            }
        }

        // Catch OPTIONS requests
        if req.method() == Method::OPTIONS {
            let clone = self.clone();
            return Box::pin(async move {
                let mut response = hyper::Response::builder();
                match clone.get_cors_headers(origin_exception, &req).await {
                    Ok(cors_header) => {
                        for (k, v) in cors_header.iter() {
                            response = response.header(k, v);
                        }
                    }
                    Err(err) => error!("{err}"),
                }
                response
                    .body(Body::empty())
                    .map_err(|_| s3_error!(InvalidRequest, "Invalid OPTIONS request"))
            });
        }

        // FIXME: This is necessary for MultipartUploadRequests, because currently only the
        // response including cors headers gets returned inside the keep-alive-body. See
        // https://github.com/s3s-project/s3s/blob/85f3842d03175537753584ad9d4f25e819aa9025/crates/s3s/src/ops/generated.rs#L613-L650
        if req.method() == Method::POST && req.uri().to_string().contains("uploadId") {
            let clone = self.clone();
            return Box::pin(async move {
                let headers = match clone.get_cors_headers(origin_exception, &req).await {
                    Ok(map) => Some(map),
                    Err(e) => {
                        error!(?e);
                        None
                    }
                };

                let resp = clone.shared.call(req);
                let res =
                    resp.map(move |r| clone.modify_response_headers(r, origin_exception, headers));

                res.await
            });
        };

        let clone = self.clone();
        let resp = clone.shared.call(req);

        let res = resp.map(move |r| clone.modify_response_headers(r, origin_exception, None));
        res.boxed()
    }
}

impl AsRef<S3Service> for WrappingService {
    #[tracing::instrument(level = "trace", skip(self))]
    fn as_ref(&self) -> &S3Service {
        self.shared.as_ref()
    }
}

impl WrappingService {
    #[tracing::instrument(level = "trace", skip(self))]
    #[must_use]
    pub fn into_make_service(self) -> MakeService<Self> {
        MakeService(self)
    }
}

#[derive(Clone)]
pub struct MakeService<S>(S);

impl<T, S: Clone> Service<T> for MakeService<S> {
    type Response = S;

    type Error = Infallible;

    type Future = Ready<Result<Self::Response, Self::Error>>;

    #[tracing::instrument(level = "trace", skip(self))]
    fn call(&self, _: T) -> Self::Future {
        ready(Ok(self.0.clone()))
    }
}

impl WrappingService {
    async fn get_cors_headers(
        &self,
        origin_exception: bool,
        req: &hyper::Request<hyper::body::Incoming>,
    ) -> Result<hyper::HeaderMap, S3Error> {
        // Return all * if origin exception matches
        if origin_exception {
            let mut map = hyper::HeaderMap::new();
            map.insert(ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("*"));
            map.insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            map.insert(ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("*"));
            return Ok(map);
        }

        // Evaluate mandatory headers of request
        let origin = req
            .headers()
            .get(ORIGIN)
            .ok_or_else(|| s3_error!(InvalidRequest, "Origin header missing"))?
            .to_str()
            .map_err(|e| {
                error!("{e}");
                s3_error!(InvalidURI, "Invalid URI encoding")
            })?;

        let url = req
            .headers()
            .get(HOST)
            .ok_or_else(|| s3_error!(InvalidHostHeader, "Host header missing"))?
            .to_str()
            .map_err(|e| {
                error!("{e}");
                s3_error!(InvalidURI, "Invalid URI encoding")
            })?
            .split(
                &CONFIG
                    .frontend
                    .as_ref()
                    .ok_or_else(|| s3_error!(InternalError, "No s3 frontend configured"))?
                    .hostname,
            )
            .next()
            .ok_or_else(|| s3_error!(InternalError, "Invalid host url set"))?;

        // Extract bucket/project from host url
        let bucket = match url.split(".").next() {
            Some("") => {
                let mut iter = req.uri().path().split("/");
                iter.next(); // first one is empty
                iter.next() // next one is bucket
                    .ok_or_else(|| s3_error!(InvalidURI, "No bucket set"))?
            }
            Some(sub_bucket) => sub_bucket,
            None => {
                let mut iter = req.uri().path().split("/");
                iter.next(); // first one is empty
                iter.next() // next one is bucket
                    .ok_or_else(|| s3_error!(InvalidURI, "No bucket set"))?
            }
        };
        let project_id = self
            .inner
            .cache
            .get_path(bucket)
            .ok_or_else(|| s3_error!(NoSuchBucket, "No bucket found with name {}", bucket))?;
        let (project, _) = self.inner.cache.get_resource(&project_id).map_err(|e| {
            error!("{e}");
            s3_error!(NoSuchBucket, "No bucket found with id {}", project_id)
        })?;

        // Convert Project key-value to CORSConfiguration
        let cors = project
            .read()
            .await
            .key_values
            .clone()
            .into_iter()
            .find(|kv| kv.key == "app.aruna-storage.org/cors")
            .map(|kv| kv.value)
            .ok_or_else(|| {
                s3_error!(
                    NoSuchBucketPolicy,
                    "No cors rules for bucket found {}",
                    bucket
                )
            })?;
        let cors: crate::structs::CORSConfiguration =
            serde_json::from_str(&cors).map_err(|_| {
                error!(error = "Unable to deserialize cors from JSON");
                s3_error!(InvalidObjectState, "Unable to deserialize cors from JSON")
            })?;

        // Check if request origin matches allowed headers in rule
        let rule = cors
            .0
            .iter()
            .find(|rule| {
                (rule.allowed_origins.contains(&origin.to_string())
                    || rule.allowed_origins.contains(&"*".to_string()))
                    && (rule.allowed_methods.contains(&req.method().to_string())
                        || req.method() == &Method::OPTIONS)
            })
            .ok_or_else(|| s3_error!(NoSuchBucketPolicy, "No cors policy found for bucket"))?;

        // Collect CORS headers
        let mut header_map = hyper::HeaderMap::new();
        if !rule.allowed_origins.contains(&"*".to_string()) {
            header_map.append(VARY, HeaderValue::from_static("Origin"));
            header_map.append(
                ACCESS_CONTROL_ALLOW_ORIGIN,
                HeaderValue::from_str(origin).map_err(|e| {
                    error!("{e}");
                    s3_error!(InvalidPolicyDocument, "Invalid header name for policy")
                })?,
            );
        } else {
            header_map.append(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
        }

        if let Some(headers) = &rule.allowed_headers {
            header_map.append(
                ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_str(&headers.join(",")).map_err(|e| {
                    error!("{e}");
                    s3_error!(InvalidPolicyDocument, "Invalid header name for policy")
                })?,
            );
        }
        header_map.append(
            ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_str(&rule.allowed_methods.join(",")).map_err(|e| {
                error!("{e}");
                s3_error!(InvalidPolicyDocument, "Invalid header name for policy")
            })?,
        );
        Ok(header_map)
    }

    fn modify_response_headers(
        &self,
        resp: Result<hyper::Response<Body>, S3Error>,
        origin_exception: bool,
        headers: Option<hyper::HeaderMap>,
    ) -> Result<hyper::Response<Body>, S3Error> {
        let res = resp.map(|mut r| {
            if r.headers().contains_key("Transfer-Encoding") {
                r.headers_mut().remove("Content-Length");
            }

            if let Some(headers) = headers {
                for (k, v) in headers {
                    if let Some(k) = k {
                        r.headers_mut().entry(k).or_insert(v);
                    }
                }
            }

            // Expose 'ETag' header if present
            if r.headers().contains_key("ETag") {
                r.headers_mut().append(
                    "Access-Control-Expose-Headers",
                    HeaderValue::from_static("ETag"),
                );
            }

            // Add CORS * if request origin matches exception regex
            if origin_exception {
                r.headers_mut()
                    .entry(ACCESS_CONTROL_ALLOW_ORIGIN)
                    .or_insert(HeaderValue::from_static("*"));
                r.headers_mut()
                    .entry(ACCESS_CONTROL_ALLOW_METHODS)
                    .or_insert(HeaderValue::from_static("*"));
                r.headers_mut()
                    .entry(ACCESS_CONTROL_ALLOW_HEADERS)
                    .or_insert(HeaderValue::from_static("*"));
            }

            // Workaround to return 206 (Partial Content) for range responses
            if r.headers().contains_key("Content-Range")
                && r.headers().contains_key("Accept-Ranges")
                && r.status().as_u16() == 200
            {
                let status = r.status_mut();
                *status = StatusCode::from_u16(206).unwrap();
            }

            r.map(Body::from)
        });

        res
    }
}
