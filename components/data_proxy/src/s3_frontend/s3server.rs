use super::auth::AuthProvider;
use super::s3service::ArunaS3Service;
use crate::caching::cache;
use crate::data_backends::storage_backend::StorageBackend;
use crate::CORS_REGEX;
use anyhow::Result;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use http::header::ACCESS_CONTROL_ALLOW_HEADERS;
use http::header::ACCESS_CONTROL_ALLOW_METHODS;
use http::header::ACCESS_CONTROL_ALLOW_ORIGIN;
use http::header::ORIGIN;
use http::HeaderValue;
use http::Method;
use http::StatusCode;
use hyper::service::Service;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::header::HOST;
use s3s::s3_error;
use s3s::service::S3Service;
use s3s::service::S3ServiceBuilder;
use s3s::service::SharedS3Service;
use s3s::Body;
use s3s::S3Error;
use std::convert::Infallible;
use std::future::ready;
use std::future::Ready;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::error;
use tracing::info;

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

        let service = {
            let mut b = S3ServiceBuilder::new(s3service.clone());
            b.set_base_domain(hostname);
            b.set_auth(AuthProvider::new(cache).await);
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
        // Catch OPTIONS requests
        if req.method() == Method::OPTIONS {
            let clone = self.clone();
            return Box::pin(async move {
                match clone.get_bucket_cors(req).await {
                    Ok(resp) => Ok(resp),
                    Err(err) => {
                        error!("{err}");
                        hyper::Response::builder()
                            .body(Body::empty())
                            .map_err(|_| s3_error!(InvalidRequest, "Invalid OPTIONS request"))
                    }
                }
            });
        }

        // Check if response gets CORS header pass
        let mut origin_exception = false;
        if let Some(origin) = req.headers().get("Origin") {
            if let Some(cors_regex) = &*CORS_REGEX {
                origin_exception =
                    cors_regex.is_match(origin.to_str().expect("Invalid Origin header"));
            }
        }

        let service = self.shared.clone();
        let resp = service.call(req);
        let res = resp.map(move |r| {
            r.map(|mut r| {
                if r.headers().contains_key("Transfer-Encoding") {
                    r.headers_mut().remove("Content-Length");
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
            })
        });
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
    async fn get_bucket_cors(
        &self,
        req: hyper::Request<hyper::body::Incoming>,
    ) -> Result<hyper::Response<Body>, S3Error> {
        let origin = req
            .headers()
            .get(ORIGIN)
            .ok_or_else(|| s3_error!(InvalidURI, "No authority provided in URI"))?
            .to_str()
            .map_err(|e| {
                error!("{e}");
                s3_error!(InvalidURI, "Invalid URI encoding")
            })?;
        let authority = req
            .headers()
            .get(HOST)
            .ok_or_else(|| s3_error!(InvalidURI, "No authority provided in URI"))?;
        let bucket = authority
            .to_str()
            .map_err(|e| {
                error!("{e}");
                s3_error!(InvalidURI, "Invalid URI encoding")
            })?
            .split(".")
            .next()
            .ok_or_else(|| s3_error!(InvalidURI, "No bucket found in URI"))?;
        let project_id = self
            .inner
            .cache
            .get_path(bucket)
            .ok_or_else(|| s3_error!(NoSuchBucket, "No bucket found with name {}", bucket))?;
        let (project, _) = self.inner.cache.get_resource(&project_id).map_err(|e| {
            error!("{e}");
            s3_error!(NoSuchBucket, "No bucket found with id {}", project_id)
        })?;
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
        let rule = cors
            .0
            .iter()
            .find(|rule| {
                rule.allowed_origins.contains(&origin.to_string())
                    || rule.allowed_origins.contains(&"*".to_string())
            })
            .ok_or_else(|| s3_error!(NoSuchBucketPolicy, "No cors policy found for bucket"))?;
        let mut builder = hyper::Response::builder();
        for origin in rule.allowed_origins.iter() {
            builder = builder.header(
                ACCESS_CONTROL_ALLOW_ORIGIN,
                HeaderValue::from_str(origin).map_err(|e| {
                    error!("{e}");
                    s3_error!(InvalidPolicyDocument, "Invalid header name for policy")
                })?,
            );
        }
        if let Some(headers) = &rule.allowed_headers {
            builder = builder.header(
                ACCESS_CONTROL_ALLOW_HEADERS,
                HeaderValue::from_str(&headers.join(",")).map_err(|e| {
                    error!("{e}");
                    s3_error!(InvalidPolicyDocument, "Invalid header name for policy")
                })?,
            )
        }
        let resp = builder
            .header(
                ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_str(&rule.allowed_methods.join(",")).map_err(|e| {
                    error!("{e}");
                    s3_error!(InvalidPolicyDocument, "Invalid header name for policy")
                })?,
            )
            .body(Body::empty())
            .map_err(|_| s3_error!(InvalidRequest, "Invalid OPTIONS request"))?;
        Ok(resp)
    }
}
