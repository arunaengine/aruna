use super::s3service::ArunaS3Service;
use crate::IOHandler;
use crate::api_s3::auth::AuthProvider;
use crate::config::config::Frontend;
use anyhow::Result;
use aruna_net::Kademlia;
use aruna_net::actor_handle::NetworkActorHandle;
use aruna_storage::storage::lmdb::LmdbStore;
use aruna_storage::storage::store::Store;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use http::HeaderValue;
use http::Method;
use http::StatusCode;
use hyper::service::Service;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use iroh::{NodeAddr, NodeId};
use opendal::Operator;
use s3s::S3Error;
use s3s::auth::{S3Auth, SecretKey};
use s3s::s3_error;
use s3s::service::S3Service;
use s3s::service::S3ServiceBuilder;
use s3s::service::SharedS3Service;
use s3s::{Body, S3Result};
use std::convert::Infallible;
use std::future::Ready;
use std::future::ready;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;
use tracing::{debug, error};

pub struct S3Server {
    s3service: S3Service,
    address: String,
    node_id: NodeId,
}

#[derive(Clone)]
pub struct WrappingService(SharedS3Service);

impl S3Server {
    #[tracing::instrument(level = "trace", skip(backend))]
    pub async fn new(
        //address: impl Into<String> + Copy,
        //hostname: impl Into<String>,
        frontend: Frontend,
        backend: Arc<IOHandler<LmdbStore>>,
        node_id: NodeId,
    ) -> Result<Self> {
        let s3service = ArunaS3Service::new(backend.clone(), node_id).await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            tonic::Status::unauthenticated(e.to_string())
        })?;

        let local_address = frontend.server.clone();

        let service = {
            let mut b = S3ServiceBuilder::new(s3service);
            b.set_host(frontend);
            b.set_auth(AuthProvider {
                store: backend.store.clone()
            });
            b.build()
        };

        Ok(Self {
            s3service: service,
            address: local_address,
            node_id,
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

        let service = WrappingService(self.s3service.into_shared());

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
        debug!("Received request: {:#?}", req);

        // Catch OPTIONS requests
        if req.method() == Method::OPTIONS {
            let resp = Box::pin(async {
                hyper::Response::builder()
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Access-Control-Allow-Methods", "*")
                    .header("Access-Control-Allow-Headers", "*")
                    .body(Body::empty())
                    .map_err(|_| s3_error!(InvalidRequest, "Invalid OPTIONS request"))
            });

            return resp;
        }

        // Check if response gets CORS header pass
        /*
        let mut origin_exception = false;
        if let Some(origin) = req.headers().get("Origin") {
            if let Some(cors_regex) = &*CORS_REGEX {
                origin_exception =
                    cors_regex.is_match(origin.to_str().expect("Invalid Origin header"));
            }
        }
        */

        let service = self.0.clone();
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
                //if origin_exception {
                r.headers_mut()
                    .append("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
                r.headers_mut().append(
                    "Access-Control-Allow-Methods",
                    HeaderValue::from_static("*"),
                );
                r.headers_mut().append(
                    "Access-Control-Allow-Headers",
                    HeaderValue::from_static("*"),
                );
                //}

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
        self.0.as_ref()
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
