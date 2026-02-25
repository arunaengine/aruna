use super::auth::AuthProvider;
use super::s3_service::ArunaS3Service;
use crate::error::S3ServerError;
use aruna_operations::driver::DriverContext;
use futures_core::future::BoxFuture;
use http::Request;
use hyper::body::Incoming;
use hyper::service::Service;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::host::SingleDomain;
use s3s::service::S3Service;
use s3s::service::S3ServiceBuilder;
use s3s::HttpError;
use s3s::HttpResponse;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::error;
use tracing::info;

pub struct S3Server {
    address: String,
    s3service: S3Service,
}

#[derive(Clone)]
pub struct WrappingService {
    shared: S3Service, // Aruna specific implementation of S3 trait
}

impl S3Server {
    #[tracing::instrument(level = "trace", skip(address, hostname, driver_ctx))]
    pub async fn new(
        address: impl Into<String> + Copy,
        hostname: impl Into<String>,
        driver_ctx: Arc<DriverContext>,
    ) -> Result<Self, S3ServerError> {
        let s3service = ArunaS3Service::new(driver_ctx.clone()).await;

        let auth = AuthProvider {
            _driver_ctx: driver_ctx.clone(),
        };

        let service = {
            let mut b = S3ServiceBuilder::new(s3service.clone());
            b.set_host(SingleDomain::new(&hostname.into())?);
            b.set_auth(auth.clone());
            b.set_access(auth);
            b.build()
        };

        Ok(Self {
            address: address.into(),
            s3service: service,
        })
    }
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn run(self) -> Result<JoinHandle<()>, S3ServerError> {
        // Run server
        let listener = TcpListener::bind(&self.address).await?;
        let local_addr = listener.local_addr()?;
        let service = WrappingService {
            shared: self.s3service,
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

        Ok(task)
    }
}

impl Service<Request<Incoming>> for WrappingService {
    type Response = HttpResponse;

    type Error = HttpError;

    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        //TODO: CORS

        // Default S3 operation call
        let (parts, body) = req.into_parts();
        let s3s_request = s3s::HttpRequest::from_parts(parts, body.into());
        let shared = self.shared.clone();
        Box::pin(async move { shared.call(s3s_request).await.map_err(Into::into) })
    }
}
