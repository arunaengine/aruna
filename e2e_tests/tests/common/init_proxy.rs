use anyhow::anyhow;
use aruna_rust_api::api::dataproxy::services::v2::bundler_service_server::BundlerServiceServer;
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_ingestion_service_server::DataproxyIngestionServiceServer;
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_replication_service_server::DataproxyReplicationServiceServer;
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_user_service_server::DataproxyUserServiceServer;
use data_proxy::caching::cache::Cache;
use data_proxy::config::{Backend, Config, Frontend, Persistence, Proxy};
use data_proxy::data_backends::filesystem_backend::FSBackend;
use data_proxy::data_backends::s3_backend::S3Backend;
use data_proxy::data_backends::storage_backend::StorageBackend;
use data_proxy::grpc_api::bundler::BundlerServiceImpl;
use data_proxy::grpc_api::ingestion_service::DataproxyIngestionServiceImpl;
use data_proxy::grpc_api::proxy_service::DataproxyReplicationServiceImpl;
use data_proxy::grpc_api::user_service::DataproxyUserServiceImpl;
use data_proxy::replication::replication_handler::ReplicationHandler;
use data_proxy::s3_frontend;
use diesel_ulid::DieselUlid;
use futures_util::TryFutureExt;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU16;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::try_join;
use tonic::transport::{Channel, Server};
use tracing::{error, info_span, trace, Instrument};

pub const PROXY_01_DEFAULT_ULID: &str = "01H81W0ZMB54YEP5711Q2BK46V";
pub const PROXY_01_ENCODING_KEY: &str =
    "MC4CAQAwBQYDK2VwBCIEIM/FI+bYw+auSKGyGqeISRIEjofvZV/lbK7QL1wkuCey";
pub const PROXY_01_DECODING_KEY: &str =
    "MCowBQYDK2VwAyEAnouQBh4GHPCD/k85VIzPyCdOijVg2qlzt2TELwTMy4c=";
pub const PROXY_01_GRPC_PORT: u16 = 50052;
pub const PROXY_01_S3_PORT: u16 = 1337;

pub const PROXY_02_DEFAULT_ULID: &str = "01HG8P9FFW12TZ1YWX32A1KT28";
pub const PROXY_02_ENCODING_KEY: &str =
    "MC4CAQAwBQYDK2VwBCIEIBWCkwQIGKaEgpLVIcT9V1za1bqZBwcrcUad8ex9iVxV";
pub const PROXY_02_DECODING_KEY: &str =
    "MCowBQYDK2VwAyEAN77yqfv4hAdHTxrG576xqOgpqS8LuZUSjYXyJiUwfws=";
pub const PROXY_02_GRPC_PORT: u16 = 50055;
pub const PROXY_02_S3_PORT: u16 = 1338;

pub static PROXY_OFFSET: AtomicU16 = AtomicU16::new(0);

pub fn generate_config(
    id: DieselUlid,
    sec_key: impl Into<String>,
    pub_key: impl Into<String>,
    pub_key_serial: u16,
    grpc_port: u16,
    s3_port: u16,
    server_port: u16,
    db_name: impl Into<String>,
) -> Config {
    let proxy = Proxy {
        endpoint_id: id,
        private_key: Some(sec_key.into()),
        public_key: pub_key.into(),
        serial: pub_key_serial as i32,
        remote_synced: true,
        enable_ingest: true,
        admin_ids: vec![],
        aruna_url: Some(format!("http://localhost:{}", server_port)),
        grpc_server: format!("0.0.0.0:{}", grpc_port),
        replication_interval: Some(30),
    };

    let persistence = Persistence::Postgres {
        host: "localhost".to_string(),
        port: 5432,
        user: "postgres".to_string(),
        password: Some("postgres".to_string()),
        database: db_name.into(),
        schema: "../components/data_proxy/src/database/schema.sql".to_string(),
    };

    let frontend = Frontend {
        server: format!("0.0.0.0:{}", s3_port),
        hostname: format!("localhost:{}", s3_port),
        cors_exception: None,
    };

    let backend = Backend::S3 {
        host: Some("http://localhost:9000".to_string()),
        access_key: Some("minioadmin".to_string()),
        secret_key: Some("minioadmin".to_string()),
        encryption: true,
        compression: true,
        deduplication: true,
        force_path_style: Some(true),
        dropbox_bucket: None,
        backend_scheme: "s3://{{PROXY_ID}}-{{PROJECT_ID}}/{{PROJECT_NAME}}/{{COLLECTION_NAME}}/{{DATASET_NAME}}/{{RANDOM:10}}/{{OBJECT_NAME}}".to_string(),
        tmp: Some("aruna_temp".to_string()),
    };

    Config {
        proxy,
        persistence: Some(persistence),
        frontend: Some(frontend),
        backend,
        rules: None,
    }
}

pub async fn init_data_proxy(
    config: Config,
) -> anyhow::Result<JoinHandle<Result<(), anyhow::Error>>> {
    // Init storage backend
    let backend: Box<dyn StorageBackend> = match config.backend {
        Backend::S3 { .. } => Box::new(S3Backend::new(&config).await?),
        Backend::FileSystem { .. } => Box::new(FSBackend::new(&config).await?),
    };
    let backend_arc: Arc<Box<dyn StorageBackend>> = Arc::new(backend);

    // Init replication handler
    let (sender, receiver) = async_channel::bounded(1000);
    let cache = Cache::new(&config, sender.clone(), Some(backend_arc.clone())).await?;

    trace!("init replication handler");
    let replication_handler = ReplicationHandler::new(
        receiver,
        backend_arc.clone(),
        cache.clone(),
        config.proxy.clone(),
    );
    tokio::spawn(async move {
        let replication = replication_handler.run().await;
        if let Err(err) = &replication {
            trace!("{err}");
        };
    });

    // Init S3 server + frontend
    let cache_clone = cache.clone();
    let s3_server = if let Some(_frontend) = &config.frontend {
        Some(s3_frontend::s3server::S3Server::new(&config, None, cache, backend_arc.clone()).await?)
    } else {
        None
    };

    // Init gRPC server
    let proxy_grpc_addr = config.proxy.grpc_server.parse::<SocketAddr>()?;
    let proxy_config_clone = config.proxy.clone();
    let grpc_server_handle = tokio::spawn(
        async move {
            let mut builder = Server::builder()
                .http2_keepalive_interval(Some(Duration::from_secs(15)))
                .add_service(DataproxyReplicationServiceServer::new(
                    DataproxyReplicationServiceImpl::new(
                        proxy_config_clone,
                        cache_clone.clone(),
                        sender,
                        backend_arc.clone(),
                    ),
                ))
                .add_service(DataproxyUserServiceServer::new(
                    DataproxyUserServiceImpl::new(cache_clone.clone()),
                ));

            if config.proxy.enable_ingest {
                builder = builder.add_service(DataproxyIngestionServiceServer::new(
                    DataproxyIngestionServiceImpl::new(
                        cache_clone.clone(),
                        backend_arc,
                        config.proxy.admin_ids.clone(),
                    ),
                ));
            }

            if let Some(frontend) = &config.frontend {
                builder = builder.add_service(BundlerServiceServer::new(BundlerServiceImpl::new(
                    cache_clone.clone(),
                    frontend.hostname.to_string(),
                    true,
                )));
            };

            builder.serve(proxy_grpc_addr).await
        }
        .instrument(info_span!("grpc_server_run")),
    )
    .map_err(|e| {
        error!(error = ?e, msg = e.to_string());
        anyhow!("an error occurred {e}")
    });

    let handle = tokio::spawn(async move {
        if let Some(s3_server) = s3_server {
            match try_join!(s3_server.run(), grpc_server_handle) {
                Ok(_) => Ok(()),
                Err(err) => {
                    error!("{}", err);
                    Err(err)
                }
            }
        } else {
            grpc_server_handle.await??;
            Ok(())
        }
    });

    // Wait until data proxy accepts connections
    let mut retry_counter: u32 = 0;
    const RETRY_BASE: u64 = 2;
    const RETRIES_MAX: u32 = 10;
    let endpoint = Channel::from_shared(format!("http://{}", config.proxy.grpc_server)).unwrap();
    while let Err(_e) = endpoint.connect().await {
        if retry_counter >= RETRIES_MAX {
            handle.abort();
            anyhow::bail!("Connection retries to Server exceeded.")
        }
        retry_counter += 1;
        let retry_millis = RETRY_BASE.pow(retry_counter);
        tokio::time::sleep(Duration::from_millis(retry_millis)).await;
    }

    // Return stuff
    Ok(handle)
}
