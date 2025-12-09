use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::dataproxy::services::v2::bundler_service_server::BundlerServiceServer;
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_ingestion_service_server::DataproxyIngestionServiceServer;
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_replication_service_server::DataproxyReplicationServiceServer;
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_user_service_server::DataproxyUserServiceServer;
use caching::cache::Cache;
use data_backends::{s3_backend::S3Backend, storage_backend::StorageBackend};
use futures_util::TryFutureExt;
use grpc_api::bundler::BundlerServiceImpl;
use grpc_api::{
    proxy_service::DataproxyReplicationServiceImpl, user_service::DataproxyUserServiceImpl,
};
use regex::Regex;
use std::panic;
use std::{net::SocketAddr, sync::Arc};
use tokio::try_join;
use tonic::transport::Server;
use tracing::error;
use tracing::info_span;
use tracing::trace;
use tracing::Instrument as _;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

mod bundler;
mod caching;
mod data_backends;
mod database;
mod replication;
mod s3_frontend;
// mod helpers;
mod grpc_api;
mod structs;
#[macro_use]
mod macros;
mod auth;
mod config;
mod helpers;

use crate::config::Config;
use crate::data_backends::filesystem_backend::FSBackend;
use crate::grpc_api::ingestion_service::DataproxyIngestionServiceImpl;
use crate::replication::replication_handler::ReplicationHandler;
use std::backtrace::Backtrace;
use std::time::Duration;

#[tracing::instrument(level = "trace", skip())]
#[tokio::main]
async fn main() -> Result<()> {
    panic::set_hook(Box::new(|info| {
        //let stacktrace = Backtrace::capture();
        let stacktrace = Backtrace::force_capture();
        println!("Got panic. @info:{info}\n@stackTrace:{stacktrace}");
        std::process::abort();
    }));

    dotenvy::from_filename(".env").ok();

    let tokio_env_filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("tokio=trace".parse().unwrap())
        .add_directive("runtime=trace".parse().unwrap());

    let logging_env_filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("data_proxy=trace".parse()?);

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .with_filter(logging_env_filter);

    let console_layer = console_subscriber::spawn().with_filter(tokio_env_filter);

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(console_layer)
        .init();

    trace!("init storage backend");

    // Load config from config.toml/env vars
    dotenvy::from_filename(".env").ok();
    let config_file = dotenvy::var("CONFIG").unwrap_or("config.toml".to_string());
    let mut config: Config = toml::from_str(std::fs::read_to_string(config_file)?.as_str())?;
    config.validate()?;

    // Evaluate CORS regex
    let cors_regex = if let Some(frontend) = &config.frontend {
        if let Some(cors_regex) = &frontend.cors_exception {
            Some(Regex::new(cors_regex).expect("CORS exception regex invalid"))
        } else {
            None
        }
    } else {
        None
    };

    // Init storage backend connection
    let backend: Box<dyn StorageBackend> = match config.backend {
        config::Backend::S3 { .. } => Box::new(S3Backend::new(&config).await?),
        config::Backend::FileSystem { .. } => Box::new(FSBackend::new(&config).await?),
    };
    let storage_backend: Arc<Box<dyn StorageBackend>> = Arc::new(backend);

    trace!("init cache");
    let (sender, receiver) = async_channel::bounded(1000);
    let cache = Cache::new(&config, sender.clone(), Some(storage_backend.clone())).await?;

    trace!("init replication handler");
    let replication_handler = ReplicationHandler::new(
        receiver,
        storage_backend.clone(),
        cache.clone(),
        config.proxy.clone(),
    );
    tokio::spawn(async move {
        let replication = replication_handler.run().await;
        if let Err(err) = replication {
            trace!("{err}");
        };
    });

    trace!("init s3 server");
    let cache_clone = cache.clone();
    let s3_server = if let Some(frontend) = &config.frontend {
        Some(
            s3_frontend::s3server::S3Server::new(
                &config,
                cors_regex,
                cache,
                storage_backend.clone(),
            )
            .await?,
        )
    } else {
        None
    };
    trace!("init grpc server");

    let proxy_grpc_addr = config.proxy.grpc_server.parse::<SocketAddr>()?;
    let grpc_server_handle = tokio::spawn(
        async move {
            let mut builder = Server::builder()
                .http2_keepalive_interval(Some(Duration::from_secs(15)))
                .add_service(DataproxyReplicationServiceServer::new(
                    DataproxyReplicationServiceImpl::new(
                        config.proxy.clone(),
                        cache_clone.clone(),
                        sender,
                        storage_backend.clone(),
                    ),
                ))
                .add_service(DataproxyUserServiceServer::new(
                    DataproxyUserServiceImpl::new(cache_clone.clone()),
                ));

            if config.proxy.enable_ingest {
                builder = builder.add_service(DataproxyIngestionServiceServer::new(
                    DataproxyIngestionServiceImpl::new(
                        cache_clone.clone(),
                        storage_backend,
                        config.proxy.admin_ids,
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
}
