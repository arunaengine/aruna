use aruna_core::errors::{BlobError, StagingSourceError};
use aruna_core::stream::BackendStream;
use aruna_core::structs::{
    Backend, BackendConfig, ResolvedSourceAccess, SourceConnectorKind, SourceMetadata,
};
use aruna_egress::{
    EgressPolicy, HTTP_SCHEMES, hardened_client, preflight_resolve, resolve_and_pin_ftp,
    validate_url,
};
use bytes::Bytes;
use opendal::layers::{HttpClientLayer, LoggingLayer, RetryLayer};
use opendal::raw::HttpClient;
use opendal::{Builder, Operator, services};
use reqwest::dns::Resolve;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

pub(crate) fn init_backend_operator(
    mut config: BackendConfig,
    bucket: String,
) -> Result<Operator, BlobError> {
    config
        .service_config
        .insert("root".to_string(), config.root);

    match config.backend_type {
        Backend::S3 => {
            config.service_config.insert("bucket".to_string(), bucket);
            build_service::<services::S3>(config.service_config)
                .map_err(blob_operator_creation_error)
        }
        Backend::HTTP => build_service::<services::Http>(config.service_config)
            .map_err(blob_operator_creation_error),
        Backend::Postgres => build_service::<services::Postgresql>(config.service_config)
            .map_err(blob_operator_creation_error),
        Backend::FileSystem => build_service::<services::Fs>(config.service_config)
            .map_err(blob_operator_creation_error),
    }
}

pub(crate) fn init_operator(
    backend_type: Backend,
    config: HashMap<String, String>,
) -> Result<Operator, BlobError> {
    match backend_type {
        Backend::S3 => build_service::<services::S3>(config).map_err(blob_operator_creation_error),
        Backend::HTTP => {
            build_service::<services::Http>(config).map_err(blob_operator_creation_error)
        }
        Backend::Postgres => {
            build_service::<services::Postgresql>(config).map_err(blob_operator_creation_error)
        }
        Backend::FileSystem => {
            build_service::<services::Fs>(config).map_err(blob_operator_creation_error)
        }
    }
}

pub(crate) async fn head_staging_source(
    access: &ResolvedSourceAccess,
    policy: &EgressPolicy,
    resolver_override: Option<Arc<dyn Resolve>>,
) -> Result<SourceMetadata, StagingSourceError> {
    preflight_staging(access, policy, resolver_override.clone()).await?;
    let (operator, path, version) =
        build_staging_source_operator(access, policy, resolver_override).await?;
    let metadata = match version {
        Some(version) => operator.stat_with(path).version(version).await,
        None => operator.stat(path).await,
    }
    .map_err(|error| map_staging_source_error(error, true))?;

    Ok(SourceMetadata {
        content_length: metadata.content_length(),
        content_type: metadata.content_type().map(ToOwned::to_owned),
        etag: metadata.etag().map(ToOwned::to_owned),
        last_modified: metadata.last_modified().map(Into::into),
        source_version: metadata.version().map(ToOwned::to_owned),
    })
}

pub(crate) async fn read_staging_source(
    access: &ResolvedSourceAccess,
    range: Option<std::ops::Range<u64>>,
    policy: &EgressPolicy,
    resolver_override: Option<Arc<dyn Resolve>>,
) -> Result<
    (
        SourceMetadata,
        BackendStream<Result<Bytes, aruna_core::stream::StreamError>>,
    ),
    StagingSourceError,
> {
    let metadata = head_staging_source(access, policy, resolver_override.clone()).await?;
    let (operator, path, version) =
        build_staging_source_operator(access, policy, resolver_override).await?;
    let reader = match version {
        Some(version) => operator.reader_with(path).version(version).await,
        None => operator.reader(path).await,
    }
    .map_err(|error| map_staging_source_error(error, false))?;
    let stream = match range {
        Some(range) => reader
            .into_bytes_stream(range)
            .await
            .map_err(|error| map_staging_source_error(error, false))?,
        None => reader
            .into_bytes_stream(..)
            .await
            .map_err(|error| map_staging_source_error(error, false))?,
    };

    Ok((metadata, BackendStream::new(stream)))
}

/// Request-time pre-flight for HTTP-family connectors so a denied endpoint
/// surfaces a precise `EgressDenied`; FTP is gated at build time by pinning.
async fn preflight_staging(
    access: &ResolvedSourceAccess,
    policy: &EgressPolicy,
    resolver_override: Option<Arc<dyn Resolve>>,
) -> Result<(), StagingSourceError> {
    let ResolvedSourceAccess::OpenDal { kind, config, .. } = access;
    match kind {
        SourceConnectorKind::Http | SourceConnectorKind::S3 | SourceConnectorKind::Webdav => {
            let url = staging_endpoint_url(config)?;
            preflight_resolve(policy, HTTP_SCHEMES, &url, resolver_override)
                .await
                .map_err(aruna_egress::EgressDenial::into_staging_error)
        }
        SourceConnectorKind::Ftp | SourceConnectorKind::ArunaNative => Ok(()),
    }
}

async fn build_staging_source_operator<'a>(
    access: &'a ResolvedSourceAccess,
    policy: &EgressPolicy,
    resolver_override: Option<Arc<dyn Resolve>>,
) -> Result<(Operator, &'a str, Option<&'a str>), StagingSourceError> {
    match access {
        ResolvedSourceAccess::OpenDal {
            kind,
            config,
            path,
            version,
        } => {
            let operator = match kind {
                SourceConnectorKind::Http => {
                    build_http_family::<services::Http>(config, policy, resolver_override)?
                }
                SourceConnectorKind::S3 => build_http_family::<services::S3>(
                    &pinned_s3_config(config),
                    policy,
                    resolver_override,
                )?,
                SourceConnectorKind::Webdav => {
                    build_http_family::<services::Webdav>(config, policy, resolver_override)?
                }
                SourceConnectorKind::Ftp => build_ftp_operator(config, policy).await?,
                SourceConnectorKind::ArunaNative => {
                    return Err(StagingSourceError::UnsupportedKind(kind.to_string()));
                }
            };
            Ok((operator, path.as_str(), version.as_deref()))
        }
    }
}

/// Connector S3 clients must only ever sign with credentials from the
/// connector config; ambient discovery (env/profile/IMDS) bypasses the
/// hardened resolver and could reach the instance metadata service.
fn pinned_s3_config(config: &HashMap<String, String>) -> HashMap<String, String> {
    let mut config = config.clone();
    config.insert("disable_config_load".to_string(), "true".to_string());
    config.insert("disable_ec2_metadata".to_string(), "true".to_string());
    config
}

fn staging_endpoint_url(config: &HashMap<String, String>) -> Result<Url, StagingSourceError> {
    let endpoint = config.get("endpoint").ok_or_else(|| {
        StagingSourceError::OperatorCreationFailed("connector config is missing endpoint".into())
    })?;
    Url::parse(endpoint).map_err(|error| StagingSourceError::EgressDenied {
        host: None,
        port: None,
        scheme: None,
        reason: format!("invalid endpoint URL: {error}"),
    })
}

fn build_http_family<B>(
    config: &HashMap<String, String>,
    policy: &EgressPolicy,
    resolver_override: Option<Arc<dyn Resolve>>,
) -> Result<Operator, StagingSourceError>
where
    B: Builder,
{
    let url = staging_endpoint_url(config)?;
    validate_url(policy, HTTP_SCHEMES, &url)
        .map_err(aruna_egress::EgressDenial::into_staging_error)?;
    let client = hardened_client(Arc::new(policy.clone()), HTTP_SCHEMES, resolver_override)
        .map_err(|error| StagingSourceError::OperatorCreationFailed(error.to_string()))?;
    build_staging_service::<B>(config.clone(), client).map_err(staging_operator_creation_error)
}

async fn build_ftp_operator(
    config: &HashMap<String, String>,
    policy: &EgressPolicy,
) -> Result<Operator, StagingSourceError> {
    let endpoint = config.get("endpoint").ok_or_else(|| {
        StagingSourceError::OperatorCreationFailed("connector config is missing endpoint".into())
    })?;
    // FTP is allowlist-only; resolve-and-pin rewrites the host to a validated IP
    // literal (PASV data channel stays server-controlled — known limitation).
    let pinned = resolve_and_pin_ftp(policy, endpoint, None)
        .await
        .map_err(aruna_egress::EgressDenial::into_staging_error)?;
    let mut config = config.clone();
    config.insert("endpoint".to_string(), pinned);
    build_service::<services::Ftp>(config).map_err(staging_operator_creation_error)
}

fn build_service<B>(config: HashMap<String, String>) -> Result<Operator, String>
where
    B: Builder,
{
    Ok(Operator::from_iter::<B>(config)
        .map_err(|error| error.to_string())?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::new())
        .finish())
}

fn build_staging_service<B>(
    config: HashMap<String, String>,
    client: reqwest::Client,
) -> Result<Operator, String>
where
    B: Builder,
{
    Ok(Operator::from_iter::<B>(config)
        .map_err(|error| error.to_string())?
        .layer(HttpClientLayer::new(HttpClient::with(client)))
        .layer(LoggingLayer::default())
        .layer(RetryLayer::new())
        .finish())
}

fn blob_operator_creation_error(error: String) -> BlobError {
    BlobError::OperatorCreationFailed(error)
}

fn staging_operator_creation_error(error: String) -> StagingSourceError {
    StagingSourceError::OperatorCreationFailed(error)
}

fn map_staging_source_error(error: opendal::Error, stat: bool) -> StagingSourceError {
    if error.kind() == opendal::ErrorKind::NotFound {
        return StagingSourceError::NotFound;
    }

    if stat {
        StagingSourceError::StatError(error.to_string())
    } else {
        StagingSourceError::ReadError(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // Building the operator performs no I/O, so a public hostname passes the
    // static gate and the path/version tuple is handed through untouched.
    #[tokio::test]
    async fn path_version_passthrough() {
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::from([("endpoint".to_string(), "https://example.org".to_string())]),
            path: "file.txt".to_string(),
            version: Some("v42".to_string()),
        };

        let (.., path, version) =
            build_staging_source_operator(&access, &EgressPolicy::deny_all(), None)
                .await
                .unwrap();
        assert_eq!(path, "file.txt");
        assert_eq!(version, Some("v42"));
    }

    #[tokio::test]
    async fn http_literal_denied() {
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::from([("endpoint".to_string(), "http://127.0.0.1:9000".to_string())]),
            path: "file.txt".to_string(),
            version: None,
        };

        let error = build_staging_source_operator(&access, &EgressPolicy::deny_all(), None)
            .await
            .unwrap_err();
        assert!(matches!(error, StagingSourceError::EgressDenied { .. }));
    }

    #[tokio::test]
    async fn ftp_pins_literal() {
        use aruna_core::structs::{EgressAllowRule, EgressConfig, HostPattern};

        let policy = EgressPolicy::from_config(&EgressConfig {
            allow: vec![EgressAllowRule {
                host: HostPattern::Cidr("10.0.0.0/8".to_string()),
                ports: None,
                schemes: None,
                comment: None,
            }],
        });
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Ftp,
            config: HashMap::from([
                ("endpoint".to_string(), "ftp://10.0.0.7:21".to_string()),
                ("root".to_string(), "/datasets".to_string()),
                ("user".to_string(), "alice".to_string()),
                ("password".to_string(), "secret".to_string()),
            ]),
            path: "run-1/data.txt".to_string(),
            version: None,
        };

        let (.., path, version) = build_staging_source_operator(&access, &policy, None)
            .await
            .unwrap();
        assert_eq!(path, "run-1/data.txt");
        assert_eq!(version, None);
    }

    #[tokio::test]
    async fn ftp_not_allowlisted() {
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Ftp,
            config: HashMap::from([("endpoint".to_string(), "ftp://10.0.0.7:21".to_string())]),
            path: "run-1/data.txt".to_string(),
            version: None,
        };

        let error = build_staging_source_operator(&access, &EgressPolicy::deny_all(), None)
            .await
            .unwrap_err();
        assert!(matches!(error, StagingSourceError::EgressDenied { .. }));
    }

    // build_service also backs non-HTTP services like Fs.
    #[tokio::test]
    async fn fs_service_stat() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_str().unwrap().to_string();
        tokio::fs::write(dir.path().join("hello.txt"), b"hello world")
            .await
            .unwrap();

        let operator =
            build_service::<services::Fs>(HashMap::from([("root".to_string(), root)])).unwrap();
        let metadata = operator.stat("hello.txt").await.unwrap();
        assert_eq!(metadata.content_length(), 11);
    }

    struct StaticResolver(std::net::IpAddr);

    impl Resolve for StaticResolver {
        fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
            let addr = std::net::SocketAddr::new(self.0, 0);
            Box::pin(async move { Ok(Box::new(std::iter::once(addr)) as reqwest::dns::Addrs) })
        }
    }

    fn http_access(endpoint: &str) -> ResolvedSourceAccess {
        ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::from([("endpoint".to_string(), endpoint.to_string())]),
            path: "file.txt".to_string(),
            version: None,
        }
    }

    fn allow_loopback_policy() -> EgressPolicy {
        use aruna_core::structs::{EgressAllowRule, EgressConfig, HostPattern};
        EgressPolicy::from_config(&EgressConfig {
            allow: vec![EgressAllowRule {
                host: HostPattern::Cidr("127.0.0.0/8".to_string()),
                ports: None,
                schemes: None,
                comment: None,
            }],
        })
    }

    // A credential-less S3 connector must never fall back to ambient discovery;
    // reqsign honors AWS_EC2_METADATA_SERVICE_ENDPOINT, so an IMDS lookup would
    // land on the counting listener.
    #[tokio::test]
    async fn s3_skips_imds() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let metadata_endpoint = format!("http://{}", listener.local_addr().unwrap());
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });

        unsafe {
            std::env::set_var("AWS_EC2_METADATA_SERVICE_ENDPOINT", &metadata_endpoint);
            std::env::set_var("AWS_CONFIG_FILE", "/nonexistent");
            std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent");
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        }

        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::S3,
            config: HashMap::from([
                ("endpoint".to_string(), "http://127.0.0.1:9".to_string()),
                ("bucket".to_string(), "reads".to_string()),
                ("region".to_string(), "us-east-1".to_string()),
            ]),
            path: "file.txt".to_string(),
            version: None,
        };

        let result = head_staging_source(&access, &allow_loopback_policy(), None).await;
        assert!(result.is_err(), "expected sign failure, got {result:?}");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(hits.load(Ordering::SeqCst), 0, "IMDS was contacted");
    }

    // A rebinding hostname resolving to loopback must be denied through the real
    // opendal path even when a server is live at that address.
    #[tokio::test]
    async fn head_blocks_rebinding() {
        let resolver: Arc<dyn Resolve> = Arc::new(StaticResolver("127.0.0.1".parse().unwrap()));
        let access = http_access("http://rebind.test/");
        let error = head_staging_source(&access, &EgressPolicy::deny_all(), Some(resolver))
            .await
            .unwrap_err();
        assert!(matches!(error, StagingSourceError::EgressDenied { .. }));
    }

    // The same rebinding hostname passes the egress gate once loopback is
    // allowlisted, reaching the live server instead of an EgressDenied.
    #[tokio::test]
    async fn head_allows_allowlisted() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let router = axum::Router::new().fallback(|| async { "hello world" });
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let resolver: Arc<dyn Resolve> = Arc::new(StaticResolver("127.0.0.1".parse().unwrap()));
        let access = http_access(&format!("http://rebind.test:{port}/"));
        let result = head_staging_source(&access, &allow_loopback_policy(), Some(resolver)).await;
        assert!(
            !matches!(result, Err(StagingSourceError::EgressDenied { .. })),
            "allowlisted host must pass the egress gate, got {result:?}"
        );
    }
}
