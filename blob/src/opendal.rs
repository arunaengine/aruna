use crate::egress::EgressGuard;
use aruna_core::errors::{BlobError, StagingSourceError};
use aruna_core::stream::BackendStream;
use aruna_core::structs::{
    Backend, GroupBackendKind, ResolvedSourceAccess, SourceConnectorKind, SourceMetadata,
};
use bytes::Bytes;
use futures::TryStreamExt;
use opendal::layers::{HttpClientLayer, LoggingLayer, RetryLayer};
use opendal::{Builder, EntryMode, Operator, services};
use std::collections::HashMap;

pub(crate) async fn abort_partial_writer(
    writer: &mut opendal::Writer,
    operator: &Operator,
    storage_path: &str,
) {
    if let Err(err) = writer.abort().await {
        tracing::warn!(error = %err, "failed to abort partial blob writer; deleting output");
        if let Err(delete_err) = operator.delete(storage_path).await {
            tracing::warn!(error = %delete_err, "failed to delete partial blob output");
        }
    }
}

/// Tenant backends always build through the guarded client; operator backends
/// are node-local topology and keep the direct one.
pub(crate) fn init_operator(
    backend_type: Backend,
    config: HashMap<String, String>,
    guard: &EgressGuard,
) -> Result<Operator, BlobError> {
    match backend_type {
        Backend::S3 => build_service::<services::S3>(s3_operator_config(config), None)
            .map_err(blob_operator_creation_error),
        Backend::FileSystem => {
            build_service::<services::Fs>(config, None).map_err(blob_operator_creation_error)
        }
        Backend::Group(kind) => {
            build_group_service(kind, config, guard).map_err(blob_operator_creation_error)
        }
    }
}

/// Every tenant build pins the provider's ambient-credential switches where the
/// service exposes them; the others rely on the mandatory static credential.
pub(crate) fn build_group_service(
    kind: GroupBackendKind,
    config: HashMap<String, String>,
    guard: &EgressGuard,
) -> Result<Operator, String> {
    let layer = Some(guard.layer());
    match kind {
        GroupBackendKind::S3 => build_service::<services::S3>(s3_operator_config(config), layer),
        GroupBackendKind::Gcs => build_service::<services::Gcs>(gcs_operator_config(config), layer),
        GroupBackendKind::Azblob => build_service::<services::Azblob>(config, layer),
        GroupBackendKind::Azdls => build_service::<services::Azdls>(config, layer),
        GroupBackendKind::B2 => build_service::<services::B2>(config, layer),
    }
}

// gcs is the only tenant kind with explicit kill-switches; both are forced so
// neither the node's gcloud config nor its VM metadata identity can be used.
fn gcs_operator_config(mut config: HashMap<String, String>) -> HashMap<String, String> {
    config.insert("disable_config_load".to_string(), "true".to_string());
    config.insert("disable_vm_metadata".to_string(), "true".to_string());
    config
}

pub(crate) async fn check_staging_source(
    guard: &EgressGuard,
    access: &ResolvedSourceAccess,
) -> Result<(), StagingSourceError> {
    let (operator, ..) = build_source_operator(guard, access).await?;
    let ResolvedSourceAccess::OpenDal { kind, .. } = access;
    check_operator(&operator, *kind).await
}

async fn check_operator(
    operator: &Operator,
    kind: SourceConnectorKind,
) -> Result<(), StagingSourceError> {
    let result = if kind == SourceConnectorKind::Http {
        match operator.stat("__aruna_connector_check__").await {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    } else {
        operator.check().await
    };
    result.map_err(|error| StagingSourceError::CheckError(error.to_string()))
}

pub(crate) async fn head_staging_source(
    guard: &EgressGuard,
    access: &ResolvedSourceAccess,
) -> Result<SourceMetadata, StagingSourceError> {
    let (operator, path, version) = build_source_operator(guard, access).await?;
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
    guard: &EgressGuard,
    access: &ResolvedSourceAccess,
    range: Option<std::ops::Range<u64>>,
) -> Result<
    (
        SourceMetadata,
        BackendStream<Result<Bytes, aruna_core::stream::StreamError>>,
    ),
    StagingSourceError,
> {
    let (operator, path, version) = build_source_operator(guard, access).await?;
    let metadata = head_staging_source(guard, access).await?;
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

pub(crate) async fn list_staging_source(
    guard: &EgressGuard,
    access: &ResolvedSourceAccess,
    offset: usize,
    limit: usize,
    recursive: bool,
    files_only: bool,
) -> Result<(Vec<aruna_core::structs::SourceEntry>, bool), StagingSourceError> {
    let ResolvedSourceAccess::OpenDal {
        kind, config, path, ..
    } = access;
    if *kind == SourceConnectorKind::Http {
        // opendal's Http service cannot list; walk autoindex pages instead.
        return crate::autoindex::list_http_autoindex(
            guard, config, path, offset, limit, recursive, files_only,
        )
        .await;
    }
    let (operator, path, ..) = build_source_operator(guard, access).await?;
    list_operator(&operator, path, offset, limit, recursive, files_only).await
}

async fn list_operator(
    operator: &Operator,
    path: &str,
    offset: usize,
    limit: usize,
    recursive: bool,
    files_only: bool,
) -> Result<(Vec<aruna_core::structs::SourceEntry>, bool), StagingSourceError> {
    let mut lister = operator
        .lister_with(path)
        .recursive(recursive)
        .await
        .map_err(|error| StagingSourceError::ListError(error.to_string()))?;
    let mut entries = Vec::with_capacity(limit);
    let mut skipped = 0usize;

    while let Some(entry) = lister
        .try_next()
        .await
        .map_err(|error| StagingSourceError::ListError(error.to_string()))?
    {
        if entry.metadata().is_dir()
            && entry.path().trim_end_matches('/') == path.trim_end_matches('/')
        {
            continue;
        }
        let kind = match entry.metadata().mode() {
            EntryMode::FILE => aruna_core::structs::SourceEntryKind::File,
            EntryMode::DIR if !files_only => aruna_core::structs::SourceEntryKind::Directory,
            EntryMode::DIR | EntryMode::Unknown => continue,
        };
        if skipped < offset {
            skipped += 1;
            continue;
        }
        if entries.len() == limit {
            return Ok((entries, true));
        }

        entries.push(aruna_core::structs::SourceEntry {
            name: entry.name().trim_end_matches('/').to_string(),
            path: entry.path().trim_end_matches('/').to_string(),
            kind,
            size: (kind == aruna_core::structs::SourceEntryKind::File)
                .then(|| entry.metadata().content_length()),
            modified: entry.metadata().last_modified().map(Into::into),
        });
    }

    Ok((entries, false))
}

/// Builds the guarded opendal operator for one resolved staging source.
async fn build_source_operator<'access>(
    guard: &EgressGuard,
    access: &'access ResolvedSourceAccess,
) -> Result<(Operator, &'access str, Option<&'access str>), StagingSourceError> {
    match access {
        ResolvedSourceAccess::OpenDal {
            kind,
            config,
            path,
            version,
        } => {
            let operator = match kind {
                SourceConnectorKind::Http => {
                    build_service::<services::Http>(config.clone(), Some(guard.layer()))
                        .map_err(staging_operator_creation_error)?
                }
                SourceConnectorKind::S3 => build_service::<services::S3>(
                    s3_operator_config(config.clone()),
                    Some(guard.layer()),
                )
                .map_err(staging_operator_creation_error)?,
                SourceConnectorKind::Webdav => {
                    build_service::<services::Webdav>(config.clone(), Some(guard.layer()))
                        .map_err(staging_operator_creation_error)?
                }
                // opendal's ftp service never touches an http client, so a
                // preflight screen is the only control available here.
                SourceConnectorKind::Ftp => {
                    let endpoint = config.get("endpoint").ok_or_else(|| {
                        StagingSourceError::OperatorCreationFailed(
                            "ftp connector endpoint is missing".to_string(),
                        )
                    })?;
                    guard.screen(endpoint).await?;
                    build_service::<services::Ftp>(config.clone(), None)
                        .map_err(staging_operator_creation_error)?
                }
                SourceConnectorKind::ArunaNative => {
                    return Err(StagingSourceError::UnsupportedKind(kind.to_string()));
                }
            };
            Ok((operator, path.as_str(), version.as_deref()))
        }
    }
}

fn build_service<B>(
    config: HashMap<String, String>,
    guard: Option<HttpClientLayer>,
) -> Result<Operator, String>
where
    B: Builder,
{
    let builder = Operator::from_iter::<B>(config)
        .map_err(|error| error.to_string())?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::new());
    Ok(match guard {
        Some(guard) => builder.layer(guard).finish(),
        None => builder.finish(),
    })
}

// reqsign resolves credentials lazily on the first request and on every retry,
// so ambient AWS config and EC2 metadata lookups must be disabled in the config
// itself. `force_path_style` is our key; opendal speaks `enable_virtual_host_style`.
fn s3_operator_config(mut config: HashMap<String, String>) -> HashMap<String, String> {
    config.insert("disable_config_load".to_string(), "true".to_string());
    config.insert("disable_ec2_metadata".to_string(), "true".to_string());
    let path_style = config
        .remove("force_path_style")
        .map(|value| value.trim().parse::<bool>().unwrap_or(true))
        .unwrap_or(true);
    config.insert(
        "enable_virtual_host_style".to_string(),
        (!path_style).to_string(),
    );
    config
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
    use aruna_core::egress::EgressPolicy;
    use std::sync::Arc;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    fn test_guard() -> EgressGuard {
        EgressGuard::new(EgressPolicy::loopback()).unwrap()
    }

    /// Ambient AWS inputs cleared before a zero-connect proof, so no provider
    /// ahead of the metadata slot can short-circuit the chain.
    const AWS_ENV_KEYS: [&str; 11] = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_PROFILE",
        "AWS_ROLE_ARN",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        "AWS_EC2_METADATA_DISABLED",
    ];

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn swap_env(entries: &[(&str, Option<String>)]) -> Vec<(String, Option<String>)> {
        entries
            .iter()
            .map(|(key, value)| {
                let previous = std::env::var(key).ok();
                match value {
                    Some(value) => unsafe { std::env::set_var(key, value) },
                    None => unsafe { std::env::remove_var(key) },
                }
                ((*key).to_string(), previous)
            })
            .collect()
    }

    fn restore_env(previous: Vec<(String, Option<String>)>) {
        for (key, value) in previous {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }

    /// Loopback endpoint that counts inbound connections and answers 404, so a
    /// credential fetch that escapes hardening is observable as an accept.
    struct CountingListener {
        endpoint: String,
        hits: Arc<AtomicUsize>,
        task: tokio::task::JoinHandle<()>,
    }

    impl CountingListener {
        async fn bind() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let hits = Arc::new(AtomicUsize::new(0));
            let counter = hits.clone();
            let task = tokio::spawn(async move {
                while let Ok((mut socket, _)) = listener.accept().await {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await;
                    let _ = socket.shutdown().await;
                }
            });
            Self {
                endpoint,
                hits,
                task,
            }
        }

        fn hits(&self) -> usize {
            self.hits.load(Ordering::SeqCst)
        }
    }

    impl Drop for CountingListener {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[tokio::test]
    async fn s3_skips_imds() {
        // Counterfactual: without the lockdown keys the metadata port accepts.
        let _guard = env_lock().lock().await;
        let metadata = CountingListener::bind().await;
        let data = CountingListener::bind().await;
        let empty = tempdir().unwrap();
        let missing = empty.path().join("absent").to_string_lossy().into_owned();

        let mut entries: Vec<(&str, Option<String>)> =
            AWS_ENV_KEYS.iter().map(|key| (*key, None)).collect();
        entries.push(("AWS_CONFIG_FILE", Some(missing.clone())));
        entries.push(("AWS_SHARED_CREDENTIALS_FILE", Some(missing)));
        entries.push((
            "AWS_EC2_METADATA_SERVICE_ENDPOINT",
            Some(metadata.endpoint.clone()),
        ));
        let previous = swap_env(&entries);

        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::S3,
            config: HashMap::from([
                ("bucket".to_string(), "reads".to_string()),
                ("endpoint".to_string(), data.endpoint.clone()),
                ("region".to_string(), "eu-central-1".to_string()),
            ]),
            path: "file.txt".to_string(),
            version: None,
        };
        let (operator, path, ..) = build_source_operator(&test_guard(), &access).await.unwrap();
        let _ = operator.stat(path).await;

        restore_env(previous);
        assert_eq!(metadata.hits(), 0);
    }

    const AZURE_ENV_KEYS: [&str; 8] = [
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_STORAGE_ACCOUNT_KEY",
        "AZURE_STORAGE_SAS_TOKEN",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
        "AZURE_CLIENT_CERTIFICATE_PATH",
        "AZURE_AUTHORITY_HOST",
    ];

    fn group_config(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[tokio::test]
    async fn gcs_skips_metadata() {
        // Counterfactual: the same build without the two forced kill-switches
        // walks the VM metadata service, which the hardened build never does.
        let _guard = env_lock().lock().await;
        let metadata = CountingListener::bind().await;
        let data = CountingListener::bind().await;
        let host = metadata.endpoint.trim_start_matches("http://").to_string();
        // APPDATA short-circuits the well-known-file lookup onto an empty dir,
        // so no gcloud credential on the machine can pre-empt the chain.
        let empty = tempdir().unwrap();
        let previous = swap_env(&[
            ("GCE_METADATA_HOST", Some(host)),
            ("GOOGLE_APPLICATION_CREDENTIALS", None),
            ("APPDATA", Some(empty.path().to_string_lossy().into_owned())),
        ]);
        let config = group_config(&[("bucket", "data"), ("endpoint", &data.endpoint)]);

        let unhardened =
            build_service::<services::Gcs>(config.clone(), Some(test_guard().layer())).unwrap();
        let _ = unhardened.stat("probe").await;
        let ambient = metadata.hits();

        let hardened = build_group_service(GroupBackendKind::Gcs, config, &test_guard()).unwrap();
        let _ = hardened.stat("probe").await;

        restore_env(previous);
        assert!(
            ambient > 0,
            "counterfactual never reached the metadata host"
        );
        assert_eq!(metadata.hits(), ambient);
    }

    #[tokio::test]
    async fn azblob_skips_ambient() {
        // Azure has no kill-switch: the static key is what keeps the ambient
        // chain unreachable, because it is pushed ahead of every provider.
        let _guard = env_lock().lock().await;
        let authority = CountingListener::bind().await;
        let data = CountingListener::bind().await;
        let mut entries: Vec<(&str, Option<String>)> =
            AZURE_ENV_KEYS.iter().map(|key| (*key, None)).collect();
        entries.push(("AZURE_TENANT_ID", Some("tenant".to_string())));
        entries.push(("AZURE_CLIENT_ID", Some("client".to_string())));
        entries.push(("AZURE_CLIENT_SECRET", Some("secret".to_string())));
        entries.push(("AZURE_AUTHORITY_HOST", Some(authority.endpoint.clone())));
        let previous = swap_env(&entries);
        let base = [
            ("container", "data"),
            ("endpoint", data.endpoint.as_str()),
            ("account_name", "acct"),
        ];

        let ambient_only =
            build_group_service(GroupBackendKind::Azblob, group_config(&base), &test_guard())
                .unwrap();
        let _ = ambient_only.stat("probe").await;
        let ambient = authority.hits();

        let mut with_key = group_config(&base);
        with_key.insert("account_key".to_string(), "a2V5c2VjcmV0".to_string());
        let hardened =
            build_group_service(GroupBackendKind::Azblob, with_key, &test_guard()).unwrap();
        let _ = hardened.stat("probe").await;

        restore_env(previous);
        assert!(
            ambient > 0,
            "counterfactual never reached the authority host"
        );
        assert_eq!(authority.hits(), ambient);
    }

    #[tokio::test]
    async fn azdls_fetches_nothing() {
        // azdls cannot be pointed at a local authority host, so the proof is
        // that a signed request opens exactly one connection and no other.
        let _guard = env_lock().lock().await;
        let data = CountingListener::bind().await;
        let previous = swap_env(&AZURE_ENV_KEYS.map(|key| (key, None)));
        let config = group_config(&[
            ("filesystem", "data"),
            ("endpoint", data.endpoint.as_str()),
            ("account_name", "acct"),
            ("account_key", "a2V5c2VjcmV0"),
        ]);

        let operator = build_group_service(GroupBackendKind::Azdls, config, &test_guard()).unwrap();
        let _ = operator.stat("probe").await;

        restore_env(previous);
        assert_eq!(data.hits(), 1);
    }

    #[tokio::test]
    async fn staging_refuses_denied() {
        // Every http-family kind must fail before a socket is opened.
        let listener = CountingListener::bind().await;
        let guard = EgressGuard::new(EgressPolicy::strict()).unwrap();

        for kind in [
            SourceConnectorKind::Http,
            SourceConnectorKind::S3,
            SourceConnectorKind::Webdav,
        ] {
            let mut config = HashMap::from([("endpoint".to_string(), listener.endpoint.clone())]);
            if kind == SourceConnectorKind::S3 {
                config.insert("bucket".to_string(), "reads".to_string());
                config.insert("region".to_string(), "eu-central-1".to_string());
                config.insert("access_key_id".to_string(), "AKIA".to_string());
                config.insert("secret_access_key".to_string(), "secret".to_string());
            }
            let access = ResolvedSourceAccess::OpenDal {
                kind,
                config,
                path: "file.txt".to_string(),
                version: None,
            };

            let (operator, path, ..) = build_source_operator(&guard, &access).await.unwrap();
            let error = operator.stat(path).await.unwrap_err();

            assert!(
                error
                    .to_string()
                    .contains("egress policy denied the target"),
                "{kind}: {error}"
            );
        }

        assert_eq!(listener.hits(), 0);
    }

    #[tokio::test]
    async fn ftp_screens_endpoint() {
        // ftp never reaches an http client, so the preflight screen is the control.
        let listener = CountingListener::bind().await;
        let endpoint = listener.endpoint.replace("http://", "ftp://");
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Ftp,
            config: HashMap::from([("endpoint".to_string(), endpoint)]),
            path: "file.txt".to_string(),
            version: None,
        };
        let guard = EgressGuard::new(EgressPolicy::strict()).unwrap();

        let error = build_source_operator(&guard, &access).await.unwrap_err();

        assert!(matches!(error, StagingSourceError::EgressDenied(_)));
        assert_eq!(listener.hits(), 0);
    }

    #[tokio::test]
    async fn filesystem_like_http_config_is_not_required_for_build_helper_tests() {
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::from([("endpoint".to_string(), "https://example.org".to_string())]),
            path: "file.txt".to_string(),
            version: Some("v42".to_string()),
        };

        let (.., path, version) = build_source_operator(&test_guard(), &access).await.unwrap();
        assert_eq!(path, "file.txt");
        assert_eq!(version, Some("v42"));
    }

    #[tokio::test]
    async fn ftp_build_helper_accepts_expected_keys() {
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Ftp,
            config: HashMap::from([
                ("endpoint".to_string(), "ftp://127.0.0.1:21".to_string()),
                ("root".to_string(), "/datasets".to_string()),
                ("user".to_string(), "alice".to_string()),
                ("password".to_string(), "secret".to_string()),
            ]),
            path: "run-1/data.txt".to_string(),
            version: None,
        };

        let (.., path, version) = build_source_operator(&test_guard(), &access).await.unwrap();
        assert_eq!(path, "run-1/data.txt");
        assert_eq!(version, None);
    }

    #[tokio::test]
    async fn head_and_read_support_filesystem_backed_s3_shape_via_fs_service_test() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_str().unwrap().to_string();
        tokio::fs::write(dir.path().join("hello.txt"), b"hello world")
            .await
            .unwrap();

        let operator =
            build_service::<services::Fs>(HashMap::from([("root".to_string(), root)]), None)
                .unwrap();
        let metadata = operator.stat("hello.txt").await.unwrap();
        assert_eq!(metadata.content_length(), 11);
    }

    #[tokio::test]
    async fn builds_unsigned_s3() {
        // `skip_signature` must stay a valid S3 config key across opendal upgrades.
        build_service::<services::S3>(
            HashMap::from([
                ("bucket".to_string(), "public-data".to_string()),
                ("endpoint".to_string(), "https://s3.example.org".to_string()),
                ("region".to_string(), "eu-central-1".to_string()),
                ("skip_signature".to_string(), "true".to_string()),
            ]),
            None,
        )
        .unwrap();

        build_service::<services::S3>(
            HashMap::from([
                ("bucket".to_string(), "public-data".to_string()),
                ("endpoint".to_string(), "https://s3.example.org".to_string()),
                ("region".to_string(), "eu-central-1".to_string()),
                ("skip_signature".to_string(), "nope".to_string()),
            ]),
            None,
        )
        .unwrap_err();
    }

    #[tokio::test]
    async fn hardens_s3_config() {
        // Lockdown + path-style keys must stay valid across opendal upgrades.
        let config = s3_operator_config(HashMap::from([
            ("bucket".to_string(), "data".to_string()),
            ("endpoint".to_string(), "https://s3.example.org".to_string()),
            ("region".to_string(), "eu-central-1".to_string()),
            ("access_key_id".to_string(), "key".to_string()),
            ("secret_access_key".to_string(), "secret".to_string()),
            ("force_path_style".to_string(), "false".to_string()),
        ]));
        assert_eq!(
            config.get("disable_config_load").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            config.get("disable_ec2_metadata").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            config.get("enable_virtual_host_style").map(String::as_str),
            Some("true")
        );
        assert!(!config.contains_key("force_path_style"));
        build_service::<services::S3>(config, None).unwrap();
    }

    #[tokio::test]
    async fn check_reports_success() {
        let dir = tempdir().unwrap();
        let operator = build_service::<services::Fs>(
            HashMap::from([("root".to_string(), dir.path().to_str().unwrap().to_string())]),
            None,
        )
        .unwrap();

        check_operator(&operator, SourceConnectorKind::S3)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn list_applies_limit() {
        let dir = tempdir().unwrap();
        tokio::fs::create_dir(dir.path().join("prefix"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("prefix/a.txt"), b"a")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("prefix/b.txt"), b"bb")
            .await
            .unwrap();
        let operator = build_service::<services::Fs>(
            HashMap::from([("root".to_string(), dir.path().to_str().unwrap().to_string())]),
            None,
        )
        .unwrap();

        let (entries, truncated) = list_operator(&operator, "prefix/", 0, 1, false, false)
            .await
            .unwrap();

        assert_eq!(entries.len(), 1);
        assert!(entries[0].path.starts_with("prefix/"));
        assert!(truncated);

        let (entries, truncated) = list_operator(&operator, "prefix/", 1, 1, false, false)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert!(!truncated);
    }

    #[tokio::test]
    async fn list_filters_directories() {
        let dir = tempdir().unwrap();
        tokio::fs::create_dir_all(dir.path().join("prefix/nested"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("prefix/nested/file.txt"), b"data")
            .await
            .unwrap();
        let operator = build_service::<services::Fs>(
            HashMap::from([("root".to_string(), dir.path().to_str().unwrap().to_string())]),
            None,
        )
        .unwrap();

        let (entries, truncated) = list_operator(&operator, "prefix/", 0, 10, true, true)
            .await
            .unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "prefix/nested/file.txt");
        assert!(!truncated);
    }

    #[tokio::test]
    async fn recursive_list_truncates() {
        let dir = tempdir().unwrap();
        tokio::fs::create_dir_all(dir.path().join("prefix/nested"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("prefix/a.txt"), b"a")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("prefix/nested/b.txt"), b"b")
            .await
            .unwrap();
        let operator = build_service::<services::Fs>(
            HashMap::from([("root".to_string(), dir.path().to_str().unwrap().to_string())]),
            None,
        )
        .unwrap();

        let (entries, truncated) = list_operator(&operator, "prefix/", 0, 1, true, true)
            .await
            .unwrap();

        assert_eq!(entries.len(), 1);
        assert!(entries[0].path.starts_with("prefix/"));
        assert_eq!(entries[0].kind, aruna_core::structs::SourceEntryKind::File);
        assert!(truncated);
    }
}
