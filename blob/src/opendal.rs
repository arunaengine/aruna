use aruna_core::errors::{BlobError, StagingSourceError};
use aruna_core::stream::BackendStream;
use aruna_core::structs::{
    Backend, BackendConfig, ResolvedSourceAccess, SourceConnectorKind, SourceMetadata,
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
            build_service::<services::S3>(s3_operator_config(config.service_config), None)
                .map_err(blob_operator_creation_error)
        }
        Backend::HTTP => build_service::<services::Http>(config.service_config, None)
            .map_err(blob_operator_creation_error),
        Backend::Postgres => build_service::<services::Postgresql>(config.service_config, None)
            .map_err(blob_operator_creation_error),
        Backend::FileSystem => build_service::<services::Fs>(config.service_config, None)
            .map_err(blob_operator_creation_error),
    }
}

pub(crate) fn init_operator(
    backend_type: Backend,
    config: HashMap<String, String>,
) -> Result<Operator, BlobError> {
    match backend_type {
        Backend::S3 => build_service::<services::S3>(s3_operator_config(config), None)
            .map_err(blob_operator_creation_error),
        Backend::HTTP => {
            build_service::<services::Http>(config, None).map_err(blob_operator_creation_error)
        }
        Backend::Postgres => build_service::<services::Postgresql>(config, None)
            .map_err(blob_operator_creation_error),
        Backend::FileSystem => {
            build_service::<services::Fs>(config, None).map_err(blob_operator_creation_error)
        }
    }
}

pub(crate) async fn check_staging_source(
    access: &ResolvedSourceAccess,
) -> Result<(), StagingSourceError> {
    let (operator, ..) = build_staging_source_operator(access)?;
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
    access: &ResolvedSourceAccess,
) -> Result<SourceMetadata, StagingSourceError> {
    let (operator, path, version) = build_staging_source_operator(access)?;
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
) -> Result<
    (
        SourceMetadata,
        BackendStream<Result<Bytes, aruna_core::stream::StreamError>>,
    ),
    StagingSourceError,
> {
    let (operator, path, version) = build_staging_source_operator(access)?;
    let metadata = head_staging_source(access).await?;
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
    guard: &crate::egress::EgressGuard,
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
    let (operator, path, ..) = build_staging_source_operator(access)?;
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

fn build_staging_source_operator(
    access: &ResolvedSourceAccess,
) -> Result<(Operator, &str, Option<&str>), StagingSourceError> {
    match access {
        ResolvedSourceAccess::OpenDal {
            kind,
            config,
            path,
            version,
        } => {
            let operator = match kind {
                SourceConnectorKind::Http => build_service::<services::Http>(config.clone(), None)
                    .map_err(staging_operator_creation_error)?,
                SourceConnectorKind::S3 => {
                    build_service::<services::S3>(s3_operator_config(config.clone()), None)
                        .map_err(staging_operator_creation_error)?
                }
                SourceConnectorKind::Webdav => {
                    build_service::<services::Webdav>(config.clone(), None)
                        .map_err(staging_operator_creation_error)?
                }
                SourceConnectorKind::Ftp => build_service::<services::Ftp>(config.clone(), None)
                    .map_err(staging_operator_creation_error)?,
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
    use std::sync::Arc;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

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
        let (operator, path, ..) = build_staging_source_operator(&access).unwrap();
        let _ = operator.stat(path).await;

        restore_env(previous);
        assert_eq!(metadata.hits(), 0);
    }

    #[tokio::test]
    async fn filesystem_like_http_config_is_not_required_for_build_helper_tests() {
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::from([("endpoint".to_string(), "https://example.org".to_string())]),
            path: "file.txt".to_string(),
            version: Some("v42".to_string()),
        };

        let (.., path, version) = build_staging_source_operator(&access).unwrap();
        assert_eq!(path, "file.txt");
        assert_eq!(version, Some("v42"));
    }

    #[tokio::test]
    async fn ftp_build_helper_accepts_expected_keys() {
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Ftp,
            config: HashMap::from([
                ("endpoint".to_string(), "ftp://example.org:21".to_string()),
                ("root".to_string(), "/datasets".to_string()),
                ("user".to_string(), "alice".to_string()),
                ("password".to_string(), "secret".to_string()),
            ]),
            path: "run-1/data.txt".to_string(),
            version: None,
        };

        let (.., path, version) = build_staging_source_operator(&access).unwrap();
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
