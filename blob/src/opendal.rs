use aruna_core::errors::{BlobError, StagingSourceError};
use aruna_core::stream::BackendStream;
use aruna_core::structs::{
    Backend, BackendConfig, ResolvedSourceAccess, SourceConnectorKind, SourceMetadata,
};
use bytes::Bytes;
use futures::TryStreamExt;
use opendal::layers::{LoggingLayer, RetryLayer};
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
    access: &ResolvedSourceAccess,
    limit: usize,
    recursive: bool,
    files_only: bool,
) -> Result<(Vec<aruna_core::structs::SourceEntry>, bool), StagingSourceError> {
    let (operator, path, ..) = build_staging_source_operator(access)?;
    list_operator(&operator, path, limit, recursive, files_only).await
}

async fn list_operator(
    operator: &Operator,
    path: &str,
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
                SourceConnectorKind::Http => build_service::<services::Http>(config.clone())
                    .map_err(staging_operator_creation_error)?,
                SourceConnectorKind::S3 => build_service::<services::S3>(config.clone())
                    .map_err(staging_operator_creation_error)?,
                SourceConnectorKind::Webdav => build_service::<services::Webdav>(config.clone())
                    .map_err(staging_operator_creation_error)?,
                SourceConnectorKind::Ftp => build_service::<services::Ftp>(config.clone())
                    .map_err(staging_operator_creation_error)?,
                SourceConnectorKind::ArunaNative => {
                    return Err(StagingSourceError::UnsupportedKind(kind.to_string()));
                }
            };
            Ok((operator, path.as_str(), version.as_deref()))
        }
    }
}

fn build_service<B>(config: HashMap<String, String>) -> Result<Operator, String>
where
    B: Builder,
{
    Ok(Operator::from_iter::<B>(config)
        .map_err(|error| error.to_string())?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::new()))
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
            build_service::<services::Fs>(HashMap::from([("root".to_string(), root)])).unwrap();
        let metadata = operator.stat("hello.txt").await.unwrap();
        assert_eq!(metadata.content_length(), 11);
    }

    #[tokio::test]
    async fn check_reports_success() {
        let dir = tempdir().unwrap();
        let operator = build_service::<services::Fs>(HashMap::from([(
            "root".to_string(),
            dir.path().to_str().unwrap().to_string(),
        )]))
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
        let operator = build_service::<services::Fs>(HashMap::from([(
            "root".to_string(),
            dir.path().to_str().unwrap().to_string(),
        )]))
        .unwrap();

        let (entries, truncated) = list_operator(&operator, "prefix/", 1, false, false)
            .await
            .unwrap();

        assert_eq!(entries.len(), 1);
        assert!(entries[0].path.starts_with("prefix/"));
        assert!(truncated);
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
        let operator = build_service::<services::Fs>(HashMap::from([(
            "root".to_string(),
            dir.path().to_str().unwrap().to_string(),
        )]))
        .unwrap();

        let (entries, truncated) = list_operator(&operator, "prefix/", 10, true, true)
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
        let operator = build_service::<services::Fs>(HashMap::from([(
            "root".to_string(),
            dir.path().to_str().unwrap().to_string(),
        )]))
        .unwrap();

        let (entries, truncated) = list_operator(&operator, "prefix/", 1, true, true)
            .await
            .unwrap();

        assert_eq!(entries.len(), 1);
        assert!(entries[0].path.starts_with("prefix/"));
        assert_eq!(entries[0].kind, aruna_core::structs::SourceEntryKind::File);
        assert!(truncated);
    }
}
