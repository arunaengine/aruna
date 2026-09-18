//! Builds opendal operators for backends and staging sources, and reads or lists a source.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::egress::EgressGuard;
use aruna_core::errors::{BlobError, StagingSourceError};
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::source_access::{ResolvedSourceAccess, SourceMetadata};
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::storage::blob::Backend;
use aruna_core::structs::storage::group_backend::GroupBackendKind;
use bytes::Bytes;
use futures::TryStreamExt;
use opendal::layers::{HttpClientLayer, LoggingLayer, RetryLayer};
use opendal::{Builder, EntryMode, Operator, services};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;

pub(crate) async fn abort_partial_writer(
    writer: &mut opendal::Writer,
    timeout_duration: Duration,
) -> Result<(), BlobError> {
    abort_writer(writer, timeout_duration, UnsupportedAbort::Uncertain).await
}

/// How an abort reports a backend that does not support aborting partial writes.
pub(crate) enum UnsupportedAbort {
    /// Report a delete error: the partial object may still exist.
    Uncertain,
    /// Report `CleanupUnsupported`, so the caller deletes the final path.
    DeletePath,
}

pub(crate) async fn abort_writer(
    writer: &mut opendal::Writer,
    timeout_duration: Duration,
    unsupported: UnsupportedAbort,
) -> Result<(), BlobError> {
    match timeout(timeout_duration, writer.abort()).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            if matches!(unsupported, UnsupportedAbort::Uncertain) {
                tracing::warn!(error = %error, "failed to abort partial blob writer");
            } else if error.kind() == opendal::ErrorKind::Unsupported {
                return Err(BlobError::CleanupUnsupported);
            }
            Err(BlobError::DeleteError(format!(
                "partial blob cleanup is uncertain: {error}"
            )))
        }
        Err(_) => Err(BlobError::DeleteError(
            "partial blob cleanup is uncertain: timed out aborting partial blob writer".to_string(),
        )),
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
            .map_err(blob_creation_error),
        Backend::FileSystem => {
            build_service::<services::Fs>(config, None).map_err(blob_creation_error)
        }
        Backend::Group(kind) => {
            build_group_service(kind, config, guard).map_err(blob_creation_error)
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
    if crate::fs_source::is_local_access(access) {
        return crate::fs_source::check_local(access).await;
    }
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
    if crate::fs_source::is_local_access(access) {
        return crate::fs_source::head_local(access).await;
    }
    let (operator, path, version) = build_source_operator(guard, access).await?;
    let metadata = match version {
        Some(version) => operator.stat_with(path).version(version).await,
        None => operator.stat(path).await,
    }
    .map_err(|error| map_source_error(error, true))?;

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
    if crate::fs_source::is_local_access(access) {
        return crate::fs_source::read_local(access, range).await;
    }
    let (operator, path, version) = build_source_operator(guard, access).await?;
    let metadata = head_staging_source(guard, access).await?;
    let capability = operator.info().full_capability();
    let mut reader = operator.reader_with(path);
    // Pinning is preferred, not required: an unpinnable source reads unpinned
    // with a warning so any source stays integrateable (user decision 2026-08-10).
    let pinned_version = version
        .or(metadata.source_version.as_deref())
        .filter(|version| !version.is_empty());
    let strong_etag = metadata
        .etag
        .as_deref()
        .map(str::trim)
        .filter(|etag| !etag.is_empty() && !etag.starts_with("W/"));
    if let Some(version) = pinned_version.filter(|_| capability.read_with_version) {
        reader = reader.version(version);
    } else if let Some(etag) = strong_etag.filter(|_| capability.read_with_if_match) {
        reader = reader.if_match(etag);
    } else {
        tracing::warn!(
            source = %path,
            has_version = pinned_version.is_some(),
            has_strong_etag = strong_etag.is_some(),
            "reading a staging source unpinned; drift is only caught after the read"
        );
    }
    let reader = reader
        .await
        .map_err(|error| map_source_error(error, false))?;
    let stream = match range {
        Some(range) => reader
            .into_bytes_stream(range)
            .await
            .map_err(|error| map_source_error(error, false))?,
        None => reader
            .into_bytes_stream(..)
            .await
            .map_err(|error| map_source_error(error, false))?,
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
) -> Result<
    (
        Vec<aruna_core::structs::execution::source_access::SourceEntry>,
        bool,
    ),
    StagingSourceError,
> {
    if crate::fs_source::is_local_access(access) {
        return crate::fs_source::list_local(access, offset, limit, recursive, files_only).await;
    }
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
) -> Result<
    (
        Vec<aruna_core::structs::execution::source_access::SourceEntry>,
        bool,
    ),
    StagingSourceError,
> {
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
            EntryMode::FILE => aruna_core::structs::execution::source_access::SourceEntryKind::File,
            EntryMode::DIR if !files_only => {
                aruna_core::structs::execution::source_access::SourceEntryKind::Directory
            }
            EntryMode::DIR | EntryMode::Unknown => continue,
        };
        if skipped < offset {
            skipped += 1;
            continue;
        }
        if entries.len() == limit {
            return Ok((entries, true));
        }

        entries.push(aruna_core::structs::execution::source_access::SourceEntry {
            name: entry.name().trim_end_matches('/').to_string(),
            path: entry.path().trim_end_matches('/').to_string(),
            kind,
            size: (kind == aruna_core::structs::execution::source_access::SourceEntryKind::File)
                .then(|| entry.metadata().content_length()),
            modified: entry.metadata().last_modified().map(Into::into),
            stat: None,
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
                        .map_err(staging_creation_error)?
                }
                SourceConnectorKind::S3 => build_service::<services::S3>(
                    s3_operator_config(config.clone()),
                    Some(guard.layer()),
                )
                .map_err(staging_creation_error)?,
                SourceConnectorKind::Webdav => {
                    build_service::<services::Webdav>(config.clone(), Some(guard.layer()))
                        .map_err(staging_creation_error)?
                }
                // opendal's ftp service cannot constrain the passive data
                // address, so the data socket cannot be screened.
                SourceConnectorKind::Ftp
                | SourceConnectorKind::ArunaNative
                | SourceConnectorKind::LocalDirectory => {
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

// reqsign resolves lazily, so the switches live in the config; sso, web
// identity, process and ecs stay in the chain, gated by the credentials.
fn s3_operator_config(mut config: HashMap<String, String>) -> HashMap<String, String> {
    config.insert("disable_config_load".to_string(), "true".to_string());
    config.insert("disable_ec2_metadata".to_string(), "true".to_string());
    // `force_path_style` is our key; opendal speaks `enable_virtual_host_style`.
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

fn blob_creation_error(error: String) -> BlobError {
    BlobError::OperatorCreationFailed(error)
}

fn staging_creation_error(error: String) -> StagingSourceError {
    StagingSourceError::OperatorCreationFailed(error)
}

fn map_source_error(error: opendal::Error, stat: bool) -> StagingSourceError {
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
#[path = "opendal_tests.rs"]
mod tests;
