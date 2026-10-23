//! Builds opendal operators for backends and staging sources, and reads or lists a source.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::egress::EgressGuard;
use aruna_core::errors::{BlobError, StagingSourceError};
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::source_access::{
    ListCursor, ListRequest, ResolvedSourceAccess, SourceEntry, SourceEntryKind, SourceMetadata,
};
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::storage::blob::Backend;
use aruna_core::structs::storage::group_backend::GroupBackendKind;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::{Builder, EntryMode, OperationContext, Operator, services};
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
        Backend::S3 => {
            let context = Some(guard.direct_context());
            build_service::<services::S3>(s3_operator_config(config), context)
                .map_err(blob_creation_error)
        }
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
    let layer = Some(guard.context());
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
    if is_invenio(access) {
        return crate::invenio::head_reference(guard, access).await;
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
        digests: Default::default(),
    })
}

pub(crate) async fn read_staging_source(
    guard: &EgressGuard,
    access: &ResolvedSourceAccess,
    range: Option<std::ops::Range<u64>>,
    expected: Option<&SourceMetadata>,
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
    if is_invenio(access) {
        return crate::invenio::read_reference(guard, access, range).await;
    }
    let (operator, path, version) = build_source_operator(guard, access).await?;
    let capability = operator.info().capability();
    if let Some(expected) = expected
        && let Some(etag) = strong_etag(expected.etag.as_deref())
        && capability.read_with_if_match
    {
        return read_bound(&operator, path, version, range, expected, etag).await;
    }
    let metadata = head_staging_source(guard, access).await?;
    let mut reader = operator.reader_with(path);
    // Pinning is preferred, not required: an unpinnable source reads unpinned
    // with a warning so any source stays integrateable (user decision 2026-08-10).
    let pinned_version = version
        .or(metadata.source_version.as_deref())
        .filter(|version| !version.is_empty());
    let strong_etag = strong_etag(metadata.etag.as_deref());
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

fn strong_etag(etag: Option<&str>) -> Option<&str> {
    etag.map(str::trim)
        .filter(|etag| !etag.is_empty() && !etag.starts_with("W/"))
}

/// Reads with the recorded ETag as `If-Match` and no HEAD first, so the bytes are the
/// registered representation or the read fails with `ConditionFailed`.
async fn read_bound(
    operator: &Operator,
    path: &str,
    version: Option<&str>,
    range: Option<std::ops::Range<u64>>,
    expected: &SourceMetadata,
    etag: &str,
) -> Result<
    (
        SourceMetadata,
        BackendStream<Result<Bytes, aruna_core::stream::StreamError>>,
    ),
    StagingSourceError,
> {
    let mut reader = operator.reader_with(path).if_match(etag);
    if let Some(version) = version.filter(|_| operator.info().capability().read_with_version) {
        reader = reader.version(version);
    }
    let reader = reader.await.map_err(|error| bound_read_error(&error))?;
    let stream = match range {
        Some(range) => reader.into_bytes_stream(range).await,
        None => reader.into_bytes_stream(..).await,
    }
    .map_err(|error| bound_read_error(&error))?;
    // The first chunk carries the provider's answer to the condition.
    let mut stream = Box::pin(stream);
    let first = stream.try_next().await.map_err(|error| {
        match error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<opendal::Error>())
        {
            Some(inner) => bound_read_error(inner),
            None => StagingSourceError::ReadError(error.to_string()),
        }
    })?;
    let stream = futures::stream::iter(first.map(Ok)).chain(stream);
    Ok((expected.clone(), BackendStream::new(stream)))
}

fn bound_read_error(error: &opendal::Error) -> StagingSourceError {
    match error.kind() {
        opendal::ErrorKind::ConditionNotMatch => StagingSourceError::ConditionFailed,
        opendal::ErrorKind::NotFound => StagingSourceError::NotFound,
        _ => StagingSourceError::ReadError(error.to_string()),
    }
}

pub(crate) async fn list_staging_source(
    guard: &EgressGuard,
    access: &ResolvedSourceAccess,
    request: &ListRequest,
) -> Result<(Vec<SourceEntry>, Option<ListCursor>), StagingSourceError> {
    if crate::fs_source::is_local_access(access) {
        let offset = fallback_offset(request, FALLBACK_SKIP_LIMIT)?;
        let (entries, truncated) = crate::fs_source::list_local(
            access,
            offset,
            request.limit,
            request.recursive,
            request.files_only,
        )
        .await?;
        return Ok(offset_page(entries, truncated, offset));
    }
    let ResolvedSourceAccess::OpenDal {
        kind, config, path, ..
    } = access;
    if *kind == SourceConnectorKind::Http {
        // opendal's Http service cannot list; walk autoindex pages instead.
        let offset = fallback_offset(request, FALLBACK_SKIP_LIMIT)?;
        let (entries, truncated) = crate::autoindex::list_http_autoindex(
            guard,
            config,
            path,
            offset,
            request.limit,
            request.recursive,
            request.files_only,
        )
        .await?;
        return Ok(offset_page(entries, truncated, offset));
    }
    let (operator, path, ..) = build_source_operator(guard, access).await?;
    list_operator(&operator, path, request).await
}

/// Entries a source without a provider cursor may skip per page: each page re-lists from the
/// start, so deeper pages are refused rather than growing without bound.
const FALLBACK_SKIP_LIMIT: usize = 100_000;
/// Largest page a provider answers in one request, so a page never needs a second request.
const PROVIDER_PAGE_LIMIT: usize = 1000;

fn fallback_offset(request: &ListRequest, skip_limit: usize) -> Result<usize, StagingSourceError> {
    if request.lower.is_some() || request.stop.is_some() {
        return Err(StagingSourceError::InvalidListing(
            "key bounds need a source with a provider cursor".to_string(),
        ));
    }
    let offset = match &request.cursor {
        None => 0,
        Some(ListCursor::Offset(offset)) => usize::try_from(*offset).unwrap_or(usize::MAX),
        Some(ListCursor::After(_)) => {
            return Err(StagingSourceError::InvalidListing(
                "this source continues by offset, not by key".to_string(),
            ));
        }
    };
    if offset > skip_limit {
        return Err(StagingSourceError::InvalidListing(format!(
            "this source cannot continue a listing past {skip_limit} entries; list a narrower path"
        )));
    }
    Ok(offset)
}

fn offset_page(
    entries: Vec<SourceEntry>,
    truncated: bool,
    offset: usize,
) -> (Vec<SourceEntry>, Option<ListCursor>) {
    let next = truncated.then(|| ListCursor::Offset((offset + entries.len()) as u64));
    (entries, next)
}

async fn list_operator(
    operator: &Operator,
    path: &str,
    request: &ListRequest,
) -> Result<(Vec<SourceEntry>, Option<ListCursor>), StagingSourceError> {
    let capability = operator.info().capability();
    if !(capability.list_with_start_after && capability.list_with_recursive) {
        let offset = fallback_offset(request, FALLBACK_SKIP_LIMIT)?;
        let (entries, truncated) = list_skipping(operator, path, request, offset).await?;
        return Ok(offset_page(entries, truncated, offset));
    }
    if !request.recursive && (request.lower.is_some() || request.stop.is_some()) {
        return Err(StagingSourceError::InvalidListing(
            "key bounds need a recursive listing".to_string(),
        ));
    }
    let floor = match &request.cursor {
        Some(ListCursor::After(key)) => Some(key.clone()),
        Some(ListCursor::Offset(_)) => {
            return Err(StagingSourceError::InvalidListing(
                "this source continues by key, not by offset".to_string(),
            ));
        }
        None => request.lower.clone(),
    };
    let limit = request.limit.clamp(1, PROVIDER_PAGE_LIMIT);
    let mut entries = Vec::with_capacity(limit);
    if request.cursor.is_none()
        && let Some(lower) = request.lower.as_deref()
        && let Some(entry) = probe_lower(operator, path, lower, request).await?
    {
        entries.push(entry);
        // The probe takes one entry of the page budget.
        if limit == 1 {
            return Ok((entries, Some(ListCursor::After(lower.to_string()))));
        }
    }
    // One level is listed as flat keys rolled up into directories, so the cursor is always a key
    // of one sorted listing and never depends on how a provider orders a page.
    let mut lister = operator
        .lister_with(path)
        .recursive(true)
        .limit(limit - entries.len());
    if let Some(floor) = floor.as_deref() {
        lister = lister.start_after(floor);
    }
    let mut lister = lister.await.map_err(list_error)?;
    let mut highest = floor.clone();
    // A directory the previous page already listed, from the key the cursor names.
    let mut last_directory = floor.as_deref().and_then(|key| child_directory(path, key));
    let budget = limit - entries.len();
    let mut consumed = 0usize;
    // A provider page holds `budget` entries, so stopping there never fetches a lookahead page.
    while consumed < budget {
        let Some(entry) = lister.try_next().await.map_err(list_error)? else {
            return Ok((entries, None));
        };
        consumed += 1;
        let entry_path = entry.path();
        if request
            .stop
            .as_deref()
            .is_some_and(|stop| entry_path >= stop)
        {
            return Ok((entries, None));
        }
        // A provider may repeat the key a cursor named; anything at or below it was served.
        if floor.as_deref().is_some_and(|floor| entry_path <= floor) {
            continue;
        }
        if highest
            .as_deref()
            .is_none_or(|highest| entry_path > highest)
        {
            highest = Some(entry_path.to_string());
        }
        let directory = (!request.recursive)
            .then(|| child_directory(path, entry_path))
            .flatten();
        match directory {
            Some(directory) => {
                if last_directory.as_deref() != Some(directory.as_str()) && !request.files_only {
                    entries.push(directory_entry(&directory));
                }
                last_directory = Some(directory);
            }
            None => {
                if let Some(entry) =
                    source_entry(entry_path, entry.metadata(), path, request.files_only)
                {
                    entries.push(entry);
                }
            }
        }
    }
    Ok((entries, highest.map(ListCursor::After)))
}

/// The direct child directory of `path` that `key` lies in, with a trailing slash.
fn child_directory(path: &str, key: &str) -> Option<String> {
    let relative = key.strip_prefix(path)?;
    let end = relative.find('/')?;
    Some(format!("{path}{}", &relative[..=end]))
}

fn directory_entry(directory: &str) -> SourceEntry {
    let trimmed = directory.trim_end_matches('/');
    SourceEntry {
        name: trimmed.rsplit('/').next().unwrap_or(trimmed).to_string(),
        path: trimmed.to_string(),
        kind: SourceEntryKind::Directory,
        size: None,
        modified: None,
        etag: None,
        stat: None,
    }
}

/// Reads the inclusive lower key itself, since `start_after` skips it.
async fn probe_lower(
    operator: &Operator,
    path: &str,
    lower: &str,
    request: &ListRequest,
) -> Result<Option<SourceEntry>, StagingSourceError> {
    if lower.ends_with('/') || !lower.starts_with(path.trim_start_matches('/')) {
        return Ok(None);
    }
    if request.stop.as_deref().is_some_and(|stop| lower >= stop) {
        return Ok(None);
    }
    let metadata = match operator.stat(lower).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == opendal::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(list_error(error)),
    };
    if !metadata.is_file() {
        return Ok(None);
    }
    Ok(source_entry(lower, &metadata, path, request.files_only))
}

/// The fallback for a provider without `start_after`: it re-lists and skips `offset` entries.
async fn list_skipping(
    operator: &Operator,
    path: &str,
    request: &ListRequest,
    offset: usize,
) -> Result<(Vec<SourceEntry>, bool), StagingSourceError> {
    let mut lister = operator
        .lister_with(path)
        .recursive(request.recursive)
        .await
        .map_err(list_error)?;
    let mut entries = Vec::with_capacity(request.limit);
    let mut skipped = 0usize;
    while let Some(entry) = lister.try_next().await.map_err(list_error)? {
        let Some(entry) = source_entry(entry.path(), entry.metadata(), path, request.files_only)
        else {
            continue;
        };
        if skipped < offset {
            skipped += 1;
            continue;
        }
        if entries.len() == request.limit {
            return Ok((entries, true));
        }
        entries.push(entry);
    }
    Ok((entries, false))
}

/// The listed entry as a source entry; `None` for the listed directory itself and for kinds
/// the request leaves out.
fn source_entry(
    entry_path: &str,
    metadata: &opendal::Metadata,
    path: &str,
    files_only: bool,
) -> Option<SourceEntry> {
    let trimmed = entry_path.trim_end_matches('/');
    if metadata.is_dir() && trimmed == path.trim_end_matches('/') {
        return None;
    }
    let kind = match metadata.mode() {
        EntryMode::FILE => SourceEntryKind::File,
        EntryMode::DIR if !files_only => SourceEntryKind::Directory,
        EntryMode::DIR | EntryMode::Unknown => return None,
    };
    let file = kind == SourceEntryKind::File;
    Some(SourceEntry {
        name: trimmed.rsplit('/').next().unwrap_or(trimmed).to_string(),
        path: trimmed.to_string(),
        kind,
        size: file.then(|| metadata.content_length()),
        modified: metadata.last_modified().map(Into::into),
        etag: metadata.etag().filter(|_| file).map(ToOwned::to_owned),
        stat: None,
    })
}

fn list_error(error: opendal::Error) -> StagingSourceError {
    StagingSourceError::ListError(error.to_string())
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
                    build_service::<services::Http>(config.clone(), Some(guard.context()))
                        .map_err(staging_creation_error)?
                }
                SourceConnectorKind::S3 => build_service::<services::S3>(
                    s3_operator_config(config.clone()),
                    Some(guard.context()),
                )
                .map_err(staging_creation_error)?,
                SourceConnectorKind::Webdav => {
                    build_service::<services::Webdav>(config.clone(), Some(guard.context()))
                        .map_err(staging_creation_error)?
                }
                // opendal's ftp service cannot constrain the passive data
                // address, so the data socket cannot be screened.
                SourceConnectorKind::Ftp
                | SourceConnectorKind::ArunaNative
                | SourceConnectorKind::LocalDirectory
                | SourceConnectorKind::Invenio => {
                    return Err(StagingSourceError::UnsupportedKind(kind.to_string()));
                }
            };
            Ok((operator, path.as_str(), version.as_deref()))
        }
    }
}

fn is_invenio(access: &ResolvedSourceAccess) -> bool {
    let ResolvedSourceAccess::OpenDal { kind, .. } = access;
    *kind == SourceConnectorKind::Invenio
}

fn build_service<B>(
    config: HashMap<String, String>,
    guard: Option<OperationContext>,
) -> Result<Operator, String>
where
    B: Builder,
{
    let operator = Operator::from_iter::<B>(config)
        .map_err(|error| error.to_string())?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::new());
    Ok(match guard {
        Some(guard) => operator.with_context(guard),
        None => operator,
    })
}

/// The signing region every S3 client of a backend uses: the configured one, else the region
/// variables opendal itself reads. `None` makes both clients refuse the backend alike.
pub(crate) fn s3_region(
    config: &HashMap<String, String>,
    variable: impl Fn(&str) -> Option<String>,
) -> Option<String> {
    config
        .get("region")
        .filter(|region| !region.trim().is_empty())
        .cloned()
        .or_else(|| variable("AWS_REGION"))
        .or_else(|| variable("AWS_DEFAULT_REGION"))
}

pub(crate) fn environment_variable(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

// reqsign resolves lazily, so the switches live in the config; sso, web
// identity, process and ecs stay in the chain, gated by the credentials.
fn s3_operator_config(mut config: HashMap<String, String>) -> HashMap<String, String> {
    if let Some(region) = s3_region(&config, environment_variable) {
        config.insert("region".to_string(), region);
    }
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
