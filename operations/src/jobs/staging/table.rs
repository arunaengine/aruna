//! Reads a staging job's table in bounded chunks: file rows are registered as references with
//! their row metadata, folder rows get a folder object and wait in the directory queue.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap};

use aruna_core::errors::StagingSourceError;
use aruna_core::keyspaces::STAGING_QUEUE_KEYSPACE;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::job::{
    StagingJobCheckpoint, StagingJobDirectory, StagingJobPhase, StagingJobSpec, TableRead,
};
use aruna_core::structs::execution::source_access::ObservationMatch;
use aruna_core::structs::execution::table::{
    GzipWindow, RowKind, RowMapper, RowOutcome, TABLE_CHUNK, TableEntry, TableImport, TableSource,
    is_gzip, parse_rows, read_header, source_path,
};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use bytes::Bytes;
use byteview::ByteView;
use futures_util::StreamExt;

use super::limiter::paced;
use super::manifest::{lasting_head, lasting_resolution, lasting_source};
use super::walk::{
    PageFailure, StagedFile, access_rules, commit_rows, coordinated, import_page, own_ranges,
    record_failure, source_permission_path,
};
use super::{ItemFailure, pinned_bucket, queue_key};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::connectors::get_connector::{GetSourceInput, GetSourceOperation};
use crate::driver::{bucket_snapshot, drive, gate_context, now_ms};
use crate::jobs::executor::JobContext;
use crate::realm::get_config::GetConfigOperation;
use crate::replication::queue::{LiveVersionInput, LiveVersionOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::get::{
    GetObjectError, GetObjectInput, ObjectRangeRequest, get_object_routed,
};
use crate::s3::object::head::{HeadObjectInput, HeadObjectOperation};
use crate::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use crate::staging::head_source::{HeadSourceInput, HeadSourceOperation};
use crate::staging::read_source::{ReadSourceError, ReadSourceInput, ReadSourceOperation};

/// Directories the queue holds before the table is read further.
pub(super) const QUEUE_PAUSE: u64 = 1024;
/// Rows one step maps, so one page and its HEADs stay bounded.
const STEP_ROWS: usize = 1000;

/// Whether the job reads its table next: the table is open, no directory is being listed
/// and the queue has room.
pub(super) fn reads_table(checkpoint: &StagingJobCheckpoint) -> bool {
    checkpoint.table.is_some()
        && checkpoint.walk.is_none()
        && checkpoint.queue_tail.saturating_sub(checkpoint.queue_head) < QUEUE_PAUSE
}

/// One window of the table and what the read learned about it.
struct Chunk {
    bytes: Vec<u8>,
    complete: bool,
    read: TableRead,
}

enum ChunkFailure {
    /// The table changed between chunks, so it is read again from its start.
    Changed,
    /// The table cannot be read; the job records it and ends the table.
    Lasting(String),
    Retry(String),
}

/// Reads the next chunk of the table: its header first, then rows, and commits their rows
/// with the checkpoint.
pub(super) async fn read_table_chunk(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    checkpoint: &mut StagingJobCheckpoint,
) -> Result<(), PageFailure> {
    let (Some(import), Some(read)) = (spec.table.as_ref(), checkpoint.table.clone()) else {
        return Ok(());
    };
    checkpoint.phase = StagingJobPhase::Discovering;
    let name = table_name(import);
    checkpoint.current_path = Some(name.clone());
    let mut next = checkpoint.clone();
    let connector = drive(
        GetSourceOperation::new(GetSourceInput {
            group_id: spec.group_id,
            connector_id: spec.connector_id,
        }),
        &ctx.driver,
    )
    .await
    .map_err(|error| PageFailure::Retry(error.to_string()))?
    .connector;
    let base = connector.base_url();
    let chunk = match &import.source {
        TableSource::Connector(path) => {
            let path = source_path(path, base.as_deref()).map_err(PageFailure::Permanent)?;
            connector_chunk(ctx, spec, (import, &path), *read).await
        }
        TableSource::Object { bucket, key } => {
            object_chunk(ctx, spec, import, (bucket, key), *read).await
        }
    };
    let chunk = match chunk {
        Ok(chunk) => chunk,
        Err(ChunkFailure::Changed) => {
            next.table = Some(Box::default());
            return commit_rows(ctx, checkpoint, next, Vec::new(), Vec::new()).await;
        }
        Err(ChunkFailure::Lasting(message)) => {
            record_failure(&mut next, &name, "", message);
            next.table = None;
            return commit_rows(ctx, checkpoint, next, Vec::new(), Vec::new()).await;
        }
        Err(ChunkFailure::Retry(message)) => return Err(PageFailure::Retry(message)),
    };
    let permanent = |error: aruna_core::structs::execution::table::TableError| {
        PageFailure::Permanent(error.to_string())
    };
    let mut read = chunk.read;
    let Some(columns) = read.columns.clone() else {
        let (columns, used) =
            read_header(&import.format, &chunk.bytes, chunk.complete).map_err(permanent)?;
        RowMapper::new(&import.rows, &columns).map_err(permanent)?;
        read.columns = Some(columns);
        read.offset += used as u64;
        next.table = Some(Box::new(read));
        return commit_rows(ctx, checkpoint, next, Vec::new(), Vec::new()).await;
    };
    let mapper = RowMapper::new(&import.rows, &columns).map_err(permanent)?;
    let width = Some(columns.len());
    let (mut rows, mut used) =
        parse_rows(&import.format, width, &chunk.bytes, chunk.complete).map_err(permanent)?;
    let mut complete = chunk.complete;
    if rows.len() > STEP_ROWS {
        used = rows[STEP_ROWS - 1].1;
        rows.truncate(STEP_ROWS);
        complete = false;
    }
    let mut entries = Vec::new();
    for (row, end) in rows {
        let at = read.offset + end as u64;
        let outcome = match row {
            Ok(fields) => mapper.map(&fields, base.as_deref()),
            Err(reason) => RowOutcome::Failed(reason),
        };
        match outcome {
            RowOutcome::Filtered | RowOutcome::Skipped => {}
            RowOutcome::Failed(reason) => record_failure(
                &mut next,
                &name,
                "",
                format!("row ending at byte {at}: {reason}"),
            ),
            RowOutcome::Import(entry) => entries.push(entry),
        }
    }
    read.offset += used as u64;
    next.table = (!complete).then(|| Box::new(read));
    match mapper.kind() {
        RowKind::File => file_rows(ctx, spec, checkpoint, next, entries).await,
        RowKind::Folder => folder_rows(ctx, spec, checkpoint, next, entries).await,
    }
}

fn table_name(import: &TableImport) -> String {
    match &import.source {
        TableSource::Connector(path) => path.clone(),
        TableSource::Object { bucket, key } => format!("{bucket}/{key}"),
    }
}

async fn source_allowed(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    path: String,
) -> Result<bool, ChunkFailure> {
    drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: spec.auth_context.clone(),
            path,
            required_permission: Permission::READ,
        }),
        &ctx.driver,
    )
    .await
    .map_err(|error| ChunkFailure::Retry(error.to_string()))
}

/// A table below the read connector, every chunk bound to its first observation.
async fn connector_chunk(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    (import, path): (&TableImport, &str),
    mut read: TableRead,
) -> Result<Chunk, ChunkFailure> {
    if !source_allowed(ctx, spec, source_permission_path(spec, path)).await? {
        return Err(ChunkFailure::Lasting("table read denied".to_string()));
    }
    let stop = (&ctx.cancel, &ctx.shutdown);
    let observation = match read.observation.clone() {
        Some(observation) => observation,
        None => {
            let head = || {
                drive(
                    HeadSourceOperation::new(HeadSourceInput {
                        group_id: spec.group_id,
                        connector_id: spec.connector_id,
                        source_path: path.to_string(),
                    }),
                    &ctx.driver,
                )
            };
            let transient = |error: &_| (!lasting_head(error)).then(|| format!("{error}"));
            match paced(stop, (spec, 1), transient, head).await {
                Ok(head) => head.metadata,
                Err(error) if lasting_head(&error) => {
                    return Err(ChunkFailure::Lasting(format!("table unavailable: {error}")));
                }
                Err(error) => return Err(ChunkFailure::Retry(error.to_string())),
            }
        }
    };
    read.observation = Some(observation.clone());
    let length = observation.content_length;
    let mut gzip = read
        .gzip
        .unwrap_or_else(|| is_gzip(&import.format, path, &[]));
    loop {
        let plain = !gzip;
        let end = (read.offset + TABLE_CHUNK as u64).min(length);
        if plain && read.offset >= end {
            return Ok(Chunk {
                bytes: Vec::new(),
                complete: true,
                read,
            });
        }
        let source = || {
            drive(
                ReadSourceOperation::new(ReadSourceInput {
                    group_id: spec.group_id,
                    connector_id: spec.connector_id,
                    source_path: path.to_string(),
                    range: plain.then_some(read.offset..end),
                    expected: Some(observation.clone()),
                }),
                &ctx.driver,
            )
        };
        let transient = |error: &ReadSourceError| (!lasting_read(error)).then(|| error.to_string());
        // An unpinned source read sends a HEAD before its GET.
        let stream = match paced(stop, (spec, 2), transient, source).await {
            Ok(result) if result.metadata.compare(&observation) == ObservationMatch::Changed => {
                return Err(ChunkFailure::Changed);
            }
            Ok(result) => result.stream,
            Err(ReadSourceError::Staging(StagingSourceError::ConditionFailed)) => {
                return Err(ChunkFailure::Changed);
            }
            Err(error) if lasting_read(&error) => {
                return Err(ChunkFailure::Lasting(format!("table read failed: {error}")));
            }
            Err(error) => return Err(ChunkFailure::Retry(error.to_string())),
        };
        let (bytes, complete) = match gzip {
            true => window(stream, read.offset).await?,
            false => (collect(stream).await?, end == length),
        };
        // The first chunk decides by its magic bytes whether a table is compressed.
        if read.gzip.is_none() && !gzip && is_gzip(&import.format, path, &bytes) {
            gzip = true;
            continue;
        }
        read.gzip = Some(gzip);
        return Ok(Chunk {
            bytes,
            complete,
            read,
        });
    }
}

/// An uploaded table, read as the submitter and pinned to the version first read.
async fn object_chunk(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    import: &TableImport,
    (bucket, key): (&str, &str),
    mut read: TableRead,
) -> Result<Chunk, ChunkFailure> {
    let info = match drive(GetBucketOperation::new(bucket.to_string()), &ctx.driver).await {
        Ok(info) => info,
        Err(GetBucketError::NotFound) => {
            return Err(ChunkFailure::Lasting(
                "the table's bucket does not exist".to_string(),
            ));
        }
        Err(error) => return Err(ChunkFailure::Retry(error.to_string())),
    };
    let auth = &spec.auth_context;
    let path = object_permission_path(auth.realm_id, info.group_id, spec.node_id, bucket, key);
    if !source_allowed(ctx, spec, path).await? {
        return Err(ChunkFailure::Lasting("table read denied".to_string()));
    }
    let mut gzip = read
        .gzip
        .unwrap_or_else(|| is_gzip(&import.format, key, &[]));
    loop {
        let start = read.offset;
        let range = (!gzip).then_some(ObjectRangeRequest::StartEnd {
            start,
            end: start + TABLE_CHUNK as u64 - 1,
        });
        let input = GetObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: read.version_id,
            range,
            group_id: info.group_id,
            user_identity: auth.user_id,
            realm_id: auth.realm_id,
            node_id: spec.node_id,
        };
        let restrictions = auth.path_restrictions.clone();
        let result = match get_object_routed(&ctx.driver, input, restrictions).await {
            Ok(result) => result,
            // A range from the table's end on reads nothing.
            Err(GetObjectError::InvalidRange) => {
                return Ok(Chunk {
                    bytes: Vec::new(),
                    complete: true,
                    read,
                });
            }
            Err(
                error @ (GetObjectError::NoSuchKey
                | GetObjectError::NoSuchVersion
                | GetObjectError::DeleteMarker),
            ) => {
                return Err(ChunkFailure::Lasting(format!("table unavailable: {error}")));
            }
            Err(error) => return Err(ChunkFailure::Retry(error.to_string())),
        };
        read.version_id = read
            .version_id
            .or(result.resolved_version_id.or(result.version_id));
        let size = result.info.size;
        let (bytes, complete) = match gzip {
            true => window(result.blob, start).await?,
            false => {
                let bytes = collect(result.blob).await?;
                let complete = start + bytes.len() as u64 >= size;
                (bytes, complete)
            }
        };
        if read.gzip.is_none() && !gzip && is_gzip(&import.format, key, &bytes) {
            gzip = true;
            continue;
        }
        read.gzip = Some(gzip);
        return Ok(Chunk {
            bytes,
            complete,
            read,
        });
    }
}

async fn collect(
    mut stream: BackendStream<Result<Bytes, StreamError>>,
) -> Result<Vec<u8>, ChunkFailure> {
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| ChunkFailure::Retry(error.to_string()))?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// The decompressed chunk after `offset`; the read stops once the chunk is full.
async fn window(
    mut stream: BackendStream<Result<Bytes, StreamError>>,
    offset: u64,
) -> Result<(Vec<u8>, bool), ChunkFailure> {
    let mut window = GzipWindow::new(offset, TABLE_CHUNK);
    let invalid = |error: aruna_core::structs::execution::table::TableError| {
        ChunkFailure::Lasting(error.to_string())
    };
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| ChunkFailure::Retry(error.to_string()))?;
        if window.push(&chunk).map_err(invalid)? {
            break;
        }
    }
    window.finish().map_err(invalid)
}

fn lasting_read(error: &ReadSourceError) -> bool {
    match error {
        ReadSourceError::Resolve(error) => lasting_resolution(error),
        ReadSourceError::Staging(error) => lasting_source(error),
        ReadSourceError::UnexpectedEvent { .. } | ReadSourceError::ReadSourceFailed => false,
    }
}

/// Registers the files of file rows as references carrying their row metadata.
async fn file_rows(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    checkpoint: &mut StagingJobCheckpoint,
    next: StagingJobCheckpoint,
    entries: Vec<TableEntry>,
) -> Result<(), PageFailure> {
    let files = (entries.into_iter())
        .map(|entry| StagedFile {
            source_path: entry.source_path,
            target_key: entry.target,
            size: None,
            observation: None,
            manifest: None,
            metadata: entry.metadata,
        })
        .collect();
    let rows = (files, Vec::new(), Vec::new());
    import_page(ctx, spec, (None, None), checkpoint, next, rows, None).await
}

/// Queues the folders of folder rows whose keys overlap a range this node coordinates, after
/// writing each folder object on the coordinator of its key.
async fn folder_rows(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    checkpoint: &mut StagingJobCheckpoint,
    mut next: StagingJobCheckpoint,
    entries: Vec<TableEntry>,
) -> Result<(), PageFailure> {
    let bucket = pinned_bucket(ctx, spec).await.map_err(page_failure)?;
    let ranges = match bucket.distribution.as_ref() {
        Some(binding) => Some(own_ranges(ctx, binding).await?),
        None => None,
    };
    let rules = access_rules(ctx, spec).await.map_err(PageFailure::Retry)?;
    let mut writes = Vec::new();
    for entry in entries {
        let prefix = format!("{}/", entry.target);
        if !overlaps(ranges.as_deref(), &prefix) {
            continue;
        }
        let source = source_permission_path(spec, &entry.source_path);
        let target = object_permission_path(
            spec.auth_context.realm_id,
            spec.group_id,
            spec.node_id,
            &spec.bucket,
            &prefix,
        );
        if !rules.allows(&source, &Permission::READ) || !rules.allows(&target, &Permission::WRITE) {
            record_failure(
                &mut next,
                &entry.source_path,
                &prefix,
                "permission denied".to_string(),
            );
            continue;
        }
        let own = coordinated(ctx, &bucket, &[prefix.as_str()]).await;
        if own.map_err(PageFailure::Retry)?.first() == Some(&true) {
            match Box::pin(put_folder(ctx, spec, &bucket, &prefix, &entry.metadata)).await {
                Ok(()) => {}
                Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
                    record_failure(&mut next, &entry.source_path, &prefix, message)
                }
                Err(failure) => return Err(page_failure(failure)),
            }
        }
        let directory = StagingJobDirectory {
            source_path: entry.source_path,
            target_prefix: entry.target,
        };
        let value = postcard::to_allocvec(&directory)
            .map_err(|error| PageFailure::Permanent(error.to_string()))?;
        let key = queue_key(ctx.job_id, next.queue_tail);
        writes.push((
            STAGING_QUEUE_KEYSPACE.to_string(),
            key,
            ByteView::from(value),
        ));
        next.queue_tail += 1;
    }
    commit_rows(ctx, checkpoint, next, writes, Vec::new()).await
}

fn page_failure(failure: ItemFailure) -> PageFailure {
    match failure {
        ItemFailure::System(message) => PageFailure::Retry(message),
        ItemFailure::Denied(message)
        | ItemFailure::Stage(message)
        | ItemFailure::Lasting(message) => PageFailure::Permanent(message),
    }
}

/// Whether keys below `prefix` lie in one of `ranges`; every key does without ranges.
fn overlaps(ranges: Option<&[(String, Option<String>)]>, prefix: &str) -> bool {
    let Some(ranges) = ranges else {
        return true;
    };
    // `0` follows `/`, so keys below the prefix sort before this one.
    let end = format!("{}0", prefix.trim_end_matches('/'));
    (ranges.iter()).any(|(lower, upper)| {
        lower.as_str() < end.as_str() && upper.as_deref().is_none_or(|upper| upper > prefix)
    })
}

/// Writes the zero-byte folder object at `key` with the row metadata; an equal head stays.
async fn put_folder(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    bucket: &BucketInfo,
    key: &str,
    metadata: &BTreeMap<String, String>,
) -> Result<(), ItemFailure> {
    let head = HeadObjectInput {
        bucket: spec.bucket.clone(),
        key: key.to_string(),
        version_id: None,
    };
    if let Ok(head) = drive(HeadObjectOperation::new(head), &ctx.driver).await {
        let empty = head
            .location
            .as_ref()
            .is_some_and(|location| location.blob_size == 0);
        let same = head.metadata.len() == metadata.len()
            && (metadata.iter()).all(|(key, value)| head.metadata.get(key) == Some(value));
        if empty && same {
            return Ok(());
        }
    }
    let target = (spec.bucket.as_str(), bucket);
    Box::pin(write_object(
        ctx,
        spec,
        target,
        key,
        (Bytes::new(), metadata),
    ))
    .await
}

/// Writes `body` at `key` through the ordinary put, as the submitter, and queues its replication.
async fn write_object(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    (name, bucket): (&str, &BucketInfo),
    key: &str,
    (body, metadata): (Bytes, &BTreeMap<String, String>),
) -> Result<(), ItemFailure> {
    let system = |error: String| ItemFailure::System(error);
    let routing = bucket_snapshot(&ctx.driver, bucket)
        .await
        .map_err(|error| system(error.to_string()))?;
    let auth = &spec.auth_context;
    let gate = gate_context(&ctx.driver, auth.realm_id, now_ms())
        .await
        .map_err(|error| system(error.to_string()))?;
    let quota = drive(GetConfigOperation::new(auth.realm_id), &ctx.driver)
        .await
        .map_err(|error| system(error.to_string()))?
        .quota
        .effective_group_ceiling(&bucket.group_id);
    let length = body.len() as u64;
    let body = BackendStream::new(futures_util::stream::iter([Ok::<_, StreamError>(body)]));
    let mut operation = PutObjectOperation::new(PutObjectConfig {
        user_id: auth.user_id,
        group_id: bucket.group_id,
        realm_id: auth.realm_id,
        node_id: spec.node_id,
        request: PutObjectInput {
            bucket: name.to_string(),
            key: key.to_string(),
            content_length: Some(length),
            body: Some(body),
        },
        expected_checksums: Vec::new(),
        checksum_type: None,
        exists: false,
        version_source: None,
        preassigned_version_id: None,
        quota_ceiling: quota,
        routing,
    })
    .with_bucket_guard(bucket.clone())
    .with_restrictions(auth.path_restrictions.clone())
    .with_metadata(metadata.clone().into_iter().collect::<HashMap<_, _>>());
    if let Some(gate) = gate {
        operation = operation.with_gate(gate);
    }
    let result = drive(operation, &ctx.driver)
        .await
        .map_err(|error| ItemFailure::Stage(format!("object not written: {error}")))?;
    drive(
        LiveVersionOperation::new(LiveVersionInput {
            local_node_id: spec.node_id,
            auth_context: auth.clone(),
            bucket: name.to_string(),
            key: key.to_string(),
            version_id: result.version_id,
            delete_marker: false,
        }),
        &ctx.driver,
    )
    .await
    .map(|_| ())
    .map_err(|error| ItemFailure::Stage(format!("replication queue failed: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    // A folder spanning a range boundary is queued by both coordinators, others by one.
    #[test]
    fn folders_follow_ranges() {
        let ranges = [
            (
                String::new(),
                Some("GCF_000005845.2_ASM584v2/genomic.fna".to_string()),
            ),
            ("GCF_000005845.2_ASM584v2/genomic.fna".to_string(), None),
        ];
        let prefix = "GCF_000005845.2_ASM584v2/";
        assert!(overlaps(Some(&ranges[..1]), prefix));
        assert!(overlaps(Some(&ranges[1..]), prefix));
        let before = [(String::new(), Some("GCF_000005845.2_ASM584v2/".to_string()))];
        assert!(!overlaps(Some(&before), prefix));
        let after = [("GCF_000005845.2_ASM584v20".to_string(), None)];
        assert!(!overlaps(Some(&after), prefix));
        assert!(overlaps(None, prefix));
    }
}

#[cfg(test)]
#[path = "table_tests.rs"]
mod adapter_tests;
