mod archive;
#[cfg(test)]
mod consortium;
mod reader;
mod rewrite;
mod upload;

#[cfg(test)]
pub(crate) mod fixture {
    pub(crate) use super::archive::{
        file_id_candidates, inspect_archive, open_archive, payload_entries, read_metadata,
        signature_entry,
    };
    pub(crate) use super::rewrite::{RewriteTarget, rewrite_document, validate_document};
}

pub use upload::{
    UploadClaimError, claim_rocrate_upload, delete_rocrate_upload, read_rocrate_upload,
    write_rocrate_upload,
};

use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use aruna_core::effects::{BlobEffect, StorageEffect};
use aruna_core::errors::{BlobError, SourceConnectorResolutionError, StagingSourceError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::ROCRATE_JOB_STATE_KEYSPACE;
use aruna_core::metadata::MetadataValidationViolation;
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
use aruna_core::structs::{
    ARUNA_DATA_PREFIX, Actor, AuthContext, BackendLocation, BucketInfo, ImportReportDetail,
    ImportReportRow, ImportRoCrateResult, ImportRoCrateSource, ImportRoCrateSpec, JobError,
    JobResultPayload, MetadataRegistryRecord, OBJECT_CONTENT_TYPE_KEY, Permission, ReasonCode,
    RoCrateCheckpointRefs, RoCrateMediaType, VersionedObjectArn, blob_bucket_permission_path,
    blob_object_permission_path,
};
use aruna_core::types::Value;
use bytes::Bytes;
use byteview::ByteView;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use ulid::Ulid;

use self::archive::{
    ArchiveInspection, file_id_candidates, inspect_archive, open_archive, payload_entries,
    read_metadata, signature_entry,
};
use self::rewrite::{CrateValidationError, RewriteTarget, rewrite_document, validate_document};
use super::executor::{JobContext, JobRunOutcome};
use super::store::{put_job_entry, put_rocrate_checkpoint};
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentPayload,
};
use crate::driver::drive;
use crate::get_realm_config::GetRealmConfigOperation;
use crate::list_metadata_documents::ListMetadataDocumentsOperation;
use crate::metadata::MetadataAuthToken;
use crate::metadata::forward::{MetadataWriteError, create_metadata_document_routed};
use crate::replication::queue::{
    QueueLiveVersionReplicationInput, QueueLiveVersionReplicationOperation,
};
use crate::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use crate::s3::get_object::{GetObjectError, GetObjectInput, GetObjectOperation};
use crate::s3::put_object::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};
use crate::staging::read_source::{
    ReadStagingSourceError, ReadStagingSourceInput, ReadStagingSourceOperation,
};

const PAYLOAD_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum ImportPhase {
    Acquire,
    Inspect,
    Validate,
    Write,
    Rewrite,
    Create,
    Cleanup,
    Done,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ImportInput {
    location: BackendLocation,
    size: u64,
    blake3: [u8; 32],
    upload_id: Option<Ulid>,
    eln: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ImportEntryPlan {
    archive_index: usize,
    archive_path: String,
    path: String,
    target_key: String,
    described_id: Option<String>,
    code: ReasonCode,
    version_id: Option<Ulid>,
    blake3: Option<[u8; 32]>,
    size: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ImportCheckpoint {
    refs: RoCrateCheckpointRefs,
    phase: ImportPhase,
    input: Option<ImportInput>,
    inspection: Option<ArchiveInspection>,
    metadata_json: Option<String>,
    entries: Vec<ImportEntryPlan>,
    next_entry: usize,
    rewritten_json: Option<String>,
    created: bool,
    failure: Option<String>,
    cancelled: bool,
}

impl Default for ImportCheckpoint {
    fn default() -> Self {
        Self {
            refs: RoCrateCheckpointRefs::default(),
            phase: ImportPhase::Acquire,
            input: None,
            inspection: None,
            metadata_json: None,
            entries: Vec::new(),
            next_entry: 0,
            rewritten_json: None,
            created: false,
            failure: None,
            cancelled: false,
        }
    }
}

enum ImportFailure {
    Permanent(String),
    Retryable(String),
    Validation(Vec<MetadataValidationViolation>),
    Cancelled,
    Interrupted,
}

pub async fn run_rocrate_import(ctx: &JobContext, spec: &ImportRoCrateSpec) -> JobRunOutcome {
    let mut checkpoint = match read_checkpoint(ctx).await {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => ImportCheckpoint::default(),
        Err(error) => return retryable_error(error),
    };
    if let Err(error) = persist_checkpoint(ctx, &checkpoint).await {
        return retryable_error(error);
    }

    loop {
        if ctx.shutdown.is_cancelled() {
            return JobRunOutcome::Interrupted;
        }
        if ctx.cancel.is_cancelled()
            && checkpoint.phase != ImportPhase::Cleanup
            && checkpoint.phase != ImportPhase::Done
        {
            checkpoint.cancelled = true;
            if let Err(error) = mark_not_attempted(ctx, &mut checkpoint).await {
                return retryable_error(error);
            }
            checkpoint.phase = ImportPhase::Cleanup;
            if let Err(error) = persist_checkpoint(ctx, &checkpoint).await {
                return retryable_error(error);
            }
        }

        let result = match checkpoint.phase {
            ImportPhase::Acquire => acquire_source(ctx, spec).await.map(|input| {
                checkpoint.refs.hidden_locations = vec![input.location.clone()];
                checkpoint.input = Some(input);
                checkpoint.phase = ImportPhase::Inspect;
            }),
            ImportPhase::Inspect => {
                inspect_source(ctx, spec, &checkpoint)
                    .await
                    .map(|inspection| {
                        checkpoint.inspection = Some(inspection);
                        checkpoint.phase = ImportPhase::Validate;
                    })
            }
            ImportPhase::Validate => validate_source(ctx, spec, &mut checkpoint).await,
            ImportPhase::Write => write_next(ctx, spec, &mut checkpoint).await,
            ImportPhase::Rewrite => rewrite_crate(ctx, spec, &mut checkpoint).await,
            ImportPhase::Create => create_document(ctx, spec, &mut checkpoint).await,
            ImportPhase::Cleanup => cleanup_source(ctx, &mut checkpoint).await,
            ImportPhase::Done => return import_outcome(&checkpoint, spec),
        };

        match result {
            Ok(()) => {
                sync_progress(ctx, &checkpoint);
                if let Err(error) = persist_checkpoint(ctx, &checkpoint).await {
                    return retryable_error(error);
                }
            }
            Err(ImportFailure::Retryable(error))
                if !ctx.final_attempt || checkpoint.phase == ImportPhase::Cleanup =>
            {
                return retryable_error(error);
            }
            Err(ImportFailure::Cancelled) => {
                checkpoint.cancelled = true;
                if let Err(error) = mark_not_attempted(ctx, &mut checkpoint).await {
                    return retryable_error(error);
                }
                checkpoint.phase = ImportPhase::Cleanup;
                if let Err(error) = persist_checkpoint(ctx, &checkpoint).await {
                    return retryable_error(error);
                }
            }
            Err(ImportFailure::Interrupted) => return JobRunOutcome::Interrupted,
            Err(ImportFailure::Validation(violations)) => {
                if let Err(error) = write_validation_rows(ctx, &violations).await {
                    return retryable_error(error);
                }
                checkpoint.failure = Some(validation_message(&violations));
                checkpoint.phase = ImportPhase::Cleanup;
                if let Err(error) = persist_checkpoint(ctx, &checkpoint).await {
                    return retryable_error(error);
                }
            }
            Err(ImportFailure::Retryable(error) | ImportFailure::Permanent(error)) => {
                if let Err(report_error) = write_phase_error(ctx, checkpoint.phase, &error).await {
                    return retryable_error(report_error);
                }
                checkpoint.failure = Some(error);
                if checkpoint.phase == ImportPhase::Write
                    && let Err(error) = mark_write_failure(ctx, &mut checkpoint).await
                {
                    return retryable_error(error);
                }
                checkpoint.phase = ImportPhase::Cleanup;
                if let Err(error) = persist_checkpoint(ctx, &checkpoint).await {
                    return retryable_error(error);
                }
            }
        }
    }
}

async fn acquire_source(
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
) -> Result<ImportInput, ImportFailure> {
    match &spec.source {
        ImportRoCrateSource::Upload { upload_id } => {
            let record = claim_rocrate_upload(
                &ctx.driver.storage_handle,
                *upload_id,
                spec.auth_context.user_id,
                ctx.job_id,
                aruna_core::util::unix_timestamp_millis(),
            )
            .await
            .map_err(|error| match error {
                UploadClaimError::Storage(message) => ImportFailure::Retryable(message),
                other => ImportFailure::Permanent(other.to_string()),
            })?;
            Ok(ImportInput {
                location: record.location,
                size: record.size,
                blake3: record.blake3,
                upload_id: Some(record.upload_id),
                eln: record.media_type == RoCrateMediaType::Eln,
            })
        }
        ImportRoCrateSource::Object {
            bucket,
            key,
            version,
        } => {
            let bucket_info = load_bucket(ctx, bucket).await?;
            ensure_permission(
                ctx,
                &spec.auth_context,
                blob_object_permission_path(
                    spec.auth_context.realm_id,
                    bucket_info.group_id,
                    ctx.owner_node_id,
                    bucket,
                    key,
                ),
                Permission::READ,
            )
            .await?;
            let result = drive(
                GetObjectOperation::new(GetObjectInput {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: *version,
                    range: None,
                    group_id: bucket_info.group_id,
                    user_identity: spec.auth_context.user_id,
                }),
                &ctx.driver,
            )
            .await
            .and_then(|result| result.transpose())
            .map_err(classify_get)?
            .ok_or_else(|| ImportFailure::Permanent("source object not found".to_string()))?;
            let expected_size = result
                .location
                .as_ref()
                .map(|location| location.blob_size)
                .or_else(|| {
                    result
                        .source_metadata
                        .as_ref()
                        .map(|metadata| metadata.content_length)
                });
            let content_type = result
                .source_metadata
                .as_ref()
                .and_then(|metadata| metadata.content_type.as_deref())
                .or_else(|| {
                    result
                        .metadata
                        .get(OBJECT_CONTENT_TYPE_KEY)
                        .map(String::as_str)
                });
            spool_source(
                ctx,
                result.blob,
                expected_size,
                source_uses_eln(key, content_type),
                None,
                spec.limits.import_source_bytes,
                spec.auth_context.user_id,
            )
            .await
        }
        ImportRoCrateSource::Connector {
            group_id,
            connector_id,
            path,
        } => {
            ensure_permission(
                ctx,
                &spec.auth_context,
                source_permission_path(
                    spec.auth_context.realm_id,
                    *group_id,
                    ctx.owner_node_id,
                    *connector_id,
                    path,
                ),
                Permission::READ,
            )
            .await?;
            let result = drive(
                ReadStagingSourceOperation::new(ReadStagingSourceInput {
                    group_id: *group_id,
                    connector_id: *connector_id,
                    source_path: path.clone(),
                    range: None,
                }),
                &ctx.driver,
            )
            .await
            .map_err(classify_read)?;
            spool_source(
                ctx,
                result.stream,
                Some(result.metadata.content_length),
                source_uses_eln(path, result.metadata.content_type.as_deref()),
                None,
                spec.limits.import_source_bytes,
                spec.auth_context.user_id,
            )
            .await
        }
    }
}

async fn spool_source(
    ctx: &JobContext,
    blob: BackendStream<Result<Bytes, aruna_core::stream::StreamError>>,
    expected_size: Option<u64>,
    eln: bool,
    upload_id: Option<Ulid>,
    limit: u64,
    created_by: aruna_core::UserId,
) -> Result<ImportInput, ImportFailure> {
    if expected_size.is_some_and(|size| size > limit) {
        return Err(ImportFailure::Permanent(format!(
            "import source exceeds limit {limit}"
        )));
    }
    let Some(blob_handle) = ctx.driver.blob_handle.as_ref() else {
        return Err(ImportFailure::Retryable(
            "import needs a blob handle".to_string(),
        ));
    };
    let blob = cancel_source_stream(blob, ctx.cancel.clone(), ctx.shutdown.clone());
    let event = blob_handle
        .send_blob_effect(BlobEffect::SpoolHidden {
            namespace: ctx.job_id.0,
            name: "input".to_string(),
            created_by,
            max_bytes: Some(limit),
            blob,
        })
        .await;
    match event {
        Event::Blob(BlobEvent::HiddenSpooled {
            location,
            blake3,
            size,
        }) => {
            if ctx.cancel.is_cancelled() || ctx.shutdown.is_cancelled() {
                let _ = crate::blob::hidden::delete_hidden(&ctx.driver, &location).await;
                return Err(if ctx.cancel.is_cancelled() {
                    ImportFailure::Cancelled
                } else {
                    ImportFailure::Interrupted
                });
            }
            Ok(ImportInput {
                location,
                size,
                blake3,
                upload_id,
                eln,
            })
        }
        Event::Blob(BlobEvent::Error(_)) if ctx.cancel.is_cancelled() => {
            Err(ImportFailure::Cancelled)
        }
        Event::Blob(BlobEvent::Error(_)) if ctx.shutdown.is_cancelled() => {
            Err(ImportFailure::Interrupted)
        }
        Event::Blob(BlobEvent::Error(error)) => Err(classify_blob(error)),
        other => Err(ImportFailure::Retryable(format!(
            "unexpected hidden spool event: {other:?}"
        ))),
    }
}

fn cancel_source_stream(
    mut blob: BackendStream<Result<Bytes, aruna_core::stream::StreamError>>,
    cancel: tokio_util::sync::CancellationToken,
    shutdown: tokio_util::sync::CancellationToken,
) -> BackendStream<Result<Bytes, aruna_core::stream::StreamError>> {
    let (sender, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    tokio::spawn(async move {
        loop {
            let next = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    Some(Err(io::Error::new(io::ErrorKind::Interrupted, "RO-Crate import cancelled")))
                }
                _ = shutdown.cancelled() => {
                    Some(Err(io::Error::new(io::ErrorKind::Interrupted, "RO-Crate import interrupted")))
                }
                next = blob.next() => next.map(|result| {
                    result.map_err(|error| io::Error::other(error.to_string()))
                }),
            };
            let Some(next) = next else {
                break;
            };
            let failed = next.is_err();
            if sender.send(next).await.is_err() || failed {
                break;
            }
        }
    });
    let receiver = Arc::new(Mutex::new(receiver));
    BackendStream::new(stream::poll_fn(move |cx| {
        let mut receiver = receiver.lock().expect("source receiver mutex poisoned");
        Pin::new(&mut *receiver).poll_recv(cx)
    }))
}

async fn inspect_source(
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
    checkpoint: &ImportCheckpoint,
) -> Result<ArchiveInspection, ImportFailure> {
    let input = checkpoint
        .input
        .as_ref()
        .ok_or_else(|| ImportFailure::Permanent("import source is missing".to_string()))?;
    if input.size > spec.limits.import_source_bytes {
        return Err(ImportFailure::Permanent(format!(
            "import source exceeds limit {}",
            spec.limits.import_source_bytes
        )));
    }
    if input.location.get_blake3() != Some(input.blake3.as_slice()) {
        return Err(ImportFailure::Permanent(
            "hidden import source hash does not match its record".to_string(),
        ));
    }
    let handle = ctx
        .driver
        .blob_handle
        .as_ref()
        .cloned()
        .ok_or_else(|| ImportFailure::Retryable("import needs a blob handle".to_string()))?;
    inspect_archive(
        handle,
        input.location.clone(),
        input.size,
        input.eln,
        &spec.limits,
    )
    .await
    .map_err(ImportFailure::Permanent)
}

async fn validate_source(
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
    checkpoint: &mut ImportCheckpoint,
) -> Result<(), ImportFailure> {
    ensure_targets(ctx, spec).await?;
    ensure_path_free(ctx, spec).await?;
    let input = checkpoint
        .input
        .as_ref()
        .ok_or_else(|| ImportFailure::Permanent("import source is missing".to_string()))?;
    let inspection = checkpoint
        .inspection
        .as_ref()
        .ok_or_else(|| ImportFailure::Permanent("archive inspection is missing".to_string()))?;
    let handle = ctx
        .driver
        .blob_handle
        .as_ref()
        .cloned()
        .ok_or_else(|| ImportFailure::Retryable("import needs a blob handle".to_string()))?;
    let mut reader = open_archive(handle, input.location.clone(), input.size)
        .await
        .map_err(ImportFailure::Permanent)?;
    let jsonld = read_metadata(
        &mut reader,
        inspection.metadata_index,
        spec.limits.metadata_bytes,
    )
    .await
    .map_err(ImportFailure::Permanent)?;
    let validated = validate_document(&jsonld).map_err(validation_failure)?;
    let payload = payload_entries(inspection);
    let mut described = HashMap::<String, String>::new();
    for file_id in &validated.file_ids {
        let Some(candidates) = file_id_candidates(file_id).map_err(ImportFailure::Permanent)?
        else {
            continue;
        };
        let matches = candidates
            .iter()
            .filter(|candidate| payload.contains_key(candidate.as_str()))
            .collect::<Vec<_>>();
        let archive_path = match matches.as_slice() {
            [archive_path] => (*archive_path).clone(),
            [] => {
                return Err(ImportFailure::Permanent(format!(
                    "File entity `{file_id}` has no archive payload"
                )));
            }
            _ => {
                return Err(ImportFailure::Permanent(format!(
                    "File entity `{file_id}` maps to multiple archive payloads"
                )));
            }
        };
        if let Some(existing) = described.insert(archive_path.clone(), file_id.clone()) {
            return Err(ImportFailure::Permanent(format!(
                "archive payload `{archive_path}` is described by `{existing}` and `{file_id}`"
            )));
        }
    }

    let mut entries = Vec::with_capacity(payload.len());
    for (path, entry) in payload {
        let target_key = target_key(&spec.target.prefix, &path, spec.limits.key_bytes)?;
        let described_id = described.remove(&path);
        entries.push(ImportEntryPlan {
            archive_index: entry.index,
            archive_path: entry.archive_path.clone(),
            path,
            target_key,
            code: if described_id.is_some() {
                ReasonCode::Imported
            } else {
                ReasonCode::Unlisted
            },
            described_id,
            version_id: None,
            blake3: None,
            size: None,
        });
    }
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    if let Some(signature) = signature_entry(inspection) {
        let row = ImportReportRow {
            entry_key: format!("signature/{}", signature.path),
            code: ReasonCode::SignatureDropped,
            message: Some("detached signature is not retained after rewriting".to_string()),
            detail: ImportReportDetail {
                archive_path: signature.archive_path.clone(),
                target_key: None,
                version_id: None,
                blake3: None,
                size: None,
                arn: None,
                w3id: None,
                validation: None,
            },
        };
        write_report(ctx, &row).await?;
    }
    checkpoint.metadata_json = Some(jsonld);
    checkpoint.entries = entries;
    checkpoint.phase = ImportPhase::Write;
    ctx.progress.set_total(checkpoint.entries.len() as u64);
    Ok(())
}

async fn write_next(
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
    checkpoint: &mut ImportCheckpoint,
) -> Result<(), ImportFailure> {
    if checkpoint.next_entry >= checkpoint.entries.len() {
        checkpoint.phase = ImportPhase::Rewrite;
        return Ok(());
    }
    let index = checkpoint.next_entry;
    if checkpoint.entries[index].version_id.is_none() {
        checkpoint.entries[index].version_id = Some(Ulid::generate());
        persist_checkpoint(ctx, checkpoint)
            .await
            .map_err(ImportFailure::Retryable)?;
    }
    let entry = checkpoint.entries[index].clone();
    let input = checkpoint
        .input
        .as_ref()
        .ok_or_else(|| ImportFailure::Permanent("import source is missing".to_string()))?;
    let bucket_info = load_bucket(ctx, &spec.target.bucket).await?;
    ensure_permission(
        ctx,
        &spec.auth_context,
        blob_object_permission_path(
            spec.auth_context.realm_id,
            bucket_info.group_id,
            ctx.owner_node_id,
            &spec.target.bucket,
            &entry.target_key,
        ),
        Permission::WRITE,
    )
    .await?;
    let expected_size = entry_size(checkpoint, entry.archive_index)?;
    let body = payload_stream(
        ctx,
        &input.location,
        input.size,
        entry.archive_index,
        expected_size,
        ctx.cancel.clone(),
        ctx.shutdown.clone(),
    )
    .await?;
    let quota = drive(
        GetRealmConfigOperation::new(spec.auth_context.realm_id),
        &ctx.driver,
    )
    .await
    .map_err(|error| ImportFailure::Retryable(error.to_string()))?
    .quota
    .effective_group_ceiling(&bucket_info.group_id);
    let version_id = entry
        .version_id
        .ok_or_else(|| ImportFailure::Permanent("import version fence is missing".to_string()))?;
    let result = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: spec.auth_context.user_id,
            group_id: bucket_info.group_id,
            realm_id: spec.auth_context.realm_id,
            node_id: ctx.owner_node_id,
            request: PutObjectInput {
                bucket: spec.target.bucket.clone(),
                key: entry.target_key.clone(),
                content_length: Some(expected_size),
                body: Some(body),
            },
            expected_checksums: vec![ExpectedChecksum {
                algorithm: ChecksumAlgorithm::Crc32,
                digest: entry_crc(checkpoint, entry.archive_index)?
                    .to_be_bytes()
                    .to_vec(),
            }],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: Some(version_id),
            quota_ceiling: quota,
        })
        .with_bucket_guard(bucket_info)
        .with_rocrate_limits(spec.limits.clone()),
        &ctx.driver,
    )
    .await
    .and_then(|result| result.transpose());
    let result = match result {
        Ok(result) => result,
        Err(_) if ctx.cancel.is_cancelled() => return Err(ImportFailure::Cancelled),
        Err(_) if ctx.shutdown.is_cancelled() => return Err(ImportFailure::Interrupted),
        Err(error) => return Err(classify_put(error)),
    }
    .ok_or_else(|| ImportFailure::Retryable("object write returned no result".to_string()))?;
    let hash: [u8; 32] = result
        .location
        .get_blake3()
        .and_then(|hash| hash.try_into().ok())
        .ok_or_else(|| ImportFailure::Retryable("written object has no BLAKE3 hash".to_string()))?;
    let arn = VersionedObjectArn::new(
        spec.auth_context.realm_id,
        ctx.owner_node_id,
        spec.target.bucket.clone(),
        entry.target_key.clone(),
        result.version_id,
    )
    .map_err(|error| ImportFailure::Permanent(error.to_string()))?;
    let row = ImportReportRow {
        entry_key: entry.path.clone(),
        code: entry.code,
        message: None,
        detail: ImportReportDetail {
            archive_path: entry.archive_path,
            target_key: Some(entry.target_key.clone()),
            version_id: Some(result.version_id),
            blake3: Some(hex::encode(hash)),
            size: Some(result.location.blob_size),
            arn: Some(arn.to_string()),
            w3id: Some(arn.to_w3id()),
            validation: None,
        },
    };
    write_report(ctx, &row).await?;
    checkpoint.entries[index].blake3 = Some(hash);
    checkpoint.entries[index].size = Some(result.location.blob_size);
    checkpoint.next_entry = checkpoint.next_entry.saturating_add(1);
    let _ = drive(
        QueueLiveVersionReplicationOperation::new(QueueLiveVersionReplicationInput {
            local_node_id: ctx.owner_node_id,
            auth_context: spec.auth_context.clone(),
            bucket: spec.target.bucket.clone(),
            key: entry.target_key,
            version_id: result.version_id,
            delete_marker: false,
        }),
        &ctx.driver,
    )
    .await;
    Ok(())
}

async fn rewrite_crate(
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
    checkpoint: &mut ImportCheckpoint,
) -> Result<(), ImportFailure> {
    let jsonld = checkpoint
        .metadata_json
        .as_deref()
        .ok_or_else(|| ImportFailure::Permanent("RO-Crate metadata is missing".to_string()))?;
    let validated = validate_document(jsonld).map_err(validation_failure)?;
    let mut targets = HashMap::new();
    for entry in &checkpoint.entries {
        let Some(file_id) = &entry.described_id else {
            continue;
        };
        let version_id = entry
            .version_id
            .ok_or_else(|| ImportFailure::Permanent("imported version is missing".to_string()))?;
        let hash = entry
            .blake3
            .ok_or_else(|| ImportFailure::Permanent("imported hash is missing".to_string()))?;
        let arn = VersionedObjectArn::new(
            spec.auth_context.realm_id,
            ctx.owner_node_id,
            spec.target.bucket.clone(),
            entry.target_key.clone(),
            version_id,
        )
        .map_err(|error| ImportFailure::Permanent(error.to_string()))?;
        targets.insert(
            file_id.clone(),
            RewriteTarget {
                w3id: arn.to_w3id(),
                hash_w3id: format!("{ARUNA_DATA_PREFIX}{}", hex::encode(hash)),
                local_path: entry.path.clone(),
            },
        );
    }
    let rewritten = rewrite_document(validated.value, &targets).map_err(validation_failure)?;
    for (index, file_id) in rewritten.warnings.iter().enumerate() {
        let row = ImportReportRow {
            entry_key: format!("warning/{index:08}"),
            code: ReasonCode::UnrewrittenReference,
            message: Some(format!(
                "scalar reference to `{file_id}` could not be rewritten safely"
            )),
            detail: ImportReportDetail {
                archive_path: file_id.clone(),
                target_key: None,
                version_id: None,
                blake3: None,
                size: None,
                arn: None,
                w3id: None,
                validation: None,
            },
        };
        write_report(ctx, &row).await?;
    }
    checkpoint.rewritten_json = Some(rewritten.jsonld);
    checkpoint.phase = ImportPhase::Create;
    Ok(())
}

async fn create_document(
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
    checkpoint: &mut ImportCheckpoint,
) -> Result<(), ImportFailure> {
    ensure_metadata_permission(ctx, spec).await?;
    ensure_path_free(ctx, spec).await?;
    let jsonld = checkpoint
        .rewritten_json
        .clone()
        .ok_or_else(|| ImportFailure::Permanent("rewritten RO-Crate is missing".to_string()))?;
    let actor = Actor {
        node_id: ctx.owner_node_id,
        user_id: spec.auth_context.user_id,
        realm_id: spec.auth_context.realm_id,
    };
    match create_metadata_document_routed(
        CreateMetadataDocumentOperation::new_for_generated_document_id(
            CreateMetadataDocumentConfig {
                actor,
                group_id: spec.metadata.group_id,
                document_id: spec.document_id,
                document_path: spec.metadata.path.clone(),
                public: spec.metadata.public,
                payload: CreateMetadataDocumentPayload::RoCrate { jsonld },
            },
        ),
        ctx.driver.clone(),
        Some(MetadataAuthToken::internal(
            spec.auth_context.user_id,
            spec.auth_context.realm_id,
        )),
    )
    .await
    {
        Ok(_) => {
            checkpoint.created = true;
            checkpoint.phase = ImportPhase::Cleanup;
            Ok(())
        }
        Err(error) if metadata_is_transient(&error) => {
            Err(ImportFailure::Retryable(error.to_string()))
        }
        Err(error) => Err(ImportFailure::Permanent(error.to_string())),
    }
}

async fn cleanup_source(
    ctx: &JobContext,
    checkpoint: &mut ImportCheckpoint,
) -> Result<(), ImportFailure> {
    let Some(input) = checkpoint.input.as_ref() else {
        checkpoint.refs.hidden_locations.clear();
        checkpoint.phase = ImportPhase::Done;
        return Ok(());
    };
    crate::blob::hidden::delete_hidden(&ctx.driver, &input.location)
        .await
        .map_err(ImportFailure::Retryable)?;
    if let Some(upload_id) = input.upload_id {
        delete_rocrate_upload(&ctx.driver.storage_handle, upload_id, ctx.job_id)
            .await
            .map_err(ImportFailure::Retryable)?;
    }
    checkpoint.refs.hidden_locations.clear();
    checkpoint.phase = ImportPhase::Done;
    Ok(())
}

async fn ensure_targets(ctx: &JobContext, spec: &ImportRoCrateSpec) -> Result<(), ImportFailure> {
    let bucket = load_bucket(ctx, &spec.target.bucket).await?;
    ensure_permission(
        ctx,
        &spec.auth_context,
        blob_bucket_permission_path(
            spec.auth_context.realm_id,
            bucket.group_id,
            ctx.owner_node_id,
            &spec.target.bucket,
        ),
        Permission::WRITE,
    )
    .await?;
    ensure_metadata_permission(ctx, spec).await
}

async fn ensure_metadata_permission(
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
) -> Result<(), ImportFailure> {
    let path = MetadataRegistryRecord::permission_path_for(
        &spec.auth_context.realm_id,
        spec.metadata.group_id,
        &spec.metadata.path,
        spec.document_id,
    );
    ensure_permission(ctx, &spec.auth_context, path, Permission::WRITE).await
}

async fn ensure_path_free(ctx: &JobContext, spec: &ImportRoCrateSpec) -> Result<(), ImportFailure> {
    let normalized = MetadataRegistryRecord::normalize_document_path(&spec.metadata.path);
    if normalized.is_empty() {
        return Err(ImportFailure::Permanent(
            "metadata path must not be empty".to_string(),
        ));
    }
    let documents = drive(
        ListMetadataDocumentsOperation::new(spec.metadata.group_id),
        &ctx.driver,
    )
    .await
    .map_err(|error| ImportFailure::Retryable(error.to_string()))?;
    if documents
        .iter()
        .any(|record| record.document_path == normalized && record.document_id != spec.document_id)
    {
        return Err(ImportFailure::Permanent(format!(
            "metadata path `{normalized}` already exists"
        )));
    }
    Ok(())
}

async fn ensure_permission(
    ctx: &JobContext,
    auth_context: &AuthContext,
    path: String,
    permission: Permission,
) -> Result<(), ImportFailure> {
    match drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth_context.clone(),
            path,
            required_permission: permission,
        }),
        &ctx.driver,
    )
    .await
    {
        Ok(true) => Ok(()),
        Ok(false) => Err(ImportFailure::Permanent("permission denied".to_string())),
        Err(error) => Err(ImportFailure::Retryable(error.to_string())),
    }
}

async fn load_bucket(ctx: &JobContext, bucket: &str) -> Result<BucketInfo, ImportFailure> {
    match drive(GetBucketInfoOperation::new(bucket.to_string()), &ctx.driver).await {
        Ok(Some(Ok(info))) => Ok(info),
        Ok(Some(Err(GetBucketInfoError::NotFound))) | Ok(None) => Err(ImportFailure::Permanent(
            format!("bucket `{bucket}` does not exist"),
        )),
        Ok(Some(Err(error))) | Err(error) => Err(ImportFailure::Retryable(error.to_string())),
    }
}

async fn payload_stream(
    ctx: &JobContext,
    location: &BackendLocation,
    size: u64,
    entry_index: usize,
    expected_size: u64,
    cancel: tokio_util::sync::CancellationToken,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<BackendStream<Result<Bytes, aruna_core::stream::StreamError>>, ImportFailure> {
    let handle = ctx
        .driver
        .blob_handle
        .as_ref()
        .cloned()
        .ok_or_else(|| ImportFailure::Retryable("import needs a blob handle".to_string()))?;
    let reader = open_archive(handle, location.clone(), size)
        .await
        .map_err(ImportFailure::Permanent)?;
    let entry = reader
        .into_entry(entry_index)
        .await
        .map_err(|error| ImportFailure::Permanent(error.to_string()))?;
    let (sender, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    tokio::spawn(async move {
        let mut reader = entry.compat();
        let mut buffer = vec![0; PAYLOAD_CHUNK_BYTES];
        let mut expanded = 0u64;
        loop {
            let read = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    let _ = sender.send(Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "RO-Crate import cancelled",
                    ))).await;
                    break;
                }
                _ = shutdown.cancelled() => {
                    let _ = sender.send(Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "RO-Crate import interrupted",
                    ))).await;
                    break;
                }
                read = reader.read(&mut buffer) => read,
            };
            match read {
                Ok(0) => break,
                Ok(read) => {
                    expanded = expanded.saturating_add(read as u64);
                    if expanded > expected_size {
                        let _ = sender
                            .send(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "ZIP entry exceeds its declared expanded size",
                            )))
                            .await;
                        break;
                    }
                    if sender
                        .send(Ok(Bytes::copy_from_slice(&buffer[..read])))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    break;
                }
            }
        }
    });
    let receiver = Arc::new(Mutex::new(receiver));
    Ok(BackendStream::new(stream::poll_fn(move |cx| {
        let mut receiver = receiver.lock().expect("payload receiver mutex poisoned");
        Pin::new(&mut *receiver).poll_recv(cx)
    })))
}

fn entry_size(checkpoint: &ImportCheckpoint, archive_index: usize) -> Result<u64, ImportFailure> {
    checkpoint
        .inspection
        .as_ref()
        .and_then(|inspection| {
            inspection
                .entries
                .iter()
                .find(|entry| entry.index == archive_index)
        })
        .map(|entry| entry.uncompressed_size)
        .ok_or_else(|| ImportFailure::Permanent("archive entry metadata is missing".to_string()))
}

fn entry_crc(checkpoint: &ImportCheckpoint, archive_index: usize) -> Result<u32, ImportFailure> {
    checkpoint
        .inspection
        .as_ref()
        .and_then(|inspection| {
            inspection
                .entries
                .iter()
                .find(|entry| entry.index == archive_index)
        })
        .map(|entry| entry.crc32)
        .ok_or_else(|| ImportFailure::Permanent("archive entry metadata is missing".to_string()))
}

async fn mark_write_failure(
    ctx: &JobContext,
    checkpoint: &mut ImportCheckpoint,
) -> Result<(), String> {
    for (index, entry) in checkpoint.entries.iter_mut().enumerate() {
        if index < checkpoint.next_entry {
            continue;
        }
        let current = index == checkpoint.next_entry;
        entry.code = if current {
            ReasonCode::Failed
        } else {
            ReasonCode::NotAttempted
        };
        let row = ImportReportRow {
            entry_key: entry.path.clone(),
            code: entry.code,
            message: Some(if current {
                checkpoint
                    .failure
                    .clone()
                    .unwrap_or_else(|| "payload import failed".to_string())
            } else {
                "not attempted after an earlier import failure".to_string()
            }),
            detail: ImportReportDetail {
                archive_path: entry.archive_path.clone(),
                target_key: Some(entry.target_key.clone()),
                version_id: entry.version_id,
                blake3: entry.blake3.map(hex::encode),
                size: entry.size,
                arn: None,
                w3id: None,
                validation: None,
            },
        };
        write_report(ctx, &row).await.map_err(failure_message)?;
    }
    Ok(())
}

async fn mark_not_attempted(
    ctx: &JobContext,
    checkpoint: &mut ImportCheckpoint,
) -> Result<(), String> {
    for (index, entry) in checkpoint.entries.iter_mut().enumerate() {
        if index < checkpoint.next_entry {
            continue;
        }
        entry.code = ReasonCode::NotAttempted;
        let row = ImportReportRow {
            entry_key: entry.path.clone(),
            code: ReasonCode::NotAttempted,
            message: Some("not attempted because the import was cancelled".to_string()),
            detail: ImportReportDetail {
                archive_path: entry.archive_path.clone(),
                target_key: Some(entry.target_key.clone()),
                version_id: entry.version_id,
                blake3: entry.blake3.map(hex::encode),
                size: entry.size,
                arn: None,
                w3id: None,
                validation: None,
            },
        };
        write_report(ctx, &row).await.map_err(failure_message)?;
    }
    Ok(())
}

async fn write_phase_error(
    ctx: &JobContext,
    phase: ImportPhase,
    message: &str,
) -> Result<(), String> {
    let key = format!("failure/{}", phase_name(phase));
    let row = ImportReportRow {
        entry_key: key,
        code: if message.contains("unsupported_crate_version") {
            ReasonCode::UnsupportedCrateVersion
        } else {
            ReasonCode::Failed
        },
        message: Some(message.to_string()),
        detail: ImportReportDetail {
            archive_path: "ro-crate-metadata.json".to_string(),
            target_key: None,
            version_id: None,
            blake3: None,
            size: None,
            arn: None,
            w3id: None,
            validation: None,
        },
    };
    write_report(ctx, &row).await.map_err(failure_message)
}

async fn write_validation_rows(
    ctx: &JobContext,
    violations: &[MetadataValidationViolation],
) -> Result<(), String> {
    for (index, violation) in violations.iter().enumerate() {
        let row = ImportReportRow {
            entry_key: format!("validation/{index:08}"),
            code: if violation.code == "unsupported_crate_version" {
                ReasonCode::UnsupportedCrateVersion
            } else {
                ReasonCode::Failed
            },
            message: Some(violation.message.clone()),
            detail: ImportReportDetail {
                archive_path: "ro-crate-metadata.json".to_string(),
                target_key: None,
                version_id: None,
                blake3: None,
                size: None,
                arn: None,
                w3id: None,
                validation: Some(violation.clone()),
            },
        };
        write_report(ctx, &row).await.map_err(failure_message)?;
    }
    Ok(())
}

async fn write_report(ctx: &JobContext, row: &ImportReportRow) -> Result<(), ImportFailure> {
    put_job_entry(
        &ctx.driver.storage_handle,
        ctx.job_id,
        ctx.claim_token,
        row.entry_key.as_bytes(),
        row,
    )
    .await
    .map_err(|error| ImportFailure::Retryable(error.to_string()))
}

async fn read_checkpoint(ctx: &JobContext) -> Result<Option<ImportCheckpoint>, String> {
    match ctx
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: ROCRATE_JOB_STATE_KEYSPACE.to_string(),
            key: ByteView::from(ctx.job_id.to_bytes().to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => postcard::from_bytes(value.as_ref())
            .map(Some)
            .map_err(|error| error.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected import checkpoint event: {other:?}")),
    }
}

async fn persist_checkpoint(ctx: &JobContext, checkpoint: &ImportCheckpoint) -> Result<(), String> {
    let value = postcard::to_allocvec(checkpoint).map_err(|error| error.to_string())?;
    put_rocrate_checkpoint(
        &ctx.driver.storage_handle,
        ctx.job_id,
        ctx.claim_token,
        Value::from(value),
    )
    .await
    .map_err(|error| error.to_string())
}

fn target_key(prefix: &str, path: &str, limit: u64) -> Result<String, ImportFailure> {
    let prefix = prefix.trim_matches('/');
    if prefix.contains('\\')
        || (!prefix.is_empty()
            && prefix
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == ".."))
        || prefix.chars().any(char::is_control)
    {
        return Err(ImportFailure::Permanent(
            "target prefix is unsafe".to_string(),
        ));
    }
    let key = if prefix.is_empty() {
        path.to_string()
    } else {
        format!("{prefix}/{path}")
    };
    if key.len() as u64 > limit {
        return Err(ImportFailure::Permanent(format!(
            "target key exceeds limit {limit}"
        )));
    }
    Ok(key)
}

fn source_permission_path(
    realm_id: aruna_core::structs::RealmId,
    group_id: Ulid,
    node_id: aruna_core::NodeId,
    connector_id: Ulid,
    path: &str,
) -> String {
    let root = format!("/{realm_id}/g/{group_id}/data/{node_id}/_sources/{connector_id}");
    if path.is_empty() {
        root
    } else {
        format!("{root}/{path}")
    }
}

fn source_uses_eln(path: &str, content_type: Option<&str>) -> bool {
    path.rsplit(['/', '\\'])
        .next()
        .is_some_and(|name| name.to_ascii_lowercase().ends_with(".eln"))
        || content_type
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/vnd.eln+zip"))
}

fn validation_failure(error: CrateValidationError) -> ImportFailure {
    match error {
        CrateValidationError::Violations(violations) => ImportFailure::Validation(violations),
        CrateValidationError::Invalid(message) => ImportFailure::Permanent(message),
    }
}

fn validation_message(violations: &[MetadataValidationViolation]) -> String {
    violations
        .iter()
        .map(|violation| {
            let entity = violation
                .entity_id
                .as_deref()
                .map(|entity| format!(" for `{entity}`"))
                .unwrap_or_default();
            format!(
                "{} at {}{entity}: {}",
                violation.code, violation.pointer, violation.message
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn classify_put(error: PutObjectError) -> ImportFailure {
    match error {
        PutObjectError::StorageError(_) => ImportFailure::Retryable(error.to_string()),
        PutObjectError::BlobWriteFailed(ref message)
            if !message.contains("checksum") && !message.contains("Content-Length") =>
        {
            ImportFailure::Retryable(error.to_string())
        }
        _ => ImportFailure::Permanent(error.to_string()),
    }
}

fn classify_get(error: GetObjectError) -> ImportFailure {
    let permanent = match &error {
        GetObjectError::ConversionError(_)
        | GetObjectError::NoSuchKey
        | GetObjectError::NoSuchVersion
        | GetObjectError::DeleteMarker
        | GetObjectError::InvalidRange => true,
        GetObjectError::ResolveReferenceError(error) => permanent_resolve(error),
        GetObjectError::StagingSourceError(error) => permanent_staging(error),
        _ => false,
    };
    if permanent {
        ImportFailure::Permanent(error.to_string())
    } else {
        ImportFailure::Retryable(error.to_string())
    }
}

fn classify_read(error: ReadStagingSourceError) -> ImportFailure {
    let permanent = match &error {
        ReadStagingSourceError::Resolve(error) => permanent_resolve(error),
        ReadStagingSourceError::Staging(error) => permanent_staging(error),
        _ => false,
    };
    if permanent {
        ImportFailure::Permanent(error.to_string())
    } else {
        ImportFailure::Retryable(error.to_string())
    }
}

fn classify_blob(error: BlobError) -> ImportFailure {
    match error {
        BlobError::SizeLimitExceeded { limit } => {
            ImportFailure::Permanent(format!("import source exceeds limit {limit}"))
        }
        error @ BlobError::StreamFailed(_) => ImportFailure::Permanent(error.to_string()),
        error => ImportFailure::Retryable(error.to_string()),
    }
}

fn permanent_resolve(error: &SourceConnectorResolutionError) -> bool {
    matches!(
        error,
        SourceConnectorResolutionError::ConversionError(_)
            | SourceConnectorResolutionError::NotFound
            | SourceConnectorResolutionError::UnsupportedConnectorKind(_)
            | SourceConnectorResolutionError::InvalidSourcePath
    )
}

fn permanent_staging(error: &StagingSourceError) -> bool {
    matches!(
        error,
        StagingSourceError::NotFound
            | StagingSourceError::AccessDenied
            | StagingSourceError::UnsupportedKind(_)
    )
}

fn metadata_is_transient(error: &MetadataWriteError) -> bool {
    matches!(
        error,
        MetadataWriteError::Create(
            CreateMetadataDocumentError::StorageError(_)
                | CreateMetadataDocumentError::MetadataError(_)
        ) | MetadataWriteError::Undeliverable(_)
    )
}

fn phase_name(phase: ImportPhase) -> &'static str {
    match phase {
        ImportPhase::Acquire => "acquire",
        ImportPhase::Inspect => "inspect",
        ImportPhase::Validate => "validate",
        ImportPhase::Write => "write",
        ImportPhase::Rewrite => "rewrite",
        ImportPhase::Create => "create",
        ImportPhase::Cleanup => "cleanup",
        ImportPhase::Done => "done",
    }
}

fn sync_progress(ctx: &JobContext, checkpoint: &ImportCheckpoint) {
    ctx.progress.set_current(checkpoint.next_entry as u64);
    if !checkpoint.entries.is_empty() {
        ctx.progress.set_total(checkpoint.entries.len() as u64);
    }
}

fn import_outcome(checkpoint: &ImportCheckpoint, spec: &ImportRoCrateSpec) -> JobRunOutcome {
    if checkpoint.cancelled {
        return JobRunOutcome::Cancelled;
    }
    if let Some(error) = &checkpoint.failure {
        return JobRunOutcome::Failed(JobError::permanent(error.clone()));
    }
    let imported = checkpoint
        .entries
        .iter()
        .filter(|entry| entry.code == ReasonCode::Imported && entry.blake3.is_some())
        .count() as u64;
    let unlisted = checkpoint
        .entries
        .iter()
        .filter(|entry| entry.code == ReasonCode::Unlisted && entry.blake3.is_some())
        .count() as u64;
    let failed = checkpoint
        .entries
        .iter()
        .filter(|entry| entry.code == ReasonCode::Failed)
        .count() as u64;
    JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(ImportRoCrateResult {
        document_id: checkpoint.created.then_some(spec.document_id),
        entries_total: checkpoint.entries.len() as u64,
        imported,
        unlisted,
        failed,
        report_digest: [0; 32],
    }))
}

fn failure_message(error: ImportFailure) -> String {
    match error {
        ImportFailure::Permanent(message) | ImportFailure::Retryable(message) => message,
        ImportFailure::Validation(violations) => validation_message(&violations),
        ImportFailure::Cancelled => "import cancelled".to_string(),
        ImportFailure::Interrupted => "import interrupted".to_string(),
    }
}

fn retryable_error(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_checks_limits() {
        assert_eq!(
            target_key("/crate/", "data/a.txt", 64).ok().as_deref(),
            Some("crate/data/a.txt")
        );
        assert!(target_key("../crate", "data/a.txt", 64).is_err());
        assert!(target_key("crate//data", "a.txt", 64).is_err());
        assert!(target_key("crate\\data", "a.txt", 64).is_err());
        assert!(target_key("crate", "data/a.txt", 4).is_err());
    }

    #[test]
    fn detects_eln_source() {
        assert!(source_uses_eln("exports/Experiment.ELN", None));
        assert!(source_uses_eln(
            "exports/experiment.zip",
            Some("Application/Vnd.Eln+Zip; version=1")
        ));
        assert!(!source_uses_eln(
            "exports/experiment.zip",
            Some("application/zip")
        ));
    }

    #[test]
    fn classifies_source_errors() {
        assert!(matches!(
            classify_get(GetObjectError::NoSuchKey),
            ImportFailure::Permanent(_)
        ));
        assert!(matches!(
            classify_get(GetObjectError::StorageError(
                aruna_core::errors::StorageError::Timeout
            )),
            ImportFailure::Retryable(_)
        ));
        assert!(matches!(
            classify_read(ReadStagingSourceError::Resolve(
                SourceConnectorResolutionError::NotFound
            )),
            ImportFailure::Permanent(_)
        ));
        assert!(matches!(
            classify_blob(BlobError::StreamFailed("source read failed".to_string())),
            ImportFailure::Permanent(_)
        ));
        assert!(matches!(
            classify_blob(BlobError::WriteError("backend failed".to_string())),
            ImportFailure::Retryable(_)
        ));
    }
}
