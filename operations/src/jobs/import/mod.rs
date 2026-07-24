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
    CreateRoCrateUploadConfig, CreateRoCrateUploadError, CreateRoCrateUploadOperation,
    UploadClaimError, claim_rocrate_upload, delete_rocrate_upload, load_rocrate_upload,
    read_rocrate_upload, write_rocrate_upload,
};

use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use aruna_core::effects::{BlobEffect, StorageEffect};
use aruna_core::errors::{BlobError, SourceConnectorResolutionError, StagingSourceError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{JOB_ENTRY_KEYSPACE, ROCRATE_JOB_STATE_KEYSPACE};
use aruna_core::metadata::MetadataValidationViolation;
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
use aruna_core::structs::{
    ARUNA_DATA_PREFIX, Actor, AuthContext, BackendLocation, BucketInfo, ImportReportDetail,
    ImportReportRow, ImportRoCrateResult, ImportRoCrateSource, ImportRoCrateSpec,
    JOB_SYSTEM_ENTRY_PREFIX, JobError, JobResultPayload, MetadataRegistryRecord,
    OBJECT_CONTENT_TYPE_KEY, Permission, ReasonCode, RoCrateCheckpointRefs, RoCrateMediaType,
    VersionedObjectArn, blob_bucket_permission_path, blob_object_permission_path, job_entry_key,
    rocrate_plan_key,
};
use aruna_core::types::Value;
use bytes::Bytes;
use byteview::ByteView;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, BufReader, SeekFrom};
use tokio::sync::mpsc;
use ulid::Ulid;

use self::archive::{
    ArchiveCompression, ArchiveInspection, file_id_candidates, inspect_reader, payload_entries,
    read_metadata, signature_entry,
};
use self::reader::HiddenRangeReader;
use self::rewrite::{CrateValidationError, RewriteTarget, rewrite_document, validate_document};
use super::executor::{JobContext, JobRunOutcome};
use super::store::{list_job_entries, put_job_entry, put_rocrate_checkpoint, put_rocrate_plan};
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
    archive_path: String,
    path: String,
    target_key: String,
    described_id: Option<String>,
    code: ReasonCode,
    data_offset: u64,
    compressed_size: u64,
    uncompressed_size: u64,
    crc32: u32,
    compression: ArchiveCompression,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ImportPlan {
    metadata_json: String,
    entries: Vec<ImportEntryPlan>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ImportCheckpoint {
    refs: RoCrateCheckpointRefs,
    phase: ImportPhase,
    input: Option<ImportInput>,
    inspection: Option<ArchiveInspection>,
    metadata_json: Option<String>,
    next_entry: usize,
    entries_total: u64,
    imported: u64,
    unlisted: u64,
    failed: u64,
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
            next_entry: 0,
            entries_total: 0,
            imported: 0,
            unlisted: 0,
            failed: 0,
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
    let mut plan = if matches!(
        checkpoint.phase,
        ImportPhase::Write | ImportPhase::Rewrite | ImportPhase::Create
    ) {
        match read_plan(ctx).await {
            Ok(Some(plan)) => Some(plan),
            Ok(None) => {
                return JobRunOutcome::Failed(JobError::permanent("import plan is missing"));
            }
            Err(error) => return retryable_error(error),
        }
    } else {
        None
    };

    loop {
        if ctx.shutdown.is_cancelled() {
            return JobRunOutcome::Interrupted;
        }
        if ctx.cancel.is_cancelled()
            && checkpoint.phase != ImportPhase::Cleanup
            && checkpoint.phase != ImportPhase::Done
        {
            checkpoint.cancelled = true;
            if let Err(error) = mark_not_attempted(ctx, plan.as_ref(), &mut checkpoint).await {
                return retryable_error(error);
            }
            checkpoint.phase = ImportPhase::Cleanup;
            if let Err(error) = persist_checkpoint(ctx, &checkpoint).await {
                return retryable_error(error);
            }
        }

        let result =
            match checkpoint.phase {
                ImportPhase::Acquire => acquire_source(ctx, spec).await.map(|input| {
                    checkpoint.refs.hidden_locations = vec![input.location.clone()];
                    checkpoint.input = Some(input);
                    checkpoint.phase = ImportPhase::Inspect;
                }),
                ImportPhase::Inspect => inspect_source(ctx, spec, &checkpoint).await.map(
                    |(inspection, metadata_json)| {
                        checkpoint.inspection = Some(inspection);
                        checkpoint.metadata_json = Some(metadata_json);
                        checkpoint.phase = ImportPhase::Validate;
                    },
                ),
                ImportPhase::Validate => match validate_source(ctx, spec, &mut checkpoint).await {
                    Ok(validated) => {
                        plan = Some(validated);
                        Ok(())
                    }
                    Err(error) => Err(error),
                },
                ImportPhase::Write => match plan.as_ref() {
                    Some(plan) => write_next(ctx, spec, &mut checkpoint, plan).await,
                    None => Err(ImportFailure::Permanent(
                        "import plan is missing".to_string(),
                    )),
                },
                ImportPhase::Rewrite => match plan.as_ref() {
                    Some(plan) => rewrite_crate(ctx, spec, &mut checkpoint, plan).await,
                    None => Err(ImportFailure::Permanent(
                        "import plan is missing".to_string(),
                    )),
                },
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
                if let Err(error) = mark_not_attempted(ctx, plan.as_ref(), &mut checkpoint).await {
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
                    && let Err(error) =
                        mark_write_failure(ctx, plan.as_ref(), &mut checkpoint).await
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

pub(crate) async fn cleanup_after_panic(ctx: &JobContext) -> JobRunOutcome {
    const PANIC_MESSAGE: &str = "job payload panicked";

    let mut checkpoint = match read_checkpoint(ctx).await {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => ImportCheckpoint::default(),
        Err(error) => return retryable_error(error),
    };
    let phase = checkpoint.phase;
    let mut cleanup_errors = Vec::new();
    if let Err(error) = write_phase_error(ctx, phase, PANIC_MESSAGE).await {
        cleanup_errors.push(error);
    }
    checkpoint.failure = Some(PANIC_MESSAGE.to_string());
    if phase == ImportPhase::Write {
        match read_plan(ctx).await {
            Ok(plan) => {
                if let Err(error) = mark_write_failure(ctx, plan.as_ref(), &mut checkpoint).await {
                    cleanup_errors.push(error);
                }
            }
            Err(error) => cleanup_errors.push(error),
        }
    }
    checkpoint.phase = ImportPhase::Cleanup;
    if let Err(error) = persist_checkpoint(ctx, &checkpoint).await {
        cleanup_errors.push(error);
    }
    if let Err(error) = cleanup_source(ctx, &mut checkpoint).await {
        cleanup_errors.push(failure_message(error));
    }
    if let Err(error) = persist_checkpoint(ctx, &checkpoint).await {
        cleanup_errors.push(error);
    }
    let message = if cleanup_errors.is_empty() {
        PANIC_MESSAGE.to_string()
    } else {
        format!(
            "{PANIC_MESSAGE}; cleanup errors: {}",
            cleanup_errors.join("; ")
        )
    };
    JobRunOutcome::Failed(JobError::permanent(message))
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
    if let Some(size) = expected_size {
        precheck_size(size, limit)?;
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
        let mut receiver = receiver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Pin::new(&mut *receiver).poll_recv(cx)
    }))
}

async fn inspect_source(
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
    checkpoint: &ImportCheckpoint,
) -> Result<(ArchiveInspection, String), ImportFailure> {
    let input = checkpoint
        .input
        .as_ref()
        .ok_or_else(|| ImportFailure::Permanent("import source is missing".to_string()))?;
    precheck_size(input.size, spec.limits.import_source_bytes)?;
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
    let (inspection, mut reader) = inspect_reader(
        handle,
        input.location.clone(),
        input.size,
        input.eln,
        &spec.limits,
    )
    .await
    .map_err(ImportFailure::Permanent)?;
    let jsonld = read_metadata(
        &mut reader,
        inspection.metadata_index,
        spec.limits.metadata_bytes,
    )
    .await
    .map_err(ImportFailure::Permanent)?;
    Ok((inspection, jsonld))
}

async fn validate_source(
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
    checkpoint: &mut ImportCheckpoint,
) -> Result<ImportPlan, ImportFailure> {
    ensure_targets(ctx, spec).await?;
    ensure_path_free(ctx, spec).await?;
    let inspection = checkpoint
        .inspection
        .as_ref()
        .ok_or_else(|| ImportFailure::Permanent("archive inspection is missing".to_string()))?;
    let jsonld = checkpoint
        .metadata_json
        .as_ref()
        .ok_or_else(|| ImportFailure::Permanent("RO-Crate metadata is missing".to_string()))?;
    let validated = validate_document(jsonld).map_err(validation_failure)?;
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
            archive_path: entry.archive_path.clone(),
            path,
            target_key,
            code: if described_id.is_some() {
                ReasonCode::Imported
            } else {
                ReasonCode::Unlisted
            },
            described_id,
            data_offset: entry.data_offset,
            compressed_size: entry.compressed_size,
            uncompressed_size: entry.uncompressed_size,
            crc32: entry.crc32,
            compression: entry.compression,
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
        write_system_report(ctx, &row).await?;
    }
    let plan = ImportPlan {
        metadata_json: jsonld.clone(),
        entries,
    };
    persist_plan(ctx, &plan)
        .await
        .map_err(ImportFailure::Retryable)?;
    checkpoint.inspection = None;
    checkpoint.metadata_json = None;
    checkpoint.entries_total = plan.entries.len() as u64;
    checkpoint.phase = ImportPhase::Write;
    ctx.progress.set_total(checkpoint.entries_total);
    Ok(plan)
}

async fn write_next(
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
    checkpoint: &mut ImportCheckpoint,
    plan: &ImportPlan,
) -> Result<(), ImportFailure> {
    if checkpoint.next_entry >= plan.entries.len() {
        checkpoint.phase = ImportPhase::Rewrite;
        return Ok(());
    }
    let index = checkpoint.next_entry;
    let entry = &plan.entries[index];
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
    let mut row = read_report(ctx, &entry.path)
        .await?
        .unwrap_or_else(|| entry_report(entry));
    if row.detail.version_id.is_none() {
        row.detail.version_id = Some(Ulid::generate());
        write_report(ctx, &row).await?;
    }
    let version_id = row
        .detail
        .version_id
        .ok_or_else(|| ImportFailure::Permanent("import version fence is missing".to_string()))?;
    let body = payload_stream(
        ctx,
        &input.location,
        input.size,
        entry,
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
    let result = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: spec.auth_context.user_id,
            group_id: bucket_info.group_id,
            realm_id: spec.auth_context.realm_id,
            node_id: ctx.owner_node_id,
            request: PutObjectInput {
                bucket: spec.target.bucket.clone(),
                key: entry.target_key.clone(),
                content_length: Some(entry.uncompressed_size),
                body: Some(body),
            },
            expected_checksums: vec![ExpectedChecksum {
                algorithm: ChecksumAlgorithm::Crc32,
                digest: entry.crc32.to_be_bytes().to_vec(),
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
    row.detail.version_id = Some(result.version_id);
    row.detail.blake3 = Some(hex::encode(hash));
    row.detail.size = Some(result.location.blob_size);
    row.detail.arn = Some(arn.to_string());
    row.detail.w3id = Some(arn.to_w3id());
    write_report(ctx, &row).await?;
    match entry.code {
        ReasonCode::Imported => checkpoint.imported = checkpoint.imported.saturating_add(1),
        ReasonCode::Unlisted => checkpoint.unlisted = checkpoint.unlisted.saturating_add(1),
        _ => {}
    }
    checkpoint.next_entry = checkpoint.next_entry.saturating_add(1);
    let _ = drive(
        QueueLiveVersionReplicationOperation::new(QueueLiveVersionReplicationInput {
            local_node_id: ctx.owner_node_id,
            auth_context: spec.auth_context.clone(),
            bucket: spec.target.bucket.clone(),
            key: entry.target_key.clone(),
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
    plan: &ImportPlan,
) -> Result<(), ImportFailure> {
    let validated = validate_document(&plan.metadata_json).map_err(validation_failure)?;
    let reports = load_reports(ctx).await?;
    let mut targets = HashMap::new();
    for entry in &plan.entries {
        let Some(file_id) = &entry.described_id else {
            continue;
        };
        let report = reports
            .get(&entry.path)
            .ok_or_else(|| ImportFailure::Permanent("import report row is missing".to_string()))?;
        let version_id = report
            .detail
            .version_id
            .ok_or_else(|| ImportFailure::Permanent("imported version is missing".to_string()))?;
        let hash: [u8; 32] = report
            .detail
            .blake3
            .as_deref()
            .ok_or_else(|| ImportFailure::Permanent("imported hash is missing".to_string()))
            .and_then(|hash| {
                hex::decode(hash)
                    .ok()
                    .and_then(|hash| hash.try_into().ok())
                    .ok_or_else(|| ImportFailure::Permanent("imported hash is invalid".to_string()))
            })?;
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
        write_system_report(ctx, &row).await?;
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
    entry: &ImportEntryPlan,
    cancel: tokio_util::sync::CancellationToken,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<BackendStream<Result<Bytes, aruna_core::stream::StreamError>>, ImportFailure> {
    let handle = ctx
        .driver
        .blob_handle
        .as_ref()
        .cloned()
        .ok_or_else(|| ImportFailure::Retryable("import needs a blob handle".to_string()))?;
    let mut source = HiddenRangeReader::new(handle, location.clone(), size);
    source
        .seek(SeekFrom::Start(entry.data_offset))
        .await
        .map_err(|error| ImportFailure::Permanent(error.to_string()))?;
    let source = BufReader::new(source.take(entry.compressed_size));
    let mut reader: Pin<Box<dyn AsyncRead + Send>> = match entry.compression {
        ArchiveCompression::Stored => Box::pin(source),
        ArchiveCompression::Deflate => Box::pin(
            async_compression::tokio::bufread::DeflateDecoder::new(source),
        ),
    };
    let expected_size = entry.uncompressed_size;
    let (sender, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    tokio::spawn(async move {
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
                Ok(0) => {
                    if expanded != expected_size {
                        let _ = sender
                            .send(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "ZIP entry is shorter than its declared expanded size",
                            )))
                            .await;
                    }
                    break;
                }
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
        let mut receiver = receiver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Pin::new(&mut *receiver).poll_recv(cx)
    })))
}

async fn mark_write_failure(
    ctx: &JobContext,
    plan: Option<&ImportPlan>,
    checkpoint: &mut ImportCheckpoint,
) -> Result<(), String> {
    let Some(plan) = plan else {
        return Ok(());
    };
    for (index, entry) in plan.entries.iter().enumerate() {
        if index < checkpoint.next_entry {
            continue;
        }
        let current = index == checkpoint.next_entry;
        let mut row = read_report(ctx, &entry.path)
            .await
            .map_err(failure_message)?
            .unwrap_or_else(|| entry_report(entry));
        row.code = if current {
            ReasonCode::Failed
        } else {
            ReasonCode::NotAttempted
        };
        row.message = Some(if current {
            checkpoint
                .failure
                .clone()
                .unwrap_or_else(|| "payload import failed".to_string())
        } else {
            "not attempted after an earlier import failure".to_string()
        });
        write_report(ctx, &row).await.map_err(failure_message)?;
    }
    Ok(())
}

async fn mark_not_attempted(
    ctx: &JobContext,
    plan: Option<&ImportPlan>,
    checkpoint: &mut ImportCheckpoint,
) -> Result<(), String> {
    let Some(plan) = plan else {
        return Ok(());
    };
    for (index, entry) in plan.entries.iter().enumerate() {
        if index < checkpoint.next_entry {
            continue;
        }
        let mut row = read_report(ctx, &entry.path)
            .await
            .map_err(failure_message)?
            .unwrap_or_else(|| entry_report(entry));
        row.code = ReasonCode::NotAttempted;
        row.message = Some("not attempted because the import was cancelled".to_string());
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
    write_system_report(ctx, &row)
        .await
        .map_err(failure_message)
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
        write_system_report(ctx, &row)
            .await
            .map_err(failure_message)?;
    }
    Ok(())
}

async fn write_report(ctx: &JobContext, row: &ImportReportRow) -> Result<(), ImportFailure> {
    put_report(ctx, row.entry_key.as_bytes(), row).await
}

async fn write_system_report(ctx: &JobContext, row: &ImportReportRow) -> Result<(), ImportFailure> {
    let key = system_report_key(&row.entry_key);
    put_report(ctx, &key, row).await
}

async fn put_report(
    ctx: &JobContext,
    entry_key: &[u8],
    row: &ImportReportRow,
) -> Result<(), ImportFailure> {
    put_job_entry(
        &ctx.driver.storage_handle,
        ctx.job_id,
        ctx.claim_token,
        entry_key,
        row,
    )
    .await
    .map_err(|error| ImportFailure::Retryable(error.to_string()))
}

fn system_report_key(entry_key: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(entry_key.len().saturating_add(1));
    key.push(JOB_SYSTEM_ENTRY_PREFIX);
    key.extend_from_slice(entry_key.as_bytes());
    key
}

fn entry_report(entry: &ImportEntryPlan) -> ImportReportRow {
    ImportReportRow {
        entry_key: entry.path.clone(),
        code: entry.code,
        message: None,
        detail: ImportReportDetail {
            archive_path: entry.archive_path.clone(),
            target_key: Some(entry.target_key.clone()),
            version_id: None,
            blake3: None,
            size: None,
            arn: None,
            w3id: None,
            validation: None,
        },
    }
}

async fn read_report(
    ctx: &JobContext,
    entry_key: &str,
) -> Result<Option<ImportReportRow>, ImportFailure> {
    match ctx
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: JOB_ENTRY_KEYSPACE.to_string(),
            key: job_entry_key(ctx.job_id, entry_key.as_bytes()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => postcard::from_bytes(value.as_ref())
            .map(Some)
            .map_err(|error| ImportFailure::Retryable(error.to_string())),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(ImportFailure::Retryable(error.to_string()))
        }
        other => Err(ImportFailure::Retryable(format!(
            "unexpected import report event: {other:?}"
        ))),
    }
}

async fn load_reports(ctx: &JobContext) -> Result<HashMap<String, ImportReportRow>, ImportFailure> {
    let mut rows = HashMap::new();
    let mut cursor = None;
    loop {
        let (page, next) = list_job_entries(&ctx.driver.storage_handle, ctx.job_id, cursor, 512)
            .await
            .map_err(ImportFailure::Retryable)?;
        for (entry_key, value) in page {
            if entry_key.first() == Some(&JOB_SYSTEM_ENTRY_PREFIX) {
                continue;
            }
            let row: ImportReportRow = postcard::from_bytes(value.as_ref())
                .map_err(|error| ImportFailure::Retryable(error.to_string()))?;
            rows.insert(row.entry_key.clone(), row);
        }
        let Some(next) = next else {
            return Ok(rows);
        };
        cursor = Some(next);
    }
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

async fn read_plan(ctx: &JobContext) -> Result<Option<ImportPlan>, String> {
    match ctx
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: ROCRATE_JOB_STATE_KEYSPACE.to_string(),
            key: rocrate_plan_key(ctx.job_id),
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
        other => Err(format!("unexpected import plan event: {other:?}")),
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

async fn persist_plan(ctx: &JobContext, plan: &ImportPlan) -> Result<(), String> {
    let value = postcard::to_allocvec(plan).map_err(|error| error.to_string())?;
    put_rocrate_plan(
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

fn precheck_size(size: u64, limit: u64) -> Result<(), ImportFailure> {
    if size > limit {
        return Err(ImportFailure::Permanent(format!(
            "import source exceeds limit {limit}"
        )));
    }
    Ok(())
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
    if checkpoint.entries_total > 0 {
        ctx.progress.set_total(checkpoint.entries_total);
    }
}

fn import_outcome(checkpoint: &ImportCheckpoint, spec: &ImportRoCrateSpec) -> JobRunOutcome {
    if checkpoint.cancelled {
        return JobRunOutcome::Cancelled;
    }
    if let Some(error) = &checkpoint.failure {
        return JobRunOutcome::Failed(JobError::permanent(error.clone()));
    }
    JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(ImportRoCrateResult {
        document_id: checkpoint.created.then_some(spec.document_id),
        entries_total: checkpoint.entries_total,
        imported: checkpoint.imported,
        unlisted: checkpoint.unlisted,
        failed: checkpoint.failed,
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
    use aruna_core::structs::{
        ImportMetadataTarget, ImportRoCrateTarget, JobClaim, JobId, JobPayload, JobRecord,
        JobState, RealmId, RoCrateUploadRecord,
    };
    use aruna_core::types::UserId;
    use tokio_util::sync::CancellationToken;

    use crate::jobs::executor::ProgressReporter;
    use crate::jobs::store::insert_job;
    use crate::staging::test_utils::setup_driver_context;

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
    fn source_checks_size() {
        assert!(precheck_size(8, 8).is_ok());
        assert!(matches!(
            precheck_size(9, 8),
            Err(ImportFailure::Permanent(message))
                if message == "import source exceeds limit 8"
        ));
    }

    #[test]
    fn report_keys_disjoint() {
        for entry_key in [
            "signature/ro-crate-metadata.json.minisig",
            "warning/00000000",
            "failure/write",
        ] {
            let system_key = system_report_key(entry_key);
            assert_ne!(system_key, entry_key.as_bytes());
            assert_eq!(system_key.first(), Some(&JOB_SYSTEM_ENTRY_PREFIX));
            assert_eq!(&system_key[1..], entry_key.as_bytes());
        }
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

    #[tokio::test]
    async fn panic_releases_upload() {
        let fixture = setup_driver_context().await;
        let driver = Arc::new(fixture.driver_context.clone());
        let blob = driver.blob_handle.as_ref().unwrap();
        let realm_id = RealmId::from_bytes([1; 32]);
        let owner = UserId::new(Ulid::from_bytes([2; 16]), realm_id);
        let upload_id = Ulid::from_bytes([3; 16]);
        let job_id = JobId::from_bytes([4; 16]);
        let token = Ulid::from_bytes([5; 16]);
        let Event::Blob(BlobEvent::HiddenSpooled {
            location,
            blake3,
            size,
        }) = blob
            .send_blob_effect(BlobEffect::SpoolHidden {
                namespace: upload_id,
                name: "input".to_string(),
                created_by: owner,
                max_bytes: Some(1),
                blob: BackendStream::new(stream::iter([Ok::<Bytes, io::Error>(
                    Bytes::from_static(b"x"),
                )])),
            })
            .await
        else {
            panic!("upload spool must succeed")
        };
        write_rocrate_upload(
            &driver.storage_handle,
            &RoCrateUploadRecord {
                upload_id,
                owner,
                location: location.clone(),
                blake3,
                size,
                media_type: RoCrateMediaType::Zip,
                expires_at_ms: u64::MAX,
                claimed_by: Some(job_id),
            },
        )
        .await
        .unwrap();

        let node_id = iroh::SecretKey::from_bytes(&[6; 32]).public();
        let mut record = JobRecord::new(
            job_id,
            JobPayload::ImportRoCrate(ImportRoCrateSpec {
                auth_context: AuthContext {
                    user_id: owner,
                    realm_id,
                    path_restrictions: None,
                },
                source: ImportRoCrateSource::Upload { upload_id },
                target: ImportRoCrateTarget {
                    bucket: "target".to_string(),
                    prefix: "crate".to_string(),
                },
                metadata: ImportMetadataTarget {
                    group_id: Ulid::from_bytes([7; 16]),
                    path: "crate".to_string(),
                    public: false,
                },
                limits: Default::default(),
                document_id: Ulid::from_bytes([8; 16]),
            }),
            owner,
            node_id,
            1,
            1,
            None,
        );
        record.state = JobState::Running;
        record.claim = Some(JobClaim {
            holder_node_id: node_id,
            claim_token: token,
            lease_expires_at_ms: u64::MAX,
        });
        insert_job(&driver.storage_handle, &record).await.unwrap();
        let ctx = JobContext {
            driver: driver.clone(),
            job_id,
            owner_node_id: node_id,
            claim_token: token,
            final_attempt: true,
            cancel: CancellationToken::new(),
            shutdown: CancellationToken::new(),
            progress: ProgressReporter::from_progress(&record.progress),
        };
        persist_checkpoint(
            &ctx,
            &ImportCheckpoint {
                refs: RoCrateCheckpointRefs {
                    hidden_locations: vec![location.clone()],
                },
                input: Some(ImportInput {
                    location,
                    size,
                    blake3,
                    upload_id: Some(upload_id),
                    eln: false,
                }),
                phase: ImportPhase::Inspect,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let JobRunOutcome::Failed(error) = cleanup_after_panic(&ctx).await else {
            panic!("panic cleanup must fail the job")
        };
        assert_eq!(error.kind, aruna_core::structs::JobErrorKind::Permanent);
        assert!(
            load_rocrate_upload(&driver, upload_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            blob.send_blob_effect(BlobEffect::ListHidden {
                namespace: Some(upload_id),
            })
            .await,
            Event::Blob(BlobEvent::HiddenListed { entries }) if entries.is_empty()
        ));
    }
}
