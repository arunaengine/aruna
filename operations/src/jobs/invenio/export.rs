//! Publishes mapped metadata and individual crate files as a native repository record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Arc, Mutex};

use aruna_blob::hash::Hasher;
use aruna_blob::invenio::{InvenioClient, InvenioError};
use aruna_core::invenio::{
    InvenioDestination, InvenioRecord, export_metadata, record_id, validate_id,
};
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::job::{ArtifactRef, ExportRoCrateSpec};
use aruna_core::structs::identity::auth::Permission;
use futures_util::StreamExt;
use reqwest::Method;
use serde_json::{Value, json};

use super::{TransferError, connect};
use crate::jobs::executor::JobContext;
use crate::jobs::import::archive::{ArchiveCompression, ArchiveEntry, inspect_reader};
use crate::jobs::service::read_artifact_range;

pub(crate) async fn create_draft(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &InvenioDestination,
    jsonld: &str,
) -> Result<InvenioRecord, TransferError> {
    let credential = destination
        .credential
        .as_ref()
        .ok_or_else(|| invalid("a personal repository login is required"))?;
    let client = connect(
        ctx,
        &spec.auth_context,
        destination.group_id,
        destination.connector_id,
        Permission::WRITE,
        spec.limits.metadata_bytes,
        Some(credential),
    )
    .await?;
    if destination.metadata_json.len() as u64 > spec.limits.metadata_bytes {
        return Err(invalid("repository metadata exceeds limit"));
    }
    let overrides: Value = serde_json::from_str(&destination.metadata_json)
        .map_err(|_| invalid("invalid repository metadata"))?;
    let document: Value =
        serde_json::from_str(jsonld).map_err(|_| invalid("invalid source crate"))?;
    let metadata = export_metadata(&document, &overrides)?;
    if metadata.to_string().len() as u64 > spec.limits.metadata_bytes {
        return Err(invalid("mapped repository metadata exceeds limit"));
    }
    let record = if let Some(id) = &destination.draft_id {
        validate_id(id)?;
        let draft = client
            .json(Method::GET, client.url(&["records", id, "draft"])?, None)
            .await?;
        if record_id(&draft)? != id || draft["is_published"] != false {
            return Err(invalid("export requires the selected unpublished draft"));
        }
        client
            .json(
                Method::PUT,
                client.url(&["records", id, "draft"])?,
                Some(&json!({"metadata": metadata})),
            )
            .await?
    } else {
        client
            .json(
                Method::POST,
                client.url(&["records"])?,
                Some(&json!({
                    "metadata": metadata, "files": {"enabled": true},
                    "access": {"record": "public", "files": if destination.public_files { "public" } else { "restricted" }}
                })),
            )
            .await?
    };
    if record["is_published"] != false {
        return Err(invalid("export requires an unpublished draft"));
    }
    let id = record_id(&record)?.to_string();
    if destination
        .draft_id
        .as_ref()
        .is_some_and(|requested| requested != &id)
    {
        return Err(invalid("draft identity mismatch"));
    }
    Ok(InvenioRecord {
        url: client.url(&["records", &id, "draft"])?.to_string(),
        id,
        published: false,
    })
}

pub(crate) async fn deposit(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &InvenioDestination,
    record: &InvenioRecord,
    artifact: &ArtifactRef,
) -> Result<InvenioRecord, TransferError> {
    let credential = destination
        .credential
        .as_ref()
        .ok_or_else(|| invalid("a personal repository login is required"))?;
    let client = connect(
        ctx,
        &spec.auth_context,
        destination.group_id,
        destination.connector_id,
        Permission::WRITE,
        spec.limits.metadata_bytes,
        Some(credential),
    )
    .await?;
    if client.url(&["records", &record.id, "draft"])?.as_str() != record.url {
        return Err(invalid(
            "repository connector endpoint changed during export",
        ));
    }
    let (draft, published) = match client
        .json(
            Method::GET,
            client.url(&["records", &record.id, "draft"])?,
            None,
        )
        .await
    {
        Ok(draft) => (draft, false),
        Err(InvenioError::Status(404)) if destination.publish => (
            client
                .json(Method::GET, client.url(&["records", &record.id])?, None)
                .await?,
            true,
        ),
        Err(error) => return Err(error.into()),
    };
    if record_id(&draft)? != record.id || draft["is_published"] != published {
        return Err(invalid(
            "export record identity or publication state changed",
        ));
    }
    let blob = ctx
        .driver
        .blob_handle
        .as_ref()
        .ok_or_else(|| TransferError::Retryable("blob handle unavailable".into()))?;
    let mut limits = spec.limits.clone();
    limits.import_source_bytes = spec.limits.export_artifact_bytes;
    limits.expanded_import_bytes = spec.limits.export_artifact_bytes;
    limits.max_entries = limits.max_entries.saturating_add(2);
    let (inspection, _) = inspect_reader(
        blob.clone(),
        artifact.location.clone(),
        artifact.size,
        false,
        &limits,
    )
    .await
    .map_err(TransferError::Permanent)?;
    let files_url = if published {
        client.url(&["records", &record.id, "files"])?
    } else {
        client.url(&["records", &record.id, "draft", "files"])?
    };
    let files = client.json(Method::GET, files_url.clone(), None).await?;
    let entries = files["entries"]
        .as_array()
        .ok_or_else(|| invalid("missing draft files"))?;
    let paths = inspection
        .entries
        .iter()
        .filter(|entry| !entry.directory)
        .map(|entry| entry.path.as_str())
        .collect::<std::collections::HashSet<_>>();
    let mut remote = std::collections::BTreeMap::new();
    for file in entries {
        let key = file["key"]
            .as_str()
            .ok_or_else(|| invalid("missing repository file key"))?;
        if remote.insert(key, file).is_some() || !paths.contains(key) {
            return Err(invalid(
                "repository draft contains duplicate or unrelated files",
            ));
        }
    }
    ctx.progress.set_total(paths.len() as u64);
    ctx.progress.set_current(0);
    for (index, entry) in inspection
        .entries
        .iter()
        .filter(|entry| !entry.directory)
        .enumerate()
    {
        let existing = remote.get(entry.path.as_str()).copied();
        upload_entry(ctx, &client, record, artifact, entry, existing, published).await?;
        ctx.progress.set_current(index as u64 + 1);
    }
    finish(&client, record, destination.publish, published).await
}

async fn upload_entry(
    ctx: &JobContext,
    client: &InvenioClient<'_>,
    record: &InvenioRecord,
    artifact: &ArtifactRef,
    entry: &ArchiveEntry,
    existing: Option<&Value>,
    published: bool,
) -> Result<(), TransferError> {
    if entry.compression != ArchiveCompression::Stored
        || entry.compressed_size != entry.uncompressed_size
    {
        return Err(invalid("repository snapshot entry is not stored verbatim"));
    }
    let range = entry.data_offset
        ..entry
            .data_offset
            .checked_add(entry.uncompressed_size)
            .filter(|end| *end <= artifact.size)
            .ok_or_else(|| invalid("repository file exceeds snapshot"))?;
    let mut read = read_artifact_range(&ctx.driver, artifact, range.clone())
        .await
        .map_err(TransferError::Retryable)?;
    let mut expected = Hasher::new();
    let mut size = 0u64;
    while let Some(chunk) = read.blob.next().await {
        let chunk = chunk.map_err(|_| TransferError::Retryable("crate file read failed".into()))?;
        size = size
            .checked_add(chunk.len() as u64)
            .filter(|size| *size <= entry.uncompressed_size)
            .ok_or_else(|| invalid("crate file exceeds snapshot size"))?;
        expected.update(&chunk);
    }
    if size != entry.uncompressed_size || expected.finalize().crc32 != entry.crc32.to_be_bytes() {
        return Err(invalid("crate file differs from snapshot"));
    }
    let key = &entry.path;
    let commit_url = client.url(&["records", &record.id, "draft", "files", key, "commit"])?;
    if let Some(file) = existing
        && file["checksum"].is_string()
    {
        verify_file(file, &expected, size, false)?;
        if file["status"] == "completed" {
            return Ok(());
        }
        if published {
            return Err(invalid("published record contains an incomplete file"));
        }
        let file = client.json(Method::POST, commit_url, None).await?;
        return verify_file(&file, &expected, size, true);
    }
    if published {
        return Err(invalid("published record is missing a complete crate file"));
    }
    if existing.is_none() {
        client
            .json(
                Method::POST,
                client.url(&["records", &record.id, "draft", "files"])?,
                Some(&json!([{"key": key}])),
            )
            .await?;
    }
    let read = read_artifact_range(&ctx.driver, artifact, range)
        .await
        .map_err(TransferError::Retryable)?;
    let hasher = Arc::new(Mutex::new(Hasher::new()));
    let hash_copy = hasher.clone();
    let stream = read.blob.map(move |chunk| {
        if let Ok(bytes) = &chunk {
            hash_copy
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .update(bytes);
        }
        chunk
    });
    client
        .upload(
            client.url(&["records", &record.id, "draft", "files", key, "content"])?,
            size,
            BackendStream::new(stream),
        )
        .await?;
    if hasher
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .finalize()
        .blake3
        != expected.finalize().blake3
    {
        return Err(invalid("crate file changed while uploading"));
    }
    let file = client.json(Method::POST, commit_url, None).await?;
    verify_file(&file, &expected, size, true)
}

async fn finish(
    client: &InvenioClient<'_>,
    record: &InvenioRecord,
    publish: bool,
    published: bool,
) -> Result<InvenioRecord, TransferError> {
    if publish && !published {
        let result = client
            .json(
                Method::POST,
                client.url(&["records", &record.id, "draft", "actions", "publish"])?,
                None,
            )
            .await?;
        if record_id(&result)? != record.id || result["is_published"] != true {
            return Err(invalid("repository did not confirm publication"));
        }
    }
    Ok(InvenioRecord {
        id: record.id.clone(),
        url: if publish {
            client.url(&["records", &record.id])?.to_string()
        } else {
            record.url.clone()
        },
        published: publish,
    })
}

fn verify_file(
    file: &Value,
    hasher: &Hasher,
    size: u64,
    completed: bool,
) -> Result<(), TransferError> {
    let checksum = file["checksum"]
        .as_str()
        .ok_or_else(|| invalid("missing uploaded checksum"))?;
    let (algorithm, digest) = checksum
        .split_once(':')
        .ok_or_else(|| invalid("invalid uploaded checksum"))?;
    let hashes = hasher.to_map();
    if (completed && file["status"] != "completed")
        || file["size"].as_u64() != Some(size)
        || hashes.get(algorithm).map(hex::encode).as_deref() != Some(digest)
    {
        return Err(invalid("repository file checksum or size mismatch"));
    }
    Ok(())
}

fn invalid(message: &str) -> TransferError {
    TransferError::Permanent(message.into())
}
