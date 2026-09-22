//! Publishes mapped metadata and individual crate files as a native repository record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Arc, Mutex};

use aruna_blob::hash::Hasher;
use aruna_blob::invenio::{InvenioClient, InvenioError};
use aruna_core::invenio::{
    InvenioDestination, InvenioRecord, export_fields, record_id, validate_id,
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
        &ctx.driver,
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
    let mut fields = export_fields(&document, &overrides)?;
    if fields.to_string().len() as u64 > spec.limits.metadata_bytes {
        return Err(invalid("mapped repository metadata exceeds limit"));
    }
    let parent = if let Some(id) = &destination.new_version {
        validate_id(id)?;
        let source = client
            .json(Method::GET, client.url(&["records", id])?, None)
            .await?;
        if source["is_published"] != true {
            return Err(invalid("new version requires a published record"));
        }
        Some(
            source["parent"]["id"]
                .as_str()
                .ok_or_else(|| invalid("missing parent identity"))?
                .to_string(),
        )
    } else {
        None
    };
    let record = if let Some(id) = &destination.draft_id {
        validate_id(id)?;
        client
            .json(Method::GET, client.url(&["records", id, "draft"])?, None)
            .await?
    } else if let Some(id) = &destination.new_version {
        client
            .json(
                Method::POST,
                client.url(&["records", id, "versions"])?,
                None,
            )
            .await?
    } else {
        fields["files"] = json!({"enabled": true});
        fields["access"] = json!({"record": "public", "files": if destination.public_files { "public" } else { "restricted" }});
        client
            .json(Method::POST, client.url(&["records"])?, Some(&fields))
            .await?
    };
    let parent_id = record["parent"]["id"]
        .as_str()
        .ok_or_else(|| invalid("missing draft parent"))?
        .to_string();
    if parent.is_some_and(|parent| parent != parent_id) {
        return Err(invalid("new version belongs to another record"));
    }
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
        parent_id,
        revision_id: record["revision_id"]
            .as_u64()
            .ok_or_else(|| invalid("missing draft revision"))?,
        doi: record["pids"]["doi"]["identifier"]
            .as_str()
            .map(str::to_string),
    })
}

pub(crate) async fn prepare_draft(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &InvenioDestination,
    record: &InvenioRecord,
    jsonld: &str,
) -> Result<(InvenioRecord, [u8; 32]), TransferError> {
    let credential = destination
        .credential
        .as_ref()
        .ok_or_else(|| invalid("personal login missing"))?;
    let client = connect(
        &ctx.driver,
        &spec.auth_context,
        destination.group_id,
        destination.connector_id,
        Permission::WRITE,
        spec.limits.metadata_bytes,
        Some(credential),
    )
    .await?;
    let document: Value =
        serde_json::from_str(jsonld).map_err(|_| invalid("invalid crate metadata"))?;
    let overrides: Value = serde_json::from_str(&destination.metadata_json)
        .map_err(|_| invalid("invalid metadata overrides"))?;
    let mut fields = export_fields(&document, &overrides)?;
    fields["files"] = json!({"enabled": true});
    let url = client.url(&["records", &record.id, "draft"])?;
    let current = client.json(Method::GET, url.clone(), None).await?;
    if record_id(&current)? != record.id
        || current["parent"]["id"] != record.parent_id
        || current["is_published"] != false
    {
        return Err(invalid("repository draft identity changed"));
    }
    let draft = if destination.draft_id.is_some() || destination.new_version.is_some() {
        if current["revision_id"].as_u64() == Some(record.revision_id) {
            client.update(url, &fields, record.revision_id).await?
        } else if current["revision_id"].as_u64() == record.revision_id.checked_add(1)
            && complete_fields(&fields["metadata"], &current["metadata"])
            && complete_fields(&fields["custom_fields"], &current["custom_fields"])
        {
            current
        } else {
            return Err(invalid(
                "draft changed after an ambiguous metadata update; inspect and recover with draft_id",
            ));
        }
    } else {
        current
    };
    if record_id(&draft)? != record.id
        || draft["parent"]["id"] != record.parent_id
        || draft["is_published"] != false
    {
        return Err(invalid("repository draft identity changed"));
    }
    verify_metadata(&fields, &draft)?;
    if destination.draft_id.is_none()
        && destination.new_version.is_none()
        && draft["revision_id"].as_u64() != Some(record.revision_id)
    {
        return Err(invalid("repository metadata changed during creation"));
    }
    let mut record = record.clone();
    record.revision_id = draft["revision_id"]
        .as_u64()
        .ok_or_else(|| invalid("missing draft revision"))?;
    Ok((record, metadata_digest(&draft)))
}

pub(crate) async fn deposit(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &InvenioDestination,
    record: &InvenioRecord,
    artifact: &ArtifactRef,
    metadata: [u8; 32],
) -> Result<InvenioRecord, TransferError> {
    let credential = destination
        .credential
        .as_ref()
        .ok_or_else(|| invalid("a personal repository login is required"))?;
    let client = connect(
        &ctx.driver,
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
    if record_id(&draft)? != record.id
        || draft["is_published"] != published
        || draft["parent"]["id"] != record.parent_id
    {
        return Err(invalid(
            "export record identity or publication state changed",
        ));
    }
    if metadata_digest(&draft) != metadata {
        return Err(invalid("repository metadata changed after preparation"));
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
    let mut verified = std::collections::BTreeMap::new();
    for (index, entry) in inspection
        .entries
        .iter()
        .filter(|entry| !entry.directory)
        .enumerate()
    {
        let existing = remote.get(entry.path.as_str()).copied();
        let hash = upload_entry(ctx, &client, record, artifact, entry, existing, published).await?;
        verified.insert(entry.path.clone(), (hash, entry.uncompressed_size));
        ctx.progress.set_current(index as u64 + 1);
    }
    finish(
        &client,
        record,
        destination.publish,
        published,
        metadata,
        &verified,
    )
    .await
}

async fn upload_entry(
    ctx: &JobContext,
    client: &InvenioClient<'_>,
    record: &InvenioRecord,
    artifact: &ArtifactRef,
    entry: &ArchiveEntry,
    existing: Option<&Value>,
    published: bool,
) -> Result<Hasher, TransferError> {
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
            return Ok(expected);
        }
        if published {
            return Err(invalid("published record contains an incomplete file"));
        }
        let file = client.json(Method::POST, commit_url, None).await?;
        verify_file(&file, &expected, size, true)?;
        return Ok(expected);
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
    verify_file(&file, &expected, size, true)?;
    Ok(expected)
}

async fn finish(
    client: &InvenioClient<'_>,
    record: &InvenioRecord,
    publish: bool,
    published: bool,
    metadata: [u8; 32],
    files: &std::collections::BTreeMap<String, (Hasher, u64)>,
) -> Result<InvenioRecord, TransferError> {
    let current_url = if published {
        client.url(&["records", &record.id])?
    } else {
        client.url(&["records", &record.id, "draft"])?
    };
    let mut current = client.json(Method::GET, current_url, None).await?;
    if metadata_digest(&current) != metadata {
        return Err(invalid("repository metadata changed before publication"));
    }
    if current["parent"]["id"] != record.parent_id {
        return Err(invalid("repository parent changed"));
    }
    verify_files(client, record, published, files).await?;
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
        if metadata_digest(&result) != metadata {
            return Err(invalid("published metadata changed"));
        }
        if result["parent"]["id"] != record.parent_id {
            return Err(invalid("published parent changed"));
        }
        verify_files(client, record, true, files).await?;
        current = result;
    }
    Ok(InvenioRecord {
        id: record.id.clone(),
        url: if publish {
            client.url(&["records", &record.id])?.to_string()
        } else {
            record.url.clone()
        },
        published: publish,
        parent_id: record.parent_id.clone(),
        revision_id: current["revision_id"]
            .as_u64()
            .ok_or_else(|| invalid("missing record revision"))?,
        doi: current["pids"]["doi"]["identifier"]
            .as_str()
            .map(str::to_string),
    })
}

async fn verify_files(
    client: &InvenioClient<'_>,
    record: &InvenioRecord,
    published: bool,
    expected: &std::collections::BTreeMap<String, (Hasher, u64)>,
) -> Result<(), TransferError> {
    let url = if published {
        client.url(&["records", &record.id, "files"])?
    } else {
        client.url(&["records", &record.id, "draft", "files"])?
    };
    let files = client.json(Method::GET, url, None).await?;
    let entries = files["entries"]
        .as_array()
        .ok_or_else(|| invalid("missing repository files"))?;
    if entries.len() != expected.len()
        || files["links"]["next"]
            .as_str()
            .is_some_and(|link| !link.is_empty())
    {
        return Err(invalid("repository file set changed"));
    }
    let mut keys = std::collections::HashSet::new();
    for file in entries {
        let key = file["key"]
            .as_str()
            .ok_or_else(|| invalid("missing file key"))?;
        if !keys.insert(key) {
            return Err(invalid("duplicate repository file"));
        }
        let (hash, size) = expected
            .get(key)
            .ok_or_else(|| invalid("unexpected repository file"))?;
        verify_file(file, hash, *size, true)?;
    }
    Ok(())
}

fn metadata_digest(record: &Value) -> [u8; 32] {
    let fields = json!({"metadata": record["metadata"], "custom_fields": record.get("custom_fields").cloned().unwrap_or_else(|| json!({}))});
    *blake3::hash(fields.to_string().as_bytes()).as_bytes()
}

fn verify_metadata(expected: &Value, record: &Value) -> Result<(), TransferError> {
    if !matches_fields(&expected["metadata"], &record["metadata"])
        || (expected.get("custom_fields").is_some()
            && !complete_fields(&expected["custom_fields"], &record["custom_fields"]))
    {
        return Err(invalid(
            "repository metadata differs from the requested crate",
        ));
    }
    Ok(())
}

fn complete_fields(expected: &Value, actual: &Value) -> bool {
    if actual.is_null() && expected.as_object().is_some_and(serde_json::Map::is_empty) {
        return true;
    }
    matches_fields(expected, actual)
        && actual.as_object().is_some_and(|fields| {
            fields.iter().all(|(key, value)| {
                expected.get(key).is_some()
                    || value.is_null()
                    || value.as_array().is_some_and(Vec::is_empty)
            })
        })
}

fn matches_fields(expected: &Value, actual: &Value) -> bool {
    if actual.is_null() && expected.as_array().is_some_and(Vec::is_empty) {
        return true;
    }
    match expected {
        Value::Object(fields) => {
            actual.is_object()
                && fields
                    .iter()
                    .all(|(key, value)| matches_fields(value, &actual[key]))
        }
        Value::Array(values) => actual.as_array().is_some_and(|actual| {
            values.len() == actual.len()
                && values
                    .iter()
                    .zip(actual)
                    .all(|(expected, actual)| matches_fields(expected, actual))
        }),
        _ => expected == actual,
    }
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
