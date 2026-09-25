//! Publishes mapped metadata and individual crate files as a native repository record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Arc, Mutex};

use aruna_blob::hash::Hasher;
use aruna_blob::invenio::{InvenioClient, InvenioError};
use aruna_core::invenio::{
    ExportIdentity, InvenioDestination, InvenioRecord, LinkFailure, MAX_RECORD_FILES,
    export_fields, record_id, validate_id,
};
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::job::{ArtifactRef, ExportRoCrateSpec};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::secondary_id::{IdentifierOrigin, RegisterIdentifiersSpec};
use futures_util::StreamExt;
use http::Method;
use serde_json::{Value, json};

use super::push::{guard, record_draft};
use super::verify::{
    complete_fields, complete_metadata, metadata_digest, verify_file, verify_files, verify_metadata,
};
use super::{TransferError, connect, interruptible};
use crate::harvest::create_connector::INVENIO_COMMUNITY;
use crate::jobs::executor::JobContext;
use crate::jobs::export::{ExportCheckpoint, persist_checkpoint};
use crate::jobs::import::archive::{
    ArchiveCompression, ArchiveEntry, ArchiveInspection, inspect_reader,
};
use crate::jobs::service::read_artifact_range;

pub(crate) async fn repository_export(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &InvenioDestination,
    checkpoint: &mut ExportCheckpoint,
) -> Result<(), TransferError> {
    let prepared;
    let destination = match &destination.link {
        Some(target) => {
            prepared = super::push::prepare(ctx, spec, destination, target, checkpoint).await?;
            &prepared
        }
        None => destination,
    };
    if checkpoint.repository.is_none() {
        if checkpoint.repository_started && destination.draft_id.is_none() {
            let recovery = if destination.link.is_some() {
                "inspect the repository drafts and link the dataset again"
            } else {
                "inspect the repository and retry with its draft_id"
            };
            return Err(TransferError::Permanent(format!(
                "draft creation outcome is unknown; {recovery}"
            )));
        }
        let artifact = checkpoint
            .artifact
            .as_ref()
            .ok_or_else(|| TransferError::Permanent("export artifact missing".into()))?;
        let inspection = inspect_artifact(ctx, spec, artifact).await?;
        if inspection.entries.iter().filter(|e| !e.directory).count() > MAX_RECORD_FILES {
            return Err(TransferError::Refused(LinkFailure::TooManyFiles));
        }
        let jsonld = checkpoint
            .raw_jsonld
            .clone()
            .ok_or_else(|| TransferError::Permanent("source crate metadata missing".into()))?;
        let identity = checkpoint.identity.clone();
        let fence = async || {
            checkpoint.repository_started = true;
            persist_checkpoint(ctx, checkpoint)
                .await
                .map_err(TransferError::Retryable)
        };
        // Not interruptible: a created draft must reach the checkpoint and the link.
        let record = create_draft(ctx, spec, destination, &jsonld, &identity, fence).await?;
        checkpoint.repository = Some(record.clone());
        persist_checkpoint(ctx, checkpoint)
            .await
            .map_err(TransferError::Retryable)?;
        if let Some(target) = &destination.link {
            record_draft(ctx, spec, target, &record).await?;
        }
    }
    if checkpoint.repository_metadata.is_none()
        && let Some(record) = checkpoint.repository.as_ref().filter(|r| r.doi.is_none())
    {
        let reserved = interruptible(ctx, reserve_doi(ctx, spec, destination, record)).await?;
        if reserved != *record {
            checkpoint.repository = Some(reserved.clone());
            persist_checkpoint(ctx, checkpoint)
                .await
                .map_err(TransferError::Retryable)?;
            if let Some(target) = &destination.link {
                record_draft(ctx, spec, target, &reserved).await?;
            }
        }
    }
    if checkpoint.repository_metadata.is_none() {
        let record = checkpoint
            .repository
            .as_ref()
            .ok_or_else(|| TransferError::Permanent("repository draft missing".into()))?;
        let jsonld = checkpoint
            .raw_jsonld
            .as_deref()
            .ok_or_else(|| TransferError::Permanent("source crate metadata missing".into()))?;
        let identity = &checkpoint.identity;
        let (record, digest) = interruptible(
            ctx,
            prepare_draft(ctx, spec, destination, record, jsonld, identity),
        )
        .await?;
        checkpoint.repository = Some(record.clone());
        checkpoint.repository_metadata = Some(digest);
        persist_checkpoint(ctx, checkpoint)
            .await
            .map_err(TransferError::Retryable)?;
        if let Some(target) = &destination.link {
            record_draft(ctx, spec, target, &record).await?;
        }
    }
    let record = checkpoint
        .repository
        .as_ref()
        .ok_or_else(|| TransferError::Permanent("repository draft missing".into()))?;
    let artifact = checkpoint
        .artifact
        .as_ref()
        .ok_or_else(|| TransferError::Permanent("export artifact missing".into()))?;
    let (record, files) = interruptible(
        ctx,
        deposit(
            ctx,
            spec,
            destination,
            record,
            artifact,
            checkpoint.repository_metadata.ok_or_else(|| {
                TransferError::Permanent("repository metadata checkpoint missing".into())
            })?,
        ),
    )
    .await?;
    checkpoint.repository = Some(record);
    checkpoint.repository_files = files;
    checkpoint.repository_complete = true;
    persist_checkpoint(ctx, checkpoint)
        .await
        .map_err(TransferError::Retryable)
}

/// Queues the record's DOIs and ids as `Published` identifiers of the exported dataset.
/// The dedup key names this job, so a rerun of the publish phase joins the queued job.
pub(crate) async fn register_published(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &InvenioDestination,
    record: &InvenioRecord,
) -> Result<(), TransferError> {
    let view =
        super::repository(&ctx.driver, destination.group_id, destination.connector_id).await?;
    let identifiers = record.identifiers(&view.connector.endpoint, IdentifierOrigin::Published);
    if identifiers.is_empty() {
        return Ok(());
    }
    crate::jobs::service::submit_identifiers(
        &ctx.driver,
        RegisterIdentifiersSpec {
            document_id: spec.document_id,
            identifiers,
            auth_context: spec.auth_context.clone(),
        },
        ctx.owner_node_id,
        ctx.job_id,
    )
    .await
    .map(|_| ())
    .map_err(|error| TransferError::Retryable(format!("queueing identifiers failed: {error}")))
}

pub(crate) async fn create_draft(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &InvenioDestination,
    jsonld: &str,
    identity: &ExportIdentity,
    fence: impl AsyncFnOnce() -> Result<(), TransferError>,
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
    let mut fields = export_fields(&document, &overrides, identity, client.endpoint())?;
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
        guard(ctx, spec, destination).await?;
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
        guard(ctx, spec, destination).await?;
        fence().await?;
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
    record_from(&client, &record)
}

/// Reserves the draft's DOI in its own step, after the draft is stored, so a failed
/// reservation retries without losing the draft.
async fn reserve_doi(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &InvenioDestination,
    record: &InvenioRecord,
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
    guard(ctx, spec, destination).await?;
    let url = client.url(&["records", &record.id, "draft", "pids", "doi"])?;
    match client.json(Method::POST, url, None).await {
        Ok(reserved)
            if record_id(&reserved)? == record.id
                && reserved["parent"]["id"] == record.parent_id.as_str() =>
        {
            record_from(&client, &reserved)
        }
        Ok(_) => Err(invalid("draft identity changed while reserving its DOI")),
        Err(error @ (InvenioError::Transport | InvenioError::Status(401 | 403 | 429))) => {
            Err(error.into())
        }
        // A repository without a DOI provider still publishes; the next push tries again.
        Err(error) => {
            tracing::warn!(%error, "Reserving the draft DOI failed");
            Ok(record.clone())
        }
    }
}

/// The mandatory fields the dataset's mapped crate lacks with these overrides for the
/// connector's repository.
pub async fn missing_metadata(
    context: &std::sync::Arc<crate::driver::DriverContext>,
    auth: &aruna_core::structs::identity::auth::AuthContext,
    document_id: ulid::Ulid,
    group_id: ulid::Ulid,
    connector_id: ulid::Ulid,
    metadata_json: &str,
    metadata_bytes: u64,
) -> Result<Vec<&'static str>, TransferError> {
    let view = super::repository(context, group_id, connector_id).await?;
    let (jsonld, _) =
        crate::jobs::export::crate_jsonld(context, auth, document_id, metadata_bytes).await?;
    let document: Value =
        serde_json::from_str(&jsonld).map_err(|_| invalid("invalid source crate"))?;
    let overrides: Value =
        serde_json::from_str(metadata_json).map_err(|_| invalid("invalid repository metadata"))?;
    Ok(aruna_core::invenio::missing_metadata(
        &document,
        &overrides,
        &view.connector.endpoint,
    )?)
}

/// The repository's record as the link and job see it; drafts point at the draft endpoint.
pub(super) fn record_from(
    client: &InvenioClient<'_>,
    record: &Value,
) -> Result<InvenioRecord, TransferError> {
    let id = record_id(record)?.to_string();
    let published = record["is_published"] == true;
    let url = if published {
        client.url(&["records", &id])?
    } else {
        client.url(&["records", &id, "draft"])?
    };
    Ok(InvenioRecord {
        url: url.to_string(),
        published,
        parent_id: record["parent"]["id"]
            .as_str()
            .ok_or_else(|| invalid("missing parent identity"))?
            .to_string(),
        revision_id: record["revision_id"]
            .as_u64()
            .ok_or_else(|| invalid("missing record revision"))?,
        doi: record["pids"]["doi"]["identifier"]
            .as_str()
            .map(str::to_string),
        html_url: page_url(client, record),
        concept_doi: record["parent"]["pids"]["doi"]["identifier"]
            .as_str()
            .map(str::to_string),
        in_review: false,
        warning: None,
        id,
    })
}

/// The file keys of a draft or published record.
pub(super) async fn file_keys(
    client: &InvenioClient<'_>,
    id: &str,
    published: bool,
) -> Result<Vec<String>, TransferError> {
    let url = if published {
        client.url(&["records", id, "files"])?
    } else {
        client.url(&["records", id, "draft", "files"])?
    };
    let files = client.json(Method::GET, url, None).await?;
    Ok(files["entries"]
        .as_array()
        .ok_or_else(|| invalid("missing repository files"))?
        .iter()
        .filter_map(|file| file["key"].as_str().map(str::to_string))
        .collect())
}

async fn inspect_artifact(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    artifact: &ArtifactRef,
) -> Result<ArchiveInspection, TransferError> {
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
    Ok(inspection)
}

pub(crate) async fn prepare_draft(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &InvenioDestination,
    record: &InvenioRecord,
    jsonld: &str,
    identity: &ExportIdentity,
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
    let mut fields = export_fields(&document, &overrides, identity, client.endpoint())?;
    fields["files"] = json!({"enabled": true});
    let url = client.url(&["records", &record.id, "draft"])?;
    let current = client.json(Method::GET, url.clone(), None).await?;
    if record_id(&current)? != record.id
        || current["parent"]["id"] != record.parent_id
        || current["is_published"] != false
    {
        return Err(invalid("repository draft identity changed"));
    }
    // Keeping pids holds the reserved DOI; file access follows the current setting.
    fields["pids"] = current["pids"].clone();
    let files = if destination.public_files {
        "public"
    } else {
        "restricted"
    };
    let mut access = json!({"record": current["access"]["record"].as_str().unwrap_or("public"),
        "files": files});
    if current["access"]["embargo"].is_object() {
        access["embargo"] = current["access"]["embargo"].clone();
    }
    fields["access"] = access;
    let draft = if destination.draft_id.is_some() || destination.new_version.is_some() {
        if current["revision_id"].as_u64() == Some(record.revision_id) {
            guard(ctx, spec, destination).await?;
            client.update(url, &fields, record.revision_id).await?
        } else if current["revision_id"]
            .as_u64()
            .is_some_and(|revision| revision > record.revision_id)
            && complete_metadata(&fields["metadata"], &current["metadata"])
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
) -> Result<(InvenioRecord, Vec<String>), TransferError> {
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
    let checked = Box::pin(async {
        if metadata_digest(&draft) != metadata {
            return Err(invalid("repository metadata changed after preparation"));
        }
        let inspection = inspect_artifact(ctx, spec, artifact).await?;
        let files_url = if published {
            client.url(&["records", &record.id, "files"])?
        } else {
            client.url(&["records", &record.id, "draft", "files"])?
        };
        let files = client.json(Method::GET, files_url, None).await?;
        let entries = files["entries"]
            .as_array()
            .ok_or_else(|| invalid("missing draft files"))?;
        let paths = inspection
            .entries
            .iter()
            .filter(|entry| !entry.directory)
            .map(|entry| entry.path.as_str())
            .collect::<std::collections::HashSet<_>>();
        // A link keeps one draft in step with the dataset, so files the dataset dropped go.
        let replace = destination.link.as_ref().filter(|_| !published);
        let mut remote = std::collections::BTreeMap::new();
        for file in entries {
            let key = file["key"]
                .as_str()
                .ok_or_else(|| invalid("missing repository file key"))?;
            if let Some(target) = replace
                && !paths.contains(key)
            {
                // A file this link never pushed was added in the repository.
                if !target.files.iter().any(|pushed| pushed == key) {
                    return Err(TransferError::Refused(LinkFailure::RemoteChanged));
                }
                guard(ctx, spec, destination).await?;
                let url = client.url(&["records", &record.id, "draft", "files", key])?;
                client.delete(url).await?;
                continue;
            }
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
            if !published {
                guard(ctx, spec, destination).await?;
            }
            let existing = remote.get(entry.path.as_str()).copied();
            let hash = upload_entry(
                ctx,
                &client,
                record,
                artifact,
                entry,
                existing,
                published,
                replace.is_some(),
            )
            .await?;
            verified.insert(entry.path.clone(), (hash, entry.uncompressed_size));
            ctx.progress.set_current(index as u64 + 1);
        }
        let done = Box::pin(finish(
            ctx,
            spec,
            destination,
            &client,
            record,
            published,
            metadata,
            &verified,
        ));
        let keys = verified.keys().cloned().collect();
        Ok((done.await?, keys))
    });
    match checked.await {
        // The record was published by an earlier attempt, so a failed check only warns.
        Err(TransferError::Permanent(message)) if published => {
            let mut result = record_from(&client, &draft)?;
            result.warning = Some(message);
            let keys = file_keys(&client, &record.id, true).await?;
            Ok((result, keys))
        }
        result => result,
    }
}

#[allow(clippy::too_many_arguments)]
async fn upload_entry(
    ctx: &JobContext,
    client: &InvenioClient<'_>,
    record: &InvenioRecord,
    artifact: &ArtifactRef,
    entry: &ArchiveEntry,
    mut existing: Option<&Value>,
    published: bool,
    replace: bool,
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
    if replace
        && existing.is_some_and(|file| {
            file["checksum"].is_string() && verify_file(file, &expected, size, false).is_err()
        })
    {
        let url = client.url(&["records", &record.id, "draft", "files", key])?;
        client.delete(url).await?;
        existing = None;
    }
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

#[allow(clippy::too_many_arguments)]
async fn finish(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &InvenioDestination,
    client: &InvenioClient<'_>,
    record: &InvenioRecord,
    published: bool,
    metadata: [u8; 32],
    files: &std::collections::BTreeMap<String, (Hasher, u64)>,
) -> Result<InvenioRecord, TransferError> {
    let current_url = if published {
        client.url(&["records", &record.id])?
    } else {
        client.url(&["records", &record.id, "draft"])?
    };
    let current = client.json(Method::GET, current_url, None).await?;
    if metadata_digest(&current) != metadata {
        return Err(invalid("repository metadata changed before publication"));
    }
    if current["parent"]["id"] != record.parent_id {
        return Err(invalid("repository parent changed"));
    }
    verify_files(client, record, published, files).await?;
    if !destination.publish || published {
        return record_from(client, &current);
    }
    guard(ctx, spec, destination).await?;
    if let Some(community) = review_community(ctx, destination, &current).await? {
        submit_review(client, record, &current, &community).await?;
        let draft_url = client.url(&["records", &record.id, "draft"])?;
        let mut result = record_from(client, &client.json(Method::GET, draft_url, None).await?)?;
        result.in_review = true;
        return Ok(result);
    }
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
    // Publishing cannot be undone, so later mismatches become a warning on the record.
    let mut published = record_from(client, &result)?;
    published.warning = if metadata_digest(&result) != metadata {
        Some("published metadata changed".into())
    } else if result["parent"]["id"] != record.parent_id {
        Some("published parent changed".into())
    } else {
        verify_files(client, record, true, files)
            .await
            .err()
            .map(|error| error.to_string())
    };
    Ok(published)
}

/// The connector's community when this draft is a record's first version, which needs review.
async fn review_community(
    ctx: &JobContext,
    destination: &InvenioDestination,
    draft: &Value,
) -> Result<Option<String>, TransferError> {
    if draft["versions"]["index"] != 1 {
        return Ok(None);
    }
    let view =
        super::repository(&ctx.driver, destination.group_id, destination.connector_id).await?;
    Ok(view
        .connector
        .public_config
        .get(INVENIO_COMMUNITY)
        .filter(|community| !community.is_empty())
        .cloned())
}

/// Submits the draft to the community; a submission still open stays as it is.
async fn submit_review(
    client: &InvenioClient<'_>,
    record: &InvenioRecord,
    draft: &Value,
    community: &str,
) -> Result<(), TransferError> {
    if draft["parent"]["review"]["status"] == "submitted" {
        return Ok(());
    }
    let found = client
        .json(Method::GET, client.url(&["communities", community])?, None)
        .await?;
    let id = found["id"]
        .as_str()
        .ok_or_else(|| invalid("community has no id"))?;
    let request = json!({"receiver": {"community": id}, "type": "community-submission"});
    client
        .json(
            Method::PUT,
            client.url(&["records", &record.id, "draft", "review"])?,
            Some(&request),
        )
        .await?;
    client
        .json(
            Method::POST,
            client.url(&["records", &record.id, "draft", "actions", "submit-review"])?,
            None,
        )
        .await?;
    Ok(())
}

/// The record's page for people, kept only on the repository's own origin.
fn page_url(client: &InvenioClient<'_>, record: &Value) -> Option<String> {
    let page = url::Url::parse(record["links"]["self_html"].as_str()?).ok()?;
    let endpoint = url::Url::parse(client.endpoint()).ok()?;
    (page.origin() == endpoint.origin() && page.username().is_empty() && page.password().is_none())
        .then(|| page.to_string())
}

pub(super) fn invalid(message: &str) -> TransferError {
    TransferError::Permanent(message.into())
}
