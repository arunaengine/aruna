//! Uploads a complete crate archive to an unpublished repository draft.
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
use crate::jobs::service::read_artifact_range;

pub(crate) async fn create_draft(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &InvenioDestination,
    jsonld: &str,
) -> Result<InvenioRecord, TransferError> {
    let client = connect(
        ctx,
        &spec.auth_context,
        destination.group_id,
        destination.connector_id,
        Permission::WRITE,
        spec.limits.metadata_bytes,
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
        client
            .json(Method::GET, client.url(&["records", id, "draft"])?, None)
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
    let client = connect(
        ctx,
        &spec.auth_context,
        destination.group_id,
        destination.connector_id,
        Permission::WRITE,
        spec.limits.metadata_bytes,
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
    let key = format!(
        "aruna-{}-{}.zip",
        spec.document_id,
        hex::encode(artifact.blake3)
    );
    let files_url = if published {
        client.url(&["records", &record.id, "files"])?
    } else {
        client.url(&["records", &record.id, "draft", "files"])?
    };
    let files = client.json(Method::GET, files_url.clone(), None).await?;
    let entries = files["entries"]
        .as_array()
        .ok_or_else(|| invalid("missing draft files"))?;
    let existing = entries.iter().find(|file| file["key"] == key);
    if let Some(file) = existing {
        if file["status"] == "completed" {
            let mut read = read_artifact_range(&ctx.driver, artifact, 0..artifact.size)
                .await
                .map_err(TransferError::Retryable)?;
            let mut hasher = Hasher::new();
            while let Some(chunk) = read.blob.next().await {
                hasher.update(
                    &chunk.map_err(|_| {
                        TransferError::Retryable("crate artifact read failed".into())
                    })?,
                );
            }
            verify_file(file, &hasher, artifact)?;
            return finish(&client, record, destination.publish, published).await;
        }
    } else if published {
        return Err(invalid("published record is missing the exported crate"));
    } else {
        client
            .json(Method::POST, files_url, Some(&json!([{"key": key}])))
            .await?;
    }
    let read = read_artifact_range(&ctx.driver, artifact, 0..artifact.size)
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
            client.url(&["records", &record.id, "draft", "files", &key, "content"])?,
            artifact.size,
            BackendStream::new(stream),
        )
        .await?;
    let file = client
        .json(
            Method::POST,
            client.url(&["records", &record.id, "draft", "files", &key, "commit"])?,
            None,
        )
        .await?;
    verify_file(
        &file,
        &hasher.lock().unwrap_or_else(|error| error.into_inner()),
        artifact,
    )?;
    finish(&client, record, destination.publish, false).await
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

fn verify_file(file: &Value, hasher: &Hasher, artifact: &ArtifactRef) -> Result<(), TransferError> {
    let checksum = file["checksum"]
        .as_str()
        .ok_or_else(|| invalid("missing uploaded checksum"))?;
    let (algorithm, digest) = checksum
        .split_once(':')
        .ok_or_else(|| invalid("invalid uploaded checksum"))?;
    let hashes = hasher.to_map();
    if file["status"] != "completed"
        || file["size"].as_u64() != Some(artifact.size)
        || hashes.get(algorithm).map(hex::encode).as_deref() != Some(digest)
        || hasher.finalize().blake3.as_bytes() != &artifact.blake3
    {
        return Err(invalid("uploaded crate checksum or size mismatch"));
    }
    Ok(())
}

fn invalid(message: &str) -> TransferError {
    TransferError::Permanent(message.into())
}
