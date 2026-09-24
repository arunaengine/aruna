//! Builds an import archive from every accessible published version and checked file.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

use aruna_blob::hash::Hasher;
use aruna_blob::invenio::InvenioClient;
use aruna_core::effects::BlobEffect;
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::invenio::{
    InvenioLink, InvenioMode, InvenioOptions, InvenioPull, InvenioRecord, LinkDirection,
    LinkFailure, LinkPull, LinkRemote, LinkStatus, PULL_CHECK_MS, PushOutcome, crate_versions,
    file_path, import_crate, pull_crate, record_id, record_identifiers, validate_id,
};
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::job::{
    ArtifactRef, ImportRoCrateSource, ImportRoCrateSpec, RoCrateLimits,
};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::secondary_id::{IdentifierOrigin, SecondaryIdentifier};
use aruna_core::time::unix_timestamp_millis;
use async_zip::{Compression, ZipEntryBuilder};
use futures_util::io::AsyncWriteExt;
use http::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use ulid::Ulid;

use super::links::{LinkChange, LinkError, change_link, read_link};
use super::{TransferError, connect, interruptible};
use crate::blob::hidden::delete_hidden;
use crate::jobs::executor::JobContext;

/// What an import that keeps or updates a pull link carries to its cleanup.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PullProgress {
    /// The version the dataset holds after the import.
    pub(super) record: InvenioRecord,
    pub(super) endpoint: String,
    /// The dataset revision an update merged into.
    base: Option<Ulid>,
    /// The dataset revision the import wrote.
    pub(crate) revision: Option<Ulid>,
}

impl PullProgress {
    pub(crate) fn base(&self) -> Option<Ulid> {
        self.base
    }
}

pub(crate) async fn acquire(
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
    group_id: Ulid,
    connector_id: Ulid,
    selected: &str,
    options: &InvenioOptions,
    pull: Option<&InvenioPull>,
) -> Result<(ArtifactRef, Vec<SecondaryIdentifier>, Option<PullProgress>), TransferError> {
    validate_id(selected)?;
    if let Some(InvenioPull::Update { link_id }) = pull {
        running(ctx, spec, *link_id).await?;
    }
    let client = connect(
        &ctx.driver,
        &spec.auth_context,
        group_id,
        connector_id,
        Permission::READ,
        spec.limits.metadata_bytes,
        None,
    )
    .await?;
    let (selected, records) =
        interruptible(ctx, history(&client, selected, &spec.limits, options)).await?;
    let latest = records
        .iter()
        .find(|(record, _)| record["id"] == selected.as_str())
        .map(|(record, _)| record.clone())
        .ok_or_else(|| invalid("requested record absent from history"))?;
    let (document, records, base) = match pull {
        Some(InvenioPull::Update { .. }) => {
            let (jsonld, base) = crate::jobs::export::crate_jsonld(
                &ctx.driver,
                &spec.auth_context,
                spec.document_id,
                spec.limits.metadata_bytes,
            )
            .await?;
            let current: Value =
                serde_json::from_str(&jsonld).map_err(|_| invalid("invalid dataset crate"))?;
            let known = crate_versions(&current);
            let added = records
                .into_iter()
                .filter(|(record, _)| {
                    record_id(record).is_ok_and(|id| !known.iter().any(|k| k == id))
                })
                .collect::<Vec<_>>();
            (
                pull_crate(&current, client.endpoint(), &latest, &added)?,
                added,
                Some(base),
            )
        }
        _ => (
            import_crate(client.endpoint(), &selected, &records)?,
            records,
            None,
        ),
    };
    let progress = match pull {
        Some(_) => Some(PullProgress {
            record: super::export::record_from(&client, &latest)?,
            endpoint: client.endpoint().to_string(),
            base,
            revision: None,
        }),
        None => None,
    };
    let mut identifiers = Vec::new();
    for record in records.iter().map(|(record, _)| record).chain([&latest]) {
        for id in record_identifiers(client.endpoint(), record, IdentifierOrigin::Imported) {
            if !identifiers.contains(&id) {
                identifiers.push(id);
            }
        }
    }
    let metadata = document.to_string();
    if metadata.len() as u64 > spec.limits.metadata_bytes {
        return Err(TransferError::Permanent(
            "generated crate exceeds metadata limit".into(),
        ));
    }
    let (writer, reader) = tokio::io::duplex(128 * 1024);
    let blob = ctx
        .driver
        .blob_handle
        .as_ref()
        .ok_or_else(|| TransferError::Retryable("blob handle unavailable".into()))?;
    let write = interruptible(
        ctx,
        write_archive(
            &client,
            writer,
            &metadata,
            &records,
            &spec.limits,
            options.mode,
        ),
    );
    let spool = blob.send_blob_effect(BlobEffect::SpoolHidden {
        namespace: ctx.job_id.as_ulid(),
        name: "input".into(),
        created_by: spec.auth_context.user_id,
        max_bytes: Some(spec.limits.import_source_bytes),
        deadline: None,
        blob: BackendStream::new(tokio_util::io::ReaderStream::new(reader)),
    });
    let (written, event) = tokio::join!(write, spool);
    match event {
        Event::Blob(BlobEvent::HiddenSpooled {
            location,
            size,
            blake3,
        }) => {
            if let Err(error) = written {
                let _ = delete_hidden(&ctx.driver, &location).await;
                return Err(error);
            }
            Ok((
                ArtifactRef {
                    location,
                    size,
                    blake3,
                    expires_at_ms: 0,
                },
                identifiers,
                progress,
            ))
        }
        Event::Blob(BlobEvent::Error(BlobError::SizeLimitExceeded { .. })) => {
            Err(invalid("repository archive exceeds import source limit"))
        }
        _ => {
            written?;
            Err(TransferError::Retryable(
                "repository archive spool failed".into(),
            ))
        }
    }
}

pub(super) async fn history(
    client: &InvenioClient<'_>,
    selected: &str,
    limits: &RoCrateLimits,
    options: &InvenioOptions,
) -> Result<(String, Vec<(Value, Value)>), TransferError> {
    let seed = client
        .json(Method::GET, client.url(&["records", selected])?, None)
        .await?;
    let selected = record_id(&seed)?.to_string();
    if seed["is_published"] != true {
        return Err(invalid("record is not published"));
    }
    let parent = seed["parent"]["id"]
        .as_str()
        .ok_or_else(|| invalid("missing parent identity"))?
        .to_string();
    let mut next = Some(client.url(&["records", &selected, "versions"])?);
    if let Some(url) = &mut next {
        url.query_pairs_mut()
            .append_pair("allversions", "true")
            .append_pair("size", "25");
    }
    let mut single = if options.all_versions {
        None
    } else {
        next = None;
        Some(json!({"hits": {"total": 1, "hits": [{"id": selected}]}}))
    };
    let mut pages = HashSet::new();
    let mut ids = HashSet::new();
    let mut records = Vec::new();
    let mut total = None;
    let mut bytes = 0u64;
    let mut entries = 1u64;
    while single.is_some() || next.is_some() {
        let page = if let Some(page) = single.take() {
            page
        } else {
            let url = next.take().ok_or_else(|| invalid("missing version page"))?;
            if pages.len() >= 10_000 || !pages.insert(url.as_str().to_string()) {
                return Err(invalid("version pagination loop or limit"));
            }
            client.json(Method::GET, url, None).await?
        };
        let count = page["hits"]["total"]
            .as_u64()
            .or_else(|| page["hits"]["total"]["value"].as_u64())
            .ok_or_else(|| invalid("missing version total"))?;
        if count > limits.max_entries
            || total.is_some_and(|total| total != count)
            || page["hits"]["total"]["relation"]
                .as_str()
                .is_some_and(|relation| relation != "eq")
        {
            return Err(invalid("version listing changed or exceeds limits"));
        }
        total = Some(count);
        let hits = page["hits"]["hits"]
            .as_array()
            .ok_or_else(|| invalid("missing version hits"))?;
        for hit in hits {
            let id = record_id(hit)?;
            if !ids.insert(id.to_string()) || ids.len() as u64 > limits.max_entries {
                return Err(invalid("duplicate version or version limit"));
            }
            let record = client
                .json(Method::GET, client.url(&["records", id])?, None)
                .await?;
            if record_id(&record)? != id
                || record["parent"]["id"] != parent
                || record["is_published"] != true
            {
                return Err(invalid("version identity or publication state changed"));
            }
            let files = if options.mode == InvenioMode::Metadata {
                json!({"entries": [], "listing_requested": false})
            } else {
                client
                    .json(Method::GET, client.url(&["records", id, "files"])?, None)
                    .await?
            };
            if files["links"]["next"]
                .as_str()
                .is_some_and(|link| !link.is_empty())
            {
                return Err(invalid("paginated file listing is not complete"));
            }
            let list = files["entries"]
                .as_array()
                .ok_or_else(|| invalid("missing file entries"))?;
            entries = entries
                .checked_add(list.len() as u64 + 1)
                .ok_or_else(|| invalid("entry overflow"))?;
            bytes = bytes
                .checked_add(record.to_string().len() as u64 + files.to_string().len() as u64)
                .ok_or_else(|| invalid("metadata overflow"))?;
            if entries > limits.max_entries || bytes > limits.metadata_bytes {
                return Err(invalid("repository history exceeds crate limits"));
            }
            records.push((record, files));
        }
        next = page["links"]["next"]
            .as_str()
            .filter(|link| !link.is_empty())
            .map(|link| client.link(link))
            .transpose()?;
    }
    if Some(records.len() as u64) != total || !ids.contains(&selected) {
        return Err(invalid("incomplete repository version history"));
    }
    records.sort_by_key(|(record, _)| record["versions"]["index"].as_u64().unwrap_or(0));
    Ok((selected, records))
}

async fn write_archive(
    client: &InvenioClient<'_>,
    writer: tokio::io::DuplexStream,
    metadata: &str,
    records: &[(Value, Value)],
    limits: &RoCrateLimits,
    mode: InvenioMode,
) -> Result<(), TransferError> {
    let mut archive = async_zip::base::write::ZipFileWriter::with_tokio(writer);
    archive
        .write_entry_whole(entry("ro-crate-metadata.json"), metadata.as_bytes())
        .await?;
    let mut size = metadata.len() as u64;
    let mut paths = HashSet::new();
    for (record, files) in records {
        let id = record_id(record)?;
        let provenance = json!({"record": record, "files": files}).to_string();
        size = checked_size(size, provenance.len() as u64, limits.expanded_import_bytes)?;
        archive
            .write_entry_whole(
                entry(&format!("versions/{id}/invenio-record.json")),
                provenance.as_bytes(),
            )
            .await?;
        for file in files["entries"]
            .as_array()
            .ok_or_else(|| invalid("missing files"))?
        {
            let key = file["key"]
                .as_str()
                .ok_or_else(|| invalid("missing file key"))?;
            let path = file_path(id, key)?;
            if path.len() as u64 > limits.key_bytes || !paths.insert(path.clone()) {
                return Err(invalid("duplicate or oversized file path"));
            }
            if mode == InvenioMode::Reference {
                let descriptor = json!({"record_id": id, "file": file}).to_string();
                size = checked_size(size, descriptor.len() as u64, limits.expanded_import_bytes)?;
                archive
                    .write_entry_whole(entry(&path), descriptor.as_bytes())
                    .await?;
                continue;
            }
            let expected = file["size"]
                .as_u64()
                .ok_or_else(|| invalid("missing file size"))?;
            size = checked_size(size, expected, limits.expanded_import_bytes)?;
            let checksum = file["checksum"]
                .as_str()
                .ok_or_else(|| invalid("missing file checksum"))?;
            let (algorithm, digest) = checksum
                .split_once(':')
                .ok_or_else(|| invalid("invalid checksum"))?;
            if !matches!(algorithm, "md5" | "sha1" | "sha256" | "blake3") {
                return Err(invalid("unsupported repository checksum"));
            }
            let mut response = client
                .download(client.url(&["records", id, "files", key, "content"])?)
                .await?;
            let mut target = archive.write_entry_stream(entry(&path)).await?;
            let mut actual = 0u64;
            let mut hasher = Hasher::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| TransferError::Retryable("repository file read failed".into()))?
            {
                actual = checked_size(actual, chunk.len() as u64, expected)?;
                hasher.update(&chunk);
                target.write_all(&chunk).await?;
            }
            let hashes = hasher.to_map();
            if actual != expected
                || hashes.get(algorithm).map(hex::encode).as_deref() != Some(digest)
            {
                return Err(invalid("repository file size or checksum mismatch"));
            }
            target.close().await?;
        }
    }
    archive.close().await?;
    Ok(())
}

fn entry(path: &str) -> ZipEntryBuilder {
    ZipEntryBuilder::new(path.into(), Compression::Stored)
}

fn checked_size(size: u64, added: u64, limit: u64) -> Result<u64, TransferError> {
    size.checked_add(added)
        .filter(|size| *size <= limit)
        .ok_or_else(|| invalid("repository data exceeds size limit"))
}

fn invalid(message: &str) -> TransferError {
    TransferError::Permanent(message.into())
}

/// Stops an update once its link is gone, paused or runs another job.
async fn running(
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
    link_id: Ulid,
) -> Result<(), TransferError> {
    let link = read_link(&ctx.driver.storage_handle, spec.document_id, link_id)
        .await
        .map_err(|error| TransferError::Retryable(error.to_string()))?
        .filter(|link| link.status != LinkStatus::Paused && link.pull().is_some())
        .ok_or(TransferError::Cancelled)?;
    match link.active_job {
        Some(job_id) if job_id == ctx.job_id => Ok(()),
        None => Err(TransferError::Retryable(
            "the link has not started this pull".into(),
        )),
        Some(_) => Err(TransferError::Permanent(
            "a newer pull of this link replaced this job".into(),
        )),
    }
}

/// Records the end of an import on its pull link: a new link after an import that keeps one,
/// the held version after an update, or the failed or cancelled update.
pub(crate) async fn settle_import(
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
    progress: Option<&PullProgress>,
    failure: Option<&str>,
) -> Result<(), LinkError> {
    let ImportRoCrateSource::Invenio {
        group_id,
        connector_id,
        options,
        pull: Some(pull),
        ..
    } = &spec.source
    else {
        return Ok(());
    };
    let done = progress.and_then(|progress| Some((progress, progress.revision?)));
    let now = std::time::SystemTime::now();
    match pull {
        InvenioPull::Keep {
            auto_update,
            owner_node_url,
        } => {
            let Some((progress, revision)) = done else {
                return Ok(());
            };
            let mut link = InvenioLink {
                link_id: ctx.job_id.as_ulid(),
                document_id: spec.document_id,
                group_id: *group_id,
                connector_id: *connector_id,
                endpoint: progress.endpoint.clone(),
                owner_node: ctx.owner_node_id,
                owner_node_url: owner_node_url.clone(),
                created_by: spec.auth_context.user_id,
                status: LinkStatus::Enabled,
                auto_publish: false,
                public_files: false,
                metadata_json: "{}".into(),
                remote: LinkRemote::default(),
                last_push: None,
                active_job: None,
                sequence: 0,
                limits: spec.limits.clone(),
                created_at: now,
                updated_at: now,
                generation: 0,
                warning: None,
                direction: LinkDirection::Pull(Box::new(LinkPull {
                    auto_update: *auto_update,
                    options: options.clone(),
                    target: spec.target.clone(),
                    latest_remote_id: None,
                    latest_revision: None,
                    last_checked_at: None,
                    next_check_ms: unix_timestamp_millis().saturating_add(PULL_CHECK_MS),
                    failures: 0,
                    revision: None,
                    local_changed: false,
                })),
            };
            link.hold(&progress.record, revision, now);
            let change = LinkChange::Create {
                link: Box::new(link.clone()),
                secret: None,
            };
            match change_link(&ctx.driver, &link, change).await {
                Ok(_) | Err(LinkError::Exists) => Ok(()),
                Err(error) => Err(error),
            }
        }
        InvenioPull::Update { link_id } => {
            let storage = &ctx.driver.storage_handle;
            let Some(link) = read_link(storage, spec.document_id, *link_id)
                .await?
                .filter(|link| link.active_job == Some(ctx.job_id))
            else {
                return Ok(());
            };
            let change = match (done, failure) {
                (Some((progress, revision)), _) => LinkChange::Pulled {
                    job_id: ctx.job_id,
                    record: Box::new(progress.record.clone()),
                    revision,
                },
                (None, failure) => LinkChange::Finish {
                    job_id: ctx.job_id,
                    outcome: Box::new(match failure {
                        Some(message) => PushOutcome::Failed(LinkFailure::Other(message.into())),
                        None => PushOutcome::Cancelled,
                    }),
                    requeue: false,
                },
            };
            match change_link(&ctx.driver, &link, change).await {
                Ok(_) | Err(LinkError::NotFound) => Ok(()),
                Err(error) => Err(error),
            }
        }
    }
}
