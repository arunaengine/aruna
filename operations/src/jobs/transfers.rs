//! Moves bytes between a plugin connection and the receiver's buckets for plugin work: every
//! attempt checks the grants, the connection and the secret's rule again before a byte moves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use aruna_core::errors::ConversionError;
use aruna_core::injection::{OutgoingRequest, expose_response};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::secrets::{ResponseExposure, SecretFields};
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
use aruna_core::structs::execution::job::{JobError, JobResultPayload, PluginWorkSpec};
use aruna_core::structs::identity::auth::Permission as Access;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::CONTENT_TYPE_KEY;
use aruna_core::structs::storage::encryption::BucketKeyError;
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_core::time::unix_timestamp_millis;
use aruna_plugins::broker::{ErrorKind, Field, HostError, Response};
use aruna_plugins::permission::Permission;
use futures_util::StreamExt;
use ulid::Ulid;

use super::executor::{JobContext, JobRunOutcome};
use super::plugin_work::{PROGRESS_TICK, account, key_wait, permanent, refused};
use crate::driver::drive;
use crate::effect_adapters::routing::{gate_context, routing_snapshot};
use crate::plugins::broker::local_object;
use crate::plugins::broker::whole_text;
use crate::plugins::check::{authorize_access, authorize_call, matching};
use crate::plugins::grants::InstallScope;
use crate::plugins::install::{pick_version, read_manifest};
use crate::plugins::marks::unmarked;
use crate::plugins::outgoing::{outgoing, release_into};
use crate::plugins::release::ReleaseKeys;
use crate::plugins::work::{Send, WorkRow, begin_send, record_sent, update_work};
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation};
use crate::s3::object::put::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};

/// What a download writes: its target, the version minted once, expected digests and a bound.
pub struct Download<'a> {
    pub target: &'a str,
    pub version_id: Ulid,
    pub checksums: &'a [(ChecksumAlgorithm, Vec<u8>)],
    pub max_bytes: Option<u64>,
}

/// Downloads the answer of the row's request into a new version of the target, as the account
/// restricted to its current download patterns of the target.
pub async fn download_work(
    ctx: &JobContext,
    spec: &PluginWorkSpec,
    (install, config, row): (&PluginInstall, &RealmConfigDocument, &WorkRow),
    (download, keys): (Download<'_>, &ReleaseKeys),
) -> JobRunOutcome {
    let patterns = match download_grants(ctx, install, (row, download.target)).await {
        Ok(found) => found,
        Err(error) => return refused(error),
    };
    let place = (spec.realm_id, ctx.owner_node_id);
    let (bucket, key) = match local_object(&ctx.driver, place, install, download.target).await {
        Ok(found) => found,
        Err(error) => return refused(error),
    };
    let request = prepared(ctx, (install, config, row), keys).await;
    let (outgoing, exposure, _) = match request {
        Ok(prepared) => prepared,
        Err(error) => return refused(error),
    };
    // The whole answer ends up in a bucket, so only a rule that shows it whole allows it.
    if exposure != ResponseExposure::Full {
        return permanent("this connection does not expose its whole answer");
    }
    let Some(blob) = ctx.driver.blob_handle.as_ref() else {
        return retryable("this node sends no requests");
    };
    let builder = match blob.repository_request(reqwest::Method::GET, outgoing.url) {
        Ok(builder) => builder,
        Err(error) => return permanent(error.to_string()),
    };
    let builder = (outgoing.headers.iter()).fold(builder, |builder, (name, value)| {
        builder.header(name, value)
    });
    let response = tokio::select! {
        sent = builder.send() => sent,
        _ = ctx.cancel.cancelled() => return JobRunOutcome::Cancelled,
        _ = ctx.shutdown.cancelled() => return JobRunOutcome::Interrupted,
    };
    let response = match response {
        Ok(response) => response,
        Err(error) => return retryable(error.without_url().to_string()),
    };
    let status = response.status().as_u16();
    match status {
        200..=299 => {}
        408 | 429 | 500..=599 => return retryable(format!("the service answered {status}")),
        _ => return permanent(format!("the service answered {status}")),
    }
    let length = response.content_length();
    if let (Some(length), Some(bound)) = (length, download.max_bytes)
        && length > bound
    {
        return permanent(too_large(bound));
    }
    let content_type = (response.headers().get(reqwest::header::CONTENT_TYPE))
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let pulled = Arc::new(AtomicU64::new(0));
    let (over, broken) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let counted = counted(response, download.max_bytes, &pulled, (&over, &broken));
    let routing = match routing_snapshot(&ctx.driver, install.group_id, &bucket).await {
        Ok(routing) => routing,
        Err(error) => return retryable(error.to_string()),
    };
    let gate = match gate_context(&ctx.driver, spec.realm_id, unix_timestamp_millis()).await {
        Ok(gate) => gate,
        Err(error) => return retryable(error.to_string()),
    };
    let expected_checksums = (download.checksums.iter())
        .map(|(algorithm, digest)| ExpectedChecksum {
            algorithm: *algorithm,
            digest: digest.clone(),
        })
        .collect();
    let metadata: HashMap<String, String> = content_type
        .map(|kind| (CONTENT_TYPE_KEY.to_string(), kind))
        .into_iter()
        .collect();
    let auth = account(install, &patterns);
    let mut put = PutObjectOperation::new(PutObjectConfig {
        user_id: install.account,
        group_id: install.group_id,
        realm_id: spec.realm_id,
        node_id: ctx.owner_node_id,
        request: PutObjectInput {
            bucket,
            key,
            content_length: length,
            body: Some(counted),
        },
        expected_checksums,
        checksum_type: None,
        exists: false,
        version_source: None,
        // The version was minted once, so a retry after a lost answer replays it.
        preassigned_version_id: Some(download.version_id),
        quota_ceiling: config.quota.effective_group_ceiling(&install.group_id),
        routing,
    })
    .with_metadata(metadata)
    .with_restrictions(auth.path_restrictions)
    .with_version_origin(Some(VersionOrigin::Source {
        job_id: Some(ctx.job_id.as_ulid()),
    }));
    if let Some(gate) = gate {
        put = put.with_gate(gate);
    }
    let written = drive(put, &ctx.driver);
    tokio::pin!(written);
    let mut tick = tokio::time::interval(PROGRESS_TICK);
    let result = loop {
        tokio::select! {
            result = &mut written => break result,
            _ = tick.tick() => ctx.progress.set_current(pulled.load(Ordering::Relaxed)),
            _ = ctx.cancel.cancelled() => return JobRunOutcome::Cancelled,
            _ = ctx.shutdown.cancelled() => return JobRunOutcome::Interrupted,
        }
    };
    match result {
        Ok(put) => {
            ctx.progress.set_current(put.location.blob_size);
            JobRunOutcome::Succeeded(JobResultPayload::PluginWork {
                bytes: put.location.blob_size,
                version_id: Some(put.version_id),
            })
        }
        Err(_) if over.load(Ordering::Relaxed) => {
            permanent(too_large(download.max_bytes.unwrap_or_default()))
        }
        Err(error) if broken.load(Ordering::Relaxed) => retryable(error.to_string()),
        Err(error @ PutObjectError::StorageError(_)) => retryable(error.to_string()),
        Err(error) => permanent(error.to_string()),
    }
}

/// What an upload sends: the object at `source`, pinned to `version` when given.
pub struct Upload<'a> {
    pub source: &'a str,
    pub version: Option<Ulid>,
}

/// Bytes of an upload's answer the work keeps at most.
pub const MAX_REPLY: usize = 256 * 1024;

/// Sends one object version as the body of the row's request, as the account restricted to its
/// current upload patterns of the source. A request that may not go twice is sent once only.
pub async fn upload_work(
    ctx: &JobContext,
    spec: &PluginWorkSpec,
    (install, config, row): (&PluginInstall, &RealmConfigDocument, &WorkRow),
    (upload, keys): (Upload<'_>, &ReleaseKeys),
) -> JobRunOutcome {
    let patterns = match upload_grants(ctx, install, (row, upload.source)).await {
        Ok(found) => found,
        Err(error) => return refused(error),
    };
    let place = (spec.realm_id, ctx.owner_node_id);
    let (bucket, key) = match local_object(&ctx.driver, place, install, upload.source).await {
        Ok(found) => found,
        Err(error) => return refused(error),
    };
    // The version checked is the version sent, so a newer marked one never slips in.
    let version = match unmarked(&ctx.driver, (&bucket, &key), upload.version).await {
        Ok(Some(version)) => version,
        Ok(None) => return permanent("the object to upload is gone"),
        Err(error) => return refused(error),
    };
    let (outgoing, exposure, fields) = match prepared(ctx, (install, config, row), keys).await {
        Ok(prepared) => prepared,
        Err(error) => return refused(error),
    };
    let auth = account(install, &patterns);
    let read = GetObjectInput {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: Some(version),
        range: None,
        group_id: install.group_id,
        user_identity: install.account,
        realm_id: spec.realm_id,
        node_id: ctx.owner_node_id,
    };
    let read = GetObjectOperation::new(read).with_restrictions(auth.path_restrictions);
    // The object is open before the boundary moves, so a parked attempt never looks sent.
    let source = match drive(read, &ctx.driver).await {
        Ok(source) => source,
        Err(error) if locked(&error) => {
            return key_wait(ctx, install, (bucket, key, Some(version))).await;
        }
        Err(GetObjectError::NoSuchKey | GetObjectError::NoSuchVersion) => {
            return permanent("the object to upload is gone");
        }
        Err(error) => return retryable(error.to_string()),
    };
    let size = source.info.size;
    let again = outgoing.method == "PUT";
    let ids = (spec.install_id, spec.work_id);
    match begin_send(&ctx.driver, ids, again).await {
        Ok(Some(Send::Now)) => {}
        Ok(Some(Send::Undecided)) => {
            let unknown = "the upload may have been sent; its outcome is unknown";
            return JobRunOutcome::Undecided(JobError::retryable(unknown));
        }
        Ok(Some(Send::Answered { status, .. })) => return sent(status, again, (size, version)),
        Ok(None) => return permanent("the plugin work is no longer recorded"),
        Err(error) => return retryable(error.0),
    }
    let Some(blob) = ctx.driver.blob_handle.as_ref() else {
        return retryable("this node sends no requests");
    };
    let method = reqwest::Method::from_bytes(outgoing.method.as_bytes());
    let method = method.unwrap_or(reqwest::Method::PUT);
    let builder = match blob.repository_request(method, outgoing.url) {
        Ok(builder) => builder,
        Err(error) => return permanent(error.to_string()),
    };
    let builder = (outgoing.headers.iter()).fold(builder, |builder, (name, value)| {
        builder.header(name, value)
    });
    let body = reqwest::Body::wrap_stream(source.blob);
    let builder = builder
        .header(reqwest::header::CONTENT_LENGTH, size)
        .body(body);
    let response = tokio::select! {
        sent = builder.send() => sent,
        _ = ctx.cancel.cancelled() => return JobRunOutcome::Cancelled,
        _ = ctx.shutdown.cancelled() => return JobRunOutcome::Interrupted,
    };
    let mut response = match response {
        Ok(response) => response,
        Err(error) if error.is_connect() => {
            // Nothing reached the service, so the boundary goes back and a later attempt sends.
            let reopened = update_work(&ctx.driver, ids, |row| row.boundary = None);
            if let Err(error) = reopened.await {
                return retryable(error.0);
            }
            return retryable(error.without_url().to_string());
        }
        Err(error) => {
            let unknown = format!("the upload may have been sent: {}", error.without_url());
            return JobRunOutcome::Undecided(JobError::retryable(unknown));
        }
    };
    let status = response.status().as_u16();
    let headers: Vec<(String, String)> = (response.headers().iter())
        .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_string())))
        .collect();
    let mut answer = Vec::new();
    let mut whole = true;
    // A reply that breaks off or passes the bound is not kept, so no cut-off reply looks whole.
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) if answer.len() + chunk.len() <= MAX_REPLY => {
                answer.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Ok(Some(_)) | Err(_) => {
                whole = false;
                break;
            }
        }
    }
    // The reply is kept as the rule shows it now; one too large for the bound keeps no body.
    let reply = whole.then(|| {
        let shown = expose_response(&exposure, &fields, status, &headers, &answer);
        let headers = (shown.headers.into_iter()).map(|(name, value)| Field { name, value });
        let shown = Response {
            status: shown.status,
            headers: headers.collect(),
            body: shown.body.unwrap_or_default(),
        };
        whole_text(&shown)
    });
    if let Err(error) = record_sent(&ctx.driver, ids, (status, reply)).await {
        return retryable(error.0);
    }
    sent(status, again, (size, version))
}

/// How an answered upload ends: a 2xx succeeds; a request that may go twice is tried again
/// after a timeout, a throttle or a server error; anything else fails.
fn sent(status: u16, again: bool, (size, version): (u64, Ulid)) -> JobRunOutcome {
    match status {
        200..=299 => JobRunOutcome::Succeeded(JobResultPayload::PluginWork {
            bytes: size,
            version_id: Some(version),
        }),
        408 | 429 | 500..=599 if again => retryable(format!("the service answered {status}")),
        _ => permanent(format!("the service answered {status}")),
    }
}

/// The upload patterns of the source, after the connection was found still granted.
async fn upload_grants(
    ctx: &JobContext,
    install: &PluginInstall,
    (row, source): (&WorkRow, &str),
) -> Result<BTreeSet<String>, HostError> {
    connection_granted(install, row)?;
    let upload = Permission::TransfersUpload;
    let patterns = matching(install, upload, source)?;
    authorize_access(
        &ctx.driver,
        install,
        (upload, Access::READ),
        source,
        &patterns,
    )
    .await?;
    Ok(patterns)
}

/// Whether the row's connection is still granted to the install.
fn connection_granted(install: &PluginInstall, row: &WorkRow) -> Result<(), HostError> {
    let context = row.context.as_ref();
    let context = context.ok_or_else(|| HostError::new(ErrorKind::Invalid, "no request"))?;
    let scope = InstallScope {
        realm_id: install.account.realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    };
    matching(
        install,
        Permission::ConnectionsSend,
        &scope.connection(&context.connection),
    )?;
    Ok(())
}

fn locked(error: &GetObjectError) -> bool {
    matches!(
        error,
        GetObjectError::ConversionError(ConversionError::BucketKey(BucketKeyError::Locked(_)))
    )
}

/// The download patterns of the target, after the connection was found still granted.
async fn download_grants(
    ctx: &JobContext,
    install: &PluginInstall,
    (row, target): (&WorkRow, &str),
) -> Result<BTreeSet<String>, HostError> {
    connection_granted(install, row)?;
    let download = Permission::TransfersDownload;
    let patterns = matching(install, download, target)?;
    authorize_call(&ctx.driver, install, download, target, &patterns).await?;
    Ok(patterns)
}

/// The row's request checked against the install's current manifest, with the connection's
/// secret released and added; the exposure is the one the rule allows now.
async fn prepared(
    ctx: &JobContext,
    (install, config, row): (&PluginInstall, &RealmConfigDocument, &WorkRow),
    keys: &ReleaseKeys,
) -> Result<(OutgoingRequest, ResponseExposure, SecretFields), HostError> {
    let context = row.context.as_ref();
    let context = context.ok_or_else(|| HostError::new(ErrorKind::Invalid, "no request"))?;
    let plugin = pick_version(config, &install.plugin, Some(&install.digest));
    let manifest = plugin.and_then(|plugin| read_manifest(&plugin));
    let manifest =
        manifest.map_err(|error| HostError::new(ErrorKind::Unavailable, error.to_string()))?;
    let (mut request, slot) = outgoing(&manifest, context)?;
    let Some(slot) = slot else {
        let fields = SecretFields::new(Default::default());
        return Ok((request, ResponseExposure::Full, fields));
    };
    let place = (config, ctx.owner_node_id, keys);
    let released = release_into(
        &ctx.driver,
        place,
        (install, &slot, row.run_id),
        &mut request,
    );
    let (exposure, fields) = released.await?;
    Ok((request, exposure, fields))
}

/// The answer's body as a put stream that counts its bytes, ends with an error past `bound`, and
/// notes a broken transfer apart from a refused one.
fn counted(
    response: reqwest::Response,
    bound: Option<u64>,
    pulled: &Arc<AtomicU64>,
    (over, broken): (&Arc<AtomicBool>, &Arc<AtomicBool>),
) -> BackendStream<Result<bytes::Bytes, aruna_core::stream::StreamError>> {
    let (pulled, over, broken) = (pulled.clone(), over.clone(), broken.clone());
    let stream = response.bytes_stream().map(move |chunk| {
        let chunk = chunk.map_err(|error| {
            broken.store(true, Ordering::Relaxed);
            std::io::Error::other(error.without_url().to_string())
        })?;
        let total = pulled.fetch_add(chunk.len() as u64, Ordering::Relaxed) + chunk.len() as u64;
        if bound.is_some_and(|bound| total > bound) {
            over.store(true, Ordering::Relaxed);
            return Err(std::io::Error::other(
                "the answer is larger than its size limit",
            ));
        }
        Ok(chunk)
    });
    BackendStream::new(stream)
}

fn too_large(bound: u64) -> String {
    format!("the answer is larger than its size limit of {bound} bytes")
}

fn retryable(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.into()))
}

#[cfg(test)]
#[path = "transfers_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "upload_tests.rs"]
mod upload_tests;
