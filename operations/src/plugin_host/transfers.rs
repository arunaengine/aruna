//! Moves bytes between a plugin connection and the receiver's buckets for plugin work: the
//! operations decide each step, the outbound backend carries the bytes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_core::errors::ConversionError;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
use aruna_core::structs::execution::job::{JobError, JobResultPayload, PluginWorkSpec, UploadForm};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::CONTENT_TYPE_KEY;
use aruna_core::structs::storage::encryption::BucketKeyError;
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_core::time::unix_timestamp_millis;
use aruna_outbound::{OutboundHandle, TransferError, UploadBody};
use ulid::Ulid;

use super::plugin_work::{PROGRESS_TICK, key_wait, permanent, refused};
use crate::driver::{drive, node_routing};
use crate::groups::routing::{BucketRules, SnapshotOperation};
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::placement::policy::GateOperation;
use crate::plugins::transfer_steps::{
    KeepReply, PrepareDownload, PrepareUpload, TransferEnd, TransferWork, UploadReply, account,
    check_answer, send_end, source_gone, write_failure,
};
use crate::plugins::work::{WorkChange, WorkRow, begin_send, update_work};
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectOperation};
use crate::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};

/// What a download writes: its target, the version minted once, expected digests and a bound.
pub struct Download<'a> {
    pub target: &'a str,
    pub version_id: Ulid,
    pub checksums: &'a [(ChecksumAlgorithm, Vec<u8>)],
    pub max_bytes: Option<u64>,
}

fn outcome(end: TransferEnd) -> JobRunOutcome {
    match end {
        TransferEnd::Done { bytes, version } => {
            JobRunOutcome::Succeeded(JobResultPayload::PluginWork {
                bytes,
                version_id: Some(version),
            })
        }
        TransferEnd::Refused(error) => refused(error),
        TransferEnd::Retry(message) => retryable(message),
        TransferEnd::Permanent(message) => permanent(message),
        TransferEnd::Undecided(message) => JobRunOutcome::Undecided(JobError::retryable(message)),
    }
}

fn outbound(ctx: &JobContext) -> Result<&OutboundHandle, JobRunOutcome> {
    (ctx.driver.outbound_handle.as_ref()).ok_or_else(|| retryable("this node sends no requests"))
}

/// Downloads the answer of the row's request into a new version of the target, as the account
/// restricted to its current download patterns of the target.
pub async fn download_work(
    ctx: &JobContext,
    spec: &PluginWorkSpec,
    (install, config, row): (&PluginInstall, &RealmConfigDocument, &WorkRow),
    download: Download<'_>,
) -> JobRunOutcome {
    let node = (spec.realm_id, ctx.owner_node_id);
    let work = TransferWork {
        node,
        config,
        install,
        row,
    };
    let prepare = PrepareDownload::new(work, download.target, download.version_id);
    let plan = match drive(prepare, &ctx.driver).await {
        Ok(plan) => plan,
        Err(end) => {
            if let TransferEnd::Done { bytes, .. } = &end {
                ctx.progress.set_current(*bytes);
            }
            return outcome(end);
        }
    };
    let backend = match outbound(ctx) {
        Ok(backend) => backend,
        Err(outcome) => return outcome,
    };
    let opened = tokio::select! {
        opened = backend.open_download(plan.outgoing, download.max_bytes) => opened,
        _ = ctx.cancel.cancelled() => return JobRunOutcome::Cancelled,
        _ = ctx.shutdown.cancelled() => return JobRunOutcome::Interrupted,
    };
    let opened = match opened {
        Ok(opened) => opened,
        Err(TransferError::Refused(why)) => return permanent(why),
        Err(error) => return retryable(error.to_string()),
    };
    if let Err(end) = check_answer(opened.status, opened.length, download.max_bytes) {
        return outcome(end);
    }
    let routing = SnapshotOperation::new(
        &node_routing(&ctx.driver),
        install.group_id,
        BucketRules::Named(plan.bucket.clone()),
    );
    let routing = match drive(routing, &ctx.driver).await {
        Ok(routing) => routing,
        Err(error) => return retryable(error.to_string()),
    };
    let gate = GateOperation::new(spec.realm_id, unix_timestamp_millis());
    let gate = match drive(gate, &ctx.driver).await {
        Ok(gate) => gate,
        Err(error) => return retryable(error.to_string()),
    };
    let expected_checksums = (download.checksums.iter())
        .map(|(algorithm, digest)| ExpectedChecksum {
            algorithm: *algorithm,
            digest: digest.clone(),
        })
        .collect();
    let metadata: HashMap<String, String> = (opened.content_type)
        .map(|kind| (CONTENT_TYPE_KEY.to_string(), kind))
        .into_iter()
        .collect();
    let auth = account(install, &plan.patterns);
    let mut put = PutObjectOperation::new(PutObjectConfig {
        user_id: install.account,
        group_id: install.group_id,
        realm_id: spec.realm_id,
        node_id: ctx.owner_node_id,
        request: PutObjectInput {
            bucket: plan.bucket,
            key: plan.key,
            content_length: opened.length,
            body: Some(opened.body),
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
    let meter = opened.meter;
    let written = drive(put, &ctx.driver);
    tokio::pin!(written);
    let mut tick = tokio::time::interval(PROGRESS_TICK);
    let result = loop {
        tokio::select! {
            result = &mut written => break result,
            _ = tick.tick() => ctx.progress.set_current(meter.pulled()),
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
        Err(error) => {
            let body = (meter.over(), meter.broken());
            outcome(write_failure(&error, body, download.max_bytes))
        }
    }
}

/// What an upload sends: the object at `source`, pinned to `version` when given, as the body or
/// as the file part of a form.
pub struct Upload<'a> {
    pub source: &'a str,
    pub version: Option<Ulid>,
    pub form: Option<&'a UploadForm>,
}

/// Bytes of an upload's answer the work keeps at most.
pub const MAX_REPLY: usize = 256 * 1024;

/// Sends one object version as the body of the row's request, as the account restricted to its
/// current upload patterns of the source. A request that may not go twice is sent once only.
pub async fn upload_work(
    ctx: &JobContext,
    spec: &PluginWorkSpec,
    (install, config, row): (&PluginInstall, &RealmConfigDocument, &WorkRow),
    upload: Upload<'_>,
) -> JobRunOutcome {
    let node = (spec.realm_id, ctx.owner_node_id);
    let work = TransferWork {
        node,
        config,
        install,
        row,
    };
    let prepare = PrepareUpload::new(work, upload.source, upload.version);
    let plan = match drive(prepare, &ctx.driver).await {
        Ok(plan) => plan,
        Err(end) => return outcome(end),
    };
    let auth = account(install, &plan.patterns);
    let (bucket, key, version) = (plan.bucket.clone(), plan.key.clone(), plan.version);
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
            return outcome(source_gone());
        }
        Err(error) => return retryable(error.to_string()),
    };
    let size = source.info.size;
    let ids = (spec.install_id, spec.work_id);
    match drive(begin_send(ids, plan.again), &ctx.driver).await {
        Ok(Some(send)) => {
            if let Some(end) = send_end(send, plan.again) {
                return outcome(end);
            }
        }
        Ok(None) => return permanent("the plugin work is no longer recorded"),
        Err(error) => return retryable(error.0),
    }
    let backend = match outbound(ctx) {
        Ok(backend) => backend,
        Err(outcome) => return outcome,
    };
    let body = UploadBody {
        bytes: source.blob,
        size,
        form: upload.form.cloned(),
    };
    let request = plan.request.outgoing.clone();
    let answer = tokio::select! {
        answer = backend.send_upload(request, body, MAX_REPLY) => answer,
        _ = ctx.cancel.cancelled() => return JobRunOutcome::Cancelled,
        _ = ctx.shutdown.cancelled() => return JobRunOutcome::Interrupted,
    };
    let answer = match answer {
        Ok(answer) => answer,
        Err(TransferError::Refused(why)) => return permanent(why),
        Err(TransferError::NotConnected(why)) => {
            // Nothing reached the service, so the boundary goes back and a later attempt sends.
            let reopened = drive(update_work(ids, WorkChange::Reopened), &ctx.driver);
            if let Err(error) = reopened.await {
                return retryable(error.0);
            }
            return retryable(why);
        }
        // A PUT may go twice, so one whose answer broke off is tried again.
        Err(TransferError::Broken(why)) if plan.again => return retryable(why),
        Err(TransferError::Broken(why)) => {
            let unknown = format!("the upload may have been sent: {why}");
            return JobRunOutcome::Undecided(JobError::retryable(unknown));
        }
    };
    let reply = UploadReply {
        status: answer.status,
        headers: answer.headers,
        body: answer.body,
    };
    let keep = KeepReply::new(ids, plan, size, reply);
    match drive(keep, &ctx.driver).await {
        Ok(end) | Err(end) => outcome(end),
    }
}

fn locked(error: &GetObjectError) -> bool {
    matches!(
        error,
        GetObjectError::ConversionError(ConversionError::BucketKey(BucketKeyError::Locked(_)))
    )
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
