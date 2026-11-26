//! Retries writerless file server checkpoints that waited for group quota, at foreground commit
//! priority: each is published, rescheduled while the group stays full, or retained.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt::Debug;
use std::sync::Arc;
use std::task::Poll;

use aruna_clock::unix_timestamp_millis;
use aruna_core::bucket_read::GetBucketOperation;
use aruna_core::id::NodeId;
use aruna_core::stream::{BackendStream, ByteStream};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::vfs::{
    BucketScope, DetachReason, NfsStageRecord, StageBase, StageDisposition,
};
use aruna_core::vfs_effects::VfsProtocol;
use aruna_ops_data::s3::object::put::{PutObjectConfig, PutObjectInput};
use aruna_ops_data::s3::realm_usage::{RealmUsageOperation, RealmUsageScope};
use aruna_ops_data::vfs::publish::PublishInput;
use aruna_ops_data::vfs::{
    CHUNK_BYTES, InstanceTarget, LiveStageOperation, LookupInstanceOperation, LookupMountOperation,
    QuotaStageOperation, ReadCheckpointOperation, VfsAccessContext, VfsError, VfsPublishOperation,
};
use aruna_ops_identity::document_repository::GetConfigOperation;
use aruna_ops_identity::groups::routing::{BucketRules, SnapshotOperation};
use aruna_runner::driver::{DriverContext, drive, node_routing};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use ulid::Ulid;

/// Checkpoint chunks read ahead of the write that consumes them.
const READ_AHEAD: usize = 2;

/// What a due checkpoint needs before its image streams.
enum Ready {
    /// The prepared publication.
    Publish(Box<(PublishInput, PutObjectConfig)>),
    /// The group has no headroom for the image yet.
    Full,
    /// No live instance carries the stage; it waits for a later retry.
    Later,
    /// The owner's mount is gone, so the checkpoint leaves the namespace.
    Lost,
    /// A writer holds the stage again, so its own close publishes it.
    Held,
}

/// Retries each due checkpoint in turn on the foreground lane; a step that fails transiently
/// leaves the stage for the next sweep.
pub(super) async fn retry_due_stages(context: &Arc<DriverContext>, due: Vec<(BucketScope, u64)>) {
    let Some(serving) = context.node_and_realm() else {
        warn!(
            stages = due.len(),
            "Quota-blocked checkpoints wait for the network transport"
        );
        return;
    };
    for (scope, handle_id) in due {
        retry_stage(context, serving, scope, handle_id).await;
    }
}

async fn retry_stage(
    context: &Arc<DriverContext>,
    serving: (NodeId, RealmId),
    scope: BucketScope,
    handle_id: u64,
) {
    let read = LiveStageOperation::new(scope.clone(), handle_id);
    let stage = match drive(read, context).await {
        Ok(Some(stage))
            if stage.disposition == StageDisposition::Live && stage.quota_retry.is_some() =>
        {
            stage
        }
        Ok(_) => return,
        Err(error) => {
            warn!(
                bucket = %scope.bucket,
                handle_id,
                error = %error,
                "Quota-blocked checkpoint read failed"
            );
            return;
        }
    };
    match prepare(context, serving, &scope, &stage).await {
        Ok(Ready::Publish(prepared)) => publish(context, scope, stage, *prepared).await,
        Ok(Ready::Full) => {
            warn!(
                bucket = %scope.bucket,
                key = %stage.key,
                stage_id = %stage.stage_id,
                bytes = stage.logical_size,
                "Group quota still blocks an acknowledged file server checkpoint"
            );
            reschedule(context, scope, stage).await;
        }
        Ok(Ready::Later) => {
            warn!(
                bucket = %scope.bucket,
                stage_id = %stage.stage_id,
                "No live instance carries a quota-blocked checkpoint"
            );
            reschedule(context, scope, stage).await;
        }
        Ok(Ready::Lost) => retain(context, scope, stage, DetachReason::LostLife).await,
        Ok(Ready::Held) => {
            debug!(
                bucket = %scope.bucket,
                stage_id = %stage.stage_id,
                "A writer holds the quota-blocked checkpoint"
            );
        }
        Err(reason) => {
            warn!(
                bucket = %scope.bucket,
                stage_id = %stage.stage_id,
                reason = %reason,
                "Quota-blocked checkpoint retry waits"
            );
        }
    }
}

/// A transient failure of one preparation step, which leaves the stage for the next sweep.
fn failed<E: Debug>(step: &'static str) -> impl FnOnce(E) -> String {
    move |error| format!("{step}: {error:?}")
}

/// Checks the owner's mount, the live instance and the group headroom, then prepares the write
/// of the checkpoint image without reading it yet.
async fn prepare(
    context: &Arc<DriverContext>,
    (node_id, realm_id): (NodeId, RealmId),
    scope: &BucketScope,
    stage: &NfsStageRecord,
) -> Result<Ready, String> {
    let mount = LookupMountOperation::new(stage.owner.mount_id);
    let Some(binding) = drive(mount, context).await.map_err(failed("mount"))? else {
        return Ok(Ready::Lost);
    };
    let access = VfsAccessContext {
        realm_id,
        node_id,
        access_key: stage.owner.access_key.clone(),
        protocol: VfsProtocol::Nfs,
        target: binding.target,
        read_only: binding.read_only,
        now_ms: unix_timestamp_millis(),
    };
    if access.bucket_scope() != Some((scope.bucket.as_str(), scope.incarnation)) {
        return Ok(Ready::Lost);
    }
    let target = InstanceTarget::Handle(stage.handle_id);
    let lookup = LookupInstanceOperation::new(scope.clone(), target);
    let Some(instance) = drive(lookup, context).await.map_err(failed("instance"))? else {
        return Ok(Ready::Later);
    };
    if instance.writer_count > 0 {
        return Ok(Ready::Held);
    }
    let bucket = GetBucketOperation::new(scope.bucket.clone());
    let info = drive(bucket, context).await.map_err(failed("bucket"))?;
    let config = GetConfigOperation::new(realm_id);
    let realm = drive(config, context)
        .await
        .map_err(failed("realm config"))?;
    let ceiling = realm.quota.effective_group_ceiling(&info.group_id);
    if let Some(ceiling) = ceiling {
        let group = RealmUsageScope::Group(info.group_id);
        let usage = RealmUsageOperation::new(node_id, Some(realm_id), group);
        let totals = drive(usage, context).await.map_err(failed("group usage"))?;
        let used = totals
            .first()
            .map_or(0, |total| total.counters.logical_bytes);
        if used.saturating_add(stage.logical_size) > ceiling {
            return Ok(Ready::Full);
        }
    }
    let rules = BucketRules::of(&info);
    let snapshot = SnapshotOperation::new(&node_routing(context), info.group_id, rules);
    let routing = drive(snapshot, context).await.map_err(failed("routing"))?;
    let put = PutObjectConfig {
        user_id: stage.owner.user,
        group_id: info.group_id,
        realm_id,
        node_id,
        request: PutObjectInput {
            bucket: scope.bucket.clone(),
            key: stage.key.clone(),
            content_length: Some(stage.logical_size),
            body: Some(checkpoint_body(context.clone(), stage)),
        },
        expected_checksums: Vec::new(),
        checksum_type: None,
        exists: matches!(stage.base, StageBase::Committed { .. }),
        version_source: None,
        preassigned_version_id: None,
        quota_ceiling: ceiling,
        routing,
    };
    let input = PublishInput {
        context: access,
        instance_id: instance.instance_id,
        key: stage.key.clone(),
        handle_id: Some(stage.handle_id),
        base: stage.base.clone(),
        replaces: false,
        stage: Some(stage.clone()),
        logical_size: stage.logical_size,
        attempt_id: Ulid::generate(),
        updates: Vec::new(),
    };
    Ok(Ready::Publish(Box::new((input, put))))
}

/// Publishes the checkpoint. A full group reschedules it, a lasting refusal retains it outside
/// the namespace, and any other failure leaves it for the next sweep.
async fn publish(
    context: &Arc<DriverContext>,
    scope: BucketScope,
    stage: NfsStageRecord,
    (input, put): (PublishInput, PutObjectConfig),
) {
    let operation = VfsPublishOperation::new(input, put);
    match drive(operation, context).await {
        Ok(published) => info!(
            bucket = %scope.bucket,
            key = %stage.key,
            version_id = %published.version_id,
            "Published a quota-blocked file server checkpoint"
        ),
        Err(VfsError::QuotaExceeded) => {
            warn!(
                bucket = %scope.bucket,
                key = %stage.key,
                stage_id = %stage.stage_id,
                bytes = stage.logical_size,
                "Group quota refused an acknowledged file server checkpoint again"
            );
            reschedule(context, scope, stage).await;
        }
        Err(
            error @ (VfsError::Denied
            | VfsError::VaultLocked
            | VfsError::KeyRequired
            | VfsError::Quarantined
            | VfsError::StaleHandle
            | VfsError::ExpiredHandle
            | VfsError::Distributed
            | VfsError::ReadOnly),
        ) => {
            warn!(
                bucket = %scope.bucket,
                key = %stage.key,
                stage_id = %stage.stage_id,
                error = %error,
                "A quota-blocked checkpoint can no longer publish; it is retained"
            );
            retain(context, scope, stage, DetachReason::LostLife).await;
        }
        Err(error) => warn!(
            bucket = %scope.bucket,
            stage_id = %stage.stage_id,
            error = %error,
            "Quota-blocked checkpoint publication failed; the next sweep retries"
        ),
    }
}

/// Keeps the stage live and readable and backs its next retry off.
async fn reschedule(context: &Arc<DriverContext>, scope: BucketScope, stage: NfsStageRecord) {
    let stage_id = stage.stage_id;
    let operation = QuotaStageOperation::reschedule(scope, stage);
    if let Err(error) = drive(operation, context).await {
        warn!(
            stage_id = %stage_id,
            error = %error,
            "Failed to reschedule a quota-blocked checkpoint"
        );
    }
}

/// Keeps the acknowledged checkpoint outside the namespace by its stage id.
async fn retain(
    context: &Arc<DriverContext>,
    scope: BucketScope,
    stage: NfsStageRecord,
    reason: DetachReason,
) {
    let stage_id = stage.stage_id;
    let operation = QuotaStageOperation::retain(scope, stage, reason);
    if let Err(error) = drive(operation, context).await {
        warn!(stage_id = %stage_id, error = %error, "Failed to retain a quota-blocked checkpoint");
    }
}

/// Reads one sealed checkpoint generation in bounded chunks.
struct CheckpointReader {
    context: Arc<DriverContext>,
    stage_id: Ulid,
    generation: u64,
    size: u64,
}

impl CheckpointReader {
    /// Sends chunks until the whole image or the first failure; an early end is a corrupt stage.
    async fn run<T: From<Vec<u8>>>(self, sender: mpsc::Sender<Result<T, VfsError>>) {
        let mut offset = 0;
        while offset < self.size {
            let length = u32::try_from((self.size - offset).min(CHUNK_BYTES)).unwrap_or(u32::MAX);
            let read = ReadCheckpointOperation::new(self.stage_id, self.generation, offset, length);
            let chunk = match drive(read, &self.context).await {
                Ok(chunk) if chunk.data.is_empty() => Err(VfsError::CorruptStage),
                Ok(chunk) => Ok(chunk.data),
                Err(error) => Err(error),
            };
            let stop = chunk.is_err();
            offset += chunk.as_ref().map_or(0, |data| data.len() as u64);
            if sender.send(chunk.map(T::from)).await.is_err() || stop {
                return;
            }
        }
    }
}

/// The reader before the write first polls, then the chunks it sends.
enum Feed<T> {
    Waiting(Option<Box<CheckpointReader>>),
    Reading(mpsc::Receiver<Result<T, VfsError>>),
}

/// The checkpoint image of `stage`, read only once the write consumes it.
fn checkpoint_body(context: Arc<DriverContext>, stage: &NfsStageRecord) -> ByteStream {
    let reader = Box::new(CheckpointReader {
        context,
        stage_id: stage.stage_id,
        generation: stage.generation,
        size: stage.logical_size,
    });
    let mut feed = Feed::Waiting(Some(reader));
    BackendStream::new(futures_util::stream::poll_fn(move |task| {
        if let Feed::Waiting(reader) = &mut feed {
            let (sender, receiver) = mpsc::channel(READ_AHEAD);
            if let Some(reader) = reader.take() {
                tokio::spawn(reader.run(sender));
            }
            feed = Feed::Reading(receiver);
        }
        match &mut feed {
            Feed::Reading(receiver) => receiver.poll_recv(task),
            Feed::Waiting(_) => Poll::Ready(None),
        }
    }))
}
