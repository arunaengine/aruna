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
    BucketScope, DetachReason, EnrolledHostRecord, NfsMountBinding, NfsMountTarget, NfsStageRecord,
    StageBase, StageDisposition,
};
use aruna_core::vfs_effects::{InstanceSummary, VfsProtocol};
use aruna_ops_data::s3::object::put::{PutObjectConfig, PutObjectInput};
use aruna_ops_data::s3::realm_usage::{RealmUsageOperation, RealmUsageScope};
use aruna_ops_data::vfs::io::CommitInput;
use aruna_ops_data::vfs::publish::PublishInput;
use aruna_ops_data::vfs::{
    CHUNK_BYTES, InstanceTarget, LiveStageOperation, LookupHostOperation, LookupInstanceOperation,
    LookupMountOperation, QuotaStageOperation, ReadCheckpointOperation, VfsAccessContext,
    VfsCommitOperation, VfsError, VfsPublishOperation,
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
    /// The owner's admission is gone, so the checkpoint and its instance, if any, leave the name.
    Lost(Option<u64>),
    /// A writer holds the stage again, or the instance no longer carries this generation.
    Held,
    /// Newer edits were sealed into the next generation, which the next sweep publishes.
    Sealed,
}

/// How the live instance stands against the sealed checkpoint a retry streams.
#[derive(Debug, PartialEq, Eq)]
enum Seal {
    /// The checkpoint holds every edit; publication is fenced at this edit.
    Current(u64),
    /// Edits after the seal live only in the overlay and are sealed before publication.
    Stale,
    /// A writer holds the instance, or it no longer carries this sealed generation.
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
        Ok(Ready::Lost(instance_id)) => {
            retain(context, scope, stage, instance_id, DetachReason::LostLife).await
        }
        Ok(Ready::Held) => {
            debug!(
                bucket = %scope.bucket,
                stage_id = %stage.stage_id,
                "A writer holds the quota-blocked checkpoint"
            );
        }
        Ok(Ready::Sealed) => {
            debug!(
                bucket = %scope.bucket,
                stage_id = %stage.stage_id,
                "Sealed newer edits of a quota-blocked checkpoint for the next sweep"
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

/// Checks the owner's admission, the live instance and the group headroom, then prepares the
/// write of the sealed checkpoint image without reading it yet.
async fn prepare(
    context: &Arc<DriverContext>,
    (node_id, realm_id): (NodeId, RealmId),
    scope: &BucketScope,
    stage: &NfsStageRecord,
) -> Result<Ready, String> {
    let target = InstanceTarget::Handle(stage.handle_id);
    let lookup = LookupInstanceOperation::new(scope.clone(), target);
    let instance = drive(lookup, context).await.map_err(failed("instance"))?;
    let carried = instance.as_ref().map(|found| found.instance_id);
    let binding = match stage.owner.mount_id.is_nil() {
        true => None,
        false => live_binding(context, stage.owner.mount_id).await?,
    };
    let Some(access) = owner_access((node_id, realm_id), scope, stage, binding.as_ref()) else {
        return Ok(Ready::Lost(carried));
    };
    let Some(instance) = instance else {
        return Ok(Ready::Later);
    };
    // Newer edits seal first, so the headroom check sees the size of the image that publishes.
    let sealed_edit = match seal_state(&instance, stage) {
        Seal::Held => return Ok(Ready::Held),
        Seal::Stale => return seal(context, access, instance.instance_id, stage).await,
        Seal::Current(edit) => edit,
    };
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
        expected_edit: Some(sealed_edit),
        require_writerless: true,
    };
    Ok(Ready::Publish(Box::new((input, put))))
}

/// The owner's NFS mount while it and its enrolled host still serve; `None` once either is gone.
async fn live_binding(
    context: &Arc<DriverContext>,
    mount_id: Ulid,
) -> Result<Option<NfsMountBinding>, String> {
    let mount = LookupMountOperation::new(mount_id);
    let Some(binding) = drive(mount, context).await.map_err(failed("mount"))? else {
        return Ok(None);
    };
    let host = LookupHostOperation::new(binding.host_id);
    match drive(host, context).await {
        Ok(Some(host)) if host_admits(&host, &binding) => Ok(Some(binding)),
        Ok(Some(_) | None) => Ok(None),
        Err(error) => Err(format!("host: {error:?}")),
    }
}

/// An unrevoked host serves its owner's bindings; a worker pool host also those of other owners.
fn host_admits(host: &EnrolledHostRecord, binding: &NfsMountBinding) -> bool {
    host.revoked_at_ms.is_none() && (binding.owner == host.owner || host.worker_pool)
}

/// The admission the owner wrote under: an SMB successor's bucket share by its access key, or
/// the NFS mount `binding`; `None` when neither still admits this bucket incarnation.
fn owner_access(
    (node_id, realm_id): (NodeId, RealmId),
    scope: &BucketScope,
    stage: &NfsStageRecord,
    binding: Option<&NfsMountBinding>,
) -> Option<VfsAccessContext> {
    let (protocol, target, read_only) = match binding {
        Some(binding) => (VfsProtocol::Nfs, binding.target.clone(), binding.read_only),
        None if stage.owner.mount_id.is_nil() => {
            let target = NfsMountTarget::for_bucket(&scope.bucket, scope.incarnation).ok()?;
            (VfsProtocol::Smb, target, false)
        }
        None => return None,
    };
    let access = VfsAccessContext {
        realm_id,
        node_id,
        access_key: stage.owner.access_key.clone(),
        protocol,
        target,
        read_only,
        now_ms: unix_timestamp_millis(),
    };
    let admitted = access.bucket_scope() == Some((scope.bucket.as_str(), scope.incarnation));
    admitted.then_some(access)
}

/// Whether the writerless instance still carries `stage` and holds no edit after its seal.
fn seal_state(instance: &InstanceSummary, stage: &NfsStageRecord) -> Seal {
    let bound =
        instance.stage_id == Some(stage.stage_id) && instance.generation == stage.generation;
    match instance.sealed_edit {
        _ if instance.writer_count > 0 || !bound => Seal::Held,
        Some(edit) if edit == instance.edit_generation => Seal::Current(edit),
        Some(_) => Seal::Stale,
        None => Seal::Held,
    }
}

/// Seals the overlay's newer edits into the next generation, so the whole current image
/// publishes; a lasting refusal retains the older acknowledged checkpoint instead.
async fn seal(
    context: &Arc<DriverContext>,
    access: VfsAccessContext,
    instance_id: u64,
    stage: &NfsStageRecord,
) -> Result<Ready, String> {
    let mut record = stage.clone();
    record.generation = stage.generation.saturating_add(1);
    record.updated_at_ms = access.now_ms;
    let input = CommitInput {
        context: access,
        instance_id,
        record,
        previous: Some(stage.clone()),
    };
    match drive(VfsCommitOperation::new(input), context).await {
        Ok(_) => Ok(Ready::Sealed),
        Err(VfsError::Denied | VfsError::ReadOnly | VfsError::VaultLocked) => {
            Ok(Ready::Lost(Some(instance_id)))
        }
        Err(error) => Err(format!("seal: {error:?}")),
    }
}

/// Publishes the checkpoint. A full group reschedules it, a lasting refusal retains it outside
/// the namespace, and any other failure leaves it for the next sweep.
async fn publish(
    context: &Arc<DriverContext>,
    scope: BucketScope,
    stage: NfsStageRecord,
    (input, put): (PublishInput, PutObjectConfig),
) {
    let instance_id = input.instance_id;
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
        Err(VfsError::Conflict) => debug!(
            bucket = %scope.bucket,
            stage_id = %stage.stage_id,
            "A writer returned or edited the quota-blocked checkpoint before publication"
        ),
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
            let carried = Some(instance_id);
            retain(context, scope, stage, carried, DetachReason::LostLife).await;
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

/// Keeps the acknowledged checkpoint outside the namespace by its stage id, and detaches the
/// runtime instance that carries it.
async fn retain(
    context: &Arc<DriverContext>,
    scope: BucketScope,
    stage: NfsStageRecord,
    instance_id: Option<u64>,
    reason: DetachReason,
) {
    let stage_id = stage.stage_id;
    let mut operation = QuotaStageOperation::retain(scope, stage, reason);
    if let Some(instance_id) = instance_id {
        operation = operation.with_instance(instance_id);
    }
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

#[cfg(test)]
mod tests {
    use aruna_core::structs::storage::vfs::{
        FileIdentity, HandleIdentity, HandleKind, StageOwner, StageScope,
    };
    use aruna_core::user::id::UserId;

    use super::*;

    fn id(seed: u8) -> Ulid {
        Ulid::from_bytes([seed; 16])
    }

    fn serving() -> (NodeId, RealmId) {
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        (node_id, RealmId::from_bytes([1; 32]))
    }

    fn scope() -> BucketScope {
        BucketScope::new("bucket", 7)
    }

    fn stage(mount_id: Ulid) -> NfsStageRecord {
        let realm_id = serving().1;
        NfsStageRecord {
            stage_id: id(12),
            handle_id: 4,
            owner: StageOwner {
                user: UserId::new(id(1), realm_id),
                access_key: "KEY1".to_string(),
                mount_id,
                scope: StageScope::User,
            },
            key: "p/a".to_string(),
            generation: 2,
            checkpoint: "c-2".to_string(),
            logical_size: 5,
            base: StageBase::Unpublished {
                token: id(6),
                predecessor: None,
            },
            source_copies: Vec::new(),
            source_policies: Vec::new(),
            destination_copies: Vec::new(),
            destination_policies: Vec::new(),
            disposition: StageDisposition::Live,
            quota_retry: None,
            foreign_life: None,
            updated_at_ms: 1,
        }
    }

    fn binding(target: NfsMountTarget) -> NfsMountBinding {
        NfsMountBinding {
            mount_id: id(13),
            host_id: id(14),
            owner: UserId::new(id(1), serving().1),
            access_key: "MOUNT".to_string(),
            group_id: id(15),
            target,
            read_only: true,
            allowed_networks: Vec::new(),
            display_uid: 0,
            display_gid: 0,
            mode: 0o755,
            created_at_ms: 1,
        }
    }

    /// A writerless instance carrying generation 2 of `stage()`, sealed at edit 3.
    fn instance() -> InstanceSummary {
        InstanceSummary {
            instance_id: 9,
            key: "p/a".to_string(),
            kind: HandleKind::File,
            handle_id: Some(4),
            identity: HandleIdentity::File(FileIdentity::Unpublished {
                token: id(6),
                predecessor: None,
            }),
            logical_size: 5,
            base_limit: 0,
            dirty: true,
            delete_pending: false,
            disposition_owner: None,
            open_count: 0,
            writer_count: 0,
            detached: None,
            stage_id: Some(id(12)),
            generation: 2,
            change: 0,
            base_version: None,
            edit_generation: 3,
            allows_delete: true,
            sealed_edit: Some(3),
            draft: None,
        }
    }

    #[test]
    fn smb_owner_access() {
        // An SMB successor names no mount: its bucket share is rebuilt from the access key.
        let access = owner_access(serving(), &scope(), &stage(Ulid::nil()), None)
            .expect("the SMB successor keeps its bucket share");
        assert_eq!(access.protocol, VfsProtocol::Smb);
        assert_eq!(access.access_key, "KEY1");
        assert!(!access.read_only);
        assert_eq!(access.bucket_scope(), Some(("bucket", 7)));
    }

    #[test]
    fn nfs_owner_access() {
        let mounted = binding(NfsMountTarget::for_bucket("bucket", 7).unwrap());
        let nfs = stage(id(13));
        let access = owner_access(serving(), &scope(), &nfs, Some(&mounted))
            .expect("the NFS owner keeps its mount");
        assert_eq!(access.protocol, VfsProtocol::Nfs);
        assert_eq!(access.access_key, "KEY1");
        assert!(
            access.read_only,
            "the binding's read-only flag carries over"
        );
        assert_eq!(access.target, mounted.target);
        assert!(
            owner_access(serving(), &scope(), &nfs, None).is_none(),
            "a missing NFS mount admits nothing"
        );
        let other = binding(NfsMountTarget::for_bucket("bucket", 8).unwrap());
        assert!(
            owner_access(serving(), &scope(), &nfs, Some(&other)).is_none(),
            "a mount of another bucket incarnation admits nothing"
        );
    }

    #[test]
    fn sealed_edit_gate() {
        let record = stage(Ulid::nil());
        assert_eq!(seal_state(&instance(), &record), Seal::Current(3));
        let edited = InstanceSummary {
            edit_generation: 4,
            ..instance()
        };
        assert_eq!(seal_state(&edited, &record), Seal::Stale);
        let writing = InstanceSummary {
            writer_count: 1,
            edit_generation: 4,
            ..instance()
        };
        assert_eq!(seal_state(&writing, &record), Seal::Held);
        let rebound = InstanceSummary {
            generation: 3,
            ..instance()
        };
        assert_eq!(seal_state(&rebound, &record), Seal::Held);
        let unsealed = InstanceSummary {
            sealed_edit: None,
            ..instance()
        };
        assert_eq!(seal_state(&unsealed, &record), Seal::Held);
    }

    #[test]
    fn truncated_overlay_seals() {
        // A writer truncated the quota-blocked file after its seal: the smaller overlay seals
        // before any headroom check, which then sees the new generation's size.
        let record = stage(Ulid::nil());
        let truncated = InstanceSummary {
            logical_size: 1,
            edit_generation: 4,
            ..instance()
        };
        assert!(truncated.logical_size < record.logical_size);
        assert_eq!(seal_state(&truncated, &record), Seal::Stale);
    }

    #[test]
    fn worker_pool_admits() {
        let mounted = binding(NfsMountTarget::for_bucket("bucket", 7).unwrap());
        let tenant = UserId::new(id(2), serving().1);
        let plain = EnrolledHostRecord {
            host_id: id(14),
            owner: tenant,
            fingerprints: Vec::new(),
            allowed_networks: Vec::new(),
            worker_pool: false,
            created_at_ms: 1,
            revoked_at_ms: None,
        };
        assert!(
            !host_admits(&plain, &mounted),
            "a plain host admits only its owner's bindings"
        );
        let own = EnrolledHostRecord {
            owner: mounted.owner,
            ..plain.clone()
        };
        assert!(host_admits(&own, &mounted));
        let pool = EnrolledHostRecord {
            worker_pool: true,
            ..plain
        };
        assert!(
            host_admits(&pool, &mounted),
            "a worker pool host admits other owners' bindings"
        );
        let revoked = EnrolledHostRecord {
            revoked_at_ms: Some(9),
            ..pool
        };
        assert!(!host_admits(&revoked, &mounted));
    }
}
