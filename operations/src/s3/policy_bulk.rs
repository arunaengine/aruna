//! Bounded bulk application of a bucket default to current heads.
//!
//! A run seals one `(bucket identity, generation, target refs)` target. Each
//! object gets a durable intent naming its observed head and one preassigned
//! successor VersionId, and the successor is minted through the same per-version
//! operation the single-object mutation uses. The application is additive: it
//! unions the sealed refs with the head re-read inside the mint transaction, so
//! applying a default never removes an object's existing constraints.

use crate::blob::blob_keyspace_helper::HeadAliasContext;
use crate::driver::{DriverContext, drive};
use crate::s3::policy_successor::{
    MintPolicySuccessorOperation, SuccessorError, SuccessorOutcome, SuccessorPlan,
};
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::structs::{
    AuthContext, BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer,
    POLICY_BULK_INTENT_KEYSPACE, POLICY_BULK_RUN_KEYSPACE, PlacementPolicyRef, PlacementSubject,
    PolicyBlockedReason, PolicyBulkIntent, PolicyBulkIntentKey, PolicyBulkRun, PolicyBulkStatus,
    PolicyIntentOutcome, PolicyRefMode, PolicyResolution, RealmId, VersionKey,
};
use aruna_core::types::{GroupId, Key, NodeId};
use std::collections::BTreeMap;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

/// Upper bound on the heads one pass touches.
pub const BULK_PAGE_LIMIT: usize = 128;

#[derive(Clone, Debug, PartialEq)]
pub struct BulkInput {
    pub operation_id: Ulid,
    pub bucket: String,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub auth_context: AuthContext,
    pub subject: PlacementSubject,
    pub resolved: BTreeMap<Ulid, PolicyResolution>,
    pub start_after: Option<Key>,
    pub limit: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockedGap {
    pub key: String,
    pub reason: PolicyBlockedReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BulkReport {
    pub operation_id: Ulid,
    pub status: PolicyBulkStatus,
    pub generation: u64,
    pub target_refs: Vec<PlacementPolicyRef>,
    pub observed: usize,
    /// Heads this pass needed no successor for: already sealed, a delete
    /// marker, or an intent an earlier pass completed.
    pub covered: usize,
    pub minted: usize,
    /// Intents replanned from a newer head; the next pass mints them.
    pub replanned: usize,
    pub blocked: Vec<BlockedGap>,
    pub cursor: Option<Key>,
    /// True when this pass exhausted the bounded local iterator.
    pub complete: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum BulkError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Successor(#[from] SuccessorError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("the run was sealed against a different bucket record")]
    BucketChanged,
    #[error("unexpected event during the bulk pass")]
    InvalidEvent,
}

/// Runs one bounded pass. Returns the sealed target, what the pass observed and
/// a resumable cursor; blocked objects stay in the run instead of completing.
pub async fn run_policy_bulk(
    context: &DriverContext,
    input: BulkInput,
) -> Result<BulkReport, BulkError> {
    let bucket = read_bucket(context, &input.bucket).await?;
    let run = match read_run(context, input.operation_id).await? {
        Some(run) => run,
        None => seal_run(context, &input, &bucket).await?,
    };
    if run.bucket != input.bucket || run.bucket_identity != bucket.identity() {
        return Err(BulkError::BucketChanged);
    }
    // A default change ends the run: one pass never mixes two policies.
    if run.generation != bucket.placement_policy_generation
        || run.target_refs != bucket.placement_policies
    {
        let superseded = write_status(context, run, PolicyBulkStatus::Superseded).await?;
        return Ok(empty_report(&input, &superseded));
    }
    if run.status != PolicyBulkStatus::Active {
        return Ok(empty_report(&input, &run));
    }

    let mut report = empty_report(&input, &run);
    let page = scan_page(context, &input).await?;
    report.cursor = page.cursor.clone();
    report.complete = page.cursor.is_none();
    for (key, pointer, version) in page.entries {
        report.observed += 1;
        if version.state == BlobVersionState::Deleted || covered(&version, &run.target_refs) {
            report.covered += 1;
            continue;
        }
        apply_object(context, &input, &run, key, pointer, &mut report).await?;
    }

    // Converged only when a full rescan from the start found nothing to do.
    if input.start_after.is_none()
        && report.complete
        && report.minted == 0
        && report.replanned == 0
        && report.blocked.is_empty()
    {
        let completed = write_status(context, run, PolicyBulkStatus::Completed).await?;
        report.status = completed.status;
    }
    Ok(report)
}

fn empty_report(input: &BulkInput, run: &PolicyBulkRun) -> BulkReport {
    BulkReport {
        operation_id: input.operation_id,
        status: run.status,
        generation: run.generation,
        target_refs: run.target_refs.clone(),
        observed: 0,
        covered: 0,
        minted: 0,
        replanned: 0,
        blocked: Vec::new(),
        cursor: None,
        complete: false,
    }
}

fn covered(version: &BlobVersion, target: &[PlacementPolicyRef]) -> bool {
    target
        .iter()
        .all(|policy| version.placement_policies.contains(policy))
}

/// Plans or reuses the object's intent, mints its successor, and records the
/// outcome. A head that moved is replanned from the new head, never advanced
/// from the one the scan saw.
async fn apply_object(
    context: &DriverContext,
    input: &BulkInput,
    run: &PolicyBulkRun,
    key: String,
    pointer: CurrentVersionPointer,
    report: &mut BulkReport,
) -> Result<(), BulkError> {
    let stored = read_intent(context, input.operation_id, &key).await?;
    if let Some(intent) = stored.as_ref()
        && matches!(intent.outcome, PolicyIntentOutcome::Completed { .. })
    {
        report.covered += 1;
        return Ok(());
    }
    let intent = match stored {
        Some(intent) if intent.observed_head == pointer => intent,
        stale => {
            if stale.is_some() {
                report.replanned += 1;
            }
            let planned = plan_intent(input.operation_id, &key, pointer);
            write_intent(context, &planned).await?;
            planned
        }
    };

    let plan = SuccessorPlan {
        context: HeadAliasContext::new(
            input.realm_id,
            input.group_id,
            input.node_id,
            input.bucket.clone(),
            key.clone(),
        ),
        // The preassigned successor is also the mutation identity, so a retried
        // pass replays onto the same version instead of minting another.
        mutation_id: intent.successor_version_id,
        expected_head: intent.observed_head.clone(),
        bucket_identity: run.bucket_identity,
        target_refs: run.target_refs.clone(),
        mode: PolicyRefMode::Union,
        successor_version_id: intent.successor_version_id,
        created_at: SystemTime::now(),
        auth_context: input.auth_context.clone(),
        subject: input.subject.clone(),
        resolved: input.resolved.clone(),
        intent: Some(intent.clone()),
    };

    match drive(MintPolicySuccessorOperation::new(plan), context).await {
        Ok(SuccessorOutcome::Minted { .. }) => {
            report.minted += 1;
            Ok(())
        }
        Ok(SuccessorOutcome::Replayed {
            version_id,
            materialized,
            ..
        }) => {
            let mut receipt = intent;
            receipt.outcome = PolicyIntentOutcome::Completed {
                version_id,
                materialized,
            };
            write_intent(context, &receipt).await?;
            report.covered += 1;
            Ok(())
        }
        Ok(SuccessorOutcome::Blocked(reason)) => {
            let mut blocked = intent;
            blocked.outcome = PolicyIntentOutcome::Blocked(reason);
            write_intent(context, &blocked).await?;
            report.blocked.push(BlockedGap { key, reason });
            Ok(())
        }
        Err(SuccessorError::HeadConflict { current }) => {
            report.replanned += 1;
            if let Some(current) = current {
                write_intent(context, &plan_intent(input.operation_id, &key, current)).await?;
            }
            Ok(())
        }
        // A head that lost its version or became a delete marker is retained as
        // a blocked gap: the pass must not claim it as completed.
        Err(SuccessorError::HeadDeleted) | Err(SuccessorError::VersionMissing) => {
            let mut blocked = intent;
            blocked.outcome = PolicyIntentOutcome::Blocked(PolicyBlockedReason::SourceUnavailable);
            write_intent(context, &blocked).await?;
            report.blocked.push(BlockedGap {
                key,
                reason: PolicyBlockedReason::SourceUnavailable,
            });
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn plan_intent(
    operation_id: Ulid,
    key: &str,
    observed_head: CurrentVersionPointer,
) -> PolicyBulkIntent {
    PolicyBulkIntent {
        operation_id,
        key: key.to_string(),
        observed_head,
        successor_version_id: Ulid::generate(),
        outcome: PolicyIntentOutcome::Planned,
    }
}

struct HeadPage {
    entries: Vec<(String, CurrentVersionPointer, BlobVersion)>,
    cursor: Option<Key>,
}

async fn scan_page(context: &DriverContext, input: &BulkInput) -> Result<HeadPage, BulkError> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: Some(BlobHeadKey::bucket_prefix(&input.bucket)?.into()),
            start: input.start_after.clone().map(IterStart::After),
            limit: input.limit.clamp(1, BULK_PAGE_LIMIT),
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::IterResult {
        values,
        next_start_after,
    }) = event
    else {
        return Err(storage_error(event));
    };

    let mut heads = Vec::with_capacity(values.len());
    let mut reads = Vec::with_capacity(values.len());
    for (key, value) in values {
        let head = BlobHeadKey::from_bytes(key.as_ref())?;
        let pointer = CurrentVersionPointer::from_bytes(value.as_ref())?;
        let version_key = VersionKey::new(&input.bucket, &head.key, pointer.version_id);
        reads.push((
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version_key.to_bytes()?.into(),
        ));
        heads.push((head.key, pointer));
    }
    if heads.is_empty() {
        return Ok(HeadPage {
            entries: Vec::new(),
            cursor: next_start_after,
        });
    }

    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
        return Err(storage_error(event));
    };
    if values.len() != heads.len() {
        return Err(BulkError::InvalidEvent);
    }
    let mut entries = Vec::with_capacity(heads.len());
    for ((key, pointer), (_, value)) in heads.into_iter().zip(values) {
        let Some(value) = value else {
            continue;
        };
        entries.push((key, pointer, BlobVersion::from_bytes(value.as_ref())?));
    }
    Ok(HeadPage {
        entries,
        cursor: next_start_after,
    })
}

async fn read_bucket(context: &DriverContext, bucket: &str) -> Result<BucketInfo, BulkError> {
    let value = read_key(context, S3_BUCKET_KEYSPACE, bucket.as_bytes().to_vec()).await?;
    let Some(value) = value else {
        return Err(BulkError::NoSuchBucket);
    };
    Ok(BucketInfo::from_bytes(value.as_ref())?)
}

async fn read_run(
    context: &DriverContext,
    operation_id: Ulid,
) -> Result<Option<PolicyBulkRun>, BulkError> {
    let value = read_key(
        context,
        POLICY_BULK_RUN_KEYSPACE,
        PolicyBulkRun::key(operation_id)?,
    )
    .await?;
    value
        .map(|value| PolicyBulkRun::from_bytes(value.as_ref()))
        .transpose()
        .map_err(BulkError::from)
}

async fn seal_run(
    context: &DriverContext,
    input: &BulkInput,
    bucket: &BucketInfo,
) -> Result<PolicyBulkRun, BulkError> {
    let run = PolicyBulkRun {
        operation_id: input.operation_id,
        bucket: input.bucket.clone(),
        bucket_identity: bucket.identity(),
        generation: bucket.placement_policy_generation,
        target_refs: bucket.placement_policies.clone(),
        status: PolicyBulkStatus::Active,
    };
    write_run(context, &run).await?;
    Ok(run)
}

async fn write_status(
    context: &DriverContext,
    mut run: PolicyBulkRun,
    status: PolicyBulkStatus,
) -> Result<PolicyBulkRun, BulkError> {
    run.status = status;
    write_run(context, &run).await?;
    Ok(run)
}

async fn write_run(context: &DriverContext, run: &PolicyBulkRun) -> Result<(), BulkError> {
    write_key(
        context,
        POLICY_BULK_RUN_KEYSPACE,
        PolicyBulkRun::key(run.operation_id)?,
        run.to_bytes()?,
    )
    .await
}

async fn read_intent(
    context: &DriverContext,
    operation_id: Ulid,
    key: &str,
) -> Result<Option<PolicyBulkIntent>, BulkError> {
    let value = read_key(
        context,
        POLICY_BULK_INTENT_KEYSPACE,
        PolicyBulkIntentKey::new(operation_id, key).to_bytes()?,
    )
    .await?;
    value
        .map(|value| PolicyBulkIntent::from_bytes(value.as_ref()))
        .transpose()
        .map_err(BulkError::from)
}

async fn write_intent(context: &DriverContext, intent: &PolicyBulkIntent) -> Result<(), BulkError> {
    write_key(
        context,
        POLICY_BULK_INTENT_KEYSPACE,
        intent.key().to_bytes()?,
        intent.to_bytes()?,
    )
    .await
}

async fn read_key(
    context: &DriverContext,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<aruna_core::types::Value>, BulkError> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        other => Err(storage_error(other)),
    }
}

async fn write_key(
    context: &DriverContext,
    key_space: &str,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), BulkError> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(storage_error(other)),
    }
}

fn storage_error(event: Event) -> BulkError {
    match event {
        Event::Storage(StorageEvent::Error { error }) => BulkError::Storage(error),
        _ => BulkError::InvalidEvent,
    }
}

/// Exit gate for step 6: a default applies to current heads by minting
/// successors, never by rewriting a stored version, and a run that cannot act
/// keeps the object as a resumable gap.
#[cfg(test)]
mod tests {
    use super::{BULK_PAGE_LIMIT, BulkInput, run_policy_bulk};
    use crate::driver::{DriverContext, drive, gate_context};
    use crate::placement_policy::fixtures::{seed_gate, subject};
    use crate::s3::bucket_placement::{PutBucketPlacementInput, PutBucketPlacementOperation};
    use crate::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, MANAGED_COPY_KEYSPACE, S3_BUCKET_KEYSPACE,
    };
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{
        AuthContext, Backend, BackendConfig, BackendRef, BlobHeadKey, BlobVersion, BucketInfo,
        CurrentVersionPointer, ManagedCopyKey, ManagedCopyRecord, POLICY_BULK_INTENT_KEYSPACE,
        PlacementPolicy, PlacementPolicyRef, PlacementSelector, PlacementSubject,
        PolicyBlockedReason, PolicyBulkIntent, PolicyBulkIntentKey, PolicyBulkStatus,
        PolicyIntentOutcome, PolicyResolution, RealmId, RoutingSnapshot, VerifiedPolicy,
        VersionKey,
    };
    use aruna_core::types::{GroupId, NodeId, UserId};
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use std::collections::{BTreeMap, HashMap};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    const BUCKET: &str = "governed";
    const OBJECT: &str = "object.txt";
    const BODY: &[u8] = b"payload";

    struct Fixture {
        realm_id: RealmId,
        group_id: GroupId,
        node_id: NodeId,
        user_id: UserId,
    }

    async fn full_context() -> (TempDir, DriverContext, Fixture) {
        let temp_handle = tempdir().expect("temp dir");
        let temp_root = temp_handle.path().to_str().expect("utf8 path");
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).expect("blob root");
        let storage_handle = storage::FjallStorage::open(temp_root).expect("storage opens");
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .expect("net handle");
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100_000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root,
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .expect("blob handle");
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let fixture = Fixture {
            realm_id,
            group_id: Ulid::generate(),
            node_id: net_handle.node_id(),
            user_id: UserId::local(Ulid::generate(), realm_id),
        };
        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        // The node advertises a subject and has already resolved the rule these
        // tests attach, which is what production wiring establishes at startup.
        seed_gate(
            &context,
            realm_id,
            subject(fixture.node_id, "eu-west"),
            &[policy(fixture.node_id)],
        )
        .await;
        (temp_handle, context, fixture)
    }

    async fn write_bucket(context: &DriverContext, fixture: &Fixture) {
        let bucket = BucketInfo {
            group_id: fixture.group_id,
            created_at: UNIX_EPOCH,
            created_by: fixture.user_id,
            cors_configuration: None,
            replication: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        };
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: BUCKET.as_bytes().to_vec().into(),
                value: bucket.to_bytes().expect("bucket encodes").into(),
                txn_id: None,
            })
            .await;
    }

    async fn put_object(context: &DriverContext, fixture: &Fixture, key: &str) -> Ulid {
        let gate = gate_context(context, fixture.realm_id, 1_000)
            .await
            .expect("subject reads");
        let mut operation = PutObjectOperation::new(PutObjectConfig {
            user_id: fixture.user_id,
            group_id: fixture.group_id,
            realm_id: fixture.realm_id,
            node_id: fixture.node_id,
            request: PutObjectInput {
                bucket: BUCKET.to_string(),
                key: key.to_string(),
                content_length: Some(BODY.len() as u64),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(BODY))),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(fixture.group_id),
        });
        if let Some(gate) = gate {
            operation = operation.with_gate(gate);
        }
        drive(operation, context)
            .await
            .expect("put drives")
            .expect("put succeeds")
            .expect("put returns a result")
            .version_id
    }

    fn policy(node_id: NodeId) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([5u8; 16]),
            "residency".to_string(),
            vec![PlacementSelector {
                node_id: Some(node_id),
                location: None,
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    async fn set_default(
        context: &DriverContext,
        fixture: &Fixture,
        refs: Vec<PlacementPolicyRef>,
    ) {
        drive(
            PutBucketPlacementOperation::new(PutBucketPlacementInput {
                bucket: BUCKET.to_string(),
                group_id: fixture.group_id,
                policies: refs,
                expected_generation: None,
            }),
            context,
        )
        .await
        .expect("default is set");
    }

    fn bulk_input(
        fixture: &Fixture,
        resolved: BTreeMap<Ulid, PolicyResolution>,
        operation_id: Ulid,
    ) -> BulkInput {
        BulkInput {
            operation_id,
            bucket: BUCKET.to_string(),
            realm_id: fixture.realm_id,
            group_id: fixture.group_id,
            node_id: fixture.node_id,
            auth_context: AuthContext {
                user_id: fixture.user_id,
                realm_id: fixture.realm_id,
                path_restrictions: None,
            },
            subject: PlacementSubject {
                node_id: fixture.node_id,
                generation: 1,
                location: "eu-west".to_string(),
                labels: BTreeMap::new(),
                executor_kind: None,
                local_to_controller: true,
            },
            resolved,
            start_after: None,
            limit: BULK_PAGE_LIMIT,
        }
    }

    async fn read_head(context: &DriverContext, key: &str) -> CurrentVersionPointer {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(BUCKET, key)
                    .to_bytes()
                    .expect("key encodes")
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };
        CurrentVersionPointer::from_bytes(value.expect("head exists").as_ref())
            .expect("pointer decodes")
    }

    async fn read_version(context: &DriverContext, key: &str, version_id: Ulid) -> BlobVersion {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(BUCKET, key, version_id)
                    .to_bytes()
                    .expect("key encodes")
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };
        BlobVersion::from_bytes(value.expect("version exists").as_ref()).expect("version decodes")
    }

    async fn read_copy(
        context: &DriverContext,
        key: &str,
        version_id: Ulid,
    ) -> Option<ManagedCopyRecord> {
        let managed_key = ManagedCopyKey::new(
            VersionKey::new(BUCKET, key, version_id),
            BackendRef::node_default(),
        );
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: MANAGED_COPY_KEYSPACE.to_string(),
                key: managed_key.to_bytes().expect("key encodes").into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };
        value.map(|value| ManagedCopyRecord::from_bytes(value.as_ref()).expect("record decodes"))
    }

    async fn read_intent(
        context: &DriverContext,
        operation_id: Ulid,
        key: &str,
    ) -> Option<PolicyBulkIntent> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: POLICY_BULK_INTENT_KEYSPACE.to_string(),
                key: PolicyBulkIntentKey::new(operation_id, key)
                    .to_bytes()
                    .expect("key encodes")
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };
        value.map(|value| PolicyBulkIntent::from_bytes(value.as_ref()).expect("intent decodes"))
    }

    #[tokio::test]
    async fn put_snapshots_default() {
        // The version seals the default observed in its own transaction.
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;

        let version_id = put_object(&context, &fixture, OBJECT).await;

        let version = read_version(&context, OBJECT, version_id).await;
        assert_eq!(version.placement_policies, vec![policy.policy_ref()]);
        let copy = read_copy(&context, OBJECT, version_id)
            .await
            .expect("copy row");
        assert_eq!(copy.policies, vec![policy.policy_ref()]);
    }

    #[tokio::test]
    async fn put_stays_ungoverned() {
        // A bucket without a default must write exactly what it wrote before.
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;

        let version_id = put_object(&context, &fixture, OBJECT).await;

        assert!(
            read_version(&context, OBJECT, version_id)
                .await
                .placement_policies
                .is_empty()
        );
        assert!(
            read_copy(&context, OBJECT, version_id)
                .await
                .expect("copy row")
                .policies
                .is_empty()
        );
    }

    #[tokio::test]
    async fn bulk_mints_successor() {
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        let predecessor = put_object(&context, &fixture, OBJECT).await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;
        let resolved = BTreeMap::from([(
            policy.policy().policy_id,
            PolicyResolution::Known(policy.clone()),
        )]);
        let operation_id = Ulid::generate();

        let report = run_policy_bulk(
            &context,
            bulk_input(&fixture, resolved.clone(), operation_id),
        )
        .await
        .expect("pass runs");

        assert_eq!(report.minted, 1);
        assert!(report.blocked.is_empty());
        let head = read_head(&context, OBJECT).await;
        assert_ne!(head.version_id, predecessor);
        let successor = read_version(&context, OBJECT, head.version_id).await;
        assert_eq!(successor.placement_policies, vec![policy.policy_ref()]);
        assert_eq!(
            successor.state,
            read_version(&context, OBJECT, predecessor).await.state
        );
        // The predecessor keeps its own refs until retention retires it.
        assert!(
            read_version(&context, OBJECT, predecessor)
                .await
                .placement_policies
                .is_empty()
        );
        assert!(read_copy(&context, OBJECT, head.version_id).await.is_some());
        assert!(matches!(
            read_intent(&context, operation_id, OBJECT).await,
            Some(PolicyBulkIntent {
                outcome: PolicyIntentOutcome::Completed { .. },
                ..
            })
        ));

        // A second pass observes no gap and converges.
        let second = run_policy_bulk(&context, bulk_input(&fixture, resolved, operation_id))
            .await
            .expect("second pass runs");
        assert_eq!(second.minted, 0);
        assert_eq!(second.covered, 1);
        assert_eq!(second.status, PolicyBulkStatus::Completed);
    }

    #[tokio::test]
    async fn bulk_blocks_unresolved() {
        // Without the policy document the object stays a resumable gap.
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        let predecessor = put_object(&context, &fixture, OBJECT).await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;
        let operation_id = Ulid::generate();

        let report = run_policy_bulk(
            &context,
            bulk_input(&fixture, BTreeMap::new(), operation_id),
        )
        .await
        .expect("pass runs");

        assert_eq!(report.minted, 0);
        assert_eq!(
            report.blocked.first().map(|gap| gap.reason),
            Some(PolicyBlockedReason::PolicyUnresolved)
        );
        assert_eq!(read_head(&context, OBJECT).await.version_id, predecessor);
        assert_eq!(report.status, PolicyBulkStatus::Active);
    }

    #[tokio::test]
    async fn bulk_stops_superseded() {
        // A changed default supersedes the run instead of mixing two policies.
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        put_object(&context, &fixture, OBJECT).await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;
        let operation_id = Ulid::generate();
        run_policy_bulk(
            &context,
            bulk_input(&fixture, BTreeMap::new(), operation_id),
        )
        .await
        .expect("first pass runs");

        set_default(&context, &fixture, Vec::new()).await;
        let report = run_policy_bulk(
            &context,
            bulk_input(&fixture, BTreeMap::new(), operation_id),
        )
        .await
        .expect("second pass runs");

        assert_eq!(report.status, PolicyBulkStatus::Superseded);
        assert_eq!(report.observed, 0);
    }

    #[tokio::test]
    async fn bulk_reuses_successor() {
        // Repeating a pass must reuse the assigned VersionId, not mint another.
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        put_object(&context, &fixture, OBJECT).await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;
        let resolved = BTreeMap::from([(
            policy.policy().policy_id,
            PolicyResolution::Known(policy.clone()),
        )]);
        let operation_id = Ulid::generate();
        run_policy_bulk(
            &context,
            bulk_input(&fixture, resolved.clone(), operation_id),
        )
        .await
        .expect("first pass runs");
        let successor = read_head(&context, OBJECT).await;

        let report = run_policy_bulk(&context, bulk_input(&fixture, resolved, operation_id))
            .await
            .expect("second pass runs");

        assert_eq!(report.minted, 0);
        assert_eq!(read_head(&context, OBJECT).await, successor);
    }

    #[tokio::test]
    async fn bulk_unions_existing() {
        // Applying a default may only add refs to the head it re-reads.
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        let first = policy(fixture.node_id);
        set_default(&context, &fixture, vec![first.policy_ref()]).await;
        put_object(&context, &fixture, OBJECT).await;

        let second = VerifiedPolicy::verify(
            PlacementPolicy::new(
                Ulid::from_bytes([6u8; 16]),
                "second".to_string(),
                vec![PlacementSelector {
                    node_id: Some(fixture.node_id),
                    location: None,
                    labels: Vec::new(),
                    executor_kind: None,
                }],
            )
            .expect("policy is valid"),
        )
        .expect("policy verifies");
        set_default(&context, &fixture, vec![second.policy_ref()]).await;
        let resolved = BTreeMap::from([
            (
                first.policy().policy_id,
                PolicyResolution::Known(first.clone()),
            ),
            (
                second.policy().policy_id,
                PolicyResolution::Known(second.clone()),
            ),
        ]);

        let report = run_policy_bulk(&context, bulk_input(&fixture, resolved, Ulid::generate()))
            .await
            .expect("pass runs");

        assert_eq!(report.minted, 1);
        let head = read_head(&context, OBJECT).await;
        let successor = read_version(&context, OBJECT, head.version_id).await;
        assert!(successor.placement_policies.contains(&first.policy_ref()));
        assert!(successor.placement_policies.contains(&second.policy_ref()));
    }

    #[tokio::test]
    async fn bulk_skips_covered() {
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;
        let version_id = put_object(&context, &fixture, OBJECT).await;
        let resolved = BTreeMap::from([(
            policy.policy().policy_id,
            PolicyResolution::Known(policy.clone()),
        )]);

        let report = run_policy_bulk(&context, bulk_input(&fixture, resolved, Ulid::generate()))
            .await
            .expect("pass runs");

        assert_eq!(report.covered, 1);
        assert_eq!(report.minted, 0);
        assert_eq!(read_head(&context, OBJECT).await.version_id, version_id);
        assert_eq!(report.status, PolicyBulkStatus::Completed);
    }

    #[tokio::test]
    async fn successor_is_fresh() {
        let (_temp, context, fixture) = full_context().await;
        write_bucket(&context, &fixture).await;
        let predecessor = put_object(&context, &fixture, OBJECT).await;
        let policy = policy(fixture.node_id);
        set_default(&context, &fixture, vec![policy.policy_ref()]).await;
        let resolved = BTreeMap::from([(
            policy.policy().policy_id,
            PolicyResolution::Known(policy.clone()),
        )]);

        run_policy_bulk(&context, bulk_input(&fixture, resolved, Ulid::generate()))
            .await
            .expect("pass runs");

        let head = read_head(&context, OBJECT).await;
        let successor = read_version(&context, OBJECT, head.version_id).await;
        let before = read_version(&context, OBJECT, predecessor).await;
        assert!(successor.created_at >= before.created_at);
        assert!(successor.created_at <= SystemTime::now());
    }
}
