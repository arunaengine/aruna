//! Plans the rows of one imported listing page: guard reads and checks, entry plans, writes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::repository::{connector_reference_write, source_connector_key};
use crate::distribution::guard::GuardError;
use crate::distribution::inventory::{
    AddedVersion, InventoryChange, entry_of, stamp_head, stamp_version, version_written,
};
use crate::jobs::store::decode_job_record;
use crate::node::usage_stats::UsageUpdateError;
use crate::replication::queue::{LiveObligationRecord, live_obligation_entry};
use crate::s3::purge_fence::{PurgeFenceError, check_decoded_fence, fence_key};
use crate::staging::descriptor::build_source_binding;
use crate::storage_read::StorageReadError;
use aruna_core::UserId;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, JOB_KEYSPACE, PURGE_FENCE_KEYSPACE,
    S3_BUCKET_KEYSPACE, SOURCE_INDEX_KEYSPACE, STAGING_STATE_KEYSPACE,
};
use aruna_core::structs::execution::job::{
    JobId, StagingJobCheckpoint, StagingJobError, job_record_key,
};
use aruna_core::structs::execution::source_access::{ObservationMatch, SourceMetadata};
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::execution::staging::StagingStrategy;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::{PlacementPolicyError, PlacementPolicyRef};
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::distribution::hint::{SetHint, mint_version};
use aruna_core::structs::storage::distribution::{DistributionBinding, OriginalEpoch};
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_core::structs::storage::storage_purge::StoragePurgeFence;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{GroupId, Key, KeySpace, Value};
use byteview::ByteView;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

/// Most entries one page registers.
pub const MAX_PAGE_ENTRIES: usize = 1000;
/// Most bytes one page writes, keys and values together.
pub const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;
/// Most per-entry errors a staging checkpoint keeps; the failed counter counts all of them.
pub const MAX_ERROR_SAMPLE: usize = 100;
/// Rows read ahead of the heads: purge fence, bucket, connector and job record.
pub(crate) const GUARD_READS: usize = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageEntry {
    pub source_path: String,
    pub target_key: String,
    pub observation: SourceMetadata,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PageImportInput {
    pub job_id: JobId,
    pub claim_token: Ulid,
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub user_id: UserId,
    /// The submitter, under whom the page's live replication obligations run.
    pub auth_context: AuthContext,
    pub node_id: NodeId,
    pub bucket: String,
    pub expected_bucket: BucketInfo,
    /// The connector as resolved for the listing, re-checked at commit.
    pub connector: SourceConnector,
    pub secret_fingerprint: Option<[u8; 16]>,
    pub entries: Vec<PageEntry>,
    /// The checkpoint after this page; the page's outcome is added before it is written.
    pub checkpoint: StagingJobCheckpoint,
    /// Further job rows committed with the page, such as directory queue changes.
    pub writes: Vec<(KeySpace, Key, Value)>,
    pub deletes: Vec<(KeySpace, Key)>,
    pub now: SystemTime,
    /// The bucket's database list entry at registration, kept in every version's origin.
    pub database: Option<Ulid>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PageImportResult {
    /// New reference versions.
    pub registered: u64,
    /// Keys whose head already held this observation of the same source.
    pub unchanged: u64,
    /// Keys left alone because a write newer than the job holds their head.
    pub kept: u64,
    pub checkpoint: StagingJobCheckpoint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PageImportState {
    Init,
    StartTransaction,
    ReadGuards,
    /// Reads a distributed bucket's record: every entry's key must be coordinated here.
    ReadRecord,
    ReadSecret,
    ReadVersions,
    WriteRows,
    DeleteRows,
    UpdateUsage,
    Commit,
    Schedule,
    ScheduleDrain,
    Abort,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PageImportError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Purge(#[from] PurgeFenceError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
    #[error(transparent)]
    Distribution(#[from] GuardError),
    #[error("the destination bucket changed after the job was submitted")]
    BucketChanged,
    #[error("the connector changed while the page was imported")]
    ConnectorChanged,
    #[error("the job no longer holds its claim")]
    ClaimLost,
    #[error("a page holds at most {MAX_PAGE_ENTRIES} entries, got {0}")]
    TooManyEntries(usize),
    #[error("a page writes at most {MAX_PAGE_BYTES} bytes, got {0}")]
    TooManyBytes(usize),
    #[error("unexpected event in state {state:?}: expected {expected}, got {received}")]
    UnexpectedEvent {
        state: PageImportState,
        expected: &'static str,
        received: String,
    },
    #[error("page import finished without output")]
    NotFinished,
}

impl From<StorageReadError> for PageImportError {
    fn from(error: StorageReadError) -> Self {
        match error {
            StorageReadError::Storage(error) => Self::Storage(error),
            StorageReadError::Conversion(error) => Self::Conversion(error),
        }
    }
}

/// What the guard reads established for the page's writes.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PageGuards {
    pub policies: Vec<PlacementPolicyRef>,
    pub job_created_ms: u64,
    pub heads: Vec<Option<CurrentVersionPointer>>,
    /// The set hint of a distributed bucket; ordinary buckets mint plain ULIDs.
    pub set_hint: Option<SetHint>,
    pub binding: Option<DistributionBinding>,
    /// The coordinator epoch of each entry's key in a distributed bucket, in entry order.
    pub epochs: Vec<OriginalEpoch>,
}

/// The rows of one page, its usage change and its outcome.
pub(crate) struct PageRows {
    pub writes: Vec<(KeySpace, Key, Value)>,
    pub delta: UsageDelta,
    pub result: PageImportResult,
    /// The inventory changes of a distributed bucket, one per registered key.
    pub inventory: Vec<InventoryChange>,
}

/// What the page does with one entry, decided against the head inside the transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EntryPlan {
    Register,
    Unchanged,
    Kept,
}

pub(crate) fn guard_reads(
    input: &PageImportInput,
) -> Result<Vec<(KeySpace, Key)>, ConversionError> {
    let connector = &input.connector;
    let mut reads = vec![
        (PURGE_FENCE_KEYSPACE.to_string(), fence_key(&input.bucket)),
        (
            S3_BUCKET_KEYSPACE.to_string(),
            ByteView::from(input.bucket.as_bytes().to_vec()),
        ),
        (
            SOURCE_INDEX_KEYSPACE.to_string(),
            source_connector_key(connector.group_id, connector.connector_id),
        ),
        (JOB_KEYSPACE.to_string(), job_record_key(input.job_id)),
    ];
    for entry in &input.entries {
        let head = BlobHeadKey::new(&input.bucket, &entry.target_key).to_bytes()?;
        reads.push((BLOB_HEAD_KEYSPACE.to_string(), head.into()));
    }
    Ok(reads)
}

/// Checks every guard of a single reference write: purge fence, bucket incarnation,
/// connector and job claim. Placement defaults come from the bucket row read here.
pub(crate) fn check_guards(
    input: &PageImportInput,
    values: Vec<(Key, Option<Value>)>,
) -> Result<PageGuards, PageImportError> {
    if values.len() != GUARD_READS + input.entries.len() {
        return Err(StorageError::ReadError("short guard read".to_string()).into());
    }
    let mut values = values.into_iter().map(|(_, value)| value);
    let fence = values
        .next()
        .flatten()
        .map(|value| StoragePurgeFence::from_bytes(value.as_ref()))
        .transpose()
        .map_err(|_| PurgeFenceError::Invalid)?;
    for entry in &input.entries {
        check_decoded_fence(fence.as_ref(), &input.bucket, &entry.target_key)?;
    }
    let bucket = values
        .next()
        .flatten()
        .map(|value| BucketInfo::from_bytes(value.as_ref()))
        .transpose()?;
    let expected = &input.expected_bucket;
    let (policies, binding) = match bucket {
        Some(bucket)
            if expected.group_id == input.group_id && bucket.identity() == expected.identity() =>
        {
            (bucket.placement_policies, bucket.distribution)
        }
        _ => return Err(PageImportError::BucketChanged),
    };
    let connector = values
        .next()
        .flatten()
        .map(|value| SourceConnector::from_bytes(value.as_ref()))
        .transpose()?;
    if connector.as_ref() != Some(&input.connector) {
        return Err(PageImportError::ConnectorChanged);
    }
    let record = values
        .next()
        .flatten()
        .map(|value| decode_job_record(value.as_ref()))
        .transpose()?
        .ok_or(PageImportError::ClaimLost)?;
    let claimed = record
        .claim
        .as_ref()
        .is_some_and(|claim| claim.claim_token == input.claim_token);
    if !claimed || record.is_settled() {
        return Err(PageImportError::ClaimLost);
    }
    let heads = values
        .map(|value| {
            value
                .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
                .transpose()
        })
        .collect::<Result<_, _>>()?;
    Ok(PageGuards {
        policies,
        job_created_ms: record.created_at_ms,
        heads,
        set_hint: binding.as_ref().map(DistributionBinding::hint),
        binding,
        epochs: Vec::new(),
    })
}

/// Reads of the head versions, in entry order, skipping keys without a head.
pub(crate) fn version_reads(
    input: &PageImportInput,
    heads: &[Option<CurrentVersionPointer>],
) -> Result<Vec<(KeySpace, Key)>, ConversionError> {
    let mut reads = Vec::new();
    for (entry, head) in input.entries.iter().zip(heads) {
        if let Some(head) = head {
            let key = VersionKey::new(&input.bucket, &entry.target_key, head.version_id);
            reads.push((BLOB_VERSIONS_KEYSPACE.to_string(), key.to_bytes()?.into()));
        }
    }
    Ok(reads)
}

/// A head of this connector's file with the same observation is kept as it is. Any other head
/// written after the job started, the same file's included, is newer than the listing.
fn plan_entry(
    input: &PageImportInput,
    job_created_ms: u64,
    entry: &PageEntry,
    head: Option<&CurrentVersionPointer>,
    version: Option<&BlobVersion>,
) -> EntryPlan {
    let (Some(head), Some(version)) = (head, version) else {
        return EntryPlan::Register;
    };
    if let BlobVersionState::Reference {
        source,
        cached_metadata,
        ..
    } = &version.state
        && source.connector_id == Some(input.connector.connector_id)
        && source.descriptor.source_path == entry.source_path
        && cached_metadata.compare(&entry.observation) == ObservationMatch::Same
    {
        return EntryPlan::Unchanged;
    }
    match head.version_id.timestamp_ms() >= job_created_ms {
        true => EntryPlan::Kept,
        false => EntryPlan::Register,
    }
}

/// Builds the page's rows: versions, heads and connector reference rows of new entries,
/// the checkpoint with the page's outcome, and the caller's own job rows.
pub(crate) fn page_rows(
    input: &PageImportInput,
    guards: &PageGuards,
    versions: Vec<Option<BlobVersion>>,
) -> Result<PageRows, PageImportError> {
    let mut versions = versions.into_iter();
    let mut writes = Vec::new();
    let mut inventory = Vec::new();
    let mut delta = UsageDelta::default();
    let mut result = PageImportResult {
        registered: 0,
        unchanged: 0,
        kept: 0,
        checkpoint: input.checkpoint.clone(),
    };
    for (index, (entry, head)) in input.entries.iter().zip(&guards.heads).enumerate() {
        let existing = head.as_ref().and_then(|_| versions.next().flatten());
        let plan = plan_entry(
            input,
            guards.job_created_ms,
            entry,
            head.as_ref(),
            existing.as_ref(),
        );
        match plan {
            EntryPlan::Unchanged => result.unchanged += 1,
            EntryPlan::Kept => {
                result.kept += 1;
                if result.checkpoint.errors.len() < MAX_ERROR_SAMPLE {
                    result.checkpoint.errors.push(StagingJobError {
                        source_path: entry.source_path.clone(),
                        target_key: entry.target_key.clone(),
                        error: "a write newer than the import holds this key".to_string(),
                    });
                }
            }
            EntryPlan::Register => {
                let epoch = guards.epochs.get(index).copied().unwrap_or_default();
                let (rows, change) = register_rows(input, guards, (entry, epoch), head.as_ref())?;
                writes.extend(rows);
                inventory.extend(change);
                // An offered directory observation is charged only while it is the head.
                let released = existing
                    .as_ref()
                    .and_then(|version| version.offered_size(&input.bucket, &entry.target_key));
                let was_live = existing
                    .as_ref()
                    .is_some_and(|version| !version.is_deleted());
                delta.objects += i128::from(!was_live);
                delta.referenced_bytes += i128::from(entry.observation.content_length)
                    - i128::from(released.unwrap_or(0));
                result.registered += 1;
            }
        }
    }
    let checkpoint = &mut result.checkpoint;
    checkpoint.items_current = checkpoint
        .items_current
        .saturating_add(input.entries.len() as u64);
    checkpoint.items_succeeded = checkpoint
        .items_succeeded
        .saturating_add(result.registered + result.unchanged);
    checkpoint.items_failed = checkpoint.items_failed.saturating_add(result.kept);
    writes.push((
        STAGING_STATE_KEYSPACE.to_string(),
        ByteView::from(input.job_id.to_bytes().to_vec()),
        ByteView::from(postcard::to_allocvec(checkpoint).map_err(ConversionError::from)?),
    ));
    writes.extend(input.writes.iter().cloned());
    let bytes: usize = writes
        .iter()
        .map(|(_, key, value)| key.len() + value.len())
        .sum();
    if bytes > MAX_PAGE_BYTES {
        return Err(PageImportError::TooManyBytes(bytes));
    }
    Ok(PageRows {
        writes,
        delta,
        result,
        inventory,
    })
}

/// The rows that register one entry and its inventory change in a distributed bucket.
type EntryRows = (Vec<(KeySpace, Key, Value)>, Option<InventoryChange>);

fn register_rows(
    input: &PageImportInput,
    guards: &PageGuards,
    (entry, epoch): (&PageEntry, OriginalEpoch),
    head: Option<&CurrentVersionPointer>,
) -> Result<EntryRows, PageImportError> {
    let connector = &input.connector;
    let binding = build_source_binding(
        StagingStrategy::Reference,
        connector,
        &entry.observation,
        entry.source_path.clone(),
        Some(input.node_id),
        Some(connector.connector_id),
    );
    let version_id = mint_version(guards.set_hint);
    let version = BlobVersion::reference(
        binding,
        entry.observation.clone(),
        input.now,
        input.user_id,
        input.now,
    )
    .with_origin(Some(VersionOrigin::Source {
        database: input.database,
    }))
    .with_policies(guards.policies.clone())?;
    let version_key = VersionKey::new(&input.bucket, &entry.target_key, version_id);
    let pointer = CurrentVersionPointer::next_for(head, version_id)?;
    let pointer = stamp_head(pointer, guards.binding.as_ref(), epoch);
    let version = stamp_version(version, guards.binding.as_ref(), pointer.generation, epoch);
    let added = AddedVersion {
        version_id,
        epoch: version.original_epoch,
        generation: pointer.generation,
        entry: entry_of(&version, entry.observation.content_length),
    };
    let change = version_written(guards.binding.as_ref(), &entry.target_key, added, true);
    let head_key = BlobHeadKey::new(&input.bucket, &entry.target_key);
    let mut rows = Vec::with_capacity(4);
    rows.extend(connector_reference_write(&version, &version_key)?);
    rows.push((
        BLOB_VERSIONS_KEYSPACE.to_string(),
        version_key.to_bytes()?.into(),
        version.to_bytes()?.into(),
    ));
    rows.push((
        BLOB_HEAD_KEYSPACE.to_string(),
        head_key.to_bytes()?.into(),
        pointer.to_bytes()?.into(),
    ));
    // The durable obligation commits with the version, so sync targets learn of it even when
    // the drain is not reached.
    let obligation = LiveObligationRecord::new(
        input.node_id,
        input.auth_context.clone(),
        input.bucket.clone(),
        entry.target_key.clone(),
        version_id,
        false,
    );
    rows.push(live_obligation_entry(&obligation)?);
    Ok((rows, change))
}
