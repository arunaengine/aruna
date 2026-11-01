//! Plans the rows of one imported listing page: guard reads and checks, entry plans, writes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::repository::{connector_reference_write, source_connector_key};
use crate::distribution::guard::GuardError;
use crate::distribution::inventory::{
    AddedVersion, InventoryChange, entry_of, stamp_head, stamp_version, version_written,
};
use crate::jobs::store::ClaimGuard;
use crate::node::usage_stats::UsageUpdateError;
use crate::replication::queue::{LiveObligationRecord, live_obligation_entry};
use crate::s3::purge_fence::{PurgeFenceError, check_decoded_fence, fence_key};
use crate::staging::descriptor::build_source_binding;
use crate::staging::root_record::{compact_version, root_write};
use crate::storage_read::StorageReadError;
use aruna_core::UserId;
use aruna_core::effects::{IterEnd, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, JOB_CLAIM_KEYSPACE, PURGE_FENCE_KEYSPACE,
    S3_BUCKET_KEYSPACE, SOURCE_INDEX_KEYSPACE, STAGING_STATE_KEYSPACE,
};
use aruna_core::structs::execution::job::{
    JobId, StagingJobCheckpoint, StagingJobError, job_record_key,
};
use aruna_core::structs::execution::source_access::{ObservationMatch, SourceMetadata};
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::execution::staging::{
    SourceMirror, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::{PlacementPolicyError, PlacementPolicyRef};
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::distribution::hint::{SetHint, mint_version};
use aruna_core::structs::storage::distribution::source_root::{RootEntry, SourceRoot};
use aruna_core::structs::storage::distribution::{DistributionBinding, OriginalEpoch};
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_core::structs::storage::storage_purge::StoragePurgeFence;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{GroupId, Key, KeySpace, TxnId, Value};
use byteview::ByteView;
use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

/// Most entries one page registers; a lane gathers several listings into one page.
pub const MAX_PAGE_ENTRIES: usize = 10_000;
/// Most bytes one page writes, keys and values together.
pub const MAX_PAGE_BYTES: usize = 32 * 1024 * 1024;
/// Most per-entry errors a staging checkpoint keeps; the failed counter counts all of them.
pub const MAX_ERROR_SAMPLE: usize = 100;
/// Rows read ahead of the heads: purge fence, bucket, connector and job claim.
pub(crate) const GUARD_READS: usize = 4;
/// Rows of other keys the head read may meet between the page's keys before the page reads its
/// heads one by one instead.
const HEAD_SLACK: usize = 1000;
/// Most bytes the head read takes.
const HEAD_READ_BYTES: usize = 4 << 20;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageEntry {
    pub source_path: String,
    pub target_key: String,
    pub observation: SourceMetadata,
    /// Metadata the version carries, such as the columns of a table row.
    pub metadata: BTreeMap<String, String>,
    /// Further sources of the same file, equal to the connector's.
    pub mirrors: Vec<SourceMirror>,
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
    /// The connectors of the entries' further sources, re-checked at commit as well.
    pub mirror_connectors: Vec<SourceConnector>,
    pub secret_fingerprint: Option<[u8; 16]>,
    pub entries: Vec<PageEntry>,
    /// The checkpoint after this page; the page's outcome is added before it is written.
    pub checkpoint: StagingJobCheckpoint,
    /// The row the checkpoint is written to: the job's own, or one prefix lane's.
    pub checkpoint_key: Key,
    /// Further job rows committed with the page, such as directory queue changes.
    pub writes: Vec<(KeySpace, Key, Value)>,
    pub deletes: Vec<(KeySpace, Key)>,
    pub now: SystemTime,
    /// The ranges whose turns the page holds; a page whose keys lie in others ends unwritten.
    pub ranges: Option<BTreeSet<Ulid>>,
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
    /// Reads the heads in the key interval of the page, which proves absent keys absent.
    ReadHeads,
    /// Reads the heads key by key, when the interval holds too many other keys.
    ReadHeadKeys,
    /// Reads a distributed bucket's record: every entry's key must be coordinated here.
    ReadRecord,
    /// Reads whether an ordinary bucket has a sync relationship that replicates its versions.
    ReadRelationships,
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
    #[error("the page's keys lie in other ranges than the turns it holds")]
    RangesMoved(BTreeSet<Ulid>),
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
    /// Whether anything replicates the bucket's versions: set members or sync relationships.
    pub downstream: bool,
    /// Whether a distributed bucket keeps a search index, which the new heads feed.
    pub search: bool,
}

/// The rows of one page, its usage change and its outcome.
#[derive(Debug, PartialEq)]
pub(crate) struct PageRows {
    pub writes: Vec<(KeySpace, Key, Value)>,
    pub delta: UsageDelta,
    pub result: PageImportResult,
    /// The inventory changes of a distributed bucket, one per registered key.
    pub inventory: Vec<InventoryChange>,
}

/// A page's rows built in a read snapshot before its turn, with the guards, heads and versions
/// they were built from; the page writes them only if its transaction reads the same.
#[derive(Debug, PartialEq)]
pub struct PreparedRows {
    pub(crate) guards: PageGuards,
    pub(crate) versions: Vec<Option<BlobVersion>>,
    pub(crate) rows: PageRows,
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
    let reads = vec![
        (PURGE_FENCE_KEYSPACE.to_string(), fence_key(&input.bucket)),
        (
            S3_BUCKET_KEYSPACE.to_string(),
            ByteView::from(input.bucket.as_bytes().to_vec()),
        ),
        (
            SOURCE_INDEX_KEYSPACE.to_string(),
            source_connector_key(connector.group_id, connector.connector_id),
        ),
        (JOB_CLAIM_KEYSPACE.to_string(), job_record_key(input.job_id)),
    ];
    let mirrors = (input.mirror_connectors.iter()).map(|connector| {
        let key = source_connector_key(connector.group_id, connector.connector_id);
        (SOURCE_INDEX_KEYSPACE.to_string(), key)
    });
    Ok(reads.into_iter().chain(mirrors).collect())
}

fn head_keys(input: &PageImportInput) -> Result<Vec<Vec<u8>>, ConversionError> {
    (input.entries.iter())
        .map(|entry| BlobHeadKey::new(&input.bucket, &entry.target_key).to_bytes())
        .collect()
}

/// One read of the head rows between the page's smallest and largest key; `None` for an
/// empty page. Read in the transaction, a head written in the interval meanwhile conflicts.
pub(crate) fn head_range(
    input: &PageImportInput,
    txn_id: TxnId,
) -> Result<Option<StorageEffect>, ConversionError> {
    let keys = head_keys(input)?;
    let (Some(first), Some(last)) = (keys.iter().min(), keys.iter().max()) else {
        return Ok(None);
    };
    Ok(Some(StorageEffect::IterRange {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        start: IterStart::At(first.clone().into()),
        end: IterEnd::Through(last.clone().into()),
        limit: keys.len() + HEAD_SLACK,
        max_bytes: HEAD_READ_BYTES,
        txn_id: Some(txn_id),
    }))
}

/// Reads of the page's heads, one per entry in entry order.
pub(crate) fn head_reads(input: &PageImportInput) -> Result<Vec<(KeySpace, Key)>, ConversionError> {
    let keys = head_keys(input)?.into_iter();
    Ok(keys
        .map(|key| (BLOB_HEAD_KEYSPACE.to_string(), key.into()))
        .collect())
}

/// Each entry's head among the rows of the page's key interval.
pub(crate) fn interval_heads(
    input: &PageImportInput,
    rows: Vec<(Key, Value)>,
) -> Result<Vec<Option<CurrentVersionPointer>>, PageImportError> {
    let rows: BTreeMap<Vec<u8>, Value> = (rows.into_iter())
        .map(|(key, value)| (key.to_vec(), value))
        .collect();
    let heads = head_keys(input)?
        .into_iter()
        .map(|key| rows.get(&key).cloned());
    decode_heads(heads)
}

/// The heads of [`head_reads`], in entry order.
pub(crate) fn decode_heads(
    values: impl IntoIterator<Item = Option<Value>>,
) -> Result<Vec<Option<CurrentVersionPointer>>, PageImportError> {
    let heads = (values.into_iter()).map(|value| {
        let head = value.map(|value| CurrentVersionPointer::from_bytes(value.as_ref()));
        head.transpose()
    });
    Ok(heads.collect::<Result<_, _>>()?)
}

/// Checks every guard of a single reference write: purge fence, bucket incarnation,
/// connector and job claim. Placement defaults come from the bucket row read here; the heads
/// are read next.
pub(crate) fn check_guards(
    input: &PageImportInput,
    values: Vec<(Key, Option<Value>)>,
) -> Result<PageGuards, PageImportError> {
    if values.len() != GUARD_READS + input.mirror_connectors.len() {
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
    let claim = values
        .next()
        .flatten()
        .map(|value| ClaimGuard::from_bytes(value.as_ref()))
        .transpose()?
        .filter(|claim| claim.claim_token == input.claim_token)
        .ok_or(PageImportError::ClaimLost)?;
    // A further source's connector replaced or removed since the page resolved it stops the page.
    for expected in &input.mirror_connectors {
        let found = values.next().flatten();
        let found = found.map(|value| SourceConnector::from_bytes(value.as_ref()));
        if found.transpose()?.as_ref() != Some(expected) {
            return Err(PageImportError::ConnectorChanged);
        }
    }
    Ok(PageGuards {
        policies,
        job_created_ms: claim.created_at_ms,
        heads: Vec::new(),
        set_hint: binding.as_ref().map(DistributionBinding::hint),
        binding,
        epochs: Vec::new(),
        downstream: true,
        search: true,
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
        && same_observation(input, entry, (source, cached_metadata))
        // An import naming no further sources leaves those a key has.
        && (entry.mirrors.is_empty() || source.mirrors == entry.mirrors)
        && version.metadata.len() == entry.metadata.len()
        && (entry.metadata.iter()).all(|(key, value)| version.metadata.get(key) == Some(value))
    {
        return EntryPlan::Unchanged;
    }
    match head.version_id.timestamp_ms() >= job_created_ms {
        true => EntryPlan::Kept,
        false => EntryPlan::Register,
    }
}

/// Whether an import saw the file as stored: equal validators, or, when there are none to
/// compare, as with a size from a table row, the identical observation of the same source.
fn same_observation(
    input: &PageImportInput,
    entry: &PageEntry,
    (source, stored): (&VersionSourceBinding, &SourceMetadata),
) -> bool {
    if stored.compare(&entry.observation) == ObservationMatch::Same {
        return true;
    }
    // A changed connector endpoint or root binds the file anew, so it is no longer unchanged.
    let connector = &input.connector;
    let binding = build_source_binding(
        StagingStrategy::Reference,
        connector,
        &entry.observation,
        entry.source_path.clone(),
        Some(input.node_id),
        Some(connector.connector_id),
    );
    let first = source.source_at(0);
    stored == &entry.observation
        && first.is_some_and(|first| RootEntry::same_source(&first, &binding))
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
    let mut roots = BTreeMap::new();
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
                let (rows, change, root) =
                    register_rows(input, guards, (entry, epoch), head.as_ref())?;
                writes.extend(rows);
                roots.extend(root);
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
    for root in roots.values() {
        writes.push(root_write(root)?);
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
        input.checkpoint_key.clone(),
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
type EntryRows = (
    Vec<(KeySpace, Key, Value)>,
    Option<InventoryChange>,
    Option<([u8; 32], SourceRoot)>,
);

fn register_rows(
    input: &PageImportInput,
    guards: &PageGuards,
    (entry, epoch): (&PageEntry, OriginalEpoch),
    head: Option<&CurrentVersionPointer>,
) -> Result<EntryRows, PageImportError> {
    let connector = &input.connector;
    let mut binding = build_source_binding(
        StagingStrategy::Reference,
        connector,
        &entry.observation,
        entry.source_path.clone(),
        Some(input.node_id),
        Some(connector.connector_id),
    );
    binding.mirrors = entry.mirrors.clone();
    let version_id = mint_version(guards.set_hint);
    let version = BlobVersion::reference(
        binding,
        entry.observation.clone(),
        input.now,
        input.user_id,
        input.now,
    )
    .with_origin(Some(VersionOrigin::Source {
        job_id: Some(input.job_id.as_ulid()),
    }))
    .with_metadata(entry.metadata.clone().into_iter().collect())
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
    let change =
        version_written(guards.binding.as_ref(), &entry.target_key, added, true).map(|change| {
            InventoryChange {
                feed: guards.search,
                ..change
            }
        });
    // The inventory saw the full binding; the stored one names its root by digest.
    let (version, root) = compact_version(version)?;
    let root = root.map(|root| root.digest().map(|digest| (digest, root)));
    let root = root.transpose()?;
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
    // A bucket nothing replicates owes no replication work for the version.
    if guards.downstream {
        rows.push(live_obligation_entry(&obligation)?);
    }
    Ok((rows, change, root))
}
