//! Plans one reference page on the member: static checks, guard reads, admission, and the rows,
//! usage and inventory changes of the records it stores.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::NodeId;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE,
    DISTRIBUTION_FENCE_KEYSPACE, DISTRIBUTION_KEYSPACE, PURGE_FENCE_KEYSPACE, S3_BUCKET_KEYSPACE,
    SYNC_RECEIPT_KEYSPACE,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::source_root::{RootEntry, SourceRoot};
use aruna_core::structs::storage::distribution::{
    AdmissionError, DistributionBinding, DistributionView,
};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::storage_purge::StoragePurgeFence;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::structs::tagged_receipt;
use aruna_core::types::{GroupId, Key, KeySpace, Value};
use thiserror::Error;
use ulid::Ulid;

use super::{PAGE_RECORDS, PageAnswer, ReferencePage};
use crate::distribution::inventory::{AddedVersion, InventoryChange, entry_of, version_written};
use crate::node::usage_stats::UsageUpdateError;
use crate::replication::incoming::{IncomingVersionError, member_reference, same_reference};
use crate::replication::protocol::VersionReplicationManifest;
use crate::s3::purge_fence::{PurgeFenceError, check_decoded_fence, fence_key};
use crate::staging::root_record::{compact_version, root_write};

/// Rows read ahead of the records: bucket, encryption, purge fence, record and member fence.
pub(super) const GUARD_READS: usize = 5;

#[derive(Debug, Error, PartialEq)]
pub enum PageError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
    #[error(transparent)]
    Purge(#[from] PurgeFenceError),
    #[error(transparent)]
    Admission(#[from] AdmissionError),
    #[error(transparent)]
    Record(#[from] IncomingVersionError),
    #[error("invalid page: {0}")]
    Invalid(&'static str),
    #[error("only a distributed bucket's coordinators publish to its member buckets")]
    ForeignPublication,
    #[error("the record of the destination's distributed bucket has not reached this node yet")]
    RecordMissing,
    #[error("the destination bucket encrypts its objects, so it accepts no references")]
    Encrypted,
    #[error("the version a head names is missing")]
    HeadMissing,
    #[error("unexpected event in state {state}: {received}")]
    UnexpectedEvent {
        state: &'static str,
        received: String,
    },
    #[error("operation did not finish")]
    NotFinished,
}

impl PageError {
    /// Whether the same page may pass later, after a conflict or a newer record.
    pub fn retryable(&self) -> bool {
        match self {
            Self::Storage(_) | Self::Usage(_) | Self::RecordMissing => true,
            Self::Admission(error) => error.retryable(),
            Self::Purge(error) => matches!(error, PurgeFenceError::Suspended),
            Self::Conversion(_)
            | Self::Record(_)
            | Self::Invalid(_)
            | Self::ForeignPublication
            | Self::Encrypted
            | Self::HeadMissing
            | Self::UnexpectedEvent { .. }
            | Self::NotFinished => false,
        }
    }
}

/// One record of the page with the version it stores.
#[derive(Debug, PartialEq)]
pub(super) struct PageItem {
    /// The version as published, with its full source binding.
    pub version: BlobVersion,
    /// The stored form, its source root named by digest, and that root.
    pub compact: BlobVersion,
    pub root: Option<SourceRoot>,
}

/// What the page does with one record, decided inside the transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Plan {
    /// Stores the record; with `install` it becomes the head, replacing `previous`.
    Write {
        install: bool,
        previous: Option<Ulid>,
    },
    /// The same record is stored already.
    Stored,
    /// Left to single-record delivery, which handles replacements and placement rules.
    Single,
}

/// What the guard reads established.
#[derive(Debug, PartialEq)]
pub(super) struct PageGuards {
    pub group_id: GroupId,
    pub binding: DistributionBinding,
    /// Whether the bucket's own placement rules govern every record it takes.
    pub governed: bool,
    /// Whether the set keeps a search index, so heads feed it.
    pub search: bool,
    pub existing: Vec<Option<BlobVersion>>,
    pub heads: Vec<Option<CurrentVersionPointer>>,
}

/// The rows of the records a page stores, their usage change and the answer.
pub(super) struct PageRows {
    pub writes: Vec<(KeySpace, Key, Value)>,
    pub delta: UsageDelta,
    pub inventory: Vec<InventoryChange>,
    pub written: u64,
    pub answer: PageAnswer,
}

/// Checks what a page asserts without storage and builds the version of each record.
pub(super) fn page_items(
    page: &ReferencePage,
    publisher: NodeId,
    realm_id: RealmId,
) -> Result<Vec<PageItem>, PageError> {
    let count = page.manifests.len();
    if count == 0 || count > PAGE_RECORDS {
        return Err(PageError::Invalid("a page holds 1 to 8192 records"));
    }
    let mut keys = BTreeSet::new();
    let mut items = Vec::with_capacity(count);
    for manifest in &page.manifests {
        manifest.validate()?;
        let single = manifest.bucket == page.bucket
            && manifest.distribution.as_ref() == Some(&page.publication)
            && manifest.is_reference()
            && manifest.blob.is_none()
            && manifest.multipart.is_none()
            && manifest.reference_advance.is_none()
            && manifest.writer_auth_context.is_none()
            && manifest.origin.is_none()
            && (!manifest.current_version || manifest.current_version_generation.is_some());
        if !single {
            return Err(PageError::Invalid(
                "a record is not a reference of the page",
            ));
        }
        let user_id = manifest.auth_context.user_id;
        if manifest.auth_context.realm_id != realm_id
            || (user_id.is_nil() && user_id.realm_id != realm_id)
        {
            return Err(PageError::Invalid("a record names another realm"));
        }
        if !keys.insert(manifest.key.as_str()) {
            return Err(PageError::Invalid("a page holds one record per key"));
        }
        let version = member_reference(manifest.clone(), publisher, realm_id)?;
        let (compact, root) = compact_version(version.clone())?;
        items.push(PageItem {
            version,
            compact,
            root,
        });
    }
    Ok(items)
}

/// The guard rows, then each record's version row and head, all read in the page transaction.
pub(super) fn guard_reads(page: &ReferencePage) -> Result<Vec<(KeySpace, Key)>, ConversionError> {
    let bucket: Key = page.bucket.as_bytes().to_vec().into();
    let set: Key = page.publication.set_id.to_bytes().to_vec().into();
    let mut reads = vec![
        (S3_BUCKET_KEYSPACE.to_string(), bucket.clone()),
        (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket),
        (PURGE_FENCE_KEYSPACE.to_string(), fence_key(&page.bucket)),
        (DISTRIBUTION_KEYSPACE.to_string(), set.clone()),
        (DISTRIBUTION_FENCE_KEYSPACE.to_string(), set),
    ];
    for manifest in &page.manifests {
        let version = VersionKey::new(&page.bucket, &manifest.key, manifest.version_id);
        let head = BlobHeadKey::new(&page.bucket, &manifest.key);
        reads.push((
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version.to_bytes()?.into(),
        ));
        reads.push((BLOB_HEAD_KEYSPACE.to_string(), head.to_bytes()?.into()));
    }
    Ok(reads)
}

/// Admits every record against this node's record of the set, as a single publication is.
pub(super) fn check_guards(
    page: &ReferencePage,
    publisher: NodeId,
    values: Vec<(Key, Option<Value>)>,
) -> Result<PageGuards, PageError> {
    if values.len() != GUARD_READS + 2 * page.manifests.len() {
        return Err(StorageError::ReadError("short page guard read".to_string()).into());
    }
    let mut values = values.into_iter().map(|(_, value)| value);
    let mut next = || values.next().flatten();
    let bucket = next()
        .map(|value| BucketInfo::from_bytes(&value))
        .transpose()?
        .ok_or(PageError::ForeignPublication)?;
    if BucketEncryption::from_row(next().as_deref())?.is_encrypted() {
        return Err(PageError::Encrypted);
    }
    let purge = next()
        .map(|value| StoragePurgeFence::from_bytes(&value).map_err(|_| PurgeFenceError::Invalid))
        .transpose()?;
    let record = next()
        .map(|value| DistributionView::from_bytes(&value))
        .transpose()?
        .ok_or(PageError::RecordMissing)?;
    let fence = LocalFence::decode(next().as_deref())?;
    let binding = bucket
        .distribution
        .filter(|binding| binding.set_id == page.publication.set_id)
        .ok_or(PageError::ForeignPublication)?;
    let snapshot = record.snapshot();
    for manifest in &page.manifests {
        check_decoded_fence(purge.as_ref(), &page.bucket, &manifest.key)?;
        snapshot.admit(&binding, publisher, &manifest.key, &page.publication)?;
        // A version never claims an epoch above the authority it was published under.
        let (epoch, published) = (manifest.original_epoch.0, page.publication.epoch.0);
        if epoch > published {
            let error = AdmissionError::FutureEpoch {
                epoch,
                publication: published,
            };
            return Err(error.into());
        }
    }
    if !fence.receiving() {
        return Err(AdmissionError::Fenced.into());
    }
    let mut existing = Vec::with_capacity(page.manifests.len());
    let mut heads = Vec::with_capacity(page.manifests.len());
    for _ in &page.manifests {
        existing.push(next().map(|v| BlobVersion::from_bytes(&v)).transpose()?);
        heads.push(
            next()
                .map(|v| CurrentVersionPointer::from_bytes(&v))
                .transpose()?,
        );
    }
    Ok(PageGuards {
        group_id: bucket.group_id,
        binding,
        governed: !bucket.placement_policies.is_empty(),
        search: snapshot.config.settings.search_index,
        existing,
        heads,
    })
}

/// Decides each record against its stored row and head.
pub(super) fn plan_items(
    page: &ReferencePage,
    items: &[PageItem],
    guards: &PageGuards,
) -> Vec<Plan> {
    let rows = page.manifests.iter().zip(items);
    let rows = rows.zip(guards.existing.iter().zip(&guards.heads));
    rows.map(|((manifest, item), (existing, head))| {
        if guards.governed || !manifest.placement_policies.is_empty() {
            return Plan::Single;
        }
        if let Some(existing) = existing {
            return match same_reference(existing, &item.compact) {
                true => Plan::Stored,
                false => Plan::Single,
            };
        }
        let install =
            manifest.current_version && head_pointer(manifest, item).supersedes(head.as_ref());
        let previous = head
            .as_ref()
            .filter(|head| install && head.version_id != manifest.version_id)
            .map(|head| head.version_id);
        Plan::Write { install, previous }
    })
    .collect()
}

/// The head a current record installs, at the generation its coordinator's head has.
fn head_pointer(manifest: &VersionReplicationManifest, item: &PageItem) -> CurrentVersionPointer {
    let generation = manifest.current_version_generation.unwrap_or_default();
    CurrentVersionPointer::new_with_generation(manifest.version_id, generation)
        .with_epoch(item.version.original_epoch)
}

/// The version rows the replaced heads name, in record order.
pub(super) fn previous_reads(
    page: &ReferencePage,
    plans: &[Plan],
) -> Result<Vec<(KeySpace, Key)>, ConversionError> {
    let mut reads = Vec::new();
    for (manifest, plan) in page.manifests.iter().zip(plans) {
        if let Plan::Write {
            previous: Some(previous),
            ..
        } = plan
        {
            let key = VersionKey::new(&page.bucket, &manifest.key, *previous);
            reads.push((BLOB_VERSIONS_KEYSPACE.to_string(), key.to_bytes()?.into()));
        }
    }
    Ok(reads)
}

/// Builds the rows of every record the page stores, with the usage and inventory change.
pub(super) fn page_rows(
    page: &ReferencePage,
    items: &[PageItem],
    plans: &[Plan],
    (guards, previous): (&PageGuards, Vec<Option<BlobVersion>>),
) -> Result<PageRows, PageError> {
    let mut previous = previous.into_iter();
    let mut rows = PageRows {
        writes: Vec::new(),
        delta: UsageDelta::default(),
        inventory: Vec::new(),
        written: 0,
        answer: PageAnswer::Applied {
            applied: 0,
            single: Vec::new(),
        },
    };
    let (mut applied, mut single) = (0u32, Vec::new());
    let mut roots = BTreeMap::new();
    for ((manifest, item), plan) in page.manifests.iter().zip(items).zip(plans) {
        let (install, replaced) = match plan {
            Plan::Single => {
                single.push(manifest.version_id);
                continue;
            }
            Plan::Stored => {
                applied += 1;
                continue;
            }
            Plan::Write {
                install,
                previous: named,
            } => {
                let replaced = match named {
                    Some(_) => Some(previous.next().flatten().ok_or(PageError::HeadMissing)?),
                    None => None,
                };
                (*install, replaced)
            }
        };
        let size = manifest
            .reference_metadata
            .as_ref()
            .map_or(0, |metadata| metadata.content_length);
        let key = &manifest.key;
        let version_key = VersionKey::new(&page.bucket, key, manifest.version_id);
        rows.writes.push((
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version_key.to_bytes()?.into(),
            item.compact.to_bytes()?.into(),
        ));
        if let Some(root) = &item.root {
            roots.insert(root.digest()?, root.clone());
        }
        if install {
            let head = BlobHeadKey::new(&page.bucket, key).to_bytes()?;
            let pointer = head_pointer(manifest, item).to_bytes()?;
            rows.writes
                .push((BLOB_HEAD_KEYSPACE.to_string(), head.into(), pointer.into()));
        }
        // A member keeps the receipt of a pulled version, so a later coordinator answers replays.
        if let Some(receipt) = tagged_receipt((&page.bucket, key), &manifest.metadata, false) {
            let id = manifest.version_id.to_bytes().to_vec();
            rows.writes
                .push((SYNC_RECEIPT_KEYSPACE.to_string(), receipt.into(), id.into()));
        }
        // A new head releases an offered directory observation and may revive the key.
        let released = replaced
            .as_ref()
            .and_then(|version| version.offered_size(&page.bucket, key));
        let was_live = replaced.as_ref().is_some_and(|v| !v.is_deleted());
        rows.delta.objects += i128::from(install) - i128::from(install && was_live);
        rows.delta.referenced_bytes += i128::from(size) - i128::from(released.unwrap_or(0));
        let added = AddedVersion {
            version_id: manifest.version_id,
            epoch: item.version.original_epoch,
            generation: item.version.generation,
            entry: entry_of(&item.version, size),
        };
        if let Some(mut change) = version_written(Some(&guards.binding), key, added, install) {
            // A reference carries its source root, which a member taking over the range checks.
            change.root = manifest.source.as_ref().and_then(RootEntry::of);
            change.feed = guards.search;
            rows.inventory.push(change);
        }
        rows.written += 1;
        applied += 1;
    }
    for root in roots.values() {
        rows.writes.push(root_write(root)?);
    }
    rows.answer = PageAnswer::Applied { applied, single };
    Ok(rows)
}
