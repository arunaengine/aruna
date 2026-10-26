//! Counts the rows of one usage rebuild page into counter deltas, joining each row only with
//! point reads from the same snapshot.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};

use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    ABE_ARCHIVE_KEYSPACE, ABE_ENVELOPE_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    PENDING_LOCATION_KEYSPACE,
};
use aruna_core::structs::identity::realm::QuotaConfig;
use aruna_core::structs::storage::abe::envelope_charge;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobHeadKey, BlobLocationKey, BlobVersion, BlobVersionState,
    BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::format::StoredLayout;
use aruna_core::structs::storage::multipart::MultipartUpload;
use aruna_core::structs::storage::usage::{
    BucketDelta, UsageCounters, UsageDelta, global_group_key, global_shard_key, shard_for_archive,
    shard_for_hash, usage_backend_key, usage_bucket_key, usage_group_key, usage_shadow_key,
};
use aruna_core::types::{GroupId, Key, Value};
use tracing::warn;
use ulid::Ulid;

use crate::abe::copies::copy_version;
use crate::node::usage_rebuild::RebuildStatsError;
use crate::node::usage_stats::StoredDelta;

/// Point reads of one page by keyspace and key; `None` for an absent row.
pub(crate) type Joined = HashMap<(&'static str, Vec<u8>), Option<Value>>;

pub(crate) type Object = (String, String);

/// Usage and bucket deltas of one page, keyed by their counter rows.
pub(crate) type CounterDeltas = (Vec<(Key, UsageDelta)>, Vec<(Key, BucketDelta)>);

/// What a rebuild has counted so far, and the deltas of the page not yet committed. Grows
/// with buckets and one page, never with objects.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct RebuildCounts {
    pub(crate) quota: Option<QuotaConfig>,
    bucket_groups: HashMap<String, GroupId>,
    /// Buckets whose group has a quota; their versions are walked first.
    pub(crate) quota_buckets: BTreeSet<String>,
    usage: BTreeMap<Vec<u8>, UsageDelta>,
    buckets: BTreeMap<Vec<u8>, BucketDelta>,
    pub(crate) total: UsageCounters,
    /// Versions whose location row is gone; their bytes cannot be counted.
    pub(crate) orphan_versions: u64,
    /// Head of the last object a page ended in, so the next page need not read it again.
    pub(crate) last_head: Option<(Object, Option<Ulid>)>,
}

impl RebuildCounts {
    pub(crate) fn is_empty(&self) -> bool {
        self.usage.is_empty() && self.buckets.is_empty()
    }

    /// The page's deltas, aimed at the rebuild rows of their counters.
    pub(crate) fn take_deltas(&mut self) -> CounterDeltas {
        let usage = std::mem::take(&mut self.usage)
            .into_iter()
            .map(|(key, delta)| (Key::from(usage_shadow_key(&key)), delta))
            .collect();
        let buckets = std::mem::take(&mut self.buckets)
            .into_iter()
            .map(|(key, delta)| (Key::from(usage_shadow_key(&key)), delta))
            .collect();
        (usage, buckets)
    }

    /// Notes a bucket's group, and whether its group has a quota, without counting it.
    pub(crate) fn remember(
        &mut self,
        key: &Key,
        value: &Value,
    ) -> Result<(String, GroupId), RebuildStatsError> {
        let info = BucketInfo::from_bytes(value.as_ref())?;
        let bucket = String::from_utf8(key.to_vec()).map_err(ConversionError::from)?;
        let quota = self.quota.as_ref();
        if quota.is_some_and(|quota| quota.effective_group_ceiling(&info.group_id).is_some()) {
            self.quota_buckets.insert(bucket.clone());
        }
        self.bucket_groups.insert(bucket.clone(), info.group_id);
        Ok((bucket, info.group_id))
    }

    pub(crate) fn bucket(&mut self, key: &Key, value: &Value) -> Result<(), RebuildStatsError> {
        let (bucket, group_id) = self.remember(key, value)?;
        let delta = UsageCounters {
            buckets: 1,
            ..Default::default()
        };
        self.total.add(&delta)?;
        self.add_usage(usage_group_key(group_id), signed(&delta));
        self.add_usage(global_group_key(group_id), signed(&delta));
        // Each bucket keeps a row, an empty one included.
        self.add_bucket(usage_bucket_key(&bucket), BucketDelta::default());
        Ok(())
    }

    /// An object is live when its head points at a version that is not a delete marker.
    /// `sizes` holds the size of any usable copy for hashes whose named row was unusable.
    pub(crate) fn version(
        &mut self,
        key: &Key,
        value: &Value,
        joined: &Joined,
        sizes: &HashMap<[u8; 32], Option<u64>>,
    ) -> Result<(), RebuildStatsError> {
        let version = BlobVersion::from_bytes(value.as_ref())?;
        let version_key = VersionKey::from_bytes(key.as_ref())?;
        let object = (version_key.bucket.clone(), version_key.key.clone());
        let head = self.head_of(&object, joined)?;
        let bucket = &version_key.bucket;
        if self.bucket_groups.contains_key(bucket) {
            let marker = version.is_deleted();
            let row = BucketDelta {
                versions: (!marker).into(),
                delete_markers: marker.into(),
                ..Default::default()
            };
            self.add_bucket(usage_bucket_key(bucket), row);
        }
        let current = head == Some(version_key.version_id);
        if current && !version.is_deleted() {
            let delta = UsageCounters {
                objects: 1,
                ..Default::default()
            };
            self.total.add(&delta)?;
            self.book_object(bucket, &delta);
        }
        // An offered directory is charged only for its current observations.
        let charged = current || version.offered_size(bucket, &version_key.key).is_none();
        let bytes = match &version.state {
            BlobVersionState::Materialized {
                blob_hash,
                backend,
                encoding,
                ..
            } => {
                let location = BlobLocationKey::new(*blob_hash, *encoding, backend.clone());
                let named = joined.get(&(BLOB_LOCATIONS_KEYSPACE, location.to_bytes()));
                let size = match named_size(named)? {
                    Some(size) => Some(size),
                    None => sizes.get(blob_hash).copied().flatten(),
                };
                self.orphan_versions += u64::from(size.is_none());
                size.map(|size| (size, 0))
            }
            BlobVersionState::Reference {
                cached_metadata, ..
            } => charged.then_some((0, cached_metadata.content_length)),
            BlobVersionState::PendingContent { archive, .. } => {
                let row = joined.get(&(PENDING_LOCATION_KEYSPACE, archive.to_bytes()));
                let size = named_size(row)?;
                self.orphan_versions += u64::from(size.is_none());
                size.map(|size| (size, 0))
            }
            BlobVersionState::Deleted => None,
        };
        if let Some((logical_bytes, referenced_bytes)) = bytes {
            let delta = UsageCounters {
                logical_bytes,
                referenced_bytes,
                ..Default::default()
            };
            self.total.add(&delta)?;
            self.book_object(bucket, &delta);
        }
        self.last_head = Some((object, head));
        Ok(())
    }

    fn head_of(&self, object: &Object, joined: &Joined) -> Result<Option<Ulid>, RebuildStatsError> {
        if let Some((last, head)) = &self.last_head
            && last == object
        {
            return Ok(*head);
        }
        let key = BlobHeadKey::new(object.0.clone(), object.1.clone()).to_bytes()?;
        match joined.get(&(BLOB_HEAD_KEYSPACE, key)) {
            Some(Some(value)) => Ok(Some(
                CurrentVersionPointer::from_bytes(value.as_ref())?.version_id,
            )),
            Some(None) => Ok(None),
            None => Err(RebuildStatsError::RebuildFailed),
        }
    }

    /// A sealed version is charged its envelope row and its archive mapping row.
    pub(crate) fn envelope(
        &mut self,
        key: &Key,
        id: &Value,
        joined: &Joined,
    ) -> Result<(), RebuildStatsError> {
        let version = VersionKey::from_bytes(key.as_ref())?;
        let row = |key_space| joined.get(&(key_space, id.to_vec())).cloned().flatten();
        let envelope = row(ABE_ENVELOPE_KEYSPACE).map_or(0, |value| value.len() as u64);
        let archive = row(ABE_ARCHIVE_KEYSPACE).map_or(0, |value| envelope_charge(&[], &value));
        self.book_logical(&version.bucket, envelope + archive)
    }

    /// A pending copy is charged its stored row until its envelope replaces it.
    pub(crate) fn copy(&mut self, key: &Key, value: &Value) -> Result<(), RebuildStatsError> {
        let version = copy_version(key)?;
        self.book_logical(&version.bucket, value.len() as u64)
    }

    fn book_logical(&mut self, bucket: &str, logical_bytes: u64) -> Result<(), RebuildStatsError> {
        let delta = UsageCounters {
            logical_bytes,
            ..Default::default()
        };
        self.total.add(&delta)?;
        self.book_object(bucket, &delta);
        Ok(())
    }

    /// Books a known Pithos archive only while it has no pending row; the pending row counts
    /// it then, as one archive is one stored blob during its promotion.
    pub(crate) fn location(
        &mut self,
        key: &Key,
        value: &Value,
        pending: bool,
        joined: &Joined,
    ) -> Result<(), RebuildStatsError> {
        let location = BackendLocation::from_bytes(value.as_ref())?;
        if !usable(&location) {
            return Ok(());
        }
        let shard = match pending {
            true => shard_for_archive(location.ulid),
            false => {
                let hash = BlobLocationKey::from_bytes(key.as_ref())?.blake3_hash;
                if is_pithos(&location) {
                    let archive = ArchiveKey::of(&location).to_bytes();
                    let row = joined.get(&(PENDING_LOCATION_KEYSPACE, archive));
                    if named_size(row)?.is_some() {
                        return Ok(());
                    }
                }
                StoredDelta::of_copy(&location, 1, 0)
                    .map_or_else(|| shard_for_hash(&hash), |stored| stored.shard)
            }
        };
        let delta = UsageCounters {
            stored_blobs: 1,
            stored_bytes: location.stored_size(),
            ..Default::default()
        };
        self.total.add(&delta)?;
        self.add_usage(global_shard_key(shard), signed(&delta));
        let backend = UsageCounters {
            stored_bytes: location.stored_size(),
            ..Default::default()
        };
        let backend_key = usage_backend_key(&location.backend, shard);
        self.add_usage(backend_key, signed(&backend));
        Ok(())
    }

    pub(crate) fn upload(&mut self, value: &Value) {
        // Like the stale sweep, an undecodable record is left out, as no flow ends it.
        let Ok(upload) = MultipartUpload::from_bytes(value.as_ref()) else {
            warn!("rebuilt usage skips an undecodable multipart upload");
            return;
        };
        if self.bucket_groups.contains_key(&upload.bucket) {
            let row = BucketDelta {
                open_uploads: 1,
                ..Default::default()
            };
            self.add_bucket(usage_bucket_key(&upload.bucket), row);
        }
    }

    /// Books an object-level delta on its bucket's group, global shard and bucket rows.
    fn book_object(&mut self, bucket: &str, delta: &UsageCounters) {
        let Some(group_id) = self.bucket_groups.get(bucket).copied() else {
            self.add_usage(global_shard_key(0), signed(delta));
            return;
        };
        self.add_usage(usage_group_key(group_id), signed(delta));
        self.add_usage(global_group_key(group_id), signed(delta));
        let share = BucketDelta {
            objects: delta.objects.into(),
            logical_bytes: delta.logical_bytes.into(),
            referenced_bytes: delta.referenced_bytes.into(),
            ..Default::default()
        };
        self.add_bucket(usage_bucket_key(bucket), share);
    }

    fn add_usage(&mut self, key: Vec<u8>, delta: UsageDelta) {
        let entry = self.usage.entry(key).or_default();
        *entry = entry.merge(delta);
    }

    fn add_bucket(&mut self, key: Vec<u8>, delta: BucketDelta) {
        let entry = self.buckets.entry(key).or_default();
        *entry = entry.merge(delta);
    }
}

/// A finished copy row; staging and partial rows hold no countable blob.
pub(crate) fn usable(location: &BackendLocation) -> bool {
    !location.staging && !location.partial
}

pub(crate) fn is_pithos(location: &BackendLocation) -> bool {
    matches!(location.format.layout, StoredLayout::Pithos(_))
}

/// The blob size of a joined location row, `None` when it is absent or not usable.
pub(crate) fn named_size(row: Option<&Option<Value>>) -> Result<Option<u64>, RebuildStatsError> {
    let Some(Some(value)) = row else {
        return Ok(None);
    };
    let location = BackendLocation::from_bytes(value.as_ref())?;
    Ok(usable(&location).then_some(location.blob_size))
}

fn signed(counters: &UsageCounters) -> UsageDelta {
    UsageDelta {
        buckets: counters.buckets.into(),
        objects: counters.objects.into(),
        stored_blobs: counters.stored_blobs.into(),
        stored_bytes: counters.stored_bytes.into(),
        logical_bytes: counters.logical_bytes.into(),
        referenced_bytes: counters.referenced_bytes.into(),
    }
}
