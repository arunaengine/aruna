//! Lists the versions of one key range of a distributed bucket by seeking in its ordered
//! inventory, newest first per key, then hydrates the page's version rows once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Interval reads are bounded, so no key's whole history is read for a short page.

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterEnd, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, INVENTORY_KEYSPACE, PENDING_LOCATION_KEYSPACE,
    VERSION_INDEX_KEYSPACE, prefix_upper_bound,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, values};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobLocationKey, BlobVersion, BlobVersionState, VersionKey,
};
use aruna_core::structs::storage::distribution::inventory::{
    Entry, HeadEntry, IndexEntry, Position, bound_position, head_position, index_key,
    prefix_interval,
};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

use crate::s3::listing::common_prefix_of;
use crate::s3::object::versions::{ListVersionsItem, ListVersionsResult};

/// Interval reads one page issues at most before it ends truncated.
const MAX_ROUNDS: usize = 64;
/// Bytes of keys and values one interval read returns at most.
const ROUND_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InventoryVersionsInput {
    pub set_id: Ulid,
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub key_marker: Option<String>,
    pub version_id_marker: Option<Ulid>,
    pub max_keys: usize,
    /// The key range `[lower, upper)` the listing stays inside.
    pub lower: String,
    pub upper: Option<String>,
}

#[derive(Debug, Error, PartialEq)]
pub enum InventoryVersionsError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("the version id marker names no version of the key marker")]
    Marker,
    #[error("the version listing received an unexpected event: {0}")]
    Unexpected(&'static str),
}

/// What the pending call reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    ReadMarker,
    Scan,
    Versions,
    Locations,
}

type Step = Result<Next<ListVersionsResult>, InventoryVersionsError>;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Candidate {
    key: String,
    version_id: Ulid,
    latest: bool,
}

#[derive(Debug, PartialEq)]
pub struct InventoryVersionsOperation {
    input: InventoryVersionsInput,
    state: State,
    start: Vec<u8>,
    /// The start is the first row after `start` rather than `start` itself.
    after: bool,
    end: Vec<u8>,
    /// The key being walked and its current version id.
    head: Option<(String, Ulid)>,
    candidates: Vec<Candidate>,
    prefixes: Vec<String>,
    last: Option<(String, Option<Ulid>)>,
    rounds: usize,
    truncated: bool,
    versions: Vec<Option<BlobVersion>>,
}

impl InventoryVersionsOperation {
    pub fn new(input: InventoryVersionsInput) -> Self {
        Self {
            input,
            state: State::Scan,
            start: Vec::new(),
            after: false,
            end: Vec::new(),
            head: None,
            candidates: Vec::new(),
            prefixes: Vec::new(),
            last: None,
            rounds: 0,
            truncated: false,
            versions: Vec::new(),
        }
    }

    fn prefix(&self) -> &str {
        self.input.prefix.as_deref().unwrap_or_default()
    }

    fn emitted(&self) -> usize {
        self.candidates.len() + self.prefixes.len()
    }

    /// The interval the page may read: the range, narrowed to the prefix.
    fn bounds(&mut self) {
        let (lower, upper) = (&self.input.lower, self.input.upper.as_deref());
        (self.start, self.end) = prefix_interval(self.input.set_id, lower, upper, self.prefix());
    }

    /// Moves the start past `position` when it lies ahead of the current one.
    fn resume(&mut self, position: Vec<u8>, after: bool) {
        if position >= self.start {
            self.start = position;
            self.after = after;
        }
    }

    /// The first row after every row of `key`.
    fn past_key(&self, key: &str) -> Vec<u8> {
        let mut rows = head_position(self.input.set_id, key);
        rows.pop();
        prefix_upper_bound(&rows).unwrap_or(rows)
    }

    fn begin(&mut self) -> Step {
        if self.input.max_keys == 0 {
            return Ok(self.finish(Vec::new()));
        }
        self.bounds();
        let marker = self.input.key_marker.clone().filter(|key| !key.is_empty());
        // The nil version id marks a start at the marker key itself, where a merged page ends
        // when it reached its range bound before any entry.
        if let (Some(key), Some(true)) =
            (&marker, self.input.version_id_marker.map(|id| id.is_nil()))
        {
            let start = bound_position(self.input.set_id, key);
            self.resume(start, false);
            return Ok(self.scan());
        }
        if let (Some(_), Some(version_id)) = (&marker, self.input.version_id_marker) {
            self.state = State::ReadMarker;
            let marker = index_key(self.input.set_id, version_id);
            return Ok(Next::Call(read_row(VERSION_INDEX_KEYSPACE, marker)));
        }
        if let Some(marker) = marker {
            let delimited = (self.input.delimiter.as_deref())
                .is_some_and(|delimiter| !delimiter.is_empty() && marker.ends_with(delimiter));
            let past = match delimited {
                true => {
                    let group = bound_position(self.input.set_id, &marker);
                    prefix_upper_bound(&group).unwrap_or(group)
                }
                false => self.past_key(&marker),
            };
            self.resume(past, false);
        }
        Ok(self.scan())
    }

    fn marker_read(&mut self, value: Option<Value>) -> Step {
        let entry = IndexEntry::from_bytes(&value.ok_or(InventoryVersionsError::Marker)?)?;
        let position = Position::decode(&entry.position)?;
        if Some(&position.key) != self.input.key_marker.as_ref() {
            return Err(InventoryVersionsError::Marker);
        }
        // The rest of the marker key is older than its head, so none of it is latest.
        self.head = Some((position.key, Ulid::nil()));
        self.resume(entry.position, true);
        Ok(self.scan())
    }

    fn scan(&mut self) -> Next<ListVersionsResult> {
        if self.start >= self.end || self.rounds >= MAX_ROUNDS {
            self.truncated = self.start < self.end && self.last.is_some();
            return self.hydrate();
        }
        self.rounds += 1;
        self.state = State::Scan;
        let remaining = self.input.max_keys.saturating_sub(self.emitted());
        let start = match self.after {
            true => IterStart::After(self.start.clone().into()),
            false => IterStart::At(self.start.clone().into()),
        };
        let range = StorageEffect::IterRange {
            key_space: INVENTORY_KEYSPACE.to_string(),
            start,
            end: IterEnd::Before(self.end.clone().into()),
            limit: remaining.saturating_mul(2).saturating_add(2),
            max_bytes: ROUND_BYTES,
            txn_id: None,
        };
        Next::Call(IoCall::Effect(Box::new(Effect::Storage(range))))
    }

    fn scanned(&mut self, values: Vec<(Key, Value)>, more: bool) -> Step {
        for (row, value) in values {
            self.resume(row.to_vec(), true);
            let position = Position::decode(&row)?;
            match position.entry {
                Entry::Head => {
                    let head = HeadEntry::from_bytes(&value)?;
                    self.head = Some((position.key, head.version_id));
                }
                Entry::Version { version_id, .. } => {
                    let group = common_prefix_of(
                        &position.key,
                        self.input.prefix.as_deref(),
                        self.input.delimiter.as_deref(),
                    );
                    if self.emitted() >= self.input.max_keys {
                        self.truncated = true;
                        return Ok(self.hydrate());
                    }
                    let Some(group) = group else {
                        let latest = self
                            .head
                            .as_ref()
                            .is_some_and(|(key, head)| *key == position.key && *head == version_id);
                        self.last = Some((position.key.clone(), Some(version_id)));
                        self.candidates.push(Candidate {
                            key: position.key,
                            version_id,
                            latest,
                        });
                        continue;
                    };
                    if self.prefixes.last() != Some(&group) {
                        self.prefixes.push(group.clone());
                        self.last = Some((group.clone(), None));
                    }
                    // The whole group is one entry; the next read starts past it.
                    let rows = bound_position(self.input.set_id, &group);
                    self.resume(prefix_upper_bound(&rows).unwrap_or(rows), false);
                    return Ok(self.scan());
                }
                Entry::Upload(_) => {}
            }
        }
        match more {
            true => Ok(self.scan()),
            false => Ok(self.hydrate()),
        }
    }

    fn hydrate(&mut self) -> Next<ListVersionsResult> {
        if self.candidates.is_empty() {
            return self.finish(Vec::new());
        }
        let reads: Vec<(String, Key)> = (self.candidates.iter())
            .filter_map(|candidate| {
                let key = VersionKey::new(&self.input.bucket, &candidate.key, candidate.version_id);
                key.to_bytes().ok()
            })
            .map(|key| (BLOB_VERSIONS_KEYSPACE.to_string(), key.into()))
            .collect();
        self.state = State::Versions;
        Next::Call(IoCall::Read(reads))
    }

    /// The location row a version's bytes are described by; governed copies stay undescribed.
    fn location_read(version: &BlobVersion) -> Option<(String, Key)> {
        if !version.placement_policies.is_empty() {
            return None;
        }
        match &version.state {
            BlobVersionState::Materialized {
                blob_hash,
                backend,
                encoding,
                ..
            } => {
                let location = BlobLocationKey::new(*blob_hash, *encoding, backend.clone());
                Some((
                    BLOB_LOCATIONS_KEYSPACE.to_string(),
                    location.to_bytes().into(),
                ))
            }
            BlobVersionState::PendingContent { archive, .. } => Some((
                PENDING_LOCATION_KEYSPACE.to_string(),
                archive.to_bytes().into(),
            )),
            BlobVersionState::Deleted | BlobVersionState::Reference { .. } => None,
        }
    }

    fn versions_read(&mut self, values: Vec<Option<Value>>) -> Step {
        self.versions = values
            .into_iter()
            .map(|value| {
                value
                    .map(|value| BlobVersion::from_bytes(&value))
                    .transpose()
            })
            .collect::<Result<_, _>>()?;
        let reads: Vec<(String, Key)> = (self.versions.iter().flatten())
            .filter_map(Self::location_read)
            .collect();
        if reads.is_empty() {
            return Ok(self.items(Vec::new()));
        }
        self.state = State::Locations;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn items(&mut self, locations: Vec<Option<Value>>) -> Next<ListVersionsResult> {
        let mut locations = locations.into_iter();
        let mut items = Vec::new();
        let candidates = std::mem::take(&mut self.candidates);
        let versions = std::mem::take(&mut self.versions);
        for (candidate, version) in candidates.into_iter().zip(versions) {
            // A row without its version is an inconsistency the next write repairs.
            let Some(version) = version else {
                continue;
            };
            let location = match Self::location_read(&version) {
                Some(_) => locations
                    .next()
                    .flatten()
                    .and_then(|value| BackendLocation::from_bytes(&value).ok()),
                None => None,
            };
            let (key, version_id, is_latest) =
                (candidate.key, candidate.version_id, candidate.latest);
            let created_at = version.created_at;
            items.push(match version.state {
                BlobVersionState::Deleted => ListVersionsItem::DeleteMarker {
                    key,
                    version_id,
                    is_latest,
                    created_at,
                },
                BlobVersionState::Reference {
                    cached_metadata,
                    source,
                    ..
                } => ListVersionsItem::Version {
                    key,
                    version_id,
                    is_latest,
                    location: None,
                    source_metadata: Some(cached_metadata),
                    created_at,
                    link: (source.descriptor.kind
                        == aruna_core::structs::execution::source_connector::SourceConnectorKind::ArunaLink)
                        .then_some(source),
                },
                _ => ListVersionsItem::Version {
                    key,
                    version_id,
                    is_latest,
                    location,
                    source_metadata: None,
                    created_at,
                    link: None,
                },
            });
        }
        self.finish(items)
    }

    fn finish(&mut self, items: Vec<ListVersionsItem>) -> Next<ListVersionsResult> {
        let (next_key_marker, next_version_marker) = match (self.truncated, self.last.take()) {
            (true, Some((key, version_id))) => (Some(key), version_id),
            _ => (None, None),
        };
        Next::Done(ListVersionsResult {
            items,
            common_prefixes: std::mem::take(&mut self.prefixes),
            is_truncated: next_key_marker.is_some(),
            next_key_marker,
            next_version_marker,
        })
    }
}

impl Operation for InventoryVersionsOperation {
    type Output = ListVersionsResult;
    type Error = InventoryVersionsError;

    fn first(&mut self) -> Step {
        self.begin()
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.state {
            State::ReadMarker => {
                let [value] = values::<1>(result)?;
                self.marker_read(value)
            }
            State::Scan => match result.event()? {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => self.scanned(values, next_start_after.is_some()),
                Event::Storage(StorageEvent::Error { error }) => {
                    Err(IoError::Storage(error).into())
                }
                other => Err(InventoryVersionsError::Unexpected(other.label())),
            },
            State::Versions => self.versions_read(result.values()?),
            State::Locations => Ok(self.items(result.values()?)),
        }
    }
}

#[cfg(test)]
#[path = "versions_tests.rs"]
mod tests;
