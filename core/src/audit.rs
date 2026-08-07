//! Contract for gathering a group's metadata audit trail across realm nodes.
//! Audit rows are node-local projections, so a complete trail is assembled by
//! asking every eligible node for its local page and merging the results.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::metadata::MetadataAuthToken;
use crate::structs::{MetadataAuditRecord, RealmId};
use crate::types::{GroupId, NodeId};

pub const AUDIT_KEY_BYTES: usize = 48;
pub const MAX_AUDIT_RECORDS: usize = 200;
pub const MAX_AUDIT_PAGE_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_AUDIT_BATCH_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_AUDIT_PEERS: usize = 64;

/// A single realm node's local audit page request. Carries the caller's
/// authority so the serving node re-checks group-admin access before answering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditPageRequest {
    pub auth_token: Option<MetadataAuthToken>,
    pub config_digest: [u8; 32],
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub document_id: Option<Ulid>,
    pub start_after: Option<Vec<u8>>,
    pub limit: usize,
}

/// One audit row plus its raw storage key. The key lets the aggregator merge the
/// same record projected onto several holders by identity and page it in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditPageEntry {
    pub key: Vec<u8>,
    pub record: MetadataAuditRecord,
}

/// One node's local audit page in key order. `next_start_after` is present when
/// the node has more rows past this page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditPageResponse {
    pub records: Vec<AuditPageEntry>,
    pub next_start_after: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditBatchEntry {
    pub source: NodeId,
    pub entry: AuditPageEntry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditPageBatch {
    pub limit: usize,
    pub records: BTreeMap<Vec<u8>, AuditBatchEntry>,
    pub horizon: Option<Vec<u8>>,
    pub completed_nodes: BTreeSet<NodeId>,
    pub missing_nodes: BTreeSet<NodeId>,
    pub missing_overflow: usize,
    pub conflict: bool,
    pub bytes: usize,
}

impl AuditPageBatch {
    pub fn new() -> Self {
        Self::with_limit(MAX_AUDIT_RECORDS)
    }

    pub fn with_limit(limit: usize) -> Self {
        Self {
            limit: limit.clamp(1, MAX_AUDIT_RECORDS),
            records: BTreeMap::new(),
            horizon: None,
            completed_nodes: BTreeSet::new(),
            missing_nodes: BTreeSet::new(),
            missing_overflow: 0,
            conflict: false,
            bytes: 0,
        }
    }

    pub fn mark_missing(&mut self, node: NodeId) {
        self.completed_nodes.remove(&node);
        if self.missing_nodes.contains(&node) {
            return;
        }
        if self.missing_nodes.len() == MAX_AUDIT_PEERS {
            self.missing_overflow = self.missing_overflow.saturating_add(1);
        } else {
            self.missing_nodes.insert(node);
        }
    }

    fn mark_complete(&mut self, node: NodeId) -> bool {
        self.missing_nodes.remove(&node);
        if self.completed_nodes.contains(&node) {
            return true;
        }
        if self.completed_nodes.len() == MAX_AUDIT_PEERS {
            self.missing_overflow = self.missing_overflow.saturating_add(1);
            return false;
        }
        self.completed_nodes.insert(node);
        true
    }

    pub fn add_page(
        &mut self,
        node: NodeId,
        page: AuditPageResponse,
        request: &AuditPageRequest,
    ) -> Result<(), AuditPageError> {
        if let Err(error) = validate_page(request, &page) {
            self.mark_missing(node);
            return Err(error);
        }
        if !self.mark_complete(node) {
            return Err(AuditPageError::TooManyPeers);
        }
        if page.next_start_after.is_some() {
            let marker = page
                .records
                .last()
                .map(|entry| entry.key.clone())
                .ok_or(AuditPageError::InvalidMarker)?;
            self.horizon = Some(match self.horizon.take() {
                Some(current) => current.min(marker),
                None => marker,
            });
        }

        for entry in page.records {
            let entry_bytes = entry.key.len()
                + postcard::to_allocvec(&entry.record)
                    .map_err(|_| AuditPageError::TooLarge)?
                    .len();
            if self.bytes.saturating_add(entry_bytes) > MAX_AUDIT_BATCH_BYTES {
                self.mark_missing(node);
                return Err(AuditPageError::TooLarge);
            }
            match self.records.entry(entry.key.clone()) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    self.bytes += entry_bytes;
                    slot.insert(AuditBatchEntry {
                        source: node,
                        entry,
                    });
                }
                std::collections::btree_map::Entry::Occupied(mut slot) => {
                    if slot.get().entry.record == entry.record {
                        if node.as_bytes() < slot.get().source.as_bytes() {
                            slot.get_mut().source = node;
                        }
                    } else {
                        self.conflict = true;
                        let candidate = postcard::to_allocvec(&entry.record)
                            .map_err(|_| AuditPageError::TooLarge)?;
                        let current = postcard::to_allocvec(&slot.get().entry.record)
                            .map_err(|_| AuditPageError::TooLarge)?;
                        let replace = (candidate, node.as_bytes().to_vec())
                            < (current, slot.get().source.as_bytes().to_vec());
                        if replace {
                            let current_bytes = slot.get().entry.key.len()
                                + postcard::to_allocvec(&slot.get().entry.record)
                                    .map_err(|_| AuditPageError::TooLarge)?
                                    .len();
                            if entry_bytes > current_bytes
                                && self.bytes.saturating_add(entry_bytes - current_bytes)
                                    > MAX_AUDIT_BATCH_BYTES
                            {
                                self.mark_missing(node);
                                continue;
                            }
                            self.bytes = self.bytes.saturating_sub(current_bytes);
                            self.bytes = self.bytes.saturating_add(entry_bytes);
                            slot.insert(AuditBatchEntry {
                                source: node,
                                entry,
                            });
                        }
                    }
                }
            }
        }
        self.prune();
        Ok(())
    }

    pub fn merge(&mut self, other: AuditPageBatch) {
        self.missing_overflow = self.missing_overflow.saturating_add(other.missing_overflow);
        for node in other.completed_nodes {
            self.mark_complete(node);
        }
        for node in other.missing_nodes {
            self.mark_missing(node);
        }
        self.conflict |= other.conflict;
        if let Some(horizon) = other.horizon {
            self.horizon = Some(match self.horizon.take() {
                Some(current) => current.min(horizon),
                None => horizon,
            });
        }
        for (key, candidate) in other.records {
            let entry_bytes = candidate.entry.key.len()
                + postcard::to_allocvec(&candidate.entry.record)
                    .map(|bytes| bytes.len())
                    .unwrap_or(usize::MAX);
            if self.bytes.saturating_add(entry_bytes) > MAX_AUDIT_BATCH_BYTES {
                self.mark_missing(candidate.source);
                continue;
            }
            match self.records.entry(key) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    self.bytes += entry_bytes;
                    slot.insert(candidate);
                }
                std::collections::btree_map::Entry::Occupied(mut slot) => {
                    if slot.get().entry.record == candidate.entry.record {
                        if candidate.source.as_bytes() < slot.get().source.as_bytes() {
                            slot.get_mut().source = candidate.source;
                        }
                    } else {
                        self.conflict = true;
                        let replace = postcard::to_allocvec(&candidate.entry.record)
                            .unwrap_or_default()
                            .cmp(
                                &postcard::to_allocvec(&slot.get().entry.record)
                                    .unwrap_or_default(),
                            )
                            .then_with(|| {
                                candidate
                                    .source
                                    .as_bytes()
                                    .cmp(slot.get().source.as_bytes())
                            })
                            .is_lt();
                        if replace {
                            let current_bytes = slot.get().entry.key.len()
                                + postcard::to_allocvec(&slot.get().entry.record)
                                    .map(|bytes| bytes.len())
                                    .unwrap_or(usize::MAX);
                            self.bytes = self.bytes.saturating_sub(current_bytes);
                            self.bytes = self.bytes.saturating_add(entry_bytes);
                            slot.insert(candidate);
                        }
                    }
                }
            }
        }
        self.prune();
    }

    fn prune(&mut self) {
        while self.records.len() > self.limit {
            let Some((key, _)) = self.records.last_key_value() else {
                break;
            };
            if self.horizon.as_ref().is_some_and(|bound| key > bound) {
                let key = key.clone();
                if let Some(entry) = self.records.remove(&key) {
                    self.bytes = self.bytes.saturating_sub(
                        entry.entry.key.len()
                            + postcard::to_allocvec(&entry.entry.record)
                                .map(|bytes| bytes.len())
                                .unwrap_or(0),
                    );
                }
                continue;
            }
            break;
        }
        let Some(bound) = self.horizon.as_ref() else {
            return;
        };
        let keys: Vec<Vec<u8>> = self
            .records
            .range((
                std::ops::Bound::Excluded(bound.clone()),
                std::ops::Bound::Unbounded,
            ))
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            if let Some(entry) = self.records.remove(&key) {
                self.bytes = self.bytes.saturating_sub(
                    entry.entry.key.len()
                        + postcard::to_allocvec(&entry.entry.record)
                            .map(|bytes| bytes.len())
                            .unwrap_or(0),
                );
            }
        }
    }
}

impl Default for AuditPageBatch {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AuditPageError {
    #[error("invalid audit key")]
    InvalidKey,
    #[error("invalid audit record")]
    InvalidRecord,
    #[error("audit page is not strictly sorted")]
    Unsorted,
    #[error("audit page exceeds its limit")]
    TooManyRecords,
    #[error("audit page exceeds its byte limit")]
    TooLarge,
    #[error("invalid audit page marker")]
    InvalidMarker,
    #[error("audit peer limit exceeded")]
    TooManyPeers,
}

pub fn validate_page(
    request: &AuditPageRequest,
    page: &AuditPageResponse,
) -> Result<(), AuditPageError> {
    validate_request(request)?;
    if page.records.len() > request.limit || page_bytes(page)? > MAX_AUDIT_PAGE_BYTES {
        return Err(if page.records.len() > request.limit {
            AuditPageError::TooManyRecords
        } else {
            AuditPageError::TooLarge
        });
    }
    let prefix = request_prefix(request);
    let mut previous = request.start_after.as_deref();
    for entry in &page.records {
        if entry.key.len() != AUDIT_KEY_BYTES || !entry.key.starts_with(&prefix) {
            return Err(AuditPageError::InvalidKey);
        }
        if previous.is_some_and(|key| entry.key.as_slice() <= key) {
            return Err(AuditPageError::Unsorted);
        }
        if entry.record.realm_id != request.realm_id
            || entry.record.group_id != request.group_id
            || request
                .document_id
                .is_some_and(|document_id| entry.record.document_id != document_id)
            || entry.record.user_id.realm_id != request.realm_id
            || key_document(&entry.key) != Some(entry.record.document_id)
            || invalid_iri(&entry.record)
        {
            return Err(AuditPageError::InvalidRecord);
        }
        previous = Some(&entry.key);
    }
    if let Some(marker) = &page.next_start_after {
        if marker.len() != AUDIT_KEY_BYTES
            || !marker.starts_with(&prefix)
            || page.records.last().is_none_or(|entry| &entry.key != marker)
        {
            return Err(AuditPageError::InvalidMarker);
        }
    }
    Ok(())
}

pub fn validate_request(request: &AuditPageRequest) -> Result<(), AuditPageError> {
    if request.limit == 0 || request.limit > MAX_AUDIT_RECORDS {
        return Err(AuditPageError::TooManyRecords);
    }
    if request
        .start_after
        .as_ref()
        .is_some_and(|key| key.len() != AUDIT_KEY_BYTES)
    {
        return Err(AuditPageError::InvalidKey);
    }
    if request
        .start_after
        .as_ref()
        .is_some_and(|key| !key.starts_with(&request_prefix(request)))
    {
        return Err(AuditPageError::InvalidKey);
    }
    Ok(())
}

fn request_prefix(request: &AuditPageRequest) -> Vec<u8> {
    let mut prefix = request.group_id.to_bytes().to_vec();
    if let Some(document_id) = request.document_id {
        prefix.extend_from_slice(&document_id.to_bytes());
    }
    prefix
}

fn page_bytes(page: &AuditPageResponse) -> Result<usize, AuditPageError> {
    postcard::to_allocvec(page)
        .map(|bytes| bytes.len())
        .map_err(|_| AuditPageError::TooLarge)
}

fn invalid_iri(record: &MetadataAuditRecord) -> bool {
    record.graph_iri.trim().is_empty() || oxrdf::NamedNode::new(&record.graph_iri).is_err()
}

fn key_document(key: &[u8]) -> Option<Ulid> {
    let bytes = key.get(16..32)?.try_into().ok()?;
    Some(Ulid::from_bytes(bytes))
}
