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

enum PageDecision {
    Insert,
    Replace,
    UpdateSource,
    Ignore,
}

struct AuditPagePlan {
    decisions: Vec<PageDecision>,
    bytes: usize,
    horizon: Option<Vec<u8>>,
    conflict: bool,
}

struct AuditEntryPlan {
    decision: PageDecision,
    bytes: usize,
    conflict: bool,
}

struct AuditMergePlan {
    decisions: Vec<PageDecision>,
    bytes: usize,
    conflict: bool,
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
        let plan = match self.preflight_page(node, &page) {
            Ok(plan) => plan,
            Err(error) => {
                self.mark_missing(node);
                return Err(error);
            }
        };
        self.apply_page(node, page, plan);
        Ok(())
    }

    fn preflight_page(
        &self,
        node: NodeId,
        page: &AuditPageResponse,
    ) -> Result<AuditPagePlan, AuditPageError> {
        if !self.completed_nodes.contains(&node) && self.completed_nodes.len() == MAX_AUDIT_PEERS {
            return Err(AuditPageError::TooManyPeers);
        }
        let horizon = page.next_start_after.as_ref().and_then(|marker| {
            if self
                .horizon
                .as_ref()
                .is_none_or(|current| marker.as_slice() < current.as_slice())
            {
                Some(marker.clone())
            } else {
                None
            }
        });
        let mut bytes = self.bytes;
        let mut conflict = false;
        let mut decisions = Vec::with_capacity(page.records.len());
        for entry in &page.records {
            let plan = self.plan_entry(node, entry, bytes)?;
            bytes = plan.bytes;
            conflict |= plan.conflict;
            decisions.push(plan.decision);
        }
        Ok(AuditPagePlan {
            decisions,
            bytes,
            horizon,
            conflict,
        })
    }

    fn plan_entry(
        &self,
        node: NodeId,
        entry: &AuditPageEntry,
        bytes: usize,
    ) -> Result<AuditEntryPlan, AuditPageError> {
        let record_bytes =
            postcard::to_allocvec(&entry.record).map_err(|_| AuditPageError::TooLarge)?;
        let entry_bytes = entry.key.len() + record_bytes.len();
        let Some(current) = self.records.get(&entry.key) else {
            if bytes.saturating_add(entry_bytes) > MAX_AUDIT_BATCH_BYTES {
                return Err(AuditPageError::TooLarge);
            }
            return Ok(AuditEntryPlan {
                decision: PageDecision::Insert,
                bytes: bytes + entry_bytes,
                conflict: false,
            });
        };
        if current.entry.record == entry.record {
            return Ok(AuditEntryPlan {
                decision: if node.as_bytes() < current.source.as_bytes() {
                    PageDecision::UpdateSource
                } else {
                    PageDecision::Ignore
                },
                bytes,
                conflict: false,
            });
        }
        let current_bytes =
            postcard::to_allocvec(&current.entry.record).map_err(|_| AuditPageError::TooLarge)?;
        let replace = record_bytes.as_slice() < current_bytes.as_slice()
            || (record_bytes.as_slice() == current_bytes.as_slice()
                && node.as_bytes() < current.source.as_bytes());
        if !replace {
            return Ok(AuditEntryPlan {
                decision: PageDecision::Ignore,
                bytes,
                conflict: true,
            });
        }
        let current_size = current.entry.key.len() + current_bytes.len();
        if entry_bytes > current_size
            && bytes.saturating_add(entry_bytes - current_size) > MAX_AUDIT_BATCH_BYTES
        {
            return Err(AuditPageError::TooLarge);
        }
        Ok(AuditEntryPlan {
            decision: PageDecision::Replace,
            bytes: bytes.saturating_sub(current_size) + entry_bytes,
            conflict: true,
        })
    }

    fn apply_page(&mut self, node: NodeId, page: AuditPageResponse, plan: AuditPagePlan) {
        self.missing_nodes.remove(&node);
        self.completed_nodes.insert(node);
        if let Some(horizon) = plan.horizon {
            self.horizon = Some(horizon);
        }
        self.conflict |= plan.conflict;
        for (entry, decision) in page.records.into_iter().zip(plan.decisions) {
            self.apply_entry(node, entry, decision);
        }
        self.bytes = plan.bytes;
        self.prune();
    }

    fn apply_entry(&mut self, node: NodeId, entry: AuditPageEntry, decision: PageDecision) {
        match decision {
            PageDecision::Insert | PageDecision::Replace => {
                self.records.insert(
                    entry.key.clone(),
                    AuditBatchEntry {
                        source: node,
                        entry,
                    },
                );
            }
            PageDecision::UpdateSource => {
                if let Some(current) = self.records.get_mut(&entry.key) {
                    current.source = node;
                }
            }
            PageDecision::Ignore => {}
        }
    }

    pub fn merge(&mut self, other: AuditPageBatch) {
        let plan = match self.preflight_merge(&other) {
            Ok(plan) => plan,
            Err(_) => {
                self.fail_merge(other);
                return;
            }
        };
        self.missing_overflow = self.missing_overflow.saturating_add(other.missing_overflow);
        for node in other.completed_nodes {
            self.mark_complete(node);
        }
        for node in other.missing_nodes {
            self.mark_missing(node);
        }
        if let Some(horizon) = other.horizon {
            self.horizon = Some(match self.horizon.take() {
                Some(current) => current.min(horizon),
                None => horizon,
            });
        }
        self.conflict |= other.conflict || plan.conflict;
        for (candidate, decision) in other.records.into_values().zip(plan.decisions) {
            self.apply_entry(candidate.source, candidate.entry, decision);
        }
        self.bytes = plan.bytes;
        self.prune();
    }

    fn preflight_merge(&self, other: &AuditPageBatch) -> Result<AuditMergePlan, AuditPageError> {
        let mut completed = self.completed_nodes.len();
        for node in &other.completed_nodes {
            if !self.completed_nodes.contains(node) {
                if completed == MAX_AUDIT_PEERS {
                    return Err(AuditPageError::TooManyPeers);
                }
                completed += 1;
            }
        }
        let mut bytes = self.bytes;
        let mut conflict = false;
        let mut decisions = Vec::with_capacity(other.records.len());
        for candidate in other.records.values() {
            let plan = self.plan_entry(candidate.source, &candidate.entry, bytes)?;
            bytes = plan.bytes;
            conflict |= plan.conflict;
            decisions.push(plan.decision);
        }
        Ok(AuditMergePlan {
            decisions,
            bytes,
            conflict,
        })
    }

    fn fail_merge(&mut self, other: AuditPageBatch) {
        self.missing_overflow = self.missing_overflow.saturating_add(other.missing_overflow);
        for node in other.completed_nodes {
            self.mark_missing(node);
        }
        for node in other.missing_nodes {
            self.mark_missing(node);
        }
        for candidate in other.records.into_values() {
            self.mark_missing(candidate.source);
        }
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
    if let Some(marker) = &page.next_start_after
        && (marker.len() != AUDIT_KEY_BYTES
            || !marker.starts_with(&prefix)
            || page.records.last().is_none_or(|entry| &entry.key != marker))
    {
        return Err(AuditPageError::InvalidMarker);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::MetadataAuditOperation;
    use crate::types::UserId;

    fn key(group_id: GroupId, document_id: Ulid, audit_id: Ulid) -> Vec<u8> {
        let mut key = Vec::with_capacity(AUDIT_KEY_BYTES);
        key.extend_from_slice(&group_id.to_bytes());
        key.extend_from_slice(&document_id.to_bytes());
        key.extend_from_slice(&audit_id.to_bytes());
        key
    }

    fn entry(
        group_id: GroupId,
        document_id: Ulid,
        audit_id: Ulid,
        realm_id: RealmId,
        node: NodeId,
    ) -> AuditPageEntry {
        AuditPageEntry {
            key: key(group_id, document_id, audit_id),
            record: MetadataAuditRecord {
                realm_id,
                group_id,
                document_id,
                graph_iri: "urn:test".to_string(),
                user_id: UserId::local(Ulid::from_bytes([1u8; 16]), realm_id),
                node_id: node,
                operation: MetadataAuditOperation::Create,
                occurred_at_ms: 1,
                details: Some("detail".to_string()),
            },
        }
    }

    fn request(realm_id: RealmId, group_id: GroupId, document_id: Ulid) -> AuditPageRequest {
        AuditPageRequest {
            auth_token: None,
            config_digest: [0u8; 32],
            realm_id,
            group_id,
            document_id: Some(document_id),
            start_after: None,
            limit: MAX_AUDIT_RECORDS,
        }
    }

    #[test]
    fn overflow_page_atomic() {
        let realm_id = RealmId([1u8; 32]);
        let group_id = Ulid::from_bytes([2u8; 16]);
        let document_id = Ulid::from_bytes([3u8; 16]);
        let node = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let request = request(realm_id, group_id, document_id);
        let first = entry(
            group_id,
            document_id,
            Ulid::from_bytes([1u8; 16]),
            realm_id,
            node,
        );
        let second = entry(
            group_id,
            document_id,
            Ulid::from_bytes([2u8; 16]),
            realm_id,
            node,
        );
        let mut batch = AuditPageBatch::new();
        let first_bytes = first.key.len() + postcard::to_allocvec(&first.record).unwrap().len();
        // Seed accounting just below the cap so the second entry would overflow.
        batch.bytes = MAX_AUDIT_BATCH_BYTES - first_bytes;
        let bytes = batch.bytes;

        let error = batch.add_page(
            node,
            AuditPageResponse {
                records: vec![first, second.clone()],
                next_start_after: Some(second.key),
            },
            &request,
        );

        assert_eq!(error, Err(AuditPageError::TooLarge));
        assert!(batch.records.is_empty());
        assert_eq!(batch.bytes, bytes);
        assert!(batch.horizon.is_none());
        assert!(!batch.completed_nodes.contains(&node));
        assert!(batch.missing_nodes.contains(&node));
    }

    #[test]
    fn chunk_overflow_atomic() {
        let realm_id = RealmId([1u8; 32]);
        let group_id = Ulid::from_bytes([2u8; 16]);
        let document_id = Ulid::from_bytes([3u8; 16]);
        let node = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let request = request(realm_id, group_id, document_id);
        let first = entry(
            group_id,
            document_id,
            Ulid::from_bytes([1u8; 16]),
            realm_id,
            node,
        );
        let second = entry(
            group_id,
            document_id,
            Ulid::from_bytes([2u8; 16]),
            realm_id,
            node,
        );
        let first_bytes = first.key.len() + postcard::to_allocvec(&first.record).unwrap().len();
        let mut batch = AuditPageBatch::new();
        // Seed accounting just below the cap so the second chunk entry overflows.
        batch.bytes = MAX_AUDIT_BATCH_BYTES - first_bytes;
        let bytes = batch.bytes;
        let existing = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        batch.mark_missing(existing);
        let mut chunk = AuditPageBatch::new();
        chunk
            .add_page(
                node,
                AuditPageResponse {
                    records: vec![first, second.clone()],
                    next_start_after: Some(second.key),
                },
                &request,
            )
            .unwrap();

        batch.merge(chunk);

        assert!(batch.records.is_empty());
        assert_eq!(batch.bytes, bytes);
        assert!(batch.horizon.is_none());
        assert!(!batch.completed_nodes.contains(&node));
        assert!(batch.missing_nodes.contains(&node));
        assert!(batch.missing_nodes.contains(&existing));
    }

    #[test]
    fn duplicate_at_cap() {
        let realm_id = RealmId([1u8; 32]);
        let group_id = Ulid::from_bytes([2u8; 16]);
        let document_id = Ulid::from_bytes([3u8; 16]);
        let node = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let request = request(realm_id, group_id, document_id);
        let duplicate = entry(
            group_id,
            document_id,
            Ulid::from_bytes([1u8; 16]),
            realm_id,
            node,
        );
        let mut batch = AuditPageBatch::new();
        batch
            .add_page(
                node,
                AuditPageResponse {
                    records: vec![duplicate.clone()],
                    next_start_after: None,
                },
                &request,
            )
            .unwrap();
        // A duplicate must not consume capacity when the aggregate is full.
        batch.bytes = MAX_AUDIT_BATCH_BYTES;

        assert!(
            batch
                .add_page(
                    node,
                    AuditPageResponse {
                        records: vec![duplicate],
                        next_start_after: None,
                    },
                    &request,
                )
                .is_ok()
        );
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.bytes, MAX_AUDIT_BATCH_BYTES);
        assert!(batch.completed_nodes.contains(&node));
        assert!(batch.missing_nodes.is_empty());
    }
}
