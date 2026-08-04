//! Contract for gathering a group's metadata audit trail across realm nodes.
//! Audit rows are node-local projections, so a complete trail is assembled by
//! asking every eligible node for its local page and merging the results.

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::metadata::MetadataAuthToken;
use crate::structs::MetadataAuditRecord;
use crate::types::GroupId;

/// A single realm node's local audit page request. Carries the caller's
/// authority so the serving node re-checks group-admin access before answering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditPageRequest {
    pub auth_token: Option<MetadataAuthToken>,
    pub config_digest: [u8; 32],
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
