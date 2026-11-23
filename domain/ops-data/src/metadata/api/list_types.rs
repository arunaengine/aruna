//! Holds the visible document list types, its limits and the pending record merge.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    AuthContext, Deserialize, GroupId, HashMap, MetadataApiError, MetadataRegistryRecord,
    REGISTRY_CANDIDATE_LIMIT, Serialize,
};

pub(super) const LIST_METADATA_LIMIT: usize = 50;

pub(super) const MAX_METADATA_LIMIT: usize = 1_000;

/// Bounds the response payload and the number of RO-Crate summary exports an
/// unauthenticated caller can force per request. The realm-wide registry scan
/// is removed by the cached list path, not by this clamp.
pub(super) const ANONYMOUS_METADATA_LIMIT: usize = 100;

/// Splits a targeted lookup from a browse page: the portal pages at 48, a
/// run-crate or preview lookup at 1, and only a browse page pays the estimate.
pub(super) const ESTIMATE_MIN_LIMIT: usize = 24;

/// Order the visible metadata listing is paginated in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataListOrder {
    /// Ascending document id, which is creation order for ULID ids.
    #[default]
    Created,
    /// Descending `updated_at_ms`, tie-broken by descending document id.
    Recent,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListVisibleRequest {
    pub group_id: Option<GroupId>,
    pub path_prefix: Option<String>,
    pub include_summary: bool,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub order: MetadataListOrder,
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListedMetadataDocument {
    pub record: MetadataRegistryRecord,
    pub rocrate_summary_jsonld: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListVisibleResult {
    pub documents: Vec<ListedMetadataDocument>,
    pub limit: usize,
    pub offset: usize,
    pub total_returned: usize,
    /// Approximate number of matching documents across all pages; group-granular,
    /// so it may over- or under-count glob read rules. `None` when not computed
    /// for a request too small to be a browse page.
    pub total_estimate: Option<usize>,
}

pub(super) fn effective_list_limit(requested: Option<usize>, anonymous: bool) -> usize {
    let maximum = if anonymous {
        ANONYMOUS_METADATA_LIMIT
    } else {
        MAX_METADATA_LIMIT
    };
    requested.unwrap_or(LIST_METADATA_LIMIT).clamp(1, maximum)
}

pub(super) fn check_policy_limit(
    group_ids: Vec<GroupId>,
) -> Result<Vec<GroupId>, MetadataApiError> {
    if group_ids.len() > REGISTRY_CANDIDATE_LIMIT {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    Ok(group_ids)
}

pub(super) fn merge_pending_records(
    records: &mut Vec<MetadataRegistryRecord>,
    pending_records: Vec<MetadataRegistryRecord>,
) {
    let mut positions = records
        .iter()
        .enumerate()
        .map(|(index, record)| (record.document_id, index))
        .collect::<HashMap<_, _>>();

    for pending_record in pending_records {
        if let Some(&index) = positions.get(&pending_record.document_id) {
            let existing_record = &records[index];
            if (pending_record.updated_at_ms, pending_record.last_event_id)
                > (existing_record.updated_at_ms, existing_record.last_event_id)
            {
                records[index] = pending_record;
            }
        } else {
            positions.insert(pending_record.document_id, records.len());
            records.push(pending_record);
        }
    }
}
