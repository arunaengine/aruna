//! Serves the usage route and loads usage counters for realms and groups.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::UnrestrictedAuth;
use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::realm::QuotaConfig;
use aruna_core::structs::storage::usage::{USAGE_GLOBAL_KEY, UsageCounters};
use aruna_ops_data::metadata::realm_counts::RealmDocumentsOperation;
use aruna_ops_data::s3::realm_usage::{RealmUsage, RealmUsageOperation, RealmUsageScope};
use aruna_ops_data::s3::usage::LoadCountersOperation;
use aruna_ops_work::federation::totals::DataTotalsOperation;
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::warn;
use utoipa::ToSchema;

/// Storage usage. The flat fields report this node's local counters, `realm` the
/// realm-wide total summed across every node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct UsageResponse {
    pub buckets: u64,
    pub objects: u64,
    /// Physical blob copies this node holds. Copies are content-addressed and
    /// shared between the groups referencing them, so they are attributed to the
    /// node, never to a group: the group usage endpoint omits this field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stored_blobs: Option<u64>,
    /// Bytes those copies occupy. Omitted wherever `stored_blobs` is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stored_bytes: Option<u64>,
    pub logical_bytes: u64,
    pub referenced_bytes: u64,
    pub realm: UsageTotals,
    /// Realm-wide live metadata document count, excluding lifecycle-deleted ones.
    /// Not filtered per caller and absent (never zero) on the group usage endpoint
    /// and on nodes without a metadata subsystem.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_documents: Option<u64>,
    /// Exact lifecycle-live metadata documents whose root is neither a Profile
    /// nor a Process Run. Present only on the group usage endpoint when all
    /// purpose counts can be computed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_count: Option<u64>,
    /// Exact lifecycle-live metadata documents whose root `@type` contains the
    /// W3C Profiles Vocabulary Profile IRI. Group usage only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_count: Option<u64>,
    /// Exact lifecycle-live non-Profile documents whose root conforms to the
    /// bundled Process Run Crate profile. Group usage only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_run_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota: Option<GroupQuotaStatus>,
}

/// Per-group quota status derived from the realm quota config. Attached only to
/// the group usage endpoint; `/info/usage` and the plain constructors leave it
/// `None` so their output is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GroupQuotaStatus {
    /// Effective pre-grace group quota (override else default). `None` = unlimited.
    pub quota_bytes: Option<u64>,
    /// Enforced hard cap (quota x grace). `None` = unlimited.
    pub ceiling_bytes: Option<u64>,
    pub warn_threshold_percent: u32,
    /// True when the group's realm-wide `logical_bytes` has reached the
    /// fractional `quota_bytes * warn_threshold_percent / 100` threshold; always
    /// false when unlimited.
    pub warning: bool,
}

impl GroupQuotaStatus {
    /// Builds the status from the realm quota config and the group's realm-wide
    /// `logical_bytes`, the same counter the put-object `QuotaGate` enforces.
    pub fn resolve(
        quota: &QuotaConfig,
        group_id: &aruna_core::types::GroupId,
        group_logical_bytes: u64,
    ) -> Self {
        let quota_bytes = quota.group_quota_bytes(group_id);
        let warning = match quota_bytes {
            Some(limit) => {
                u128::from(group_logical_bytes) * 100
                    >= u128::from(limit) * u128::from(quota.warn_threshold_percent)
            }
            None => false,
        };
        Self {
            quota_bytes,
            ceiling_bytes: quota.effective_group_ceiling(group_id),
            warn_threshold_percent: quota.warn_threshold_percent,
            warning,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UsageTotals {
    /// Bucket counters summed over the nodes: a distributed bucket counts once per member node.
    pub buckets: u64,
    /// `buckets` with each distributed bucket counted once, from this node's records of the sets;
    /// omitted when they cannot all be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distinct_buckets: Option<u64>,
    pub objects: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stored_blobs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stored_bytes: Option<u64>,
    pub logical_bytes: u64,
    pub referenced_bytes: u64,
}

impl From<UsageCounters> for UsageTotals {
    fn from(counters: UsageCounters) -> Self {
        Self {
            buckets: counters.buckets,
            distinct_buckets: None,
            objects: counters.objects,
            stored_blobs: Some(counters.stored_blobs),
            stored_bytes: Some(counters.stored_bytes),
            logical_bytes: counters.logical_bytes,
            referenced_bytes: counters.referenced_bytes,
        }
    }
}

impl UsageTotals {
    fn without_stored(counters: UsageCounters) -> Self {
        Self {
            stored_blobs: None,
            stored_bytes: None,
            ..Self::from(counters)
        }
    }
}

impl UsageResponse {
    pub fn new(local: UsageCounters, realm: UsageCounters) -> Self {
        Self {
            buckets: local.buckets,
            objects: local.objects,
            stored_blobs: Some(local.stored_blobs),
            stored_bytes: Some(local.stored_bytes),
            logical_bytes: local.logical_bytes,
            referenced_bytes: local.referenced_bytes,
            realm: realm.into(),
            metadata_documents: None,
            dataset_count: None,
            profile_count: None,
            process_run_count: None,
            quota: None,
        }
    }

    /// Group scope. Physical copies are content-addressed and shared between
    /// groups, so the counters carry no group dimension and the `stored_*`
    /// fields are omitted instead of reported as an unattributed zero.
    pub fn for_group(local: UsageCounters, realm: UsageCounters) -> Self {
        Self {
            stored_blobs: None,
            stored_bytes: None,
            realm: UsageTotals::without_stored(realm),
            ..Self::new(local, realm)
        }
    }
}

pub async fn load_usage_counters(state: &ServerState, key: Vec<u8>) -> ServerResult<UsageCounters> {
    drive(LoadCountersOperation::new(key), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.into()))
}

/// The realm or group bucket total with each distributed bucket once; `None` when this node cannot
/// read all its set records or a set of the scope is being deleted, so no guess passes as exact.
pub(crate) async fn distinct_buckets(
    state: &ServerState,
    group: Option<ulid::Ulid>,
    buckets: u64,
) -> Option<u64> {
    let count = aruna_ops_data::distribution::counts::SetCountsOperation::new(group);
    match aruna_runner::driver::drive(count, &state.get_ctx()).await {
        Ok(counts) => counts.distinct(buckets),
        Err(error) => {
            warn!(error = %error, "distributed bucket counts unavailable for usage response");
            None
        }
    }
}

/// The realm's current objects and data bytes, stored and referenced alike, private data
/// included; aggregates only, so no bucket or dataset is named. `None` when unknown here.
pub(crate) async fn realm_totals(state: &ServerState) -> Option<(u64, u64)> {
    let context = state.get_ctx();
    let realm_id = context.node_and_realm().map(|(_, realm_id)| realm_id);
    drive(
        DataTotalsOperation::new(state.get_node_id(), realm_id),
        context.as_ref(),
    )
    .await
    .inspect_err(|error| warn!(error = %error, "realm usage unavailable for the realm totals"))
    .ok()
}

pub async fn load_realm_usage(
    state: &ServerState,
    scope: RealmUsageScope,
) -> ServerResult<RealmUsage> {
    let context = state.get_ctx();
    let realm_id = context.node_and_realm().map(|(_, realm_id)| realm_id);
    let operation = RealmUsageOperation::new(state.get_node_id(), realm_id, scope);
    let mut totals = drive(operation, context.as_ref())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string().into()))?;
    totals
        .pop()
        .ok_or_else(|| ServerError::InternalError("no realm usage total".into()))
}

#[utoipa::path(
    get,
    path = "/system/usage",
    tag = "system/info",
    summary = "Report this node's and the realm's storage usage",
    description = r#"Reports this node's own storage counters together with the realm-wide totals.

**Authentication**: realm bearer token without path restrictions. No further permission is
checked, because the figures are realm-wide totals and not per-caller views.

**Behavior**
- The flat fields are this node's own counters, while `realm` is the total summed from every realm
  node's replicated usage snapshot, so it is eventually consistent: a node whose snapshot has not
  arrived or has not refreshed yet is simply not part of the sum, which can make the total lag
  reality after a burst of writes.
- `realm.objects`, `realm.logical_bytes` and `realm.referenced_bytes` count each object once: the
  copies other member nodes hold of a distributed bucket's key range are left out, so the node
  coordinating the range counts it. The flat fields count everything this node holds.
- `stored_blobs` and `stored_bytes` count physical, content-addressed blob copies. A copy is shared
  by every group referencing it, so it is attributed to this node and its backend, never to a group;
  the group usage endpoint therefore omits both fields.
- `realm.buckets` sums the nodes' bucket counters, so a distributed bucket counts once per member
  node; `realm.distinct_buckets` counts it once, from this node's records of the distributed
  buckets. It is omitted when they cannot all be read, and while a distributed bucket is being
  deleted, because its members purge their buckets at their own pace.
- `metadata_documents` counts the realm's live metadata documents, excluding lifecycle-deleted ones,
  and is deliberately unfiltered by what the caller may read. It is omitted, never zeroed, when this
  node has no metadata subsystem or the count cannot be produced, so an absent field means unknown.
- `quota` is not reported here, it belongs to the per-group usage view."#,
    responses(
        (
            status = 200,
            description = "This node's counters plus the realm-wide totals",
            body = UsageResponse,
            example = json!({
                "buckets": 4,
                "objects": 128,
                "stored_blobs": 130,
                "stored_bytes": 1073741824,
                "logical_bytes": 1099511627776_i64,
                "referenced_bytes": 2147483648_i64,
                "realm": {
                    "buckets": 12,
                    "distinct_buckets": 10,
                    "objects": 512,
                    "stored_blobs": 530,
                    "stored_bytes": 4294967296_i64,
                    "logical_bytes": 4398046511104_i64,
                    "referenced_bytes": 8589934592_i64
                },
                "metadata_documents": 4096
            })
        ),
        (status = 403, description = "Token is path-restricted, or the caller is not a member of this realm", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_usage(
    State(state): State<Arc<ServerState>>,
    _: UnrestrictedAuth,
) -> ServerResult<(StatusCode, Json<UsageResponse>)> {
    let local = load_usage_counters(&state, USAGE_GLOBAL_KEY.to_vec()).await?;
    let realm = load_realm_usage(&state, RealmUsageScope::Global).await?;
    let mut response = UsageResponse::new(local, realm.counters);
    response.realm.distinct_buckets = distinct_buckets(&state, None, response.realm.buckets).await;
    // Best effort: storage counters stay reportable when the metadata
    // subsystem cannot answer, and the omitted field never reads as zero.
    response.metadata_documents = match drive(
        RealmDocumentsOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(count) => count,
        Err(error) => {
            warn!(error = %error, "metadata document count unavailable for usage response");
            None
        }
    };
    Ok((StatusCode::OK, Json(response)))
}
