//! Runs one node's bucket search and drops hits that realm or group policies deny.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::types::{GroupId, Key};

use super::search::{
    BucketPage, BucketSearchHit, SearchBucketsError, SearchBucketsInput, SearchBucketsOperation,
    candidate_path,
};
use crate::auth::enforcement::PolicyBulkOperation;
use crate::auth::enforcement::PolicyEvaluator;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::driver::{DriverContext, drive};
use crate::s3::object::search_scan::{open_position, seal_position};

/// Runs one node's bucket search, dropping hits denied by realm/group policies.
/// Policy state is read once per candidate group; unreadable groups stay invisible,
/// and scans continue so hidden buckets cannot shorten the page.
pub async fn search_local_buckets(
    context: &DriverContext,
    input: SearchBucketsInput,
) -> Result<BucketPage, SearchBucketsError> {
    search_with_budget(context, input, SearchBucketsOperation::MAX_SCAN_ROWS).await
}

pub(super) async fn search_with_budget(
    context: &DriverContext,
    input: SearchBucketsInput,
    max_scan_rows: usize,
) -> Result<BucketPage, SearchBucketsError> {
    if input
        .extras
        .as_ref()
        .is_some_and(|extras| !extras_valid(extras))
    {
        return Err(SearchBucketsError::Extras);
    }
    let limit = input.limit.clamp(1, SearchBucketsOperation::MAX_LIMIT);
    let mut evaluators: HashMap<(RealmId, GroupId), PolicyEvaluator> = HashMap::new();
    let mut visible: Vec<BucketSearchHit> = Vec::with_capacity(limit);
    // A cut scan's position comes back sealed, as it may name a bucket the caller cannot read.
    let opened = (input.start_after.as_ref()).map(|after| open_position(after));
    let Ok(opened) = opened.transpose() else {
        return Ok(BucketPage {
            retired: true,
            ..BucketPage::default()
        });
    };
    let mut start_after = opened.map(Key::from);
    let mut scanned_rows = 0;
    let budget = max_scan_rows.min(SearchBucketsOperation::MAX_SCAN_ROWS);
    loop {
        let remaining = budget.saturating_sub(scanned_rows);
        let output = drive(
            SearchBucketsOperation::with_budget(
                SearchBucketsInput {
                    start_after,
                    ..input.clone()
                },
                remaining,
            ),
            context,
        )
        .await?;
        scanned_rows += output.scanned_rows;
        let pending = output
            .hits
            .iter()
            .map(|hit| hit.group_id)
            .filter(|group_id| !evaluators.contains_key(&(input.realm_id, *group_id)))
            .map(|group_id| (input.realm_id, group_id))
            .collect::<Vec<_>>();
        evaluators.extend(drive(PolicyBulkOperation::new(pending), context).await?);
        for mut hit in output.hits {
            if !policy_allows(&evaluators, &input, &hit) {
                continue;
            }
            if input.database {
                hit.public = anonymous_reads(context, &input, &hit).await;
            }
            visible.push(hit);
            if visible.len() >= limit {
                return Ok(BucketPage {
                    hits: visible,
                    ..BucketPage::default()
                });
            }
        }
        match output.next_start_after {
            Some(key) if scanned_rows >= budget => {
                let scanned_to = seal_position(&key).map_err(|_| SearchBucketsError::Position)?;
                return Ok(BucketPage {
                    hits: visible,
                    scanned_to: Some(scanned_to),
                    retired: false,
                });
            }
            Some(key) => start_after = Some(key),
            None => {
                return Ok(BucketPage {
                    hits: visible,
                    ..BucketPage::default()
                });
            }
        }
    }
}

/// Whether a caller's policy context may join a bucket search: an MCP tool's operation and
/// parameters only, so it adds a check and never stands in for the mandatory bucket read.
pub(super) fn extras_valid(extras: &PolicyRequestExtras) -> bool {
    let tool = extras.operation.strip_prefix("mcp:");
    tool.is_some_and(|tool| !tool.is_empty()) && extras.headers.is_empty() && extras.body.is_none()
}

/// Whether a caller without a login may read the bucket, deny policies included.
async fn anonymous_reads(
    context: &DriverContext,
    input: &SearchBucketsInput,
    hit: &BucketSearchHit,
) -> bool {
    let path = bucket_permission_path(input.realm_id, hit.group_id, hit.node_id, &hit.bucket);
    let anonymous = AuthContext::anonymous(input.realm_id);
    let extras = PolicyRequestExtras::operation("s3.ListBuckets");
    drive(
        AuthorizeOperation::new(input.realm_id, &anonymous, &path, &Permission::READ, extras),
        context,
    )
    .await
    .is_ok()
}

/// Evaluates the loaded policies for one hit on the path the RBAC check used,
/// under the S3 bucket-read action so one policy covers both read surfaces. A
/// group with no loaded evaluator fails closed.
fn policy_allows(
    evaluators: &HashMap<(RealmId, GroupId), PolicyEvaluator>,
    input: &SearchBucketsInput,
    hit: &BucketSearchHit,
) -> bool {
    let path = candidate_path(input, hit.group_id, hit.node_id, &hit.bucket);
    let mandatory = PolicyRequestExtras::operation("s3.ListBuckets");
    let checks = std::iter::once(mandatory).chain(input.extras.clone());
    let evaluator = evaluators.get(&(input.realm_id, hit.group_id));
    checks.into_iter().all(|extras| {
        let request = policy_request_with(&path, &Permission::READ, Some(&input.auth), extras);
        evaluator.is_some_and(|evaluator| evaluator.evaluate(&request).is_ok())
    })
}
