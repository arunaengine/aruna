//! Runs one node's bucket search and drops hits that realm or group policies deny.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::types::Key;

use super::search::{
    BucketPage, BucketSearchHit, SearchBucketsError, SearchBucketsInput, SearchBucketsOperation,
    SearchBucketsOutput, candidate_path,
};
use crate::auth::enforcement::{PolicyBulkOperation, PolicyEvaluators};
use crate::auth::error::{AuthorizeError, PolicyEnforcementError};
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::s3::object::search_scan::{open_position, seal_position};

#[derive(Debug, PartialEq)]
enum LocalStep {
    Scan,
    LoadPolicies,
    CheckAnonymous,
}

type LocalTurn = Result<Next<BucketPage>, SearchBucketsError>;

/// Runs one node's bucket search, dropping hits denied by realm/group policies.
/// Policy state is read once per candidate group; unreadable groups stay invisible,
/// and scans continue so hidden buckets cannot shorten the page.
#[derive(Debug, PartialEq)]
pub struct LocalBucketSearch {
    input: SearchBucketsInput,
    budget: usize,
    limit: usize,
    step: LocalStep,
    evaluators: PolicyEvaluators,
    visible: Vec<BucketSearchHit>,
    start_after: Option<Key>,
    scanned_rows: usize,
    /// The scan's hits still to decide, and the position it stopped at.
    hits: VecDeque<BucketSearchHit>,
    next_start_after: Option<Key>,
    /// A readable hit of a database search, waiting for the anonymous check.
    checking: Option<BucketSearchHit>,
}

impl LocalBucketSearch {
    pub fn new(input: SearchBucketsInput) -> Self {
        Self::with_budget(input, SearchBucketsOperation::MAX_SCAN_ROWS)
    }

    pub(super) fn with_budget(input: SearchBucketsInput, max_scan_rows: usize) -> Self {
        Self {
            limit: input.limit.clamp(1, SearchBucketsOperation::MAX_LIMIT),
            budget: max_scan_rows.min(SearchBucketsOperation::MAX_SCAN_ROWS),
            input,
            step: LocalStep::Scan,
            evaluators: PolicyEvaluators::new(),
            visible: Vec::new(),
            start_after: None,
            scanned_rows: 0,
            hits: VecDeque::new(),
            next_start_after: None,
            checking: None,
        }
    }

    fn page(&mut self, scanned_to: Option<Vec<u8>>) -> LocalTurn {
        Ok(Next::Done(BucketPage {
            hits: std::mem::take(&mut self.visible),
            scanned_to,
            retired: false,
        }))
    }

    fn scan(&mut self) -> LocalTurn {
        let remaining = self.budget.saturating_sub(self.scanned_rows);
        let input = SearchBucketsInput {
            start_after: self.start_after.clone(),
            ..self.input.clone()
        };
        self.step = LocalStep::Scan;
        let search = SearchBucketsOperation::with_budget(input, remaining);
        Ok(Next::Call(IoCall::child(search)))
    }

    fn scanned(&mut self, output: SearchBucketsOutput) -> LocalTurn {
        self.scanned_rows += output.scanned_rows;
        self.next_start_after = output.next_start_after;
        self.hits = output.hits.into();
        let realm_id = self.input.realm_id;
        let pending: Vec<_> = (self.hits.iter())
            .map(|hit| (realm_id, hit.group_id))
            .filter(|scope| !self.evaluators.contains_key(scope))
            .collect();
        if pending.is_empty() {
            return self.decide_hits();
        }
        self.step = LocalStep::LoadPolicies;
        let load = PolicyBulkOperation::new(pending);
        Ok(Next::Call(IoCall::child(load)))
    }

    fn decide_hits(&mut self) -> LocalTurn {
        while let Some(hit) = self.hits.pop_front() {
            if !self.policy_allows(&hit) {
                continue;
            }
            if self.input.database {
                let check = self.anonymous_check(&hit);
                self.checking = Some(hit);
                self.step = LocalStep::CheckAnonymous;
                return Ok(Next::Call(IoCall::child(check)));
            }
            self.visible.push(hit);
            if self.visible.len() >= self.limit {
                return self.page(None);
            }
        }
        self.after_hits()
    }

    fn after_hits(&mut self) -> LocalTurn {
        match self.next_start_after.take() {
            Some(key) if self.scanned_rows >= self.budget => {
                let scanned_to = seal_position(&key).map_err(|_| SearchBucketsError::Position)?;
                self.page(Some(scanned_to))
            }
            Some(key) => {
                self.start_after = Some(key);
                self.scan()
            }
            None => self.page(None),
        }
    }

    /// Whether a caller without a login may read the bucket, deny policies included.
    fn anonymous_check(&self, hit: &BucketSearchHit) -> AuthorizeOperation {
        let realm_id = self.input.realm_id;
        AuthorizeOperation::new(
            realm_id,
            &AuthContext::anonymous(realm_id),
            &bucket_permission_path(realm_id, hit.group_id, hit.node_id, &hit.bucket),
            &Permission::READ,
            PolicyRequestExtras::operation("s3.ListBuckets"),
        )
    }

    /// Evaluates the loaded policies for one hit on the path the RBAC check used,
    /// under the S3 bucket-read action so one policy covers both read surfaces. A
    /// group with no loaded evaluator fails closed.
    fn policy_allows(&self, hit: &BucketSearchHit) -> bool {
        let input = &self.input;
        let path = candidate_path(input, hit.group_id, hit.node_id, &hit.bucket);
        let mandatory = PolicyRequestExtras::operation("s3.ListBuckets");
        let checks = std::iter::once(mandatory).chain(input.extras.clone());
        let evaluator = self.evaluators.get(&(input.realm_id, hit.group_id));
        checks.into_iter().all(|extras| {
            let request = policy_request_with(&path, &Permission::READ, Some(&input.auth), extras);
            evaluator.is_some_and(|evaluator| evaluator.evaluate(&request).is_ok())
        })
    }
}

/// Whether a caller's policy context may join a bucket search: an MCP tool's operation and
/// parameters only, so it adds a check and never stands in for the mandatory bucket read.
pub(super) fn extras_valid(extras: &PolicyRequestExtras) -> bool {
    let tool = extras.operation.strip_prefix("mcp:");
    tool.is_some_and(|tool| !tool.is_empty()) && extras.headers.is_empty() && extras.body.is_none()
}

impl Operation for LocalBucketSearch {
    type Output = BucketPage;
    type Error = SearchBucketsError;

    fn first(&mut self) -> LocalTurn {
        if (self.input.extras.as_ref()).is_some_and(|extras| !extras_valid(extras)) {
            return Err(SearchBucketsError::Extras);
        }
        // A cut scan's position comes back sealed, as it may name a bucket the caller cannot read.
        let opened = (self.input.start_after.as_ref()).map(|after| open_position(after));
        let Ok(opened) = opened.transpose() else {
            return Ok(Next::Done(BucketPage {
                retired: true,
                ..BucketPage::default()
            }));
        };
        self.start_after = opened.map(Key::from);
        self.scan()
    }

    fn next(&mut self, result: IoResult) -> LocalTurn {
        match self.step {
            LocalStep::Scan => {
                let output = result.child::<SearchBucketsOutput, SearchBucketsError>()??;
                self.scanned(output)
            }
            LocalStep::LoadPolicies => {
                let loaded = result.child::<PolicyEvaluators, PolicyEnforcementError>()??;
                self.evaluators.extend(loaded);
                self.decide_hits()
            }
            LocalStep::CheckAnonymous => {
                let public = result.child::<(), AuthorizeError>()?.is_ok();
                let mut hit = self.checking.take().ok_or(IoError::Unfinished)?;
                hit.public = public;
                self.visible.push(hit);
                if self.visible.len() >= self.limit {
                    return self.page(None);
                }
                self.decide_hits()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::NodeId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::realm::RealmId;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn realm() -> RealmId {
        RealmId::from_bytes([9; 32])
    }

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[3; 32]).public()
    }

    fn input() -> SearchBucketsInput {
        SearchBucketsInput {
            auth: AuthContext::anonymous(realm()),
            realm_id: realm(),
            node_id: node(),
            query: String::new(),
            key: None,
            extras: None,
            limit: 1,
            group_id: None,
            start_after: None,
            database: false,
        }
    }

    fn hit(bucket: &str) -> BucketSearchHit {
        BucketSearchHit {
            arn: format!("arn:{bucket}"),
            bucket: bucket.to_string(),
            node_id: node(),
            group_id: Ulid::from_parts(1, 1),
            group_name: None,
            created_at: SystemTime::UNIX_EPOCH,
            set_id: None,
            metadata: None,
            public: false,
        }
    }

    fn scanned(hits: Vec<BucketSearchHit>, next: Option<Key>) -> IoResult {
        let output = SearchBucketsOutput {
            hits,
            next_start_after: next,
            scanned_rows: 1,
        };
        IoResult::Child(ChildOutcome::new(Ok::<_, SearchBucketsError>(output)))
    }

    #[test]
    fn refuses_foreign_extras() {
        let mut search = LocalBucketSearch::new(SearchBucketsInput {
            extras: Some(PolicyRequestExtras::operation("s3.ListBuckets")),
            ..input()
        });
        assert_eq!(search.first(), Err(SearchBucketsError::Extras));
    }

    #[test]
    fn foreign_position_retires() {
        let mut search = LocalBucketSearch::new(SearchBucketsInput {
            start_after: Some(Key::from(vec![1, 2, 3])),
            ..input()
        });
        let Ok(Next::Done(page)) = search.first() else {
            panic!("a position this node cannot open ends its part");
        };
        assert!(page.retired && page.hits.is_empty());
    }

    #[test]
    fn empty_scan_ends_page() {
        let mut search = LocalBucketSearch::new(input());
        assert!(matches!(search.first(), Ok(Next::Call(IoCall::Child(_)))));
        let Ok(Next::Done(page)) = search.next(scanned(Vec::new(), None)) else {
            panic!("nothing is left to scan");
        };
        assert!(page.hits.is_empty() && page.scanned_to.is_none());
    }

    #[test]
    fn hits_load_policies() {
        // A group without a loaded evaluator is read first, and an empty set hides its hit.
        let mut search = LocalBucketSearch::new(input());
        search.first().unwrap();
        let turn = search.next(scanned(vec![hit("a")], None));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let none = ChildOutcome::new(Ok::<_, PolicyEnforcementError>(PolicyEvaluators::new()));
        let Ok(Next::Done(page)) = search.next(IoResult::Child(none)) else {
            panic!("the only hit was decided");
        };
        assert!(page.hits.is_empty());
    }

    #[test]
    fn used_budget_seals_position() {
        let mut search = LocalBucketSearch::with_budget(input(), 1);
        search.first().unwrap();
        let next = Some(Key::from(b"bucket".to_vec()));
        let Ok(Next::Done(page)) = search.next(scanned(Vec::new(), next)) else {
            panic!("a used-up budget ends the page");
        };
        assert!(page.scanned_to.is_some());
    }
}
