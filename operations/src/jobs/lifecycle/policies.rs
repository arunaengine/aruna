//! Resolves the placement policies a request names, each through the verifying resolver.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::{PlacementPolicyRef, PolicyResolution};
use tracing::debug;
use ulid::Ulid;

use crate::placement::policy::{
    ReadPolicyError, ResolvePolicyConfig, ResolvePolicyOperation, ResolvedPolicy,
};

/// Each policy and whether it resolved, by policy id.
pub type Resolutions = BTreeMap<Ulid, PolicyResolution>;

/// Resolves every distinct ref, one child resolve after another. A ref that stays unresolved is
/// recorded as such, so it blocks a target instead of allowing it.
#[derive(Debug, PartialEq)]
pub(crate) struct ResolvePoliciesOperation {
    realm_id: RealmId,
    local: NodeId,
    now_ms: u64,
    /// Reversed, so the next ref pops off the end.
    remaining: Vec<PlacementPolicyRef>,
    current: Option<Ulid>,
    resolved: Resolutions,
}

impl ResolvePoliciesOperation {
    pub(crate) fn new(
        realm_id: RealmId,
        local: NodeId,
        now_ms: u64,
        mut refs: Vec<PlacementPolicyRef>,
    ) -> Self {
        refs.sort_unstable();
        refs.dedup();
        refs.reverse();
        Self {
            realm_id,
            local,
            now_ms,
            remaining: refs,
            current: None,
            resolved: BTreeMap::new(),
        }
    }

    fn ask(&mut self) -> Result<Next<Resolutions>, IoError> {
        let Some(policy_ref) = self.remaining.pop() else {
            return Ok(Next::Done(std::mem::take(&mut self.resolved)));
        };
        self.current = Some(policy_ref.policy_id);
        Ok(Next::Call(IoCall::child(ResolvePolicyOperation::new(
            ResolvePolicyConfig {
                realm_id: self.realm_id,
                policy_ref,
                local_node_id: self.local,
                now_ms: self.now_ms,
            },
        ))))
    }
}

impl Operation for ResolvePoliciesOperation {
    type Output = Resolutions;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Resolutions>, IoError> {
        self.ask()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Resolutions>, IoError> {
        let resolution = match result.child::<ResolvedPolicy, ReadPolicyError>()? {
            Ok(resolved) => PolicyResolution::Known(resolved.policy),
            Err(error) => {
                debug!(error = %error, "Placement policy stayed unresolved");
                PolicyResolution::Unresolved
            }
        };
        let policy_id = self.current.take().ok_or(IoError::Unfinished)?;
        self.resolved.insert(policy_id, resolution);
        self.ask()
    }
}

