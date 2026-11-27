//! Approved scope, concurrency limits and widening checks for automations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::record::Spec;
use crate::structs::execution::workflow_run::RunLimits;

/// Default hard limits applied when an automation does not specify tighter ones.
pub const DEFAULT_ACTIVE_FIRINGS: u32 = 4;
pub const DEFAULT_CONCURRENT_WORKFLOWS: u32 = 4;
pub const DEFAULT_PER_HOUR: u32 = 30;
pub const DEFAULT_PER_DAY: u32 = 200;
pub const DEFAULT_BYTES_DAY: u64 = 10 * 1024 * 1024 * 1024;
pub const DEFAULT_CHAIN_DEPTH: u8 = 3;
pub const MAX_CHAIN_DEPTH: u8 = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Audience {
    GroupAdmins,
    GroupMembers,
}

impl Audience {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GroupAdmins => "group_admins",
            Self::GroupMembers => "group_members",
        }
    }
}

/// Hard caps on firing rate, concurrency, copy volume, chain depth and task compute.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub max_active_firings: u32,
    pub max_concurrent_workflows: u32,
    pub per_hour: u32,
    pub per_day: u32,
    pub bytes_per_day: u64,
    pub cpu_cores: Option<u32>,
    pub ram_bytes: Option<u64>,
    pub walltime_ms: Option<u64>,
    pub max_chain_depth: u8,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_active_firings: DEFAULT_ACTIVE_FIRINGS,
            max_concurrent_workflows: DEFAULT_CONCURRENT_WORKFLOWS,
            per_hour: DEFAULT_PER_HOUR,
            per_day: DEFAULT_PER_DAY,
            bytes_per_day: DEFAULT_BYTES_DAY,
            cpu_cores: None,
            ram_bytes: None,
            walltime_ms: None,
            max_chain_depth: DEFAULT_CHAIN_DEPTH,
        }
    }
}

/// Approved permissions, targets and limits for an automation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scope {
    pub read: Vec<String>,
    pub write: Vec<String>,
    pub metadata: Vec<String>,
    pub workflows: Vec<(Ulid, u32)>,
    pub audiences: Vec<Audience>,
    pub secrets: Vec<String>,
    pub allow_downgrade: bool,
    pub limits: Limits,
}

impl Limits {
    /// Compute and active-run caps forwarded to each workflow run started by this automation.
    pub fn run_limits(&self) -> RunLimits {
        RunLimits {
            cpu_cores: self.cpu_cores,
            ram_bytes: self.ram_bytes,
            walltime_ms: self.walltime_ms,
            active_cap: Some(self.max_concurrent_workflows),
        }
    }

    /// Whether every bound in `self` is at least as strict as `other`.
    pub fn is_tighter_than(&self, other: &Self) -> bool {
        let opt = |a: Option<u64>, b: Option<u64>| match (a, b) {
            (_, None) => true,
            (Some(a), Some(b)) => a <= b,
            (None, Some(_)) => false,
        };
        self.max_active_firings <= other.max_active_firings
            && self.max_concurrent_workflows <= other.max_concurrent_workflows
            && self.per_hour <= other.per_hour
            && self.per_day <= other.per_day
            && self.bytes_per_day <= other.bytes_per_day
            && self.max_chain_depth <= other.max_chain_depth
            && opt(
                self.cpu_cores.map(u64::from),
                other.cpu_cores.map(u64::from),
            )
            && opt(self.ram_bytes, other.ram_bytes)
            && opt(self.walltime_ms, other.walltime_ms)
    }
}

fn subset<T: Ord>(after: &[T], before: &[T]) -> bool {
    let allowed: BTreeSet<&T> = before.iter().collect();
    after.iter().all(|item| allowed.contains(item))
}

/// Whether changing `before` to `after` widens authority or alters executable behavior.
pub fn is_widening(before: &Spec, after: &Spec) -> bool {
    if before.stages != after.stages {
        return true;
    }
    if after
        .triggers
        .iter()
        .any(|trigger| !before.triggers.contains(trigger))
    {
        return true;
    }
    let (bs, as_) = (&before.scope, &after.scope);
    if !subset(&as_.read, &bs.read)
        || !subset(&as_.write, &bs.write)
        || !subset(&as_.metadata, &bs.metadata)
        || !subset(&as_.workflows, &bs.workflows)
        || !subset(&as_.audiences, &bs.audiences)
        || !subset(&as_.secrets, &bs.secrets)
    {
        return true;
    }
    if as_.allow_downgrade && !bs.allow_downgrade {
        return true;
    }
    !as_.limits.is_tighter_than(&bs.limits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::automation::record::Trigger;

    #[test]
    fn detects_widening_changes() {
        let mut base = Spec::default();
        base.triggers.push(Trigger::Manual { form_json: None });
        base.scope.read = vec!["/r/g/a/**".into(), "/r/g/b/**".into()];
        base.scope.limits.per_hour = 20;

        let mut narrow = base.clone();
        narrow.scope.read = vec!["/r/g/a/**".into()];
        narrow.scope.limits.per_hour = 10;
        narrow.scope.limits.cpu_cores = Some(2);
        assert!(!is_widening(&base, &narrow));

        let mut wide_limit = base.clone();
        wide_limit.scope.limits.per_hour = 25;
        assert!(is_widening(&base, &wide_limit));

        let mut wide_path = base.clone();
        wide_path.scope.read.push("/r/g/c/**".into());
        assert!(is_widening(&base, &wide_path));

        let mut downgrade = base.clone();
        downgrade.scope.allow_downgrade = true;
        assert!(is_widening(&base, &downgrade));
    }
}
