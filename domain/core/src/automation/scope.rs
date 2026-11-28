//! Approved scope, concurrency limits and widening checks for automations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::record::Spec;
use super::rules::conditions_widen;
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
/// `None` uses the node's default bound, so explicitly chosen limits remain distinct from defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub max_active_firings: Option<u32>,
    pub max_concurrent_workflows: Option<u32>,
    pub per_hour: Option<u32>,
    pub per_day: Option<u32>,
    pub bytes_per_day: Option<u64>,
    pub cpu_cores: Option<u32>,
    pub ram_bytes: Option<u64>,
    pub walltime_ms: Option<u64>,
    pub max_chain_depth: Option<u8>,
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
    pub allow_outside_input: bool,
    pub limits: Limits,
}

impl Limits {
    pub fn active_firings(&self) -> u32 {
        self.max_active_firings.unwrap_or(DEFAULT_ACTIVE_FIRINGS)
    }

    pub fn concurrent_workflows(&self) -> u32 {
        self.max_concurrent_workflows
            .unwrap_or(DEFAULT_CONCURRENT_WORKFLOWS)
    }

    pub fn hour_limit(&self) -> u32 {
        self.per_hour.unwrap_or(DEFAULT_PER_HOUR)
    }

    pub fn day_limit(&self) -> u32 {
        self.per_day.unwrap_or(DEFAULT_PER_DAY)
    }

    pub fn copy_bytes_day(&self) -> u64 {
        self.bytes_per_day.unwrap_or(DEFAULT_BYTES_DAY)
    }

    pub fn chain_depth(&self) -> u8 {
        self.max_chain_depth.unwrap_or(DEFAULT_CHAIN_DEPTH)
    }

    /// Compute and active-run caps forwarded to each workflow run started by this automation.
    pub fn run_limits(&self) -> RunLimits {
        RunLimits {
            cpu_cores: self.cpu_cores,
            ram_bytes: self.ram_bytes,
            walltime_ms: self.walltime_ms,
            active_cap: Some(self.concurrent_workflows()),
        }
    }

    /// Whether every effective bound in `self` is at least as strict as `other`.
    pub fn is_tighter_than(&self, other: &Self) -> bool {
        let opt = |a: Option<u64>, b: Option<u64>| match (a, b) {
            (_, None) => true,
            (Some(a), Some(b)) => a <= b,
            (None, Some(_)) => false,
        };
        self.active_firings() <= other.active_firings()
            && self.concurrent_workflows() <= other.concurrent_workflows()
            && self.hour_limit() <= other.hour_limit()
            && self.day_limit() <= other.day_limit()
            && self.copy_bytes_day() <= other.copy_bytes_day()
            && self.chain_depth() <= other.chain_depth()
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
    if conditions_widen(&before.conditions, &after.conditions) {
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
    if as_.allow_outside_input && !bs.allow_outside_input {
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
        base.scope.limits.per_hour = Some(20);

        let mut narrow = base.clone();
        narrow.scope.read = vec!["/r/g/a/**".into()];
        narrow.scope.limits.per_hour = Some(10);
        narrow.scope.limits.cpu_cores = Some(2);
        assert!(!is_widening(&base, &narrow));

        let mut wide_limit = base.clone();
        wide_limit.scope.limits.per_hour = Some(25);
        assert!(is_widening(&base, &wide_limit));

        let mut reset_default = base.clone();
        reset_default.scope.limits.per_hour = None;
        assert!(is_widening(&base, &reset_default));

        let mut wide_path = base.clone();
        wide_path.scope.read.push("/r/g/c/**".into());
        assert!(is_widening(&base, &wide_path));

        let mut downgrade = base.clone();
        downgrade.scope.allow_downgrade = true;
        assert!(is_widening(&base, &downgrade));

        let mut outside = base.clone();
        outside.scope.allow_outside_input = true;
        assert!(is_widening(&base, &outside));
        assert!(!is_widening(&outside, &base));
    }
}
