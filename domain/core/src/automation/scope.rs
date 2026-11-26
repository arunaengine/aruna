//! Approved scope, concurrency limits and widening checks for automations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

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
