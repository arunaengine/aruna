//! The operator's workflow settings: the coordinator image or a local binary, the preflight
//! workers and the limits of every coordinator container.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::PathBuf;
use std::time::Duration;

/// How a node runs its coordinators. The node parses it once from its operator input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowSettings {
    /// The coordinator image, run on the node's executor backend.
    pub image: Option<String>,
    /// A coordinator binary run as a plain child process, for local development and tests only;
    /// it has no container sandbox.
    pub binary: Option<PathBuf>,
    /// How many preflight requests run at once, each in its own coordinator.
    pub preflight_workers: usize,
    /// How long one preflight request or a greeting may take.
    pub request_timeout: Duration,
    /// A warm preflight coordinator ends after this many requests, so no state builds up.
    pub recycle_after: u64,
    pub limits: ContainerLimits,
}

/// The bounds of one coordinator container.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContainerLimits {
    pub cpus: u32,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub pids: u64,
    pub walltime: Duration,
}

impl Default for ContainerLimits {
    fn default() -> Self {
        Self {
            cpus: 2,
            memory_bytes: 2 << 30,
            disk_bytes: 4 << 30,
            pids: 512,
            walltime: Duration::from_secs(24 * 60 * 60),
        }
    }
}

impl Default for WorkflowSettings {
    fn default() -> Self {
        Self {
            image: None,
            binary: None,
            preflight_workers: 2,
            request_timeout: Duration::from_secs(120),
            recycle_after: 64,
            limits: ContainerLimits::default(),
        }
    }
}

impl WorkflowSettings {
    /// Whether the node runs coordinators at all: an image or a binary is named.
    pub fn enabled(&self) -> bool {
        self.image.is_some() || self.binary.is_some()
    }
}
