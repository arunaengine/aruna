//! Declares per-run resource limits and the node ceilings of the runtime.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

/// Resource budget of one run. Every field is a hard upper bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Bytes of guest linear memory.
    pub memory: usize,
    /// Fuel units of guest computation.
    pub fuel: u64,
    /// Wall-clock time, including awaited host calls.
    pub wall: Duration,
    /// Longest a started write may take, even after the run ended; then its outcome is unknown.
    pub write: Duration,
    /// Host calls, including log lines.
    pub calls: u32,
    /// Bytes moved through host calls in both directions, all calls together.
    pub bytes: u64,
    /// Bytes of one host request or reply.
    pub call_bytes: usize,
    /// Host calls in flight at the same time.
    pub pending: u32,
    /// Bytes of run input: settings, manual input and webhook body.
    pub input: usize,
    /// Bytes of the handler result.
    pub output: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            memory: 256 << 20,
            fuel: 2_000_000_000,
            wall: Duration::from_secs(60),
            write: Duration::from_secs(30),
            calls: 1_000,
            bytes: 16 << 20,
            call_bytes: 1 << 20,
            pending: 8,
            input: 1 << 20,
            output: 64 << 10,
        }
    }
}

impl Limits {
    /// Lowers every field to `ceiling`; a request never raises a limit.
    pub fn within(self, ceiling: &Limits) -> Self {
        Self {
            memory: self.memory.min(ceiling.memory),
            fuel: self.fuel.min(ceiling.fuel),
            wall: self.wall.min(ceiling.wall),
            write: self.write.min(ceiling.write),
            calls: self.calls.min(ceiling.calls),
            bytes: self.bytes.min(ceiling.bytes),
            call_bytes: self.call_bytes.min(ceiling.call_bytes),
            pending: self.pending.min(ceiling.pending),
            input: self.input.min(ceiling.input),
            output: self.output.min(ceiling.output),
        }
    }
}

/// Node configuration of the runtime.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// Largest accepted artifact in bytes.
    pub max_artifact: usize,
    /// Compiled artifacts kept in memory.
    pub cache: usize,
    /// Runs on this node at the same time.
    pub invocations: usize,
    /// Artifact compilations on this node at the same time.
    pub compiles: usize,
    /// Hard node ceiling for every run budget.
    pub ceiling: Limits,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_artifact: 64 << 20,
            cache: 32,
            invocations: 16,
            compiles: 1,
            ceiling: Limits::default(),
        }
    }
}
