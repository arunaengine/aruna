//! Binds the plugin WIT and holds the state of one run: grants, broker and budgets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use tokio_util::task::TaskTracker;
use wasmtime::ResourceLimiter;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::broker::{Broker, CallContext, ErrorKind, HostError};
use crate::limits::Limits;
use crate::permission::Permission;

pub mod wit {
    wasmtime::component::bindgen!({
        world: "plugin",
        path: "wit",
        imports: { default: async | trappable },
        additional_derives: [PartialEq, Eq],
    });
}

const MAX_TABLE: usize = 100_000;
const MAX_INSTANCES: usize = 64;
const MAX_MEMORIES: usize = 8;

/// A resource whose budget a run exhausted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Limit {
    Fuel,
    Memory,
    Calls,
    Bytes,
    Pending,
    Input,
    Output,
}

impl fmt::Display for Limit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Limit::Fuel => "fuel",
            Limit::Memory => "memory",
            Limit::Calls => "host call",
            Limit::Bytes => "host byte",
            Limit::Pending => "pending host call",
            Limit::Input => "input",
            Limit::Output => "output",
        })
    }
}

/// Trap payload raised by a host import when a budget is exhausted.
#[derive(Debug, thiserror::Error)]
#[error("plugin exceeded its {0} limit")]
pub(crate) struct Exceeded(pub Limit);

/// What a host call needs once its checks passed.
pub(crate) struct Started {
    pub broker: Arc<dyn Broker>,
    pub cx: CallContext,
    /// Runs writes to completion even if the guest is dropped.
    pub tracker: TaskTracker,
    /// This run's writes, which settle before the run reports an outcome.
    pub writes: TaskTracker,
    /// How long a write may take before its outcome counts as unknown.
    pub deadline: std::time::Duration,
}

/// What one host call must be granted before it reaches the broker.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Needs<'a> {
    Free,
    Permission(Permission),
    Connection(&'a str),
}

/// Store data of one run: grants, broker, ordinal counter and consumed budget.
pub(crate) struct Session {
    broker: Arc<dyn Broker>,
    cx: CallContext,
    permissions: BTreeSet<Permission>,
    connections: BTreeMap<String, Vec<String>>,
    tracker: TaskTracker,
    writes: TaskTracker,
    limits: Limits,
    ordinal: u32,
    calls: u32,
    bytes: u64,
    pending: u32,
    memory: usize,
    pub(crate) exceeded: Option<Limit>,
    /// WASI for guests whose toolchain imports it: no files, environment, arguments or network.
    wasi: WasiCtx,
    table: ResourceTable,
}

pub(crate) struct Grants {
    pub permissions: BTreeSet<Permission>,
    /// Granted connections and the response fields the manifest lets the code read.
    pub connections: BTreeMap<String, Vec<String>>,
}

impl Session {
    pub(crate) fn new(
        broker: Arc<dyn Broker>,
        cx: CallContext,
        grants: Grants,
        trackers: (TaskTracker, TaskTracker),
        limits: Limits,
    ) -> Self {
        Self {
            broker,
            cx,
            permissions: grants.permissions,
            connections: grants.connections,
            tracker: trackers.0,
            writes: trackers.1,
            limits,
            ordinal: 0,
            calls: 0,
            bytes: 0,
            pending: 0,
            memory: 0,
            exceeded: None,
            wasi: closed_wasi(),
            table: ResourceTable::new(),
        }
    }

    pub(crate) fn limits(&self) -> Limits {
        self.limits
    }

    fn exceed(&mut self, limit: Limit) -> wasmtime::Error {
        self.exceeded.get_or_insert(limit);
        wasmtime::Error::new(Exceeded(limit))
    }

    /// Counts one call moving `size` bytes into the host.
    pub(crate) fn charge(&mut self, size: usize) -> wasmtime::Result<()> {
        self.calls += 1;
        if self.calls > self.limits.calls {
            return Err(self.exceed(Limit::Calls));
        }
        self.receive(size)
    }

    fn receive(&mut self, size: usize) -> wasmtime::Result<()> {
        self.bytes += size as u64;
        if size > self.limits.call_bytes || self.bytes > self.limits.bytes {
            return Err(self.exceed(Limit::Bytes));
        }
        Ok(())
    }

    /// The next outside effect's ordinal; reruns reach the same ordinals in the same order.
    pub(crate) fn next_ordinal(&mut self) -> u32 {
        self.ordinal += 1;
        self.ordinal
    }

    /// Starts a broker call after the budget and grant checks. A missing grant is
    /// `not-granted`, never a fallback to other rights.
    pub(crate) fn begin(
        &mut self,
        size: usize,
        needs: Needs<'_>,
    ) -> wasmtime::Result<Result<Started, HostError>> {
        self.charge(size)?;
        let granted = match needs {
            Needs::Free => true,
            Needs::Permission(permission) => self.permissions.contains(&permission),
            Needs::Connection(name) => self.connections.contains_key(name),
        };
        if !granted {
            let what = match needs {
                Needs::Permission(permission) => format!("permission `{permission}`"),
                Needs::Connection(name) => format!("connection `{name}`"),
                Needs::Free => String::new(),
            };
            let message = format!("{what} is not granted to this install");
            self.receive(message.len())?;
            return Ok(Err(HostError::new(ErrorKind::NotGranted, message)));
        }
        self.pending += 1;
        if self.pending > self.limits.pending {
            return Err(self.exceed(Limit::Pending));
        }
        Ok(Ok(Started {
            broker: self.broker.clone(),
            cx: self.cx.clone(),
            tracker: self.tracker.clone(),
            writes: self.writes.clone(),
            deadline: self.limits.write,
        }))
    }

    /// The response fields a granted connection lets the code read; empty means status only.
    pub(crate) fn response_fields(&self, connection: &str) -> Vec<String> {
        self.connections
            .get(connection)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn finish(&mut self, size: usize) -> wasmtime::Result<()> {
        self.pending -= 1;
        self.receive(size)
    }
}

/// A WASI context that grants nothing: no preopened directories, no environment or
/// arguments, no network, and stdio that reads nothing and discards output.
fn closed_wasi() -> WasiCtx {
    let mut builder = WasiCtxBuilder::new();
    builder
        .allow_tcp(false)
        .allow_udp(false)
        .allow_ip_name_lookup(false);
    builder.build()
}

impl WasiView for Session {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl ResourceLimiter for Session {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let total = self.memory.saturating_sub(current).saturating_add(desired);
        if total > self.limits.memory {
            self.exceeded.get_or_insert(Limit::Memory);
            return Ok(false);
        }
        self.memory = total;
        Ok(true)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= MAX_TABLE)
    }

    fn instances(&self) -> usize {
        MAX_INSTANCES
    }

    fn tables(&self) -> usize {
        MAX_INSTANCES
    }

    fn memories(&self) -> usize {
        MAX_MEMORIES
    }
}
