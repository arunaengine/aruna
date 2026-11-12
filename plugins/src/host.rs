//! Binds the plugin WIT and holds the state of one run: grants, broker and budgets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use tokio_util::task::TaskTracker;
use wasmtime::component::{Linker, Resource, ResourceTable};
use wasmtime::{ResourceLimiter, StoreContextMut};
use wasmtime_wasi::cli::{IsTerminal, StdoutStream};
use wasmtime_wasi::p2::{DynPollable, OutputStream, Pollable, StreamError, StreamResult};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_io::bindings::wasi::io::poll::Host as _;

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
    /// Host calls and bytes so far, shared with the WASI output streams.
    meter: Arc<Meter>,
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
        let meter = Arc::new(Meter::new(&limits));
        Self {
            broker,
            cx,
            permissions: grants.permissions,
            connections: grants.connections,
            tracker: trackers.0,
            writes: trackers.1,
            limits,
            ordinal: 0,
            meter: meter.clone(),
            pending: 0,
            memory: 0,
            exceeded: None,
            wasi: closed_wasi(meter),
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

    /// Counts one host call we implement or wrap and refuses it once the budget is spent.
    pub(crate) fn count_call(&mut self) -> wasmtime::Result<()> {
        match self.meter.call() {
            true => Ok(()),
            false => Err(self.exceed(Limit::Calls)),
        }
    }

    /// Counts `size` bytes moved through a host call against the per-call and total budgets.
    pub(crate) fn charge(&mut self, size: usize) -> wasmtime::Result<()> {
        match self.meter.bytes(size) {
            true => Ok(()),
            false => Err(self.exceed(Limit::Bytes)),
        }
    }

    /// The budget a WASI output stream exhausted, if any.
    pub(crate) fn stream_limit(&self) -> Option<Limit> {
        self.meter.exhausted()
    }

    /// The next outside effect's ordinal; reruns reach the same ordinals in the same order.
    pub(crate) fn next_ordinal(&mut self) -> u32 {
        self.ordinal += 1;
        self.ordinal
    }

    /// The broker and call context a guest log line goes to.
    pub(crate) fn log_target(&self) -> (Arc<dyn Broker>, CallContext) {
        (self.broker.clone(), self.cx.clone())
    }

    /// Starts a broker call after the budget and grant checks. A missing grant is
    /// `not-granted`, never a fallback to other rights.
    pub(crate) fn begin(
        &mut self,
        size: usize,
        needs: Needs<'_>,
    ) -> wasmtime::Result<Result<Started, HostError>> {
        self.count_call()?;
        self.charge(size)?;
        let granted = match needs {
            Needs::Free => true,
            Needs::Permission(permission) => self.permissions.contains(&permission),
            Needs::Connection(name) => self.connections.contains_key(name),
        };
        // The reply's charge is held back first, so a call never ends the run on its way back.
        if granted && !self.meter.reserve() {
            let message = "the run has too few bytes left for a reply";
            return Ok(Err(HostError::new(ErrorKind::Limit, message)));
        }
        if !granted {
            let what = match needs {
                Needs::Permission(permission) => format!("permission `{permission}`"),
                Needs::Connection(name) => format!("connection `{name}`"),
                Needs::Free => String::new(),
            };
            let message = format!("{what} is not granted to this install");
            self.charge(message.len())?;
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

    /// Ends a started call: releases its reservation, refuses a reply over the per-call bound
    /// with an error the guest can catch, and charges what the guest gets.
    pub(crate) fn finish<R>(
        &mut self,
        reply: Result<R, HostError>,
        size: usize,
    ) -> wasmtime::Result<Result<R, HostError>> {
        self.pending -= 1;
        self.meter.release();
        if size as u64 <= self.meter.per_call {
            self.charge(size)?;
            return Ok(reply);
        }
        let message = format!("the reply is larger than {} bytes", self.meter.per_call);
        self.charge(message.len())?;
        Ok(Err(HostError::new(ErrorKind::Limit, message)))
    }
}

/// WASI random interfaces whose byte calls replace the default ones, so a guest cannot ask the
/// host for more random bytes than its per-call and total byte budgets allow. `poll` is charged too.
const RANDOM_BYTES: [(&str, &str); 2] = [
    ("wasi:random/random@0.2.12", "get-random-bytes"),
    ("wasi:random/insecure@0.2.12", "get-insecure-random-bytes"),
];

/// Shadows the WASI random byte calls and `poll` with versions that charge the run first.
pub(crate) fn bounded_wasi(linker: &mut Linker<Session>) -> wasmtime::Result<()> {
    linker.allow_shadowing(true);
    for (interface, function) in RANDOM_BYTES {
        linker.instance(interface)?.func_wrap(
            function,
            |mut store: StoreContextMut<'_, Session>, (len,): (u64,)| {
                let len = usize::try_from(len).unwrap_or(usize::MAX);
                store.data_mut().count_call()?;
                store.data_mut().charge(len)?;
                let mut bytes = vec![0u8; len];
                getrandom::fill(&mut bytes).map_err(|err| wasmtime::Error::msg(err.to_string()))?;
                Ok((bytes,))
            },
        )?;
    }
    // Poll lists and replies are charged like any other host call bytes.
    linker.instance("wasi:io/poll@0.2.12")?.func_wrap_async(
        "poll",
        |mut store: StoreContextMut<'_, Session>, (list,): (Vec<Resource<DynPollable>>,)| {
            Box::new(async move {
                let session = store.data_mut();
                session.count_call()?;
                session.charge(list.len() * 4)?;
                let reply = session.table.poll(list).await?;
                session.charge(reply.len() * 4)?;
                Ok((reply,))
            })
        },
    )?;
    linker.allow_shadowing(false);
    Ok(())
}

/// A WASI context that grants nothing: no preopened directories, no environment or
/// arguments, no network; stdin reads nothing and output is charged, then discarded.
fn closed_wasi(meter: Arc<Meter>) -> WasiCtx {
    let mut builder = WasiCtxBuilder::new();
    builder
        .stdout(Charged(meter.clone()))
        .stderr(Charged(meter))
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

/// Host calls and bytes of a run, shared with its WASI output streams.
#[derive(Debug)]
pub(crate) struct Meter {
    calls: AtomicU64,
    bytes: AtomicU64,
    /// Bytes held back for the replies of calls in flight, one per-call bound each.
    reserved: AtomicU64,
    /// 0 while within budget, else 1 for bytes or 2 for calls.
    exhausted: AtomicU8,
    max_calls: u64,
    per_call: u64,
    total: u64,
}

impl Meter {
    fn new(limits: &Limits) -> Self {
        Self {
            calls: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            reserved: AtomicU64::new(0),
            exhausted: AtomicU8::new(0),
            max_calls: u64::from(limits.calls),
            per_call: limits.call_bytes as u64,
            total: limits.bytes,
        }
    }

    /// Counts one call; false, and remembered, once the call budget is spent.
    fn call(&self) -> bool {
        let calls = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        self.within(calls <= self.max_calls, 2)
    }

    /// Charges `size` bytes; false, and remembered, once a byte budget is exceeded.
    fn bytes(&self, size: usize) -> bool {
        let size = size as u64;
        let used = self
            .bytes
            .fetch_add(size, Ordering::Relaxed)
            .saturating_add(size);
        self.within(size <= self.per_call && used <= self.total, 1)
    }

    /// Holds back one per-call bound for a reply; false, without exhausting the run, when the
    /// rest of the run's bytes cannot take it.
    fn reserve(&self) -> bool {
        let mut reserved = self.reserved.load(Ordering::Relaxed);
        loop {
            let used = self.bytes.load(Ordering::Relaxed);
            let needed = used.saturating_add(reserved).saturating_add(self.per_call);
            if needed > self.total {
                return false;
            }
            let next = reserved + self.per_call;
            let swapped = (self.reserved).compare_exchange(
                reserved,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            match swapped {
                Ok(_) => return true,
                Err(current) => reserved = current,
            }
        }
    }

    fn release(&self) {
        self.reserved.fetch_sub(self.per_call, Ordering::Relaxed);
    }

    fn within(&self, within: bool, code: u8) -> bool {
        if !within {
            let _ = self
                .exhausted
                .compare_exchange(0, code, Ordering::Relaxed, Ordering::Relaxed);
        }
        within
    }

    fn exhausted(&self) -> Option<Limit> {
        match self.exhausted.load(Ordering::Relaxed) {
            1 => Some(Limit::Bytes),
            2 => Some(Limit::Calls),
            _ => None,
        }
    }
}

/// Guest stdout and stderr: discards what is written, after charging it to the run's budget.
struct Charged(Arc<Meter>);

impl IsTerminal for Charged {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdoutStream for Charged {
    fn async_stream(&self) -> Box<dyn tokio::io::AsyncWrite + Send + Sync> {
        Box::new(tokio::io::sink())
    }

    fn p2_stream(&self) -> Box<dyn OutputStream> {
        Box::new(Discard(self.0.clone()))
    }
}

struct Discard(Arc<Meter>);

impl Discard {
    /// Counts the write as a host call and charges its bytes before anything is allocated.
    fn take(&self, size: usize) -> StreamResult<()> {
        let within = self.0.call() && self.0.bytes(size);
        match self.0.exhausted() {
            Some(limit) if !within => Err(StreamError::Trap(wasmtime::Error::new(Exceeded(limit)))),
            _ => Ok(()),
        }
    }
}

#[async_trait::async_trait]
impl Pollable for Discard {
    async fn ready(&mut self) {}
}

#[async_trait::async_trait]
impl OutputStream for Discard {
    fn write(&mut self, bytes: bytes::Bytes) -> StreamResult<()> {
        self.take(bytes.len())
    }

    fn flush(&mut self) -> StreamResult<()> {
        Ok(())
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        Ok(4096)
    }

    /// Charged before anything is allocated; the zeroes are never made.
    fn write_zeroes(&mut self, nelem: usize) -> StreamResult<()> {
        self.take(nelem)
    }
}
