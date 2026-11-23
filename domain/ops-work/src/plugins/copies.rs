//! The memory whole artifact copies take on this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::plugins::MemoryPermit;
use aruna_core::plugins::{PluginEffect, PluginEvent};

/// The call that reserves memory for `bytes` from the node's budget.
pub(crate) fn reserve_call(bytes: u64) -> IoCall {
    let effect = Effect::Plugin(PluginEffect::Reserve { bytes });
    IoCall::Effect(Box::new(effect))
}

/// The reservation a [`reserve_call`] answered, or `None` while the node spends its budget
/// elsewhere.
pub(crate) fn reserved(result: IoResult) -> Result<Option<MemoryPermit>, IoError> {
    match result {
        IoResult::Event(event) => match *event {
            Event::Plugin(PluginEvent::Reserved(permit)) => Ok(permit),
            other => Err(IoError::Unexpected {
                expected: "Event::Plugin(PluginEvent::Reserved)",
                got: other.label(),
            }),
        },
        other => Err(IoError::Unexpected {
            expected: "Event::Plugin(PluginEvent::Reserved)",
            got: other.label(),
        }),
    }
}

/// Reserves memory for an artifact upload of `bytes` before its body is read.
#[derive(Debug, PartialEq)]
pub struct ReserveMemory {
    bytes: u64,
}

impl ReserveMemory {
    pub fn new(bytes: u64) -> Self {
        Self { bytes }
    }
}

impl Operation for ReserveMemory {
    type Output = Option<MemoryPermit>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Option<MemoryPermit>>, IoError> {
        Ok(Next::Call(reserve_call(self.bytes)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<MemoryPermit>>, IoError> {
        Ok(Next::Done(reserved(result)?))
    }
}

/// A whole artifact in memory together with the memory reserved for it; dropping it releases
/// the reservation, so whoever consumes the bytes holds it until done.
pub struct Assembled {
    pub bytes: Vec<u8>,
    memory: MemoryPermit,
}

impl Assembled {
    pub fn new(bytes: Vec<u8>, memory: MemoryPermit) -> Self {
        Self { bytes, memory }
    }

    /// MiB this artifact holds of the node's budget.
    pub fn reserved_mib(&self) -> u32 {
        self.memory.mib()
    }
}

impl PartialEq for Assembled {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl std::fmt::Debug for Assembled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Assembled({} bytes)", self.bytes.len())
    }
}

#[cfg(test)]
#[path = "copies_tests.rs"]
mod tests;
