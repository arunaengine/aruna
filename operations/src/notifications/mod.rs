//! Groups notification delivery, inbox storage, watches, pruning and routing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod client;
pub mod dispatch;
pub mod emit;
pub mod inbox;
pub mod incoming;
pub mod list;
pub mod mark_read;
pub mod outbox;
pub mod placement;
pub mod protocol;
pub mod prune;
pub mod routing;
#[cfg(test)]
mod tests;
pub mod unread;
pub mod watch;
