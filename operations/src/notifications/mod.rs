//! Groups notification delivery, inbox storage, watches, pruning and routing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod checks;
pub mod client;
pub mod deliver;
pub mod dispatch;
pub mod email;
pub mod email_job;
pub mod emit;
pub mod enqueue;
pub mod holder;
pub mod inbox;
#[cfg(test)]
mod incoming_tests;
pub mod list;
pub mod mark_read;
pub mod opt_in;
pub mod outbox;
pub mod outbox_record;
pub mod placement;
pub mod protocol;
pub mod prune;
pub mod routing;
pub mod send;
pub mod serve;
pub mod setting_rows;
pub mod settings;
#[cfg(test)]
mod tests;
pub mod user_call;
pub mod watch;
pub mod webhook;
pub mod webhook_job;

use aruna_core::calls::{IoError, IoResult};
use aruna_core::events::Event;

/// The error for a result that does not answer the call it came for.
pub(crate) fn unexpected(expected: &'static str, result: &IoResult) -> IoError {
    IoError::Unexpected {
        expected,
        got: result.label(),
    }
}

/// The error for an event that does not answer the effect it came for.
pub(crate) fn unexpected_event(expected: &'static str, event: &Event) -> IoError {
    IoError::Unexpected {
        expected,
        got: event.label(),
    }
}
