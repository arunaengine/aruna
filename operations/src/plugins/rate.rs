//! Host calls and webhook requests per install on its receiver, counted by the plugin host's
//! token buckets, so a busy install waits without slowing the others.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::plugins::{PluginEffect, PluginEvent};
use ulid::Ulid;

/// The call that takes one host call of `install_id` at `now_ms` from its rate.
pub(crate) fn take_call(install_id: Ulid, now_ms: u64) -> IoCall {
    let effect = Effect::Plugin(PluginEffect::TakeCall { install_id, now_ms });
    IoCall::Effect(Box::new(effect))
}

/// The call that takes one webhook request of `install_id` at `now_ms` from its rate.
pub(crate) fn take_hook(install_id: Ulid, now_ms: u64) -> IoCall {
    let effect = Effect::Plugin(PluginEffect::TakeHook { install_id, now_ms });
    IoCall::Effect(Box::new(effect))
}

/// Whether a [`take_call`] or [`take_hook`] was within the install's rate.
pub(crate) fn taken(result: IoResult) -> Result<bool, IoError> {
    let expected = "Event::Plugin(PluginEvent::Taken)";
    match result {
        IoResult::Event(event) => match *event {
            Event::Plugin(PluginEvent::Taken(taken)) => Ok(taken),
            other => Err(IoError::Unexpected {
                expected,
                got: other.label(),
            }),
        },
        other => Err(IoError::Unexpected {
            expected,
            got: other.label(),
        }),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The answer of a rate call.
    pub(crate) fn rate_answer(taken: bool) -> IoResult {
        IoResult::Event(Box::new(Event::Plugin(PluginEvent::Taken(taken))))
    }

    #[test]
    fn reads_taken() {
        assert_eq!(taken(rate_answer(true)), Ok(true));
        assert_eq!(taken(rate_answer(false)), Ok(false));
        assert!(taken(IoResult::Done).is_err());
    }
}
