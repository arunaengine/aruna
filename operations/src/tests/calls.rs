//! Builds the results operations get from children and shard effects, and reads their calls.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt::Debug;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect, ShardNetEffect};
use aruna_core::events::{Event, NetEvent, ShardNetEvent};

pub(crate) fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

/// A child that ended with `result`.
pub(crate) fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + Debug + PartialEq + 'static,
    E: Send + Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

/// The call a turn asks for.
pub(crate) fn called<T: Debug, E: Debug>(turn: Result<Next<T>, E>) -> IoCall {
    match turn {
        Ok(Next::Call(call)) => call,
        other => panic!("expected a call, got {other:?}"),
    }
}

/// Whether a turn asks to run a child.
pub(crate) fn runs_child<T: Debug, E: Debug>(turn: Result<Next<T>, E>) -> bool {
    matches!(called(turn), IoCall::Child(_))
}

pub(crate) fn shard_event(event: ShardNetEvent) -> IoResult {
    let event = Event::Net(NetEvent::Shard(Box::new(event)));
    IoResult::Event(Box::new(event))
}

/// The shard effect a turn asks for.
pub(crate) fn shard_effect<T: Debug, E: Debug>(turn: Result<Next<T>, E>) -> ShardNetEffect {
    match called(turn) {
        IoCall::Effect(effect) => match *effect {
            Effect::Net(NetEffect::Shard(effect)) => *effect,
            other => panic!("expected a shard effect, got {other:?}"),
        },
        other => panic!("expected a shard effect, got {other:?}"),
    }
}
