//! Decides after a start whether this node caught up with the realm config, and records the time
//! of its last contact with the management nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::str::FromStr;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{CatchUpEffect, Effect, NetEffect};
use aruna_core::events::{CatchUpEvent, Event, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::COMPUTE_DEPARTURE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, Value};
use tracing::{debug, warn};

use crate::document_repository::{GetConfigError, GetConfigOperation};

/// The node-local row holding the wall time at which this node last caught up.
pub const LAST_CONTACT_KEY: &[u8] = b"last_contact";
/// Spacing of the passes after the node caught up; each one refreshes the last contact.
pub const CATCH_UP_REFRESH: Duration = Duration::from_secs(300);
/// Spacing of the passes while the node has not caught up yet.
pub const CATCH_UP_RETRY: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatchUpOutcome {
    /// Every counted round target is applied here and the last contact row was refreshed.
    pub caught_up: bool,
    /// Management nodes without a counted round; the next pass asks them again.
    pub unanswered: Vec<NodeId>,
}

#[derive(Debug, PartialEq)]
enum CatchUpStep {
    Config,
    Round(usize),
    Applied,
    /// The realm config is read again for management nodes the sync revealed.
    Reload,
    Contact,
}

type CatchUpNext = Result<Next<CatchUpOutcome>, IoError>;

/// Counts a sync round without `WouldBlock` with every management node that answers, at least one
/// if any exists, then waits until the applied cursor covers each round's target.
#[derive(Debug, PartialEq)]
pub struct CatchUpOperation {
    local: NodeId,
    realm_id: RealmId,
    now_ms: u64,
    /// The management nodes of the applied realm config, without this node.
    management: Vec<NodeId>,
    /// Each counted round's target with the management node that answered it.
    targets: Vec<(NodeId, irokle::ActorClock)>,
    unanswered: Vec<NodeId>,
    step: CatchUpStep,
}

impl CatchUpOperation {
    pub fn new(local: NodeId, realm_id: RealmId) -> Self {
        Self {
            local,
            realm_id,
            now_ms: 0,
            management: Vec::new(),
            targets: Vec::new(),
            unanswered: Vec::new(),
            step: CatchUpStep::Config,
        }
    }

    fn topic(&self) -> irokle::TopicId {
        let target = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        target.sync_topic_id(self.realm_id, &PlacementRef::NIL)
    }

    fn ask(&mut self, step: CatchUpStep, effect: CatchUpEffect) -> CatchUpNext {
        self.step = step;
        let effect = Effect::Net(NetEffect::ConfigCatchUp(Box::new(effect)));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    fn read_config(&mut self, step: CatchUpStep) -> CatchUpNext {
        self.step = step;
        let read = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    /// A management node without other management nodes counts through its own config.
    fn configured(&mut self, config: &RealmConfigDocument) -> CatchUpNext {
        self.management = management_nodes(config, self.local);
        match self.management.is_empty() {
            true => self.contact(),
            false => self.round(0),
        }
    }

    fn round(&mut self, index: usize) -> CatchUpNext {
        let Some(peer) = self.management.get(index).copied() else {
            return self.applied();
        };
        let topic = self.topic();
        self.ask(
            CatchUpStep::Round(index),
            CatchUpEffect::Round { topic, peer },
        )
    }

    fn answered(&mut self, index: usize, answer: CatchUpEvent) -> CatchUpNext {
        let peer = self
            .management
            .get(index)
            .copied()
            .ok_or(IoError::Unfinished)?;
        match answer {
            CatchUpEvent::Counted { target } => self.targets.push((peer, target)),
            CatchUpEvent::Partial | CatchUpEvent::Failed(_) => self.unanswered.push(peer),
            CatchUpEvent::Applied { .. } => return Err(unexpected("a sync round answer")),
        }
        self.round(index + 1)
    }

    /// Without any counted round the node has not caught up; it never waits on nothing.
    fn applied(&mut self) -> CatchUpNext {
        if self.targets.is_empty() {
            return self.finish(false);
        }
        let topic = self.topic();
        let targets = (self.targets.iter())
            .map(|(_, target)| target.clone())
            .collect();
        self.ask(
            CatchUpStep::Applied,
            CatchUpEffect::Applied { topic, targets },
        )
    }

    /// Asks the management nodes the applied config names that had no round yet. Only rounds
    /// with a current management node count; with none of them the node has not caught up.
    fn reloaded(&mut self, config: &RealmConfigDocument) -> CatchUpNext {
        let current = management_nodes(config, self.local);
        self.targets
            .retain(|(node_id, _)| current.contains(node_id));
        let asked = self.management.len();
        for node_id in &current {
            if !self.management.contains(node_id) {
                self.management.push(*node_id);
            }
        }
        if self.management.len() > asked {
            return self.round(asked);
        }
        match self.targets.is_empty() && !current.is_empty() {
            true => self.finish(false),
            false => self.contact(),
        }
    }

    fn contact(&mut self) -> CatchUpNext {
        self.step = CatchUpStep::Contact;
        let value = Value::from(self.now_ms.to_be_bytes().as_slice());
        let row = (
            COMPUTE_DEPARTURE_KEYSPACE.to_string(),
            Key::from(LAST_CONTACT_KEY),
            value,
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn finish(&mut self, caught_up: bool) -> CatchUpNext {
        let unanswered = std::mem::take(&mut self.unanswered);
        Ok(Next::Done(CatchUpOutcome {
            caught_up,
            unanswered,
        }))
    }
}

fn unexpected(expected: &'static str) -> IoError {
    IoError::Unexpected {
        expected,
        got: "another result",
    }
}

/// The catch-up step answer an effect result carries.
fn catch_up_answer(result: IoResult) -> Result<CatchUpEvent, IoError> {
    match result.event()? {
        Event::Net(NetEvent::ConfigCatchUp(answer)) => Ok(answer),
        _ => Err(unexpected("a realm config catch-up answer")),
    }
}

impl Operation for CatchUpOperation {
    type Output = CatchUpOutcome;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> CatchUpNext {
        self.read_config(CatchUpStep::Config)
    }

    fn next(&mut self, result: IoResult) -> CatchUpNext {
        match std::mem::replace(&mut self.step, CatchUpStep::Contact) {
            step @ (CatchUpStep::Config | CatchUpStep::Reload) => {
                let config = match result.child::<RealmConfigDocument, GetConfigError>()? {
                    Ok(config) => config,
                    Err(error) => {
                        warn!(error = %error, "Realm config is unreadable; the catch-up waits");
                        return self.finish(false);
                    }
                };
                match step {
                    CatchUpStep::Config => self.configured(&config),
                    _ => self.reloaded(&config),
                }
            }
            CatchUpStep::Round(index) => {
                let answer = catch_up_answer(result)?;
                self.answered(index, answer)
            }
            CatchUpStep::Applied => match catch_up_answer(result)? {
                CatchUpEvent::Applied { covered: true } => self.read_config(CatchUpStep::Reload),
                CatchUpEvent::Applied { covered: false } | CatchUpEvent::Failed(_) => {
                    self.finish(false)
                }
                CatchUpEvent::Counted { .. } | CatchUpEvent::Partial => {
                    Err(unexpected("an applied cursor answer"))
                }
            },
            CatchUpStep::Contact => {
                result.done()?;
                if !self.unanswered.is_empty() {
                    debug!(unanswered = ?self.unanswered, "Caught up without every management node");
                }
                self.finish(true)
            }
        }
    }
}

/// The management nodes of `config` other than `local`. A device does not sync the realm config
/// with them and runs only its owner's local jobs, so it waits on none.
fn management_nodes(config: &RealmConfigDocument, local: NodeId) -> Vec<NodeId> {
    let syncs = (config.nodes.iter())
        .find(|node| NodeId::from_str(&node.node_id).ok() == Some(local))
        .is_some_and(|node| node.kind.is_sync_eligible());
    if !syncs {
        return Vec::new();
    }
    (config.nodes.iter())
        .filter(|node| node.kind == RealmNodeKind::Management)
        .filter_map(|node| NodeId::from_str(&node.node_id).ok())
        .filter(|node_id| *node_id != local)
        .collect()
}

/// When the next pass is due after `outcome`.
pub fn next_pass(outcome: &Result<CatchUpOutcome, IoError>) -> Duration {
    match outcome {
        Ok(outcome) if outcome.caught_up => CATCH_UP_REFRESH,
        Ok(_) => CATCH_UP_RETRY,
        Err(error) => {
            warn!(error = %error, "Realm config catch-up failed");
            CATCH_UP_RETRY
        }
    }
}

/// The timer of the first catch-up pass of a start.
pub fn catch_up_timer() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::ConfigCatchUp,
        after: Duration::ZERO,
    })
}

#[cfg(test)]
#[path = "catch_up_tests.rs"]
mod tests;
