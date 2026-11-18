//! Emits an origin watch event after its commit: expands it for this node's inboxes when this
//! node holds a matching watch, and forwards it once to each remote holder the interest names.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::structs::execution::notification::NotificationTransportMessage;
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventDetail, WatchEventKind, watch_path_matches,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::GroupId;
use tracing::warn;
use ulid::Ulid;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::notifications::client::{HolderRequestOperation, RemoteError, events_delivered};
use crate::notifications::placement::filter_local_subscriptions;
use crate::notifications::unexpected_event;
use crate::notifications::watch::digest::{InterestError, InterestOperation};
use crate::notifications::watch::expansion::{ExpandError, ExpandOutcome, WatchExpandOperation};
use crate::notifications::watch::listing::{ListWatchesOperation, WatchPage, WatchScope};
use crate::notifications::watch::records::WatchSubscriptionError;

#[derive(Debug, Default, PartialEq)]
enum EmitState {
    #[default]
    Config,
    List,
    /// Asks for a publish after this node's stale watches, then delivers.
    Stale,
    Expand,
    Wake {
        dropped: bool,
    },
    Dropped,
    Forward(NodeId),
}

/// Delivers one event to known holders. Best effort: every failure only warns. System writes
/// with a nil actor are not emitted, since watches are a user-plane feature.
#[derive(Debug, PartialEq)]
pub(crate) struct EmitWatchOperation {
    event: WatchEvent,
    local: NodeId,
    /// The remote holders the interest table matches, and this node when it holds a watch.
    holders: Vec<NodeId>,
    config: Option<RealmConfigDocument>,
    pending: VecDeque<NodeId>,
    state: EmitState,
}

type Turn = Result<Next<()>, IoError>;

impl EmitWatchOperation {
    /// `holders` are the nodes this node's interest table matches for the event.
    pub fn new(event: WatchEvent, local: NodeId, holders: Vec<NodeId>) -> Self {
        Self {
            event,
            local,
            holders,
            config: None,
            pending: VecDeque::new(),
            state: EmitState::Config,
        }
    }

    fn configured(&mut self, config: Result<RealmConfigDocument, GetConfigError>) -> Turn {
        match config {
            Ok(config) => {
                self.config = Some(config);
                self.state = EmitState::List;
                let list = ListWatchesOperation::new(WatchScope::Realm(self.event.realm_id));
                Ok(Next::Call(IoCall::child(list)))
            }
            Err(error) => {
                warn!(%error, "Failed to scan local watch subscriptions while emitting event");
                self.deliver()
            }
        }
    }

    /// Adds or drops this node as a holder from its own durable watches; an oversized scan
    /// keeps it, so no local watch is missed.
    fn listed(&mut self, listed: Result<WatchPage, WatchSubscriptionError>) -> Turn {
        let local = self.local;
        let subscriptions = match listed {
            Ok(page) => page.subscriptions,
            Err(WatchSubscriptionError::Storage(error))
                if error.contains("subscription scan cap reached") =>
            {
                if !self.holders.contains(&local) {
                    self.holders.push(local);
                }
                return self.deliver();
            }
            Err(error) => {
                warn!(%error, "Failed to scan local watch subscriptions while emitting event");
                self.config = None;
                return self.deliver();
            }
        };
        let Some(config) = self.config.as_ref() else {
            return self.deliver();
        };
        let (held, stale) = match filter_local_subscriptions(subscriptions, config, local) {
            Ok(held) => held,
            Err(error) => {
                warn!(%error, "Failed to scan local watch subscriptions while emitting event");
                self.config = None;
                return self.deliver();
            }
        };
        self.holders.retain(|holder| *holder != local);
        let event = &self.event;
        let matches = held.iter().any(|watch| {
            watch.created_at_ms <= event.occurred_at_ms
                && watch.event_mask.contains(event.kind)
                && watch_path_matches(event.kind, &event.path, &watch.path_prefix)
        });
        if matches {
            self.holders.push(local);
        }
        if stale {
            self.state = EmitState::Stale;
            return Ok(Next::Call(IoCall::child(InterestOperation::mark(
                event.realm_id,
            ))));
        }
        self.deliver()
    }

    /// Orders the holders, then delivers to each once.
    fn deliver(&mut self) -> Turn {
        let mut holders = std::mem::take(&mut self.holders);
        holders.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        holders.dedup();
        self.pending = holders.into();
        self.next_holder()
    }

    fn next_holder(&mut self) -> Turn {
        let Some(holder) = self.pending.pop_front() else {
            return Ok(Next::Done(()));
        };
        if holder != self.local {
            self.state = EmitState::Forward(holder);
            let events = vec![self.event.clone()];
            let deliver = NotificationTransportMessage::DeliverWatchEvents { events };
            return Ok(Next::Call(IoCall::child(HolderRequestOperation::new(
                holder, deliver,
            ))));
        }
        // Without the realm config this node cannot expand for itself.
        let Some(config) = self.config.clone() else {
            return self.next_holder();
        };
        let (event, local) = (self.event.clone(), self.local);
        let realm_id = event.realm_id;
        let build = move || {
            let events = vec![event.clone()];
            WatchExpandOperation::new(realm_id, config.clone(), local, events, None)
        };
        let expand = boxed_retry(build, realm_id.as_bytes(), |result| {
            Event::Child(ChildOutcome::new(result))
        });
        self.state = EmitState::Expand;
        Ok(Next::Call(IoCall::Child(expand)))
    }

    fn expanded(&mut self, outcome: Result<ExpandOutcome, ExpandError>) -> Turn {
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                warn!(%error, "Failed to expand watch event for local holder");
                return self.next_holder();
            }
        };
        let dropped = outcome.dropped;
        if outcome.inbox.recipients.is_empty() {
            return self.after_wake(dropped);
        }
        self.state = EmitState::Wake { dropped };
        let wake = Effect::Net(NetEffect::WakeInbox(outcome.inbox.recipients));
        Ok(Next::Call(IoCall::from(wake)))
    }

    fn after_wake(&mut self, dropped: bool) -> Turn {
        if !dropped {
            return self.next_holder();
        }
        self.state = EmitState::Dropped;
        let mark = InterestOperation::mark(self.event.realm_id);
        Ok(Next::Call(IoCall::child(mark)))
    }
}

impl Operation for EmitWatchOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Turn {
        if self.event.actor == UserId::nil(self.event.realm_id) {
            return Ok(Next::Done(()));
        }
        let config = GetConfigOperation::new(self.event.realm_id);
        Ok(Next::Call(IoCall::child(config)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::take(&mut self.state) {
            EmitState::Config => {
                self.configured(result.child::<RealmConfigDocument, GetConfigError>()?)
            }
            EmitState::List => self.listed(result.child::<WatchPage, WatchSubscriptionError>()?),
            EmitState::Stale => {
                if let Err(error) = result.child::<bool, InterestError>()? {
                    warn!(%error, "Failed to mark stale watch interest dirty while emitting event");
                }
                self.deliver()
            }
            EmitState::Expand => self.expanded(result.child::<ExpandOutcome, ExpandError>()?),
            EmitState::Wake { dropped } => match result.event()? {
                Event::Net(NetEvent::InboxWoken) => self.after_wake(dropped),
                other => Err(unexpected_event("NetEvent::InboxWoken", &other)),
            },
            EmitState::Dropped => {
                if let Err(error) = result.child::<bool, InterestError>()? {
                    warn!(%error, "Failed to retract dropped local watch interest");
                }
                self.next_holder()
            }
            EmitState::Forward(holder) => {
                let answer = result.child::<NotificationTransportMessage, RemoteError>()?;
                if let Err(error) = events_delivered(answer) {
                    warn!(holder = %holder, %error, "Failed to forward watch event to remote holder");
                }
                self.next_holder()
            }
        }
    }
}

/// Post-commit emission of an origin watch event to the holders this node's interest matches
/// and to this node when it holds a matching watch. A node without a net handle emits nothing.
#[derive(Debug, PartialEq)]
pub struct EmitEventOperation {
    event: WatchEvent,
    emitting: bool,
}

impl EmitEventOperation {
    pub fn new(event: WatchEvent) -> Self {
        Self {
            event,
            emitting: false,
        }
    }

    /// The event of every dataset creation path, so an import or a job notifies exactly like
    /// `POST /metadata`. `event_id` dates it, keeping a retried create's inbox key stable.
    pub fn metadata_created(
        realm_id: RealmId,
        actor: UserId,
        group_id: GroupId,
        document_id: Ulid,
        document_path: &str,
        event_id: Ulid,
    ) -> Self {
        Self::new(WatchEvent {
            event_id,
            realm_id,
            kind: WatchEventKind::MetadataCreated,
            path: format!("meta/{group_id}/{document_path}"),
            actor,
            occurred_at_ms: event_id.timestamp_ms(),
            detail: WatchEventDetail::MetadataCreated {
                group_id,
                document_id,
            },
        })
    }
}

impl Operation for EmitEventOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Turn {
        let holders = NetEffect::WatchHolders {
            realm_id: self.event.realm_id,
            path: self.event.path.clone(),
            kind: self.event.kind,
        };
        Ok(Next::Call(IoCall::from(Effect::Net(holders))))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        if self.emitting {
            return result.child::<(), IoError>()?.map(Next::Done);
        }
        match result.event()? {
            Event::Net(NetEvent::WatchHolders(None)) => Ok(Next::Done(())),
            Event::Net(NetEvent::WatchHolders(Some((local, holders)))) => {
                self.emitting = true;
                let emit = EmitWatchOperation::new(self.event.clone(), local, holders);
                Ok(Next::Call(IoCall::child(emit)))
            }
            other => Err(unexpected_event("NetEvent::WatchHolders", &other)),
        }
    }
}

#[cfg(test)]
#[path = "origin_tests.rs"]
mod tests;
