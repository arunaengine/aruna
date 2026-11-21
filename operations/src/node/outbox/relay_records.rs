//! Handles the drained records this node can never publish itself: admin records go to a holder
//! under their origin's signature and leave the outbox; a device drops rows no device publishes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentOutboxEvent;
use aruna_core::document_apply::ApplyError;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::row_plan::PlanOperation;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use irokle::TopicId;
use tracing::{error, info, warn};

use crate::forward::realm::RelayAdminOperation;
use crate::forward::transport::ForwardError;
use crate::realm::peer_trust::is_sync_eligible;
use crate::sync::document_outbox::DeleteOutboxPlan;
use crate::sync::outbox::defer::DrainRecord;

/// Which records left this node's custody and how many stay undeliverable in the outbox, and
/// whether a relay or its cleanup failed, so the drain runs again. A relay that did not go out
/// holds its shard topic and origin; `held` counts the later relays that wait behind one.
#[derive(Debug, Default, PartialEq)]
pub struct RelayedRecords {
    pub relayed: Vec<Vec<u8>>,
    pub left: usize,
    pub retry: bool,
    pub held: usize,
    pub held_topics: Vec<TopicId>,
    pub held_origins: Vec<NodeId>,
    /// Origins with a relay a holder took or none could: their later records wait.
    pub kept_origins: Vec<NodeId>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum RelayStep {
    Sign,
    Relay,
    Delete,
}

type RelayTurn = Result<Next<RelayedRecords>, IoError>;

/// Relays the undeliverable records of one page in order and deletes those a holder took.
#[derive(Debug, PartialEq)]
pub struct RelayRecordsOperation {
    local: NodeId,
    config: Option<Arc<RealmConfigDocument>>,
    device: bool,
    records: VecDeque<DrainRecord>,
    current: Option<DrainRecord>,
    out: RelayedRecords,
    step: RelayStep,
    now_ms: u64,
}

impl RelayRecordsOperation {
    pub fn new(
        local: NodeId,
        config: Option<Arc<RealmConfigDocument>>,
        records: Vec<DrainRecord>,
    ) -> Self {
        // A device holds no sync topic, so it can never become a holder of what it wrote.
        let device = (config.as_deref())
            .and_then(|config| config.node_kind(&local))
            .is_some_and(|kind| !kind.is_sync_eligible());
        Self {
            local,
            config,
            device,
            records: records.into(),
            current: None,
            out: RelayedRecords::default(),
            step: RelayStep::Sign,
            now_ms: 0,
        }
    }

    /// Starts the relay of the next record that has one, or deletes the relayed records.
    fn advance(&mut self) -> RelayTurn {
        while let Some(record) = self.records.pop_front() {
            if let Some(turn) = self.start(record) {
                return turn;
            }
        }
        if self.out.relayed.is_empty() {
            return Ok(Next::Done(std::mem::take(&mut self.out)));
        }
        let keys = self.out.relayed.clone();
        self.step = RelayStep::Delete;
        Ok(Next::Call(IoCall::child(PlanOperation::new(
            DeleteOutboxPlan { keys },
        ))))
    }

    /// The call that relays `record`, or `None` once it is decided without one.
    fn start(&mut self, record: DrainRecord) -> Option<RelayTurn> {
        // A relay behind one that did not go out, by origin or shard topic, waits for it.
        let origin = match &record.1.event {
            DocumentOutboxEvent::AdminOperation { event, .. } => Some(event.origin_node_id),
            _ => None,
        };
        let out = &self.out;
        if origin.is_some_and(|origin| {
            out.held_origins.contains(&origin) || out.held_topics.contains(&record.2)
        }) {
            self.hold(&record);
            self.out.held += 1;
            return None;
        }
        let admin = match &record.1.event {
            DocumentOutboxEvent::AdminOperation {
                event,
                origin_signature,
            } if self.config.is_some() => Some((event.clone(), *origin_signature)),
            _ => None,
        };
        match admin {
            Some((_, Some(signature))) => self.relay(record, signature),
            Some((event, None)) if event.origin_node_id == self.local => {
                let placement = record.1.placement;
                self.current = Some(record);
                self.step = RelayStep::Sign;
                let sign = Effect::Net(NetEffect::AdminSign { event, placement });
                Some(Ok(Next::Call(IoCall::from(sign))))
            }
            _ => {
                self.left(record);
                None
            }
        }
    }

    /// Hands `record` to a holder under `signature`, or leaves it when no holder exists.
    fn relay(&mut self, record: DrainRecord, signature: iroh::Signature) -> Option<RelayTurn> {
        let holders = match self.config.as_deref() {
            Some(config) => resolve_shard_holders(config, &record.1.placement),
            None => Vec::new(),
        };
        let event = match &record.1.event {
            DocumentOutboxEvent::AdminOperation { event, .. } => Some(event.clone()),
            _ => None,
        };
        let Some(event) = event.filter(|_| !holders.is_empty()) else {
            self.left(record);
            return None;
        };
        let config = self.config.as_deref();
        if !config.is_some_and(|config| is_sync_eligible(config, event.origin_node_id)) {
            self.rejected(record);
            return None;
        }
        let (target, placement) = (record.1.target.clone(), record.1.placement);
        let relay = RelayAdminOperation::new(
            Some(self.local),
            &holders,
            target,
            event,
            placement,
            signature,
        );
        self.current = Some(record);
        self.step = RelayStep::Relay;
        Some(Ok(Next::Call(IoCall::child(relay))))
    }

    /// Reports a relay a holder rejected, or whose origin this node's config no longer admits: no
    /// retry changes that verdict, so the row stays without one and holds nothing.
    fn rejected(&mut self, record: DrainRecord) {
        error!(
            event = "pipeline.drain.relay_rejected",
            target = ?record.1.target,
            "A relayed admin outbox record was rejected; leaving it in the outbox"
        );
        self.out.left += 1;
    }

    /// Holds the shard topic and admin origin of a relay that did not go out for the later ones.
    fn hold(&mut self, (_, record, topic): &DrainRecord) {
        self.out.held_topics.push(*topic);
        if let DocumentOutboxEvent::AdminOperation { event, .. } = &record.event {
            self.out.held_origins.push(event.origin_node_id);
        }
    }

    /// A record that stays with this node: a device drops a row no device publishes, any other
    /// stays in the outbox and is reported.
    fn left(&mut self, (record_key, record, topic): DrainRecord) {
        if self.device && !matches!(record.event, DocumentOutboxEvent::AdminOperation { .. }) {
            warn!(
                event = "pipeline.drain.dropped",
                target = ?record.target,
                "Dropping a document sync outbox record no device can publish"
            );
            self.out.relayed.push(record_key);
            return;
        }
        self.out.left += 1;
        if let DocumentOutboxEvent::AdminOperation { event, .. } = &record.event {
            self.out.kept_origins.push(event.origin_node_id);
        }
        error!(
            event = "pipeline.drain.undeliverable",
            target = ?record.target,
            %topic,
            strategy = %record.placement.strategy_id,
            shard = record.placement.shard,
            age_ms = self.now_ms.saturating_sub(record.outbox_id.timestamp_ms()),
            "Cannot publish a document sync outbox record from this node and it is not relayable; leaving it in the outbox"
        );
    }
}

impl Operation for RelayRecordsOperation {
    type Output = RelayedRecords;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> RelayTurn {
        self.advance()
    }

    fn next(&mut self, result: IoResult) -> RelayTurn {
        if self.step == RelayStep::Delete {
            if let Err(error) = result.child::<(), ApplyError>()? {
                warn!(%error, "Failed to delete relayed admin outbox records");
                self.out.retry = true;
            }
            return Ok(Next::Done(std::mem::take(&mut self.out)));
        }
        let Some(record) = self.current.take() else {
            return Err(IoError::Unfinished);
        };
        if self.step == RelayStep::Sign {
            let signed = match result.event()? {
                Event::Net(NetEvent::AdminSigned(signed)) => signed,
                other => Err(format!("unexpected answer {}", other.label())),
            };
            match signed {
                Ok(signature) => {
                    if let Some(turn) = self.relay(record, signature) {
                        return turn;
                    }
                }
                Err(error) => {
                    warn!(%error, "Cannot sign an admin event for relay");
                    self.out.retry = true;
                    self.hold(&record);
                    self.left(record);
                }
            }
            return self.advance();
        }
        match result.child::<(), ForwardError>()? {
            Ok(()) => {
                if let DocumentOutboxEvent::AdminOperation { event, .. } = &record.1.event {
                    self.out.kept_origins.push(event.origin_node_id);
                    info!(
                        event = "pipeline.drain.relayed",
                        target = ?record.1.target,
                        origin = %event.origin_node_id,
                        "Relayed an admin outbox record to a holder"
                    );
                }
                self.out.relayed.push(record.0);
            }
            Err(ForwardError::Forbidden) => self.rejected(record),
            Err(error) => {
                warn!(
                    event = "pipeline.drain.relay_failed",
                    target = ?record.1.target,
                    %error,
                    "No holder accepted a relayed admin outbox record"
                );
                self.out.retry = true;
                self.hold(&record);
                self.left(record);
            }
        }
        self.advance()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget,
    };
    use aruna_core::calls::ChildOutcome;
    use aruna_core::document::DocumentTarget;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::user::id::UserId;
    use ulid::Ulid;

    use crate::sync::outbox::record::new_identified_record;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    /// An admin record this node originated and has not signed yet.
    fn admin_record() -> DrainRecord {
        let realm_id = RealmId([3; 32]);
        let user_id = UserId::nil(realm_id);
        let event = AdminDocumentEvent {
            event_id: Ulid::from_parts(9, 1),
            target: AdminDocumentTarget::User { user_id },
            origin_node_id: node(),
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor: Actor {
                node_id: node(),
                user_id,
                realm_id,
            },
            op: AdminDocumentOperation::UserNameSet {
                name: "user".to_string(),
            },
        };
        let target = DocumentTarget::Group {
            group_id: Ulid::from_parts(7, 1),
        };
        let event = DocumentOutboxEvent::admin(event);
        let (id, placement) = (Ulid::from_parts(10, 1), PlacementRef::NIL);
        let record = new_identified_record(id, node(), target, Vec::new(), event, placement, false);
        (b"admin".to_vec(), record, TopicId::hash(b"topic"))
    }

    // An own admin record is signed before its relay; one that cannot be signed stays in the
    // outbox and counts as undeliverable.
    #[test]
    fn unsigned_record_left() {
        let config = RealmConfigDocument::new(RealmId([3; 32]), Vec::new(), 2);
        let records = vec![admin_record()];
        let mut run = RelayRecordsOperation::new(node(), Some(Arc::new(config)), records);
        let Ok(Next::Call(IoCall::Effect(effect))) = run.first() else {
            panic!("the record is signed first");
        };
        assert!(matches!(*effect, Effect::Net(NetEffect::AdminSign { .. })));
        let refused = Event::Net(NetEvent::AdminSigned(Err("no key".to_string())));
        let left = RelayedRecords {
            left: 1,
            retry: true,
            held_topics: vec![TopicId::hash(b"topic")],
            held_origins: vec![node()],
            kept_origins: vec![node()],
            ..RelayedRecords::default()
        };
        assert_eq!(
            run.next(IoResult::Event(Box::new(refused))),
            Ok(Next::Done(left))
        );
    }

    // Without a realm configuration no record is relayed, and none is dropped.
    #[test]
    fn unconfigured_record_left() {
        let mut run = RelayRecordsOperation::new(node(), None, vec![admin_record()]);
        let left = RelayedRecords {
            left: 1,
            kept_origins: vec![node()],
            ..RelayedRecords::default()
        };
        assert_eq!(run.first(), Ok(Next::Done(left)));
    }

    /// A configuration whose one server holds the shard of [`signed_record`].
    fn holder_config() -> RealmConfigDocument {
        use aruna_core::structs::identity::realm::RealmNodeKind;
        use aruna_core::structs::placement::record::PlacementStrategy;

        let mut config = RealmConfigDocument::new(RealmId([3; 32]), Vec::new(), 2);
        config.strategies = vec![PlacementStrategy {
            strategy_id: Ulid::from_bytes([9; 16]),
            name: "default".to_string(),
            replica_count: None,
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 1,
        }];
        config.default_strategy_id = Some(Ulid::from_bytes([9; 16]));
        let holder = iroh::SecretKey::from_bytes(&[2; 32]).public();
        config.ensure_node(holder, RealmNodeKind::Server);
        // The origins of the relayed records, which this config admits.
        config.ensure_node(node(), RealmNodeKind::Server);
        config.ensure_node(
            iroh::SecretKey::from_bytes(&[5; 32]).public(),
            RealmNodeKind::Server,
        );
        config.snapshot_candidate_map();
        config
    }

    /// An admin record of `origin` under `key`, signed and placed on the held shard.
    fn signed_record(key: &[u8], origin: NodeId) -> DrainRecord {
        let mut record = admin_record();
        record.0 = key.to_vec();
        record.1.placement = PlacementRef {
            strategy_id: Ulid::from_bytes([9; 16]),
            shard: 0,
        };
        if let DocumentOutboxEvent::AdminOperation {
            event,
            origin_signature,
        } = &mut record.1.event
        {
            event.origin_node_id = origin;
            *origin_signature = Some(iroh::SecretKey::from_bytes(&[1; 32]).sign(b"event"));
        }
        record
    }

    // A relay no holder accepted leaves the record and asks for another drain, so the record
    // keeps a retry when it is the last one in the outbox.
    #[test]
    fn failed_relay_retries() {
        let records = vec![signed_record(b"admin", node())];
        let mut run = RelayRecordsOperation::new(node(), Some(Arc::new(holder_config())), records);
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Child(_)))));
        let refused = Err::<(), _>(ForwardError::Unavailable);
        let left = RelayedRecords {
            left: 1,
            retry: true,
            held_topics: vec![TopicId::hash(b"topic")],
            held_origins: vec![node()],
            kept_origins: vec![node()],
            ..RelayedRecords::default()
        };
        let answer = run.next(IoResult::Child(ChildOutcome::new(refused)));
        assert_eq!(answer, Ok(Next::Done(left)));
    }

    // A holder's explicit rejection judges the envelope: the record is reported and stays, but no
    // retry is scheduled for it and no later relay waits behind it.
    #[test]
    fn rejected_relay_reported() {
        let records = vec![signed_record(b"admin", node())];
        let mut run = RelayRecordsOperation::new(node(), Some(Arc::new(holder_config())), records);
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Child(_)))));
        let rejected = Err::<(), _>(ForwardError::Forbidden);
        let left = RelayedRecords {
            left: 1,
            ..RelayedRecords::default()
        };
        let answer = run.next(IoResult::Child(ChildOutcome::new(rejected)));
        assert_eq!(answer, Ok(Next::Done(left)));
    }

    // After a holder rejects a record, the later record of its origin still gets its own verdict
    // instead of waiting behind the rejected one.
    #[test]
    fn rejected_relay_releases() {
        let records = vec![
            signed_record(b"admin/1", node()),
            signed_record(b"admin/2", node()),
        ];
        let mut run = RelayRecordsOperation::new(node(), Some(Arc::new(holder_config())), records);
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Child(_)))));
        let rejected = Err::<(), _>(ForwardError::Forbidden);
        let later = run.next(IoResult::Child(ChildOutcome::new(rejected)));
        assert!(matches!(later, Ok(Next::Call(IoCall::Child(_)))));
    }

    // An origin this node's own config no longer admits gets no holder's verdict: the record is
    // reported and stays, without a retry.
    #[test]
    fn removed_origin_reported() {
        let removed = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let records = vec![signed_record(b"admin", removed)];
        let mut run = RelayRecordsOperation::new(node(), Some(Arc::new(holder_config())), records);
        let left = RelayedRecords {
            left: 1,
            ..RelayedRecords::default()
        };
        assert_eq!(run.first(), Ok(Next::Done(left)));
    }

    // A relay to the shard topic of one that failed waits behind it, also from another origin,
    // so the holder never takes it first.
    #[test]
    fn failed_relay_holds() {
        let other = iroh::SecretKey::from_bytes(&[5; 32]).public();
        let records = vec![
            signed_record(b"admin/1", node()),
            signed_record(b"admin/2", other),
        ];
        let mut run = RelayRecordsOperation::new(node(), Some(Arc::new(holder_config())), records);
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Child(_)))));
        let refused = Err::<(), _>(ForwardError::Unavailable);
        let Ok(Next::Done(out)) = run.next(IoResult::Child(ChildOutcome::new(refused))) else {
            panic!("the later relay is not attempted");
        };
        assert_eq!((out.left, out.held), (1, 1));
        assert_eq!(out.held_origins, vec![node(), other]);
    }
}
