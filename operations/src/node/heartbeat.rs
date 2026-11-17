//! Pushes and keeps heartbeats with live telemetry between sync peers.
//! One stream carries one length-prefixed heartbeat and no response; nothing is written to disk.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, EffectGroup, GroupForm, NetEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult, NetEvent};
use aruna_core::heartbeat::{
    HeartbeatEffect, HeartbeatEvent, INFO_PUBLISH_INTERVAL, NodeHeartbeat,
};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::{Effects, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::{debug, warn};

/// Heartbeat sends in flight at once.
const PARALLEL_SENDS: usize = 16;
/// Longest one send pass over all peers may take, well inside the heartbeat interval.
const SEND_PASS_TIMEOUT: Duration = Duration::from_secs(INFO_PUBLISH_INTERVAL.as_secs() / 2);

/// Why a heartbeat run stopped; a failed send or an invalid heartbeat is only logged.
#[derive(Debug, Error, PartialEq)]
pub enum HeartbeatError {
    #[error("unexpected heartbeat event {0}")]
    UnexpectedEvent(&'static str),
    #[error("the heartbeat run has not finished")]
    Unfinished,
}

#[derive(Debug, PartialEq)]
enum SendPhase {
    Idle,
    Record,
    Push,
    Done(Result<(), HeartbeatError>),
}

/// Keeps this node's own heartbeat and pushes it to every sync peer, at most
/// [`PARALLEL_SENDS`] at once and for at most [`SEND_PASS_TIMEOUT`]. Sends are best effort: a
/// failed one only logs, and the next tick sends again.
#[derive(Debug, PartialEq)]
pub(crate) struct SendHeartbeatOperation {
    heartbeat: Option<NodeHeartbeat>,
    sequence: u64,
    frame: Value,
    phase: SendPhase,
}

impl SendHeartbeatOperation {
    pub(crate) fn new(heartbeat: NodeHeartbeat) -> Self {
        Self {
            heartbeat: Some(heartbeat),
            sequence: 0,
            frame: Value::from(Vec::new()),
            phase: SendPhase::Idle,
        }
    }

    fn finish(&mut self, result: Result<(), HeartbeatError>) -> Effects {
        self.phase = SendPhase::Done(result);
        smallvec![]
    }

    /// One push per peer; each pass starts at another peer, so peers that time out cannot
    /// starve the ones after them.
    fn push(&mut self, mut peers: Vec<NodeId>) -> Effects {
        if peers.is_empty() {
            return self.finish(Ok(()));
        }
        let start = self.sequence % peers.len() as u64;
        peers.rotate_left(start as usize);
        let members = peers
            .into_iter()
            .map(|peer| {
                Effect::Net(NetEffect::Heartbeat(Box::new(HeartbeatEffect::Push {
                    peer,
                    frame: self.frame.clone(),
                })))
            })
            .collect();
        self.phase = SendPhase::Push;
        smallvec![Effect::Group(EffectGroup {
            members,
            limit: PARALLEL_SENDS,
            form: GroupForm::All {
                deadline: Some(SEND_PASS_TIMEOUT),
            },
        })]
    }
}

impl EffectOperation for SendHeartbeatOperation {
    type Output = ();
    type Error = HeartbeatError;

    fn start(&mut self) -> Effects {
        let Some(heartbeat) = self.heartbeat.take() else {
            return self.finish(Err(HeartbeatError::Unfinished));
        };
        let bytes = match heartbeat.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => {
                warn!(%error, "Not sending an invalid heartbeat");
                return self.finish(Ok(()));
            }
        };
        self.sequence = heartbeat.sequence;
        let length = (bytes.len() as u32).to_be_bytes();
        self.frame = Value::from([length.as_slice(), &bytes].concat());
        self.phase = SendPhase::Record;
        smallvec![Effect::Net(NetEffect::Heartbeat(Box::new(
            HeartbeatEffect::Record(Box::new(heartbeat))
        )))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::replace(&mut self.phase, SendPhase::Idle), event) {
            (SendPhase::Record, Event::Net(NetEvent::Heartbeat(HeartbeatEvent::Peers(peers)))) => {
                self.push(peers)
            }
            // Without a net handle there is nobody to send to.
            (SendPhase::Record, Event::Net(NetEvent::Error(_))) => self.finish(Ok(())),
            (SendPhase::Push, Event::Group(GroupEvent::Finished { results })) => {
                for result in results {
                    match result {
                        GroupResult::Done(event) => match *event {
                            Event::Net(NetEvent::Heartbeat(HeartbeatEvent::Pushed)) => {}
                            Event::Net(NetEvent::Heartbeat(HeartbeatEvent::PushFailed(error))) => {
                                debug!(%error, "Heartbeat send failed")
                            }
                            other => debug!(event = other.label(), "Heartbeat send failed"),
                        },
                        GroupResult::Unfinished => {
                            debug!("Heartbeat send pass stopped at its time limit")
                        }
                        GroupResult::Refused => debug!("Heartbeat send was refused"),
                    }
                }
                self.finish(Ok(()))
            }
            (_, event) => self.finish(Err(HeartbeatError::UnexpectedEvent(event.label()))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.phase, SendPhase::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.phase {
            SendPhase::Done(result) => result,
            _ => Err(HeartbeatError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Keeps one heartbeat frame `peer` sent when it decodes, names this node's realm, and is newer
/// than the held one of a sync peer. Every refusal is only logged.
#[derive(Debug, PartialEq)]
pub(crate) struct ReceiveHeartbeatOperation {
    peer: NodeId,
    realm_id: RealmId,
    frame: Option<Vec<u8>>,
    result: Option<Result<(), HeartbeatError>>,
}

impl ReceiveHeartbeatOperation {
    pub(crate) fn new(peer: NodeId, realm_id: RealmId, frame: Vec<u8>) -> Self {
        Self {
            peer,
            realm_id,
            frame: Some(frame),
            result: None,
        }
    }
}

impl EffectOperation for ReceiveHeartbeatOperation {
    type Output = ();
    type Error = HeartbeatError;

    fn start(&mut self) -> Effects {
        let peer = self.peer;
        let Some(frame) = self.frame.take() else {
            self.result = Some(Err(HeartbeatError::Unfinished));
            return smallvec![];
        };
        let heartbeat = match NodeHeartbeat::from_bytes(&frame) {
            Ok(heartbeat) => heartbeat,
            Err(error) => {
                debug!(%peer, %error, "Dropping an invalid heartbeat");
                self.result = Some(Ok(()));
                return smallvec![];
            }
        };
        if heartbeat.realm_id != self.realm_id {
            debug!(%peer, "Dropping a heartbeat of another realm");
            self.result = Some(Ok(()));
            return smallvec![];
        }
        smallvec![Effect::Net(NetEffect::Heartbeat(Box::new(
            HeartbeatEffect::Store {
                peer,
                heartbeat: Box::new(heartbeat),
            }
        )))]
    }

    fn step(&mut self, event: Event) -> Effects {
        let peer = self.peer;
        self.result = Some(match (self.result.is_none(), event) {
            (true, Event::Net(NetEvent::Heartbeat(HeartbeatEvent::Stored(stored)))) => {
                if !stored {
                    debug!(%peer, "Heartbeat not stored: not newer, too frequent or not a sync peer");
                }
                Ok(())
            }
            (_, event) => Err(HeartbeatError::UnexpectedEvent(event.label())),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.result.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.result.unwrap_or(Err(HeartbeatError::Unfinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use aruna_core::compute::quota::{ComputeDemandSnapshot, ComputeReservationSnapshot};
    use aruna_core::compute::{ExecutorAvailability, ExecutorCapability};
    use aruna_core::heartbeat::{HeldHeartbeat, live_telemetry};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::policy::PlacementSubject;
    use aruna_core::structs::storage::node_info::NodeInfoDocument;
    use aruna_core::structs::storage::node_info::{AdvertisementEpoch, NodeUrls, NodeUtilization};

    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn utilization(load_permille: Option<u32>) -> NodeUtilization {
        NodeUtilization {
            storage_bytes_used: 0,
            documents_held: None,
            load_permille,
            heartbeat_at_ms: 0,
            download_slots: None,
            storage_capacity_bytes: None,
        }
    }

    fn availability(active_executions: u32) -> ExecutorAvailability {
        ExecutorAvailability {
            free_cpu_cores: Some(4),
            free_ram_bytes: None,
            free_disk_bytes: None,
            active_executions,
            observed_at_ms: 1,
        }
    }

    fn document(node_id: NodeId, publisher_generation: u64) -> NodeInfoDocument {
        let subject = PlacementSubject {
            node_id,
            generation: 1,
            location: String::new(),
            labels: BTreeMap::new(),
            executor_kind: None,
            local_to_controller: true,
        };
        let mut executor = ExecutorCapability::new("docker".to_string(), subject).unwrap();
        executor.availability = Some(availability(9));
        NodeInfoDocument {
            node_id,
            executors: vec![executor],
            labels: BTreeMap::new(),
            urls: NodeUrls {
                api: None,
                s3: None,
            },
            utilization: utilization(Some(900)),
            updated_at_ms: 1,
            epoch: AdvertisementEpoch {
                membership_generation: 1,
                publisher_generation,
                observed_at_ms: 1,
            },
            compute_draining: false,
            leaving: false,
            demand: ComputeDemandSnapshot::default(),
            reservation: ComputeReservationSnapshot::default(),
        }
    }

    fn held(node_id: NodeId, publisher_generation: u64) -> HeldHeartbeat {
        let epoch = AdvertisementEpoch {
            membership_generation: 1,
            publisher_generation,
            observed_at_ms: 0,
        };
        HeldHeartbeat {
            node_id,
            heartbeat: NodeHeartbeat {
                realm_id: RealmId([1; 32]),
                epoch,
                sequence: 1,
                utilization: utilization(Some(100)),
                availability: vec![("docker".to_string(), availability(2))],
                reservation: ComputeReservationSnapshot {
                    epoch,
                    ..ComputeReservationSnapshot::default()
                },
                demand: ComputeDemandSnapshot {
                    epoch,
                    ..ComputeDemandSnapshot::default()
                },
                jobs: None,
            },
            age_ms: 5_000,
            received_at_ms: 0,
        }
    }

    /// Telemetry comes from a heartbeat naming the stored advertisement, observed at its receive
    /// time; a heartbeat of another revision or none at all leaves it unknown.
    #[test]
    fn telemetry_needs_revision() {
        let (matching, stale, silent) = (node(1), node(2), node(3));
        let mut documents = BTreeMap::from([
            (matching, document(matching, 4)),
            (stale, document(stale, 4)),
            (silent, document(silent, 4)),
        ]);
        let fresh = BTreeMap::from([(matching, held(matching, 4)), (stale, held(stale, 3))]);

        live_telemetry(&fresh, &mut documents, 60_000);

        let live = &documents[&matching];
        assert_eq!(live.utilization.load_permille, Some(100));
        let sample = live.executors[0].availability.expect("live availability");
        assert_eq!(sample.active_executions, 2);
        assert_eq!(sample.observed_at_ms, 55_000);
        for node_id in [stale, silent] {
            let document = &documents[&node_id];
            assert_eq!(document.utilization.load_permille, None);
            assert_eq!(document.executors[0].availability, None);
        }
    }

    fn beat(sequence: u64) -> NodeHeartbeat {
        NodeHeartbeat {
            sequence,
            ..held(node(9), 1).heartbeat
        }
    }

    fn heartbeat(effect: HeartbeatEffect) -> Effect {
        Effect::Net(NetEffect::Heartbeat(Box::new(effect)))
    }

    fn answer(event: HeartbeatEvent) -> Event {
        Event::Net(NetEvent::Heartbeat(event))
    }

    fn frame(heartbeat: &NodeHeartbeat) -> Value {
        let bytes = heartbeat.to_bytes().unwrap();
        let length = (bytes.len() as u32).to_be_bytes();
        Value::from([length.as_slice(), &bytes].concat())
    }

    /// Sends start at the peer the sequence names and run as one bounded group.
    #[test]
    fn sends_rotate_peers() {
        let sent = beat(4);
        let mut operation = SendHeartbeatOperation::new(sent.clone());
        let pushes = [node(3), node(1), node(2)]
            .into_iter()
            .map(|peer| {
                heartbeat(HeartbeatEffect::Push {
                    peer,
                    frame: frame(&sent),
                })
            })
            .collect();
        let group = Effect::Group(EffectGroup {
            members: pushes,
            limit: PARALLEL_SENDS,
            form: GroupForm::All {
                deadline: Some(SEND_PASS_TIMEOUT),
            },
        });
        let finished = Event::Group(GroupEvent::Finished {
            results: vec![
                GroupResult::Done(Box::new(answer(HeartbeatEvent::Pushed))),
                GroupResult::Done(Box::new(answer(HeartbeatEvent::PushFailed(
                    "refused".to_string(),
                )))),
                GroupResult::Unfinished,
            ],
        });
        check_trace(
            &mut operation,
            &[heartbeat(HeartbeatEffect::Record(Box::new(sent.clone())))],
            vec![
                (
                    answer(HeartbeatEvent::Peers(vec![node(1), node(2), node(3)])),
                    vec![group],
                ),
                (finished, vec![]),
            ],
        );
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn nothing_to_send() {
        // No peers, or no net handle, end the run; failed sends never fail it.
        for event in [
            answer(HeartbeatEvent::Peers(Vec::new())),
            Event::Net(NetEvent::Error(aruna_core::events::NetError::ChannelClosed)),
        ] {
            let mut operation = SendHeartbeatOperation::new(beat(1));
            operation.start();
            assert!(operation.step(event).is_empty());
            assert_eq!(operation.finalize(), Ok(()));
        }
        // A heartbeat that breaks its own bounds is never sent.
        let mut invalid = beat(1);
        invalid.availability = vec![("x".repeat(10_000), availability(1))];
        let mut operation = SendHeartbeatOperation::new(invalid);
        assert!(operation.start().is_empty());
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn keeps_peer_heartbeats() {
        let realm = RealmId([1; 32]);
        let kept = beat(2);
        let mut operation =
            ReceiveHeartbeatOperation::new(node(5), realm, kept.to_bytes().unwrap());
        let store = heartbeat(HeartbeatEffect::Store {
            peer: node(5),
            heartbeat: Box::new(kept.clone()),
        });
        check_trace(
            &mut operation,
            &[store],
            vec![(answer(HeartbeatEvent::Stored(false)), vec![])],
        );
        assert_eq!(operation.finalize(), Ok(()));
        // A foreign realm or an invalid frame is dropped without a store.
        let foreign =
            ReceiveHeartbeatOperation::new(node(5), RealmId([2; 32]), kept.to_bytes().unwrap());
        let garbled = ReceiveHeartbeatOperation::new(node(5), realm, vec![0xff; 3]);
        for mut dropped in [foreign, garbled] {
            assert!(dropped.start().is_empty());
            assert!(dropped.is_complete());
            assert_eq!(dropped.finalize(), Ok(()));
        }
    }

    #[test]
    fn rejects_stray_events() {
        let recording = || {
            let mut operation = SendHeartbeatOperation::new(beat(1));
            operation.start();
            operation
        };
        let expected = |event: &Event| {
            matches!(
                event,
                Event::Net(NetEvent::Heartbeat(_) | NetEvent::Error(_))
            )
        };
        for (label, effects, error) in rejects_events(recording, sample_events, expected) {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert!(
                matches!(error, HeartbeatError::UnexpectedEvent(_)),
                "{label}"
            );
        }
        let storing = || {
            let realm = RealmId([1; 32]);
            let mut operation =
                ReceiveHeartbeatOperation::new(node(5), realm, beat(2).to_bytes().unwrap());
            operation.start();
            operation
        };
        let refused = rejects_events(storing, sample_events, |_| false);
        assert_eq!(refused.len(), sample_events().len());
    }

    #[test]
    fn abort_and_finalize() {
        let mut sending = SendHeartbeatOperation::new(beat(1));
        check_abort(&mut sending, &[]);
        sending.start();
        check_abort(&mut sending, &[]);
        rejects_finalize(sending);
        let realm = RealmId([1; 32]);
        let mut receiving =
            ReceiveHeartbeatOperation::new(node(5), realm, beat(2).to_bytes().unwrap());
        receiving.start();
        check_abort(&mut receiving, &[]);
        rejects_finalize(receiving);
    }
}
