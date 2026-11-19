//! Pushes and keeps heartbeats with live telemetry between sync peers.
//! One stream carries one length-prefixed heartbeat and no response; nothing is written to disk.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm, NetEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult, NetEvent};
use aruna_core::heartbeat::{
    HeartbeatEffect, HeartbeatEvent, INFO_PUBLISH_INTERVAL, NodeHeartbeat,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::Value;
use thiserror::Error;
use tracing::{debug, warn};

/// Heartbeat sends in flight at once.
const PARALLEL_SENDS: usize = 16;
/// Longest one send pass over all peers may take, well inside the heartbeat interval.
const SEND_PASS_TIMEOUT: Duration = Duration::from_secs(INFO_PUBLISH_INTERVAL.as_secs() / 2);

/// Why a heartbeat run stopped; a failed send or an invalid heartbeat is only logged.
#[derive(Debug, Error, PartialEq)]
pub enum HeartbeatError {
    #[error(transparent)]
    Io(#[from] IoError),
}

type Turn = Result<Next<()>, HeartbeatError>;

fn heartbeat_call(effect: HeartbeatEffect) -> Turn {
    let effect = Effect::Net(NetEffect::Heartbeat(Box::new(effect)));
    Ok(Next::Call(IoCall::Effect(Box::new(effect))))
}

fn unexpected(got: &'static str) -> HeartbeatError {
    let expected = "a heartbeat event";
    IoError::Unexpected { expected, got }.into()
}

/// Keeps this node's own heartbeat and pushes it to every sync peer, at most
/// [`PARALLEL_SENDS`] at once and for at most [`SEND_PASS_TIMEOUT`]. Sends are best effort: a
/// failed one only logs, and the next tick sends again.
#[derive(Debug, PartialEq)]
pub(crate) struct SendHeartbeatOperation {
    heartbeat: Option<NodeHeartbeat>,
    sequence: u64,
    frame: Value,
}

impl SendHeartbeatOperation {
    pub(crate) fn new(heartbeat: NodeHeartbeat) -> Self {
        Self {
            sequence: heartbeat.sequence,
            heartbeat: Some(heartbeat),
            frame: Value::from(Vec::new()),
        }
    }

    /// One push per peer; each pass starts at another peer, so peers that time out cannot
    /// starve the ones after them.
    fn push(&self, mut peers: Vec<NodeId>) -> Turn {
        if peers.is_empty() {
            return Ok(Next::Done(()));
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
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members,
            limit: PARALLEL_SENDS,
            form: GroupForm::All {
                deadline: Some(SEND_PASS_TIMEOUT),
            },
        })))
    }
}

impl Operation for SendHeartbeatOperation {
    type Output = ();
    type Error = HeartbeatError;

    fn first(&mut self) -> Turn {
        let Some(heartbeat) = self.heartbeat.take() else {
            return Ok(Next::Done(()));
        };
        let bytes = match heartbeat.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => {
                warn!(%error, "Not sending an invalid heartbeat");
                return Ok(Next::Done(()));
            }
        };
        let length = (bytes.len() as u32).to_be_bytes();
        self.frame = Value::from([length.as_slice(), &bytes].concat());
        heartbeat_call(HeartbeatEffect::Record(Box::new(heartbeat)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match result {
            IoResult::Event(event) => match *event {
                Event::Net(NetEvent::Heartbeat(HeartbeatEvent::Peers(peers))) => self.push(peers),
                // Without a net handle there is nobody to send to.
                Event::Net(NetEvent::Error(_)) => Ok(Next::Done(())),
                event => Err(unexpected(event.label())),
            },
            IoResult::Group(GroupEvent::Finished { results }) => {
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
                Ok(Next::Done(()))
            }
            _ => Err(unexpected("another result")),
        }
    }
}

/// Keeps one heartbeat frame `peer` sent when it decodes, names this node's realm, and is newer
/// than the held one of a sync peer. Every refusal is only logged.
#[derive(Debug, PartialEq)]
pub struct ReceiveHeartbeatOperation {
    peer: NodeId,
    realm_id: RealmId,
    frame: Vec<u8>,
}

impl ReceiveHeartbeatOperation {
    pub fn new(peer: NodeId, realm_id: RealmId, frame: Vec<u8>) -> Self {
        Self {
            peer,
            realm_id,
            frame,
        }
    }
}

impl Operation for ReceiveHeartbeatOperation {
    type Output = ();
    type Error = HeartbeatError;

    fn first(&mut self) -> Turn {
        let peer = self.peer;
        let heartbeat = match NodeHeartbeat::from_bytes(&self.frame) {
            Ok(heartbeat) => heartbeat,
            Err(error) => {
                debug!(%peer, %error, "Dropping an invalid heartbeat");
                return Ok(Next::Done(()));
            }
        };
        if heartbeat.realm_id != self.realm_id {
            debug!(%peer, "Dropping a heartbeat of another realm");
            return Ok(Next::Done(()));
        }
        let heartbeat = Box::new(heartbeat);
        heartbeat_call(HeartbeatEffect::Store { peer, heartbeat })
    }

    fn next(&mut self, result: IoResult) -> Turn {
        let peer = self.peer;
        let IoResult::Event(event) = result else {
            return Err(unexpected("another result"));
        };
        match *event {
            Event::Net(NetEvent::Heartbeat(HeartbeatEvent::Stored(stored))) => {
                if !stored {
                    debug!(%peer, "Heartbeat not stored: not newer, too frequent or not a sync peer");
                }
                Ok(Next::Done(()))
            }
            event => Err(unexpected(event.label())),
        }
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

    fn answer(event: HeartbeatEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Net(NetEvent::Heartbeat(event))))
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
        assert_eq!(
            operation.first(),
            heartbeat_call(HeartbeatEffect::Record(Box::new(sent.clone())))
        );
        // Sequence 4 over three peers starts at the second one.
        let pushes = [node(2), node(3), node(1)]
            .into_iter()
            .map(|peer| {
                let push = HeartbeatEffect::Push {
                    peer,
                    frame: frame(&sent),
                };
                Effect::Net(NetEffect::Heartbeat(Box::new(push)))
            })
            .collect();
        let group = IoCall::Group(EffectGroup {
            members: pushes,
            limit: PARALLEL_SENDS,
            form: GroupForm::All {
                deadline: Some(SEND_PASS_TIMEOUT),
            },
        });
        let peers = answer(HeartbeatEvent::Peers(vec![node(1), node(2), node(3)]));
        assert_eq!(operation.next(peers), Ok(Next::Call(group)));
        let failed = HeartbeatEvent::PushFailed("refused".to_string());
        let pushed = Event::Net(NetEvent::Heartbeat(HeartbeatEvent::Pushed));
        let finished = IoResult::Group(GroupEvent::Finished {
            results: vec![
                GroupResult::Done(Box::new(pushed)),
                GroupResult::Done(Box::new(Event::Net(NetEvent::Heartbeat(failed)))),
                GroupResult::Unfinished,
            ],
        });
        assert_eq!(operation.next(finished), Ok(Next::Done(())));
    }

    #[test]
    fn nothing_to_send() {
        // No peers, or no net handle, end the run; failed sends never fail it.
        let closed = Event::Net(NetEvent::Error(aruna_core::events::NetError::ChannelClosed));
        for result in [
            answer(HeartbeatEvent::Peers(Vec::new())),
            IoResult::Event(Box::new(closed)),
        ] {
            let mut operation = SendHeartbeatOperation::new(beat(1));
            operation.first().unwrap();
            assert_eq!(operation.next(result), Ok(Next::Done(())));
        }
        // A heartbeat that breaks its own bounds is never sent.
        let mut invalid = beat(1);
        invalid.availability = vec![("x".repeat(10_000), availability(1))];
        let mut operation = SendHeartbeatOperation::new(invalid);
        assert_eq!(operation.first(), Ok(Next::Done(())));
    }

    #[test]
    fn keeps_peer_heartbeats() {
        let realm = RealmId([1; 32]);
        let kept = beat(2);
        let mut operation =
            ReceiveHeartbeatOperation::new(node(5), realm, kept.to_bytes().unwrap());
        let store = heartbeat_call(HeartbeatEffect::Store {
            peer: node(5),
            heartbeat: Box::new(kept.clone()),
        });
        assert_eq!(operation.first(), store);
        let answer = operation.next(answer(HeartbeatEvent::Stored(false)));
        assert_eq!(answer, Ok(Next::Done(())));
        // A foreign realm or an invalid frame is dropped without a store.
        let foreign =
            ReceiveHeartbeatOperation::new(node(5), RealmId([2; 32]), kept.to_bytes().unwrap());
        let garbled = ReceiveHeartbeatOperation::new(node(5), realm, vec![0xff; 3]);
        for mut dropped in [foreign, garbled] {
            assert_eq!(dropped.first(), Ok(Next::Done(())));
        }
    }

    #[test]
    fn rejects_stray_results() {
        let mut sending = SendHeartbeatOperation::new(beat(1));
        sending.first().unwrap();
        let stored = sending.next(answer(HeartbeatEvent::Stored(true)));
        assert!(matches!(stored, Err(HeartbeatError::Io(_))));
        let realm = RealmId([1; 32]);
        let mut receiving =
            ReceiveHeartbeatOperation::new(node(5), realm, beat(2).to_bytes().unwrap());
        receiving.first().unwrap();
        let pushed = receiving.next(answer(HeartbeatEvent::Pushed));
        assert!(matches!(pushed, Err(HeartbeatError::Io(_))));
        assert!(matches!(
            receiving.next(IoResult::Done),
            Err(HeartbeatError::Io(_))
        ));
    }
}
