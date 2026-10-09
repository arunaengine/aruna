//! Pushes and receives heartbeats with live telemetry between sync peers over the heartbeat
//! protocol. One stream carries one length-prefixed heartbeat and no response; nothing is written
//! to disk.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::compute::ExecutorAvailability;
use aruna_core::heartbeat::{HeldHeartbeat, MAX_HEARTBEAT_BYTES, NodeHeartbeat};
use aruna_core::structs::storage::node_info::NodeInfoDocument;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use futures_util::{StreamExt, stream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::driver::DriverContext;
use crate::node::node_info::INFO_PUBLISH_INTERVAL;

/// Deadline for reading or sending one heartbeat.
pub(crate) const HEARTBEAT_IO_TIMEOUT: Duration = Duration::from_secs(5);
/// Heartbeat sends in flight at once.
const PARALLEL_SENDS: usize = 16;
/// Oldest heartbeat that still counts as current telemetry: three missed intervals.
pub const MAX_HEARTBEAT_AGE: Duration = Duration::from_secs(3 * INFO_PUBLISH_INTERVAL.as_secs());

/// Heartbeats held for each node, fresh or stale; none without a network.
pub fn held_heartbeats(net_handle: Option<&NetHandle>) -> BTreeMap<NodeId, HeldHeartbeat> {
    net_handle
        .map(NetHandle::held_heartbeats)
        .unwrap_or_default()
        .into_iter()
        .map(|held| (held.node_id, held))
        .collect()
}

/// Fresh heartbeats held for each node; none without a network.
pub fn fresh_heartbeats(net_handle: Option<&NetHandle>) -> BTreeMap<NodeId, HeldHeartbeat> {
    let max_age_ms = MAX_HEARTBEAT_AGE.as_millis() as u64;
    let mut held = held_heartbeats(net_handle);
    held.retain(|_, held| held.age_ms <= max_age_ms);
    held
}

/// Takes the load and executor availability of each document from the node's fresh heartbeat
/// when that names the document's advertisement, and clears them otherwise: unknown ranks worst.
pub fn live_telemetry(
    fresh: &BTreeMap<NodeId, HeldHeartbeat>,
    documents: &mut BTreeMap<NodeId, NodeInfoDocument>,
    now_ms: u64,
) {
    for (node_id, document) in documents.iter_mut() {
        let held = fresh.get(node_id).filter(|held| {
            let epoch = &held.heartbeat.epoch;
            (epoch.membership_generation, epoch.publisher_generation)
                == (
                    document.epoch.membership_generation,
                    document.epoch.publisher_generation,
                )
        });
        let observed_at_ms = held.map_or(0, |held| now_ms.saturating_sub(held.age_ms));
        document.utilization.load_permille =
            held.and_then(|held| held.heartbeat.utilization.load_permille);
        for executor in &mut document.executors {
            executor.availability = held
                .and_then(|held| {
                    held.heartbeat
                        .availability
                        .iter()
                        .find(|(kind, _)| *kind == executor.kind)
                })
                .map(|(_, availability)| ExecutorAvailability {
                    observed_at_ms,
                    ..*availability
                });
        }
    }
}

/// Keeps this node's own `heartbeat` and pushes it to every sync peer. Sends are best effort: a
/// failed one only logs, and the next tick sends again.
pub async fn send_heartbeat(net_handle: &NetHandle, heartbeat: NodeHeartbeat) {
    let bytes = match heartbeat.to_bytes() {
        Ok(bytes) => bytes,
        Err(error) => {
            warn!(%error, "Not sending an invalid heartbeat");
            return;
        }
    };
    net_handle.record_own_heartbeat(heartbeat);
    let frame = [(bytes.len() as u32).to_be_bytes().as_slice(), &bytes].concat();
    stream::iter(net_handle.realm_peers().await)
        .map(|peer| {
            let frame = &frame;
            async move {
                let sent = timeout(HEARTBEAT_IO_TIMEOUT, push_frame(net_handle, peer, frame)).await;
                (peer, sent)
            }
        })
        .buffer_unordered(PARALLEL_SENDS)
        .for_each(|(peer, sent)| async move {
            match sent {
                Ok(Ok(())) => {}
                Ok(Err(error)) => debug!(%peer, %error, "Heartbeat send failed"),
                Err(_) => debug!(%peer, "Heartbeat send timed out"),
            }
        })
        .await;
}

async fn push_frame(net_handle: &NetHandle, peer: NodeId, frame: &[u8]) -> Result<(), String> {
    let mut stream = net_handle
        .open_stream(peer, Alpn::Heartbeat)
        .await
        .map_err(|error| error.to_string())?;
    stream
        .0
        .write_all(frame)
        .await
        .map_err(|error| error.to_string())?;
    stream.0.finish().map_err(|error| error.to_string())
}

/// Reads one heartbeat of `peer` and stores it when the sender is a sync peer of this realm and
/// the heartbeat is newer than the held one.
pub async fn handle_heartbeat_stream(context: &DriverContext, mut stream: BiStream, peer: NodeId) {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return;
    };
    let Some(_slot) = net_handle.heartbeat_slot(peer) else {
        debug!(%peer, "Dropping a heartbeat stream past the stream limits");
        return;
    };
    let read = timeout(HEARTBEAT_IO_TIMEOUT, read_heartbeat(&mut stream)).await;
    let _ = stream.0.finish();
    let heartbeat = match read {
        Ok(Ok(heartbeat)) => heartbeat,
        Ok(Err(error)) => {
            debug!(%peer, %error, "Dropping an invalid heartbeat");
            return;
        }
        Err(_) => {
            debug!(%peer, "Heartbeat read timed out");
            return;
        }
    };
    if heartbeat.realm_id != *net_handle.realm_id() {
        debug!(%peer, "Dropping a heartbeat of another realm");
        return;
    }
    if !net_handle.record_heartbeat(peer, heartbeat) {
        debug!(%peer, "Heartbeat not stored: not newer, too frequent or not a sync peer");
    }
}

async fn read_heartbeat(stream: &mut BiStream) -> Result<NodeHeartbeat, String> {
    let mut length = [0u8; 4];
    stream
        .1
        .read_exact(&mut length)
        .await
        .map_err(|error| error.to_string())?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_HEARTBEAT_BYTES {
        return Err("heartbeat frame exceeds the size limit".to_string());
    }
    let mut bytes = vec![0u8; length];
    stream
        .1
        .read_exact(&mut bytes)
        .await
        .map_err(|error| error.to_string())?;
    NodeHeartbeat::from_bytes(&bytes).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use aruna_core::compute::ExecutorCapability;
    use aruna_core::compute::quota::{ComputeDemandSnapshot, ComputeReservationSnapshot};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::policy::PlacementSubject;
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
}
