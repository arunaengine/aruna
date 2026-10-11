//! Defines the heartbeat a realm node pushes to its peers with its live telemetry. Heartbeats are
//! kept in memory only; the durable advertisement stays the NodeInfo document.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::NodeId;
use crate::compute::quota::{ComputeDemandSnapshot, ComputeReservationSnapshot, SnapshotError};
use crate::compute::{ExecutorAvailability, MAX_ADVERTISED_EXECUTORS};
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::policy::MAX_KIND_LEN;
use crate::structs::storage::node_info::{AdvertisementEpoch, NodeUtilization};

/// Largest encoded heartbeat a peer accepts.
pub const MAX_HEARTBEAT_BYTES: usize = 32 * 1024;

/// One heartbeat. `epoch` names the committed advertisement it belongs to, with the send time as
/// `observed_at_ms`; `sequence` grows by one per heartbeat of one process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeHeartbeat {
    pub realm_id: RealmId,
    pub epoch: AdvertisementEpoch,
    pub sequence: u64,
    pub utilization: NodeUtilization,
    /// Availability per advertised executor kind, ordered by kind.
    pub availability: Vec<(String, ExecutorAvailability)>,
    pub reservation: ComputeReservationSnapshot,
    pub demand: ComputeDemandSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum HeartbeatError {
    #[error("a heartbeat names at most {MAX_ADVERTISED_EXECUTORS} executors")]
    ExecutorCount,
    #[error("executor kinds must be 1..={MAX_KIND_LEN} bytes, unique and ordered")]
    ExecutorKind,
    #[error("snapshot epochs must name the heartbeat's advertisement")]
    EpochMismatch,
    #[error("a heartbeat is at most {MAX_HEARTBEAT_BYTES} bytes")]
    TooLarge,
    #[error("heartbeat does not decode: {0}")]
    Decode(String),
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
}

impl NodeHeartbeat {
    /// Bounds, canonical order and snapshots bound to the heartbeat's advertisement.
    pub fn validate(&self) -> Result<(), HeartbeatError> {
        if self.availability.len() > MAX_ADVERTISED_EXECUTORS {
            return Err(HeartbeatError::ExecutorCount);
        }
        let malformed = self
            .availability
            .iter()
            .any(|(kind, _)| kind.is_empty() || kind.len() > MAX_KIND_LEN || kind.trim() != kind);
        if malformed
            || self
                .availability
                .windows(2)
                .any(|pair| pair[0].0 >= pair[1].0)
        {
            return Err(HeartbeatError::ExecutorKind);
        }
        self.demand.validate()?;
        let current = revision(&self.epoch);
        if revision(&self.demand.epoch) != current || revision(&self.reservation.epoch) != current {
            return Err(HeartbeatError::EpochMismatch);
        }
        Ok(())
    }

    /// Ordering key: the advertisement revision first, then the sequence. Wall time is never part
    /// of it, so a sender whose clock jumps still orders correctly.
    pub fn order(&self) -> (u64, u64, u64) {
        let (membership, publisher) = revision(&self.epoch);
        (membership, publisher, self.sequence)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, HeartbeatError> {
        self.validate()?;
        let bytes = postcard::to_allocvec(self)
            .map_err(|error| HeartbeatError::Decode(error.to_string()))?;
        match bytes.len() > MAX_HEARTBEAT_BYTES {
            true => Err(HeartbeatError::TooLarge),
            false => Ok(bytes),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, HeartbeatError> {
        if bytes.len() > MAX_HEARTBEAT_BYTES {
            return Err(HeartbeatError::TooLarge);
        }
        let heartbeat: Self = postcard::from_bytes(bytes)
            .map_err(|error| HeartbeatError::Decode(error.to_string()))?;
        heartbeat.validate()?;
        Ok(heartbeat)
    }
}

/// The latest heartbeat held for one node, with its age on the receiver's monotonic clock. The
/// receive time in wall milliseconds is for display only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeldHeartbeat {
    pub node_id: NodeId,
    pub heartbeat: NodeHeartbeat,
    pub age_ms: u64,
    pub received_at_ms: u64,
}

fn revision(epoch: &AdvertisementEpoch) -> (u64, u64) {
    (epoch.membership_generation, epoch.publisher_generation)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn heartbeat(sequence: u64) -> NodeHeartbeat {
        let epoch = AdvertisementEpoch {
            membership_generation: 2,
            publisher_generation: 7,
            observed_at_ms: 1_000,
        };
        NodeHeartbeat {
            realm_id: RealmId([3; 32]),
            epoch,
            sequence,
            utilization: NodeUtilization {
                storage_bytes_used: 10,
                documents_held: Some(1),
                load_permille: Some(250),
                heartbeat_at_ms: 1_000,
            },
            availability: vec![("docker".into(), availability())],
            reservation: ComputeReservationSnapshot {
                epoch,
                ..ComputeReservationSnapshot::default()
            },
            demand: ComputeDemandSnapshot {
                epoch,
                ..ComputeDemandSnapshot::default()
            },
        }
    }

    fn availability() -> ExecutorAvailability {
        ExecutorAvailability {
            free_cpu_cores: Some(4),
            free_ram_bytes: None,
            free_disk_bytes: None,
            active_executions: 0,
            observed_at_ms: 1_000,
        }
    }

    #[test]
    fn heartbeat_roundtrips() {
        let beat = heartbeat(3);
        let bytes = beat.to_bytes().expect("heartbeat encodes");
        assert_eq!(NodeHeartbeat::from_bytes(&bytes), Ok(beat));
    }

    /// A newer advertisement revision outranks any sequence of an older one.
    #[test]
    fn revision_orders_first() {
        let older = heartbeat(900);
        let mut newer = heartbeat(1);
        newer.epoch.publisher_generation += 1;
        assert!(newer.order() > older.order());
        assert!(heartbeat(4).order() > heartbeat(3).order());
    }

    #[test]
    fn rejects_unordered_kinds() {
        let mut beat = heartbeat(1);
        beat.availability = vec![
            ("kubernetes".into(), availability()),
            ("docker".into(), availability()),
        ];
        assert_eq!(beat.validate(), Err(HeartbeatError::ExecutorKind));
        beat.availability = vec![
            ("docker".into(), availability()),
            ("docker".into(), availability()),
        ];
        assert_eq!(beat.validate(), Err(HeartbeatError::ExecutorKind));
    }

    #[test]
    fn rejects_foreign_snapshot() {
        let mut beat = heartbeat(1);
        beat.demand.epoch.publisher_generation -= 1;
        assert_eq!(beat.validate(), Err(HeartbeatError::EpochMismatch));
    }

    #[test]
    fn rejects_oversized_frame() {
        assert_eq!(
            NodeHeartbeat::from_bytes(&vec![0; MAX_HEARTBEAT_BYTES + 1]),
            Err(HeartbeatError::TooLarge)
        );
    }
}
