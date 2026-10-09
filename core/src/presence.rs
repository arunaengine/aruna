//! Defines the live telemetry a realm node pushes to its peers each heartbeat. Samples are kept in
//! memory only; the durable advertisement stays the NodeInfo document.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::compute::quota::{ComputeDemandSnapshot, ComputeReservationSnapshot, SnapshotError};
use crate::compute::{ExecutorAvailability, MAX_ADVERTISED_EXECUTORS};
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::policy::MAX_KIND_LEN;
use crate::structs::storage::node_info::{AdvertisementEpoch, NodeUtilization};

/// Largest encoded sample a peer accepts.
pub const MAX_PRESENCE_BYTES: usize = 32 * 1024;

/// One heartbeat of telemetry. `epoch` names the committed advertisement the sample belongs to,
/// with the send time as `observed_at_ms`; `sequence` grows by one per sample of one process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePresence {
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
pub enum PresenceError {
    #[error("a presence sample names at most {MAX_ADVERTISED_EXECUTORS} executors")]
    ExecutorCount,
    #[error("executor kinds must be 1..={MAX_KIND_LEN} bytes, unique and ordered")]
    ExecutorKind,
    #[error("snapshot epochs must name the sample's advertisement")]
    EpochMismatch,
    #[error("a presence sample is at most {MAX_PRESENCE_BYTES} bytes")]
    TooLarge,
    #[error("presence sample does not decode: {0}")]
    Decode(String),
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
}

impl NodePresence {
    /// Bounds, canonical order and snapshots bound to the sample's advertisement.
    pub fn validate(&self) -> Result<(), PresenceError> {
        if self.availability.len() > MAX_ADVERTISED_EXECUTORS {
            return Err(PresenceError::ExecutorCount);
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
            return Err(PresenceError::ExecutorKind);
        }
        self.demand.validate()?;
        let current = revision(&self.epoch);
        if revision(&self.demand.epoch) != current || revision(&self.reservation.epoch) != current {
            return Err(PresenceError::EpochMismatch);
        }
        Ok(())
    }

    /// Ordering key: the advertisement revision first, then the sample sequence. Wall time is
    /// never part of it, so a sender whose clock jumps still orders correctly.
    pub fn order(&self) -> (u64, u64, u64) {
        let (membership, publisher) = revision(&self.epoch);
        (membership, publisher, self.sequence)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, PresenceError> {
        self.validate()?;
        let bytes = postcard::to_allocvec(self)
            .map_err(|error| PresenceError::Decode(error.to_string()))?;
        match bytes.len() > MAX_PRESENCE_BYTES {
            true => Err(PresenceError::TooLarge),
            false => Ok(bytes),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PresenceError> {
        if bytes.len() > MAX_PRESENCE_BYTES {
            return Err(PresenceError::TooLarge);
        }
        let sample: Self = postcard::from_bytes(bytes)
            .map_err(|error| PresenceError::Decode(error.to_string()))?;
        sample.validate()?;
        Ok(sample)
    }
}

fn revision(epoch: &AdvertisementEpoch) -> (u64, u64) {
    (epoch.membership_generation, epoch.publisher_generation)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(sequence: u64) -> NodePresence {
        let epoch = AdvertisementEpoch {
            membership_generation: 2,
            publisher_generation: 7,
            observed_at_ms: 1_000,
        };
        NodePresence {
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
    fn sample_roundtrips() {
        let sample = sample(3);
        let bytes = sample.to_bytes().expect("sample encodes");
        assert_eq!(NodePresence::from_bytes(&bytes), Ok(sample));
    }

    /// A newer advertisement revision outranks any sequence of an older one.
    #[test]
    fn revision_orders_first() {
        let older = sample(900);
        let mut newer = sample(1);
        newer.epoch.publisher_generation += 1;
        assert!(newer.order() > older.order());
        assert!(sample(4).order() > sample(3).order());
    }

    #[test]
    fn rejects_unordered_kinds() {
        let mut sample = sample(1);
        sample.availability = vec![
            ("kubernetes".into(), availability()),
            ("docker".into(), availability()),
        ];
        assert_eq!(sample.validate(), Err(PresenceError::ExecutorKind));
        sample.availability = vec![
            ("docker".into(), availability()),
            ("docker".into(), availability()),
        ];
        assert_eq!(sample.validate(), Err(PresenceError::ExecutorKind));
    }

    #[test]
    fn rejects_foreign_snapshot() {
        let mut sample = sample(1);
        sample.demand.epoch.publisher_generation -= 1;
        assert_eq!(sample.validate(), Err(PresenceError::EpochMismatch));
    }

    #[test]
    fn rejects_oversized_frame() {
        assert_eq!(
            NodePresence::from_bytes(&vec![0; MAX_PRESENCE_BYTES + 1]),
            Err(PresenceError::TooLarge)
        );
    }
}
