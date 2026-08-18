//! Realm-wide compute configuration the pure planner reads.
//!
//! It carries operator knowledge no node can measure for itself: the directed
//! bandwidth between placement locations, the bandwidth to assume for an
//! unconfigured link, and how long an availability sample stays meaningful.

use crate::structs::MAX_NODE_LOCATION_LEN;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

/// Maximum directed links one realm configures. Quadratic in locations, so this
/// covers a realm with roughly sixteen distinct locations.
pub const MAX_LOCATION_LINKS: usize = 256;
/// Bandwidth assumed for an unconfigured directed link: 100 Mbit/s.
pub const DEFAULT_PESSIMISTIC_BANDWIDTH: u64 = 12_500_000;
/// Age above which an availability sample only counts as unknown.
pub const DEFAULT_AVAILABILITY_STALE_MS: u64 = 300_000;
/// Delay one witness rank waits before it plans a submission itself. With
/// replication factor `RF`, `witness_base_delay_ms * (RF - 1)` is the worst-case
/// wait before any witness launches while higher ranks are down, so an operator
/// tunes the leaderless failover latency here instead of in a hidden constant.
pub const DEFAULT_WITNESS_BASE_DELAY_MS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ComputeConfigError {
    #[error("a realm configures at most {MAX_LOCATION_LINKS} location links")]
    LinkCount,
    #[error("link endpoints must be 1..={MAX_NODE_LOCATION_LEN} bytes")]
    InvalidLocation,
    #[error("link bandwidth must be greater than zero")]
    ZeroBandwidth,
    #[error("link {from} -> {to} is configured twice")]
    DuplicateLink { from: String, to: String },
    #[error("witness base delay must be greater than zero")]
    ZeroWitnessDelay,
}

/// One directed transfer estimate between two placement locations. Direction
/// matters: asymmetric uplinks are the normal case between sites.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct LocationLink {
    pub from: String,
    pub to: String,
    pub bandwidth_bytes_per_sec: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct RealmComputeConfig {
    pub links: Vec<LocationLink>,
    /// Assumed for a link the realm never configured. It keeps an unknown route
    /// expensive and size-sensitive instead of impossible.
    pub pessimistic_bandwidth_bytes_per_sec: u64,
    pub availability_stale_after_ms: u64,
    /// Per-rank fallback delay of the leaderless witness schedule.
    pub witness_base_delay_ms: u64,
}

impl Default for RealmComputeConfig {
    fn default() -> Self {
        Self {
            links: Vec::new(),
            pessimistic_bandwidth_bytes_per_sec: DEFAULT_PESSIMISTIC_BANDWIDTH,
            availability_stale_after_ms: DEFAULT_AVAILABILITY_STALE_MS,
            witness_base_delay_ms: DEFAULT_WITNESS_BASE_DELAY_MS,
        }
    }
}

impl RealmComputeConfig {
    /// Bounds every configured link. A zero bandwidth would make one transfer
    /// estimate infinite, so it is rejected instead of clamped.
    pub fn validate(&self) -> Result<(), ComputeConfigError> {
        if self.links.len() > MAX_LOCATION_LINKS {
            return Err(ComputeConfigError::LinkCount);
        }
        if self.pessimistic_bandwidth_bytes_per_sec == 0 {
            return Err(ComputeConfigError::ZeroBandwidth);
        }
        // Zero would let every rank plan at once, which is the launch storm the
        // ranked fallback exists to avoid.
        if self.witness_base_delay_ms == 0 {
            return Err(ComputeConfigError::ZeroWitnessDelay);
        }
        let mut seen = BTreeSet::new();
        for link in &self.links {
            let (from, to) = (link.from.trim(), link.to.trim());
            if from.is_empty()
                || to.is_empty()
                || from.len() > MAX_NODE_LOCATION_LEN
                || to.len() > MAX_NODE_LOCATION_LEN
            {
                return Err(ComputeConfigError::InvalidLocation);
            }
            if link.bandwidth_bytes_per_sec == 0 {
                return Err(ComputeConfigError::ZeroBandwidth);
            }
            if !seen.insert((from, to)) {
                return Err(ComputeConfigError::DuplicateLink {
                    from: link.from.clone(),
                    to: link.to.clone(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(from: &str, to: &str, bandwidth: u64) -> LocationLink {
        LocationLink {
            from: from.to_string(),
            to: to.to_string(),
            bandwidth_bytes_per_sec: bandwidth,
        }
    }

    #[test]
    fn accepts_directed_links() {
        // Both directions of one pair are separate links, not a duplicate.
        let config = RealmComputeConfig {
            links: vec![link("eu", "us", 1_000), link("us", "eu", 2_000)],
            ..Default::default()
        };
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn rejects_invalid_links() {
        for links in [
            vec![link("eu", "us", 0)],
            vec![link("", "us", 10)],
            vec![link("eu", &"x".repeat(MAX_NODE_LOCATION_LEN + 1), 10)],
            vec![link("eu", "us", 10), link(" eu ", "us", 20)],
        ] {
            let config = RealmComputeConfig {
                links,
                ..Default::default()
            };
            assert!(config.validate().is_err(), "{config:?}");
        }
        assert!(
            RealmComputeConfig {
                pessimistic_bandwidth_bytes_per_sec: 0,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert_eq!(
            RealmComputeConfig {
                witness_base_delay_ms: 0,
                ..Default::default()
            }
            .validate(),
            Err(ComputeConfigError::ZeroWitnessDelay)
        );
    }
}
