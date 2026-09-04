//! Realm-wide compute configuration the pure planner reads.
//!
//! It carries operator knowledge no node can measure for itself: the directed
//! bandwidth between placement locations, the bandwidth to assume for an
//! unconfigured link, and how long an availability sample stays meaningful.

use crate::compute_quota::ComputeQuota;
use crate::structs::MAX_NODE_LOCATION_LEN;
use crate::types::GroupId;
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
/// How long a witness waits for a launch to produce a receipt, and how long an
/// executor node may stay silent, before the round plans again: 5 minutes.
pub const DEFAULT_CATCH_UP_AFTER_MS: u64 = 300_000;
/// Groups one realm gives an explicit compute quota.
pub const MAX_GROUP_COMPUTE_QUOTAS: usize = 256;

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
    #[error("catch-up wait must be greater than zero")]
    ZeroCatchUpWait,
    #[error("a realm configures at most {MAX_GROUP_COMPUTE_QUOTAS} group compute quotas")]
    QuotaCount,
    #[error("group {group_id} has two compute quotas")]
    DuplicateQuota { group_id: GroupId },
}

/// One directed transfer estimate between two placement locations. Direction
/// matters: asymmetric uplinks are the normal case between sites.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct LocationLink {
    pub from: String,
    pub to: String,
    pub bandwidth_bytes_per_sec: u64,
}

/// One group's standing compute quota. An entry replaces the realm default
/// wholesale, so an explicitly unlimited group is an entry of all-`None`.
#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct GroupComputeQuota {
    pub group_id: GroupId,
    pub quota: ComputeQuota,
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
    /// Applies to every group without its own entry.
    pub default_group_quota: ComputeQuota,
    pub group_quotas: Vec<GroupComputeQuota>,
    /// Wait window before a round plans again: how long a launch may stay
    /// without a receipt, and how long an executor node may stay silent.
    pub catch_up_after_ms: u64,
}

impl Default for RealmComputeConfig {
    fn default() -> Self {
        Self {
            links: Vec::new(),
            pessimistic_bandwidth_bytes_per_sec: DEFAULT_PESSIMISTIC_BANDWIDTH,
            availability_stale_after_ms: DEFAULT_AVAILABILITY_STALE_MS,
            witness_base_delay_ms: DEFAULT_WITNESS_BASE_DELAY_MS,
            default_group_quota: ComputeQuota::default(),
            group_quotas: Vec::new(),
            catch_up_after_ms: DEFAULT_CATCH_UP_AFTER_MS,
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
        // Zero would make every launch look overdue at once.
        if self.catch_up_after_ms == 0 {
            return Err(ComputeConfigError::ZeroCatchUpWait);
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
        self.validate_quotas()
    }

    /// The standing quota of one group. Fails closed on an unbounded or
    /// ambiguous quota section instead of resolving it to the realm default.
    pub fn effective_quota(&self, group_id: &GroupId) -> Result<ComputeQuota, ComputeConfigError> {
        self.validate_quotas()?;
        Ok(self
            .group_quotas
            .iter()
            .find(|entry| &entry.group_id == group_id)
            .map(|entry| entry.quota)
            .unwrap_or(self.default_group_quota))
    }

    fn validate_quotas(&self) -> Result<(), ComputeConfigError> {
        if self.group_quotas.len() > MAX_GROUP_COMPUTE_QUOTAS {
            return Err(ComputeConfigError::QuotaCount);
        }
        let mut seen = BTreeSet::new();
        for entry in &self.group_quotas {
            if !seen.insert(entry.group_id) {
                return Err(ComputeConfigError::DuplicateQuota {
                    group_id: entry.group_id,
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
        assert_eq!(
            RealmComputeConfig {
                catch_up_after_ms: 0,
                ..Default::default()
            }
            .validate(),
            Err(ComputeConfigError::ZeroCatchUpWait)
        );
    }

    fn quota(max_jobs: u32) -> ComputeQuota {
        ComputeQuota {
            max_jobs: Some(max_jobs),
            ..Default::default()
        }
    }

    #[test]
    fn override_replaces_default() {
        // An entry replaces the realm default wholesale, including an entry
        // that makes one group explicitly unlimited.
        let group = GroupId::from_bytes([1; 16]);
        let unlimited = GroupId::from_bytes([2; 16]);
        let config = RealmComputeConfig {
            default_group_quota: quota(4),
            group_quotas: vec![
                GroupComputeQuota {
                    group_id: group,
                    quota: quota(9),
                },
                GroupComputeQuota {
                    group_id: unlimited,
                    quota: ComputeQuota::default(),
                },
            ],
            ..Default::default()
        };

        assert_eq!(config.effective_quota(&group), Ok(quota(9)));
        assert_eq!(
            config.effective_quota(&GroupId::from_bytes([3; 16])),
            Ok(quota(4))
        );
        assert_eq!(
            config.effective_quota(&unlimited),
            Ok(ComputeQuota::default())
        );
    }

    #[test]
    fn duplicate_quota_fails_closed() {
        // An ambiguous quota section must not silently resolve to one of its
        // entries or to the realm default.
        let group = GroupId::from_bytes([1; 16]);
        let config = RealmComputeConfig {
            group_quotas: vec![
                GroupComputeQuota {
                    group_id: group,
                    quota: quota(1),
                },
                GroupComputeQuota {
                    group_id: group,
                    quota: quota(99),
                },
            ],
            ..Default::default()
        };

        assert_eq!(
            config.effective_quota(&GroupId::from_bytes([7; 16])),
            Err(ComputeConfigError::DuplicateQuota { group_id: group })
        );
        assert!(config.validate().is_err());
    }
}
