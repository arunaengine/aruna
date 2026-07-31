use serde::{Deserialize, Serialize};
use std::time::Duration;

/// What a backend does with bytes no materialized version references any more.
/// A request, not a guarantee: on a versioned or object-locked bucket the
/// physical delete only writes a marker.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum CleanupStrategy {
    #[default]
    Retain,
    Reclaim {
        after: Duration,
    },
}

impl CleanupStrategy {
    pub const DEFAULT_RECLAIM_AFTER: Duration = Duration::from_secs(24 * 60 * 60);

    /// Operators pay for their own free space, so node backends reclaim unless
    /// told otherwise; tenant backends keep the safe default.
    pub const fn node_default() -> Self {
        Self::Reclaim {
            after: Self::DEFAULT_RECLAIM_AFTER,
        }
    }

    pub const fn grace(&self) -> Option<Duration> {
        match self {
            Self::Retain => None,
            Self::Reclaim { after } => Some(*after),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CleanupStrategy;

    #[test]
    fn defaults_stay_split() {
        assert_eq!(CleanupStrategy::default(), CleanupStrategy::Retain);
        assert_eq!(
            CleanupStrategy::node_default().grace(),
            Some(CleanupStrategy::DEFAULT_RECLAIM_AFTER)
        );
        assert_eq!(CleanupStrategy::Retain.grace(), None);
    }
}
