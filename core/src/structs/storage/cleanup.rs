use crate::errors::ConversionError;
use crate::structs::BackendRef;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

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

/// Separates the backend from the hash in a candidate key. No backend name can
/// contain it, so one backend's candidates are an unambiguous scan range.
const KEY_SEPARATOR: u8 = 0;

/// Reclaim queue key. Backend first, unlike the hash-first location key, so the
/// queue can be counted and drained per backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReclaimCandidateKey {
    pub backend: BackendRef,
    pub blake3: [u8; 32],
}

impl ReclaimCandidateKey {
    pub fn new(backend: BackendRef, blake3: [u8; 32]) -> Self {
        Self { backend, blake3 }
    }

    pub fn prefix(backend: &BackendRef) -> Vec<u8> {
        let mut prefix = backend.key_bytes();
        prefix.push(KEY_SEPARATOR);
        prefix
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut key = Self::prefix(&self.backend);
        key.extend_from_slice(&self.blake3);
        key
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let split = bytes.len().checked_sub(33).ok_or_else(|| {
            ConversionError::InvalidLength("reclaim candidate key is too short".to_string())
        })?;
        let (backend, tail) = bytes.split_at(split);
        let blake3: [u8; 32] = tail[1..].try_into()?;
        Ok(Self::new(BackendRef::from_key_bytes(backend)?, blake3))
    }
}

/// Queue entry for a copy that lost a reference. The strategy and its grace are
/// resolved at sweep time, never frozen into the row.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReclaimCandidate {
    pub enqueued_at: SystemTime,
}

impl ReclaimCandidate {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::{CleanupStrategy, ReclaimCandidate, ReclaimCandidateKey};
    use crate::structs::BackendRef;
    use std::time::SystemTime;

    #[test]
    fn key_round_trips() {
        // A name that prefixes another must not fall inside its scan range.
        let key = ReclaimCandidateKey::new(BackendRef::Node("cold".to_string()), [4u8; 32]);
        let bytes = key.to_bytes();

        assert_eq!(ReclaimCandidateKey::from_bytes(&bytes).unwrap(), key);
        assert!(bytes.starts_with(&ReclaimCandidateKey::prefix(&key.backend)));
        assert!(
            !bytes.starts_with(&ReclaimCandidateKey::prefix(&BackendRef::Node(
                "col".to_string()
            )))
        );
        assert!(ReclaimCandidateKey::from_bytes(&bytes[..20]).is_err());
    }

    #[test]
    fn candidate_round_trips() {
        let candidate = ReclaimCandidate {
            enqueued_at: SystemTime::UNIX_EPOCH,
        };
        let bytes = candidate.to_bytes().unwrap();

        assert_eq!(ReclaimCandidate::from_bytes(&bytes).unwrap(), candidate);
    }

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
