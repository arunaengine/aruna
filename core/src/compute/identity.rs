//! Keeps the durable identity of one compute backend: where its attempts live, what placement
//! and admission see of it, and a generation that advances only when that changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};

use crate::compute::backend::RuntimeDestination;
use crate::errors::ConversionError;

const HOME_DOMAIN: &[u8] = b"aruna-compute-home-v1";

/// Digest of where a backend's attempts live: its kind and runtime destination. A backend
/// that cannot name its destination hashes only its kind.
pub fn home_digest(kind: &str, destination: Option<&RuntimeDestination>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(HOME_DOMAIN);
    hasher.update(&(kind.len() as u64).to_le_bytes());
    hasher.update(kind.as_bytes());
    match destination.map(postcard::to_allocvec) {
        Some(Ok(bytes)) => {
            hasher.update(&[1]);
            hasher.update(&bytes);
        }
        _ => {
            hasher.update(&[0]);
        }
    }
    *hasher.finalize().as_bytes()
}

/// The stored identity of one backend name on this node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendConfigRecord {
    pub home_digest: [u8; 32],
    /// Covers the home digest and everything placement or admission reads of the backend.
    pub config_digest: [u8; 32],
    /// Starts at one and advances only when `config_digest` changes.
    pub config_generation: u64,
}

impl BackendConfigRecord {
    /// The first record of a backend name.
    pub fn seed(home_digest: [u8; 32], config_digest: [u8; 32]) -> Self {
        Self {
            home_digest,
            config_digest,
            config_generation: 1,
        }
    }

    /// The record for the digests observed now: `Ok(None)` while they are unchanged, so a
    /// restart with the same configuration keeps the generation.
    pub fn advance(
        &self,
        home_digest: [u8; 32],
        config_digest: [u8; 32],
    ) -> Result<Option<Self>, ConversionError> {
        if self.home_digest == home_digest && self.config_digest == config_digest {
            return Ok(None);
        }
        let config_generation = self
            .config_generation
            .checked_add(1)
            .ok_or(ConversionError::HeadGenerationExhausted)?;
        Ok(Some(Self {
            home_digest,
            config_digest,
            config_generation,
        }))
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn docker(host: &str) -> RuntimeDestination {
        RuntimeDestination::Docker {
            host: host.to_string(),
            state_root: "/var/lib/aruna".to_string(),
        }
    }

    #[test]
    fn homes_follow_destination() {
        let local = home_digest("docker", Some(&docker("unix:///var/run/docker.sock")));
        let remote = home_digest("docker", Some(&docker("tcp://10.0.0.5:2376")));

        assert_ne!(local, remote);
        assert_eq!(
            local,
            home_digest("docker", Some(&docker("unix:///var/run/docker.sock")))
        );
        assert_ne!(home_digest("docker", None), home_digest("slurm", None));
    }

    #[test]
    fn generation_follows_digests() {
        let first = BackendConfigRecord::seed([1u8; 32], [2u8; 32]);
        assert_eq!(first.config_generation, 1);

        // A restart with the same configuration keeps the generation.
        assert_eq!(first.advance([1u8; 32], [2u8; 32]), Ok(None));
        let changed = first.advance([1u8; 32], [3u8; 32]).unwrap().unwrap();
        assert_eq!(changed.config_generation, 2);
        assert_eq!(changed.config_digest, [3u8; 32]);
        let moved = changed.advance([4u8; 32], [3u8; 32]).unwrap().unwrap();
        assert_eq!(moved.config_generation, 3);

        let bytes = moved.to_bytes().unwrap();
        assert_eq!(BackendConfigRecord::from_bytes(&bytes).unwrap(), moved);
    }
}
