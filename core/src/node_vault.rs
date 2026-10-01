//! Seals node vault records: secrets one node opens by itself, bound to their purpose and id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::compute::SecretBytes;
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use std::fmt;
use thiserror::Error;
use ulid::Ulid;
use zeroize::Zeroize;

const KEY_CONTEXT: &str = "aruna node vault v1";
const NONCE_LEN: usize = 12;

/// What a vault record is for. Its tag is part of the record key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VaultPurpose {
    SourceConnector,
    GroupBackend,
}

impl VaultPurpose {
    const fn tag(self) -> u8 {
        match self {
            Self::SourceConnector => 1,
            Self::GroupBackend => 2,
        }
    }
}

/// Address of one node vault record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VaultEntry {
    pub purpose: VaultPurpose,
    pub id: Ulid,
}

impl VaultEntry {
    pub fn new(purpose: VaultPurpose, id: Ulid) -> Self {
        Self { purpose, id }
    }

    /// Row key in the node vault keyspace and associated data of the sealed value.
    pub fn key(&self) -> Vec<u8> {
        [&[self.purpose.tag()][..], &self.id.to_bytes()].concat()
    }
}

/// AES-256-GCM key of the node vault, derived from the node secret with its own label.
/// Only this node opens its records, and a restart derives the same key again.
pub struct NodeVaultKey([u8; 32]);

impl NodeVaultKey {
    pub fn derive(node_secret: &[u8; 32]) -> Self {
        Self(blake3::derive_key(KEY_CONTEXT, node_secret))
    }

    /// Key with no node secret behind it, for stores that never restart.
    pub fn random() -> Self {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).expect("operating system random number generator failed");
        Self(bytes)
    }

    fn cipher(&self) -> Aes256Gcm {
        Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.0))
    }

    /// Seals `secret` for `entry` as `nonce || ciphertext`.
    pub fn seal(&self, entry: VaultEntry, secret: &SecretBytes) -> Result<Vec<u8>, VaultError> {
        let mut nonce = [0u8; NONCE_LEN];
        getrandom::fill(&mut nonce).map_err(|_| VaultError::Seal)?;
        let payload = Payload {
            msg: secret.expose(),
            aad: &entry.key(),
        };
        let ciphertext = self
            .cipher()
            .encrypt(Nonce::from_slice(&nonce), payload)
            .map_err(|_| VaultError::Seal)?;
        Ok([nonce.as_slice(), &ciphertext].concat())
    }

    /// Opens a record sealed for `entry`; another key, entry or a changed byte fails.
    pub fn open(&self, entry: VaultEntry, sealed: &[u8]) -> Result<SecretBytes, VaultError> {
        let (nonce, ciphertext) = sealed
            .split_first_chunk::<NONCE_LEN>()
            .ok_or(VaultError::Open)?;
        let payload = Payload {
            msg: ciphertext,
            aad: &entry.key(),
        };
        self.cipher()
            .decrypt(Nonce::from_slice(nonce), payload)
            .map(SecretBytes::new)
            .map_err(|_| VaultError::Open)
    }
}

impl fmt::Debug for NodeVaultKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NodeVaultKey(***)")
    }
}

impl Drop for NodeVaultKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum VaultError {
    #[error("node vault record could not be sealed")]
    Seal,
    #[error("node vault record could not be opened")]
    Open,
}

#[cfg(test)]
mod tests {
    use super::*;

    const CANARY: &[u8] = b"canary-51f8";

    #[test]
    fn opens_only_bound() {
        let key = NodeVaultKey::derive(&[7u8; 32]);
        let entry = VaultEntry::new(VaultPurpose::SourceConnector, Ulid::from_bytes([1u8; 16]));
        let sealed = key.seal(entry, &SecretBytes::new(CANARY.to_vec())).unwrap();
        assert!(!sealed.windows(CANARY.len()).any(|window| window == CANARY));

        // A restart derives the same key from the same node secret.
        let restarted = NodeVaultKey::derive(&[7u8; 32]);
        assert_eq!(restarted.open(entry, &sealed).unwrap().expose(), CANARY);

        let other_id = VaultEntry::new(entry.purpose, Ulid::from_bytes([2u8; 16]));
        let other_purpose = VaultEntry::new(VaultPurpose::GroupBackend, entry.id);
        assert_eq!(key.open(other_id, &sealed), Err(VaultError::Open));
        assert_eq!(key.open(other_purpose, &sealed), Err(VaultError::Open));
        let other_node = NodeVaultKey::derive(&[8u8; 32]);
        assert_eq!(other_node.open(entry, &sealed), Err(VaultError::Open));
        // The S3 credential key comes from the same node secret but never opens a vault record.
        let credential = blake3::derive_key("aruna s3 credential seal v1", &[7u8; 32]);
        assert_eq!(
            NodeVaultKey(credential).open(entry, &sealed),
            Err(VaultError::Open)
        );
        assert_eq!(key.open(entry, &sealed[..20]), Err(VaultError::Open));
        assert_eq!(format!("{key:?}"), "NodeVaultKey(***)");
    }
}
