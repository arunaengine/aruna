//! Seals realm vault records: secrets realm admins set once for every node of the realm.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::UserId;
use crate::compute::SecretBytes;
use crate::structs::identity::realm::{RealmId, RealmNodeKind};
use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{Aead, KeyInit, Nonce, Payload};
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use std::fmt;
use subtle::ConstantTimeEq;
use thiserror::Error;
use ulid::Ulid;
use zeroize::Zeroize;

const KEY_CONTEXT: &str = "aruna realm vault v1";
const NONCE_LEN: usize = 12;

/// What a realm vault record is for. Its tag is part of the record key; new purposes append tags.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub enum RealmVaultPurpose {
    /// The password of the realm email profile.
    SmtpPassword,
}

impl RealmVaultPurpose {
    pub const fn tag(self) -> u8 {
        match self {
            Self::SmtpPassword => 1,
        }
    }

    /// Which node kinds may fetch records of this purpose from a management node.
    pub const fn fetch_scope(self) -> FetchScope {
        match self {
            Self::SmtpPassword => FetchScope::Infrastructure,
        }
    }
}

impl TryFrom<u8> for RealmVaultPurpose {
    type Error = RealmVaultError;

    fn try_from(tag: u8) -> Result<Self, Self::Error> {
        match tag {
            1 => Ok(Self::SmtpPassword),
            _ => Err(RealmVaultError::UnknownPurpose(tag)),
        }
    }
}

impl From<RealmVaultPurpose> for u8 {
    fn from(purpose: RealmVaultPurpose) -> Self {
        purpose.tag()
    }
}

/// The node kinds that may fetch a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchScope {
    /// Server and management nodes, never user devices.
    Infrastructure,
}

impl FetchScope {
    pub fn allows(self, kind: &RealmNodeKind) -> bool {
        match self {
            Self::Infrastructure => {
                matches!(kind, RealmNodeKind::Server | RealmNodeKind::Management)
            }
        }
    }
}

/// Address of one realm vault record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RealmVaultEntry {
    pub purpose: RealmVaultPurpose,
    pub id: Ulid,
}

impl RealmVaultEntry {
    pub fn new(purpose: RealmVaultPurpose, id: Ulid) -> Self {
        Self { purpose, id }
    }

    /// Path suffix in the realm config and part of the associated data of the sealed value.
    pub fn key(&self) -> Vec<u8> {
        [&[self.purpose.tag()][..], &self.id.to_bytes()].concat()
    }
}

/// One sealed secret in the replicated realm config. Each write gets a new revision.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealmVaultRecord {
    pub entry: RealmVaultEntry,
    pub revision: Ulid,
    pub sealed: Vec<u8>,
    pub changed_by: UserId,
    pub changed_at_ms: u64,
}

impl fmt::Debug for RealmVaultRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RealmVaultRecord")
            .field("entry", &self.entry)
            .field("revision", &self.revision)
            .field("sealed_len", &self.sealed.len())
            .field("changed_by", &self.changed_by)
            .field("changed_at_ms", &self.changed_at_ms)
            .finish()
    }
}

/// How other realm state points at one revision of a realm vault record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealmSecretRef {
    pub entry: RealmVaultEntry,
    pub revision: Ulid,
}

/// AES-256-GCM key of the realm vault, derived from the realm signing key with its own label.
/// Every holder of the realm signing key derives it; other nodes never hold it.
#[derive(Clone)]
pub struct RealmVaultKey([u8; 32]);

impl RealmVaultKey {
    pub fn derive(realm_key: &SigningKey) -> Self {
        Self(blake3::derive_key(KEY_CONTEXT, &realm_key.to_bytes()))
    }

    fn cipher(&self) -> Aes256Gcm {
        Aes256Gcm::new((&self.0).into())
    }

    /// Seals `secret` for one revision of `entry` as `nonce || ciphertext`.
    pub fn seal(
        &self,
        realm_id: &RealmId,
        entry: RealmVaultEntry,
        revision: Ulid,
        secret: &SecretBytes,
    ) -> Result<Vec<u8>, RealmVaultError> {
        let mut nonce = [0u8; NONCE_LEN];
        getrandom::fill(&mut nonce).map_err(|_| RealmVaultError::Seal)?;
        let payload = Payload {
            msg: secret.expose(),
            aad: &associated(realm_id, entry, revision),
        };
        let ciphertext = self
            .cipher()
            .encrypt(&Nonce::<Aes256Gcm>::from(nonce), payload)
            .map_err(|_| RealmVaultError::Seal)?;
        Ok([nonce.as_slice(), &ciphertext].concat())
    }

    /// Opens a record; another realm, entry, revision, key or a changed byte fails.
    pub fn open(
        &self,
        realm_id: &RealmId,
        record: &RealmVaultRecord,
    ) -> Result<SecretBytes, RealmVaultError> {
        let (nonce, ciphertext) = record
            .sealed
            .split_first_chunk::<NONCE_LEN>()
            .ok_or(RealmVaultError::Open)?;
        let payload = Payload {
            msg: ciphertext,
            aad: &associated(realm_id, record.entry, record.revision),
        };
        self.cipher()
            .decrypt(&Nonce::<Aes256Gcm>::from(*nonce), payload)
            .map(SecretBytes::new)
            .map_err(|_| RealmVaultError::Open)
    }
}

fn associated(realm_id: &RealmId, entry: RealmVaultEntry, revision: Ulid) -> Vec<u8> {
    [
        realm_id.as_bytes().as_slice(),
        &entry.key(),
        &revision.to_bytes(),
    ]
    .concat()
}

impl fmt::Debug for RealmVaultKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RealmVaultKey(***)")
    }
}

impl PartialEq for RealmVaultKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.ct_eq(&other.0).into()
    }
}

impl Eq for RealmVaultKey {}

impl Drop for RealmVaultKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RealmVaultError {
    #[error("realm vault record could not be sealed")]
    Seal,
    #[error("realm vault record could not be opened")]
    Open,
    #[error("unknown realm vault purpose tag {0}")]
    UnknownPurpose(u8),
}

#[cfg(test)]
mod tests {
    use super::*;

    const CANARY: &[u8] = b"canary-7c21";

    fn record(key: &RealmVaultKey, realm_id: &RealmId, entry: RealmVaultEntry) -> RealmVaultRecord {
        let revision = Ulid::from_bytes([3u8; 16]);
        let secret = SecretBytes::new(CANARY.to_vec());
        RealmVaultRecord {
            entry,
            revision,
            sealed: key.seal(realm_id, entry, revision, &secret).unwrap(),
            changed_by: UserId::local(Ulid::from_bytes([4u8; 16]), *realm_id),
            changed_at_ms: 1,
        }
    }

    #[test]
    fn opens_only_bound() {
        let realm_key = SigningKey::from_bytes(&[7u8; 32]);
        let key = RealmVaultKey::derive(&realm_key);
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let entry =
            RealmVaultEntry::new(RealmVaultPurpose::SmtpPassword, Ulid::from_bytes([2u8; 16]));
        let sealed = record(&key, &realm_id, entry);
        assert!(
            !sealed
                .sealed
                .windows(CANARY.len())
                .any(|window| window == CANARY)
        );
        let restarted = RealmVaultKey::derive(&SigningKey::from_bytes(&[7u8; 32]));
        assert_eq!(restarted.open(&realm_id, &sealed).unwrap().expose(), CANARY);

        let other_realm = RealmId::from_bytes([9u8; 32]);
        assert_eq!(key.open(&other_realm, &sealed), Err(RealmVaultError::Open));
        let mut moved = sealed.clone();
        moved.entry.id = Ulid::from_bytes([5u8; 16]);
        assert_eq!(key.open(&realm_id, &moved), Err(RealmVaultError::Open));
        let mut replayed = sealed.clone();
        replayed.revision = Ulid::from_bytes([6u8; 16]);
        assert_eq!(key.open(&realm_id, &replayed), Err(RealmVaultError::Open));
        let other_key = RealmVaultKey::derive(&SigningKey::from_bytes(&[8u8; 32]));
        assert_eq!(
            other_key.open(&realm_id, &sealed),
            Err(RealmVaultError::Open)
        );
        let mut cut = sealed.clone();
        cut.sealed.truncate(20);
        assert_eq!(key.open(&realm_id, &cut), Err(RealmVaultError::Open));
    }

    #[test]
    fn differs_from_node_vault() {
        // Equal input bytes must not give the node vault key, or a node secret could open realm records.
        let bytes = [7u8; 32];
        let realm = RealmVaultKey::derive(&SigningKey::from_bytes(&bytes));
        assert_ne!(realm.0, blake3::derive_key("aruna node vault v1", &bytes));
        assert_ne!(realm.0, bytes);
    }

    #[test]
    fn debug_hides_secrets() {
        let key = RealmVaultKey::derive(&SigningKey::from_bytes(&[7u8; 32]));
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let entry =
            RealmVaultEntry::new(RealmVaultPurpose::SmtpPassword, Ulid::from_bytes([2u8; 16]));
        let sealed = record(&key, &realm_id, entry);
        assert_eq!(format!("{key:?}"), "RealmVaultKey(***)");
        let shown = format!("{sealed:?}");
        assert!(shown.contains(&format!("sealed_len: {}", sealed.sealed.len())));
        assert!(!shown.contains(&format!("{:?}", sealed.sealed)));
    }

    #[test]
    fn refuses_unknown_purpose() {
        assert_eq!(
            RealmVaultPurpose::try_from(0),
            Err(RealmVaultError::UnknownPurpose(0))
        );
        let entry =
            RealmVaultEntry::new(RealmVaultPurpose::SmtpPassword, Ulid::from_bytes([2u8; 16]));
        let mut bytes = postcard::to_allocvec(&entry).unwrap();
        assert_eq!(bytes[0], 1);
        bytes[0] = 2;
        assert!(postcard::from_bytes::<RealmVaultEntry>(&bytes).is_err());
        let json = serde_json::to_string(&entry)
            .unwrap()
            .replacen(":1,", ":9,", 1);
        assert!(serde_json::from_str::<RealmVaultEntry>(&json).is_err());
        assert!(FetchScope::Infrastructure.allows(&RealmNodeKind::Server));
        let device = RealmNodeKind::User {
            owner: UserId::local(Ulid::from_bytes([4u8; 16]), RealmId::from_bytes([1u8; 32])),
        };
        assert!(!FetchScope::Infrastructure.allows(&device));
    }
}
