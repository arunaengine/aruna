use crate::types::{GroupId, UserId};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use zeroize::Zeroize;

const SEAL_CONTEXT: &str = "aruna s3 credential seal v1";

/// Node-held symmetric key that seals S3 credential secrets at rest. It is
/// derived from the node's long-term secret, so only the issuing node can
/// unseal, and it is reproduced identically across restarts.
#[derive(Clone, PartialEq, Eq)]
pub struct CredentialSealKey([u8; 32]);

impl CredentialSealKey {
    /// Deterministic domain-separated derivation from the node's secret bytes.
    pub fn derive(node_secret: &[u8; 32]) -> Self {
        Self(blake3::derive_key(SEAL_CONTEXT, node_secret))
    }

    /// Independent key with no issuing node; sealing stays self-consistent but
    /// nothing derived from a real node secret can unseal it.
    pub fn random() -> Self {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).expect("operating system random number generator failed");
        Self(bytes)
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new((&self.0).into())
    }
}

impl fmt::Debug for CredentialSealKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CredentialSealKey(***)")
    }
}

impl Drop for CredentialSealKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SealError {
    #[error("credential seal failed")]
    Seal,
    #[error("credential unseal failed")]
    Open,
    #[error("sealed secret is not valid utf-8")]
    Utf8,
}

/// Authenticated ciphertext of an S3 secret. The plaintext is never stored: it
/// is returned once at issuance and re-derived only on the issuing node.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SealedS3Secret {
    nonce: [u8; 24],
    ciphertext: Vec<u8>,
}

impl fmt::Debug for SealedS3Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SealedS3Secret(***)")
    }
}

impl SealedS3Secret {
    /// Placeholder for records that never sign, such as the anonymous
    /// principal. Opening it always fails, so it can never authenticate.
    pub fn empty() -> Self {
        Self {
            nonce: [0u8; 24],
            ciphertext: Vec::new(),
        }
    }

    pub fn seal(key: &CredentialSealKey, plaintext: &str, aad: &[u8]) -> Result<Self, SealError> {
        let mut nonce = [0u8; 24];
        getrandom::fill(&mut nonce).map_err(|_| SealError::Seal)?;
        let ciphertext = key
            .cipher()
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: plaintext.as_bytes(),
                    aad,
                },
            )
            .map_err(|_| SealError::Seal)?;
        Ok(Self { nonce, ciphertext })
    }

    pub fn open(&self, key: &CredentialSealKey, aad: &[u8]) -> Result<String, SealError> {
        let plaintext = key
            .cipher()
            .decrypt(
                XNonce::from_slice(&self.nonce),
                Payload {
                    msg: &self.ciphertext,
                    aad,
                },
            )
            .map_err(|_| SealError::Open)?;
        String::from_utf8(plaintext).map_err(|_| SealError::Utf8)
    }
}

/// Additional authenticated data binding a sealed secret to the fields that
/// must not change: access key id, user, group, issuing node, and expiry. A
/// record moved to another key, user, group, node, or expiry no longer opens.
pub fn credential_aad(
    access_key: &str,
    user_identity: UserId,
    group_id: GroupId,
    issued_by: [u8; 32],
    expiry: SystemTime,
) -> Vec<u8> {
    let expiry_secs = expiry
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    let mut aad = Vec::new();
    aad.extend_from_slice(access_key.as_bytes());
    aad.push(0);
    aad.extend_from_slice(&user_identity.to_bytes());
    aad.extend_from_slice(&group_id.to_bytes());
    aad.extend_from_slice(&issued_by);
    aad.extend_from_slice(&expiry_secs.to_le_bytes());
    aad
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::RealmId;
    use ulid::Ulid;

    fn sample_aad() -> Vec<u8> {
        credential_aad(
            "AKIA",
            UserId::local(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            Ulid::from_bytes([3u8; 16]),
            [4u8; 32],
            SystemTime::UNIX_EPOCH,
        )
    }

    #[test]
    fn seal_open_roundtrip() {
        let key = CredentialSealKey::derive(&[7u8; 32]);
        let aad = sample_aad();
        let sealed = SealedS3Secret::seal(&key, "top-secret", &aad).unwrap();
        assert_eq!(sealed.open(&key, &aad).unwrap(), "top-secret");
    }

    #[test]
    fn wrong_key_fails() {
        // A copied record on a node with a different secret never yields plaintext.
        let aad = sample_aad();
        let sealed =
            SealedS3Secret::seal(&CredentialSealKey::derive(&[7u8; 32]), "s", &aad).unwrap();
        let other = CredentialSealKey::derive(&[8u8; 32]);
        assert_eq!(sealed.open(&other, &aad), Err(SealError::Open));
    }

    #[test]
    fn tampered_aad_fails() {
        // Rebinding any AAD field (here the group) fails the authentication tag.
        let key = CredentialSealKey::derive(&[7u8; 32]);
        let sealed = SealedS3Secret::seal(&key, "s", &sample_aad()).unwrap();
        let moved = credential_aad(
            "AKIA",
            UserId::local(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            Ulid::from_bytes([9u8; 16]),
            [4u8; 32],
            SystemTime::UNIX_EPOCH,
        );
        assert_eq!(sealed.open(&key, &moved), Err(SealError::Open));
    }

    #[test]
    fn restart_reproduces_key() {
        // Restart re-derives the identical key from the same node secret.
        let sealed =
            SealedS3Secret::seal(&CredentialSealKey::derive(&[5u8; 32]), "s", &[]).unwrap();
        assert_eq!(
            sealed
                .open(&CredentialSealKey::derive(&[5u8; 32]), &[])
                .unwrap(),
            "s"
        );
    }

    #[test]
    fn empty_never_opens() {
        assert!(
            SealedS3Secret::empty()
                .open(&CredentialSealKey::derive(&[1u8; 32]), &[])
                .is_err()
        );
    }
}
