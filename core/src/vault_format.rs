//! Reads and writes the portal's passphrase-sealed user vault payload without any I/O.
//! Randomness comes from the caller, so every function here is deterministic.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::recovery_code::{RecoveryCodeError, parse_recovery};
use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{Aead, KeyInit, Nonce, Payload};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use thiserror::Error;
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

pub const VAULT_VERSION: u32 = 1;
pub const KDF_NAME: &str = "pbkdf2-sha256";
pub const VAULT_ITERATIONS: u32 = 600_000;
/// Rounds a payload may ask for, so a stored payload cannot stall a client.
pub const MAX_ITERATIONS: u32 = 10_000_000;
/// Associated data of every AES-GCM seal in a version 1 payload.
pub const VAULT_AAD: &[u8] = b"aruna user vault v1";
pub const X25519_KIND: &str = "x25519";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum VaultFormatError {
    #[error("the vault payload is not readable")]
    Unreadable,
    #[error("vault payload version {0} is not supported")]
    Version(u32),
    /// The passphrase, recovery code or master key does not open this block.
    #[error("the secret does not open the vault")]
    WrongSecret,
    #[error("this vault has no recovery code")]
    NoRecovery,
    #[error(transparent)]
    RecoveryCode(#[from] RecoveryCodeError),
    #[error("sealing failed")]
    Seal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultKdf {
    pub name: String,
    pub iterations: u32,
    pub salt: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WrappedMaster {
    pub nonce: String,
    pub wrapped: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryBlock {
    pub salt: String,
    pub nonce: String,
    pub wrapped: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedData {
    pub nonce: String,
    pub sealed: String,
}

/// One keypair in the `keys` slot. The private key is sealed with the master key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultKey {
    pub id: String,
    pub kind: String,
    pub public: String,
    pub nonce: String,
    pub wrapped_private: String,
    pub created_at: String,
    #[serde(default)]
    pub retired_at: Option<String>,
}

/// The vault payload JSON as the portal writes it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultPayload {
    pub version: u32,
    pub kdf: VaultKdf,
    pub master: WrappedMaster,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<RecoveryBlock>,
    pub keys: Vec<VaultKey>,
    pub data: SealedData,
}

/// The 32-byte key that seals the vault data and the private keys.
pub struct MasterKey(Zeroizing<[u8; 32]>);

impl fmt::Debug for MasterKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MasterKey(redacted)")
    }
}

impl MasterKey {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(Zeroizing::new(bytes))
    }

    /// Opens the vault data JSON.
    pub fn open_data(
        &self,
        payload: &VaultPayload,
    ) -> Result<Zeroizing<Vec<u8>>, VaultFormatError> {
        open(
            &self.0,
            &decode(&payload.data.nonce)?,
            &decode(&payload.data.sealed)?,
        )
    }

    pub fn seal_data(&self, plain: &[u8], nonce: [u8; 12]) -> Result<SealedData, VaultFormatError> {
        Ok(SealedData {
            nonce: STANDARD.encode(nonce),
            sealed: STANDARD.encode(seal(&self.0, &nonce, plain)?),
        })
    }

    /// Wraps this key under a passphrase or recovery code secret.
    pub fn wrap(
        &self,
        secret: &[u8],
        salt: &[u8],
        iterations: u32,
        nonce: [u8; 12],
    ) -> Result<WrappedMaster, VaultFormatError> {
        let kek = derive_kek(secret, salt, iterations);
        Ok(WrappedMaster {
            nonce: STANDARD.encode(nonce),
            wrapped: STANDARD.encode(seal(&kek, &nonce, self.0.as_slice())?),
        })
    }

    /// Seals an X25519 private key into a `keys` entry.
    pub fn wrap_keypair(
        &self,
        id: String,
        private: &StaticSecret,
        nonce: [u8; 12],
        created_at: String,
    ) -> Result<VaultKey, VaultFormatError> {
        Ok(VaultKey {
            id,
            kind: X25519_KIND.to_string(),
            public: STANDARD.encode(PublicKey::from(private).as_bytes()),
            nonce: STANDARD.encode(nonce),
            wrapped_private: STANDARD.encode(seal(&self.0, &nonce, private.as_bytes())?),
            created_at,
            retired_at: None,
        })
    }

    /// Opens a `keys` entry and checks that its public key matches the private key.
    pub fn unwrap_keypair(&self, key: &VaultKey) -> Result<StaticSecret, VaultFormatError> {
        if key.kind != X25519_KIND {
            return Err(VaultFormatError::Unreadable);
        }
        let plain = open(
            &self.0,
            &decode(&key.nonce)?,
            &decode(&key.wrapped_private)?,
        )?;
        let bytes: [u8; 32] = plain
            .as_slice()
            .try_into()
            .map_err(|_| VaultFormatError::Unreadable)?;
        let private = StaticSecret::from(bytes);
        if STANDARD.encode(PublicKey::from(&private).as_bytes()) != key.public {
            return Err(VaultFormatError::Unreadable);
        }
        Ok(private)
    }
}

impl VaultPayload {
    /// Parses payload text and checks the version, KDF and every encoded field.
    pub fn parse(text: &str) -> Result<Self, VaultFormatError> {
        let payload: Self = serde_json::from_str(text).map_err(|_| VaultFormatError::Unreadable)?;
        if payload.version != VAULT_VERSION {
            return Err(VaultFormatError::Version(payload.version));
        }
        if payload.kdf.name != KDF_NAME || !(1..=MAX_ITERATIONS).contains(&payload.kdf.iterations) {
            return Err(VaultFormatError::Unreadable);
        }
        let mut fields = vec![
            &payload.kdf.salt,
            &payload.master.nonce,
            &payload.master.wrapped,
            &payload.data.nonce,
            &payload.data.sealed,
        ];
        if let Some(recovery) = &payload.recovery {
            fields.extend([&recovery.salt, &recovery.nonce, &recovery.wrapped]);
        }
        for key in &payload.keys {
            fields.extend([&key.public, &key.nonce, &key.wrapped_private]);
        }
        for field in fields {
            decode(field)?;
        }
        Ok(payload)
    }

    pub fn unlock_passphrase(&self, passphrase: &str) -> Result<MasterKey, VaultFormatError> {
        let kek = derive_kek(
            passphrase.as_bytes(),
            &decode(&self.kdf.salt)?,
            self.kdf.iterations,
        );
        unwrap_master(&kek, &self.master.nonce, &self.master.wrapped)
    }

    pub fn unlock_recovery(&self, code: &str) -> Result<MasterKey, VaultFormatError> {
        let recovery = self.recovery.as_ref().ok_or(VaultFormatError::NoRecovery)?;
        let code = parse_recovery(code)?;
        let kek = derive_kek(
            code.as_slice(),
            &decode(&recovery.salt)?,
            self.kdf.iterations,
        );
        unwrap_master(&kek, &recovery.nonce, &recovery.wrapped)
    }
}

/// SHA-256 of a raw public key, the fingerprint users compare.
pub fn key_fingerprint(public: &[u8; 32]) -> [u8; 32] {
    Sha256::digest(public).into()
}

fn derive_kek(secret: &[u8], salt: &[u8], iterations: u32) -> Zeroizing<[u8; 32]> {
    let mut kek = Zeroizing::new([0u8; 32]);
    pbkdf2::pbkdf2_hmac::<Sha256>(secret, salt, iterations, kek.as_mut_slice());
    kek
}

fn unwrap_master(
    kek: &[u8; 32],
    nonce: &str,
    wrapped: &str,
) -> Result<MasterKey, VaultFormatError> {
    let plain = open(kek, &decode(nonce)?, &decode(wrapped)?)?;
    let bytes: [u8; 32] = plain
        .as_slice()
        .try_into()
        .map_err(|_| VaultFormatError::Unreadable)?;
    Ok(MasterKey::from_bytes(bytes))
}

fn decode(text: &str) -> Result<Vec<u8>, VaultFormatError> {
    STANDARD
        .decode(text)
        .map_err(|_| VaultFormatError::Unreadable)
}

fn seal(key: &[u8; 32], nonce: &[u8; 12], plain: &[u8]) -> Result<Vec<u8>, VaultFormatError> {
    let cipher = Aes256Gcm::new(key.into());
    cipher
        .encrypt(
            &Nonce::<Aes256Gcm>::from(*nonce),
            Payload {
                msg: plain,
                aad: VAULT_AAD,
            },
        )
        .map_err(|_| VaultFormatError::Seal)
}

fn open(
    key: &[u8; 32],
    nonce: &[u8],
    sealed: &[u8],
) -> Result<Zeroizing<Vec<u8>>, VaultFormatError> {
    let nonce: [u8; 12] = nonce.try_into().map_err(|_| VaultFormatError::Unreadable)?;
    let cipher = Aes256Gcm::new(key.into());
    cipher
        .decrypt(
            &Nonce::<Aes256Gcm>::from(nonce),
            Payload {
                msg: sealed,
                aad: VAULT_AAD,
            },
        )
        .map(Zeroizing::new)
        .map_err(|_| VaultFormatError::WrongSecret)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Vector {
        passphrase: String,
        code: String,
        code_bytes: [u8; 32],
        master: [u8; 32],
        data: String,
        payload: VaultPayload,
        private: [u8; 32],
        fingerprint: [u8; 32],
    }

    fn vector() -> Vector {
        let all: serde_json::Value =
            serde_json::from_str(include_str!("../tests/vectors/vault.json")).unwrap();
        let text = |section: &str, name: &str| all[section][name].as_str().unwrap().to_string();
        let bytes = |section: &str, name: &str| -> [u8; 32] {
            hex::decode(text(section, name))
                .unwrap()
                .try_into()
                .unwrap()
        };
        Vector {
            passphrase: text("vault", "passphrase"),
            code: text("vault", "recovery_code"),
            code_bytes: bytes("vault", "recovery_bytes"),
            master: bytes("vault", "master_key"),
            data: text("vault", "data"),
            payload: VaultPayload::parse(&text("vault", "payload")).expect("vector parses"),
            private: bytes("keypair", "private"),
            fingerprint: bytes("keypair", "fingerprint"),
        }
    }

    fn nonce(text: &str) -> [u8; 12] {
        decode(text).unwrap().try_into().unwrap()
    }

    #[test]
    fn opens_vector_vault() {
        let vector = vector();
        let master = vector
            .payload
            .unlock_passphrase(&vector.passphrase)
            .unwrap();
        assert_eq!(*master.0, vector.master);
        let data = master.open_data(&vector.payload).unwrap();
        assert_eq!(data.as_slice(), vector.data.as_bytes());
        let recovered = vector.payload.unlock_recovery(&vector.code).unwrap();
        assert_eq!(*recovered.0, vector.master);
        let private = master.unwrap_keypair(&vector.payload.keys[0]).unwrap();
        assert_eq!(private.to_bytes(), vector.private);
        let public = PublicKey::from(&private);
        assert_eq!(key_fingerprint(public.as_bytes()), vector.fingerprint);
    }

    #[test]
    fn reproduces_vector_seals() {
        // Same inputs give the portal's bytes, so either side can write a vault.
        let vector = vector();
        let payload = &vector.payload;
        let master = MasterKey::from_bytes(vector.master);
        let salt = decode(&payload.kdf.salt).unwrap();
        let iterations = payload.kdf.iterations;
        let wrapped = master
            .wrap(
                vector.passphrase.as_bytes(),
                &salt,
                iterations,
                nonce(&payload.master.nonce),
            )
            .unwrap();
        assert_eq!(wrapped, payload.master);
        let data = master
            .seal_data(vector.data.as_bytes(), nonce(&payload.data.nonce))
            .unwrap();
        assert_eq!(data, payload.data);
        let entry = &payload.keys[0];
        let key = master
            .wrap_keypair(
                entry.id.clone(),
                &StaticSecret::from(vector.private),
                nonce(&entry.nonce),
                entry.created_at.clone(),
            )
            .unwrap();
        assert_eq!(&key, entry);
    }

    #[test]
    fn rejects_wrong_secrets() {
        let vector = vector();
        assert_eq!(
            vector.payload.unlock_passphrase("wrong passphrase").err(),
            Some(VaultFormatError::WrongSecret)
        );
        let mut other = vector.code_bytes;
        other[0] ^= 1;
        assert_eq!(
            vector
                .payload
                .unlock_recovery(&crate::recovery_code::format_recovery(&other))
                .err(),
            Some(VaultFormatError::WrongSecret)
        );
        let mut tampered = vector.payload.keys[0].clone();
        tampered.public = vector.payload.keys[0].nonce.clone();
        let master = MasterKey::from_bytes(vector.master);
        assert_eq!(
            master.unwrap_keypair(&tampered).err(),
            Some(VaultFormatError::Unreadable)
        );
    }

    fn parse_changed(
        payload: &VaultPayload,
        change: impl Fn(&mut serde_json::Value),
    ) -> Result<VaultPayload, VaultFormatError> {
        let mut value = serde_json::to_value(payload).unwrap();
        change(&mut value);
        VaultPayload::parse(&value.to_string())
    }

    #[test]
    fn checks_payload_shape() {
        let vector = vector();
        let parse =
            |change: fn(&mut serde_json::Value)| parse_changed(&vector.payload, change).err();
        assert_eq!(
            parse(|value| value["version"] = 2.into()),
            Some(VaultFormatError::Version(2))
        );
        let rounds = |value: &mut serde_json::Value| {
            value["kdf"]["iterations"] = (MAX_ITERATIONS + 1).into()
        };
        assert_eq!(parse(rounds), Some(VaultFormatError::Unreadable));
        let sealed = |value: &mut serde_json::Value| value["data"]["sealed"] = "not base64!".into();
        assert_eq!(parse(sealed), Some(VaultFormatError::Unreadable));
        // A vault without a recovery code omits the block.
        let payload = parse_changed(&vector.payload, |value| {
            value.as_object_mut().unwrap().remove("recovery");
        })
        .unwrap();
        assert_eq!(
            payload.unlock_recovery(&vector.code).err(),
            Some(VaultFormatError::NoRecovery)
        );
        assert!(
            !serde_json::to_string(&payload)
                .unwrap()
                .contains("recovery")
        );
    }
}
