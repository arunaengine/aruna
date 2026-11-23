//! Password hashing and passphrase key derivation: argon2id for new secrets, with the
//! algorithm and its parameters stored next to every hash so older pbkdf2 entries stay readable.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use argon2::{Algorithm, Argon2, Params, Version};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use thiserror::Error;
use zeroize::Zeroizing;

pub const ARGON2_NAME: &str = "argon2id";
pub const PBKDF2_NAME: &str = "pbkdf2-sha256";
/// Bounds for stored parameters, so a stored hash cannot stall the reader.
pub const MAX_MEMORY_KIB: u32 = 1024 * 1024;
pub const MAX_PASSES: u32 = 64;
pub const MAX_LANES: u32 = 16;
pub const MAX_PBKDF2_ROUNDS: u32 = 10_000_000;

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PasswordError {
    #[error("the key derivation parameters are not supported")]
    Parameters,
}

/// A key derivation function with its cost parameters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Kdf {
    Argon2id {
        memory_kib: u32,
        passes: u32,
        lanes: u32,
    },
    Pbkdf2Sha256 {
        rounds: u32,
    },
}

impl Kdf {
    /// Passphrases of user vaults; the client derives the key (RFC 9106, second choice).
    pub const VAULT: Self = Self::Argon2id {
        memory_kib: 64 * 1024,
        passes: 3,
        lanes: 1,
    };
    /// Share link passwords, which the node checks on every open (OWASP minimum).
    pub const SHARE: Self = Self::Argon2id {
        memory_kib: 19 * 1024,
        passes: 2,
        lanes: 1,
    };

    /// Checks that the parameters are in the readable range.
    pub fn validate(&self) -> Result<(), PasswordError> {
        let valid = match *self {
            Self::Argon2id {
                memory_kib,
                passes,
                lanes,
            } => {
                (1..=MAX_PASSES).contains(&passes)
                    && (1..=MAX_LANES).contains(&lanes)
                    && memory_kib <= MAX_MEMORY_KIB
                    && Params::new(memory_kib, passes, lanes, Some(32)).is_ok()
            }
            Self::Pbkdf2Sha256 { rounds } => (1..=MAX_PBKDF2_ROUNDS).contains(&rounds),
        };
        valid.then_some(()).ok_or(PasswordError::Parameters)
    }

    /// Derives a 32-byte key from `secret` and `salt`.
    pub fn derive(&self, secret: &[u8], salt: &[u8]) -> Result<Zeroizing<[u8; 32]>, PasswordError> {
        self.validate()?;
        let mut key = Zeroizing::new([0u8; 32]);
        match *self {
            Self::Argon2id {
                memory_kib,
                passes,
                lanes,
            } => {
                let params = Params::new(memory_kib, passes, lanes, Some(32))
                    .map_err(|_| PasswordError::Parameters)?;
                Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
                    .hash_password_into(secret, salt, key.as_mut_slice())
                    .map_err(|_| PasswordError::Parameters)?;
            }
            Self::Pbkdf2Sha256 { rounds } => {
                pbkdf2::pbkdf2_hmac::<Sha256>(secret, salt, rounds, key.as_mut_slice());
            }
        }
        Ok(key)
    }
}

/// A stored password check: algorithm, parameters, salt and derived hash.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PasswordHash {
    pub kdf: Kdf,
    pub salt: [u8; 16],
    pub hash: [u8; 32],
}

impl std::fmt::Debug for PasswordHash {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PasswordHash")
            .field("kdf", &self.kdf)
            .finish_non_exhaustive()
    }
}

impl PasswordHash {
    /// Hashes `password` with `kdf` and a caller-chosen random salt.
    pub fn new(password: &str, kdf: Kdf, salt: [u8; 16]) -> Result<Self, PasswordError> {
        let hash = kdf.derive(password.as_bytes(), &salt)?;
        Ok(Self {
            kdf,
            salt,
            hash: *hash,
        })
    }

    /// Compares in constant time; unreadable parameters never match.
    pub fn verify(&self, password: &str) -> bool {
        self.kdf
            .derive(password.as_bytes(), &self.salt)
            .is_ok_and(|hash| bool::from(hash.as_slice().ct_eq(&self.hash)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHEAP: Kdf = Kdf::Argon2id {
        memory_kib: 64,
        passes: 1,
        lanes: 1,
    };

    #[test]
    fn verifies_only_password() {
        let hash = PasswordHash::new("open sesame", CHEAP, [7; 16]).unwrap();
        assert!(hash.verify("open sesame"));
        assert!(!hash.verify("open sesame "));
        let stored = postcard::to_allocvec(&hash).unwrap();
        let read: PasswordHash = postcard::from_bytes(&stored).unwrap();
        assert!(read.verify("open sesame"));
    }

    #[test]
    fn rejects_stalling_parameters() {
        let huge = Kdf::Argon2id {
            memory_kib: MAX_MEMORY_KIB + 1,
            passes: 1,
            lanes: 1,
        };
        assert_eq!(huge.derive(b"x", &[0; 16]), Err(PasswordError::Parameters));
        let mut hash = PasswordHash::new("x", CHEAP, [1; 16]).unwrap();
        hash.kdf = Kdf::Pbkdf2Sha256 { rounds: 0 };
        assert!(!hash.verify("x"));
    }

    #[test]
    fn matches_reference_vectors() {
        // RFC 9106 style check against an independent implementation (argon2-cffi).
        let argon = Kdf::Argon2id {
            memory_kib: 64,
            passes: 2,
            lanes: 1,
        };
        let key = argon.derive(b"password", b"somesaltsomesalt").unwrap();
        assert_eq!(hex::encode(*key), ARGON2_EXPECTED);
        let pbkdf2 = Kdf::Pbkdf2Sha256 { rounds: 1 };
        let key = pbkdf2.derive(b"password", b"salt").unwrap();
        assert_eq!(
            hex::encode(*key),
            "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"
        );
    }

    const ARGON2_EXPECTED: &str =
        "1a98fba8e6394425c9bdc333a9c750aaf811df070f5fb4a5788e1cf4db5793a1";
}
