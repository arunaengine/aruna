//! Records of encrypted buckets: key generations, key holders and sealed copies of bucket keys.
//! No record holds a plain private key; a node copy lives only in the node vault.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::UserId;
use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::structs::identity::realm::RealmId;
use crate::vault_format::key_fingerprint;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

/// HPKE purpose label of a bucket private key sealed to a user key.
pub const COPY_PURPOSE: &[u8] = b"aruna bucket key copy v1";
/// Holder tag of a user copy in a copy key.
const USER_TAG: u8 = 1;
/// Reserved for token credential copies (stage 5): tag, then the access key. Never stored yet.
const TOKEN_TAG: u8 = 2;
const REF_LEN: usize = 24;
const USER_KEY_LEN: usize = 48;

/// Names one key generation of one bucket. Keys bind to a stable id, because a deleted
/// bucket's name may be used again.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct BucketKeyRef {
    pub bucket_id: Ulid,
    pub generation: u64,
}

impl BucketKeyRef {
    pub fn new(bucket_id: Ulid, generation: u64) -> Self {
        Self {
            bucket_id,
            generation,
        }
    }

    /// Bucket id, then the big-endian generation, so a bucket's generations scan in order.
    pub fn key(&self) -> Vec<u8> {
        [
            &self.bucket_id.to_bytes()[..],
            &self.generation.to_be_bytes(),
        ]
        .concat()
    }

    pub fn from_key(bytes: &[u8]) -> Result<Self, ConversionError> {
        let (id, generation) = bytes
            .split_first_chunk::<16>()
            .and_then(|(id, rest)| Some((id, rest.first_chunk::<8>()?)))
            .filter(|_| bytes.len() == REF_LEN)
            .ok_or_else(|| ConversionError::InvalidLength("bucket key reference".to_string()))?;
        Ok(Self::new(
            Ulid::from_bytes(*id),
            u64::from_be_bytes(*generation),
        ))
    }
}

/// Lifecycle of one key generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KeyState {
    /// New writes seal to this generation.
    Active,
    /// Archives still use it while a rotation or decrypting change runs.
    Retiring,
    /// No archive uses it any more.
    Retired,
}

/// One key generation of a bucket, stored in `bucket_keys` under its reference.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BucketKeyRecord {
    pub key: BucketKeyRef,
    pub record_id: Ulid,
    pub public_key: [u8; 32],
    pub fingerprint: [u8; 32],
    pub created_at_ms: u64,
    pub state: KeyState,
    /// Node vault entry holding the private key, for `node_managed` buckets only.
    pub vault_entry: Option<Ulid>,
}

impl BucketKeyRecord {
    pub fn new(key: BucketKeyRef, record_id: Ulid, public_key: [u8; 32], now_ms: u64) -> Self {
        Self {
            key,
            record_id,
            public_key,
            fingerprint: key_fingerprint(&public_key),
            created_at_ms: now_ms,
            state: KeyState::Active,
            vault_entry: None,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        self.checked()?;
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let record: Self = postcard::from_bytes(bytes)?;
        record.checked()?;
        Ok(record)
    }

    fn checked(&self) -> Result<(), ConversionError> {
        if key_fingerprint(&self.public_key) != self.fingerprint {
            return Err(BucketKeyError::Fingerprint.into());
        }
        Ok(())
    }
}

/// Why a user may hold a bucket key.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum HolderOrigin {
    Creator,
    /// A user with WRITE on the group's admin path.
    Admin,
    Explicit,
}

/// Whether a holder has a sealed copy of the active generation. Pending holders never count
/// toward recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GrantState {
    Pending,
    Ready,
}

/// One key holder of a bucket, stored in `bucket_holders` under bucket id and user.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BucketHolder {
    pub bucket_id: Ulid,
    pub user_id: UserId,
    pub origin: HolderOrigin,
    pub state: GrantState,
    pub granted_by: UserId,
    pub granted_at_ms: u64,
}

impl BucketHolder {
    pub fn key(&self) -> Vec<u8> {
        [
            &self.bucket_id.to_bytes()[..],
            &self.user_id.to_storage_key(),
        ]
        .concat()
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// A bucket private key sealed with HPKE to one public key of a user, stored in
/// `bucket_key_copies`. The bucket node never opens it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SealedCopy {
    pub key: BucketKeyRef,
    pub user_id: UserId,
    /// The user key record the copy is sealed to.
    pub key_record: Ulid,
    /// Vault slot id of that user key, so the client opens the matching private key.
    pub key_id: String,
    pub enc: [u8; 32],
    pub ciphertext: Vec<u8>,
    pub created_at_ms: u64,
}

impl SealedCopy {
    /// Reference, user tag, user and user key record: one user's copies scan per generation.
    pub fn key(&self) -> Vec<u8> {
        [
            &self.key.key()[..],
            &[USER_TAG],
            &self.user_id.to_storage_key(),
            &self.key_record.to_bytes(),
        ]
        .concat()
    }

    /// Reads the reference and user of a copy key. Token copies are refused until stage 5.
    pub fn parse_key(bytes: &[u8]) -> Result<(BucketKeyRef, UserId, Ulid), ConversionError> {
        let (reference, rest) = bytes
            .split_at_checked(REF_LEN)
            .ok_or_else(|| ConversionError::InvalidLength("sealed copy key".to_string()))?;
        match rest.split_first() {
            Some((&USER_TAG, holder)) if holder.len() == USER_KEY_LEN + 16 => {
                let (user, record) = holder.split_at(USER_KEY_LEN);
                Ok((
                    BucketKeyRef::from_key(reference)?,
                    UserId::from_storage_key(user)?,
                    Ulid::from_bytes(record.try_into()?),
                ))
            }
            Some((&TOKEN_TAG, _)) => Err(BucketKeyError::Unsupported.into()),
            _ => Err(ConversionError::InvalidLength(
                "sealed copy holder".to_string(),
            )),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// HPKE info of a copy: the purpose label, then the realm, node, bucket, generation, user and
/// user key record. Every part has a fixed length, so the encoding is canonical.
pub fn copy_info(
    realm_id: RealmId,
    node_id: NodeId,
    key: BucketKeyRef,
    user_id: UserId,
    key_record: Ulid,
) -> Vec<u8> {
    [
        COPY_PURPOSE,
        &[0],
        realm_id.as_bytes(),
        node_id.as_bytes(),
        &key.key(),
        &user_id.to_storage_key(),
        &key_record.to_bytes(),
    ]
    .concat()
}

/// Typed failures of bucket keys, unlock state and encrypted content access.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum BucketKeyError {
    /// The bucket key is not unlocked on this node, so plaintext stays unavailable.
    #[error("bucket {0} is locked")]
    Locked(Ulid),
    #[error("the key does not match the bucket public key")]
    WrongKey,
    #[error("key generation {requested} is not the expected generation {current}")]
    StaleGeneration { requested: u64, current: u64 },
    #[error("the unlock session does not match")]
    SessionMismatch,
    /// The registry is full; no other bucket is locked to make room.
    #[error("no room for another unlocked bucket key")]
    Capacity,
    #[error("the key fingerprint does not match the public key")]
    Fingerprint,
    #[error("this encrypted bucket operation is not supported yet")]
    Unsupported,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_seal::{KeySealError, open_sealed, seal_to};
    use iroh::SecretKey;

    fn user(seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
    }

    #[test]
    fn keys_round_trip() {
        let reference = BucketKeyRef::new(Ulid::from_bytes([4; 16]), 3);
        assert_eq!(BucketKeyRef::from_key(&reference.key()).unwrap(), reference);
        assert!(BucketKeyRef::from_key(&reference.key()[..23]).is_err());
        let later = BucketKeyRef::new(reference.bucket_id, 256);
        assert!(reference.key() < later.key());

        let copy = SealedCopy {
            key: reference,
            user_id: user(5),
            key_record: Ulid::from_bytes([6; 16]),
            key_id: "slot".to_string(),
            enc: [7; 32],
            ciphertext: vec![8; 48],
            created_at_ms: 9,
        };
        let parsed = SealedCopy::parse_key(&copy.key()).unwrap();
        assert_eq!(parsed, (reference, copy.user_id, copy.key_record));
        let token = [&reference.key()[..], &[TOKEN_TAG], b"ACCESSKEY"].concat();
        assert!(SealedCopy::parse_key(&token).is_err());
        assert_eq!(
            SealedCopy::from_bytes(&copy.to_bytes().unwrap()).unwrap(),
            copy
        );
    }

    #[test]
    fn key_record_checks_fingerprint() {
        let reference = BucketKeyRef::new(Ulid::from_bytes([1; 16]), 1);
        let mut record = BucketKeyRecord::new(reference, Ulid::from_bytes([2; 16]), [3; 32], 4);
        let decoded = BucketKeyRecord::from_bytes(&record.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded, record);
        record.public_key[0] ^= 1;
        assert!(record.to_bytes().is_err());
    }

    #[test]
    fn copies_open_bound() {
        let realm = RealmId::from_bytes([1; 32]);
        let node = SecretKey::from_bytes(&[2; 32]).public();
        let reference = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 1);
        let record = Ulid::from_bytes([4; 16]);
        let private = [9u8; 32];
        let public = x25519_dalek::PublicKey::from(&x25519_dalek::StaticSecret::from(private));
        let info = copy_info(realm, node, reference, user(5), record);
        let sealed = seal_to(public.as_bytes(), &info, &[], b"bucket private key").unwrap();
        assert_eq!(
            &*open_sealed(&private, &sealed, &info, &[]).unwrap(),
            b"bucket private key"
        );

        let other_node = SecretKey::from_bytes(&[3; 32]).public();
        let next = BucketKeyRef::new(reference.bucket_id, 2);
        let other_bucket = BucketKeyRef::new(Ulid::from_bytes([8; 16]), 1);
        let wrong = [
            copy_info(
                RealmId::from_bytes([2; 32]),
                node,
                reference,
                user(5),
                record,
            ),
            copy_info(realm, other_node, reference, user(5), record),
            copy_info(realm, node, next, user(5), record),
            copy_info(realm, node, other_bucket, user(5), record),
            copy_info(realm, node, reference, user(6), record),
            copy_info(realm, node, reference, user(5), Ulid::from_bytes([5; 16])),
            [b"other purpose".as_slice(), &info[COPY_PURPOSE.len()..]].concat(),
        ];
        for info in wrong {
            let opened = open_sealed(&private, &sealed, &info, &[]);
            assert_eq!(opened.unwrap_err(), KeySealError::Open);
        }
        let mut tampered = sealed.clone();
        tampered.ciphertext[0] ^= 1;
        let opened = open_sealed(&private, &tampered, &info, &[]);
        assert_eq!(opened.unwrap_err(), KeySealError::Open);
    }
}
