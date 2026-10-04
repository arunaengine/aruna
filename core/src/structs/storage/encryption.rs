//! Records of encrypted buckets: key generations, key holders and sealed copies of bucket keys.
//! No record holds a plain private key; a node copy lives only in the node vault.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::UserId;
use crate::compute::SecretBytes;
use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::blob::ArchiveKey;
use crate::vault_format::key_fingerprint;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use subtle::ConstantTimeEq;
use thiserror::Error;
use ulid::Ulid;
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

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

/// Encryption setting of a bucket.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EncryptionMode {
    #[default]
    Off,
    /// The node vault holds a copy of the bucket key, so the bucket unlocks at startup.
    NodeManaged,
    /// Only key holders unlock the bucket; a restart locks it.
    VaultLocked,
}

/// Cipher of new Pithos block payloads.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum BlockCipher {
    #[default]
    #[serde(rename = "chacha20_poly1305")]
    ChaCha20Poly1305,
    #[serde(rename = "aes256_gcm")]
    Aes256Gcm,
}

/// How Pithos keys new blocks: from their content, or with a fresh random key each.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockKeys {
    #[default]
    ContentDerived,
    Unique,
}

/// Encryption settings and write fences of one node-local bucket, kept in `bucket_encryption`
/// under the bucket name. A bucket without a row does not encrypt.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BucketEncryption {
    pub mode: EncryptionMode,
    /// Stable id the bucket keys bind to, set when encryption is first enabled.
    pub bucket_id: Option<Ulid>,
    /// Generation new writes seal to; zero until the first key exists.
    pub key_generation: u64,
    /// Advances whenever the stored format of new writes changes, so a write that captured an
    /// older plan fails its final publication.
    pub storage_generation: u64,
    pub cipher: BlockCipher,
    pub block_keys: BlockKeys,
    /// Longest unlock a holder may request; none means until lock or restart.
    pub max_unlock_ms: Option<u64>,
}

impl BucketEncryption {
    pub fn is_encrypted(&self) -> bool {
        self.mode != EncryptionMode::Off
    }

    /// The generation new writes seal to, if the bucket encrypts them.
    pub fn active_key(&self) -> Option<BucketKeyRef> {
        let bucket_id = self.bucket_id.filter(|_| self.is_encrypted())?;
        Some(BucketKeyRef::new(bucket_id, self.key_generation))
    }

    /// The settings of a bucket from its row; no row means encryption off.
    pub fn from_row(row: Option<&[u8]>) -> Result<Self, ConversionError> {
        row.map_or_else(|| Ok(Self::default()), Self::from_bytes)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        self.checked()?;
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let settings: Self = postcard::from_bytes(bytes)?;
        settings.checked()?;
        Ok(settings)
    }

    /// An encrypting bucket needs its stable id and a key generation.
    pub fn checked(&self) -> Result<(), ConversionError> {
        if self.is_encrypted() && (self.bucket_id.is_none() || self.key_generation == 0) {
            return Err(ConversionError::InvalidLength(
                "an encrypted bucket needs a bucket id and a key generation".to_string(),
            ));
        }
        Ok(())
    }
}

/// How an encrypting write seals, captured from the bucket when the write is resolved.
/// It holds public material only.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SealPlan {
    pub key: BucketKeyRef,
    pub public_key: [u8; 32],
    pub cipher: BlockCipher,
    pub block_keys: BlockKeys,
    /// The bucket's storage generation when the plan was captured.
    pub storage_generation: u64,
}

impl SealPlan {
    /// The plan of new writes to a bucket with `settings`, sealing to its active `record`.
    /// A plain bucket has none.
    pub fn capture(
        settings: &BucketEncryption,
        record: &BucketKeyRecord,
    ) -> Result<Option<Self>, BucketKeyError> {
        if !settings.is_encrypted() {
            return Ok(None);
        }
        let active = settings.active_key().ok_or(BucketKeyError::Unsupported)?;
        if record.key != active || record.state != KeyState::Active {
            return Err(BucketKeyError::StaleGeneration {
                requested: record.key.generation,
                current: active.generation,
            });
        }
        Ok(Some(Self {
            key: active,
            public_key: record.public_key,
            cipher: settings.cipher,
            block_keys: settings.block_keys,
            storage_generation: settings.storage_generation,
        }))
    }

    /// Fails unless the bucket still seals to exactly this key and stored format. Another
    /// bucket id, mode off or a new generation all count as a different key.
    pub fn still_current(&self, settings: &BucketEncryption) -> Result<(), BucketKeyError> {
        if settings.active_key() != Some(self.key) {
            return Err(BucketKeyError::StaleGeneration {
                requested: self.key.generation,
                current: settings.key_generation,
            });
        }
        if settings.storage_generation != self.storage_generation {
            return Err(BucketKeyError::StaleGeneration {
                requested: self.storage_generation,
                current: settings.storage_generation,
            });
        }
        Ok(())
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
#[serde(rename_all = "snake_case")]
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

/// A user key a bucket private key is sealed to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyTarget {
    pub user_id: UserId,
    pub key_record: Ulid,
    pub key_id: String,
    pub public_key: [u8; 32],
}

/// A checked bucket key the adapter holds before reads may use it. Activating it starts the
/// unlock session; discarding it forgets the key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyTicket {
    pub key: BucketKeyRef,
    pub session_id: Ulid,
}

/// The unlock state of one key generation on this node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnlockStatus {
    pub key: BucketKeyRef,
    pub session_id: Ulid,
    /// False while the key is prepared but no read may use it yet.
    pub active: bool,
    pub unlocked_at: SystemTime,
    /// Time left until the timed lock; none means until lock or restart.
    pub remaining: Option<Duration>,
    /// Time left until the session maximum, the limit of every extension.
    pub max_remaining: Option<Duration>,
}

/// An admitted plaintext read of one archive. It keeps the bucket key and the archive in use
/// until dropped, but exposes no key to an operation.
pub struct ReadLease {
    pub key: BucketKeyRef,
    pub archive: ArchiveKey,
    pub session_id: Ulid,
    guard: Arc<dyn Any + Send + Sync>,
}

impl ReadLease {
    pub fn new(
        key: BucketKeyRef,
        archive: ArchiveKey,
        session_id: Ulid,
        guard: Arc<dyn Any + Send + Sync>,
    ) -> Self {
        Self {
            key,
            archive,
            session_id,
            guard,
        }
    }

    /// The adapter state behind the lease, which only the adapter can interpret.
    pub fn guard(&self) -> &(dyn Any + Send + Sync) {
        self.guard.as_ref()
    }
}

impl fmt::Debug for ReadLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadLease")
            .field("key", &self.key)
            .field("archive", &self.archive)
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl PartialEq for ReadLease {
    fn eq(&self, other: &Self) -> bool {
        (self.key, &self.archive, self.session_id) == (other.key, &other.archive, other.session_id)
            && Arc::ptr_eq(&self.guard, &other.guard)
    }
}

/// The X25519 public key of a 32-byte private key.
pub fn public_key_of(private: &SecretBytes) -> Option<[u8; 32]> {
    let mut bytes = Zeroizing::new([0u8; 32]);
    if private.expose().len() != bytes.len() {
        return None;
    }
    bytes.copy_from_slice(private.expose());
    let secret = StaticSecret::from(*bytes);
    Some(PublicKey::from(&secret).to_bytes())
}

/// Whether `private` is the X25519 private key of `public`, compared in constant time.
pub fn key_matches(private: &SecretBytes, public: &[u8; 32]) -> bool {
    public_key_of(private).is_some_and(|derived| derived.ct_eq(public).into())
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
    #[error("the unlock duration exceeds the bucket maximum")]
    InvalidDuration,
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
    fn settings_need_identity() {
        let mut settings = BucketEncryption::default();
        assert_eq!(settings.active_key(), None);
        settings.checked().unwrap();
        settings.mode = EncryptionMode::VaultLocked;
        assert!(settings.checked().is_err());
        settings.bucket_id = Some(Ulid::from_bytes([2; 16]));
        assert!(settings.checked().is_err());
        settings.key_generation = 1;
        settings.checked().unwrap();
        let active = BucketKeyRef::new(Ulid::from_bytes([2; 16]), 1);
        assert_eq!(settings.active_key(), Some(active));
        settings.mode = EncryptionMode::Off;
        assert_eq!(settings.active_key(), None);

        assert_eq!(
            BucketEncryption::from_row(None).unwrap(),
            BucketEncryption::default()
        );
        settings.mode = EncryptionMode::VaultLocked;
        settings.max_unlock_ms = Some(60_000);
        let row = settings.to_bytes().unwrap();
        assert_eq!(BucketEncryption::from_row(Some(&row)).unwrap(), settings);
        settings.key_generation = 0;
        assert!(settings.to_bytes().is_err());
        let broken = postcard::to_allocvec(&settings).unwrap();
        assert!(BucketEncryption::from_row(Some(&broken)).is_err());
    }

    #[test]
    fn plans_follow_settings() {
        let mut settings = BucketEncryption::default();
        let key = BucketKeyRef::new(Ulid::from_bytes([2; 16]), 1);
        let record = BucketKeyRecord::new(key, Ulid::from_bytes([3; 16]), [4; 32], 5);
        assert_eq!(SealPlan::capture(&settings, &record), Ok(None));

        settings.mode = EncryptionMode::NodeManaged;
        settings.bucket_id = Some(key.bucket_id);
        settings.key_generation = 1;
        settings.cipher = BlockCipher::Aes256Gcm;
        settings.storage_generation = 6;
        let plan = SealPlan::capture(&settings, &record).unwrap().unwrap();
        assert_eq!((plan.key, plan.public_key), (key, [4; 32]));
        assert_eq!(plan.cipher, BlockCipher::Aes256Gcm);
        plan.still_current(&settings).unwrap();

        let mut retiring = record.clone();
        retiring.state = KeyState::Retiring;
        assert!(SealPlan::capture(&settings, &retiring).is_err());
        settings.storage_generation = 7;
        assert!(plan.still_current(&settings).is_err());
        settings.storage_generation = 6;
        settings.key_generation = 2;
        assert!(plan.still_current(&settings).is_err());
        assert!(SealPlan::capture(&settings, &record).is_err());

        // Equal numbers do not hide another bucket id or a switch to off.
        settings.key_generation = 1;
        plan.still_current(&settings).unwrap();
        let mut other = settings.clone();
        other.bucket_id = Some(Ulid::from_bytes([9; 16]));
        assert!(plan.still_current(&other).is_err());
        let mut off = settings.clone();
        off.mode = EncryptionMode::Off;
        assert!(plan.still_current(&off).is_err());
    }

    #[test]
    fn wire_names_match() {
        let names = serde_json::to_value((
            [
                EncryptionMode::Off,
                EncryptionMode::NodeManaged,
                EncryptionMode::VaultLocked,
            ],
            [BlockCipher::ChaCha20Poly1305, BlockCipher::Aes256Gcm],
            [BlockKeys::ContentDerived, BlockKeys::Unique],
            [
                HolderOrigin::Creator,
                HolderOrigin::Admin,
                HolderOrigin::Explicit,
            ],
        ))
        .unwrap();
        let expected = serde_json::json!([
            ["off", "node_managed", "vault_locked"],
            ["chacha20_poly1305", "aes256_gcm"],
            ["content_derived", "unique"],
            ["creator", "admin", "explicit"],
        ]);
        assert_eq!(names, expected);
    }

    #[test]
    fn keys_never_formatted() {
        use crate::compute::SecretBytes;
        use crate::effects::BlobEffect;
        use crate::events::BlobEvent;
        use crate::structs::storage::blob::BackendRef;

        const CANARY: [u8; 32] = *b"canary-bucket-key-6f1d-0000-0000";
        let key = BucketKeyRef::new(Ulid::from_bytes([1; 16]), 2);
        let prepare = BlobEffect::PrepareKey {
            key,
            public_key: [3; 32],
            private_key: SecretBytes::new(CANARY.to_vec()),
            duration: None,
            max: None,
        };
        let generated = BlobEvent::BucketKeyGenerated {
            public_key: [3; 32],
            private_key: SecretBytes::new(CANARY.to_vec()),
        };
        let archive = ArchiveKey::new(Ulid::from_bytes([4; 16]), BackendRef::node_default());
        let guard: Arc<dyn Any + Send + Sync> = Arc::new(SecretBytes::new(CANARY.to_vec()));
        let lease = ReadLease::new(
            key,
            archive.clone(),
            Ulid::from_bytes([5; 16]),
            guard.clone(),
        );
        let canary = String::from_utf8_lossy(&CANARY).to_string();
        for formatted in [
            format!("{prepare:?}"),
            format!("{generated:?}"),
            format!("{lease:?}"),
        ] {
            assert!(!formatted.contains(&canary), "{formatted}");
            assert!(!formatted.contains("99, 97, 110"), "{formatted}");
        }
        // Two leases are equal only when they share one adapter state.
        let same = ReadLease::new(key, archive.clone(), lease.session_id, guard);
        let other = ReadLease::new(key, archive, lease.session_id, Arc::new(()));
        assert_eq!(lease, same);
        assert_ne!(lease, other);
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
