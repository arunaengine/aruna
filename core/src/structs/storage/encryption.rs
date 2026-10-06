//! Records of encrypted buckets: key generations, key holders and sealed copies of bucket keys.
//! No record holds a plain private key; a node copy lives only in the node vault.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::UserId;
use crate::compute::{SecretBytes, SharedSecret};
use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::key_seal::seal_to;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::blob::ArchiveKey;
use crate::vault_format::key_fingerprint;
use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{Aead, KeyInit, Nonce, Payload};
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
/// AES-GCM purpose label of a bucket private key sealed with a token key.
pub const TOKEN_PURPOSE: &[u8] = b"aruna bucket key token v1";
/// Holder tag of a user copy in a copy key.
const USER_TAG: u8 = 1;
/// Holder tag of a token credential copy: tag, then the access key.
const TOKEN_TAG: u8 = 2;
const REF_LEN: usize = 24;
const USER_KEY_LEN: usize = 48;
const TOKEN_LEN: usize = 32;

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

    /// Scan prefix of the copies `user_id` holds of one key generation.
    pub fn user_prefix(key: BucketKeyRef, user_id: UserId) -> Vec<u8> {
        [&key.key()[..], &[USER_TAG], &user_id.to_storage_key()].concat()
    }

    /// The user copy rows of a `bucket_key_copies` scan; token copies are left out undecoded.
    pub fn user_rows<K: AsRef<[u8]>, V>(rows: Vec<(K, V)>) -> Vec<(K, V)> {
        rows.into_iter()
            .filter(|(key, _)| Self::parse_key(key.as_ref()).is_ok())
            .collect()
    }

    /// Reads the reference and user of a copy key. A token copy key is refused.
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

/// A bucket private key sealed with AES-256-GCM under the random key of a token credential,
/// stored in `bucket_key_copies`. Only the client holds the token key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TokenCopy {
    pub key: BucketKeyRef,
    pub access_key: String,
    /// The holder who created the credential; the copy opens only while they hold the key.
    pub created_by: UserId,
    pub nonce: [u8; 12],
    pub ciphertext: Vec<u8>,
    pub created_at_ms: u64,
}

impl TokenCopy {
    pub fn key(&self) -> Vec<u8> {
        Self::copy_key(self.key, &self.access_key)
    }

    /// Reference, token tag and access key.
    pub fn copy_key(key: BucketKeyRef, access_key: &str) -> Vec<u8> {
        [&key.key()[..], &[TOKEN_TAG], access_key.as_bytes()].concat()
    }

    /// Reads the reference and access key of a token copy key; a user copy key is refused.
    pub fn parse_key(bytes: &[u8]) -> Result<(BucketKeyRef, String), ConversionError> {
        let (reference, rest) = bytes
            .split_at_checked(REF_LEN)
            .ok_or_else(|| ConversionError::InvalidLength("token copy key".to_string()))?;
        match rest.split_first() {
            Some((&TOKEN_TAG, access_key)) if !access_key.is_empty() => Ok((
                BucketKeyRef::from_key(reference)?,
                String::from_utf8(access_key.to_vec())?,
            )),
            _ => Err(ConversionError::InvalidLength(
                "token copy holder".to_string(),
            )),
        }
    }

    /// The `bucket_key_tokens` key of this copy: access key, a zero byte, then the reference.
    pub fn index_key(&self) -> Vec<u8> {
        [&Self::index_prefix(&self.access_key)[..], &self.key.key()].concat()
    }

    /// Scan prefix of the copies of one credential; access keys are alphanumeric.
    pub fn index_prefix(access_key: &str) -> Vec<u8> {
        [access_key.as_bytes(), &[0]].concat()
    }

    /// The reference an index key of `access_key` names.
    pub fn parse_index(bytes: &[u8], access_key: &str) -> Result<BucketKeyRef, ConversionError> {
        let reference = bytes
            .strip_prefix(Self::index_prefix(access_key).as_slice())
            .ok_or_else(|| ConversionError::InvalidLength("token index key".to_string()))?;
        BucketKeyRef::from_key(reference)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// The access key and token of a token credential, held for one request only.
#[derive(Clone, PartialEq, Eq)]
pub struct TokenCredential {
    pub access_key: String,
    pub token: SharedSecret,
}

impl TokenCredential {
    /// The token as clients send it in `x-amz-security-token`: lowercase hex.
    pub fn encode(token: &SecretBytes) -> Zeroizing<String> {
        Zeroizing::new(hex::encode(token.expose()))
    }

    /// Reads a token a client sent; anything but 32 hex-encoded bytes is refused.
    pub fn parse(access_key: &str, text: &[u8]) -> Option<Self> {
        let mut bytes = Zeroizing::new([0u8; TOKEN_LEN]);
        hex::decode_to_slice(text, bytes.as_mut_slice()).ok()?;
        Some(Self {
            access_key: access_key.to_string(),
            token: SharedSecret::new(SecretBytes::new(bytes.to_vec())),
        })
    }
}

impl fmt::Debug for TokenCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenCredential")
            .field("access_key", &self.access_key)
            .finish_non_exhaustive()
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
    pub sequence: Ulid,
    pub deadline_ms: Option<u64>,
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

/// A fresh bucket keypair from the system random number generator.
pub fn generate_key() -> Result<([u8; 32], SharedSecret), BucketKeyError> {
    let mut bytes = Zeroizing::new(vec![0u8; 32]);
    getrandom::fill(&mut bytes).map_err(|_| BucketKeyError::Seal)?;
    let private = SecretBytes::new(std::mem::take(&mut *bytes));
    let public = public_key_of(&private).ok_or(BucketKeyError::Seal)?;
    Ok((public, SharedSecret::new(private)))
}

/// Seals the private key of `key` to every target, bound to this realm and node by `copy_info`.
/// The key must match `public_key`, so a wrong key is never handed out.
pub fn seal_copies(
    key: BucketKeyRef,
    public_key: &[u8; 32],
    private: &SecretBytes,
    origin: (RealmId, NodeId),
    targets: &[CopyTarget],
    now_ms: u64,
) -> Result<Vec<SealedCopy>, BucketKeyError> {
    if !key_matches(private, public_key) {
        return Err(BucketKeyError::WrongKey);
    }
    let (realm_id, node_id) = origin;
    targets
        .iter()
        .map(|target| {
            let info = copy_info(realm_id, node_id, key, target.user_id, target.key_record);
            let sealed = seal_to(&target.public_key, &info, &[], private.expose())
                .map_err(|_| BucketKeyError::Seal)?;
            Ok(SealedCopy {
                key,
                user_id: target.user_id,
                key_record: target.key_record,
                key_id: target.key_id.clone(),
                enc: sealed.enc,
                ciphertext: sealed.ciphertext,
                created_at_ms: now_ms,
            })
        })
        .collect()
}

/// The wall-clock deadline `left` after `from_ms`; none when it cannot be represented.
pub fn deadline_after(from_ms: u64, left: Duration) -> Option<u64> {
    from_ms.checked_add(u64::try_from(left.as_millis()).ok()?)
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

/// A fresh random token key from the system random number generator.
pub fn generate_token() -> Result<SharedSecret, BucketKeyError> {
    let mut bytes = Zeroizing::new(vec![0u8; TOKEN_LEN]);
    getrandom::fill(&mut bytes).map_err(|_| BucketKeyError::Seal)?;
    Ok(SharedSecret::new(SecretBytes::new(std::mem::take(
        &mut *bytes,
    ))))
}

/// Seals the private key of `key` with `token`, bound to this realm, node and access key.
/// The key must match `public_key`, so a wrong key is never handed out.
pub fn seal_token(
    key: BucketKeyRef,
    public_key: &[u8; 32],
    private: &SecretBytes,
    origin: (RealmId, NodeId),
    holder: (&str, UserId),
    token: &SecretBytes,
    now_ms: u64,
) -> Result<TokenCopy, BucketKeyError> {
    if !key_matches(private, public_key) {
        return Err(BucketKeyError::WrongKey);
    }
    let (access_key, created_by) = holder;
    let cipher = token_cipher(token).ok_or(BucketKeyError::Seal)?;
    let mut nonce = [0u8; 12];
    getrandom::fill(&mut nonce).map_err(|_| BucketKeyError::Seal)?;
    let aad = token_info(origin.0, origin.1, key, access_key);
    let payload = Payload {
        msg: private.expose(),
        aad: &aad,
    };
    let ciphertext = cipher
        .encrypt(&Nonce::<Aes256Gcm>::from(nonce), payload)
        .map_err(|_| BucketKeyError::Seal)?;
    Ok(TokenCopy {
        key,
        access_key: access_key.to_string(),
        created_by,
        nonce,
        ciphertext,
        created_at_ms: now_ms,
    })
}

/// Opens `copy` with `token`. Another token or binding fails as `InvalidToken`; a key that does
/// not match `public_key` fails as `WrongKey`.
pub fn open_token(
    copy: &TokenCopy,
    public_key: &[u8; 32],
    origin: (RealmId, NodeId),
    token: &SecretBytes,
) -> Result<SecretBytes, BucketKeyError> {
    let cipher = token_cipher(token).ok_or(BucketKeyError::InvalidToken)?;
    let aad = token_info(origin.0, origin.1, copy.key, &copy.access_key);
    let payload = Payload {
        msg: &copy.ciphertext,
        aad: &aad,
    };
    let private = cipher
        .decrypt(&Nonce::<Aes256Gcm>::from(copy.nonce), payload)
        .map(SecretBytes::new)
        .map_err(|_| BucketKeyError::InvalidToken)?;
    if !key_matches(&private, public_key) {
        return Err(BucketKeyError::WrongKey);
    }
    Ok(private)
}

fn token_cipher(token: &SecretBytes) -> Option<Aes256Gcm> {
    let key = <&[u8; TOKEN_LEN]>::try_from(token.expose()).ok()?;
    Some(Aes256Gcm::new(key.into()))
}

/// AAD of a token copy: the purpose label, then the realm, node, bucket and generation, and the
/// access key last, so the encoding is canonical.
pub fn token_info(
    realm_id: RealmId,
    node_id: NodeId,
    key: BucketKeyRef,
    access_key: &str,
) -> Vec<u8> {
    [
        TOKEN_PURPOSE,
        &[0],
        realm_id.as_bytes(),
        node_id.as_bytes(),
        &key.key(),
        access_key.as_bytes(),
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
    #[error("a bucket key could not be generated or sealed")]
    Seal,
    #[error("this encrypted bucket operation is not supported yet")]
    Unsupported,
    /// The token of a token credential does not open its copy of the bucket key.
    #[error("the token does not open the bucket key")]
    InvalidToken,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_seal::{KeySealError, SealedSecret, open_sealed, seal_to};
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
        assert!(
            copy.key()
                .starts_with(&SealedCopy::user_prefix(reference, user(5)))
        );
        assert!(
            !copy
                .key()
                .starts_with(&SealedCopy::user_prefix(reference, user(6)))
        );
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
        use crate::compute::{SecretBytes, SharedSecret};
        use crate::effects::BlobEffect;
        use crate::events::BlobEvent;
        use crate::structs::storage::blob::BackendRef;

        const CANARY: [u8; 32] = *b"canary-bucket-key-6f1d-0000-0000";
        let key = BucketKeyRef::new(Ulid::from_bytes([1; 16]), 2);
        let prepare = BlobEffect::PrepareKey {
            key,
            public_key: [3; 32],
            private_key: SharedSecret::new(SecretBytes::new(CANARY.to_vec())),
            duration: None,
            max: None,
        };
        let generated = BlobEvent::BucketKeyGenerated {
            public_key: [3; 32],
            private_key: SharedSecret::new(SecretBytes::new(CANARY.to_vec())),
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
        // A shared handle hands one key on without copying it and still prints no bytes.
        let shared = SharedSecret::new(SecretBytes::new(CANARY.to_vec()));
        let handed = shared.clone();
        assert!(std::ptr::eq(shared.bytes(), handed.bytes()));
        assert!(!format!("{handed:?}").contains(&canary));

        // Two leases are equal only when they share one adapter state.
        let same = ReadLease::new(key, archive.clone(), lease.session_id, guard);
        let other = ReadLease::new(key, archive, lease.session_id, Arc::new(()));
        assert_eq!(lease, same);
        assert_ne!(lease, other);
    }

    #[test]
    fn seals_holder_copies() {
        let (public, private) = generate_key().unwrap();
        assert_eq!(public_key_of(private.bytes()), Some(public));
        let realm = RealmId::from_bytes([1; 32]);
        let node = SecretKey::from_bytes(&[2; 32]).public();
        let key = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 1);
        let holder_keys = [[5u8; 32], [6u8; 32]];
        let targets: Vec<_> = holder_keys
            .iter()
            .enumerate()
            .map(|(index, secret)| CopyTarget {
                user_id: user(index as u8 + 5),
                key_record: Ulid::from_bytes([index as u8 + 7; 16]),
                key_id: format!("slot-{index}"),
                public_key: public_key_of(&SecretBytes::new(secret.to_vec())).unwrap(),
            })
            .collect();

        let copies =
            seal_copies(key, &public, private.bytes(), (realm, node), &targets, 9).unwrap();
        for ((copy, target), secret) in copies.iter().zip(&targets).zip(&holder_keys) {
            assert_eq!(
                (copy.user_id, copy.key_record, copy.key),
                (target.user_id, target.key_record, key)
            );
            let info = copy_info(realm, node, key, target.user_id, target.key_record);
            let sealed = SealedSecret {
                enc: copy.enc,
                ciphertext: copy.ciphertext.clone(),
            };
            let opened = open_sealed(secret, &sealed, &info, &[]).unwrap();
            assert_eq!(opened.as_slice(), private.bytes().expose());
        }
        // A key that does not match the generation's public key is never sealed.
        let (other, _) = generate_key().unwrap();
        let wrong = seal_copies(key, &other, private.bytes(), (realm, node), &targets, 9);
        assert_eq!(wrong, Err(BucketKeyError::WrongKey));
    }

    #[test]
    fn deadlines_stay_representable() {
        assert_eq!(deadline_after(1_000, Duration::from_secs(2)), Some(3_000));
        assert_eq!(deadline_after(u64::MAX, Duration::from_millis(1)), None);
        assert_eq!(deadline_after(0, Duration::MAX), None);
    }

    #[test]
    fn records_check_fingerprint() {
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

    #[test]
    fn token_copies_bound() {
        let (public, private) = generate_key().unwrap();
        let realm = RealmId::from_bytes([1; 32]);
        let node = SecretKey::from_bytes(&[2; 32]).public();
        let key = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 1);
        let token = generate_token().unwrap();
        let holder = ("TOKENKEY", user(5));
        let copy = seal_token(
            key,
            &public,
            private.bytes(),
            (realm, node),
            holder,
            token.bytes(),
            9,
        );
        let copy = copy.unwrap();
        let opened = open_token(&copy, &public, (realm, node), token.bytes()).unwrap();
        assert_eq!(opened.expose(), private.bytes().expose());
        assert_eq!((copy.created_by, copy.created_at_ms), (user(5), 9));

        // Another token, binding or changed byte opens nothing.
        let other = generate_token().unwrap();
        let wrong = open_token(&copy, &public, (realm, node), other.bytes());
        assert_eq!(wrong, Err(BucketKeyError::InvalidToken));
        let short = SecretBytes::new(vec![1; 16]);
        let wrong = open_token(&copy, &public, (realm, node), &short);
        assert_eq!(wrong, Err(BucketKeyError::InvalidToken));
        let other_node = SecretKey::from_bytes(&[3; 32]).public();
        let wrong = open_token(&copy, &public, (realm, other_node), token.bytes());
        assert_eq!(wrong, Err(BucketKeyError::InvalidToken));
        let other_realm = RealmId::from_bytes([2; 32]);
        let wrong = open_token(&copy, &public, (other_realm, node), token.bytes());
        assert_eq!(wrong, Err(BucketKeyError::InvalidToken));
        for changed in [
            TokenCopy {
                key: BucketKeyRef::new(key.bucket_id, 2),
                ..copy.clone()
            },
            TokenCopy {
                access_key: "OTHERKEY".to_string(),
                ..copy.clone()
            },
            TokenCopy {
                nonce: [7; 12],
                ..copy.clone()
            },
        ] {
            let wrong = open_token(&changed, &public, (realm, node), token.bytes());
            assert_eq!(wrong, Err(BucketKeyError::InvalidToken));
        }
        let mut tampered = copy.clone();
        tampered.ciphertext[0] ^= 1;
        let wrong = open_token(&tampered, &public, (realm, node), token.bytes());
        assert_eq!(wrong, Err(BucketKeyError::InvalidToken));
        // A copy whose key does not match the generation's public key is neither made nor used.
        let (other_public, _) = generate_key().unwrap();
        let wrong = open_token(&copy, &other_public, (realm, node), token.bytes());
        assert_eq!(wrong, Err(BucketKeyError::WrongKey));
        let sealed = seal_token(
            key,
            &other_public,
            private.bytes(),
            (realm, node),
            holder,
            token.bytes(),
            9,
        );
        assert_eq!(sealed, Err(BucketKeyError::WrongKey));
    }

    #[test]
    fn token_keys_separate() {
        let key = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 2);
        let copy = TokenCopy {
            key,
            access_key: "AB".to_string(),
            created_by: user(5),
            nonce: [0; 12],
            ciphertext: vec![0; 48],
            created_at_ms: 1,
        };
        assert_eq!(
            TokenCopy::parse_key(&copy.key()).unwrap(),
            (key, "AB".to_string())
        );
        assert_eq!(
            TokenCopy::from_bytes(&copy.to_bytes().unwrap()).unwrap(),
            copy
        );
        assert!(SealedCopy::parse_key(&copy.key()).is_err());
        let holder = SealedCopy::user_prefix(key, user(5));
        assert!(TokenCopy::parse_key(&holder).is_err());
        // A scan of a bucket's copies keeps user copies only.
        let rows = vec![(copy.key(), 1), ([&holder[..], &[0; 16]].concat(), 2)];
        let users: Vec<_> = SealedCopy::user_rows(rows)
            .into_iter()
            .map(|row| row.1)
            .collect();
        assert_eq!(users, [2]);

        // The index of one credential never covers another whose access key extends it.
        assert_eq!(TokenCopy::parse_index(&copy.index_key(), "AB"), Ok(key));
        let longer = TokenCopy {
            access_key: "ABC".to_string(),
            ..copy.clone()
        };
        assert!(
            !longer
                .index_key()
                .starts_with(&TokenCopy::index_prefix("AB"))
        );
        assert!(TokenCopy::parse_index(&longer.index_key(), "AB").is_err());
    }

    #[test]
    fn tokens_parse_strictly() {
        let token = generate_token().unwrap();
        let text = TokenCredential::encode(token.bytes());
        assert_eq!(text.len(), 64);
        let parsed = TokenCredential::parse("KEY", text.as_bytes()).unwrap();
        assert_eq!((parsed.access_key.as_str(), &parsed.token), ("KEY", &token));
        for invalid in [&text[..62], format!("{}00", *text).as_str(), "zz", ""] {
            assert!(TokenCredential::parse("KEY", invalid.as_bytes()).is_none());
        }
    }

    #[test]
    fn tokens_never_formatted() {
        use crate::effects::BlobEffect;
        use crate::events::BlobEvent;
        use crate::structs::storage::blob::BackendRef;

        const CANARY: [u8; 32] = *b"canary-token-key-7a2c-0000-00000";
        let shared = || SharedSecret::new(SecretBytes::new(CANARY.to_vec()));
        let key = BucketKeyRef::new(Ulid::from_bytes([1; 16]), 2);
        let text = TokenCredential::encode(&SecretBytes::new(CANARY.to_vec()));
        let credential = TokenCredential::parse("TOKENKEY", text.as_bytes()).unwrap();
        let copy = TokenCopy {
            key,
            access_key: "TOKENKEY".to_string(),
            created_by: user(5),
            nonce: [0; 12],
            ciphertext: vec![0; 48],
            created_at_ms: 1,
        };
        let admit = BlobEffect::AdmitToken {
            key,
            archive: ArchiveKey::new(Ulid::from_bytes([4; 16]), BackendRef::node_default()),
            copy: Box::new(copy.clone()),
            public_key: [3; 32],
            token: shared(),
            realm_id: RealmId::from_bytes([1; 32]),
            node_id: SecretKey::from_bytes(&[2; 32]).public(),
        };
        let sealed = BlobEvent::TokenSealed {
            copies: vec![copy],
            token: shared(),
        };
        let canary = String::from_utf8_lossy(&CANARY).to_string();
        for formatted in [
            format!("{credential:?}"),
            format!("{admit:?}"),
            format!("{sealed:?}"),
            BucketKeyError::InvalidToken.to_string(),
        ] {
            assert!(!formatted.contains(&canary), "{formatted}");
            assert!(!formatted.contains(&*text), "{formatted}");
            assert!(!formatted.contains("99, 97, 110"), "{formatted}");
        }
    }
}
