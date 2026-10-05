//! This node's progress moving one bucket's stored copies to a new encryption setting or key
//! generation. Kept apart from compression migrations, so their stored rows keep decoding.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::errors::ConversionError;
use crate::structs::storage::blob::BackendLocation;
use crate::structs::storage::encryption::{BucketEncryption, BucketKeyRef, SealPlan};
use crate::structs::storage::format::{Compression, EncodingClass, StoredEncryption, StoredLayout};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// What a transition changes; the names are the portal's.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionKind {
    /// Plain copies become archives sealed with the public key only.
    Encrypt,
    /// Archives become plain copies; needs the source key.
    Decrypt,
    /// Archives get a new cipher, block-key mode or compression; needs the source key.
    Reencode,
    /// Archives keep their blocks and only get grants for the new key generation.
    Rotate,
}

/// Where a transition stands; the names are the portal's.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionState {
    Running,
    /// Copies are left that only the locked source generation opens.
    AwaitingKey,
    /// Every copy moved; old copies still wait for removal.
    Cleanup,
    Blocked,
    Finished,
}

/// The stored format every copy of the bucket moves to.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransitionTarget {
    pub compression: Compression,
    /// The seal plan of an encrypted target; none stores raw bytes or frames.
    pub plan: Option<SealPlan>,
}

/// One bucket's transition, keyed by bucket name in `encryption_transitions`. A newer change
/// replaces it; its queue row lives until the transition finishes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EncryptionTransition {
    pub kind: TransitionKind,
    pub state: TransitionState,
    /// The key generation old copies are sealed to; none when they are plain.
    pub source: Option<BucketKeyRef>,
    pub target: TransitionTarget,
    /// The bucket's storage generation this transition writes for; another one stops it.
    pub storage_generation: u64,
    /// Node vault entry of the source key, removed once no copy needs it.
    pub source_vault: Option<Ulid>,
    /// Last version key handled; the next batch resumes after it.
    pub cursor: Option<Vec<u8>>,
    /// Versions moved over all passes.
    pub done: u64,
    /// Versions the last pass left for a key that was locked.
    pub remaining: u64,
    /// Versions the current or last pass could not move.
    pub failed: u64,
    /// Old copies that still wait for removal.
    pub cleanup_remaining: u64,
    pub retries: u32,
    pub retry_at_ms: Option<u64>,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub blocked_reason: Option<String>,
}

impl EncryptionTransition {
    pub fn new(
        kind: TransitionKind,
        source: Option<BucketKeyRef>,
        target: TransitionTarget,
        storage_generation: u64,
        started_at_ms: u64,
    ) -> Self {
        Self {
            kind,
            state: TransitionState::Running,
            source,
            target,
            storage_generation,
            source_vault: None,
            cursor: None,
            done: 0,
            remaining: 0,
            failed: 0,
            cleanup_remaining: 0,
            retries: 0,
            retry_at_ms: None,
            started_at_ms,
            finished_at_ms: None,
            blocked_reason: None,
        }
    }

    /// The state to report: finished only when no copy, failure or cleanup is left.
    pub fn reported_state(&self) -> TransitionState {
        let settled = self.remaining == 0 && self.failed == 0 && self.cleanup_remaining == 0;
        match self.state {
            TransitionState::Finished if !settled => TransitionState::Blocked,
            state => state,
        }
    }

    /// Fails unless `settings` still write exactly this transition's target, so a copy made
    /// for an older plan is never published.
    pub fn still_current(&self, settings: &BucketEncryption) -> bool {
        let key = self.target.plan.map(|plan| plan.key);
        settings.storage_generation == self.storage_generation && settings.active_key() == key
    }

    /// Whether the stored format and captured generation still need this transition.
    pub fn needs(&self, location: &BackendLocation) -> bool {
        let format = &location.format;
        let Some(plan) = self.target.plan else {
            let wanted = EncodingClass::from(self.target.compression);
            return format.encryption != StoredEncryption::None || format.encoding() != wanted;
        };
        if format.bucket_key() != Some(plan.key) {
            return true;
        }
        matches!(&format.layout, StoredLayout::Pithos(layout)
            if layout.storage_generation != self.storage_generation)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Key of an old copy a transition still has to see removed: bucket name, a zero byte, then
/// the copy's location key. Bucket names never contain a zero byte.
pub fn cleanup_key(bucket: &str, location_key: &[u8]) -> Vec<u8> {
    [bucket.as_bytes(), &[0], location_key].concat()
}

/// Prefix of all cleanup keys of one bucket.
pub fn cleanup_prefix(bucket: &str) -> Vec<u8> {
    [bucket.as_bytes(), &[0]].concat()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::storage::encryption::{BlockCipher, BlockKeys, EncryptionMode};
    use crate::structs::storage::format::{PithosLayout, StoredFormat};
    use std::time::{Duration, UNIX_EPOCH};

    fn plan(generation: u64) -> SealPlan {
        SealPlan {
            key: BucketKeyRef::new(Ulid::from_bytes([1; 16]), generation),
            public_key: [2; 32],
            cipher: BlockCipher::default(),
            block_keys: BlockKeys::default(),
            storage_generation: 4,
        }
    }

    fn settings(generation: u64, storage: u64) -> BucketEncryption {
        BucketEncryption {
            mode: EncryptionMode::NodeManaged,
            bucket_id: Some(Ulid::from_bytes([1; 16])),
            key_generation: generation,
            storage_generation: storage,
            ..BucketEncryption::default()
        }
    }

    fn location(format: StoredFormat, written_ms: u64) -> BackendLocation {
        BackendLocation {
            backend: crate::structs::storage::blob::BackendRef::node_default(),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: String::new(),
            ulid: Ulid::from_bytes([3; 16]),
            format,
            created_by: crate::UserId::default(),
            created_at: UNIX_EPOCH + Duration::from_millis(written_ms),
            staging: false,
            partial: false,
            blob_size: 1,
            hashes: Default::default(),
        }
    }

    fn sealed(generation: u64) -> StoredFormat {
        let layout = PithosLayout {
            stored_size: 9,
            metadata_digest: [4; 32],
            storage_generation: 4,
        };
        StoredFormat::pithos(layout, plan(generation).key)
    }

    #[test]
    fn fences_the_target() {
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: Some(plan(2)),
        };
        let transition = EncryptionTransition::new(TransitionKind::Rotate, None, target, 4, 0);
        assert!(transition.still_current(&settings(2, 4)));
        assert!(!transition.still_current(&settings(2, 5)));
        assert!(!transition.still_current(&settings(3, 4)));
        let mut off = settings(2, 4);
        off.mode = EncryptionMode::Off;
        assert!(!transition.still_current(&off));
    }

    #[test]
    fn decides_needed_copies() {
        let source = Some(plan(1).key);
        let sealing = TransitionTarget {
            compression: Compression::Off,
            plan: Some(plan(2)),
        };
        let rotate = EncryptionTransition::new(TransitionKind::Rotate, source, sealing, 4, 50);
        assert!(rotate.needs(&location(sealed(1), 10)));
        assert!(!rotate.needs(&location(sealed(2), 10)));
        assert!(rotate.needs(&location(StoredFormat::default(), 10)));
        let reencode = EncryptionTransition::new(TransitionKind::Reencode, source, sealing, 4, 50);
        assert!(!reencode.needs(&location(sealed(2), 10)));
        assert!(!reencode.needs(&location(sealed(2), 60)));
        let plain = TransitionTarget {
            compression: Compression::Off,
            plan: None,
        };
        let decrypt = EncryptionTransition::new(TransitionKind::Decrypt, source, plain, 4, 50);
        assert!(decrypt.needs(&location(sealed(1), 10)));
        assert!(!decrypt.needs(&location(StoredFormat::default(), 10)));
    }

    #[test]
    fn generation_beats_timestamp() {
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: Some(plan(2)),
        };
        let transition =
            EncryptionTransition::new(TransitionKind::Reencode, Some(plan(2).key), target, 4, 50);
        for written_ms in [50, 60] {
            let mut old = location(sealed(2), written_ms);
            let StoredLayout::Pithos(layout) = &mut old.format.layout else {
                panic!("expected Pithos")
            };
            layout.storage_generation = 3;
            assert!(transition.needs(&old));
            let bytes = old.to_bytes().unwrap();
            assert_eq!(
                BackendLocation::from_bytes(&bytes)
                    .unwrap()
                    .to_bytes()
                    .unwrap(),
                bytes
            );
        }
    }

    #[test]
    fn finished_needs_settlement() {
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: None,
        };
        let mut transition = EncryptionTransition::new(TransitionKind::Decrypt, None, target, 1, 0);
        transition.state = TransitionState::Finished;
        assert_eq!(transition.reported_state(), TransitionState::Finished);
        transition.cleanup_remaining = 1;
        assert_eq!(transition.reported_state(), TransitionState::Blocked);
        let bytes = transition.to_bytes().unwrap();
        assert_eq!(
            EncryptionTransition::from_bytes(&bytes).unwrap(),
            transition
        );
    }
}
