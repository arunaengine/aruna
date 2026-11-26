//! Fixed 58-byte NFS filehandles: version, kind, flags, mount id, object id, bucket
//! incarnation and a truncated HMAC-SHA256 under a key derived from the node secret.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use ulid::Ulid;

use crate::backend::MountExport;
use crate::types::NfsStat4;

pub const HANDLE_SIZE: usize = 58;
const HANDLE_VERSION: u8 = 1;
const MAC_SIZE: usize = 14;
const BODY_SIZE: usize = HANDLE_SIZE - MAC_SIZE;
/// The derivation context; a handle key never equals another key of the node secret.
const KEY_CONTEXT: &str = "aruna-nfs-filehandle-v1";
/// Set on handles of immutable job views (`FH4_PERSISTENT`).
pub const PERSISTENT_FLAG: u16 = 0x0001;
/// Fileids of the two pseudo directories; export objects use their backend fileids.
pub const ROOT_FILEID: u64 = 1;
pub const MOUNTS_FILEID: u64 = 2;
/// The name of the one pseudo directory under `/`.
pub const MOUNTS_NAME: &str = "m";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FilehandleKind {
    /// `/`
    PseudoRoot = 1,
    /// `/m`, which never lists its entries.
    Mounts = 2,
    /// `/m/<mount_id>`
    ExportRoot = 3,
    Object = 4,
}

impl FilehandleKind {
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::PseudoRoot),
            2 => Some(Self::Mounts),
            3 => Some(Self::ExportRoot),
            4 => Some(Self::Object),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Filehandle {
    pub kind: FilehandleKind,
    pub flags: u16,
    pub mount_id: Ulid,
    pub object_id: u64,
    pub incarnation: u128,
}

impl Filehandle {
    pub fn pseudo_root() -> Self {
        Self::pseudo(FilehandleKind::PseudoRoot)
    }

    pub fn mounts() -> Self {
        Self::pseudo(FilehandleKind::Mounts)
    }

    fn pseudo(kind: FilehandleKind) -> Self {
        Self {
            kind,
            flags: PERSISTENT_FLAG,
            mount_id: Ulid::nil(),
            object_id: 0,
            incarnation: 0,
        }
    }

    /// The handle of `object_id` in `export`; the export root gets its own kind.
    pub fn object(export: &MountExport, object_id: u64) -> Self {
        let kind = if object_id == export.root {
            FilehandleKind::ExportRoot
        } else {
            FilehandleKind::Object
        };
        Self {
            kind,
            flags: if export.persistent {
                PERSISTENT_FLAG
            } else {
                0
            },
            mount_id: export.mount_id,
            object_id,
            incarnation: export.incarnation,
        }
    }

    pub fn is_persistent(&self) -> bool {
        self.flags & PERSISTENT_FLAG != 0
    }
}

/// The derived MAC key. It never prints.
#[derive(Clone)]
pub struct FilehandleKey {
    mac: Hmac<Sha256>,
}

impl std::fmt::Debug for FilehandleKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FilehandleKey(..)")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the filehandle key cannot be built")]
pub struct KeyError;

impl FilehandleKey {
    pub fn derive(node_secret: &[u8]) -> Result<Self, KeyError> {
        let key = blake3::derive_key(KEY_CONTEXT, node_secret);
        let mac = Hmac::<Sha256>::new_from_slice(&key).map_err(|_| KeyError)?;
        Ok(Self { mac })
    }

    pub fn seal(&self, handle: &Filehandle) -> [u8; HANDLE_SIZE] {
        let mut raw = [0u8; HANDLE_SIZE];
        raw[0] = HANDLE_VERSION;
        raw[1] = handle.kind as u8;
        raw[2..4].copy_from_slice(&handle.flags.to_be_bytes());
        raw[4..20].copy_from_slice(&handle.mount_id.to_bytes());
        raw[20..28].copy_from_slice(&handle.object_id.to_be_bytes());
        raw[28..44].copy_from_slice(&handle.incarnation.to_be_bytes());
        let tag = self.tag(&raw[..BODY_SIZE]);
        raw[BODY_SIZE..].copy_from_slice(&tag);
        raw
    }

    /// Checks size, version and MAC in constant time before reading any field, so a forged or
    /// damaged handle is BADHANDLE without a lookup.
    pub fn open(&self, raw: &[u8]) -> Result<Filehandle, NfsStat4> {
        if raw.len() != HANDLE_SIZE {
            return Err(NfsStat4::BadHandle);
        }
        let expected = self.tag(&raw[..BODY_SIZE]);
        if !bool::from(expected[..].ct_eq(&raw[BODY_SIZE..])) || raw[0] != HANDLE_VERSION {
            return Err(NfsStat4::BadHandle);
        }
        let kind = FilehandleKind::from_byte(raw[1]).ok_or(NfsStat4::BadHandle)?;
        let mut mount = [0u8; 16];
        mount.copy_from_slice(&raw[4..20]);
        let mut object = [0u8; 8];
        object.copy_from_slice(&raw[20..28]);
        let mut incarnation = [0u8; 16];
        incarnation.copy_from_slice(&raw[28..44]);
        Ok(Filehandle {
            kind,
            flags: u16::from_be_bytes([raw[2], raw[3]]),
            mount_id: Ulid::from_bytes(mount),
            object_id: u64::from_be_bytes(object),
            incarnation: u128::from_be_bytes(incarnation),
        })
    }

    fn tag(&self, body: &[u8]) -> [u8; MAC_SIZE] {
        let mut mac = self.mac.clone();
        mac.update(body);
        let digest = mac.finalize().into_bytes();
        let mut tag = [0u8; MAC_SIZE];
        tag.copy_from_slice(&digest[..MAC_SIZE]);
        tag
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn export(persistent: bool) -> MountExport {
        MountExport {
            mount_id: Ulid::from_parts(1, 42),
            host_id: Ulid::from_parts(1, 7),
            principal: aruna_core::user::id::UserId::nil(
                aruna_core::structs::identity::realm::RealmId([0; 32]),
            ),
            access_key: "key".to_string(),
            incarnation: 99,
            root: 1,
            read_only: false,
            persistent,
        }
    }

    #[test]
    fn seals_fixed_size() {
        let key = FilehandleKey::derive(&[7; 32]).expect("key");
        let handle = Filehandle::object(&export(false), 12);
        let raw = key.seal(&handle);
        assert_eq!(raw.len(), 58);
        assert_eq!(key.open(&raw), Ok(handle));
        assert_eq!(handle.kind, FilehandleKind::Object);
        assert!(!handle.is_persistent());
        let root = Filehandle::object(&export(true), 1);
        assert_eq!(root.kind, FilehandleKind::ExportRoot);
        assert!(root.is_persistent());
    }

    // Any flipped bit, a short handle or another node's key is BADHANDLE.
    #[test]
    fn rejects_forged_handles() {
        let key = FilehandleKey::derive(&[7; 32]).expect("key");
        let raw = key.seal(&Filehandle::object(&export(false), 12));
        for index in 0..HANDLE_SIZE {
            let mut forged = raw;
            forged[index] ^= 0x01;
            assert_eq!(key.open(&forged), Err(NfsStat4::BadHandle), "byte {index}");
        }
        assert_eq!(key.open(&raw[..57]), Err(NfsStat4::BadHandle));
        let other = FilehandleKey::derive(&[8; 32]).expect("key");
        assert_eq!(other.open(&raw), Err(NfsStat4::BadHandle));
    }

    #[test]
    fn pseudo_handles_distinct() {
        let key = FilehandleKey::derive(&[7; 32]).expect("key");
        let root = key.seal(&Filehandle::pseudo_root());
        let mounts = key.seal(&Filehandle::mounts());
        assert_ne!(root, mounts);
        assert_eq!(
            key.open(&root).map(|handle| handle.kind),
            Ok(FilehandleKind::PseudoRoot)
        );
        assert_eq!(
            key.open(&mounts).map(|handle| handle.kind),
            Ok(FilehandleKind::Mounts)
        );
    }
}
