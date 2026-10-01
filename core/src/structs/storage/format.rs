//! Describes how one stored copy keeps its bytes on a backend: its layout and encryption.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::errors::ConversionError;
use serde::{Deserialize, Serialize};

/// How one copy stores its bytes. Its raw default encodes like the two `false`
/// flags it replaced, so older location rows decode unchanged.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredFormat {
    pub layout: StoredLayout,
    pub encryption: StoredEncryption,
}

/// Arrangement of the stored bytes.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum StoredLayout {
    /// The original bytes, unchanged.
    #[default]
    Raw,
}

/// Encryption of the stored bytes. No variant encrypts yet.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum StoredEncryption {
    #[default]
    None,
}

impl StoredFormat {
    /// The class this copy shares physical bytes within.
    pub fn encoding(&self) -> EncodingClass {
        match self.layout {
            StoredLayout::Raw => EncodingClass::Raw,
        }
    }
}

/// Copies of one hash on one backend share bytes only within one class, so
/// buckets with different settings never share a physical copy.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum EncodingClass {
    Raw,
}

impl EncodingClass {
    /// Bytes placed between hash and backend in a location key. Raw adds none,
    /// so raw keys keep their shape; backend keys start with `n:` or `g:`.
    pub fn key_bytes(&self) -> Vec<u8> {
        match self {
            Self::Raw => Vec::new(),
        }
    }

    /// Reads a class written by `key_bytes`.
    pub fn from_key_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        match bytes {
            [] => Ok(Self::Raw),
            _ => Err(ConversionError::InvalidLength(
                "unknown encoding class in key".to_string(),
            )),
        }
    }

    /// Splits the class from the backend part of a location key.
    pub fn split_key(bytes: &[u8]) -> Result<(Self, &[u8]), ConversionError> {
        match bytes.first() {
            Some(b'n' | b'g') => Ok((Self::Raw, bytes)),
            _ => Err(ConversionError::InvalidLength(
                "unknown encoding class in location key".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StoredFormat;

    #[test]
    fn raw_matches_flags() {
        let flags = postcard::to_allocvec(&(false, false)).unwrap();
        let format = postcard::to_allocvec(&StoredFormat::default()).unwrap();

        assert_eq!(format, flags);
    }
}
