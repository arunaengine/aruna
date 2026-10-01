//! Describes how one stored copy keeps its bytes on a backend: its layout and encryption.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

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
