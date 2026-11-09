//! Describes a plugin version published to the realm: its artifact digest, size and manifest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};

use crate::UserId;

/// Largest artifact a realm admin may publish; it travels whole in one node message.
pub const MAX_ARTIFACT: usize = 4 << 20;
pub const MAX_MANIFEST: usize = 64 * 1024;
const MAX_NAME: usize = 64;

/// One published plugin version. Management nodes keep its artifact; every other
/// node fetches the artifact by digest and checks it against this entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedPlugin {
    pub name: String,
    pub version: String,
    /// `sha256:<64 lowercase hex>` of the exact artifact bytes.
    pub digest: String,
    pub size: u64,
    /// The artifact's checked manifest as JSON, so nodes can read it without the artifact.
    pub manifest: String,
    pub published_by: UserId,
    pub published_at_ms: u64,
}

impl PublishedPlugin {
    /// Whether the entry is well formed. The publishing management node checked the artifact.
    pub fn valid(&self) -> bool {
        let lower = |byte: u8| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte);
        let digest = self
            .digest
            .strip_prefix("sha256:")
            .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(lower));
        let size = usize::try_from(self.size).is_ok_and(|size| (1..=MAX_ARTIFACT).contains(&size));
        let manifest = (1..=MAX_MANIFEST).contains(&self.manifest.len())
            && serde_json::from_str::<serde_json::Value>(&self.manifest)
                .is_ok_and(|value| value.is_object());
        valid_name(&self.name) && valid_version(&self.version) && digest && size && manifest
    }
}

/// A plugin name: 1 to 64 of `a-z`, `0-9` or `-`, starting with a letter, no `-` at the end.
pub fn valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    (1..=MAX_NAME).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && !name.ends_with('-')
        && (bytes.iter()).all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

/// A version: 1 to 64 of ASCII letters, digits, `.`, `-` or `+`; the manifest checked its form.
pub fn valid_version(version: &str) -> bool {
    (1..=MAX_NAME).contains(&version.len())
        && (version.bytes()).all(|b| b.is_ascii_alphanumeric() || b"-+.".contains(&b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::identity::realm::RealmId;

    fn entry() -> PublishedPlugin {
        PublishedPlugin {
            name: "dataset-announcer".into(),
            version: "0.1.0".into(),
            digest: format!("sha256:{}", "ab".repeat(32)),
            size: 1024,
            manifest: r#"{"name":"dataset-announcer"}"#.into(),
            published_by: UserId::nil(RealmId::from_bytes([1u8; 32])),
            published_at_ms: 1,
        }
    }

    #[test]
    fn entry_shape() {
        assert!(entry().valid());
        let cases: [fn(&mut PublishedPlugin); 7] = [
            |e| e.name = "Announcer".into(),
            |e| e.version = "1.0 beta".into(),
            |e| e.digest = format!("sha256:{}", "AB".repeat(32)),
            |e| e.size = 0,
            |e| e.size = MAX_ARTIFACT as u64 + 1,
            |e| e.manifest = "[]".into(),
            |e| e.manifest = "{".into(),
        ];
        for edit in cases {
            let mut invalid = entry();
            edit(&mut invalid);
            assert!(!invalid.valid(), "{invalid:?}");
        }
    }
}
