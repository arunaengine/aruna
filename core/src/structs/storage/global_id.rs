//! Parses and formats the global ids of distributed buckets: object versions, buckets and prefixes.
//! The path forms name no node, so every member and every realm node resolves the same id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::structs::identity::realm::RealmId;

/// The w3id base of the URL form; the path form follows it unchanged.
pub const GLOBAL_URL_BASE: &str = "https://w3id.org/aruna/data";
/// An encoded prefix names at most one S3 key's worth of bytes.
pub const MAX_PREFIX_BYTES: usize = 1024;

const REALM_SEGMENT_LEN: usize = 43;
const ULID_SEGMENT_LEN: usize = 26;

/// A node-free name of a distributed bucket's version, bucket or prefix.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum GlobalId {
    /// `/{realm-id}/{version-id}`: one object version, shared by every member.
    Version { realm_id: RealmId, version_id: Ulid },
    /// `/{realm-id}/{set-id}/{encoded-prefix}`; an empty prefix names the whole bucket.
    Set {
        realm_id: RealmId,
        set_id: Ulid,
        prefix: String,
    },
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum GlobalIdError {
    #[error("global id must start with `/`")]
    MissingSlash,
    #[error("global id URL must start with `{GLOBAL_URL_BASE}/`")]
    NotGlobalUrl,
    #[error("global id must have a realm and a version, or a realm, a set and a prefix")]
    InvalidShape,
    #[error("global id realm segment is not a canonical realm id")]
    InvalidRealm,
    #[error("global id version or set segment is not a canonical ULID")]
    InvalidUlid,
    #[error("global id prefix is not canonical URL-safe Base64 of UTF-8 bytes")]
    InvalidPrefix,
    #[error("global id prefix exceeds {MAX_PREFIX_BYTES} bytes")]
    PrefixTooLong,
}

impl GlobalId {
    /// Parses the short path form. Two segments without a trailing slash are a version;
    /// a slash after the second segment starts the set form. Nothing else is accepted.
    pub fn parse(input: &str) -> Result<Self, GlobalIdError> {
        let path = input.strip_prefix('/').ok_or(GlobalIdError::MissingSlash)?;
        let segments: Vec<&str> = path.split('/').collect();
        match segments.as_slice() {
            [realm, version] => Ok(Self::Version {
                realm_id: parse_realm(realm)?,
                version_id: parse_ulid(version)?,
            }),
            [realm, set, prefix] => Ok(Self::Set {
                realm_id: parse_realm(realm)?,
                set_id: parse_ulid(set)?,
                prefix: decode_prefix(prefix)?,
            }),
            _ => Err(GlobalIdError::InvalidShape),
        }
    }

    /// Parses the URL form `https://w3id.org/aruna/data/...` with the same rules.
    pub fn parse_url(input: &str) -> Result<Self, GlobalIdError> {
        let path = input
            .strip_prefix(GLOBAL_URL_BASE)
            .filter(|path| path.starts_with('/'))
            .ok_or(GlobalIdError::NotGlobalUrl)?;
        Self::parse(path)
    }

    pub fn realm_id(&self) -> RealmId {
        match self {
            Self::Version { realm_id, .. } | Self::Set { realm_id, .. } => *realm_id,
        }
    }

    pub fn to_url(&self) -> String {
        format!("{GLOBAL_URL_BASE}{self}")
    }
}

impl fmt::Display for GlobalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Version {
                realm_id,
                version_id,
            } => write!(f, "/{realm_id}/{version_id}"),
            Self::Set {
                realm_id,
                set_id,
                prefix,
            } => write!(
                f,
                "/{realm_id}/{set_id}/{}",
                URL_SAFE_NO_PAD.encode(prefix.as_bytes())
            ),
        }
    }
}

fn parse_realm(segment: &str) -> Result<RealmId, GlobalIdError> {
    if segment.len() != REALM_SEGMENT_LEN {
        return Err(GlobalIdError::InvalidRealm);
    }
    let realm_id = RealmId::from_base64(segment).map_err(|_| GlobalIdError::InvalidRealm)?;
    (realm_id.to_base64() == segment)
        .then_some(realm_id)
        .ok_or(GlobalIdError::InvalidRealm)
}

fn parse_ulid(segment: &str) -> Result<Ulid, GlobalIdError> {
    if segment.len() != ULID_SEGMENT_LEN {
        return Err(GlobalIdError::InvalidUlid);
    }
    let id = Ulid::from_string(segment).map_err(|_| GlobalIdError::InvalidUlid)?;
    (id.to_string() == segment)
        .then_some(id)
        .ok_or(GlobalIdError::InvalidUlid)
}

fn decode_prefix(segment: &str) -> Result<String, GlobalIdError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(segment)
        .map_err(|_| GlobalIdError::InvalidPrefix)?;
    if bytes.len() > MAX_PREFIX_BYTES {
        return Err(GlobalIdError::PrefixTooLong);
    }
    if URL_SAFE_NO_PAD.encode(&bytes) != segment {
        return Err(GlobalIdError::InvalidPrefix);
    }
    String::from_utf8(bytes).map_err(|_| GlobalIdError::InvalidPrefix)
}

#[cfg(test)]
mod tests {
    use super::{GLOBAL_URL_BASE, GlobalId, GlobalIdError, MAX_PREFIX_BYTES};
    use crate::structs::identity::realm::RealmId;
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use ulid::Ulid;

    fn realm() -> RealmId {
        RealmId::from_bytes([251; 32])
    }

    fn id(seed: u8) -> Ulid {
        Ulid::from_bytes([seed; 16])
    }

    fn set(prefix: &str) -> GlobalId {
        GlobalId::Set {
            realm_id: realm(),
            set_id: id(2),
            prefix: prefix.to_string(),
        }
    }

    #[test]
    fn version_roundtrips() {
        let version = GlobalId::Version {
            realm_id: realm(),
            version_id: id(1),
        };
        let path = format!("/{}/{}", realm(), id(1));
        assert_eq!(version.to_string(), path);
        assert_eq!(GlobalId::parse(&path), Ok(version.clone()));
        assert_eq!(version.to_url(), format!("{GLOBAL_URL_BASE}{path}"));
        assert_eq!(GlobalId::parse_url(&version.to_url()), Ok(version));
    }

    #[test]
    fn bucket_keeps_slash() {
        let path = format!("/{}/{}/", realm(), id(2));
        assert_eq!(GlobalId::parse(&path), Ok(set("")));
        assert_eq!(set("").to_string(), path);
        // Without the slash the same segments name a version, never a bucket.
        let version = GlobalId::parse(path.trim_end_matches('/')).unwrap();
        assert!(matches!(version, GlobalId::Version { .. }));
    }

    #[test]
    fn prefixes_roundtrip_losslessly() {
        for prefix in [
            "a/b/", ".", "..", "%2E", "a/../b", "x@y", "/", "data/", "ü/ß",
        ] {
            let global = set(prefix);
            let text = global.to_string();
            let encoded = text.rsplit('/').next().unwrap();
            assert!(
                !encoded.contains(['/', '.', '%', '@']),
                "{prefix}: {encoded}"
            );
            assert_eq!(GlobalId::parse(&text), Ok(global.clone()), "{prefix}");
            assert_eq!(
                GlobalId::parse_url(&global.to_url()),
                Ok(global),
                "{prefix}"
            );
        }
    }

    #[test]
    fn rejects_malformed_forms() {
        let realm = realm().to_string();
        let ulid = id(3).to_string();
        let encoded = URL_SAFE_NO_PAD.encode(b"a/");
        let cases = [
            (format!("{realm}/{ulid}"), GlobalIdError::MissingSlash),
            (format!("/{realm}"), GlobalIdError::InvalidShape),
            (format!("/{realm}/"), GlobalIdError::InvalidUlid),
            (
                format!("/{realm}/{ulid}/{encoded}/"),
                GlobalIdError::InvalidShape,
            ),
            (format!("/{realm}/{ulid}//"), GlobalIdError::InvalidShape),
            (format!("//{realm}/{ulid}"), GlobalIdError::InvalidRealm),
            (
                format!("/{realm}/{}", ulid.to_lowercase()),
                GlobalIdError::InvalidUlid,
            ),
            (format!("/{realm}=/{ulid}"), GlobalIdError::InvalidRealm),
            (
                format!("/{}/{ulid}", "a".repeat(43)),
                GlobalIdError::InvalidRealm,
            ),
            (
                format!("/{realm}/{ulid}/{encoded}="),
                GlobalIdError::InvalidPrefix,
            ),
            (format!("/{realm}/{ulid}/a.b"), GlobalIdError::InvalidPrefix),
            (format!("/{realm}/{ulid}/%2E"), GlobalIdError::InvalidPrefix),
            (format!("/{realm}/{ulid}/YR"), GlobalIdError::InvalidPrefix),
            (
                format!("/{realm}/{ulid}/{}", URL_SAFE_NO_PAD.encode([0xff])),
                GlobalIdError::InvalidPrefix,
            ),
            (
                format!("/{realm}/{}", "0".repeat(64)),
                GlobalIdError::InvalidUlid,
            ),
            (
                format!("/arn:aruna:{realm}/{ulid}"),
                GlobalIdError::InvalidRealm,
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(GlobalId::parse(&input), Err(expected), "{input}");
        }
        let long = URL_SAFE_NO_PAD.encode(vec![b'a'; MAX_PREFIX_BYTES + 1]);
        assert_eq!(
            GlobalId::parse(&format!("/{realm}/{ulid}/{long}")),
            Err(GlobalIdError::PrefixTooLong)
        );
    }

    #[test]
    fn url_needs_base() {
        let path = format!("/{}/{}", realm(), id(1));
        assert_eq!(
            GlobalId::parse_url(&format!("https://w3id.org/aruna/datax{path}")),
            Err(GlobalIdError::NotGlobalUrl)
        );
        assert_eq!(
            GlobalId::parse_url(&format!("http://w3id.org/aruna/data{path}")),
            Err(GlobalIdError::NotGlobalUrl)
        );
        assert_eq!(GlobalId::parse_url(&path), Err(GlobalIdError::NotGlobalUrl));
    }
}
