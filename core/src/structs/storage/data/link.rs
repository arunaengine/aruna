//! Names the Aruna data a metadata document links to, in the one canonical form its link
//! postings are keyed by: URL, short path and ARN spellings of one identifier share a key.
//! Prefix links keep their prefix symbolic, so a bucket link never expands per file.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::id::NodeId;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::global_id::{GlobalId, MAX_PREFIX_BYTES};
use crate::structs::storage::replication::{
    ArunaArn, ArunaArnType, VersionedObjectArn, W3idIdentifier,
};

/// At most this many link postings per document; a larger document is marked truncated.
pub const MAX_DOCUMENT_LINKS: usize = 65_536;
/// At most this many subject IRIs per posting.
pub const MAX_LINK_SUBJECTS: usize = 8;
/// A bucket name is at most this long in a posting key.
pub const MAX_BUCKET_BYTES: usize = 255;

const VERSION_TAG: u8 = 1;
const HASH_TAG: u8 = 2;
const SET_TAG: u8 = 3;
const NODE_TAG: u8 = 4;
const SET_VERSION_TAG: u8 = 5;
const UNVERIFIED_TAG: u8 = 6;
const TRUNCATED_TAG: u8 = 7;

/// One Aruna data identifier a document names.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum DataLink {
    /// A distributed bucket's object version, by its global id.
    Version { realm_id: RealmId, version_id: Ulid },
    /// Content by its BLAKE3 digest; it names no set, so set deletion cannot see it.
    Hash([u8; 32]),
    /// A distributed bucket root (empty prefix) or a prefix in it.
    SetPrefix {
        realm_id: RealmId,
        set_id: Ulid,
        prefix: String,
    },
    /// An ordinary bucket root (empty prefix) or a prefix in it, from a bucket-prefix ARN.
    NodePrefix {
        realm_id: RealmId,
        node_id: NodeId,
        bucket: String,
        prefix: String,
    },
    /// Derived, never parsed: a version link verified in its set on this node, so the set's
    /// deletion preview finds it by set.
    SetVersion {
        realm_id: RealmId,
        set_id: Ulid,
        version_id: Ulid,
    },
    /// Derived, never parsed: a version link no local member could verify yet.
    Unverified { realm_id: RealmId, version_id: Ulid },
    /// Derived, never parsed: the document names more links than one projection keeps.
    Truncated,
}

/// The key all derived set-version postings of one set start with.
pub fn set_versions_key(set_id: Ulid) -> Vec<u8> {
    let mut key = vec![SET_VERSION_TAG];
    key.extend_from_slice(&set_id.to_bytes());
    key
}

/// The key the truncation marker of every truncated document starts with.
pub fn truncated_key() -> Vec<u8> {
    vec![TRUNCATED_TAG]
}

/// The key all unverified version postings start with.
pub fn unverified_key() -> Vec<u8> {
    vec![UNVERIFIED_TAG]
}

/// The bucket a prefix link lives in: postings of one scope sort together by prefix length.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum LinkScope {
    Set {
        realm_id: RealmId,
        set_id: Ulid,
    },
    Node {
        realm_id: RealmId,
        node_id: NodeId,
        bucket: String,
    },
}

/// What one posting row stores about one document's link.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LinkPosting {
    /// The document event the projection was made from; older rows are fenced.
    pub cursor: Ulid,
    /// A Dataset-typed `hasPart` of the root dataset: a linked part whose files belong to it.
    pub part: bool,
    /// Entities that name the identifier, at most [`MAX_LINK_SUBJECTS`].
    pub subjects: Vec<String>,
}

impl LinkPosting {
    pub fn to_bytes(&self) -> Result<Vec<u8>, crate::errors::ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, crate::errors::ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

impl DataLink {
    /// Reads one identifier value: content w3id or hash ARN, global id URL or short path,
    /// bucket-prefix ARN. Versioned node ARNs and anything else name no link here.
    pub fn parse(value: &str) -> Option<Self> {
        if let Ok(identifier) = W3idIdentifier::parse(value) {
            return match identifier {
                W3idIdentifier::ContentHash(hash) => Some(Self::Hash(hash)),
                W3idIdentifier::Global(global) => Some(Self::from_global(global)),
                W3idIdentifier::VersionedObject(_) => None,
            };
        }
        if value.starts_with('/') {
            return GlobalId::parse(value).ok().map(Self::from_global);
        }
        if VersionedObjectArn::parse(value).is_ok() {
            return None;
        }
        let arn = ArunaArn::parse(value).ok()?;
        match arn.resource_type {
            ArunaArnType::ContentHash => {
                crate::structs::storage::data::identity::parse_hash(&arn.path).map(Self::Hash)
            }
            ArunaArnType::S3 => {
                let bucket = arn.bucket()?.to_string();
                let prefix = arn.key_prefix().unwrap_or_default().to_string();
                (bucket.len() <= MAX_BUCKET_BYTES && prefix.len() <= MAX_PREFIX_BYTES).then_some(
                    Self::NodePrefix {
                        realm_id: arn.realm_id,
                        node_id: arn.node_id,
                        bucket,
                        prefix,
                    },
                )
            }
        }
    }

    pub fn from_global(global: GlobalId) -> Self {
        match global {
            GlobalId::Version {
                realm_id,
                version_id,
            } => Self::Version {
                realm_id,
                version_id,
            },
            GlobalId::Set {
                realm_id,
                set_id,
                prefix,
            } => Self::SetPrefix {
                realm_id,
                set_id,
                prefix,
            },
        }
    }

    /// The scope and prefix of a prefix link; `None` for versions and hashes.
    pub fn scope(&self) -> Option<(LinkScope, &str)> {
        match self {
            Self::SetPrefix {
                realm_id,
                set_id,
                prefix,
            } => Some((
                LinkScope::Set {
                    realm_id: *realm_id,
                    set_id: *set_id,
                },
                prefix,
            )),
            Self::NodePrefix {
                realm_id,
                node_id,
                bucket,
                prefix,
            } => Some((
                LinkScope::Node {
                    realm_id: *realm_id,
                    node_id: *node_id,
                    bucket: bucket.clone(),
                },
                prefix,
            )),
            Self::Version { .. }
            | Self::Hash(_)
            | Self::SetVersion { .. }
            | Self::Unverified { .. }
            | Self::Truncated => None,
        }
    }

    /// The identifier as datasets write it: the URL form, or the ARN of an ordinary prefix.
    pub fn to_id(&self) -> String {
        match self {
            Self::Version {
                realm_id,
                version_id,
            }
            | Self::SetVersion {
                realm_id,
                version_id,
                ..
            }
            | Self::Unverified {
                realm_id,
                version_id,
            } => GlobalId::Version {
                realm_id: *realm_id,
                version_id: *version_id,
            }
            .to_url(),
            Self::Hash(hash) => W3idIdentifier::ContentHash(*hash).to_w3id(),
            Self::SetPrefix {
                realm_id,
                set_id,
                prefix,
            } => GlobalId::Set {
                realm_id: *realm_id,
                set_id: *set_id,
                prefix: prefix.clone(),
            }
            .to_url(),
            Self::NodePrefix {
                realm_id,
                node_id,
                bucket,
                prefix,
            } => format!("arn:aruna:{realm_id}:{node_id}:s3/{bucket}/{prefix}"),
            Self::Truncated => String::new(),
        }
    }

    /// The posting key without the document: postings of one identifier share it.
    pub fn key(&self) -> Vec<u8> {
        match self {
            Self::Version {
                realm_id,
                version_id,
            } => {
                let mut key = vec![VERSION_TAG];
                key.extend_from_slice(realm_id.as_bytes());
                key.extend_from_slice(&version_id.to_bytes());
                key
            }
            Self::Hash(hash) => {
                let mut key = vec![HASH_TAG];
                key.extend_from_slice(hash);
                key
            }
            Self::SetVersion {
                set_id, version_id, ..
            } => {
                let mut key = set_versions_key(*set_id);
                key.extend_from_slice(&version_id.to_bytes());
                key
            }
            Self::Unverified {
                realm_id,
                version_id,
            } => {
                let mut key = unverified_key();
                key.extend_from_slice(realm_id.as_bytes());
                key.extend_from_slice(&version_id.to_bytes());
                key
            }
            Self::Truncated => truncated_key(),
            Self::SetPrefix { .. } | Self::NodePrefix { .. } => {
                let Some((scope, prefix)) = self.scope() else {
                    return Vec::new();
                };
                scope.prefix_key(prefix)
            }
        }
    }
}

impl LinkScope {
    /// The key all prefix postings of this bucket start with.
    pub fn key(&self) -> Vec<u8> {
        match self {
            Self::Set { realm_id, set_id } => {
                let mut key = vec![SET_TAG];
                key.extend_from_slice(realm_id.as_bytes());
                key.extend_from_slice(&set_id.to_bytes());
                key
            }
            Self::Node {
                realm_id,
                node_id,
                bucket,
            } => {
                let mut key = vec![NODE_TAG];
                key.extend_from_slice(realm_id.as_bytes());
                key.extend_from_slice(node_id.as_bytes());
                key.extend_from_slice(&(bucket.len() as u16).to_be_bytes());
                key.extend_from_slice(bucket.as_bytes());
                key
            }
        }
    }

    /// The key of one exact prefix: the length first, so `ab` never matches `abc`.
    pub fn prefix_key(&self, prefix: &str) -> Vec<u8> {
        let mut key = self.key();
        key.extend_from_slice(&(prefix.len() as u16).to_be_bytes());
        key.extend_from_slice(prefix.as_bytes());
        key
    }

    /// Where postings of prefixes at least `length` bytes long start.
    pub fn length_start(&self, length: u16) -> Vec<u8> {
        let mut key = self.key();
        key.extend_from_slice(&length.to_be_bytes());
        key
    }

    /// The prefix length of a posting key of this scope.
    pub fn length_of(&self, posting_key: &[u8]) -> Option<u16> {
        let start = self.key().len();
        let bytes = posting_key.get(start..start + 2)?;
        Some(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    pub fn link(&self, prefix: &str) -> DataLink {
        match self {
            Self::Set { realm_id, set_id } => DataLink::SetPrefix {
                realm_id: *realm_id,
                set_id: *set_id,
                prefix: prefix.to_string(),
            },
            Self::Node {
                realm_id,
                node_id,
                bucket,
            } => DataLink::NodePrefix {
                realm_id: *realm_id,
                node_id: *node_id,
                bucket: bucket.clone(),
                prefix: prefix.to_string(),
            },
        }
    }
}

/// One posting row key: the identifier, then the document.
pub fn posting_key(link_key: &[u8], document_id: Ulid) -> Vec<u8> {
    let mut key = link_key.to_vec();
    key.extend_from_slice(&document_id.to_bytes());
    key
}

/// The document a posting row key names: its last 16 bytes.
pub fn posting_document(posting_key: &[u8]) -> Option<Ulid> {
    let start = posting_key.len().checked_sub(16)?;
    let bytes: [u8; 16] = posting_key.get(start..)?.try_into().ok()?;
    Some(Ulid::from_bytes(bytes))
}

/// The per-document row that lets a materialization replace exactly its own postings.
pub fn document_key(document_id: Ulid, posting_key: &[u8]) -> Vec<u8> {
    let mut key = document_id.to_bytes().to_vec();
    key.extend_from_slice(posting_key);
    key
}

#[cfg(test)]
#[path = "link_tests.rs"]
mod tests;
