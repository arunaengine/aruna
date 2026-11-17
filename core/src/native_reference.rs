//! Frames of the native reference protocol, which reads one object version from another node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::ops::Range;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::errors::StagingSourceError;
use crate::structs::execution::source_access::{ResolvedSourceAccess, SourceMetadata};
use crate::structs::execution::source_connector::SourceConnectorKind;
use crate::structs::storage::distribution::delegation::SignedDelegation;
use crate::structs::storage::read_grant::SignedRequest;
use crate::types::GroupId;

/// Connector setting that names the sync relationship a native source reads through.
pub const NATIVE_RELATIONSHIP_ID: &str = "relationship_id";
/// Connector setting that names the node holding the source bytes.
pub const ORIGIN_NODE_ID: &str = "origin_node_id";
/// Largest request or response header frame in bytes.
pub const MAX_HEADER_SIZE: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativeReferenceRequest {
    pub authority: NativeAuthority,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub head: bool,
    pub range: Option<NativeRange>,
}

/// On whose behalf a native read runs: a sync relationship's creator, or a caller a read grant
/// of this node names, presented by the reading node in one signed request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NativeAuthority {
    Relationship(Ulid),
    Granted(Box<SignedRequest>),
    /// A caller the reading node verified itself, for one request bound by a D33 delegation.
    Delegated(Box<SignedDelegation>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativeRange {
    pub start: u64,
    pub end: u64,
}

impl NativeRange {
    /// The wire range of a requested byte range; an empty or reversed range is refused.
    pub fn checked(range: Option<Range<u64>>) -> Result<Option<Self>, StagingSourceError> {
        range
            .map(|range| {
                (range.start < range.end)
                    .then_some(NativeRange {
                        start: range.start,
                        end: range.end,
                    })
                    .ok_or_else(|| {
                        StagingSourceError::ReadError("invalid source range".to_string())
                    })
            })
            .transpose()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NativeReferenceResponse {
    Content(SourceMetadata),
    Rejected(NativeReferenceReject),
    /// A caller read's answer, with the group of the bucket that holds the version.
    Described {
        metadata: SourceMetadata,
        group_id: GroupId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NativeReferenceReject {
    AccessDenied,
    NotFound,
    Unavailable(String),
}

impl From<NativeReferenceReject> for StagingSourceError {
    fn from(reject: NativeReferenceReject) -> Self {
        match reject {
            NativeReferenceReject::AccessDenied => StagingSourceError::AccessDenied,
            NativeReferenceReject::NotFound => StagingSourceError::NotFound,
            NativeReferenceReject::Unavailable(message) => StagingSourceError::ReadError(message),
        }
    }
}

/// Whether a resolved source reads through another node's native reference protocol.
pub fn is_native(access: &ResolvedSourceAccess) -> bool {
    matches!(
        access,
        ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::ArunaNative,
            ..
        }
    )
}

/// The origin node and the request a sync relationship's native source sends there.
pub fn relationship_request(
    access: &ResolvedSourceAccess,
    range: Option<Range<u64>>,
    head: bool,
) -> Result<(NodeId, NativeReferenceRequest), StagingSourceError> {
    let ResolvedSourceAccess::OpenDal {
        kind: SourceConnectorKind::ArunaNative,
        config,
        path,
        version: Some(version),
    } = access
    else {
        return Err(StagingSourceError::UnsupportedKind(
            "invalid ArunaNative access".to_string(),
        ));
    };
    let origin_node_id = config
        .get(ORIGIN_NODE_ID)
        .and_then(|value| NodeId::from_str(value).ok())
        .ok_or_else(|| StagingSourceError::ReadError("missing origin node".to_string()))?;
    let relationship_id = config
        .get(NATIVE_RELATIONSHIP_ID)
        .and_then(|value| Ulid::from_string(value).ok())
        .ok_or_else(|| StagingSourceError::ReadError("missing relationship id".to_string()))?;
    let version_id = Ulid::from_string(version)
        .map_err(|_| StagingSourceError::ReadError("invalid source version".to_string()))?;
    let (bucket, key) = path
        .split_once('/')
        .filter(|(bucket, key)| !bucket.is_empty() && !key.is_empty())
        .ok_or_else(|| StagingSourceError::ReadError("invalid source path".to_string()))?;
    Ok((
        origin_node_id,
        NativeReferenceRequest {
            authority: NativeAuthority::Relationship(relationship_id),
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
            head,
            range: NativeRange::checked(range)?,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrips() {
        let request = NativeReferenceRequest {
            authority: NativeAuthority::Relationship(Ulid::from_bytes([1u8; 16])),
            bucket: "source".to_string(),
            key: "nested/data.txt".to_string(),
            version_id: Ulid::from_bytes([2u8; 16]),
            head: false,
            range: Some(NativeRange { start: 3, end: 9 }),
        };
        let bytes = postcard::to_allocvec(&request).unwrap();
        assert_eq!(
            postcard::from_bytes::<NativeReferenceRequest>(&bytes).unwrap(),
            request
        );
    }

    #[test]
    fn response_roundtrips() {
        let response = NativeReferenceResponse::Content(SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: Some("etag".to_string()),
            last_modified: Some(std::time::SystemTime::UNIX_EPOCH),
            source_version: None,
            digests: Default::default(),
        });
        let bytes = postcard::to_allocvec(&response).unwrap();
        assert_eq!(
            postcard::from_bytes::<NativeReferenceResponse>(&bytes).unwrap(),
            response
        );
    }

    #[test]
    fn rejects_empty_range() {
        assert!(NativeRange::checked(Some(4..4)).is_err());
        assert_eq!(
            NativeRange::checked(Some(1..2)).unwrap(),
            Some(NativeRange { start: 1, end: 2 })
        );
        assert_eq!(NativeRange::checked(None).unwrap(), None);
    }
}
