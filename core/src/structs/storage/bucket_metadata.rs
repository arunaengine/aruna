//! Defines bucket metadata: what marks and describes a bucket as a database (D34), and the
//! release a version belongs to.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::structs::identity::identifier::{Identifier, IdentifierParser};
use crate::structs::storage::origin::OriginVersion;

/// Metadata of one bucket. The owner is the bucket's group, read from the bucket itself.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BucketMetadata {
    /// Show the bucket as a database; the server treats it as any bucket.
    pub database: bool,
    pub title: Option<String>,
    pub description: Option<String>,
    pub version: Option<String>,
    /// A license URL or SPDX id.
    pub license: Option<String>,
    /// DOI and other identifiers of the bucket.
    pub identifiers: Vec<Identifier>,
    /// Identifier rules applied to keys and source URLs.
    pub parser: Option<IdentifierParser>,
    /// The dataset document that describes the bucket, if any.
    pub dataset: Option<Ulid>,
}

/// A release a version is a member of: the release crate's exact version and its label.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseRef {
    pub crate_version: OriginVersion,
    pub label: String,
    pub upstream: Vec<Identifier>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::identity::identifier::IdentifierTarget;
    use crate::structs::identity::realm::RealmId;
    use crate::structs::storage::global_id::GlobalId;

    #[test]
    fn metadata_round_trip() {
        let identifier = Identifier {
            name: "DOI".to_string(),
            value: "10.5281/zenodo.1".to_string(),
            url: "https://doi.org/10.5281/zenodo.1".to_string(),
            target: IdentifierTarget::Key,
        };
        let metadata = BucketMetadata {
            database: true,
            title: Some("Bakta DB".to_string()),
            version: Some("6.0".to_string()),
            identifiers: vec![identifier.clone()],
            dataset: Some(Ulid::from_bytes([2; 16])),
            ..BucketMetadata::default()
        };
        let release = ReleaseRef {
            crate_version: OriginVersion::Global(GlobalId::Version {
                realm_id: RealmId::from_bytes([1; 32]),
                version_id: Ulid::from_bytes([3; 16]),
            }),
            label: "6.0-light".to_string(),
            upstream: vec![identifier],
        };
        let bytes = postcard::to_allocvec(&(metadata.clone(), release.clone())).unwrap();
        let restored: (BucketMetadata, ReleaseRef) = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(restored, (metadata, release));
    }
}
