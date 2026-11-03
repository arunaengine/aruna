//! Defines bucket metadata: what marks and describes a bucket as a database (D34), and the
//! release a version belongs to.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::errors::ConversionError;
use crate::structs::identity::identifier::{Identifier, IdentifierParser, validate_parser};
use crate::structs::storage::dataset_description::DatasetDescription;
use crate::structs::storage::origin::OriginVersion;

/// Metadata of one bucket. The owner is the bucket's group, read from the bucket itself. Title,
/// description, license, identifiers and version live only in the linked dataset.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct BucketMetadata {
    /// Show the bucket as a database; the server treats it as any bucket.
    pub database: bool,
    /// Identifier rules applied to keys and source URLs.
    pub parser: Option<IdentifierParser>,
    /// The dataset document that describes the bucket, if any.
    pub dataset: Option<Ulid>,
}

/// A database input at admission: the bucket's metadata and its dataset's description then. Job
/// records store it, and its byte layout is the one they stored before the description moved.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapturedDatabase {
    pub database: bool,
    pub described: DatasetDescription,
    pub parser: Option<IdentifierParser>,
    pub dataset: Option<Ulid>,
}

impl CapturedDatabase {
    pub fn new(metadata: BucketMetadata, described: DatasetDescription) -> Self {
        Self {
            database: metadata.database,
            described,
            parser: metadata.parser,
            dataset: metadata.dataset,
        }
    }
}

/// A release a version is a member of: the release crate's exact version and its label.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleaseRef {
    pub crate_version: OriginVersion,
    pub label: String,
    pub upstream: Vec<Identifier>,
}

/// The permission path whose WRITE makes a realm curator of database releases.
pub fn curator_path(realm_id: crate::structs::identity::realm::RealmId) -> String {
    format!("/{realm_id}/admin/databases")
}

/// Version metadata key whose value is the release label; only the server sets it.
pub const RELEASE_TAG: &str = "aruna-engine.org/release";
/// Version metadata key naming the key prefix a release covers.
pub const RELEASE_PREFIX_TAG: &str = "aruna-engine.org/release-prefix";

/// Removes the release keys from version metadata copied out of another bucket, so a copy is
/// never a release.
pub fn strip_release_keys(metadata: &mut std::collections::HashMap<String, String>) {
    metadata.remove(RELEASE_TAG);
    metadata.remove(RELEASE_PREFIX_TAG);
}

/// An ordinary bucket's metadata row, bound to the bucket incarnation it was set on.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredMetadata {
    pub incarnation: u128,
    pub metadata: BucketMetadata,
}

impl StoredMetadata {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// One tagged release crate version this node holds: where it is and what it covers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReleasePointer {
    pub key: String,
    pub label: String,
    pub prefix: String,
}

impl ReleasePointer {
    /// The pointer a version's own metadata declares, if it is tagged as a release.
    pub fn of(key: &str, metadata: &std::collections::HashMap<String, String>) -> Option<Self> {
        Some(Self {
            key: key.to_string(),
            label: metadata.get(RELEASE_TAG)?.clone(),
            prefix: metadata
                .get(RELEASE_PREFIX_TAG)
                .cloned()
                .unwrap_or_default(),
        })
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Row key of a release pointer: the bucket name, `/`, then the crate's version id.
pub fn release_key(bucket: &str, version_id: Ulid) -> Vec<u8> {
    let mut key = release_prefix(bucket);
    key.extend_from_slice(&version_id.to_bytes());
    key
}

/// The prefix all release rows of one bucket share.
pub fn release_prefix(bucket: &str) -> Vec<u8> {
    let mut key = bucket.as_bytes().to_vec();
    key.push(b'/');
    key
}

/// The row every new release row of a bucket rewrites with its crate's version id, so a delete
/// that checked the bucket's releases fails when one was added since. Readers of release rows
/// skip it, since it carries no version id.
pub fn release_marker(bucket: &str) -> Vec<u8> {
    release_prefix(bucket)
}

/// Checks what a reader relies on: identifier rules that compile, and a dataset for a database.
pub fn validate_metadata(metadata: &BucketMetadata) -> Result<(), String> {
    if let Some(parser) = &metadata.parser {
        validate_parser(parser).map_err(|error| error.to_string())?;
    }
    if metadata.database && metadata.dataset.is_none() {
        return Err("a database needs a dataset that describes it".to_string());
    }
    Ok(())
}

/// Checks that every identifier links to an http or https page.
pub fn validate_identifiers(identifiers: &[Identifier]) -> Result<(), String> {
    for identifier in identifiers {
        let scheme = identifier.url.split_once("://").map(|(scheme, _)| scheme);
        if !matches!(scheme, Some("http" | "https")) {
            return Err(format!(
                "identifier {} needs an http or https link",
                identifier.name
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::identity::identifier::IdentifierTarget;
    use crate::structs::identity::realm::RealmId;
    use crate::structs::storage::global_id::GlobalId;

    fn doi() -> Identifier {
        Identifier {
            name: "DOI".to_string(),
            value: "10.5281/zenodo.1".to_string(),
            url: "https://doi.org/10.5281/zenodo.1".to_string(),
            target: IdentifierTarget::Key,
        }
    }

    #[test]
    fn metadata_round_trip() {
        let metadata = BucketMetadata {
            database: true,
            dataset: Some(Ulid::from_bytes([2; 16])),
            ..BucketMetadata::default()
        };
        let release = ReleaseRef {
            crate_version: OriginVersion::Global(GlobalId::Version {
                realm_id: RealmId::from_bytes([1; 32]),
                version_id: Ulid::from_bytes([3; 16]),
            }),
            label: "6.0-light".to_string(),
            upstream: vec![doi()],
        };
        let bytes = postcard::to_allocvec(&(metadata.clone(), release.clone())).unwrap();
        let restored: (BucketMetadata, ReleaseRef) = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(restored, (metadata, release));
        assert!(validate_metadata(&restored.0).is_ok());
    }

    /// The bucket metadata layout job records and set settings stored before the description
    /// moved into the dataset.
    #[derive(Default, Serialize)]
    struct PreviousMetadata {
        database: bool,
        title: Option<String>,
        description: Option<String>,
        version: Option<String>,
        license: Option<String>,
        identifiers: Vec<Identifier>,
        parser: Option<IdentifierParser>,
        dataset: Option<Ulid>,
    }

    #[test]
    fn previous_records_decode() {
        // A job record captured the whole previous metadata; it reads back as the description.
        let previous = PreviousMetadata {
            database: true,
            title: Some("Bakta DB".to_string()),
            description: Some("Annotation database".to_string()),
            version: Some("6.0".to_string()),
            license: Some("CC-BY-4.0".to_string()),
            identifiers: vec![doi()],
            parser: Some(IdentifierParser::default()),
            dataset: Some(Ulid::from_bytes([2; 16])),
        };
        let bytes = postcard::to_allocvec(&(Some(&previous), 7u8)).unwrap();
        let (captured, tail): (Option<CapturedDatabase>, u8) =
            postcard::from_bytes(&bytes).unwrap();
        let captured = captured.unwrap();
        assert_eq!(tail, 7, "the rest of the record still lines up");
        assert_eq!(captured.described.title.as_deref(), Some("Bakta DB"));
        assert_eq!(captured.described.license.as_deref(), Some("CC-BY-4.0"));
        assert_eq!(captured.described.identifiers, vec![doi()]);
        assert_eq!(captured.parser, Some(IdentifierParser::default()));
        assert_eq!(captured.dataset, Some(Ulid::from_bytes([2; 16])));
        // New captures keep that layout.
        let again = postcard::to_allocvec(&(Some(&captured), 7u8)).unwrap();
        assert_eq!(again, bytes);

        // Cleared metadata reads the same in both layouts; a filled previous row does not.
        let cleared = postcard::to_allocvec(&(None::<PreviousMetadata>, 7u8)).unwrap();
        let read: (Option<BucketMetadata>, u8) = postcard::from_bytes(&cleared).unwrap();
        assert_eq!(read, (None, 7));
        let filled = postcard::to_allocvec(&(Some(&previous), 7u8)).unwrap();
        let read = postcard::from_bytes::<(Option<BucketMetadata>, u8)>(&filled);
        assert_ne!(read.ok(), Some((Some(BucketMetadata::default()), 7)));
    }

    #[test]
    fn database_needs_dataset() {
        let metadata = BucketMetadata {
            database: true,
            ..BucketMetadata::default()
        };
        assert!(validate_metadata(&metadata).is_err());
        assert!(validate_metadata(&BucketMetadata::default()).is_ok());
        let mut link = doi();
        assert!(validate_identifiers(std::slice::from_ref(&link)).is_ok());
        link.url = "ftp://example.org/x".to_string();
        assert!(validate_identifiers(&[link]).is_err());
    }

    #[test]
    fn release_tags_read() {
        let mut metadata = std::collections::HashMap::from([
            (RELEASE_TAG.to_string(), "6.0".to_string()),
            (RELEASE_PREFIX_TAG.to_string(), "db/".to_string()),
            (
                "content-type".to_string(),
                "application/ld+json".to_string(),
            ),
        ]);
        let pointer = ReleasePointer::of("db/release.json", &metadata).unwrap();
        assert_eq!(pointer.label, "6.0");
        assert_eq!(pointer.prefix, "db/");
        let bytes = pointer.to_bytes().unwrap();
        assert_eq!(ReleasePointer::from_bytes(&bytes).unwrap(), pointer);
        // A copy loses the release keys and keeps everything else.
        strip_release_keys(&mut metadata);
        assert!(ReleasePointer::of("db/release.json", &metadata).is_none());
        assert_eq!(metadata.len(), 1);
        // Rows of one bucket share a prefix that another bucket's name cannot extend.
        let key = release_key("db", Ulid::from_bytes([4; 16]));
        assert!(key.starts_with(&release_prefix("db")));
        assert!(!release_key("db2", Ulid::nil()).starts_with(&release_prefix("db")));
    }
}
