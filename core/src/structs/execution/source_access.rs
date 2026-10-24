//! A resolved data source: its access config, object metadata and listed entries.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::source_connector::{SourceConnector, SourceConnectorKind};
use crate::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::time::SystemTime;

#[derive(Clone, Eq, PartialEq)]
pub enum ResolvedSourceAccess {
    OpenDal {
        kind: SourceConnectorKind,
        config: HashMap<String, String>,
        path: String,
        version: Option<String>,
    },
}

/// Shows only the config keys; the values include connector credentials.
impl fmt::Debug for ResolvedSourceAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self::OpenDal {
            kind,
            config,
            path,
            version,
        } = self;
        f.debug_struct("OpenDal")
            .field("kind", kind)
            .field("config_keys", &config.keys().collect::<Vec<_>>())
            .field("path", path)
            .field("version", version)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedSourceConnector {
    pub connector: SourceConnector,
    pub secret_fingerprint: Option<[u8; 16]>,
    pub access: ResolvedSourceAccess,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceMetadata {
    pub content_length: u64,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<SystemTime>,
    #[serde(skip)]
    pub source_version: Option<String>,
    /// At most one digest per algorithm, so the map stays bounded.
    pub digests: BTreeMap<ChecksumAlgorithm, SourceDigest>,
}

/// How much of the object a source digest covers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DigestScope {
    FullObject,
    /// Combined from part digests, like a multipart ETag; never a hash of the whole object.
    Composite,
}

/// Where a source digest came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DigestOrigin {
    /// Read from a listing ETag: evidence only, never enforced on a read.
    ListingEtag,
    /// A checksum manifest shipped next to the source files.
    Manifest,
    /// Computed over a complete read whose bytes were kept.
    VerifiedRead,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceDigest {
    pub digest: Vec<u8>,
    pub scope: DigestScope,
    pub origin: DigestOrigin,
}

/// The MD5 an S3-style ETag carries, as evidence: `hex` is a full-object digest and
/// `hex-parts` a composite one. Weak or other ETags carry none.
pub fn etag_digest(etag: &str) -> Option<(ChecksumAlgorithm, SourceDigest)> {
    let value = etag.trim();
    if value.starts_with("W/") {
        return None;
    }
    let value = value.trim_matches('"');
    let (hex_part, scope) = match value.split_once('-') {
        Some((hex_part, parts))
            if !parts.is_empty() && parts.bytes().all(|b| b.is_ascii_digit()) =>
        {
            (hex_part, DigestScope::Composite)
        }
        Some(_) => return None,
        None => (value, DigestScope::FullObject),
    };
    let digest = hex::decode(hex_part).ok()?;
    (digest.len() == ChecksumAlgorithm::Md5.digest_len()).then(|| {
        let origin = DigestOrigin::ListingEtag;
        (
            ChecksumAlgorithm::Md5,
            SourceDigest {
                digest,
                scope,
                origin,
            },
        )
    })
}

impl SourceMetadata {
    /// Canonical fingerprint of HEAD-derived drift fields. Equal fingerprints
    /// identify one representation; provider version and complete-read BLAKE3
    /// are folded in by the verified reference cache (#375).
    pub fn observation_fingerprint(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.content_length.to_le_bytes());
        write_fingerprint_field(&mut hasher, self.content_type.as_deref());
        write_fingerprint_field(&mut hasher, self.etag.as_deref());
        match self
            .last_modified
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        {
            Some(since_epoch) => {
                hasher.update(&[1]);
                hasher.update(&since_epoch.as_millis().to_le_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
        *hasher.finalize().as_bytes()
    }

    /// Compares this stored observation with a fresh one. Content type and validators the stored
    /// side lacks are unknown evidence and never compared; times compare in whole seconds.
    pub fn compare(&self, fresh: &SourceMetadata) -> ObservationMatch {
        if self.content_length != fresh.content_length {
            return ObservationMatch::Changed;
        }
        if let (Some(stored), Some(seen)) = (&self.content_type, &fresh.content_type)
            && stored != seen
        {
            return ObservationMatch::Changed;
        }
        let mut compared = false;
        let mut missing = false;
        if let Some(stored) = self.etag.as_deref() {
            match fresh.etag.as_deref() {
                Some(seen) if etag_value(stored) == etag_value(seen) => compared = true,
                Some(_) => return ObservationMatch::Changed,
                None => missing = true,
            }
        }
        if let Some(stored) = self.last_modified {
            match fresh.last_modified {
                Some(seen) if same_second(stored, seen) => compared = true,
                Some(_) => return ObservationMatch::Changed,
                None => missing = true,
            }
        }
        // ETag evidence repeats the ETag and may not be a hash at all, so only shipped or
        // verified digests are compared.
        let compared_digest = |digest: &&SourceDigest| digest.origin != DigestOrigin::ListingEtag;
        for (algorithm, stored) in self
            .digests
            .iter()
            .filter(|(_, digest)| compared_digest(digest))
        {
            if let Some(seen) = fresh.digests.get(algorithm).filter(compared_digest)
                && seen.scope == stored.scope
            {
                if seen.digest != stored.digest {
                    return ObservationMatch::Changed;
                }
                compared = true;
            }
        }
        match (compared, missing) {
            (true, false) => ObservationMatch::Same,
            _ => ObservationMatch::Unknown,
        }
    }

    /// Full-object digests a complete read must match before its bytes are kept.
    pub fn enforced_checksums(&self) -> Vec<ExpectedChecksum> {
        self.digests
            .iter()
            .filter(|(_, digest)| {
                digest.scope == DigestScope::FullObject
                    && digest.origin != DigestOrigin::ListingEtag
            })
            .map(|(algorithm, digest)| ExpectedChecksum {
                algorithm: *algorithm,
                digest: digest.digest.clone(),
            })
            .collect()
    }
}

/// How a fresh source observation relates to a stored one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationMatch {
    /// Same length, and every stored validator is present and equal.
    Same,
    /// A compared field differs.
    Changed,
    /// Nothing proves either: a stored validator disappeared or none could be compared.
    Unknown,
}

fn etag_value(etag: &str) -> &str {
    etag.trim().trim_matches('"')
}

/// Listings report milliseconds while HEAD dates carry whole seconds.
fn same_second(left: SystemTime, right: SystemTime) -> bool {
    let seconds = |time: SystemTime| {
        time.duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|since| since.as_secs())
    };
    match (seconds(left), seconds(right)) {
        (Some(left), Some(right)) => left == right,
        _ => left == right,
    }
}

/// Length-prefixed canonical encoding of an optional string field so distinct
/// fields cannot collide across boundaries.
fn write_fingerprint_field(hasher: &mut blake3::Hasher, value: Option<&str>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hasher.update(&(value.len() as u64).to_le_bytes());
            hasher.update(value.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceEntryKind {
    File,
    Directory,
}

/// Where a source listing page continues.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ListCursor {
    /// Provider key: the next page starts after it.
    After(String),
    /// Provider key: the next page starts at it, so a key a full page deferred is read first.
    At(String),
    /// Entries to skip, for a source whose provider cannot start after a key.
    Offset(u64),
}

/// One bounded page of a source listing. Key bounds need a flat recursive listing.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListRequest {
    pub cursor: Option<ListCursor>,
    /// Inclusive lowest key, probed on the first page only.
    pub lower: Option<String>,
    /// Exclusive upper key: the listing ends before it.
    pub stop: Option<String>,
    pub limit: usize,
    pub recursive: bool,
    pub files_only: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceEntry {
    pub name: String,
    pub path: String,
    pub kind: SourceEntryKind,
    pub size: Option<u64>,
    pub modified: Option<SystemTime>,
    pub etag: Option<String>,
    /// Full stat values, for a source that has them. A listing and a later stat
    /// of the same file must derive one identity, so the local lister fills
    /// this in and every other source leaves it absent.
    pub stat: Option<crate::structs::execution::offered_directory::FileStat>,
}

impl SourceEntry {
    /// The observation a listing gives for a file: content type is unknown evidence and the
    /// ETag digest is evidence only. `None` without a size.
    pub fn observation(&self) -> Option<SourceMetadata> {
        if self.kind != SourceEntryKind::File {
            return None;
        }
        let digests = self
            .etag
            .as_deref()
            .and_then(etag_digest)
            .into_iter()
            .collect();
        Some(SourceMetadata {
            content_length: self.size?,
            content_type: None,
            etag: self.etag.clone(),
            last_modified: self.modified,
            source_version: None,
            digests,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn access_hides_secrets() {
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::S3,
            config: HashMap::from([("secret_access_key".to_string(), "canary-8b1d".to_string())]),
            path: "data".to_string(),
            version: None,
        };
        let effect = crate::effects::StagingSourceEffect::Check { access };
        let rendered = format!("{effect:?}");
        assert!(!rendered.contains("canary"), "{rendered}");
        assert!(rendered.contains("secret_access_key"));
    }

    fn metadata() -> SourceMetadata {
        SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: Some("abc".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1000)),
            source_version: None,
            digests: BTreeMap::new(),
        }
    }

    #[test]
    fn etag_digest_scopes() {
        let (algorithm, full) = etag_digest("\"9e107d9d372bb6826bd81d3542a419d6\"").unwrap();
        assert_eq!(algorithm, ChecksumAlgorithm::Md5);
        assert_eq!(
            (full.scope, full.origin),
            (DigestScope::FullObject, DigestOrigin::ListingEtag)
        );
        let (_, parts) = etag_digest("\"9e107d9d372bb6826bd81d3542a419d6-12\"").unwrap();
        assert_eq!(parts.scope, DigestScope::Composite);
        assert!(etag_digest("W/\"9e107d9d372bb6826bd81d3542a419d6\"").is_none());
        assert!(etag_digest("etag-1").is_none());
    }

    #[test]
    fn evidence_never_enforced() {
        let (algorithm, evidence) = etag_digest("9e107d9d372bb6826bd81d3542a419d6").unwrap();
        let mut stored = metadata();
        stored.digests.insert(algorithm, evidence.clone());
        assert!(stored.enforced_checksums().is_empty());
        let mut other_evidence = stored.clone();
        other_evidence.digests.get_mut(&algorithm).unwrap().digest[0] ^= 1;
        assert_eq!(stored.compare(&other_evidence), ObservationMatch::Same);

        let shipped = SourceDigest {
            origin: DigestOrigin::Manifest,
            ..evidence
        };
        stored.digests.insert(algorithm, shipped.clone());
        assert_eq!(stored.enforced_checksums()[0].digest, shipped.digest);

        let mut changed = stored.clone();
        changed.digests.get_mut(&algorithm).unwrap().digest[0] ^= 1;
        assert_eq!(stored.compare(&changed), ObservationMatch::Changed);
    }

    #[test]
    fn fingerprint_tracks_fields() {
        let base = metadata();
        assert_eq!(
            base.observation_fingerprint(),
            base.observation_fingerprint()
        );

        let mut longer = base.clone();
        longer.content_length = 43;
        assert_ne!(
            base.observation_fingerprint(),
            longer.observation_fingerprint()
        );

        let mut retagged = base.clone();
        retagged.etag = Some("def".to_string());
        assert_ne!(
            base.observation_fingerprint(),
            retagged.observation_fingerprint()
        );

        let mut touched = base.clone();
        touched.last_modified = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(2000));
        assert_ne!(
            base.observation_fingerprint(),
            touched.observation_fingerprint()
        );
    }

    #[test]
    fn compare_needs_validators() {
        let stored = metadata();
        assert_eq!(stored.compare(&stored), ObservationMatch::Same);

        let mut bare = stored.clone();
        bare.etag = None;
        bare.last_modified = None;
        assert_eq!(bare.compare(&bare), ObservationMatch::Unknown);
        assert_eq!(stored.compare(&bare), ObservationMatch::Unknown);

        let mut retagged = stored.clone();
        retagged.etag = Some("other".to_string());
        retagged.last_modified = None;
        assert_eq!(stored.compare(&retagged), ObservationMatch::Changed);

        let mut longer = stored.clone();
        longer.content_length = 43;
        assert_eq!(stored.compare(&longer), ObservationMatch::Changed);
    }

    #[test]
    fn listing_matches_head() {
        let listed = SourceMetadata {
            content_type: None,
            etag: Some("\"abc\"".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH + Duration::from_millis(1_000_250)),
            ..metadata()
        };
        assert_eq!(listed.compare(&metadata()), ObservationMatch::Same);

        let mut typed = metadata();
        typed.content_type = Some("application/json".to_string());
        assert_eq!(metadata().compare(&typed), ObservationMatch::Changed);
    }

    // source_version is transient (not persisted), so it must not perturb the
    // fingerprint or a stored observation would spuriously read as drifted.
    #[test]
    fn fingerprint_ignores_version() {
        let base = metadata();
        let mut versioned = base.clone();
        versioned.source_version = Some("v7".to_string());
        assert_eq!(
            base.observation_fingerprint(),
            versioned.observation_fingerprint()
        );
    }
}
