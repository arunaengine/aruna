//! External identifiers of a document, such as a repository DOI, and their reverse index keys.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::structs::identity::auth::AuthContext;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use thiserror::Error;
use ulid::Ulid;

const MAX_VALUE_BYTES: usize = 512;

/// Stored enum: new kinds are appended at the end.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SecondaryIdKind {
    Doi,
    InvenioRecord,
    InvenioParent,
}

impl SecondaryIdKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Doi => "doi",
            Self::InvenioRecord => "invenio_record",
            Self::InvenioParent => "invenio_parent",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "doi" => Some(Self::Doi),
            "invenio_record" => Some(Self::InvenioRecord),
            "invenio_parent" => Some(Self::InvenioParent),
            _ => None,
        }
    }
}

impl fmt::Display for SecondaryIdKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a document came to hold an identifier. Stored enum: new origins are appended at the end.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum IdentifierOrigin {
    /// The document was pushed or exported to the record the identifier names.
    Published,
    /// The document was copied from that record.
    Imported,
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("invalid secondary identifier: {0}")]
pub struct SecondaryIdError(pub &'static str);

/// A normalized external identifier. Invenio ids are only unique per repository, so they carry
/// the repository API root; DOIs are global and never carry one. Ordering is by all fields.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SecondaryIdentifier {
    pub kind: SecondaryIdKind,
    pub value: String,
    pub endpoint: Option<String>,
    pub origin: IdentifierOrigin,
}

impl SecondaryIdentifier {
    pub fn new(
        kind: SecondaryIdKind,
        value: &str,
        endpoint: Option<&str>,
        origin: IdentifierOrigin,
    ) -> Result<Self, SecondaryIdError> {
        let value = normalize_value(kind, value)?;
        let endpoint = match kind {
            SecondaryIdKind::Doi => None,
            SecondaryIdKind::InvenioRecord | SecondaryIdKind::InvenioParent => {
                let endpoint = endpoint.ok_or(SecondaryIdError("repository endpoint missing"))?;
                Some(normalize_endpoint(endpoint)?)
            }
        };
        Ok(Self {
            kind,
            value,
            endpoint,
            origin,
        })
    }

    /// Whether both name the same external identifier, whatever their origin.
    pub fn same_identity(&self, other: &Self) -> bool {
        self.kind == other.kind && self.value == other.value && self.endpoint == other.endpoint
    }

    /// Reverse index key: `kind 0 value 0 endpoint 0 document_id`, so every claimant keeps a row.
    pub fn index_key(&self, document_id: Ulid) -> Vec<u8> {
        let mut key = secondary_id_prefix(self.kind, &self.value, Some(self.endpoint_text()));
        key.extend_from_slice(&document_id.to_bytes());
        key
    }

    fn endpoint_text(&self) -> &str {
        self.endpoint.as_deref().unwrap_or_default()
    }
}

/// Adds an identifier with set semantics: `Published` outranks `Imported` for one identity.
/// Returns whether the set changed.
pub fn insert_identifier(
    identifiers: &mut BTreeSet<SecondaryIdentifier>,
    identifier: SecondaryIdentifier,
) -> bool {
    let existing = identifiers
        .iter()
        .find(|known| known.same_identity(&identifier))
        .cloned();
    match existing {
        Some(known)
            if known.origin == identifier.origin || known.origin == IdentifierOrigin::Published =>
        {
            false
        }
        Some(known) => {
            identifiers.remove(&known);
            identifiers.insert(identifier)
        }
        None => identifiers.insert(identifier),
    }
}

/// Internal job payload that adds identifiers through the document's PID authority, so a
/// caller never waits for that authority. Adding is idempotent, so a rerun is safe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterIdentifiersSpec {
    pub document_id: Ulid,
    pub identifiers: Vec<SecondaryIdentifier>,
    pub auth_context: AuthContext,
}

/// Index prefix for one identifier value. Without an endpoint it finds every repository that
/// uses the value; values and endpoints never hold a zero byte, so prefixes cannot overlap.
pub fn secondary_id_prefix(kind: SecondaryIdKind, value: &str, endpoint: Option<&str>) -> Vec<u8> {
    let mut key = Vec::with_capacity(kind.as_str().len() + value.len() + 3);
    key.extend_from_slice(kind.as_str().as_bytes());
    key.push(0);
    key.extend_from_slice(value.as_bytes());
    key.push(0);
    if let Some(endpoint) = endpoint {
        key.extend_from_slice(endpoint.as_bytes());
        key.push(0);
    }
    key
}

/// The document id at the end of a reverse index key.
pub fn index_document(key: &[u8]) -> Option<Ulid> {
    let start = key.len().checked_sub(16)?;
    let bytes = <[u8; 16]>::try_from(&key[start..]).ok()?;
    Some(Ulid::from_bytes(bytes))
}

pub fn normalize_value(kind: SecondaryIdKind, value: &str) -> Result<String, SecondaryIdError> {
    match kind {
        SecondaryIdKind::Doi => normalize_doi(value),
        SecondaryIdKind::InvenioRecord | SecondaryIdKind::InvenioParent => {
            Ok(checked_text(value.trim())?.to_string())
        }
    }
}

/// Lowercases a DOI and strips the `doi:` and resolver URL prefixes.
pub fn normalize_doi(value: &str) -> Result<String, SecondaryIdError> {
    let mut doi = value.trim();
    for prefix in [
        "https://doi.org/",
        "http://doi.org/",
        "https://dx.doi.org/",
        "http://dx.doi.org/",
        "doi:",
    ] {
        if let Some(head) = doi.get(..prefix.len())
            && head.eq_ignore_ascii_case(prefix)
        {
            doi = doi[prefix.len()..].trim_start();
            break;
        }
    }
    let doi = checked_text(doi)?.to_lowercase();
    if !doi.starts_with("10.") || !doi.contains('/') {
        return Err(SecondaryIdError(
            "DOI must start with 10. and contain a suffix",
        ));
    }
    Ok(doi)
}

pub fn normalize_endpoint(endpoint: &str) -> Result<String, SecondaryIdError> {
    let endpoint = checked_text(endpoint.trim())?.trim_end_matches('/');
    if !(endpoint.starts_with("https://") || endpoint.starts_with("http://")) {
        return Err(SecondaryIdError("repository endpoint must be an http URL"));
    }
    Ok(endpoint.to_string())
}

fn checked_text(value: &str) -> Result<&str, SecondaryIdError> {
    if value.is_empty() || value.len() > MAX_VALUE_BYTES {
        return Err(SecondaryIdError("value must hold 1 to 512 bytes"));
    }
    if value.chars().any(char::is_control) {
        return Err(SecondaryIdError(
            "value must not contain control characters",
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doi_normalization() {
        for input in [
            "10.5281/Zenodo.123",
            " doi:10.5281/zenodo.123",
            "DOI:10.5281/ZENODO.123",
            "https://doi.org/10.5281/zenodo.123",
            "HTTPS://DX.DOI.ORG/10.5281/zenodo.123",
        ] {
            let id = SecondaryIdentifier::new(
                SecondaryIdKind::Doi,
                input,
                Some("https://x"),
                IdentifierOrigin::Imported,
            )
            .unwrap();
            assert_eq!(id.value, "10.5281/zenodo.123", "{input}");
            assert_eq!(id.endpoint, None);
        }
        assert!(normalize_doi("zenodo.123").is_err());
        assert!(normalize_doi("doi:").is_err());
    }

    #[test]
    fn invenio_needs_endpoint() {
        let id = SecondaryIdentifier::new(
            SecondaryIdKind::InvenioParent,
            " abcd-1234 ",
            Some("https://zenodo.org/api/"),
            IdentifierOrigin::Imported,
        )
        .unwrap();
        assert_eq!(id.value, "abcd-1234");
        assert_eq!(id.endpoint.as_deref(), Some("https://zenodo.org/api"));
        let record = |value, endpoint| {
            SecondaryIdentifier::new(
                SecondaryIdKind::InvenioRecord,
                value,
                endpoint,
                IdentifierOrigin::Imported,
            )
        };
        assert!(record("1", None).is_err());
        assert!(record("a\0b", Some("https://x")).is_err());
    }

    fn record(value: &str, origin: IdentifierOrigin) -> SecondaryIdentifier {
        SecondaryIdentifier::new(
            SecondaryIdKind::InvenioRecord,
            value,
            Some("https://zenodo.org/api"),
            origin,
        )
        .unwrap()
    }

    #[test]
    fn index_key_prefix() {
        let document = Ulid::from_bytes([7; 16]);
        let key = record("12", IdentifierOrigin::Imported).index_key(document);
        let other = record("123", IdentifierOrigin::Imported).index_key(document);
        let prefix = secondary_id_prefix(SecondaryIdKind::InvenioRecord, "12", None);
        let exact = secondary_id_prefix(
            SecondaryIdKind::InvenioRecord,
            "12",
            Some("https://zenodo.org/api"),
        );
        assert!(key.starts_with(&prefix) && key.starts_with(&exact));
        assert!(!other.starts_with(&prefix));
        assert_eq!(index_document(&key), Some(document));
        // Two claimants of one identifier keep separate rows.
        let second = record("12", IdentifierOrigin::Imported).index_key(Ulid::from_bytes([8; 16]));
        assert_ne!(key, second);
        assert!(second.starts_with(&exact));
    }

    #[test]
    fn published_outranks_imported() {
        let mut forward = BTreeSet::new();
        assert!(insert_identifier(
            &mut forward,
            record("1", IdentifierOrigin::Imported)
        ));
        assert!(insert_identifier(
            &mut forward,
            record("1", IdentifierOrigin::Published)
        ));
        assert!(!insert_identifier(
            &mut forward,
            record("1", IdentifierOrigin::Imported)
        ));
        let mut backward = BTreeSet::new();
        assert!(insert_identifier(
            &mut backward,
            record("1", IdentifierOrigin::Published)
        ));
        assert!(!insert_identifier(
            &mut backward,
            record("1", IdentifierOrigin::Imported)
        ));
        assert_eq!(forward, backward);
        assert_eq!(forward.len(), 1);
        assert_eq!(forward.first().unwrap().origin, IdentifierOrigin::Published);
    }
}
