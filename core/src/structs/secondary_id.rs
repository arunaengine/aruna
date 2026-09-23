//! External identifiers of a document, such as a repository DOI, and their reverse index keys.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

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
}

impl SecondaryIdentifier {
    pub fn new(
        kind: SecondaryIdKind,
        value: &str,
        endpoint: Option<&str>,
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
        })
    }

    /// Reverse index key: `kind 0 value 0 endpoint`. A prefix without the endpoint finds every
    /// repository that uses the same value.
    pub fn index_key(&self) -> Vec<u8> {
        let mut key = secondary_id_prefix(self.kind, &self.value);
        key.extend_from_slice(self.endpoint.as_deref().unwrap_or_default().as_bytes());
        key
    }
}

pub fn secondary_id_prefix(kind: SecondaryIdKind, value: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(kind.as_str().len() + value.len() + 2);
    key.extend_from_slice(kind.as_str().as_bytes());
    key.push(0);
    key.extend_from_slice(value.as_bytes());
    key.push(0);
    key
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
            let id =
                SecondaryIdentifier::new(SecondaryIdKind::Doi, input, Some("https://x")).unwrap();
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
        )
        .unwrap();
        assert_eq!(id.value, "abcd-1234");
        assert_eq!(id.endpoint.as_deref(), Some("https://zenodo.org/api"));
        assert!(SecondaryIdentifier::new(SecondaryIdKind::InvenioRecord, "1", None).is_err());
        assert!(
            SecondaryIdentifier::new(SecondaryIdKind::InvenioRecord, "a\0b", Some("https://x"))
                .is_err()
        );
    }

    #[test]
    fn index_key_prefix() {
        let id = SecondaryIdentifier::new(
            SecondaryIdKind::InvenioRecord,
            "12",
            Some("https://zenodo.org/api"),
        )
        .unwrap();
        let other = SecondaryIdentifier::new(
            SecondaryIdKind::InvenioRecord,
            "123",
            Some("https://zenodo.org/api"),
        )
        .unwrap();
        let prefix = secondary_id_prefix(SecondaryIdKind::InvenioRecord, "12");
        assert!(id.index_key().starts_with(&prefix));
        assert!(!other.index_key().starts_with(&prefix));
    }
}
