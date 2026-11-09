//! Defines named vault secrets: names, kinds, field values, settings and use rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::compute::{Secret, SecretBytes};

/// Encoded bytes a secret value may take.
pub const MAX_VALUE_BYTES: usize = 16 * 1024;
/// Named fields one secret value may hold.
pub const MAX_FIELDS: usize = 32;
const MAX_NAME: usize = 64;
const MAX_FIELD_NAME: usize = 32;
const MAX_SETTING: usize = 256;
const MAX_HEADER: usize = 64;
/// Characters a description may take.
pub const MAX_DESCRIPTION: usize = 1024;

/// Name of a secret, unique per scope and owner: 1 to 64 of `a-z`, `0-9`, `.`, `_` and `-`,
/// starting with a letter or digit.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SecretName(String);

impl SecretName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SecretName {
    type Error = SecretError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        let starts = name
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit());
        let allowed = name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        });
        match starts && allowed && name.len() <= MAX_NAME {
            true => Ok(Self(name)),
            false => Err(SecretError::Name),
        }
    }
}

impl From<SecretName> for String {
    fn from(name: SecretName) -> Self {
        name.0
    }
}

impl fmt::Display for SecretName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// How the host adds a secret to an outgoing request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Injection {
    /// `Authorization: Bearer <token>`.
    Bearer,
    /// `Authorization: Basic <username:password>`.
    Basic,
    /// `<header setting>: <value>`.
    Header,
}

/// How the host checks an incoming request against a secret.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verification {
    /// The bearer token equals the `token` field.
    Bearer,
    /// The header named by the `header` setting equals the `value` field.
    Header,
    /// The header named by the `header` setting holds the HMAC-SHA256 of the raw body.
    Hmac,
}

/// The code-side table entry of one kind: its fields, settings and host actions.
#[derive(Debug, PartialEq, Eq)]
pub struct KindSpec {
    pub name: &'static str,
    /// Required field names; `None` accepts any field names.
    pub fields: Option<&'static [&'static str]>,
    pub required: &'static [&'static str],
    pub optional: &'static [&'static str],
    pub inject: Option<Injection>,
    pub verify: Option<Verification>,
}

const KINDS: &[KindSpec] = &[
    KindSpec {
        name: "bearer",
        fields: Some(&["token"]),
        required: &[],
        optional: &[],
        inject: Some(Injection::Bearer),
        verify: Some(Verification::Bearer),
    },
    KindSpec {
        name: "basic",
        fields: Some(&["password", "username"]),
        required: &[],
        optional: &[],
        inject: Some(Injection::Basic),
        verify: None,
    },
    KindSpec {
        name: "header",
        fields: Some(&["value"]),
        required: &["header"],
        optional: &[],
        inject: Some(Injection::Header),
        verify: Some(Verification::Header),
    },
    KindSpec {
        name: "hmac",
        fields: Some(&["key"]),
        required: &["header"],
        optional: &["prefix"],
        inject: None,
        verify: Some(Verification::Hmac),
    },
    KindSpec {
        name: "fields",
        fields: None,
        required: &[],
        optional: &[],
        inject: None,
        verify: None,
    },
];

/// The kind of a secret, stored as its name so a new kind never changes the record format.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretKind(String);

impl SecretKind {
    /// A built-in kind; plugin kinds join with the plugin manifests.
    pub fn parse(name: &str) -> Result<Self, SecretError> {
        let known = KINDS.iter().any(|kind| kind.name == name);
        known
            .then(|| Self(name.to_string()))
            .ok_or_else(|| SecretError::Kind(name.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn spec(&self) -> Result<&'static KindSpec, SecretError> {
        (KINDS.iter())
            .find(|kind| kind.name == self.0)
            .ok_or_else(|| SecretError::Kind(self.0.clone()))
    }
}

/// Settings of a secret that are not secret, such as the header a `header` secret goes into.
pub type SecretSettings = BTreeMap<String, String>;

/// Checks settings against the kind: required ones present, no unknown ones, safe values.
pub fn check_settings(kind: &SecretKind, settings: &SecretSettings) -> Result<(), SecretError> {
    let spec = kind.spec()?;
    if let Some(missing) = (spec.required.iter()).find(|name| !settings.contains_key(**name)) {
        return Err(SecretError::Setting((*missing).to_string()));
    }
    for (name, value) in settings {
        let known =
            spec.required.contains(&name.as_str()) || spec.optional.contains(&name.as_str());
        let valid = match name.as_str() {
            "header" => header_name(value),
            _ => value.len() <= MAX_SETTING && value.bytes().all(|byte| byte.is_ascii_graphic()),
        };
        if !known || !valid {
            return Err(SecretError::Setting(name.clone()));
        }
    }
    Ok(())
}

/// A header token the host may set or read; never one that frames or routes the request.
pub fn header_name(name: &str) -> bool {
    const RESERVED: &[&str] = &[
        "connection",
        "content-length",
        "cookie",
        "host",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ];
    let token = name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte));
    let lower = name.to_ascii_lowercase();
    !name.is_empty() && name.len() <= MAX_HEADER && token && !RESERVED.contains(&lower.as_str())
}

/// The named fields of a secret value. Ordered, so equal values encode to equal bytes.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretFields(BTreeMap<String, Secret>);

impl SecretFields {
    pub fn new(fields: BTreeMap<String, Secret>) -> Self {
        Self(fields)
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(Secret::expose)
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    pub fn values(&self) -> impl Iterator<Item = &str> {
        self.0.values().map(Secret::expose)
    }

    /// Checks the fields against the kind and the size limits.
    pub fn check(&self, kind: &SecretKind) -> Result<(), SecretError> {
        let spec = kind.spec()?;
        let names_valid = self.0.keys().all(|name| {
            !name.is_empty()
                && name.len() <= MAX_FIELD_NAME
                && (name.bytes())
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        });
        let shape = match spec.fields {
            Some(required) => self
                .0
                .keys()
                .map(String::as_str)
                .eq(required.iter().copied()),
            None => !self.0.is_empty(),
        };
        let filled = self.0.values().all(|value| !value.expose().is_empty());
        if !names_valid || !shape || !filled || self.0.len() > MAX_FIELDS {
            return Err(SecretError::Fields);
        }
        match self.encode()?.expose().len() <= MAX_VALUE_BYTES {
            true => Ok(()),
            false => Err(SecretError::TooLarge),
        }
    }

    pub fn encode(&self) -> Result<SecretBytes, SecretError> {
        postcard::to_allocvec(&self.0)
            .map(SecretBytes::new)
            .map_err(|_| SecretError::Encoding)
    }

    pub fn decode(bytes: &SecretBytes) -> Result<Self, SecretError> {
        postcard::from_bytes(bytes.expose()).map_err(|_| SecretError::Encoding)
    }
}

impl fmt::Debug for SecretFields {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretFields")
            .field("names", &self.0.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Why a secret, its value or a rule is not valid. Never carries a value.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum SecretError {
    #[error(
        "a secret name is 1 to 64 of a-z, 0-9, '.', '_' and '-', starting with a letter or digit"
    )]
    Name,
    #[error("unknown secret kind {0}")]
    Kind(String),
    #[error("the setting {0} is missing, unknown or invalid for this kind")]
    Setting(String),
    #[error("the fields do not match the kind, are empty, or have invalid names")]
    Fields,
    #[error("the secret value is larger than 16 KiB")]
    TooLarge,
    #[error("the secret value could not be encoded or decoded")]
    Encoding,
    #[error("the description is longer than 1024 characters")]
    Description,
}

#[cfg(test)]
#[path = "secrets_tests.rs"]
mod tests;
