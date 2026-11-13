//! Lists the permissions a plugin may request and the host interfaces each one unlocks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// One grantable plugin permission. A manifest requests it; only an admin grants it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Permission {
    MetadataRead,
    MetadataWrite,
    /// Creates datasets below the granted metadata paths.
    MetadataCreate,
    ObjectsRead,
    ObjectsCopy,
    TransfersDownload,
    TransfersUpload,
    JobsRun,
    /// Generated once per declared connection; a manifest never lists it.
    ConnectionsSend,
    SecretsValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("`{0}` is not a plugin permission")]
pub struct UnknownPermission(pub String);

impl Permission {
    pub const ALL: [Permission; 10] = [
        Self::MetadataRead,
        Self::MetadataWrite,
        Self::MetadataCreate,
        Self::ObjectsRead,
        Self::ObjectsCopy,
        Self::TransfersDownload,
        Self::TransfersUpload,
        Self::JobsRun,
        Self::ConnectionsSend,
        Self::SecretsValue,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MetadataRead => "metadata.read",
            Self::MetadataWrite => "metadata.write",
            Self::MetadataCreate => "metadata.create",
            Self::ObjectsRead => "objects.read",
            Self::ObjectsCopy => "objects.copy",
            Self::TransfersDownload => "transfers.download",
            Self::TransfersUpload => "transfers.upload",
            Self::JobsRun => "jobs.run",
            Self::ConnectionsSend => "connections.send",
            Self::SecretsValue => "secrets.value",
        }
    }

    /// Whether the host derives this permission from the manifest instead of a request list.
    pub const fn generated(self) -> bool {
        matches!(self, Self::ConnectionsSend)
    }
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Permission {
    type Err = UnknownPermission;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|permission| permission.as_str() == value)
            .ok_or_else(|| UnknownPermission(value.to_string()))
    }
}

impl TryFrom<String> for Permission {
    type Error = UnknownPermission;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<Permission> for String {
    fn from(value: Permission) -> Self {
        value.as_str().to_string()
    }
}

/// What importing one host interface of the plugin package requires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Import {
    /// Shared types, logging, private state and grant checks need no grant.
    Free,
    Needs(Permission),
    /// Outgoing requests need at least one declared connection.
    Connections,
}

impl Import {
    /// The requirement of an interface name such as `metadata-read`, if the host provides it.
    pub fn of(interface: &str) -> Option<Self> {
        Some(match interface {
            "types" | "log" | "state" | "grants" => Self::Free,
            "metadata-read" => Self::Needs(Permission::MetadataRead),
            "metadata-write" => Self::Needs(Permission::MetadataWrite),
            "metadata-create" => Self::Needs(Permission::MetadataCreate),
            "objects-read" => Self::Needs(Permission::ObjectsRead),
            "secrets" => Self::Needs(Permission::SecretsValue),
            "connections" => Self::Connections,
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip() {
        for permission in Permission::ALL {
            assert_eq!(permission.as_str().parse(), Ok(permission));
            let json = serde_json::to_string(&permission).unwrap();
            assert_eq!(json, format!("\"{permission}\""));
            assert_eq!(
                serde_json::from_str::<Permission>(&json).unwrap(),
                permission
            );
        }
        assert_eq!(
            "metadata.delete".parse::<Permission>(),
            Err(UnknownPermission("metadata.delete".into()))
        );
        assert!(serde_json::from_str::<Permission>("\"Metadata.Read\"").is_err());
    }

    #[test]
    fn interface_requirements() {
        assert_eq!(Import::of("log"), Some(Import::Free));
        assert_eq!(
            Import::of("metadata-write"),
            Some(Import::Needs(Permission::MetadataWrite))
        );
        assert_eq!(
            Import::of("metadata-create"),
            Some(Import::Needs(Permission::MetadataCreate))
        );
        assert_eq!(Import::of("connections"), Some(Import::Connections));
        assert_eq!(Import::of("wasi:cli/environment"), None);
    }
}
