use super::source_connector::{SourceConnector, SourceConnectorKind};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolvedSourceAccess {
    OpenDal {
        kind: SourceConnectorKind,
        config: HashMap<String, String>,
        path: String,
        version: Option<String>,
    },
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceEntryKind {
    File,
    Directory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceEntry {
    pub name: String,
    pub path: String,
    pub kind: SourceEntryKind,
    pub size: Option<u64>,
    pub modified: Option<SystemTime>,
}
