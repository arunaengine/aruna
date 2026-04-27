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
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedSourceConnector {
    pub connector: SourceConnector,
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
