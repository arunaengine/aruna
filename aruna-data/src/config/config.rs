use s3s::S3Result;
use s3s::host::{S3Host, VirtualHost};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::util::opendal::Backend;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub persistence: Persistence,
    pub backend: BackendConfig,
    pub frontend: Frontend,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Persistence {
    pub path: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct BackendConfig {
    pub backend_type: Backend,
    pub encryption: bool,
    pub compression: bool,
    pub deduplication: bool,
    pub access: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Frontend {
    pub server: String,
    pub hostname: String,
    pub cors_exception: Option<String>,
}

impl S3Host for Frontend {
    fn parse_host_header<'a>(&'a self, host: &'a str) -> S3Result<VirtualHost<'a>> {
        Ok(VirtualHost::new(self.server.clone()))
    }
}
