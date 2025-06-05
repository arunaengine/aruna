use s3s::S3Result;
use s3s::host::{S3Host, VirtualHost};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::util::opendal::Backend;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub general: General,
    pub persistence: Persistence,
    pub backend: BackendConfig,
    pub frontend: Frontend,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct General {
    pub realm_id: String,
    pub node_key: String,
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
    pub openapi_frontend: OpenApiFrontend,
    pub s3_frontend: S3Frontend,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct OpenApiFrontend {
    pub address: String,
    pub port: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct S3Frontend {
    pub server: String,
    pub hostname: String,
    pub cors_exception: Option<String>,
}

impl S3Host for S3Frontend {
    fn parse_host_header<'a>(&'a self, _host: &'a str) -> S3Result<VirtualHost<'a>> {
        Ok(VirtualHost::new(self.server.clone()))
    }
}
