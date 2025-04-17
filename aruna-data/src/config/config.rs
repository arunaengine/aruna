use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::util::opendal::Backend;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub backend: BackendConfig,
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
