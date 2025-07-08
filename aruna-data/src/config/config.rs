use crate::util::opendal::Backend;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use iroh::SecretKey;
use s3s::S3Result;
use s3s::host::{S3Host, VirtualHost};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub general: General,
    pub persistence: Persistence,
    pub backend: BackendConfig,
    pub frontend: Frontend,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct General {
    pub realm_key: SigningKey,
    pub node_key: SecretKey,
    pub p2p_address: String,
    pub p2p_port: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Persistence {
    pub path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct BackendConfig {
    pub backend_type: Backend,
    pub access_config: HashMap<String, String>,
    pub max_bucket_size: u64,
    pub encryption: bool,
    pub compression: bool,
    pub deduplication: bool,
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

impl Config {
    pub fn load_from_env() -> anyhow::Result<Config> {
        if let Ok(file) = dotenvy::var("ENV") {
            dotenvy::from_filename_override(file)?;
        }

        let general = General {
            realm_key: SigningKey::from_pkcs8_pem(&dotenvy::var("REALM_KEY")?)?,
            node_key: SecretKey::from_str(&dotenvy::var("P2P_KEY")?)?,
            p2p_address: dotenvy::var("P2P_ADDRESS")?,
            p2p_port: dotenvy::var("P2P_PORT")?.parse()?,
        };

        let persistence = Persistence {
            path: dotenvy::var("DB_PATH")?.to_string(),
        };

        let backend = BackendConfig {
            backend_type: Backend::from_str(&dotenvy::var("BACKEND_TYPE")?)?,
            encryption: dotenvy::var("BACKEND_ENCRYPTION")?.parse()?,
            compression: dotenvy::var("BACKEND_COMPRESSION")?.parse()?,
            deduplication: dotenvy::var("BACKEND_DEDUPLICATION")?.parse()?,
            max_bucket_size: dotenvy::var("BACKEND_MAX_BUCKET_SIZE")?.parse()?,
            access_config: load_access_config("BACKEND_ACCESS_")?,
        };

        let openapi_frontend = OpenApiFrontend {
            address: dotenvy::var("OPENAPI_ADDRESS")?.to_string(),
            port: dotenvy::var("OPENAPI_PORT")?.parse()?,
        };

        let s3_frontend = S3Frontend {
            server: dotenvy::var("S3_SERVER")?.to_string(),
            hostname: dotenvy::var("S3_HOSTNAME")?.to_string(),
            cors_exception: dotenvy::var("S3_CORS_EXCEPTION").ok(),
        };

        let frontend = Frontend {
            openapi_frontend,
            s3_frontend,
        };

        Ok(Config {
            general,
            persistence,
            backend,
            frontend,
        })
    }
}

impl S3Host for S3Frontend {
    fn parse_host_header<'a>(&'a self, _host: &'a str) -> S3Result<VirtualHost<'a>> {
        Ok(VirtualHost::new(self.server.clone()))
    }
}

fn load_access_config(prefix: &str) -> anyhow::Result<HashMap<String, String>> {
    let mut map = HashMap::new();

    for (key, value) in dotenvy::vars() {
        if key.starts_with(prefix) {
            map.insert(strip_prefix(prefix, &key), value);
        }
    }

    Ok(map)
}

fn strip_prefix(prefix: &str, target: &str) -> String {
    target.strip_prefix(prefix).unwrap().to_lowercase()
}
