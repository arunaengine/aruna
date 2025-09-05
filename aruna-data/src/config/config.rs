use crate::error::ArunaDataError;
use crate::util::opendal::Backend;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use iroh::SecretKey;
use rand::rngs::OsRng;
use s3s::S3Result;
use s3s::host::{S3Host, VirtualHost};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use ulid::Ulid;

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
    pub fn load_from_env() -> Result<Config, ArunaDataError> {
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
            address: dotenvy::var("DATA_OPENAPI_ADDRESS")?.to_string(),
            port: dotenvy::var("DATA_OPENAPI_PORT")?.parse()?,
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

    pub fn create_dummy_config(
        node_idx: u16,
        realm_key: Option<SigningKey>,
        p2p_port: u16,
        openapi_port: u16,
        s3_port: u16,
        backend_type: &Backend,
    ) -> Config {
        Config {
            general: General {
                realm_key: realm_key.unwrap_or(SigningKey::generate(&mut OsRng)),
                node_key: SecretKey::generate(&mut OsRng),
                p2p_address: "127.0.0.1".to_string(),
                p2p_port,
            },
            persistence: Persistence {
                path: format!("/dev/shm/tests/test-{}/node-{}", Ulid::new(), node_idx),
                //path: format!("/tmp/aruna-data/test-{}/node-{}", Ulid::new(), node_idx),
            },
            backend: BackendConfig {
                backend_type: backend_type.clone(),
                access_config: Config::create_dummy_access(backend_type),
                max_bucket_size: 100,
                encryption: false,
                compression: false,
                deduplication: false,
            },
            frontend: Frontend {
                openapi_frontend: OpenApiFrontend {
                    address: "0.0.0.0".to_string(),
                    port: openapi_port,
                },
                s3_frontend: S3Frontend {
                    server: format!("0.0.0.0:{s3_port}",),
                    hostname: format!("localhost:{s3_port}",),
                    cors_exception: Some("http://localhost:3000".to_string()),
                },
            },
        }
    }

    fn create_dummy_access(backend_type: &Backend) -> HashMap<String, String> {
        let mut access_config = HashMap::new();
        match backend_type {
            Backend::S3 => access_config.extend([
                ("access_key_id".to_string(), "minioadmin".to_string()),
                ("secret_access_key".to_string(), "minioadmin".to_string()),
                ("endpoint".to_string(), "http://localhost:9000".to_string()),
                ("region".to_string(), "eu-central-1".to_string()),
                ("disable_config_load".to_string(), "true".to_string()),
                ("enable_virtual_host_style".to_string(), "false".to_string()),
            ]),
            Backend::HTTP => access_config.extend([
                ("endpoint".to_string(), "https://zenodo.com".to_string()),
                ("root".to_string(), "records".to_string()),
            ]),
            Backend::Memory => {}
            Backend::Postgres => access_config.extend([
                (
                    "connection_string".to_string(),
                    "postgres://postgres:mysecretpassword@localhost:5432/test".to_string(),
                ),
                ("secret_access_key".to_string(), "aruna".to_string()),
                ("key_field".to_string(), "path".to_string()),
                ("value_field".to_string(), "data".to_string()),
            ]),
            Backend::FileSystem => access_config.extend([
                ("root".to_string(), "/tmp/aruna".to_string()),
                (
                    "atomic_write_dir".to_string(),
                    "/tmp/atomic_write".to_string(),
                ),
            ]),
        }
        access_config
    }
}

impl S3Host for S3Frontend {
    fn parse_host_header<'a>(&'a self, _host: &'a str) -> S3Result<VirtualHost<'a>> {
        Ok(VirtualHost::new(self.server.clone()))
    }
}

fn load_access_config(prefix: &str) -> Result<HashMap<String, String>, ArunaDataError> {
    let mut map = HashMap::new();

    for (key, value) in dotenvy::vars() {
        if key.starts_with(prefix) {
            map.insert(strip_prefix(prefix, &key)?, value);
        }
    }

    Ok(map)
}

fn strip_prefix(prefix: &str, target: &str) -> Result<String, ArunaDataError> {
    Ok(match target.strip_prefix(prefix) {
        None => {
            return Err(ArunaDataError::InvalidParameter {
                name: "Key".to_string(),
                error: format!("Key {target} should start with prefix {prefix}"),
            });
        }
        Some(stripped_target) => stripped_target.to_lowercase(),
    })
}
