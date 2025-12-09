use anyhow::{anyhow, bail, Result};
use base64::engine::general_purpose;
use base64::Engine;
use diesel_ulid::DieselUlid;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub proxy: Proxy,
    pub persistence: Option<Persistence>,
    pub frontend: Option<Frontend>,
    pub backend: Backend,
    pub rules: Option<Vec<Rule>>,
}

impl Config {
    pub fn validate(&mut self) -> Result<()> {
        let Config {
            proxy,
            persistence,
            backend,
            ..
        } = self;

        proxy.validate()?;
        if let Some(persistence) = persistence {
            persistence.validate()?;
        }
        backend.validate()?;
        Ok(())
    }

    pub fn get_rules(&self) -> Vec<Rule> {
        self.rules.clone().unwrap_or_default()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Proxy {
    pub endpoint_id: DieselUlid,
    pub private_key: Option<String>,
    pub public_key: String,
    pub serial: i32,
    pub remote_synced: bool,
    pub enable_ingest: bool,
    pub admin_ids: Vec<DieselUlid>,
    pub aruna_url: Option<String>,
    pub grpc_server: String,
    pub replication_interval: Option<u64>,
}

impl Proxy {
    pub fn validate(&mut self) -> Result<()> {
        let Proxy {
            private_key,
            serial,
            ..
        } = self;

        if let Some(private_key) = private_key {
            if private_key.len() < 32 {
                return Err(anyhow::anyhow!(
                    "private_key must be at least 32 characters long"
                ));
            }
        } else {
            let env_var = dotenvy::var("PROXY_PRIVATE_KEY").map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
            *private_key = Some(env_var);
        }

        if *serial < 1 {
            return Err(anyhow::anyhow!("serial must be at least 1"));
        }

        Ok(())
    }

    pub fn _get_private_key(&self) -> Result<[u8; 32]> {
        let Some(private_key) = self.private_key.clone() else {
            bail!("Private key not set")
        };
        let key = general_purpose::STANDARD.decode(private_key)?;
        let key = key
            .get(key.len() - 32..)
            .ok_or_else(|| anyhow!("Invalid key length"))?;
        Ok(key.try_into()?)
    }

    pub fn get_private_key_x25519(&self) -> Result<[u8; 32]> {
        let Some(private_key) = self.private_key.clone() else {
            bail!("Private key not set")
        };
        crate::auth::crypto::ed25519_to_x25519_privatekey(&private_key)
    }

    pub fn _get_public_key(&self) -> Result<[u8; 32]> {
        let key = general_purpose::STANDARD.decode(self.public_key.clone())?;
        let key = key
            .get(0..32)
            .ok_or_else(|| anyhow!("Invalid key length"))?;
        Ok(key.try_into()?)
    }

    pub fn get_public_key_x25519(&self) -> Result<[u8; 32]> {
        crate::auth::crypto::ed25519_to_x25519_pubkey(&self.public_key)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Persistence {
    Postgres {
        host: String,
        port: u16,
        user: String,
        password: Option<String>,
        database: String,
        schema: String,
    },
}

impl Persistence {
    fn validate(&mut self) -> Result<()> {
        let Persistence::Postgres {
            host,
            port,
            user,
            password,
            database,
            schema,
        } = self;

        if host.is_empty() {
            return Err(anyhow::anyhow!("host cannot be empty"));
        }

        if *port < 1 {
            return Err(anyhow::anyhow!("port must be at least 1"));
        }

        if user.is_empty() {
            return Err(anyhow::anyhow!("user cannot be empty"));
        }

        if database.is_empty() {
            return Err(anyhow::anyhow!("database cannot be empty"));
        }

        if schema.is_empty() {
            return Err(anyhow::anyhow!("schema cannot be empty"));
        }

        if password.is_none() {
            let env_var = dotenvy::var("POSTGRES_PASSWORD").map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
            *password = Some(env_var);
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Frontend {
    pub server: String,
    pub hostname: String,
    pub cors_exception: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    S3 {
        host: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        encryption: bool,
        compression: bool,
        deduplication: bool,
        force_path_style: Option<bool>,
        dropbox_bucket: Option<String>,
        backend_scheme: String,
        tmp: Option<String>,
    },
    FileSystem {
        root_path: String,
        encryption: bool,
        compression: bool,
        dropbox_folder: Option<String>,
        backend_scheme: String,
        tmp: Option<String>, // Will default to /tmp
    },
}

impl Backend {
    fn validate(&mut self) -> Result<()> {
        match self {
            Self::S3 {
                access_key,
                secret_key,
                host,
                ..
            } => {
                if host.is_none() {
                    let env_var = dotenvy::var("AWS_S3_HOST").map_err(|e| {
                        tracing::error!(error = ?e, msg = e.to_string());
                        e
                    })?;
                    *host = Some(env_var);
                }

                if access_key.is_none() {
                    let env_var = dotenvy::var("AWS_ACCESS_KEY_ID").map_err(|e| {
                        tracing::error!(error = ?e, msg = e.to_string(), "AWS_ACCESS_KEY_ID");
                        e
                    })?;
                    *access_key = Some(env_var);
                }

                if secret_key.is_none() {
                    let env_var = dotenvy::var("AWS_SECRET_ACCESS_KEY").map_err(|e| {
                        tracing::error!(error = ?e, msg = e.to_string(), "AWS_SECRET_ACCESS");
                        e
                    })?;
                    *secret_key = Some(env_var);
                }

                Ok(())
            }
            Self::FileSystem { .. } => Ok(()),
        }
    }

    #[allow(dead_code)]
    pub fn get_tmp(&self) -> Option<String> {
        match self {
            Self::S3 { tmp, .. } => tmp.clone(),
            Self::FileSystem { tmp, .. } => tmp.clone(),
        }
    }

    #[allow(dead_code)]
    pub fn is_encrypted(&self) -> bool {
        match self {
            Self::S3 { encryption, .. } => *encryption,
            Self::FileSystem { encryption, .. } => *encryption,
        }
    }

    #[allow(dead_code)]
    pub fn is_compressed(&self) -> bool {
        match self {
            Self::S3 { compression, .. } => *compression,
            Self::FileSystem { compression, .. } => *compression,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuleTarget {
    ROOT, // Info
    OBJECT,
    OBJECTPACKAGE,
    BUNDLE,
    REPLICATIONIN,
    REPLICATIONOUT,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub target: RuleTarget,
    pub rule: String,
}
