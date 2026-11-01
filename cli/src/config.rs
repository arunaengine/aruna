//! Stores the signed-in profile as JSON and its token in the OS keyring or a private file.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::CliError;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};

const KEYRING_SERVICE: &str = "aruna-cli";
const PROFILE_FILE: &str = "profile.json";
const TOKEN_FILE: &str = "token";

/// Where a token is kept: `auto` tries the keyring first and falls back to the file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum TokenStore {
    #[default]
    Auto,
    Keyring,
    File,
}

/// The realm and session a login recorded; never holds the token itself.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Profile {
    pub api: String,
    pub website: String,
    pub node_id: Option<String>,
    pub realm_id: Option<String>,
    pub user_id: Option<String>,
    pub user_name: Option<String>,
    pub session_id: Option<String>,
    pub expires_at: Option<String>,
    pub store: TokenStore,
}

/// The configuration directory with its profile and token.
pub struct Config {
    pub dir: PathBuf,
    pub store: TokenStore,
}

impl Config {
    pub fn new(dir: Option<PathBuf>, store: TokenStore) -> Result<Self, CliError> {
        let dir = match dir {
            Some(dir) => dir,
            None => dirs::config_dir()
                .ok_or_else(|| CliError::usage("no config directory; pass --config-dir"))?
                .join("aruna-cli"),
        };
        Ok(Self { dir, store })
    }

    pub fn load_profile(&self) -> Result<Option<Profile>, CliError> {
        match std::fs::read(self.dir.join(PROFILE_FILE)) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn save_profile(&self, profile: &Profile) -> Result<(), CliError> {
        std::fs::create_dir_all(&self.dir)?;
        let bytes = serde_json::to_vec_pretty(profile)?;
        write_private(&self.dir.join(PROFILE_FILE), &bytes)?;
        Ok(())
    }

    /// Saves the token and returns the store that took it.
    pub fn save_token(&self, api: &str, token: &str) -> Result<TokenStore, CliError> {
        if self.store != TokenStore::File {
            match keyring_entry(api).and_then(|entry| entry.set_password(token)) {
                Ok(()) => return Ok(TokenStore::Keyring),
                Err(error) if self.store == TokenStore::Keyring => {
                    return Err(CliError::usage(format!("keyring unavailable: {error}")));
                }
                Err(_) => {}
            }
        }
        std::fs::create_dir_all(&self.dir)?;
        write_private(&self.dir.join(TOKEN_FILE), token.as_bytes())?;
        Ok(TokenStore::File)
    }

    /// The `ARUNA_TOKEN` variable wins over the stored token, so scripts need no login.
    pub fn load_token(&self, profile: &Profile) -> Result<String, CliError> {
        if let Ok(token) = std::env::var("ARUNA_TOKEN")
            && !token.trim().is_empty()
        {
            return Ok(token.trim().to_string());
        }
        let token = match profile.store {
            TokenStore::Keyring => keyring_entry(&profile.api)
                .and_then(|entry| entry.get_password())
                .map_err(|error| CliError::usage(format!("keyring: {error}")))?,
            _ => match std::fs::read_to_string(self.dir.join(TOKEN_FILE)) {
                Ok(token) => token,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(CliError::NotSignedIn);
                }
                Err(error) => return Err(error.into()),
            },
        };
        Ok(token.trim().to_string())
    }

    /// Removes the token from both stores and the profile; missing entries are fine.
    pub fn forget(&self, profile: &Profile) -> Result<(), CliError> {
        if profile.store == TokenStore::Keyring
            && let Ok(entry) = keyring_entry(&profile.api)
        {
            let _ = entry.delete_credential();
        }
        for name in [TOKEN_FILE, PROFILE_FILE] {
            match std::fs::remove_file(self.dir.join(name)) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                    return Err(error.into());
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn keyring_entry(api: &str) -> keyring::Result<keyring::Entry> {
    keyring::Entry::new(KEYRING_SERVICE, api)
}

/// Writes a file only its owner can read, also on the first write.
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        options.mode(0o600);
        let mut file = options.open(path)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.write_all(bytes)
    }
    #[cfg(not(unix))]
    {
        options.open(path)?.write_all(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_store_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config::new(Some(dir.path().to_path_buf()), TokenStore::File).unwrap();
        let store = config.save_token("http://node/api/v1/", "secret").unwrap();
        assert_eq!(store, TokenStore::File);
        let profile = Profile {
            api: "http://node/api/v1/".into(),
            store,
            ..Profile::default()
        };
        config.save_profile(&profile).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.path().join(TOKEN_FILE))
                .unwrap()
                .permissions();
            assert_eq!(mode.mode() & 0o777, 0o600);
        }
        let loaded = config.load_profile().unwrap().unwrap();
        assert_eq!(loaded.store, TokenStore::File);
        config.forget(&loaded).unwrap();
        assert!(config.load_profile().unwrap().is_none());
    }
}
