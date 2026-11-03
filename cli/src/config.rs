//! Stores the signed-in profile as JSON and its token in the OS keyring, never in a file.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::CliError;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};

const KEYRING_SERVICE: &str = "aruna-cli";
const PROFILE_FILE: &str = "profile.json";

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
}

/// The configuration directory with the profile; the token is in the keyring.
pub struct Config {
    pub dir: PathBuf,
}

impl Config {
    pub fn new(dir: Option<PathBuf>) -> Result<Self, CliError> {
        let dir = match dir {
            Some(dir) => dir,
            None => dirs::config_dir()
                .ok_or_else(|| CliError::usage("no config directory; pass --config-dir"))?
                .join("aruna-cli"),
        };
        Ok(Self { dir })
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

    /// Saves the token in the OS keyring; without one, login fails and scripts use ARUNA_TOKEN.
    pub fn save_token(&self, api: &str, token: &str) -> Result<(), CliError> {
        keyring_entry(api)
            .and_then(|entry| entry.set_password(token))
            .map_err(keyring_missing)
    }

    /// The `ARUNA_TOKEN` variable wins over the stored token, so scripts need no login.
    pub fn load_token(&self, profile: &Profile) -> Result<String, CliError> {
        if let Ok(token) = std::env::var("ARUNA_TOKEN")
            && !token.trim().is_empty()
        {
            return Ok(token.trim().to_string());
        }
        match keyring_entry(&profile.api).and_then(|entry| entry.get_password()) {
            Ok(token) => Ok(token.trim().to_string()),
            Err(keyring::Error::NoEntry) => Err(CliError::NotSignedIn),
            Err(error) => Err(keyring_missing(error)),
        }
    }

    /// Removes the token from the keyring and the profile; missing entries are fine.
    pub fn forget(&self, profile: &Profile) -> Result<(), CliError> {
        if let Ok(entry) = keyring_entry(&profile.api) {
            let _ = entry.delete_credential();
        }
        match std::fs::remove_file(self.dir.join(PROFILE_FILE)) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error.into()),
            _ => Ok(()),
        }
    }
}

fn keyring_missing(error: keyring::Error) -> CliError {
    CliError::usage(format!(
        "the OS keyring is not available ({error}); set ARUNA_API and ARUNA_TOKEN instead of login"
    ))
}

/// An entry in the store the embedding program set, such as a test's mock, else the OS keyring.
fn keyring_entry(api: &str) -> keyring::Result<keyring::Entry> {
    match keyring_core::get_default_store() {
        Some(_) => Ok(keyring::Entry {
            inner: keyring_core::Entry::new(KEYRING_SERVICE, api)?,
        }),
        None => keyring::Entry::new(KEYRING_SERVICE, api),
    }
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
    fn keyring_roundtrip() {
        keyring_core::set_default_store(keyring_core::mock::Store::new().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let config = Config::new(Some(dir.path().to_path_buf())).unwrap();
        let profile = Profile {
            api: "http://keyring-roundtrip/api/v1/".into(),
            ..Profile::default()
        };
        config.save_token(&profile.api, "secret").unwrap();
        config.save_profile(&profile).unwrap();
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
        assert_eq!(config.load_token(&profile).unwrap(), "secret");
        config.forget(&profile).unwrap();
        assert!(config.load_profile().unwrap().is_none());
        assert!(matches!(
            config.load_token(&profile),
            Err(CliError::NotSignedIn)
        ));
    }
}
