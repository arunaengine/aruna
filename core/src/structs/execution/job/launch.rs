//! Records where and with what an execution attempt was launched, for its run crate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

/// Where and with what one attempt was launched, written with its attempt intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchRecord {
    pub node_id: NodeId,
    pub executor_kind: String,
    /// Declared site of the workers; empty when the backend proves none.
    pub location: String,
    pub resources: crate::compute::GrantedResources,
    pub workdir: Option<String>,
    /// The user-declared environment with secret-looking values hidden.
    pub env: BTreeMap<String, String>,
    /// Set once the backend accepted this attempt; `None` means planned only.
    pub started_at_ms: Option<u64>,
}

impl LaunchRecord {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// The crate a device keeps for one of its local runs, with the user it belongs to, so it
/// stays readable after the job record is pruned.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalCrate {
    pub owner: UserId,
    pub jsonld: String,
}

impl LocalCrate {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Value a run record shows in place of a secret-looking environment variable.
pub const HIDDEN_VALUE: &str = "hidden";

/// Name parts that mark a variable as secret, matched case-insensitively.
const SECRET_MARKERS: [&str; 7] = [
    "TOKEN",
    "SECRET",
    "KEY",
    "PASS",
    "CREDENTIAL",
    "AUTH",
    "PRIVATE",
];

/// The user-declared environment as a run record shows it: secret-looking values are hidden.
pub fn recorded_env(env: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    env.iter()
        .map(|(name, value)| {
            let upper = name.to_ascii_uppercase();
            let secret = SECRET_MARKERS.iter().any(|marker| upper.contains(marker));
            let shown = if secret { HIDDEN_VALUE } else { value.as_str() };
            (name.clone(), shown.to_string())
        })
        .collect()
}

#[cfg(test)]
mod launch_tests {
    use super::*;

    #[test]
    fn hides_secret_values() {
        let names = [
            ("API_TOKEN", true),
            ("aws_secret_access_key", true),
            ("DB_PASSWORD", true),
            ("PASSWD", true),
            ("GOOGLE_CREDENTIALS", true),
            ("AUTH_HEADER", true),
            ("PRIVATE_URL", true),
            ("BAKTA_DB", false),
            ("THREADS", false),
            ("PATH", false),
        ];
        let env: BTreeMap<String, String> = names
            .iter()
            .map(|(name, _)| (name.to_string(), "value".to_string()))
            .collect();
        let recorded = recorded_env(&env);
        for (name, secret) in names {
            let expected = if secret { HIDDEN_VALUE } else { "value" };
            assert_eq!(recorded[name], expected, "{name}");
        }
    }
}
