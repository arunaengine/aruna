//! Checks the setting values of an install against the manifest's typed fields.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugin_installs::MAX_SETTINGS;
use aruna_plugins::manifest::{Manifest, SettingKind, https_origin};
use serde_json::{Map, Value};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum SettingError {
    #[error("`{0}` is not a setting of this plugin")]
    Unknown(String),
    #[error("setting `{0}` needs a value")]
    Missing(String),
    #[error("setting `{0}` has the wrong type or an invalid value")]
    Invalid(String),
    #[error("the settings exceed {MAX_SETTINGS} bytes")]
    TooLarge,
}

/// Every setting with its value or default, as the JSON object an install stores.
pub fn check_settings(
    manifest: &Manifest,
    values: &Map<String, Value>,
) -> Result<String, SettingError> {
    if let Some(unknown) = values
        .keys()
        .find(|key| !manifest.settings.contains_key(*key))
    {
        return Err(SettingError::Unknown(unknown.clone()));
    }
    let mut checked = Map::new();
    for (name, setting) in &manifest.settings {
        let value = (values.get(name).or(setting.default.as_ref()))
            .ok_or_else(|| SettingError::Missing(name.clone()))?;
        let valid = match setting.kind {
            SettingKind::String => value.is_string(),
            SettingKind::Integer => value.is_i64() || value.is_u64(),
            SettingKind::Boolean => value.is_boolean(),
            SettingKind::Url => value.as_str().is_some_and(https_origin),
            SettingKind::Choice => value
                .as_str()
                .is_some_and(|value| setting.choices.iter().any(|choice| choice == value)),
        };
        if !valid {
            return Err(SettingError::Invalid(name.clone()));
        }
        checked.insert(name.clone(), value.clone());
    }
    let json = Value::Object(checked).to_string();
    match json.len() <= MAX_SETTINGS {
        true => Ok(json),
        false => Err(SettingError::TooLarge),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn manifest() -> Manifest {
        serde_json::from_value(json!({
            "name": "dataset-announcer",
            "version": "0.1.0",
            "settings": {
                "catalog_url": { "title": "Catalog", "type": "url" },
                "batch": { "title": "Batch", "type": "integer", "default": 10 },
                "mode": { "title": "Mode", "type": "choice", "choices": ["fast", "safe"] }
            }
        }))
        .unwrap()
    }

    fn values(value: Value) -> Map<String, Value> {
        value.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn values_and_defaults() {
        let given = values(json!({ "catalog_url": "https://catalog.example.org", "mode": "safe" }));
        let stored: Value =
            serde_json::from_str(&check_settings(&manifest(), &given).unwrap()).unwrap();
        assert_eq!(stored["batch"], json!(10));
        assert_eq!(stored["mode"], json!("safe"));
        let cases = [
            (
                json!({ "mode": "safe" }),
                SettingError::Missing("catalog_url".into()),
            ),
            (
                json!({ "catalog_url": "http://catalog.example.org", "mode": "safe" }),
                SettingError::Invalid("catalog_url".into()),
            ),
            (
                json!({ "catalog_url": "https://c.example.org", "mode": "slow" }),
                SettingError::Invalid("mode".into()),
            ),
            (
                json!({ "catalog_url": "https://c.example.org", "mode": "safe", "batch": "x" }),
                SettingError::Invalid("batch".into()),
            ),
            (
                json!({ "catalog_url": "https://c.example.org", "mode": "safe", "extra": 1 }),
                SettingError::Unknown("extra".into()),
            ),
        ];
        for (given, expected) in cases {
            assert_eq!(check_settings(&manifest(), &values(given)), Err(expected));
        }
    }
}
