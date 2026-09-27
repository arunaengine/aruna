//! Validates user attribute keys and values against the allowed charset and size limits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use thiserror::Error;

pub const MAX_USER_ATTRIBUTES: usize = 128;
pub const ATTRIBUTE_KEY_BYTES: usize = 128;
pub const ATTRIBUTE_VALUE_BYTES: usize = 4096;
/// Prefix of attributes that only Aruna operations set, never a user update.
pub const RESERVED_PREFIX: &str = "aruna-engine.org/";
/// Present while the account is deactivated.
pub const DEACTIVATED_ATTRIBUTE: &str = "aruna-engine.org/deactivated";
/// Group that owns a service account.
pub const SERVICE_GROUP_ATTRIBUTE: &str = "aruna-engine.org/service-group";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum UserAttributeError {
    #[error("invalid user attribute key: {0}")]
    InvalidKey(String),
    #[error("invalid user attribute value for key: {0}")]
    InvalidValue(String),
    #[error("too many user attributes")]
    TooManyAttributes,
}

pub fn is_reserved_attribute(key: &str) -> bool {
    key.starts_with(RESERVED_PREFIX)
}

pub fn validate_attribute_key(key: &str) -> Result<(), UserAttributeError> {
    let name = key.strip_prefix(RESERVED_PREFIX).unwrap_or(key);
    if name.is_empty()
        || key.len() > ATTRIBUTE_KEY_BYTES
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
    {
        return Err(UserAttributeError::InvalidKey(key.to_string()));
    }

    Ok(())
}

pub fn validate_attribute_value(key: &str, value: &str) -> Result<(), UserAttributeError> {
    if value.len() > ATTRIBUTE_VALUE_BYTES
        || value.chars().any(char::is_control)
        || (key.starts_with(crate::user::profile::VISIBILITY_PREFIX)
            && !matches!(value, "public" | "private"))
    {
        return Err(UserAttributeError::InvalidValue(key.to_string()));
    }

    Ok(())
}

pub fn validate_attribute_count(count: usize) -> Result<(), UserAttributeError> {
    if count > MAX_USER_ATTRIBUTES {
        return Err(UserAttributeError::TooManyAttributes);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ATTRIBUTE_KEY_BYTES, ATTRIBUTE_VALUE_BYTES, MAX_USER_ATTRIBUTES, UserAttributeError,
        validate_attribute_count, validate_attribute_key, validate_attribute_value,
    };

    #[test]
    fn user_attribute_separators() {
        for key in [
            "orcid",
            "profile.department",
            "edu_person:principal_name",
            "team-name",
            "team_name",
            "a1",
        ] {
            assert_eq!(validate_attribute_key(key), Ok(()));
        }
    }

    #[test]
    fn reserved_attribute_keys() {
        assert_eq!(validate_attribute_key(super::DEACTIVATED_ATTRIBUTE), Ok(()));
        assert!(super::is_reserved_attribute(super::SERVICE_GROUP_ATTRIBUTE));
        for key in ["aruna-engine.org/", "aruna-engine.org/a/b", "other.org/key"] {
            assert!(validate_attribute_key(key).is_err());
        }
    }

    #[test]
    fn user_attribute_bytes() {
        for key in ["", "display name", "\u{fc}mlaut", "owner/slash"] {
            assert_eq!(
                validate_attribute_key(key),
                Err(UserAttributeError::InvalidKey(key.to_string()))
            );
        }

        let key = "a".repeat(ATTRIBUTE_KEY_BYTES + 1);
        assert_eq!(
            validate_attribute_key(&key),
            Err(UserAttributeError::InvalidKey(key))
        );
    }

    #[test]
    fn user_attribute_text() {
        assert_eq!(validate_attribute_value("department", ""), Ok(()));
        assert_eq!(
            validate_attribute_value("department", "biology and medicine"),
            Ok(())
        );
    }

    #[test]
    fn user_values_text() {
        assert_eq!(
            validate_attribute_value("department", "bio\nmedicine"),
            Err(UserAttributeError::InvalidValue("department".to_string()))
        );

        assert_eq!(
            validate_attribute_value("department", &"a".repeat(ATTRIBUTE_VALUE_BYTES + 1)),
            Err(UserAttributeError::InvalidValue("department".to_string()))
        );
    }

    #[test]
    fn user_attribute_limit() {
        assert_eq!(validate_attribute_count(MAX_USER_ATTRIBUTES), Ok(()));
        assert_eq!(
            validate_attribute_count(MAX_USER_ATTRIBUTES + 1),
            Err(UserAttributeError::TooManyAttributes)
        );
    }
}
