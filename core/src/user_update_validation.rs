use thiserror::Error;

pub const MAX_USER_ATTRIBUTES: usize = 128;
pub const MAX_USER_ATTRIBUTE_KEY_BYTES: usize = 128;
pub const MAX_USER_ATTRIBUTE_VALUE_BYTES: usize = 4096;

/// Attribute keys directory endpoints may expose. Keys are free-form, so this
/// explicit allowlist is the contract; `email` is deliberately excluded and
/// must never appear in resolve or member output.
pub const SAFE_USER_ATTRIBUTE_KEYS: &[&str] = &["orcid", "affiliation", "department"];

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum UserAttributeValidationError {
    #[error("invalid user attribute key: {0}")]
    InvalidKey(String),
    #[error("invalid user attribute value for key: {0}")]
    InvalidValue(String),
    #[error("too many user attributes")]
    TooManyAttributes,
}

pub fn validate_user_attribute_key(key: &str) -> Result<(), UserAttributeValidationError> {
    if key.is_empty()
        || key.len() > MAX_USER_ATTRIBUTE_KEY_BYTES
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
    {
        return Err(UserAttributeValidationError::InvalidKey(key.to_string()));
    }

    Ok(())
}

pub fn validate_user_attribute_value(
    key: &str,
    value: &str,
) -> Result<(), UserAttributeValidationError> {
    if value.len() > MAX_USER_ATTRIBUTE_VALUE_BYTES || value.chars().any(char::is_control) {
        return Err(UserAttributeValidationError::InvalidValue(key.to_string()));
    }

    Ok(())
}

pub fn validate_user_attribute_count(count: usize) -> Result<(), UserAttributeValidationError> {
    if count > MAX_USER_ATTRIBUTES {
        return Err(UserAttributeValidationError::TooManyAttributes);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_USER_ATTRIBUTE_KEY_BYTES, MAX_USER_ATTRIBUTE_VALUE_BYTES, MAX_USER_ATTRIBUTES,
        UserAttributeValidationError, validate_user_attribute_count, validate_user_attribute_key,
        validate_user_attribute_value,
    };

    #[test]
    fn user_attribute_keys_allow_supported_ascii_separators() {
        for key in [
            "orcid",
            "profile.department",
            "edu_person:principal_name",
            "team-name",
            "team_name",
            "a1",
        ] {
            assert_eq!(validate_user_attribute_key(key), Ok(()));
        }
    }

    #[test]
    fn user_attribute_keys_reject_empty_long_or_unsupported_bytes() {
        for key in ["", "display name", "\u{fc}mlaut", "owner/slash"] {
            assert_eq!(
                validate_user_attribute_key(key),
                Err(UserAttributeValidationError::InvalidKey(key.to_string()))
            );
        }

        let key = "a".repeat(MAX_USER_ATTRIBUTE_KEY_BYTES + 1);
        assert_eq!(
            validate_user_attribute_key(&key),
            Err(UserAttributeValidationError::InvalidKey(key))
        );
    }

    #[test]
    fn user_attribute_values_allow_empty_and_printable_text() {
        assert_eq!(validate_user_attribute_value("department", ""), Ok(()));
        assert_eq!(
            validate_user_attribute_value("department", "biology and medicine"),
            Ok(())
        );
    }

    #[test]
    fn user_attribute_values_reject_long_or_control_text() {
        assert_eq!(
            validate_user_attribute_value("department", "bio\nmedicine"),
            Err(UserAttributeValidationError::InvalidValue(
                "department".to_string()
            ))
        );

        assert_eq!(
            validate_user_attribute_value(
                "department",
                &"a".repeat(MAX_USER_ATTRIBUTE_VALUE_BYTES + 1)
            ),
            Err(UserAttributeValidationError::InvalidValue(
                "department".to_string()
            ))
        );
    }

    #[test]
    fn user_attribute_count_rejects_more_than_limit() {
        assert_eq!(validate_user_attribute_count(MAX_USER_ATTRIBUTES), Ok(()));
        assert_eq!(
            validate_user_attribute_count(MAX_USER_ATTRIBUTES + 1),
            Err(UserAttributeValidationError::TooManyAttributes)
        );
    }
}
