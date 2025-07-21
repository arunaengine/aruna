use anyhow::Result;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, RequestChecksumCalculation};
use fancy_regex::Regex;
use lazy_static::lazy_static;
use std::collections::HashMap;

/// This regex matches all the rules from the official Amazon S3 bucket naming rules specification_
/// https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
///  - Bucket names must be between 3 (min) and 63 (max) characters long.
///  - Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
///  - Bucket names must begin and end with a letter or number.
///  - Bucket names must not be formatted as an IP address (for example, 192.168.5.4).
///  - Bucket names must not start with the prefix xn--.
///  - Bucket names must not end with the suffix -s3alias.
///  - Buckets used with Amazon S3 Transfer Acceleration can't have dots (.) in their names.
const S3_BUCKET_PATTERN: &str = "(?!(^xn--|.+-s3alias$))^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$";

/// Permissive UTF-8 Pattern:
///  - Negative lookahead (?!.*[...]) excludes problematic characters
///  - \x00-\x1F: Control characters (0-31)
///  - \x7F: DEL character (127)
///  - \\{}^%`\]"<>#|~: Characters to avoid per AWS docs
///  - [\x20-\x7E\u0080-\uFFFF]: Printable ASCII + Unicode characters
const S3_KEY_PATTERN: &str =
    r#"^(?!.*[\x00-\x1F\x7F\\{}^%`\]"<>#|~])[\x20-\x7E\u0080-\uFFFF]{1,1024}$"#;

lazy_static! {
    pub static ref BUCKET_REGEX: Regex =
        Regex::new(S3_BUCKET_PATTERN).expect("Regex must be valid");
    pub static ref S3_KEY_REGEX: Regex = Regex::new(S3_KEY_PATTERN).expect("Regex must be valid");
}

pub fn validate_s3_bucket_name(key: &str) -> Result<bool> {
    Ok(S3_KEY_REGEX.is_match(key)? && key.len() <= 1024)
}

pub fn validate_s3_object_key(key: &str) -> Result<bool> {
    Ok(S3_KEY_REGEX.is_match(key)? && key.len() <= 1024)
}

pub async fn create_s3_client(
    endpoint: &str,
    region: Option<String>,
    access_key_id: &str,
    secret_key: &str,
    force_path_style: bool,
) -> Result<Client> {
    let creds = Credentials::new(access_key_id, secret_key, None, None, "Aruna_v3");
    let client_config = aws_config::defaults(BehaviorVersion::v2025_01_17())
        .credentials_provider(creds)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired)
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&client_config)
        .region(Region::new(region.unwrap_or("eu-central-1".to_string())))
        .endpoint_url(endpoint)
        .force_path_style(force_path_style)
        .build();

    Ok(Client::from_conf(s3_config))
}

pub async fn make_bucket(bucket: String, config: HashMap<String, String>) -> Result<()> {
    let s3_client = create_s3_client(
        config
            .get("endpoint")
            .expect("Config is missing endpoint URL"),
        None,
        config
            .get("access_key_id")
            .expect("Config is missing access key id"),
        config
            .get("secret_access_key")
            .expect("Config is missing secret access key"),
        true,
    )
    .await?;
    match s3_client
        .get_bucket_location()
        .bucket(bucket.clone())
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(e1) => match s3_client.create_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(()),
            Err(err) => {
                tracing::error!(?e1, ?err, "Error creating bucket");
                Err(anyhow::anyhow!(err))
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_keys() {
        assert!(validate_s3_object_key("my-file.txt").unwrap());
        assert!(validate_s3_object_key("folder/subfolder/file.jpg").unwrap());
        assert!(validate_s3_object_key("files/2024/document.pdf").unwrap());
        assert!(validate_s3_object_key("user@domain.com/file.dat").unwrap());
        assert!(validate_s3_object_key("file with spaces.txt").unwrap());
        assert!(validate_s3_object_key("café.txt").unwrap()); // Unicode
        assert!(validate_s3_object_key("файл.txt").unwrap()); // Cyrillic
    }

    #[test]
    fn test_invalid_keys() {
        assert!(!validate_s3_object_key("").unwrap()); // Empty
        assert!(!validate_s3_object_key("file\x00.txt").unwrap()); // Null byte
        assert!(!validate_s3_object_key("file\x1F.txt").unwrap()); // Control character
        assert!(!validate_s3_object_key("file\\path.txt").unwrap()); // Backslash
        assert!(!validate_s3_object_key("file{with}braces.txt").unwrap()); // Braces
        assert!(!validate_s3_object_key("file^caret.txt").unwrap()); // Caret
        assert!(!validate_s3_object_key("file%percent.txt").unwrap()); // Percent
        assert!(!validate_s3_object_key("file`backtick.txt").unwrap()); // Backtick
        assert!(!validate_s3_object_key("file[bracket].txt").unwrap()); // Brackets
        assert!(!validate_s3_object_key("file\"quote.txt").unwrap()); // Quote
        assert!(!validate_s3_object_key("file<less>greater.txt").unwrap()); // Angle brackets
        assert!(!validate_s3_object_key("file#hash.txt").unwrap()); // Hash
        assert!(!validate_s3_object_key("file|pipe.txt").unwrap()); // Pipe
        assert!(!validate_s3_object_key("file~tilde.txt").unwrap()); // Tilde

        // Too long (over 1024 characters)
        let long_key = "a".repeat(1025);
        assert!(!validate_s3_object_key(&long_key).unwrap());
    }
}
