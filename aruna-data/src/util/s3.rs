use crate::error::ArunaDataError;
use crate::io::io_handler::ObjectInfo;
use aruna_permission::{Path, UserIdentity};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, RequestChecksumCalculation};
use fancy_regex::Regex;
use iroh::NodeAddr;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use ulid::Ulid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicationRule {
    pub user_id: UserIdentity,
    pub group_id: Ulid,
    pub target: Destination,
    pub source_bucket: String,
    pub source_filter: Option<Filter>, // TODO: Remove filters -> Only allow bucket full-sync
    pub existing_object_replication: bool, // Should be removed for the same reasons
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicationTask {
    pub user_id: UserIdentity,
    pub group_id: Ulid,
    pub replication_id: Ulid,
    pub replication_node: NodeAddr,
    pub permission_path: Path,
    pub object_info: ObjectInfo,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Destination {
    pub endpoint_id: Option<String>, // Destination should always have the same
    // group/bucket/content mappings, so bucket should be removed
    pub bucket: String,
    // Key should be ignored
    pub key: Option<String>,
}

// Filters can be removed
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Filter {
    pub prefix: Option<String>,
    pub tag: Option<Tag>,
    pub and: Option<And>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Tag {
    pub key: Option<String>,
    pub value: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct And {
    pub prefix: Option<String>,
    pub tags: Vec<Tag>,
}

impl Tag {
    fn verify(&self) -> Result<(), ArunaDataError> {
        if let (None, None) = (&self.key, &self.value) {
            Err(ArunaDataError::InvalidParameter {
                name: "ReplicationRuleFilter".to_string(),
                error: "Filter is empty".to_string(),
            })
        } else {
            Ok(())
        }
    }
}

impl And {
    fn verify(&self) -> Result<(), ArunaDataError> {
        match (&self.prefix, self.tags.is_empty()) {
            (Some(_), true) => Ok(()),
            (None, true) => Err(ArunaDataError::InvalidParameter {
                name: "ReplicationRuleFilter".to_string(),
                error: "Filter is empty".to_string(),
            }),
            (_, false) => {
                for t in &self.tags {
                    t.verify()?
                }
                Ok(())
            }
        }
    }
}

impl TryFrom<s3s::dto::ReplicationRuleFilter> for Filter {
    type Error = ArunaDataError;

    fn try_from(value: s3s::dto::ReplicationRuleFilter) -> Result<Self, Self::Error> {
        let filter = Filter {
            prefix: value.prefix,
            tag: value.tag.map(|t| Tag {
                key: t.key,
                value: t.value,
            }),
            and: value.and.map(|a| And {
                prefix: a.prefix,
                tags: a
                    .tags
                    .unwrap_or_default()
                    .into_iter()
                    .map(|t| Tag {
                        key: t.key,
                        value: t.value,
                    })
                    .collect(),
            }),
        };
        // TODO: Validate
        match (&filter.prefix, &filter.tag, &filter.and) {
            (None, None, None) => {
                return Err(ArunaDataError::InvalidParameter {
                    name: "ReplicationRuleFilter".to_string(),
                    error: "Filter is empty".to_string(),
                });
            }
            (None, None, Some(and)) => and.verify()?,
            (None, Some(tag), Some(and)) => {
                tag.verify()?;
                and.verify()?;
            }
            (Some(_), None, None) => (),
            (Some(_), None, Some(and)) => and.verify()?,
            (Some(_), Some(tag), None) => tag.verify()?,
            (Some(_), Some(tag), Some(and)) => {
                tag.verify()?;
                and.verify()?
            }
            (None, Some(tag), None) => tag.verify()?,
        }
        Ok(filter)
    }
}

// Applies S3 replication rule filters to a list of object keys.
/// Note: Tag filtering requires object metadata and cannot be applied to key strings alone.
/// This function only applies prefix-based filtering.
pub fn apply_filter(object_keys: &[String], filter: &Filter) -> Vec<String> {
    object_keys
        .iter()
        .filter(|key| matches_filter(key, filter))
        .cloned()
        .collect()
}

/// Checks if an object key matches the given filter.
fn matches_filter(key: &str, filter: &Filter) -> bool {
    // If no filter criteria are specified, match everything
    if filter.prefix.is_none() && filter.tag.is_none() && filter.and.is_none() {
        return true;
    }

    // Handle top-level prefix
    if let Some(ref prefix) = filter.prefix {
        return key.starts_with(prefix);
    }

    // Handle tag filter (cannot be applied to keys alone, so we skip it)
    if filter.tag.is_some() {
        // In a real implementation, you would need object metadata to check tags
        // For now, we'll assume no match since we only have the key
        return false;
    }

    // Handle AND condition
    if let Some(ref and_filter) = filter.and {
        return matches_and_filter(key, and_filter);
    }

    false
}

/// Checks if an object key matches the AND filter conditions.
fn matches_and_filter(key: &str, and_filter: &And) -> bool {
    // Check prefix condition if present
    if let Some(ref prefix) = and_filter.prefix {
        if !key.starts_with(prefix) {
            return false;
        }
    }

    // For tags, we would need object metadata which we don't have from just the key
    // In a real implementation, you'd need to fetch object metadata to check tags
    // For now, if there are tag requirements, we return false since we can't verify them
    if !and_filter.tags.is_empty() {
        return false;
    }

    true
}

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
    pub static ref S3_BUCKET_REGEX: Regex =
        Regex::new(S3_BUCKET_PATTERN).expect("Regex must be valid");
    pub static ref S3_KEY_REGEX: Regex = Regex::new(S3_KEY_PATTERN).expect("Regex must be valid");
}

pub fn validate_s3_bucket_name(key: &str) -> Result<bool, ArunaDataError> {
    Ok(S3_BUCKET_REGEX.is_match(key)? && key.len() <= 1024)
}

pub fn validate_s3_object_key(key: &str) -> Result<bool, ArunaDataError> {
    Ok(S3_KEY_REGEX.is_match(key)? && key.len() <= 1024)
}

pub async fn create_s3_client(
    endpoint: &str,
    region: Option<String>,
    access_key_id: &str,
    secret_key: &str,
    force_path_style: bool,
) -> Result<Client, ArunaDataError> {
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

pub async fn make_bucket(
    bucket: String,
    config: HashMap<String, String>,
) -> Result<(), ArunaDataError> {
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
                Err(ArunaDataError::ServerError(err.to_string()))
            }
        },
    }
}

impl Destination {
    pub fn from_arn(arn: String) -> Result<Self, ArunaDataError> {
        let mut iterator = arn.split(":");
        let _arn = iterator.next();
        let _partition = iterator.next();
        let _service = iterator.next();
        // TODO: alias for node_id
        let region = if let Some(r) = iterator.next() {
            if r.is_empty() {
                None
            } else {
                Some(r.to_string())
            }
        } else {
            None
        }; // node_id
        let _account_id = iterator.next();
        // TODO: Should we allow keys here? aws only allows ARNs for buckets
        let bucket_key = iterator.next().map(|b| b.to_string()).ok_or_else(|| {
            ArunaDataError::InvalidParameter {
                name: "ARN".to_string(),
                error: "Invalid ARN for replication task: No path specified".to_string(),
            }
        })?; // bucket
        let _version = iterator.next();

        let (bucket, key) = match bucket_key.split_once("/") {
            Some((bucket, key)) => (bucket.to_string(), Some(key.to_string())),
            None => (bucket_key.to_string(), None),
        };

        if bucket.is_empty() || bucket.contains("*") {
            return Err(ArunaDataError::InvalidParameter {
                name: "ARN".to_string(),
                error: "Invalid ARN for replication task: Invalid bucket".to_string(),
            });
        }
        if let Some(node_id) = &region {
            if node_id.contains("*") {
                return Err(ArunaDataError::InvalidParameter {
                    name: "ARN".to_string(),
                    error: "Invalid ARN for replication task: Region wildcars are not allowed"
                        .to_string(),
                });
            }
        }

        Ok(Destination {
            endpoint_id: region,
            bucket,
            key,
        })
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

    #[test]
    fn test_arns() {
        assert_eq!(
            Destination::from_arn("arn:aws:s3:my-node-id:account_id:my-test-bucket".to_string())
                .unwrap(),
            Destination {
                endpoint_id: Some("my-node-id".to_string()),
                bucket: "my-test-bucket".to_string(),
                key: None,
            }
        );
        assert_eq!(
            Destination::from_arn("arn:::::my-test-bucket".to_string()).unwrap(),
            Destination {
                endpoint_id: None,
                bucket: "my-test-bucket".to_string(),
                key: None,
            }
        );
        assert!(Destination::from_arn("arn:::node-id::".to_string()).is_err(),);
        assert!(Destination::from_arn("arn:::node-id::*".to_string()).is_err(),);
        assert!(Destination::from_arn("arn:::*::bucket".to_string()).is_err(),);

        assert_eq!(
            Destination::from_arn(
                "arn:aws:s3:my-node-id:account_id:my-test-bucket/a/test/key".to_string()
            )
            .unwrap(),
            Destination {
                endpoint_id: Some("my-node-id".to_string()),
                bucket: "my-test-bucket".to_string(),
                key: Some("a/test/key".to_string()),
            }
        );
    }

    #[test]
    fn test_filter_by_prefix() {
        let keys = vec![
            "documents/file1.txt".to_string(),
            "documents/file2.pdf".to_string(),
            "images/photo1.jpg".to_string(),
            "logs/app.log".to_string(),
        ];

        let filter = Filter {
            prefix: Some("documents/".to_string()),
            tag: None,
            and: None,
        };

        let result = apply_filter(&keys, &filter);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"documents/file1.txt".to_string()));
        assert!(result.contains(&"documents/file2.pdf".to_string()));
    }

    #[test]
    fn test_filter_with_and_prefix() {
        let keys = vec![
            "backup/documents/file1.txt".to_string(),
            "backup/images/photo1.jpg".to_string(),
            "documents/file2.pdf".to_string(),
        ];

        let filter = Filter {
            prefix: None,
            tag: None,
            and: Some(And {
                prefix: Some("backup/".to_string()),
                tags: vec![], // Empty tags for testing prefix only
            }),
        };

        let result = apply_filter(&keys, &filter);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"backup/documents/file1.txt".to_string()));
        assert!(result.contains(&"backup/images/photo1.jpg".to_string()));
    }

    #[test]
    fn test_empty_filter_matches_all() {
        let keys = vec!["file1.txt".to_string(), "file2.pdf".to_string()];

        let filter = Filter {
            prefix: None,
            tag: None,
            and: None,
        };

        let result = apply_filter(&keys, &filter);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_filter_with_tags_returns_empty() {
        let keys = vec!["file1.txt".to_string(), "file2.pdf".to_string()];

        let filter = Filter {
            prefix: None,
            tag: Some(Tag {
                key: Some("Environment".to_string()),
                value: Some("Production".to_string()),
            }),
            and: None,
        };

        let result = apply_filter(&keys, &filter);
        assert_eq!(result.len(), 0); // No matches since we can't verify tags from keys alone
    }
}
