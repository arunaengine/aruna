use super::checksum::header_value_to_str;
use aruna_rust_api::api::storage::models::v2::{KeyValue, KeyValueVariant};
use aruna_rust_api::api::storage::services::v2::UpdateObjectRequest;
use http::{HeaderMap, HeaderValue};
use s3s::{s3_error, S3Error};
use std::collections::{BTreeMap, HashMap};

pub const USER_METADATA_PREFIX: &str = "x-amz-meta-";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct S3UserMetadata {
    entries: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MetadataPatch {
    pub add_key_values: Vec<KeyValue>,
    pub remove_key_values: Vec<KeyValue>,
}

impl S3UserMetadata {
    pub fn apply_to_key_values(&self, key_values: &mut Vec<KeyValue>) {
        key_values.retain(|key_value| normalize_user_metadata_key(&key_value.key).is_none());
        key_values.extend(Vec::<KeyValue>::from(self));
    }

    pub fn into_headers(self) -> HashMap<String, String> {
        self.entries.into_iter().collect()
    }

    pub fn diff_against(&self, key_values: &[KeyValue]) -> MetadataPatch {
        let current = S3UserMetadata::from(key_values);
        let desired_key_values = Vec::<KeyValue>::from(self);
        let current_key_values = Vec::<KeyValue>::from(&current);

        let add_key_values = desired_key_values
            .iter()
            .filter(|desired| !current_key_values.contains(desired))
            .cloned()
            .collect();

        let remove_key_values = current_key_values
            .iter()
            .filter(|existing| !desired_key_values.contains(existing))
            .cloned()
            .collect();

        MetadataPatch {
            add_key_values,
            remove_key_values,
        }
    }
}

impl MetadataPatch {
    pub fn is_empty(&self) -> bool {
        self.add_key_values.is_empty() && self.remove_key_values.is_empty()
    }

    pub fn into_update_request(self, object_id: String) -> UpdateObjectRequest {
        UpdateObjectRequest {
            object_id,
            name: None,
            description: None,
            add_key_values: self.add_key_values,
            remove_key_values: self.remove_key_values,
            data_class: 0,
            hashes: vec![],
            force_revision: false,
            parent: None,
            metadata_license_tag: None,
            data_license_tag: None,
        }
    }
}

impl TryFrom<&HeaderMap<HeaderValue>> for S3UserMetadata {
    type Error = S3Error;

    fn try_from(headers: &HeaderMap<HeaderValue>) -> Result<Self, Self::Error> {
        let mut entries = BTreeMap::new();

        for (name, value) in headers {
            let header_name = name.as_str();
            if header_name.eq_ignore_ascii_case(USER_METADATA_PREFIX) {
                return Err(s3_error!(
                    InvalidArgument,
                    "invalid user metadata header name"
                ));
            }

            let Some(header_name) = normalize_user_metadata_key(header_name) else {
                continue;
            };

            let header_value = header_value_to_str(value)?;
            entries
                .entry(header_name)
                .and_modify(|existing: &mut String| {
                    existing.push(',');
                    existing.push_str(header_value);
                })
                .or_insert_with(|| header_value.to_string());
        }

        Ok(S3UserMetadata { entries })
    }
}

impl From<&[KeyValue]> for S3UserMetadata {
    fn from(key_values: &[KeyValue]) -> Self {
        let mut entries = BTreeMap::new();

        for key_value in key_values {
            let Some(key) = normalize_user_metadata_key(&key_value.key) else {
                continue;
            };

            entries
                .entry(key)
                .and_modify(|existing: &mut String| {
                    existing.push(',');
                    existing.push_str(&key_value.value);
                })
                .or_insert_with(|| key_value.value.clone());
        }

        S3UserMetadata { entries }
    }
}

impl From<&S3UserMetadata> for Vec<KeyValue> {
    fn from(metadata: &S3UserMetadata) -> Self {
        metadata
            .entries
            .iter()
            .map(|(key, value)| KeyValue {
                key: key.clone(),
                value: value.clone(),
                variant: KeyValueVariant::StaticLabel as i32,
            })
            .collect()
    }
}

fn normalize_user_metadata_key(key: &str) -> Option<String> {
    let key = key.to_ascii_lowercase();
    let suffix = key.strip_prefix(USER_METADATA_PREFIX)?;

    if suffix.is_empty() {
        None
    } else {
        Some(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_rust_api::api::storage::models::v2::KeyValueVariant;
    use http::{HeaderMap, HeaderName, HeaderValue};

    fn metadata(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: value.to_string(),
            variant: KeyValueVariant::StaticLabel as i32,
        }
    }

    #[test]
    fn try_from_headers_combines_duplicate_headers() {
        let mut headers = HeaderMap::new();
        let metadata_name = HeaderName::from_static("x-amz-meta-author");

        headers.append(metadata_name.clone(), HeaderValue::from_static("alice"));
        headers.append(metadata_name, HeaderValue::from_static("bob"));
        headers.insert(
            "content-type",
            HeaderValue::from_static("application/octet-stream"),
        );

        let extracted = S3UserMetadata::try_from(&headers).expect("metadata should parse");

        assert_eq!(
            extracted,
            S3UserMetadata {
                entries: BTreeMap::from([(
                    "x-amz-meta-author".to_string(),
                    "alice,bob".to_string(),
                )]),
            }
        );
    }

    #[test]
    fn try_from_headers_rejects_non_ascii_values() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-meta-nonascii",
            HeaderValue::from_bytes(&[
                0x3d, 0x3f, 0x55, 0x54, 0x46, 0x2d, 0x38, 0x3f, 0x42, 0x3f, 0xc3,
            ])
            .expect("header value should be constructible"),
        );

        assert!(S3UserMetadata::try_from(&headers).is_err());
    }

    #[test]
    fn from_key_values_extracts_only_s3_metadata() {
        let metadata = S3UserMetadata::from(
            [
                metadata("x-amz-meta-owner", "alice"),
                metadata("X-Amz-Meta-Mtime", "1"),
                KeyValue {
                    key: "app.aruna-storage.org/cors".to_string(),
                    value: "cors".to_string(),
                    variant: KeyValueVariant::Label as i32,
                },
            ]
            .as_slice(),
        );

        assert_eq!(
            metadata,
            S3UserMetadata {
                entries: BTreeMap::from([
                    ("x-amz-meta-mtime".to_string(), "1".to_string()),
                    ("x-amz-meta-owner".to_string(), "alice".to_string()),
                ]),
            }
        );
    }

    #[test]
    fn into_key_values_creates_static_labels() {
        let s3_metadata = S3UserMetadata {
            entries: BTreeMap::from([("x-amz-meta-owner".to_string(), "alice".to_string())]),
        };

        assert_eq!(
            Vec::<KeyValue>::from(&s3_metadata),
            vec![metadata("x-amz-meta-owner", "alice")]
        );
    }

    #[test]
    fn apply_to_key_values_preserves_non_s3_key_values() {
        let mut key_values = vec![
            metadata("x-amz-meta-old", "before"),
            KeyValue {
                key: "app.aruna-storage.org/cors".to_string(),
                value: "cors".to_string(),
                variant: KeyValueVariant::Label as i32,
            },
        ];

        S3UserMetadata {
            entries: BTreeMap::from([("x-amz-meta-new".to_string(), "after".to_string())]),
        }
        .apply_to_key_values(&mut key_values);

        assert_eq!(
            key_values,
            vec![
                KeyValue {
                    key: "app.aruna-storage.org/cors".to_string(),
                    value: "cors".to_string(),
                    variant: KeyValueVariant::Label as i32,
                },
                metadata("x-amz-meta-new", "after"),
            ]
        );
    }

    #[test]
    fn diff_against_only_touches_s3_metadata_subset() {
        let key_values = vec![
            metadata("x-amz-meta-mtime", "1"),
            KeyValue {
                key: "app.aruna-storage.org/cors".to_string(),
                value: "cors".to_string(),
                variant: KeyValueVariant::Label as i32,
            },
        ];

        let patch = S3UserMetadata {
            entries: BTreeMap::from([("x-amz-meta-owner".to_string(), "alice".to_string())]),
        }
        .diff_against(&key_values);

        assert_eq!(
            patch.add_key_values,
            vec![metadata("x-amz-meta-owner", "alice")]
        );
        assert_eq!(
            patch.remove_key_values,
            vec![metadata("x-amz-meta-mtime", "1")]
        );
    }

    #[test]
    fn diff_against_returns_empty_patch_for_unchanged_metadata() {
        let key_values = vec![metadata("x-amz-meta-owner", "alice")];

        let patch = S3UserMetadata {
            entries: BTreeMap::from([("x-amz-meta-owner".to_string(), "alice".to_string())]),
        }
        .diff_against(&key_values);

        assert!(patch.is_empty());
    }

    #[test]
    fn into_headers_merges_keys_in_canonical_form() {
        let headers = S3UserMetadata::from(
            [
                metadata("x-amz-meta-owner", "alice"),
                metadata("X-Amz-Meta-Owner", "bob"),
            ]
            .as_slice(),
        )
        .into_headers();

        assert_eq!(headers.len(), 1);
        assert_eq!(
            headers.get("x-amz-meta-owner"),
            Some(&"alice,bob".to_string())
        );
    }

    #[test]
    fn metadata_patch_into_update_request_forces_revision() {
        let request = MetadataPatch {
            add_key_values: vec![metadata("x-amz-meta-owner", "alice")],
            remove_key_values: vec![metadata("x-amz-meta-mtime", "1")],
        }
        .into_update_request("object-id".to_string());

        assert!(!request.force_revision);
        assert_eq!(request.object_id, "object-id");
        assert_eq!(
            request.add_key_values,
            vec![metadata("x-amz-meta-owner", "alice")]
        );
        assert_eq!(
            request.remove_key_values,
            vec![metadata("x-amz-meta-mtime", "1")]
        );
    }

    #[test]
    fn empty_patch_into_update_request_still_uses_force_revision_flag() {
        let request = MetadataPatch::default().into_update_request("object-id".to_string());

        assert!(!request.force_revision);
        assert!(request.add_key_values.is_empty());
        assert!(request.remove_key_values.is_empty());
    }
}
