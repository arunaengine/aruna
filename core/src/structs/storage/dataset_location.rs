//! Where a dataset's files are stored: a bucket and a key prefix, with a default per dataset.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::types::GroupId;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

const MAX_PREFIX: usize = 512;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetLocation {
    pub bucket: String,
    /// Empty, or relative path segments ending in `/`.
    pub prefix: String,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum LocationError {
    #[error("bucket names use 3 to 63 lowercase letters, digits, dots and dashes")]
    Bucket,
    #[error("the prefix must be a relative path without empty, `.` or `..` segments")]
    Prefix,
}

/// The group bucket that holds dataset files and Git content by default.
pub fn default_bucket(group_id: GroupId) -> String {
    format!("datasets-{}", group_id.to_string().to_lowercase())
}

impl DatasetLocation {
    /// Checks the bucket name and normalizes the prefix: no leading `/`, one trailing `/`.
    pub fn new(bucket: &str, prefix: &str) -> Result<Self, LocationError> {
        let alphanumeric = |byte: Option<u8>| byte.is_some_and(|byte| byte.is_ascii_alphanumeric());
        let bucket_valid = (3..=63).contains(&bucket.len())
            && bucket.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
            })
            && alphanumeric(bucket.bytes().next())
            && alphanumeric(bucket.bytes().next_back())
            && !bucket.contains("..");
        if !bucket_valid {
            return Err(LocationError::Bucket);
        }
        let trimmed = prefix.trim_start_matches('/');
        let trimmed = trimmed.strip_suffix('/').unwrap_or(trimmed);
        let unsafe_part = trimmed
            .split('/')
            .any(|part| matches!(part, "" | "." | ".."));
        if trimmed.len() > MAX_PREFIX
            || trimmed.contains(['\\', '\0'])
            || trimmed.chars().any(char::is_control)
            || (!trimmed.is_empty() && unsafe_part)
        {
            return Err(LocationError::Prefix);
        }
        let prefix = if trimmed.is_empty() {
            String::new()
        } else {
            format!("{trimmed}/")
        };
        Ok(Self {
            bucket: bucket.to_string(),
            prefix,
        })
    }

    /// A group's default until an admin changes it: its generated bucket, without a prefix.
    pub fn group_default(group_id: GroupId) -> Self {
        Self {
            bucket: default_bucket(group_id),
            prefix: String::new(),
        }
    }

    /// The location used while the dataset has no chosen one.
    pub fn default_for(group_id: GroupId, document_id: Ulid) -> Self {
        Self {
            bucket: default_bucket(group_id),
            prefix: format!("{document_id}/"),
        }
    }

    /// This location narrowed to one dataset: prefix `<prefix><document id>/`.
    pub fn for_dataset(&self, document_id: Ulid) -> Self {
        Self {
            bucket: self.bucket.clone(),
            prefix: format!("{}{document_id}/", self.prefix),
        }
    }

    /// Whether the values are already valid and normalized.
    pub fn valid(&self) -> bool {
        Self::new(&self.bucket, &self.prefix).as_ref() == Ok(self)
    }

    /// The object key of a dataset path.
    pub fn key(&self, path: &str) -> String {
        format!("{}{path}", self.prefix)
    }

    /// The dataset path of an object inside this location.
    pub fn path<'a>(&self, bucket: &str, key: &'a str) -> Option<&'a str> {
        (bucket == self.bucket)
            .then(|| key.strip_prefix(self.prefix.as_str()))
            .flatten()
            .filter(|path| !path.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_prefix() {
        let location = DatasetLocation::new("lab-data", "/runs/2026").unwrap();
        assert_eq!(location.prefix, "runs/2026/");
        assert!(location.valid());
        assert_eq!(
            DatasetLocation::new("lab-data", "a/b/").unwrap().prefix,
            "a/b/"
        );
        assert_eq!(DatasetLocation::new("lab-data", "").unwrap().prefix, "");
        assert_eq!(DatasetLocation::new("lab-data", "/").unwrap().prefix, "");
        for prefix in ["a/../b", "a//b", "./a", "a\\b", "a/\u{1}"] {
            assert_eq!(
                DatasetLocation::new("lab-data", prefix),
                Err(LocationError::Prefix),
                "{prefix}"
            );
        }
        for bucket in ["ab", "Upper", "-lead", "a..b", "under_score"] {
            assert_eq!(
                DatasetLocation::new(bucket, "p"),
                Err(LocationError::Bucket),
                "{bucket}"
            );
        }
        let raw = DatasetLocation {
            bucket: "lab-data".into(),
            prefix: "/p".into(),
        };
        assert!(!raw.valid());
    }

    #[test]
    fn default_location() {
        let group = Ulid::from_string("01JABCDEF0123456789ABCDEFG").unwrap();
        let document = Ulid::from_string("01JMETADATA0123456789ABCDE").unwrap();
        let location = DatasetLocation::default_for(group, document);
        assert_eq!(location.bucket, "datasets-01jabcdef0123456789abcdefg");
        assert_eq!(location.prefix, "01JMETADATA0123456789ABCDE/");
        assert!(location.valid());
        assert_eq!(
            location.key("data/a.csv"),
            "01JMETADATA0123456789ABCDE/data/a.csv"
        );
        let key = location.key("data/a.csv");
        assert_eq!(location.path(&location.bucket, &key), Some("data/a.csv"));
        assert_eq!(location.path("other", &key), None);
        assert_eq!(location.path(&location.bucket, "elsewhere/a.csv"), None);
    }
}
