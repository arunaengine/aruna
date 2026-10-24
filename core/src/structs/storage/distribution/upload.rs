//! Names a multipart upload of a distributed bucket by its set, range and coordinator epoch.
//! The coordinator accepts its parts and completion only while that authority still holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use super::{DistributionSnapshot, RouteError};

const SEPARATOR: char = '-';

/// The upload id a client sees for a distributed bucket: the stored upload, then the set,
/// range and epoch it was opened under.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UploadTag {
    pub upload_id: Ulid,
    pub set_id: Ulid,
    pub range_id: Ulid,
    pub epoch: u64,
}

impl UploadTag {
    pub fn encode(&self) -> String {
        format!(
            "{}{SEPARATOR}{}{SEPARATOR}{}{SEPARATOR}{}",
            self.upload_id, self.set_id, self.range_id, self.epoch
        )
    }

    /// Reads a tagged upload id; a plain upload id of an ordinary bucket gives `None`.
    pub fn parse(value: &str) -> Option<Self> {
        let mut parts = value.split(SEPARATOR);
        let tag = Self {
            upload_id: Ulid::from_string(parts.next()?).ok()?,
            set_id: Ulid::from_string(parts.next()?).ok()?,
            range_id: Ulid::from_string(parts.next()?).ok()?,
            epoch: parts.next()?.parse().ok()?,
        };
        parts.next().is_none().then_some(tag)
    }
}

impl DistributionSnapshot {
    /// Whether `member_id` still coordinates the upload `tag` names for `key`: same set, the
    /// key's range is the named one or descends from it, and its epoch did not move.
    pub fn upload_authority(
        &self,
        member_id: Ulid,
        key: &str,
        tag: &UploadTag,
    ) -> Result<(), RouteError> {
        if tag.set_id != self.set_id() {
            return Err(RouteError::StaleRange(tag.range_id));
        }
        let range = self.resolve(tag.range_id, key)?;
        if range.epoch != tag.epoch || range.coordinator != member_id {
            return Err(RouteError::StaleRange(tag.range_id));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::UploadTag;
    use ulid::Ulid;

    fn tag() -> UploadTag {
        UploadTag {
            upload_id: Ulid::from_bytes([1; 16]),
            set_id: Ulid::from_bytes([2; 16]),
            range_id: Ulid::from_bytes([3; 16]),
            epoch: 7,
        }
    }

    #[test]
    fn tags_round_trip() {
        let encoded = tag().encode();
        assert_eq!(UploadTag::parse(&encoded), Some(tag()));
        assert!(
            encoded
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        );
    }

    #[test]
    fn plain_ids_untagged() {
        assert_eq!(
            UploadTag::parse(&Ulid::from_bytes([1; 16]).to_string()),
            None
        );
        assert_eq!(UploadTag::parse(&format!("{}-x", tag().encode())), None);
        assert_eq!(UploadTag::parse("a-b-c-1"), None);
    }
}
