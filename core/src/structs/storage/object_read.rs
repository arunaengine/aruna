//! The whole-object facts and the byte range of one object read, as every read path answers
//! them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::ops::Range;
use std::time::SystemTime;

use thiserror::Error;

use crate::structs::storage::multipart::MultipartChecksumType;

/// A requested range that no byte of the object satisfies.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("the requested range is not satisfiable")]
pub struct InvalidRange;

/// Authoritative whole-object facts of one read, carried independently of any
/// physical location. `size` is the whole-object size; a ranged read reports its
/// response length in `GetObjectResult::resolved_range` instead.
#[derive(Clone, Debug, PartialEq)]
pub struct ObjectInfo {
    pub size: u64,
    pub version_created_at: Option<SystemTime>,
    pub etag: Option<String>,
    pub checksum_type: MultipartChecksumType,
    pub hashes: HashMap<String, Vec<u8>>,
    pub composite_hashes: HashMap<String, Vec<u8>>,
    pub part_count: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObjectRangeRequest {
    StartEnd { start: u64, end: u64 },
    Start { start: u64 },
    Suffix { length: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedObjectRange {
    pub range: Range<u64>,
    pub content_length: i64,
    pub content_range: String,
}

impl ObjectRangeRequest {
    pub fn resolve(&self, full_length: u64) -> Result<ResolvedObjectRange, InvalidRange> {
        if full_length == 0 {
            return Err(InvalidRange);
        }

        let range = match self {
            ObjectRangeRequest::StartEnd { start, end } => {
                if start > end || *start >= full_length {
                    return Err(InvalidRange);
                }
                *start..((*end).min(full_length - 1) + 1)
            }
            ObjectRangeRequest::Start { start } => {
                if *start >= full_length {
                    return Err(InvalidRange);
                }
                *start..full_length
            }
            ObjectRangeRequest::Suffix { length } => {
                if *length == 0 {
                    return Err(InvalidRange);
                }
                full_length.saturating_sub(*length)..full_length
            }
        };

        Ok(ResolvedObjectRange {
            content_range: format!("bytes {}-{}/{}", range.start, range.end - 1, full_length),
            content_length: (range.end - range.start) as i64,
            range,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_range_resolves() {
        let resolved = ObjectRangeRequest::StartEnd { start: 2, end: 5 }
            .resolve(10)
            .unwrap();

        assert_eq!(resolved.range, 2..6);
        assert_eq!(resolved.content_length, 4);
        assert_eq!(resolved.content_range, "bytes 2-5/10");
    }

    #[test]
    fn suffix_range_resolves() {
        let resolved = ObjectRangeRequest::Suffix { length: 3 }
            .resolve(10)
            .unwrap();

        assert_eq!(resolved.range, 7..10);
        assert_eq!(resolved.content_length, 3);
        assert_eq!(resolved.content_range, "bytes 7-9/10");
    }

    #[test]
    fn open_range_resolves() {
        let resolved = ObjectRangeRequest::Start { start: 4 }.resolve(10).unwrap();

        assert_eq!(resolved.range, 4..10);
        assert_eq!(resolved.content_length, 6);
        assert_eq!(resolved.content_range, "bytes 4-9/10");
    }

    #[test]
    fn invalid_ranges_rejected() {
        assert_eq!(
            ObjectRangeRequest::Suffix { length: 1 }.resolve(0),
            Err(InvalidRange)
        );
        assert_eq!(
            ObjectRangeRequest::Start { start: 10 }.resolve(10),
            Err(InvalidRange)
        );
        assert_eq!(
            ObjectRangeRequest::StartEnd { start: 6, end: 5 }.resolve(10),
            Err(InvalidRange)
        );
    }
}
