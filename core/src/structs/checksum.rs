//! Checksum algorithms with their hash keys, S3 names and digest lengths.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const HASH_BLAKE3: &str = "blake3";
pub const HASH_MD5: &str = "md5";
pub const HASH_SHA1: &str = "sha1";
pub const HASH_SHA256: &str = "sha256";
pub const HASH_CRC32: &str = "crc32";
pub const HASH_CRC32C: &str = "crc32c";
pub const HASH_CRC64NVME: &str = "crc64nvme";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    Md5,
    Sha1,
    Sha256,
    Crc32,
    Crc32c,
    Crc64Nvme,
}

impl ChecksumAlgorithm {
    pub const fn hash_key(self) -> &'static str {
        match self {
            Self::Md5 => HASH_MD5,
            Self::Sha1 => HASH_SHA1,
            Self::Sha256 => HASH_SHA256,
            Self::Crc32 => HASH_CRC32,
            Self::Crc32c => HASH_CRC32C,
            Self::Crc64Nvme => HASH_CRC64NVME,
        }
    }

    pub const fn s3_name(self) -> &'static str {
        match self {
            Self::Md5 => "Content-MD5",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Crc64Nvme => "CRC64NVME",
        }
    }

    pub const fn digest_len(self) -> usize {
        match self {
            Self::Md5 => 16,
            Self::Sha1 => 20,
            Self::Sha256 => 32,
            Self::Crc32 | Self::Crc32c => 4,
            Self::Crc64Nvme => 8,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpectedChecksum {
    pub algorithm: ChecksumAlgorithm,
    pub digest: Vec<u8>,
}

/// The first expected checksum the computed hashes do not confirm; a missing hash fails too.
pub fn unmatched_checksum<'a>(
    expected: &'a [ExpectedChecksum],
    hashes: &HashMap<String, Vec<u8>>,
) -> Option<&'a ExpectedChecksum> {
    expected
        .iter()
        .find(|checksum| hashes.get(checksum.algorithm.hash_key()) != Some(&checksum.digest))
}

/// One line of an `md5sum` or `sha256sum` manifest: the algorithm follows from the digest
/// length, and the path is relative to the manifest. Comments and escaped names are skipped.
pub fn parse_manifest_line(line: &str) -> Option<(ChecksumAlgorithm, Vec<u8>, &str)> {
    let line = line.trim_end_matches(['\r', '\n']);
    if line.starts_with(['#', '\\']) {
        return None;
    }
    let (digest, path) = line.split_once(' ')?;
    let path = path.strip_prefix([' ', '*'])?;
    let path = path.strip_prefix("./").unwrap_or(path);
    let algorithm = match digest.len() {
        32 => ChecksumAlgorithm::Md5,
        64 => ChecksumAlgorithm::Sha256,
        _ => return None,
    };
    let digest = hex::decode(digest).ok()?;
    (!path.is_empty()).then_some((algorithm, digest, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_lines_parse() {
        let md5 = "9e107d9d372bb6826bd81d3542a419d6  ./data/a.txt";
        let (algorithm, digest, path) = parse_manifest_line(md5).unwrap();
        assert_eq!(
            (algorithm, digest.len(), path),
            (ChecksumAlgorithm::Md5, 16, "data/a.txt")
        );
        let sha = format!("{} *b.bin\r", "ab".repeat(32));
        let (algorithm, _, path) = parse_manifest_line(&sha).unwrap();
        assert_eq!((algorithm, path), (ChecksumAlgorithm::Sha256, "b.bin"));
        for skipped in [
            "# comment",
            "\\9e107d9d372bb6826bd81d3542a419d6  a\\nb",
            "abc  a",
            "zz",
        ] {
            assert_eq!(parse_manifest_line(skipped), None);
        }
    }

    #[test]
    fn unmatched_names_algorithm() {
        let expected = vec![ExpectedChecksum {
            algorithm: ChecksumAlgorithm::Sha256,
            digest: vec![7; 32],
        }];
        let mut hashes = HashMap::from([(HASH_MD5.to_string(), vec![1; 16])]);
        assert_eq!(unmatched_checksum(&expected, &hashes), expected.first());
        hashes.insert(HASH_SHA256.to_string(), vec![7; 32]);
        assert_eq!(unmatched_checksum(&expected, &hashes), None);
        hashes.insert(HASH_SHA256.to_string(), vec![8; 32]);
        assert_eq!(unmatched_checksum(&expected, &hashes), expected.first());
    }
}
