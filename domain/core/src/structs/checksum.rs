//! Checksum algorithms with their hash keys, S3 names and digest lengths, and the hasher that
//! computes all of them in one pass.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use sha1::Digest as _;
use sha1::Sha1;
use sha2::Sha256;
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

#[derive(Debug, Clone)]
pub struct Hashes {
    pub blake3: blake3::Hash,
    pub crc32: [u8; 4],
    pub crc32c: [u8; 4],
    pub crc64nvme: [u8; 8],
    pub sha1: [u8; 20],
    pub sha256: [u8; 32],
    pub md5: [u8; 16],
}

#[derive(Clone)]
pub struct Hasher {
    blake3: blake3::Hasher,
    crc32: crc_fast::Digest,
    crc32c: crc_fast::Digest,
    crc64nvme: crc_fast::Digest,
    sha1: Sha1,
    sha256: Sha256,
    md5: md5::Context,
}

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher {
    pub fn new() -> Self {
        Hasher {
            blake3: blake3::Hasher::new(),
            crc32: crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc),
            crc32c: crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi),
            crc64nvme: crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme),
            sha1: Sha1::new(),
            sha256: Sha256::new(),
            md5: md5::Context::new(),
        }
    }

    pub fn new_with_bytes(bytes: &[u8]) -> Self {
        let mut hasher = Self::new();
        hasher.update(bytes);
        hasher
    }

    pub fn update(&mut self, bytes: &[u8]) {
        self.blake3.update(bytes);
        self.crc32.update(bytes);
        self.crc32c.update(bytes);
        self.crc64nvme.update(bytes);
        self.sha1.update(bytes);
        self.sha256.update(bytes);
        self.md5.consume(bytes);
    }

    pub fn finalize(&self) -> Hashes {
        Hashes {
            blake3: self.blake3.finalize(),
            crc32: (self.crc32.finalize() as u32).to_be_bytes(),
            crc32c: (self.crc32c.finalize() as u32).to_be_bytes(),
            crc64nvme: self.crc64nvme.finalize().to_be_bytes(),
            sha1: self.sha1.clone().finalize().into(),
            sha256: self.sha256.clone().finalize().into(),
            md5: self.md5.clone().finalize().into(),
        }
    }

    pub fn to_map(&self) -> HashMap<String, Vec<u8>> {
        let hashes = self.finalize();
        HashMap::from_iter([
            (HASH_BLAKE3.to_string(), hashes.blake3.as_bytes().to_vec()),
            (HASH_CRC32.to_string(), hashes.crc32.to_vec()),
            (HASH_CRC32C.to_string(), hashes.crc32c.to_vec()),
            (HASH_CRC64NVME.to_string(), hashes.crc64nvme.to_vec()),
            (HASH_SHA1.to_string(), hashes.sha1.to_vec()),
            (HASH_SHA256.to_string(), hashes.sha256.to_vec()),
            (HASH_MD5.to_string(), hashes.md5.to_vec()),
        ])
    }
}

/// Full-object CRC32, CRC32C and CRC64NVME of parts written one after another, from each part's
/// checksums and length. `None` when a part lacks one of them.
pub fn combine_crcs<'a>(
    parts: impl IntoIterator<Item = (&'a HashMap<String, Vec<u8>>, u64)>,
) -> Option<HashMap<String, Vec<u8>>> {
    use crc_fast::CrcAlgorithm::{Crc32Iscsi, Crc32IsoHdlc, Crc64Nvme};
    let algorithms = [
        (HASH_CRC32, Crc32IsoHdlc),
        (HASH_CRC32C, Crc32Iscsi),
        (HASH_CRC64NVME, Crc64Nvme),
    ];
    let mut combined = [None::<u64>; 3];
    for (hashes, len) in parts {
        for ((name, algorithm), total) in algorithms.iter().zip(&mut combined) {
            let digest = hashes.get(*name)?;
            let mut value = [0u8; 8];
            value.get_mut(8 - digest.len()..)?.copy_from_slice(digest);
            let value = u64::from_be_bytes(value);
            *total = Some(match *total {
                Some(left) => crc_fast::checksum_combine(*algorithm, left, value, len),
                None => value,
            });
        }
    }
    let mut out = HashMap::new();
    for ((name, _), total) in algorithms.iter().zip(combined) {
        let bytes = total?.to_be_bytes();
        let width = if *name == HASH_CRC64NVME { 0 } else { 4 };
        out.insert(name.to_string(), bytes[width..].to_vec());
    }
    Some(out)
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

    #[test]
    fn combines_part_crcs() {
        // Combined part CRCs equal the CRCs of the whole object, including an empty part.
        let parts: [&[u8]; 3] = [b"first part bytes", b"", b"and the short last"];
        let maps: Vec<_> = parts
            .iter()
            .map(|part| Hasher::new_with_bytes(part).to_map())
            .collect();
        let lens = parts.iter().map(|part| part.len() as u64);
        let combined = combine_crcs(maps.iter().zip(lens)).unwrap();
        let whole = Hasher::new_with_bytes(&parts.concat()).to_map();
        for name in [HASH_CRC32, HASH_CRC32C, HASH_CRC64NVME] {
            assert_eq!(combined[name], whole[name], "{name}");
        }
        let mut missing = maps[0].clone();
        missing.remove(HASH_CRC32C);
        assert_eq!(combine_crcs([(&missing, 16)]), None);
    }

    #[test]
    fn computes_known_checksums() {
        let hashes = Hasher::new_with_bytes(b"123456789").finalize();

        assert_eq!(hashes.crc32, 0xcbf4_3926u32.to_be_bytes());
        assert_eq!(hashes.crc32c, 0xe306_9283u32.to_be_bytes());
        assert_eq!(hashes.crc64nvme, 0xae8b_1486_0a79_9888u64.to_be_bytes());
        assert_eq!(
            hashes.sha1,
            [
                0xf7, 0xc3, 0xbc, 0x1d, 0x80, 0x8e, 0x04, 0x73, 0x2a, 0xdf, 0x67, 0x99, 0x65, 0xcc,
                0xc3, 0x4c, 0xa7, 0xae, 0x34, 0x41,
            ]
        );
    }
}
