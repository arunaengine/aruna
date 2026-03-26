use aruna_core::structs::checksum::{
    HASH_BLAKE3, HASH_CRC32, HASH_CRC32C, HASH_CRC64NVME, HASH_MD5, HASH_SHA1, HASH_SHA256,
};
use sha1::Digest as _;
use sha1::Sha1;
use sha2::Digest as _;
use sha2::Sha256;
use std::collections::HashMap;

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

#[cfg(test)]
mod tests {
    use super::Hasher;

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
