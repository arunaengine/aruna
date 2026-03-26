pub const HASH_BLAKE3: &str = "blake3";
pub const HASH_MD5: &str = "md5";
pub const HASH_SHA1: &str = "sha1";
pub const HASH_SHA256: &str = "sha256";
pub const HASH_CRC32: &str = "crc32";
pub const HASH_CRC32C: &str = "crc32c";
pub const HASH_CRC64NVME: &str = "crc64nvme";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
