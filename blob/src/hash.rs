use sha2::{Digest, Sha256};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Hashes {
    pub blake3: blake3::Hash,
    pub sha256: Vec<u8>,
    pub md5: Vec<u8>,
}

pub struct Hasher {
    blake3: blake3::Hasher,
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
            sha256: Sha256::new(),
            md5: md5::Context::new(),
        }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        self.blake3.update(bytes);
        self.sha256.update(bytes);
        self.md5.consume(bytes);
    }

    pub fn finalize(&self) -> Hashes {
        Hashes {
            blake3: self.blake3.finalize(),
            sha256: self.sha256.clone().finalize().to_vec(),
            md5: self.md5.clone().finalize().to_vec(),
        }
    }

    pub fn to_map(&self) -> HashMap<String, Vec<u8>> {
        HashMap::from_iter([
            (
                "blake3".to_string(),
                self.blake3.finalize().as_bytes().to_vec(),
            ),
            (
                "sha256".to_string(),
                self.sha256.clone().finalize().to_vec(),
            ),
            ("md5".to_string(), self.md5.clone().finalize().to_vec()),
        ])
    }
}
