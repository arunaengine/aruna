use crate::io::io_handler::Hashes;
use sha2::{Digest, Sha256};

pub struct Hasher {
    blake3: blake3::Hasher,
    sha256: Sha256,
    md5: md5::Context,
}

impl Hasher {
    pub fn new() -> Self {
        Hasher {
            blake3: blake3::Hasher::new(),
            sha256: sha2::Sha256::new(),
            md5: md5::Context::new(),
        }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        self.blake3.update(bytes);
        self.sha256.update(bytes);
        self.md5.consume(bytes);
    }

    pub fn finalize(&mut self) -> anyhow::Result<Hashes> {
        Ok(Hashes {
            blake3: self.blake3.finalize(),
            sha256: format!("{:x}", self.sha256.clone().finalize()),
            md5: format!("{:x}", self.md5.clone().compute()),
        })
    }
}
