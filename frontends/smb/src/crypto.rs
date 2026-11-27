//! SMB 3.1.1 cryptography: the SHA-512 preauthentication hash chain, the SP800-108 key
//! derivation, AES-CMAC and AES-GMAC signing and AES-GCM transform encryption.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aes::Aes128;
use aes_gcm::aead::{Nonce, Tag};
use aes_gcm::{AeadInOut, Aes128Gcm, Aes256Gcm};
use cmac::Cmac;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use crate::wire::{FLAG_RESPONSE, FLAG_SIGNED, HEADER_SIZE, TRANSFORM_SIZE, TransformHeader};

pub const HASH_SHA512: u16 = 0x0001;
pub const CIPHER_AES128_CCM: u16 = 0x0001;
pub const CIPHER_AES128_GCM: u16 = 0x0002;
pub const CIPHER_AES256_CCM: u16 = 0x0003;
pub const CIPHER_AES256_GCM: u16 = 0x0004;
pub const SIGNING_HMAC: u16 = 0x0000;
pub const SIGNING_CMAC: u16 = 0x0001;
pub const SIGNING_GMAC: u16 = 0x0002;

const SIGNING_LABEL: &[u8] = b"SMBSigningKey\0";
const CLIENT_LABEL: &[u8] = b"SMBC2SCipherKey\0";
const SERVER_LABEL: &[u8] = b"SMBS2CCipherKey\0";
const APPLICATION_LABEL: &[u8] = b"SMBAppKey\0";
const CANCEL_CODE: u16 = 0x000C;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CryptoError {
    #[error("invalid key length")]
    KeyLength,
    #[error("encryption failed")]
    Seal,
    #[error("decryption or authentication failed")]
    Open,
    #[error("the session nonce counter is exhausted")]
    NonceExhausted,
    #[error("malformed transform: {0}")]
    Transform(&'static str),
}

/// The AES-GCM transform ciphers this server accepts; CCM is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cipher {
    Aes128Gcm,
    Aes256Gcm,
}

impl Cipher {
    pub fn from_code(code: u16) -> Option<Self> {
        match code {
            CIPHER_AES128_GCM => Some(Self::Aes128Gcm),
            CIPHER_AES256_GCM => Some(Self::Aes256Gcm),
            _ => None,
        }
    }

    pub fn code(self) -> u16 {
        match self {
            Self::Aes128Gcm => CIPHER_AES128_GCM,
            Self::Aes256Gcm => CIPHER_AES256_GCM,
        }
    }

    /// Key length in bits for the encryption and decryption keys.
    pub fn key_bits(self) -> u32 {
        match self {
            Self::Aes128Gcm => 128,
            Self::Aes256Gcm => 256,
        }
    }
}

/// The signing algorithm negotiated through the signing capabilities context.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Signing {
    AesCmac,
    AesGmac,
}

impl Signing {
    pub fn from_code(code: u16) -> Option<Self> {
        match code {
            SIGNING_CMAC => Some(Self::AesCmac),
            SIGNING_GMAC => Some(Self::AesGmac),
            _ => None,
        }
    }

    pub fn code(self) -> u16 {
        match self {
            Self::AesCmac => SIGNING_CMAC,
            Self::AesGmac => SIGNING_GMAC,
        }
    }
}

/// The SHA-512 preauthentication integrity hash `H_i = SHA512(H_{i-1} || message)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreauthHash([u8; 64]);

impl Default for PreauthHash {
    fn default() -> Self {
        Self([0u8; 64])
    }
}

impl PreauthHash {
    pub fn update(&mut self, message: &[u8]) {
        let mut hasher = Sha512::new();
        hasher.update(self.0);
        hasher.update(message);
        self.0.copy_from_slice(&hasher.finalize());
    }

    pub fn value(&self) -> &[u8; 64] {
        &self.0
    }
}

fn hmac_sha256(key: &[u8], parts: &[&[u8]]) -> Result<[u8; 32], CryptoError> {
    let mut mac =
        <Hmac<Sha256> as hmac::KeyInit>::new_from_slice(key).map_err(|_| CryptoError::KeyLength)?;
    for part in parts {
        mac.update(part);
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&mac.finalize().into_bytes());
    Ok(out)
}

/// SP800-108 counter-mode KDF with HMAC-SHA256 as MS-SMB2 section 3.1.4.2 defines it.
/// `label` includes its terminating NUL; the KDF adds the separator byte itself.
pub fn derive_key(
    key: &[u8],
    label: &[u8],
    context: &[u8],
    bits: u32,
) -> Result<Zeroizing<Vec<u8>>, CryptoError> {
    let length = (bits / 8) as usize;
    let mut out = Zeroizing::new(Vec::with_capacity(length + 32));
    let mut counter: u32 = 1;
    while out.len() < length {
        let block = Zeroizing::new(hmac_sha256(
            key,
            &[
                &counter.to_be_bytes(),
                label,
                &[0u8],
                context,
                &bits.to_be_bytes(),
            ],
        )?);
        out.extend_from_slice(block.as_slice());
        counter += 1;
    }
    out.truncate(length);
    Ok(out)
}

/// The keys of one authenticated session, seen from the server side.
pub struct SessionKeys {
    pub signing: Zeroizing<Vec<u8>>,
    /// Server-to-client transform key.
    pub encryption: Zeroizing<Vec<u8>>,
    /// Client-to-server transform key.
    pub decryption: Zeroizing<Vec<u8>>,
    pub application: Zeroizing<Vec<u8>>,
}

impl std::fmt::Debug for SessionKeys {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SessionKeys(***)")
    }
}

impl SessionKeys {
    /// Derives every key from the session key and the final preauthentication hash.
    pub fn derive(
        session_key: &[u8],
        preauth: &[u8; 64],
        cipher: Cipher,
    ) -> Result<Self, CryptoError> {
        let bits = cipher.key_bits();
        Ok(Self {
            signing: derive_key(session_key, SIGNING_LABEL, preauth, 128)?,
            encryption: derive_key(session_key, SERVER_LABEL, preauth, bits)?,
            decryption: derive_key(session_key, CLIENT_LABEL, preauth, bits)?,
            application: derive_key(session_key, APPLICATION_LABEL, preauth, 128)?,
        })
    }
}

fn gcm_seal(
    cipher: Cipher,
    key: &[u8],
    nonce: [u8; 12],
    associated: &[u8],
    buffer: &mut [u8],
) -> Result<[u8; 16], CryptoError> {
    let tag = match cipher {
        Cipher::Aes128Gcm => {
            let engine = <Aes128Gcm as aes_gcm::KeyInit>::new_from_slice(key)
                .map_err(|_| CryptoError::KeyLength)?;
            engine
                .encrypt_inout_detached(&Nonce::<Aes128Gcm>::from(nonce), associated, buffer.into())
                .map_err(|_| CryptoError::Seal)?
        }
        Cipher::Aes256Gcm => {
            let engine = <Aes256Gcm as aes_gcm::KeyInit>::new_from_slice(key)
                .map_err(|_| CryptoError::KeyLength)?;
            engine
                .encrypt_inout_detached(&Nonce::<Aes256Gcm>::from(nonce), associated, buffer.into())
                .map_err(|_| CryptoError::Seal)?
        }
    };
    let mut out = [0u8; 16];
    out.copy_from_slice(&tag);
    Ok(out)
}

fn gcm_open(
    cipher: Cipher,
    key: &[u8],
    nonce: [u8; 12],
    associated: &[u8],
    buffer: &mut [u8],
    tag: [u8; 16],
) -> Result<(), CryptoError> {
    let result = match cipher {
        Cipher::Aes128Gcm => {
            let engine = <Aes128Gcm as aes_gcm::KeyInit>::new_from_slice(key)
                .map_err(|_| CryptoError::KeyLength)?;
            engine.decrypt_inout_detached(
                &Nonce::<Aes128Gcm>::from(nonce),
                associated,
                buffer.into(),
                &Tag::<Aes128Gcm>::from(tag),
            )
        }
        Cipher::Aes256Gcm => {
            let engine = <Aes256Gcm as aes_gcm::KeyInit>::new_from_slice(key)
                .map_err(|_| CryptoError::KeyLength)?;
            engine.decrypt_inout_detached(
                &Nonce::<Aes256Gcm>::from(nonce),
                associated,
                buffer.into(),
                &Tag::<Aes256Gcm>::from(tag),
            )
        }
    };
    result.map_err(|_| CryptoError::Open)
}

/// The signature of a message whose signature field is already zeroed.
fn signature_of(algorithm: Signing, key: &[u8], message: &[u8]) -> Result<[u8; 16], CryptoError> {
    match algorithm {
        Signing::AesCmac => {
            let mut mac = <Cmac<Aes128> as hmac::KeyInit>::new_from_slice(key)
                .map_err(|_| CryptoError::KeyLength)?;
            mac.update(message);
            let mut out = [0u8; 16];
            out.copy_from_slice(&mac.finalize().into_bytes());
            Ok(out)
        }
        Signing::AesGmac => {
            let flags = u32::from_le_bytes([message[16], message[17], message[18], message[19]]);
            let command = u16::from_le_bytes([message[12], message[13]]);
            let mut nonce = [0u8; 12];
            nonce[..8].copy_from_slice(&message[24..32]);
            let mut marker = 0u32;
            if flags & FLAG_RESPONSE != 0 {
                marker |= 0x1;
            }
            if command == CANCEL_CODE && flags & FLAG_RESPONSE == 0 {
                marker |= 0x2;
            }
            nonce[8..].copy_from_slice(&marker.to_le_bytes());
            gcm_seal(Cipher::Aes128Gcm, key, nonce, message, &mut [])
        }
    }
}

/// Sets the signed flag and writes the signature of one SMB2 message in place.
pub fn sign(algorithm: Signing, key: &[u8], message: &mut [u8]) -> Result<(), CryptoError> {
    if message.len() < HEADER_SIZE {
        return Err(CryptoError::Transform("message shorter than a header"));
    }
    let flags = u32::from_le_bytes([message[16], message[17], message[18], message[19]]);
    message[16..20].copy_from_slice(&(flags | FLAG_SIGNED).to_le_bytes());
    message[48..64].fill(0);
    let signature = signature_of(algorithm, key, message)?;
    message[48..64].copy_from_slice(&signature);
    Ok(())
}

/// Checks the signature of one SMB2 message in constant time.
pub fn verify(algorithm: Signing, key: &[u8], message: &[u8]) -> Result<bool, CryptoError> {
    if message.len() < HEADER_SIZE {
        return Ok(false);
    }
    let mut copy = message.to_vec();
    copy[48..64].fill(0);
    let expected = signature_of(algorithm, key, &copy)?;
    Ok(bool::from(expected.ct_eq(&message[48..64])))
}

/// Encrypts outgoing messages of one session with a strictly increasing nonce counter.
pub struct Sealer {
    cipher: Cipher,
    key: Zeroizing<Vec<u8>>,
    counter: u64,
}

impl std::fmt::Debug for Sealer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Sealer")
            .field("cipher", &self.cipher)
            .field("counter", &self.counter)
            .finish()
    }
}

impl Sealer {
    pub fn new(cipher: Cipher, key: Zeroizing<Vec<u8>>) -> Self {
        Self::starting(cipher, key, 0)
    }

    /// A sealer whose next nonce counter is `counter`; tests use it to reach the limit.
    pub(crate) fn starting(cipher: Cipher, key: Zeroizing<Vec<u8>>, counter: u64) -> Self {
        Self {
            cipher,
            key,
            counter,
        }
    }

    /// Wraps `message` in a transform header. Fails before a nonce value could repeat, so
    /// the caller must end the session on `NonceExhausted`.
    pub fn seal(&mut self, session_id: u64, message: &[u8]) -> Result<Vec<u8>, CryptoError> {
        if self.counter == u64::MAX {
            return Err(CryptoError::NonceExhausted);
        }
        let value = self.counter;
        self.counter += 1;
        let mut nonce = [0u8; 16];
        nonce[..8].copy_from_slice(&value.to_le_bytes());
        let mut header = TransformHeader {
            signature: [0u8; 16],
            nonce,
            message_size: u32::try_from(message.len())
                .map_err(|_| CryptoError::Transform("message too large"))?,
            flags: TransformHeader::ENCRYPTED,
            session_id,
        };
        let associated = header.associated();
        let mut buffer = message.to_vec();
        let mut short = [0u8; 12];
        short.copy_from_slice(&nonce[..12]);
        header.signature = gcm_seal(self.cipher, &self.key, short, &associated, &mut buffer)?;
        let mut out = Vec::with_capacity(TRANSFORM_SIZE + buffer.len());
        header.encode(&mut out);
        out.extend_from_slice(&buffer);
        Ok(out)
    }
}

/// Decrypts one transform packet with the matching session key.
pub fn unseal(cipher: Cipher, key: &[u8], packet: &[u8]) -> Result<Vec<u8>, CryptoError> {
    let header =
        TransformHeader::decode(packet).map_err(|_| CryptoError::Transform("transform header"))?;
    if header.flags != TransformHeader::ENCRYPTED {
        return Err(CryptoError::Transform("transform flags"));
    }
    let payload = &packet[TRANSFORM_SIZE..];
    if header.message_size as usize != payload.len() || payload.len() < HEADER_SIZE {
        return Err(CryptoError::Transform("original message size"));
    }
    let associated = header.associated();
    let mut buffer = payload.to_vec();
    let mut short = [0u8; 12];
    short.copy_from_slice(&header.nonce[..12]);
    gcm_open(
        cipher,
        key,
        short,
        &associated,
        &mut buffer,
        header.signature,
    )?;
    Ok(buffer)
}

#[cfg(test)]
#[path = "crypto_tests.rs"]
mod tests;
