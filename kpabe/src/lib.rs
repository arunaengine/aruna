//! FAME KP KEM from Agrawal and Chase (2017), Appendix B, Figure B.1.
//! Callers admit parameter fingerprints and supply canonical contexts and cryptographic entropy.
//! Production use requires external review of this crate, its protocol and its pairing dependency.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

mod crypto;
mod encoding;
mod envelope;
mod policy;
mod scheme;

pub use envelope::{Envelope, open, seal};
pub use policy::{Attribute, Policy};
pub use scheme::{Ciphertext, KemKey, MasterSecret, PublicParameters, UserKey};
pub use scheme::{decapsulate, encapsulate, issue, setup_from_seed};

/// Maximum attributes in one ciphertext.
pub const MAX_ATTRIBUTES: usize = 64;
/// Maximum LSSS rows in one key.
pub const MAX_ROWS: usize = 64;
/// Maximum admitted epochs in one policy.
pub const MAX_EPOCHS: usize = 16;
/// Maximum bytes in one attribute value.
pub const MAX_ATTRIBUTE_BYTES: usize = 4096;
/// Maximum bytes in one canonical context.
pub const MAX_CONTEXT_BYTES: usize = 8192;
/// Maximum bytes in one encoded value.
pub const MAX_BYTES: usize = 65536;

const PROFILE: &[u8] = b"aruna-kpabe/FAME-KP/BLS12-381/XMD-SHA256/HKDF-SHA256/AES256GCM/v1";

/// A refused input, policy, encoding, entropy source or authenticated ciphertext.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid KP-ABE input")]
pub struct Error;

/// Frame context fields as a field count followed by big-endian u32 lengths and bytes.
/// Rejects more than 64 fields or a result exceeding the context byte limit.
pub fn frame_context(fields: &[&[u8]]) -> Result<alloc::vec::Vec<u8>, Error> {
    if fields.len() > MAX_ATTRIBUTES {
        return Err(Error);
    }
    let output = frame(fields)?;
    if output.len() > MAX_CONTEXT_BYTES {
        return Err(Error);
    }
    Ok(output)
}

fn frame(fields: &[&[u8]]) -> Result<alloc::vec::Vec<u8>, Error> {
    let size = fields.iter().try_fold(4usize, |size, field| {
        size.checked_add(4)?.checked_add(field.len())
    });
    if size.is_none_or(|size| size > MAX_BYTES) {
        return Err(Error);
    }
    let mut output = alloc::vec::Vec::with_capacity(size.ok_or(Error)?);
    output.extend_from_slice(&(fields.len() as u32).to_be_bytes());
    for field in fields {
        output.extend_from_slice(&(field.len() as u32).to_be_bytes());
        output.extend_from_slice(field);
    }
    Ok(output)
}

fn check_context(context: &[u8]) -> Result<(), Error> {
    if context.is_empty() || context.len() > MAX_CONTEXT_BYTES {
        return Err(Error);
    }
    Ok(())
}
