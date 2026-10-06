use alloc::vec::Vec;

use aes_gcm_core::{Aes256Gcm, KeyInit, aead::AeadInOut};
use rand_core::TryCryptoRng;
use zeroize::Zeroizing;

use crate::encoding::{Reader, bounded, header};
use crate::{Attribute, Ciphertext, Error, PublicParameters, SecretKey, UserKey, check_context};
use crate::{decapsulate, encapsulate};

/// A public KEM ciphertext and an authenticated, sealed 32-byte object private key.
#[derive(Debug, Clone)]
pub struct Envelope {
    pub(crate) ciphertext: Ciphertext,
    pub(crate) nonce: [u8; 12],
    pub(crate) sealed: [u8; 48],
}

impl Envelope {
    /// Encodes the envelope within the byte limit, including its KEM ciphertext length.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let ciphertext = self.ciphertext.to_bytes()?;
        let mut bytes = header(4, &self.ciphertext.fingerprint);
        bytes.extend_from_slice(&(ciphertext.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&ciphertext);
        bytes.extend_from_slice(&self.nonce);
        bytes.extend_from_slice(&self.sealed);
        bounded(bytes)
    }

    /// Imports the checked KEM ciphertext and exact nonce and sealed-key lengths.
    pub fn from_bytes(parameters: &PublicParameters, bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, 4, &parameters.fingerprint)?;
        let length = u32::from_be_bytes(reader.array()?) as usize;
        let ciphertext = Ciphertext::from_bytes(parameters, reader.take(length)?)?;
        let envelope = Self {
            ciphertext,
            nonce: reader.array()?,
            sealed: reader.array()?,
        };
        reader.finish()?;
        Ok(envelope)
    }
}

/// Encapsulates and seals exactly 32 object-key bytes using context as HKDF info and AEAD data.
/// Refuses invalid attributes, context, exceeded limits or failed caller entropy.
pub fn seal(
    parameters: &PublicParameters,
    attributes: &[Attribute],
    object_key: &[u8; 32],
    context: &[u8],
    rng: &mut (impl TryCryptoRng + ?Sized),
) -> Result<Envelope, Error> {
    check_context(context)?;
    let (ciphertext, shared) = encapsulate(parameters, attributes, rng)?;
    let key = shared.derive(context)?;
    let cipher = Aes256Gcm::new_from_slice(key.as_bytes()).map_err(|_| Error)?;
    let mut envelope = Envelope {
        ciphertext,
        nonce: [0; 12],
        sealed: [0; 48],
    };
    rng.try_fill_bytes(&mut envelope.nonce).map_err(|_| Error)?;
    let mut plaintext = Zeroizing::new(*object_key);
    let tag = cipher
        .encrypt_inout_detached(
            (&envelope.nonce).into(),
            context,
            plaintext[..].as_mut().into(),
        )
        .map_err(|_| Error)?;
    envelope.sealed[..32].copy_from_slice(&plaintext[..]);
    envelope.sealed[32..].copy_from_slice(&tag);
    envelope.to_bytes()?;
    Ok(envelope)
}

/// Authenticates the policy, context and associated data and returns a zeroizing object key.
/// All failures use the same error and expose no partially recovered plaintext.

/// ```compile_fail,E0277
/// let debug = |p, k, e| format!("{:?}", aruna_kpabe::open(p, k, e, b"ctx").unwrap());
/// ```

/// ```compile_fail,E0599
/// let clone = |p, k, e| aruna_kpabe::open(p, k, e, b"ctx").unwrap().clone();
/// ```
pub fn open(
    parameters: &PublicParameters,
    key: &UserKey,
    envelope: &Envelope,
    context: &[u8],
) -> Result<SecretKey, Error> {
    check_context(context)?;
    let shared = decapsulate(parameters, key, &envelope.ciphertext)?;
    let key = shared.derive(context)?;
    let cipher = Aes256Gcm::new_from_slice(key.as_bytes()).map_err(|_| Error)?;
    let mut plaintext = Zeroizing::new([0u8; 32]);
    plaintext.copy_from_slice(&envelope.sealed[..32]);
    cipher
        .decrypt_inout_detached(
            (&envelope.nonce).into(),
            context,
            plaintext[..].as_mut().into(),
            envelope.sealed[32..].try_into().map_err(|_| Error)?,
        )
        .map_err(|_| Error)?;
    Ok(SecretKey(plaintext))
}
