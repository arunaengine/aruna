//! Canonical, bounded byte encodings of parameters, ciphertexts and sealed user keys.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use alloc::vec::Vec;

use bls12_381_plus::{G1Projective, G2Projective, Gt};
use zeroize::Zeroizing;

use crate::crypto::{checked_target, fingerprint};
use crate::policy::canonical_attributes;
use crate::{Attribute, Ciphertext, Error, MAX_ATTRIBUTE_BYTES, MAX_ATTRIBUTES, MAX_BYTES};
use crate::{MAX_EPOCHS, MAX_ROWS, Policy, PublicParameters, UserKey, check_context};

pub(crate) fn header(kind: u8, fingerprint: &[u8; 32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(37);
    bytes.extend_from_slice(&[b'A', b'K', b'P', 1, kind]);
    bytes.extend_from_slice(fingerprint);
    bytes
}

pub(crate) fn bounded(bytes: Vec<u8>) -> Result<Vec<u8>, Error> {
    if bytes.len() > MAX_BYTES {
        return Err(Error);
    }
    Ok(bytes)
}

pub(crate) struct Reader<'a>(pub(crate) &'a [u8]);

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8], kind: u8, fingerprint: &[u8; 32]) -> Result<Self, Error> {
        if bytes.len() > MAX_BYTES {
            return Err(Error);
        }
        let mut reader = Self(bytes);
        if reader.take(5)? != [b'A', b'K', b'P', 1, kind] || reader.take(32)? != fingerprint {
            return Err(Error);
        }
        Ok(reader)
    }

    pub(crate) fn take(&mut self, length: usize) -> Result<&'a [u8], Error> {
        let (value, remaining) = self.0.split_at_checked(length).ok_or(Error)?;
        self.0 = remaining;
        Ok(value)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        self.take(N)?.try_into().map_err(|_| Error)
    }

    fn count(&mut self, limit: usize) -> Result<usize, Error> {
        let count = usize::from(u16::from_be_bytes(self.array()?));
        if count > limit {
            return Err(Error);
        }
        Ok(count)
    }

    fn attribute(&mut self) -> Result<Attribute, Error> {
        let tag = self.take(1)?[0];
        let length = u32::from_be_bytes(self.array()?) as usize;
        if length > MAX_ATTRIBUTE_BYTES {
            return Err(Error);
        }
        let value = self.take(length)?;
        let attribute = match tag {
            b'd' => Attribute::Domain(value.to_vec()),
            b'e' => Attribute::Epoch(u64::from_be_bytes(value.try_into().map_err(|_| Error)?)),
            b'p' => Attribute::Prefix(value.to_vec()),
            b'k' => Attribute::Key(value.to_vec()),
            b'w' => Attribute::Write(value.try_into().map_err(|_| Error)?),
            _ => return Err(Error),
        };
        attribute.validate()?;
        Ok(attribute)
    }

    pub(crate) fn g1(&mut self) -> Result<G1Projective, Error> {
        let bytes = Zeroizing::new(self.array()?);
        Option::<G1Projective>::from(G1Projective::from_compressed(&bytes)).ok_or(Error)
    }

    pub(crate) fn g2(&mut self) -> Result<G2Projective, Error> {
        let bytes = Zeroizing::new(self.array()?);
        let point = Zeroizing::new(
            Option::<G2Projective>::from(G2Projective::from_compressed(&bytes)).ok_or(Error)?,
        );
        if bool::from(point.is_identity()) {
            return Err(Error);
        }
        Ok(*point)
    }

    pub(crate) fn finish(self) -> Result<(), Error> {
        if !self.0.is_empty() {
            return Err(Error);
        }
        Ok(())
    }
}

impl PublicParameters {
    /// Encodes the fixed profile version, fingerprint and canonical checked group values.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = header(1, &self.fingerprint);
        bytes.extend_from_slice(&self.body());
        bytes
    }

    /// Imports canonical prime-order parameters against an independently admitted fingerprint.
    /// Refuses context substitution, identity parameters, invalid points and trailing bytes.
    pub fn from_bytes(bytes: &[u8], context: &[u8], expected: &[u8; 32]) -> Result<Self, Error> {
        check_context(context)?;
        let mut reader = Reader::new(bytes, 1, expected)?;
        let h = [reader.g2()?, reader.g2()?, reader.g2()?];
        if h[0] != G2Projective::GENERATOR {
            return Err(Error);
        }
        let targets = [
            checked_target(&reader.array::<{ Gt::BYTES }>()?)?,
            checked_target(&reader.array::<{ Gt::BYTES }>()?)?,
        ];
        reader.finish()?;
        let parameters = Self {
            h,
            targets,
            fingerprint: *expected,
        };
        if fingerprint(context, &parameters.body())? != *expected {
            return Err(Error);
        }
        Ok(parameters)
    }
}

impl Ciphertext {
    /// Encodes a canonical, versioned KEM ciphertext within the byte limit.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = header(2, &self.fingerprint);
        bytes.extend_from_slice(&(self.attributes.len() as u16).to_be_bytes());
        for attribute in &self.attributes {
            attribute.encode(&mut bytes);
        }
        for point in self.base {
            bytes.extend_from_slice(&point.to_compressed());
        }
        for row in &self.rows {
            for point in row {
                bytes.extend_from_slice(&point.to_compressed());
            }
        }
        bounded(bytes)
    }

    /// Imports canonical, checked points under the admitted parameters, with no trailing bytes.
    pub fn from_bytes(parameters: &PublicParameters, bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, 2, &parameters.fingerprint)?;
        let count = reader.count(MAX_ATTRIBUTES)?;
        let mut attributes = Vec::with_capacity(count);
        for _ in 0..count {
            attributes.push(reader.attribute()?);
        }
        if canonical_attributes(&attributes, MAX_ATTRIBUTES)? != attributes {
            return Err(Error);
        }
        let base = [reader.g2()?, reader.g2()?, reader.g2()?];
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            rows.push([reader.g1()?, reader.g1()?, reader.g1()?]);
        }
        reader.finish()?;
        Ok(Self {
            attributes,
            base,
            rows,
            fingerprint: parameters.fingerprint,
        })
    }
}

pub(crate) fn encode_key(key: &UserKey) -> Result<Zeroizing<Vec<u8>>, Error> {
    let mut bytes = Zeroizing::new(header(3, &key.fingerprint));
    Attribute::Domain(key.policy.domain.clone()).encode(&mut bytes);
    bytes.extend_from_slice(&(key.policy.epochs.len() as u16).to_be_bytes());
    for epoch in &key.policy.epochs {
        bytes.extend_from_slice(&epoch.to_be_bytes());
    }
    bytes.extend_from_slice(&(key.policy.alternatives.len() as u16).to_be_bytes());
    for alternative in &key.policy.alternatives {
        alternative.encode(&mut bytes);
    }
    let size = bytes.len() + 288 + 144 * key.rows.len();
    if size > MAX_BYTES {
        return Err(Error);
    }
    bytes.reserve_exact(288 + 144 * key.rows.len());
    for point in key.base.iter() {
        let encoded = Zeroizing::new(point.to_compressed());
        bytes.extend_from_slice(&encoded[..]);
    }
    for row in key.rows.iter() {
        for point in row {
            let encoded = Zeroizing::new(point.to_compressed());
            bytes.extend_from_slice(&encoded[..]);
        }
    }
    Ok(bytes)
}

impl UserKey {
    /// Gives a temporary encoding to a caller's sealing function, then clears it on every exit.
    /// The callback must encrypt it with recipient and context authentication before returning.
    pub fn seal<T>(&self, seal: impl FnOnce(&[u8]) -> Result<T, Error>) -> Result<T, Error> {
        let bytes = encode_key(self)?;
        seal(&bytes)
    }

    /// Opens an explicitly sealed key using a caller's authenticated opener and checked decoding.
    /// The opener must return a zeroizing plaintext and authenticate its recipient and context.
    /// Allows 32 encapsulation bytes and a 16-byte authentication tag beyond the encoding limit.
    pub fn open(
        parameters: &PublicParameters,
        sealed: &[u8],
        open: impl FnOnce(&[u8]) -> Result<Zeroizing<Vec<u8>>, Error>,
    ) -> Result<Self, Error> {
        if sealed.len() > MAX_BYTES + 32 + 16 {
            return Err(Error);
        }
        let bytes = open(sealed)?;
        let mut reader = Reader::new(&bytes, 3, &parameters.fingerprint)?;
        let Attribute::Domain(domain) = reader.attribute()? else {
            return Err(Error);
        };
        let epoch_count = reader.count(MAX_EPOCHS)?;
        let mut epochs = Vec::with_capacity(epoch_count);
        for _ in 0..epoch_count {
            epochs.push(u64::from_be_bytes(reader.array()?));
        }
        let count = reader.count(MAX_ROWS.saturating_sub(1 + epoch_count))?;
        let mut alternatives = Vec::with_capacity(count);
        for _ in 0..count {
            alternatives.push(reader.attribute()?);
        }
        let policy = Policy::new(&domain, &epochs, &alternatives)?;
        if policy.epochs != epochs || policy.alternatives != alternatives {
            return Err(Error);
        }
        let mut key = Self {
            policy,
            base: Zeroizing::new([G2Projective::IDENTITY; 3]),
            rows: Zeroizing::new(Vec::with_capacity(1 + epoch_count + count)),
            fingerprint: parameters.fingerprint,
        };
        for point in key.base.iter_mut() {
            *point = reader.g2()?;
        }
        for _ in 0..(1 + epoch_count + count) {
            let mut row = Zeroizing::new([G1Projective::IDENTITY; 3]);
            for point in row.iter_mut() {
                *point = reader.g1()?;
            }
            key.rows.push(*row);
        }
        reader.finish()?;
        Ok(key)
    }
}
