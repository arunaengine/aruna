//! Setup, key issuance, encapsulation and decapsulation of the KP KEM.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use alloc::vec::Vec;

use bls12_381_plus::{G1Affine, G1Projective, G2Affine, G2Projective, Gt, Scalar, pairing};
use rand_core::TryCryptoRng;
use zeroize::Zeroizing;

use crate::crypto::{derive_key, fingerprint, master_scalar, oracle, random_pair, random_scalar};
use crate::policy::canonical_attributes;
use crate::{Attribute, Error, MAX_ATTRIBUTES, MAX_BYTES, Policy, check_context};

/// Public parameters bound to one profile and caller context by their fingerprint.
#[derive(Debug, Clone)]
pub struct PublicParameters {
    pub(crate) h: [G2Projective; 3],
    pub(crate) targets: [Gt; 2],
    pub(crate) fingerprint: [u8; 32],
}

impl PublicParameters {
    /// The fingerprint to admit independently of ciphertexts and requests.
    pub fn fingerprint(&self) -> &[u8; 32] {
        &self.fingerprint
    }

    pub(crate) fn body(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(3 * 96 + 2 * Gt::BYTES);
        for point in self.h {
            bytes.extend_from_slice(&point.to_compressed());
        }
        for target in self.targets {
            bytes.extend_from_slice(&target.to_bytes());
        }
        bytes
    }
}

/// Deterministic, zeroizing issuance secret. Recreate it from the caller's protected seed.
pub struct MasterSecret {
    pub(crate) scalars: Zeroizing<[Scalar; 7]>,
    pub(crate) fingerprint: [u8; 32],
}

/// A randomized policy key. Secret encodings are available only through explicit sealing.
pub struct UserKey {
    pub(crate) policy: Policy,
    pub(crate) base: Zeroizing<[G2Projective; 3]>,
    pub(crate) rows: Zeroizing<Vec<[G1Projective; 3]>>,
    pub(crate) fingerprint: [u8; 32],
}

impl UserKey {
    /// The admitted policy represented by this key.
    pub fn policy(&self) -> &Policy {
        &self.policy
    }
}

/// Canonical public KEM ciphertext with typed attribute labels.
#[derive(Debug, Clone)]
pub struct Ciphertext {
    pub(crate) attributes: Vec<Attribute>,
    pub(crate) base: [G2Projective; 3],
    pub(crate) rows: Vec<[G1Projective; 3]>,
    pub(crate) fingerprint: [u8; 32],
}

impl Ciphertext {
    /// The canonical attribute set carried by this ciphertext.
    pub fn attributes(&self) -> &[Attribute] {
        &self.attributes
    }
}

/// A zeroizing KEM shared secret, used through a context-bound HKDF key derivation.
pub struct KemKey(pub(crate) Zeroizing<Gt>);

/// An owned 32-byte key, cleared on drop and accessible only by deliberate borrowing.
pub struct SecretKey(pub(crate) Zeroizing<[u8; 32]>);

impl SecretKey {
    /// Borrows the secret bytes without copying them.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl KemKey {
    /// Derives 32 bytes with HKDF-SHA256; refuses empty or oversized contexts.
    #[doc = ""]
    /// ```compile_fail,E0277
    /// fn check(k: &aruna_kpabe::KemKey) { format!("{:?}", k.derive(b"ctx").unwrap()); }
    /// ```
    #[doc = ""]
    /// ```compile_fail,E0599
    /// fn check(k: &aruna_kpabe::KemKey) { k.derive(b"ctx").unwrap().clone(); }
    /// ```
    pub fn derive(&self, context: &[u8]) -> Result<SecretKey, Error> {
        check_context(context)?;
        derive_key(&self.0, context).map(SecretKey)
    }
}

/// Expands each labelled master scalar from a 32-byte PRK, with 512-bit LE reduction and zero retry.
/// The caller performs bucket-key clamping and HKDF first; this function binds its own context.
pub fn setup_from_seed(
    seed: &[u8; 32],
    context: &[u8],
) -> Result<(PublicParameters, MasterSecret), Error> {
    check_context(context)?;
    let mut scalars = Zeroizing::new([Scalar::ZERO; 7]);
    for (scalar, label) in scalars
        .iter_mut()
        .zip([b"a1", b"a2", b"b1", b"b2", b"d1", b"d2", b"d3"])
    {
        *scalar = master_scalar(seed, context, label)?;
    }
    let generator = Zeroizing::new(pairing(&G1Affine::generator(), &G2Affine::generator()));
    let exponents = Zeroizing::new([
        scalars[4] * scalars[0] + scalars[6],
        scalars[5] * scalars[1] + scalars[6],
    ]);
    if exponents.contains(&Scalar::ZERO) {
        return Err(Error);
    }
    let mut parameters = PublicParameters {
        h: [
            G2Projective::GENERATOR,
            G2Projective::GENERATOR * scalars[0],
            G2Projective::GENERATOR * scalars[1],
        ],
        targets: [*generator * exponents[0], *generator * exponents[1]],
        fingerprint: [0; 32],
    };
    parameters.fingerprint = fingerprint(context, &parameters.body())?;
    let master = MasterSecret {
        scalars,
        fingerprint: parameters.fingerprint,
    };
    Ok((parameters, master))
}

fn signed(point: &G1Projective, coefficient: i8) -> G1Projective {
    match coefficient {
        -1 => -point,
        1 => *point,
        _ => G1Projective::IDENTITY,
    }
}

/// Issues a randomized policy key, refusing a different parameter fingerprint or failed entropy.
pub fn issue(
    parameters: &PublicParameters,
    master: &MasterSecret,
    policy: &Policy,
    rng: &mut (impl TryCryptoRng + ?Sized),
) -> Result<UserKey, Error> {
    if parameters.fingerprint != master.fingerprint {
        return Err(Error);
    }
    let random = random_pair(rng)?;
    let r = Zeroizing::new([
        master.scalars[2] * random[0],
        master.scalars[3] * random[1],
        random[0] + random[1],
    ]);
    let inverse = Zeroizing::new([
        Option::<Scalar>::from(master.scalars[0].invert()).ok_or(Error)?,
        Option::<Scalar>::from(master.scalars[1].invert()).ok_or(Error)?,
    ]);
    let columns = if policy.alternatives.is_empty() { 2 } else { 3 };
    let mut terms = Zeroizing::new([[G1Projective::IDENTITY; 3]; 2]);
    for column in 1..columns {
        let sigma = Zeroizing::new(random_scalar(rng)?);
        for component in 0..2 {
            let mut value = Zeroizing::new(G1Projective::GENERATOR * (*sigma * inverse[component]));
            for coordinate in 0..3 {
                *value += oracle(
                    &parameters.fingerprint,
                    None,
                    column + 1,
                    coordinate,
                    component,
                )? * (r[coordinate] * inverse[component]);
            }
            terms[column - 1][component] = *value;
        }
        terms[column - 1][2] = G1Projective::GENERATOR * -*sigma;
    }
    let labels = policy.labels();
    let matrix = policy.matrix();
    let mut key = UserKey {
        policy: policy.clone(),
        base: Zeroizing::new(core::array::from_fn(|index| parameters.h[0] * r[index])),
        rows: Zeroizing::new(Vec::with_capacity(labels.len())),
        fingerprint: parameters.fingerprint,
    };
    for (label, row) in labels.iter().zip(matrix) {
        let sigma = Zeroizing::new(random_scalar(rng)?);
        let mut values = Zeroizing::new([G1Projective::IDENTITY; 3]);
        for component in 0..2 {
            values[component] = G1Projective::GENERATOR * (*sigma * inverse[component]);
            for coordinate in 0..3 {
                values[component] += oracle(
                    &parameters.fingerprint,
                    Some(label),
                    0,
                    coordinate,
                    component,
                )? * (r[coordinate] * inverse[component]);
            }
            let share = Zeroizing::new(G1Projective::GENERATOR * master.scalars[4 + component]);
            values[component] += signed(&share, row[0]);
            for column in 1..columns {
                values[component] += signed(&terms[column - 1][component], row[column]);
            }
        }
        values[2] = G1Projective::GENERATOR * -*sigma;
        let share = Zeroizing::new(G1Projective::GENERATOR * master.scalars[6]);
        values[2] += signed(&share, row[0]);
        for column in 1..columns {
            values[2] += signed(&terms[column - 1][2], row[column]);
        }
        key.rows.push(*values);
    }
    crate::encoding::encode_key(&key)?;
    Ok(key)
}

/// Encapsulates to a canonical attribute set; refuses repeats, exceeded limits or failed entropy.
pub fn encapsulate(
    parameters: &PublicParameters,
    attributes: &[Attribute],
    rng: &mut (impl TryCryptoRng + ?Sized),
) -> Result<(Ciphertext, KemKey), Error> {
    let attributes = canonical_attributes(attributes, MAX_ATTRIBUTES)?;
    let mut labels = Vec::new();
    for label in &attributes {
        label.encode(&mut labels);
    }
    if 327 + 144 * attributes.len() + labels.len() > MAX_BYTES {
        return Err(Error);
    }
    let random = random_pair(rng)?;
    let mut ciphertext = Ciphertext {
        base: [
            parameters.h[1] * random[0],
            parameters.h[2] * random[1],
            parameters.h[0] * (random[0] + random[1]),
        ],
        rows: Vec::with_capacity(attributes.len()),
        attributes,
        fingerprint: parameters.fingerprint,
    };
    for label in &ciphertext.attributes {
        let mut values = [G1Projective::IDENTITY; 3];
        for (coordinate, value) in values.iter_mut().enumerate() {
            for component in 0..2 {
                *value += oracle(
                    &parameters.fingerprint,
                    Some(label),
                    0,
                    coordinate,
                    component,
                )? * random[component];
            }
        }
        ciphertext.rows.push(values);
    }
    let key = KemKey(Zeroizing::new(
        parameters.targets[0] * random[0] + parameters.targets[1] * random[1],
    ));
    Ok((ciphertext, key))
}

fn secret_pair(first: &G1Projective, second: &G2Projective) -> Gt {
    let first = Zeroizing::new(G1Affine::from(first));
    let second = Zeroizing::new(G2Affine::from(second));
    pairing(&first, &second)
}

/// Recovers the KEM secret only for a satisfied policy under the admitted parameter fingerprint.
/// Use envelope open to authenticate ciphertexts and recovered keys.
pub fn decapsulate(
    parameters: &PublicParameters,
    key: &UserKey,
    ciphertext: &Ciphertext,
) -> Result<KemKey, Error> {
    if parameters.fingerprint != key.fingerprint || parameters.fingerprint != ciphertext.fingerprint
    {
        return Err(Error);
    }
    let selected = key.policy.select(&ciphertext.attributes)?;
    let mut public_sum = [G1Projective::IDENTITY; 3];
    let mut secret_sum = Zeroizing::new([G1Projective::IDENTITY; 3]);
    for (row, attribute) in selected {
        for coordinate in 0..3 {
            public_sum[coordinate] += ciphertext.rows[attribute][coordinate];
            secret_sum[coordinate] += key.rows[row][coordinate];
        }
    }
    let mut result = Zeroizing::new(Gt::IDENTITY);
    for coordinate in 0..3 {
        let denominator = Zeroizing::new(secret_pair(
            &secret_sum[coordinate],
            &ciphertext.base[coordinate],
        ));
        let numerator = Zeroizing::new(secret_pair(&public_sum[coordinate], &key.base[coordinate]));
        *result += *denominator - *numerator;
    }
    Ok(KemKey(result))
}
