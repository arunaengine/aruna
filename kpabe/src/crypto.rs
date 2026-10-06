use alloc::vec::Vec;

use bls12_381_plus::{G1Projective, Gt, Scalar, elliptic_curve_013::hash2curve::ExpandMsgXmd};
use hmac::{Hmac, KeyInit, Mac};
use rand_core::TryCryptoRng;
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, Zeroizing};

use crate::{Attribute, Error, PROFILE, frame};

const ORDER: [u8; 32] = [
    0x73, 0xed, 0xa7, 0x53, 0x29, 0x9d, 0x7d, 0x48, 0x33, 0x39, 0xd8, 0x08, 0x09, 0xa1, 0xd8, 0x05,
    0x53, 0xbd, 0xa4, 0x02, 0xff, 0xfe, 0x5b, 0xfe, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x01,
];

fn authenticate(key: &[u8], fields: &[&[u8]]) -> Result<Zeroizing<[u8; 32]>, Error> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| Error)?;
    for field in fields {
        mac.update(field);
    }
    let mut output = mac.finalize().into_bytes();
    let result = Zeroizing::new(output.as_slice().try_into().map_err(|_| Error)?);
    output.zeroize();
    Ok(result)
}

// RFC 5869 expansion, with each intermediate HMAC output cleared before release.
pub(crate) fn expand(prk: &[u8], info: &[u8], output: &mut [u8]) -> Result<(), Error> {
    if output.len() > 255 * 32 {
        return Err(Error);
    }
    let mut previous = Zeroizing::new([0u8; 32]);
    for (index, block) in output.chunks_mut(32).enumerate() {
        let prefix = if index == 0 { &[][..] } else { &previous[..] };
        let next = authenticate(prk, &[prefix, info, &[(index + 1) as u8]])?;
        block.copy_from_slice(&next[..block.len()]);
        previous.copy_from_slice(&next[..]);
    }
    Ok(())
}

pub(crate) fn derive_key(value: &Gt, context: &[u8]) -> Result<Zeroizing<[u8; 32]>, Error> {
    let bytes = Zeroizing::new(value.to_bytes());
    let prk = authenticate(b"aruna-kpabe/envelope/v1", &[&bytes[..]])?;
    let mut key = Zeroizing::new([0u8; 32]);
    expand(&prk[..], context, &mut key[..])?;
    Ok(key)
}

pub(crate) fn master_scalar(
    seed: &[u8; 32],
    context: &[u8],
    label: &[u8],
) -> Result<Scalar, Error> {
    let mut bytes = Zeroizing::new([0u8; 64]);
    for counter in 0u32..128 {
        let info = frame(&[
            PROFILE,
            b"master-scalar",
            context,
            label,
            &counter.to_be_bytes(),
        ])?;
        expand(seed, &info, &mut bytes[..])?;
        let scalar = Zeroizing::new(Scalar::from_bytes_wide(&bytes));
        if *scalar != Scalar::ZERO {
            return Ok(*scalar);
        }
    }
    Err(Error)
}

pub(crate) fn random_scalar(rng: &mut (impl TryCryptoRng + ?Sized)) -> Result<Scalar, Error> {
    let mut bytes = Zeroizing::new([0u8; 64]);
    for _ in 0..128 {
        rng.try_fill_bytes(&mut bytes[..]).map_err(|_| Error)?;
        let scalar = Zeroizing::new(Scalar::from_bytes_wide(&bytes));
        if *scalar != Scalar::ZERO {
            return Ok(*scalar);
        }
    }
    Err(Error)
}

pub(crate) fn random_pair(
    rng: &mut (impl TryCryptoRng + ?Sized),
) -> Result<Zeroizing<[Scalar; 2]>, Error> {
    let mut pair = Zeroizing::new([Scalar::ZERO; 2]);
    for _ in 0..128 {
        pair[0] = random_scalar(rng)?;
        pair[1] = random_scalar(rng)?;
        if pair[0] + pair[1] != Scalar::ZERO {
            return Ok(pair);
        }
    }
    Err(Error)
}

pub(crate) fn oracle(
    fingerprint: &[u8; 32],
    label: Option<&Attribute>,
    column: usize,
    coordinate: usize,
    component: usize,
) -> Result<G1Projective, Error> {
    let mut encoded = Vec::new();
    if let Some(label) = label {
        encoded.push(1);
        label.encode(&mut encoded);
    } else {
        encoded.push(0);
        encoded.extend_from_slice(&(column as u32).to_be_bytes());
    }
    let message = frame(&[
        fingerprint,
        &encoded,
        &[(coordinate + 1) as u8, (component + 1) as u8],
    ])?;
    let point = G1Projective::hash::<ExpandMsgXmd<sha2_010::Sha256>>(
        &message,
        b"ARUNA_KPABE_FAME_G1_XMD:SHA-256_SSWU_RO_V1",
    );
    if bool::from(point.is_identity()) {
        return Err(Error);
    }
    Ok(point)
}

pub(crate) fn fingerprint(context: &[u8], parameters: &[u8]) -> Result<[u8; 32], Error> {
    Ok(Sha256::digest(frame(&[PROFILE, context, parameters])?).into())
}

pub(crate) fn checked_target(bytes: &[u8; Gt::BYTES]) -> Result<Gt, Error> {
    let value = Option::<Gt>::from(Gt::from_bytes(bytes)).ok_or(Error)?;
    let mut power = Gt::IDENTITY;
    for byte in ORDER {
        for bit in (0..8).rev() {
            power = power.double();
            if (byte >> bit) & 1 == 1 {
                power += value;
            }
        }
    }
    if power != Gt::IDENTITY || value == Gt::IDENTITY {
        return Err(Error);
    }
    Ok(value)
}
