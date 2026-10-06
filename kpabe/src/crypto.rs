use alloc::vec::Vec;

use bls12_381_plus::{G1Projective, Gt, Scalar, elliptic_curve_013::hash2curve::ExpandMsgXmd};
use rand_core::TryCryptoRng;
use sha2::{
    Digest, Sha256,
    block_api::Sha256VarCore,
    digest::block_api::{Buffer, UpdateCore, VariableOutputCore},
};
use zeroize::{Zeroize, Zeroizing};

use crate::{Attribute, Error, PROFILE, frame};

const ORDER: [u8; 32] = [
    0x73, 0xed, 0xa7, 0x53, 0x29, 0x9d, 0x7d, 0x48, 0x33, 0x39, 0xd8, 0x08, 0x09, 0xa1, 0xd8, 0x05,
    0x53, 0xbd, 0xa4, 0x02, 0xff, 0xfe, 0x5b, 0xfe, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x01,
];

fn hash_into<'a>(
    fields: impl IntoIterator<Item = &'a [u8]>,
    output: &mut Zeroizing<[u8; 32]>,
) -> Result<(), Error> {
    let mut hash = Sha256VarCore::new(output.len()).map_err(|_| Error)?;
    let mut buffer = Buffer::<Sha256VarCore>::default();
    for field in fields {
        buffer.digest_blocks(field, |blocks| hash.update_blocks(blocks));
    }
    hash.finalize_variable_core(&mut buffer, (&mut **output).into());
    Ok(())
}

pub(crate) fn authenticate(key: &[u8], fields: &[&[u8]]) -> Result<Zeroizing<[u8; 32]>, Error> {
    let mut padded = Zeroizing::new([0u8; 64]);
    let mut digest = Zeroizing::new([0u8; 32]);
    if key.len() > padded.len() {
        hash_into([key], &mut digest)?;
        padded[..32].copy_from_slice(&digest[..]);
        digest.zeroize();
    } else {
        padded[..key.len()].copy_from_slice(key);
    }
    for byte in padded.iter_mut() {
        *byte ^= 0x36;
    }
    hash_into(
        core::iter::once(&padded[..]).chain(fields.iter().copied()),
        &mut digest,
    )?;
    for byte in padded.iter_mut() {
        *byte ^= 0x36 ^ 0x5c;
    }
    let mut result = Zeroizing::new([0u8; 32]);
    hash_into([&padded[..], &digest[..]], &mut result)?;
    padded.zeroize();
    digest.zeroize();
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

#[cfg(test)]
mod tests {
    use alloc::vec;

    use super::*;

    #[test]
    fn sha256_boundaries() {
        for length in [0, 1, 55, 56, 63, 64, 65, 119, 120, 127, 128, 129] {
            let input = vec![0xab; length];
            let expected = Sha256::digest(&input);
            for split in 0..=length {
                let mut output = Zeroizing::new([0xa5; 32]);
                hash_into([&input[..split], &[], &input[split..]], &mut output).unwrap();
                assert_eq!(&output[..], &expected[..], "{length} bytes at {split}");
            }
        }
    }

    #[test]
    fn hmac_vectors() {
        for (key, data, expected) in [
            (
                vec![0x0b; 20],
                b"Hi There".to_vec(),
                "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7",
            ),
            (
                b"Jefe".to_vec(),
                b"what do ya want for nothing?".to_vec(),
                "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843",
            ),
            (
                vec![0xaa; 20],
                vec![0xdd; 50],
                "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe",
            ),
            (
                (1..=25).collect(),
                vec![0xcd; 50],
                "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b",
            ),
            (
                vec![0x0c; 20],
                b"Test With Truncation".to_vec(),
                "a3b6167473100ee06e0c796c2955552b",
            ),
            (
                vec![0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First".to_vec(),
                "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54",
            ),
            (
                vec![0xaa; 131],
                b"This is a test using a larger than block-size key and a larger than block-size data. The key needs to be hashed before being used by the HMAC algorithm.".to_vec(),
                "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2",
            ),
        ] {
            let length = expected.len() / 2;
            let output = authenticate(&key, &[&data]).unwrap();
            assert_eq!(hex::encode(&output[..length]), expected);
            let (first, second) = data.split_at(data.len() / 2);
            let split = authenticate(&key, &[first, &[], second]).unwrap();
            assert_eq!(&split[..], &output[..]);
        }
    }
}
