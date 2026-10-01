//! Seals small secrets to an X25519 public key with HPKE (RFC 9180 base mode).
//! The suite is DHKEM(X25519, HKDF-SHA256), HKDF-SHA256 and AES-256-GCM.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use hpke::aead::AesGcm256;
use hpke::kdf::HkdfSha256;
use hpke::kem::X25519HkdfSha256;
use hpke::{Deserializable, OpModeR, OpModeS, Serializable};
use thiserror::Error;
use zeroize::Zeroizing;

type Kem = X25519HkdfSha256;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum KeySealError {
    #[error("the public key is not a valid X25519 key")]
    PublicKey,
    #[error("sealing failed")]
    Seal,
    /// Wrong private key, info, associated data or tampered ciphertext.
    #[error("the sealed secret does not open")]
    Open,
}

/// An HPKE result: the encapsulated ephemeral key and the AES-GCM ciphertext.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedSecret {
    pub enc: [u8; 32],
    pub ciphertext: Vec<u8>,
}

/// Seals `plain` to `public` with a fresh ephemeral key from the system RNG.
pub fn seal_to(
    public: &[u8; 32],
    info: &[u8],
    aad: &[u8],
    plain: &[u8],
) -> Result<SealedSecret, KeySealError> {
    let public = public_key(public)?;
    let sealed = hpke::single_shot_seal::<AesGcm256, HkdfSha256, Kem>(
        &OpModeS::Base,
        &public,
        info,
        plain,
        aad,
    );
    sealed_secret(sealed)
}

/// Opens a sealed secret with the recipient's X25519 private key.
pub fn open_sealed(
    private: &[u8; 32],
    sealed: &SealedSecret,
    info: &[u8],
    aad: &[u8],
) -> Result<Zeroizing<Vec<u8>>, KeySealError> {
    let private =
        <Kem as hpke::Kem>::PrivateKey::from_bytes(private).map_err(|_| KeySealError::Open)?;
    let enc =
        <Kem as hpke::Kem>::EncappedKey::from_bytes(&sealed.enc).map_err(|_| KeySealError::Open)?;
    hpke::single_shot_open::<AesGcm256, HkdfSha256, Kem>(
        &OpModeR::Base,
        &private,
        &enc,
        info,
        &sealed.ciphertext,
        aad,
    )
    .map(Zeroizing::new)
    .map_err(|_| KeySealError::Open)
}

fn public_key(public: &[u8; 32]) -> Result<<Kem as hpke::Kem>::PublicKey, KeySealError> {
    <Kem as hpke::Kem>::PublicKey::from_bytes(public).map_err(|_| KeySealError::PublicKey)
}

fn sealed_secret(
    sealed: Result<(<Kem as hpke::Kem>::EncappedKey, Vec<u8>), hpke::HpkeError>,
) -> Result<SealedSecret, KeySealError> {
    let (enc, ciphertext) = sealed.map_err(|_| KeySealError::Seal)?;
    let enc: [u8; 32] = enc
        .to_bytes()
        .as_slice()
        .try_into()
        .map_err(|_| KeySealError::Seal)?;
    Ok(SealedSecret { enc, ciphertext })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    /// Hands out the vector's ephemeral input keying material, so a seal is reproducible.
    struct FixedRng(Vec<u8>);

    impl hpke::rand_core::TryRng for FixedRng {
        type Error = Infallible;

        fn try_next_u32(&mut self) -> Result<u32, Infallible> {
            unreachable!("HPKE asks for key bytes only")
        }

        fn try_next_u64(&mut self) -> Result<u64, Infallible> {
            unreachable!("HPKE asks for key bytes only")
        }

        fn try_fill_bytes(&mut self, dst: &mut [u8]) -> Result<(), Infallible> {
            dst.copy_from_slice(&self.0.drain(..dst.len()).collect::<Vec<_>>());
            Ok(())
        }
    }

    impl hpke::rand_core::TryCryptoRng for FixedRng {}

    struct Vector {
        private: [u8; 32],
        public: [u8; 32],
        ikm: Vec<u8>,
        sealed: SealedSecret,
        info: Vec<u8>,
        aad: Vec<u8>,
        plain: Vec<u8>,
    }

    fn vector() -> Vector {
        let all: serde_json::Value =
            serde_json::from_str(include_str!("../tests/vectors/vault.json")).unwrap();
        let field = |name: &str| hex::decode(all["hpke"][name].as_str().unwrap()).unwrap();
        Vector {
            private: field("recipient_private").try_into().unwrap(),
            public: field("recipient_public").try_into().unwrap(),
            ikm: field("ephemeral_ikm"),
            sealed: SealedSecret {
                enc: field("enc").try_into().unwrap(),
                ciphertext: field("ciphertext"),
            },
            info: field("info"),
            aad: field("aad"),
            plain: field("plaintext"),
        }
    }

    #[test]
    fn opens_vector_secret() {
        let vector = vector();
        let plain = open_sealed(&vector.private, &vector.sealed, &vector.info, &vector.aad)
            .expect("vector opens");
        assert_eq!(plain.as_slice(), vector.plain.as_slice());
    }

    #[test]
    fn reproduces_vector_seal() {
        // A second implementation must produce the same bytes from the same ephemeral input.
        let vector = vector();
        let public = public_key(&vector.public).unwrap();
        let sealed = sealed_secret(
            hpke::single_shot_seal_with_rng::<AesGcm256, HkdfSha256, Kem>(
                &OpModeS::Base,
                &public,
                &vector.info,
                &vector.plain,
                &vector.aad,
                &mut FixedRng(vector.ikm.clone()),
            ),
        )
        .unwrap();
        assert_eq!(sealed, vector.sealed);
    }

    #[test]
    fn binds_seal_context() {
        let vector = vector();
        let sealed = seal_to(&vector.public, b"purpose a", b"object a", b"secret").unwrap();
        assert_eq!(
            open_sealed(&vector.private, &sealed, b"purpose a", b"object a")
                .unwrap()
                .as_slice(),
            b"secret"
        );
        for (info, aad) in [(b"purpose b", b"object a"), (b"purpose a", b"object b")] {
            assert_eq!(
                open_sealed(&vector.private, &sealed, info, aad),
                Err(KeySealError::Open)
            );
        }
        let mut tampered = sealed;
        tampered.ciphertext[0] ^= 1;
        assert_eq!(
            open_sealed(&vector.private, &tampered, b"purpose a", b"object a"),
            Err(KeySealError::Open)
        );
    }
}
