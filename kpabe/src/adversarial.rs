use alloc::{vec, vec::Vec};

use bls12_381_plus::{G1Affine, G1Projective, G2Affine, G2Projective, Gt, Scalar};
use rand::{SeedableRng, rngs::StdRng};
use rand_core::{TryCryptoRng, TryRng};
use zeroize::{Zeroize, Zeroizing};

use crate::crypto::{checked_target, expand};
use crate::encoding::{Reader, encode_key};
use crate::*;

fn fixture() -> (PublicParameters, MasterSecret, StdRng) {
    let (parameters, master) = setup_from_seed(&[7; 32], b"bucket generation").unwrap();
    (parameters, master, StdRng::from_seed([3; 32]))
}

fn facts(epoch: u64, scope: Attribute) -> Vec<Attribute> {
    vec![
        Attribute::Domain(b"bucket".to_vec()),
        Attribute::Epoch(epoch),
        scope,
    ]
}

#[test]
fn collusion_fails() {
    let (parameters, master, mut rng) = fixture();
    let scope = Attribute::Prefix(b"foo/".to_vec());
    let policy = Policy::new(b"bucket", &[2], core::slice::from_ref(&scope)).unwrap();
    let first = issue(
        &parameters,
        &master,
        &Policy::new(b"bucket", &[1], core::slice::from_ref(&scope)).unwrap(),
        &mut rng,
    )
    .unwrap();
    let second = issue(
        &parameters,
        &master,
        &Policy::new(b"bucket", &[2], &[Attribute::Prefix(b"bar/".to_vec())]).unwrap(),
        &mut rng,
    )
    .unwrap();
    let envelope = seal(
        &parameters,
        &facts(2, scope),
        &[5; 32],
        b"write context",
        &mut rng,
    )
    .unwrap();
    assert!(open(&parameters, &first, &envelope, b"write context").is_err());
    assert!(open(&parameters, &second, &envelope, b"write context").is_err());
    for base in [&first.base, &second.base] {
        for mask in 0..8 {
            let rows = (0..3)
                .map(|index| {
                    if mask & (1 << index) == 0 {
                        first.rows[index]
                    } else {
                        second.rows[index]
                    }
                })
                .collect();
            let pooled = UserKey {
                policy: policy.clone(),
                base: Zeroizing::new(**base),
                rows: Zeroizing::new(rows),
                fingerprint: parameters.fingerprint,
            };
            assert!(open(&parameters, &pooled, &envelope, b"write context").is_err());
        }
    }
}

#[test]
fn literal_scopes() {
    let (parameters, master, mut rng) = fixture();
    for (allowed, denied) in [
        (
            Attribute::Prefix(b"foo/".to_vec()),
            Attribute::Prefix(b"foobar/".to_vec()),
        ),
        (
            Attribute::Key(b"foo".to_vec()),
            Attribute::Key(b"foo/file".to_vec()),
        ),
        (Attribute::Write([1; 16]), Attribute::Write([2; 16])),
        (Attribute::Prefix(Vec::new()), Attribute::Key(Vec::new())),
        (
            Attribute::Prefix("foo//\u{03bb}/".as_bytes().to_vec()),
            Attribute::Prefix("foo/\u{03bb}/".as_bytes().to_vec()),
        ),
        (
            Attribute::Key(b"literal*".to_vec()),
            Attribute::Key(b"literal/file".to_vec()),
        ),
    ] {
        let key = issue(
            &parameters,
            &master,
            &Policy::new(b"bucket", &[1], core::slice::from_ref(&allowed)).unwrap(),
            &mut rng,
        )
        .unwrap();
        let envelope = seal(
            &parameters,
            &facts(1, allowed),
            &[5; 32],
            b"write",
            &mut rng,
        )
        .unwrap();
        assert_eq!(
            open(&parameters, &key, &envelope, b"write")
                .unwrap()
                .as_bytes(),
            &[5; 32]
        );
        let denied = seal(&parameters, &facts(1, denied), &[5; 32], b"write", &mut rng).unwrap();
        assert!(open(&parameters, &key, &denied, b"write").is_err());
    }
}

#[test]
fn epochs_and_domains() {
    let (parameters, master, mut rng) = fixture();
    let scope = Attribute::Prefix(b"foo/".to_vec());
    let envelope = seal(
        &parameters,
        &facts(2, scope.clone()),
        &[5; 32],
        b"write",
        &mut rng,
    )
    .unwrap();
    for alternatives in [&[][..], &[scope][..]] {
        let old = issue(
            &parameters,
            &master,
            &Policy::new(b"bucket", &[1], alternatives).unwrap(),
            &mut rng,
        )
        .unwrap();
        assert!(open(&parameters, &old, &envelope, b"write").is_err());
        let merged = issue(
            &parameters,
            &master,
            &Policy::new(b"bucket", &[1, 2], alternatives).unwrap(),
            &mut rng,
        )
        .unwrap();
        assert!(open(&parameters, &merged, &envelope, b"write").is_ok());
        let foreign = issue(
            &parameters,
            &master,
            &Policy::new(b"another bucket", &[2], alternatives).unwrap(),
            &mut rng,
        )
        .unwrap();
        assert!(open(&parameters, &foreign, &envelope, b"write").is_err());
    }
}

#[test]
fn points_are_checked() {
    let mut torsion = [0u8; 48];
    torsion[0] = 0x80;
    let point = G1Affine::from_compressed_unchecked(&torsion).unwrap();
    assert!(bool::from(point.is_on_curve()));
    assert!(!bool::from(point.is_torsion_free()));
    assert!(Reader(&torsion).g1().is_err());
    let mut found = false;
    for coordinate in 0..32 {
        let mut bytes = [0u8; 96];
        bytes[0] = 0x80;
        bytes[95] = coordinate;
        if let Some(point) = Option::<G2Affine>::from(G2Affine::from_compressed_unchecked(&bytes))
            && !bool::from(point.is_torsion_free())
        {
            assert!(Reader(&bytes).g2().is_err());
            found = true;
            break;
        }
    }
    assert!(found);
    assert!(Reader(&[0xff; 48]).g1().is_err());
    assert!(Reader(&[0xff; 96]).g2().is_err());
    assert!(
        Reader(&G2Projective::IDENTITY.to_compressed())
            .g2()
            .is_err()
    );
    let mut noncanonical = G1Projective::IDENTITY.to_compressed();
    noncanonical[47] = 1;
    assert!(Reader(&noncanonical).g1().is_err());
    assert!(checked_target(&[0u8; Gt::BYTES]).is_err());
    assert!(checked_target(&Gt::IDENTITY.to_bytes()).is_err());
    let mut outsider = Gt::IDENTITY.to_bytes();
    outsider[47] = 2;
    assert!(bool::from(Gt::from_bytes(&outsider).is_some()));
    assert!(checked_target(&outsider).is_err());
    assert!(checked_target(&[0xff; Gt::BYTES]).is_err());
}

#[test]
fn encoding_refusals() {
    let (parameters, master, mut rng) = fixture();
    let policy = Policy::new(b"bucket", &[1], &[]).unwrap();
    let key = issue(&parameters, &master, &policy, &mut rng).unwrap();
    let (ciphertext, _) = encapsulate(&parameters, &policy.labels(), &mut rng).unwrap();
    let bytes = ciphertext.to_bytes().unwrap();
    for cut in 0..bytes.len() {
        assert!(Ciphertext::from_bytes(&parameters, &bytes[..cut]).is_err());
    }
    for index in [0, 3, 4, 5, 37, 38, 39] {
        let mut invalid = bytes.clone();
        invalid[index] ^= 0xff;
        assert!(Ciphertext::from_bytes(&parameters, &invalid).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(Ciphertext::from_bytes(&parameters, &trailing).is_err());
    let mut reversed = ciphertext.clone();
    reversed.attributes.reverse();
    assert!(Ciphertext::from_bytes(&parameters, &reversed.to_bytes().unwrap()).is_err());
    reversed.attributes[1] = reversed.attributes[0].clone();
    assert!(Ciphertext::from_bytes(&parameters, &reversed.to_bytes().unwrap()).is_err());
    let import = |bytes: &[u8]| {
        UserKey::open(&parameters, bytes, |value| {
            Ok(Zeroizing::new(value.to_vec()))
        })
    };
    let encoded = encode_key(&key).unwrap();
    let imported = import(&encoded).unwrap();
    assert_eq!(
        encode_key(&imported).unwrap().as_slice(),
        encoded.as_slice()
    );
    for cut in [0, 37, encoded.len() - 1] {
        assert!(import(&encoded[..cut]).is_err());
    }
    let mut trailing = encoded.to_vec();
    trailing.push(0);
    assert!(import(&trailing).is_err());
    let mut invalid = encoded.to_vec();
    invalid[48..50].copy_from_slice(&17u16.to_be_bytes());
    assert!(import(&invalid).is_err());
    let mut substituted = parameters.to_bytes();
    substituted[37..133].copy_from_slice(&parameters.h[1].to_compressed());
    assert!(
        PublicParameters::from_bytes(&substituted, b"bucket generation", parameters.fingerprint())
            .is_err()
    );
    substituted = parameters.to_bytes();
    substituted.push(0);
    assert!(
        PublicParameters::from_bytes(&substituted, b"bucket generation", parameters.fingerprint())
            .is_err()
    );
}

#[test]
fn policy_limits() {
    let (parameters, master, mut rng) = fixture();
    assert!(Policy::new(b"", &[1], &[]).is_err());
    assert!(Policy::new(b"bucket", &[], &[]).is_err());
    assert!(Policy::new(b"bucket", &[0], &[]).is_err());
    assert!(Policy::new(b"bucket", &[1, 1], &[]).is_err());
    assert!(Policy::new(b"bucket", &(1..=17).collect::<Vec<_>>(), &[]).is_err());
    let repeated = Attribute::Prefix(b"foo/".to_vec());
    assert!(Policy::new(b"bucket", &[1], &[repeated.clone(), repeated]).is_err());
    assert!(Policy::new(b"bucket", &[1], &[Attribute::Epoch(1)]).is_err());
    let scopes: Vec<_> = (0..63).map(|index| Attribute::Write([index; 16])).collect();
    assert!(Policy::new(b"bucket", &[1], &scopes).is_err());
    let policy = Policy::new(b"bucket", &[1], &scopes[..62]).unwrap();
    assert!(issue(&parameters, &master, &policy, &mut rng).is_ok());
    let maximum: Vec<_> = (0..64).map(|index| Attribute::Write([index; 16])).collect();
    assert!(encapsulate(&parameters, &maximum, &mut rng).is_ok());
    assert!(
        encapsulate(
            &parameters,
            &[maximum.clone(), vec![Attribute::Epoch(1)]].concat(),
            &mut rng
        )
        .is_err()
    );
    assert!(encapsulate(&parameters, &[], &mut rng).is_err());
    assert!(
        encapsulate(
            &parameters,
            &[Attribute::Epoch(1), Attribute::Epoch(1)],
            &mut rng
        )
        .is_err()
    );
    assert!(
        encapsulate(
            &parameters,
            &[Attribute::Key(vec![1; MAX_ATTRIBUTE_BYTES + 1])],
            &mut rng
        )
        .is_err()
    );
    let oversized: Vec<_> = (0..16)
        .map(|index| Attribute::Key(vec![index; 4000]))
        .collect();
    assert!(encapsulate(&parameters, &oversized, &mut rng).is_err());
    let oversized = Policy::new(b"bucket", &[1], &oversized).unwrap();
    assert!(issue(&parameters, &master, &oversized, &mut rng).is_err());
    assert!(Ciphertext::from_bytes(&parameters, &vec![0; MAX_BYTES + 1]).is_err());
    assert!(setup_from_seed(&[7; 32], &[]).is_err());
    assert!(setup_from_seed(&[7; 32], &vec![0; MAX_CONTEXT_BYTES + 1]).is_err());
    assert!(setup_from_seed(&[7; 32], &vec![0; MAX_CONTEXT_BYTES]).is_ok());
    assert_ne!(
        frame_context(&[b"a", b"bc"]).unwrap(),
        frame_context(&[b"ab", b"c"]).unwrap()
    );
    assert!(frame_context(&[&[0; MAX_CONTEXT_BYTES]]).is_err());
    assert!(frame_context(&[&[][..]; 65]).is_err());
}

#[test]
fn envelope_binding() {
    let (parameters, master, mut rng) = fixture();
    let policy = Policy::new(b"bucket", &[1], &[]).unwrap();
    let key = issue(&parameters, &master, &policy, &mut rng).unwrap();
    let envelope = seal(&parameters, &policy.labels(), &[5; 32], b"write", &mut rng).unwrap();
    for index in [0, 31, 32, 47] {
        let mut corrupted = envelope.clone();
        corrupted.sealed[index] ^= 1;
        assert!(open(&parameters, &key, &corrupted, b"write").is_err());
    }
    let mut corrupted = envelope.clone();
    corrupted.nonce[0] ^= 1;
    assert!(open(&parameters, &key, &corrupted, b"write").is_err());
    let shared = decapsulate(&parameters, &key, &envelope.ciphertext).unwrap();
    let aes_key = shared.derive(b"write").unwrap();
    use aes_gcm_core::{Aes256Gcm, KeyInit, aead::AeadInOut};
    let cipher = Aes256Gcm::new_from_slice(aes_key.as_bytes()).unwrap();
    let mut plaintext = [5u8; 32];
    let tag = cipher
        .encrypt_inout_detached(
            (&corrupted.nonce).into(),
            b"foreign data",
            plaintext[..].as_mut().into(),
        )
        .unwrap();
    corrupted.sealed[..32].copy_from_slice(&plaintext);
    corrupted.sealed[32..].copy_from_slice(&tag);
    assert!(open(&parameters, &key, &corrupted, b"write").is_err());
    let (other, _) = setup_from_seed(&[7; 32], b"another generation").unwrap();
    assert_ne!(other.fingerprint(), parameters.fingerprint());
    assert!(issue(&other, &master, &policy, &mut rng).is_err());
    assert!(open(&other, &key, &envelope, b"write").is_err());
    assert!(
        PublicParameters::from_bytes(
            &parameters.to_bytes(),
            b"another generation",
            parameters.fingerprint()
        )
        .is_err()
    );
    let mut bytes = envelope.to_bytes().unwrap();
    bytes.push(0);
    assert!(Envelope::from_bytes(&parameters, &bytes).is_err());
    bytes.truncate(40);
    assert!(Envelope::from_bytes(&parameters, &bytes).is_err());
}

struct BrokenEntropy;

impl TryRng for BrokenEntropy {
    type Error = Error;
    fn try_next_u32(&mut self) -> Result<u32, Error> {
        Err(Error)
    }
    fn try_next_u64(&mut self) -> Result<u64, Error> {
        Err(Error)
    }
    fn try_fill_bytes(&mut self, _: &mut [u8]) -> Result<(), Error> {
        Err(Error)
    }
}
impl TryCryptoRng for BrokenEntropy {}

#[test]
fn secret_storage() {
    let (parameters, mut master, mut rng) = fixture();
    let policy = Policy::new(b"bucket", &[1], &[]).unwrap();
    let mut key = issue(&parameters, &master, &policy, &mut rng).unwrap();
    assert!(issue(&parameters, &master, &policy, &mut BrokenEntropy).is_err());
    assert!(encapsulate(&parameters, &policy.labels(), &mut BrokenEntropy).is_err());
    assert!(key.seal::<()>(|_| Err(Error)).is_err());
    master.scalars.zeroize();
    assert!(master.scalars.iter().all(|scalar| *scalar == Scalar::ZERO));
    key.base.zeroize();
    assert!(key.base.iter().all(|point| bool::from(point.is_identity())));
    key.rows.zeroize();
    assert!(key.rows.is_empty());
}

#[test]
fn hkdf_vector() {
    let prk =
        hex::decode("077709362c2e32df0ddc3f0dc47bba6390b6c73bb50f9c3122ec844ad7c2b3e5").unwrap();
    let info = hex::decode("f0f1f2f3f4f5f6f7f8f9").unwrap();
    let mut output = [0u8; 42];
    expand(&prk, &info, &mut output).unwrap();
    assert_eq!(
        hex::encode(output),
        "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"
    );
}
