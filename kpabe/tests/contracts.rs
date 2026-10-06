use aruna_kpabe::{
    Attribute, Envelope, Policy, PublicParameters, issue, open, seal, setup_from_seed,
};
use rand::{SeedableRng, rngs::StdRng};

#[test]
fn policy_shapes() {
    let (parameters, master) = setup_from_seed(&[7; 32], b"bucket generation").unwrap();
    let imported = PublicParameters::from_bytes(
        &parameters.to_bytes(),
        b"bucket generation",
        parameters.fingerprint(),
    )
    .unwrap();
    let alternatives = [
        Attribute::Prefix(b"foo/".to_vec()),
        Attribute::Key(b"foo/file".to_vec()),
        Attribute::Write([9; 16]),
    ];
    let mut attributes = vec![Attribute::Domain(b"bucket".to_vec()), Attribute::Epoch(2)];
    attributes.extend(alternatives.clone());
    let mut rng = StdRng::from_seed([3; 32]);
    let envelope = seal(&imported, &attributes, &[5; 32], b"write context", &mut rng).unwrap();
    let imported = Envelope::from_bytes(&imported, &envelope.to_bytes().unwrap()).unwrap();
    for scopes in [
        &[][..],
        &alternatives[..1],
        &alternatives[1..2],
        &alternatives[2..],
        &alternatives[..],
    ] {
        for epochs in [&[2][..], &[1, 2][..]] {
            let policy = Policy::new(b"bucket", epochs, scopes).unwrap();
            let key = issue(&parameters, &master, &policy, &mut rng).unwrap();
            assert_eq!(
                open(&parameters, &key, &imported, b"write context")
                    .unwrap()
                    .as_bytes(),
                &[5; 32]
            );
            assert!(open(&parameters, &key, &imported, b"wrong context").is_err());
        }
    }
    let policy = Policy::new(b"bucket", &[1, 2], &alternatives).unwrap();
    let key = issue(&parameters, &master, &policy, &mut rng).unwrap();
    for epoch in [1, 2] {
        for alternative in &alternatives {
            let attributes = [
                Attribute::Domain(b"bucket".to_vec()),
                Attribute::Epoch(epoch),
                alternative.clone(),
            ];
            let envelope = seal(&parameters, &attributes, &[5; 32], b"write", &mut rng).unwrap();
            assert_eq!(
                open(&parameters, &key, &envelope, b"write")
                    .unwrap()
                    .as_bytes(),
                &[5; 32]
            );
        }
    }
}

#[test]
fn sealed_boundary() {
    use aes_gcm_core::{Aes256Gcm, KeyInit, aead::AeadInOut};
    use aruna_kpabe::{Error, MAX_BYTES, UserKey};
    use zeroize::Zeroizing;

    let (parameters, master) = setup_from_seed(&[7; 32], b"bucket generation").unwrap();
    let mut rng = StdRng::from_seed([3; 32]);
    let cipher = Aes256Gcm::new_from_slice(&[5; 32]).unwrap();
    let seal = |plain: &[u8]| {
        let mut bytes = Zeroizing::new(plain.to_vec());
        let tag = cipher
            .encrypt_inout_detached(
                (&[9; 12]).into(),
                b"recipient context",
                bytes[..].as_mut().into(),
            )
            .map_err(|_| Error)?;
        let mut sealed = vec![9; 32];
        sealed.extend_from_slice(&bytes);
        sealed.extend_from_slice(&tag);
        Ok(sealed)
    };
    let opener = |sealed: &[u8]| {
        if sealed[..32] != [9; 32] {
            return Err(Error);
        }
        let mut bytes = Zeroizing::new(sealed[32..sealed.len() - 16].to_vec());
        cipher
            .decrypt_inout_detached(
                (&[9; 12]).into(),
                b"recipient context",
                bytes[..].as_mut().into(),
                sealed[sealed.len() - 16..].try_into().unwrap(),
            )
            .map_err(|_| Error)?;
        Ok(bytes)
    };
    for length in [MAX_BYTES - 1, MAX_BYTES] {
        let mut alternatives: Vec<_> = (0..61)
            .map(|index| Attribute::Prefix(vec![index; 900]))
            .collect();
        alternatives.push(Attribute::Prefix(vec![61; length - (MAX_BYTES - 762)]));
        let policy = Policy::new(b"bucket", &[1], &alternatives).unwrap();
        let key = issue(&parameters, &master, &policy, &mut rng).unwrap();
        let mut sealed = key
            .seal(|plain| {
                assert_eq!(plain.len(), length);
                seal(plain)
            })
            .unwrap();
        assert_eq!(sealed.len(), length + 48);
        let imported = UserKey::open(&parameters, &sealed, opener).unwrap();
        assert_eq!(imported.seal(seal).unwrap(), sealed);
        *sealed.last_mut().unwrap() ^= 1;
        assert!(UserKey::open(&parameters, &sealed, opener).is_err());
    }
    assert!(
        UserKey::open(&parameters, &vec![0; MAX_BYTES + 49], |_| {
            panic!("oversized transport reached opener")
        })
        .is_err()
    );
    assert!(
        UserKey::open(&parameters, &[], |_| {
            Ok(Zeroizing::new(vec![0; MAX_BYTES + 1]))
        })
        .is_err()
    );
}
