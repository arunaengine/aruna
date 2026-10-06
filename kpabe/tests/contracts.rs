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
