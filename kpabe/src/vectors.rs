//! Checks the fixed test vectors and prints them on request.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use alloc::{vec, vec::Vec};

use rand::{SeedableRng, rngs::StdRng};
use serde_json::{Value, json};
use zeroize::Zeroizing;

use crate::encoding::encode_key;
use crate::*;

fn cases() -> Value {
    let setup_context = frame_context(&[
        b"vectors/v1",
        b"realm",
        b"node",
        b"bucket",
        &1u64.to_be_bytes(),
    ])
    .unwrap();
    let (parameters, master) = setup_from_seed(&[7; 32], &setup_context).unwrap();
    let mut cases = Vec::new();
    let prefix = Attribute::Prefix(b"foo/".to_vec());
    let exact = Attribute::Key(b"foo/file".to_vec());
    let write = Attribute::Write([9; 16]);
    for (index, (name, scopes, epochs)) in [
        ("bucket", vec![], vec![1]),
        ("prefix", vec![prefix.clone()], vec![1]),
        ("key", vec![exact.clone()], vec![1]),
        ("write", vec![write.clone()], vec![1]),
        (
            "union",
            vec![prefix.clone(), exact.clone(), write.clone()],
            vec![1, 2, 3],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let context = frame_context(&[
            b"envelope/v1",
            &setup_context,
            parameters.fingerprint(),
            &[index as u8],
        ])
        .unwrap();
        let policy = Policy::new(b"bucket", &epochs, &scopes).unwrap();
        let attributes = vec![
            Attribute::Domain(b"bucket".to_vec()),
            Attribute::Epoch(1),
            prefix.clone(),
            exact.clone(),
            write.clone(),
        ];
        let issue_seed = [index as u8 + 11; 32];
        let kem_seed = [index as u8 + 21; 32];
        let seal_seed = [index as u8 + 31; 32];
        let key = issue(
            &parameters,
            &master,
            &policy,
            &mut StdRng::from_seed(issue_seed),
        )
        .unwrap();
        let (ciphertext, shared) =
            encapsulate(&parameters, &attributes, &mut StdRng::from_seed(kem_seed)).unwrap();
        assert_eq!(
            shared.derive(&context).unwrap().as_bytes(),
            decapsulate(&parameters, &key, &ciphertext)
                .unwrap()
                .derive(&context)
                .unwrap()
                .as_bytes()
        );
        let envelope = seal(
            &parameters,
            &attributes,
            &[5; 32],
            &context,
            &mut StdRng::from_seed(seal_seed),
        )
        .unwrap();
        assert_eq!(
            open(&parameters, &key, &envelope, &context)
                .unwrap()
                .as_bytes(),
            &[5; 32]
        );
        cases.push(json!({
            "name": name, "epochs": epochs,
            "policy_labels": policy.labels().iter().map(|label| { let mut bytes = Vec::new(); label.encode(&mut bytes); hex::encode(bytes) }).collect::<Vec<_>>(),
            "context": hex::encode(context), "issue_seed": hex::encode(issue_seed),
            "kem_seed": hex::encode(kem_seed), "seal_seed": hex::encode(seal_seed),
            "user_key": hex::encode(&*encode_key(&key).unwrap()),
            "ciphertext": hex::encode(ciphertext.to_bytes().unwrap()),
            "derived_key": hex::encode(shared.derive(b"vector KDF").unwrap().as_bytes()),
            "envelope": hex::encode(envelope.to_bytes().unwrap()),
        }));
    }
    json!({
        "version": 1, "profile": core::str::from_utf8(PROFILE).unwrap(),
        "rng": "rand 0.10.2 StdRng (ChaCha12)", "seed": hex::encode([7; 32]),
        "setup_context": hex::encode(setup_context),
        "fingerprint": hex::encode(parameters.fingerprint()),
        "parameters": hex::encode(parameters.to_bytes()),
        "master_scalars": master.scalars.iter().map(|scalar| hex::encode(scalar.to_le_bytes())).collect::<Vec<_>>(),
        "object_key": hex::encode([5; 32]), "cases": cases,
    })
}

#[test]
fn fixed_vectors() {
    let expected: Value = serde_json::from_str(include_str!("../tests/vectors.json")).unwrap();
    assert_eq!(cases(), expected);
    let bytes = |value: &Value| hex::decode(value.as_str().unwrap()).unwrap();
    let context = bytes(&expected["setup_context"]);
    let fingerprint: [u8; 32] = bytes(&expected["fingerprint"]).try_into().unwrap();
    let parameters =
        PublicParameters::from_bytes(&bytes(&expected["parameters"]), &context, &fingerprint)
            .unwrap();
    for case in expected["cases"].as_array().unwrap() {
        let key = UserKey::open(&parameters, &bytes(&case["user_key"]), |value| {
            Ok(Zeroizing::new(value.to_vec()))
        })
        .unwrap();
        let ciphertext = Ciphertext::from_bytes(&parameters, &bytes(&case["ciphertext"])).unwrap();
        assert_eq!(
            hex::encode(
                decapsulate(&parameters, &key, &ciphertext)
                    .unwrap()
                    .derive(b"vector KDF")
                    .unwrap()
                    .as_bytes()
            ),
            case["derived_key"].as_str().unwrap()
        );
        let envelope = Envelope::from_bytes(&parameters, &bytes(&case["envelope"])).unwrap();
        assert_eq!(
            hex::encode(
                open(&parameters, &key, &envelope, &bytes(&case["context"]))
                    .unwrap()
                    .as_bytes()
            ),
            expected["object_key"].as_str().unwrap()
        );
    }
}

#[test]
#[ignore = "prints deterministic fixture data for explicit regeneration"]
fn print_vectors() {
    std::println!("KPABE_VECTORS={}", cases());
}
