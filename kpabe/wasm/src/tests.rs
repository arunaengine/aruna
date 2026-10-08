//! Tests the browser binding against crate vectors, issued scopes and refused policies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_kpabe::{Attribute, frame_context, seal};
use rand::{SeedableRng, rngs::StdRng};
use serde_json::{Value, json};

use super::*;

/// X25519 public key of the fixture object key `[5; 32]`.
const PUBLIC_KEY: [u8; 32] = [
    0x50, 0xa6, 0x14, 0x09, 0xb1, 0xdd, 0xd0, 0x32, 0x5e, 0x9b, 0x16, 0xb7, 0x00, 0xe7, 0x19, 0xe9,
    0x77, 0x2c, 0x07, 0x00, 0x0b, 0x1b, 0xd7, 0x78, 0x6e, 0x90, 0x7c, 0x65, 0x3d, 0x20, 0x49, 0x5d,
];

fn bytes(value: &Value) -> Vec<u8> {
    hex::decode(value.as_str().unwrap()).unwrap()
}

fn fixture() -> Value {
    let bucket_key = [3u8; 32];
    let generation = 1u64.to_be_bytes();
    let fields: [&[u8]; 5] = [
        b"aruna bucket ABE v1",
        &[1; 32],
        &[2; 32],
        &[4; 16],
        &generation,
    ];
    let context = frame_context(&fields).unwrap();
    let (parameters, _) = derive_master(&bucket_key, &context).unwrap();
    let object = "foo/file";
    let envelope_context = frame_context(&[
        b"aruna object envelope v1",
        &[1; 32],
        &[2; 32],
        &[4; 16],
        &generation,
        parameters.fingerprint(),
        &1u64.to_be_bytes(),
        object.as_bytes(),
        &[9; 16],
        &PUBLIC_KEY,
    ])
    .unwrap();
    let attributes = [
        Attribute::Domain(blake3::hash(&context).as_bytes().to_vec()),
        Attribute::Epoch(1),
        Attribute::Key(object.into()),
        Attribute::Write([9; 16]),
        Attribute::Prefix(Vec::new()),
        Attribute::Prefix(b"foo/".to_vec()),
    ];
    let mut rng = StdRng::from_seed([6; 32]);
    let envelope = seal(
        &parameters,
        &attributes,
        &[5; 32],
        &envelope_context,
        &mut rng,
    )
    .unwrap();
    json!({
        "bucket_key": hex::encode(bucket_key), "setup_context": hex::encode(&context),
        "parameters": hex::encode(parameters.to_bytes()),
        "fingerprint": hex::encode(parameters.fingerprint()), "object": object, "epoch": 1,
        "envelope_context": hex::encode(envelope_context),
        "envelope": hex::encode(envelope.to_bytes().unwrap()), "object_key": hex::encode([5; 32]),
    })
}

#[test]
fn crate_vectors() {
    let vectors: Value = serde_json::from_str(include_str!("../../tests/vectors.json")).unwrap();
    let (parameters, context) = (
        bytes(&vectors["parameters"]),
        bytes(&vectors["setup_context"]),
    );
    let fingerprint = bytes(&vectors["fingerprint"]);
    for case in vectors["cases"].as_array().unwrap() {
        let key = open_key(
            &parameters,
            &context,
            &fingerprint,
            &bytes(&case["user_key"]),
        )
        .unwrap();
        let object = key.open(&bytes(&case["envelope"]), &bytes(&case["context"]));
        assert_eq!(
            object.unwrap().as_bytes()[..],
            bytes(&vectors["object_key"])
        );
        assert!(key.open(&bytes(&case["envelope"]), b"other").is_err());
    }
}

#[test]
fn issued_scopes() {
    let fixture = fixture();
    assert_eq!(
        fixture,
        serde_json::from_str::<Value>(include_str!("../tests/issue.json")).unwrap()
    );
    let (parameters, context) = (
        bytes(&fixture["parameters"]),
        bytes(&fixture["setup_context"]),
    );
    let fingerprint = bytes(&fixture["fingerprint"]);
    let envelope = (
        bytes(&fixture["envelope"]),
        bytes(&fixture["envelope_context"]),
    );
    let issue = |kind: &str, scope: &str, bucket_key: &[u8]| {
        issue_scope(
            bucket_key,
            &parameters,
            &context,
            &fingerprint,
            kind,
            scope,
            &[1],
        )
        .map(|key| key.seal(|bytes| Ok(bytes.to_vec())).unwrap())
    };
    for (kind, scope, opens) in [
        ("subtree", "", true),
        ("subtree", "foo/", true),
        ("exact", "foo/file", true),
        ("subtree", "bar/", false),
        ("exact", "foo/other", false),
        ("writes", &"09".repeat(16), true),
        (
            "writes",
            &format!("{},{}", "0a".repeat(16), "09".repeat(16)),
            true,
        ),
        ("writes", &"0a".repeat(16), false),
    ] {
        let mut plain = issue(kind, scope, &bytes(&fixture["bucket_key"])).unwrap();
        let import = |kind, scope, epochs: &[u64], plain: &mut [u8]| {
            import_key(
                &parameters,
                &context,
                &fingerprint,
                kind,
                scope,
                epochs,
                plain,
            )
        };
        let key = import(kind, scope, &[1], &mut plain).unwrap();
        assert!(plain.iter().all(|byte| *byte == 0));
        let object = key.open(&envelope.0, &envelope.1);
        let object = object.ok().map(|key| key.as_bytes().to_vec());
        assert_eq!(object, opens.then(|| bytes(&fixture["object_key"])));
    }
    for (kind, scope) in [
        ("subtree", "foo"),
        ("exact", ""),
        ("prefix", "foo/"),
        ("exact", "a\0"),
        ("writes", ""),
        ("writes", "09"),
        ("writes", &format!("{0},{0}", "09".repeat(16))),
    ] {
        assert!(issue(kind, scope, &bytes(&fixture["bucket_key"])).is_err());
    }
    // An enumerated key names writes of one epoch only.
    assert!(policy(&context, "writes", &"09".repeat(16), &[1, 2]).is_err());
    assert!(issue("subtree", "", &[4; 32]).is_err());
}

#[test]
fn refuses_other_policy() {
    let fixture = fixture();
    let (parameters, context) = (
        bytes(&fixture["parameters"]),
        bytes(&fixture["setup_context"]),
    );
    let fingerprint = bytes(&fixture["fingerprint"]);
    let key = issue_scope(
        &bytes(&fixture["bucket_key"]),
        &parameters,
        &context,
        &fingerprint,
        "subtree",
        "foo/",
        &[1],
    )
    .unwrap();
    let plain = key.seal(|bytes| Ok(bytes.to_vec())).unwrap();
    for (kind, scope, epochs) in [
        ("subtree", "", &[1][..]),
        ("exact", "foo/", &[1]),
        ("subtree", "foo/", &[1, 2]),
    ] {
        let mut copy = plain.clone();
        let imported = import_key(
            &parameters,
            &context,
            &fingerprint,
            kind,
            scope,
            epochs,
            &mut copy,
        );
        assert!(imported.is_err());
        assert!(copy.iter().all(|byte| *byte == 0));
    }
}

#[test]
#[ignore = "prints deterministic fixture data for explicit regeneration"]
fn print_fixture() {
    std::println!("KPABE_ISSUE={}", fixture());
}
