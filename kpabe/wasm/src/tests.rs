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
        let mut plain = bytes(&case["user_key"]);
        let key = import_key(&parameters, &context, &fingerprint, &mut plain).unwrap();
        assert!(plain.iter().all(|byte| *byte == 0));
        let object = key.open_object(&bytes(&case["envelope"]), &bytes(&case["context"]));
        assert_eq!(object.unwrap(), bytes(&vectors["object_key"]));
        assert!(
            key.open_object(&bytes(&case["envelope"]), b"other")
                .is_err()
        );
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
    let issue = |kind: &str, scope: &str, bucket_key: &mut [u8]| {
        issue_key(
            bucket_key,
            &parameters,
            &context,
            &fingerprint,
            kind,
            scope,
            &[1],
        )
    };
    for (kind, scope, opens) in [
        ("subtree", "", true),
        ("subtree", "foo/", true),
        ("exact", "foo/file", true),
        ("subtree", "bar/", false),
        ("exact", "foo/other", false),
    ] {
        let mut bucket_key = bytes(&fixture["bucket_key"]);
        let mut plain = issue(kind, scope, &mut bucket_key).unwrap();
        assert!(bucket_key.iter().all(|byte| *byte == 0));
        let key = import_key(&parameters, &context, &fingerprint, &mut plain).unwrap();
        let object = key.open_object(&envelope.0, &envelope.1);
        assert_eq!(object.ok(), opens.then(|| bytes(&fixture["object_key"])));
    }
    for (kind, scope) in [
        ("subtree", "foo"),
        ("exact", ""),
        ("prefix", "foo/"),
        ("exact", "a\0"),
    ] {
        assert!(issue(kind, scope, &mut bytes(&fixture["bucket_key"])).is_err());
    }
    assert!(issue("subtree", "", &mut [4; 32]).is_err());
}

#[test]
#[ignore = "prints deterministic fixture data for explicit regeneration"]
fn print_fixture() {
    std::println!("KPABE_ISSUE={}", fixture());
}
