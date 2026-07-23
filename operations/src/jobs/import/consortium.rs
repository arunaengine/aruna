use std::collections::BTreeSet;

use aruna_blob::blob::BlobHandle;
use aruna_core::UserId;
use aruna_core::effects::BlobEffect;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::stream::BackendStream;
use aruna_core::structs::{BackendLocation, RealmId, RoCrateLimits};
use bytes::Bytes;
use futures_util::stream;
use serde_json::Value;
use ulid::Ulid;

use super::archive::{
    file_id_candidates, inspect_archive, open_archive, payload_entries, read_metadata,
    signature_entry,
};
use super::rewrite::validate_document;
use crate::staging::test_utils::setup_driver_context;

const ELABFTW: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/eln/elabftw.eln"
));
const PASTA: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/eln/pasta.eln"
));
const KADI4MAT: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/eln/kadi4mat.eln"
));

struct FixtureData {
    value: Value,
    file_ids: Vec<String>,
    paths: BTreeSet<String>,
    external: Vec<String>,
    signature: bool,
    wrapper: Option<String>,
}

async fn spool_archive(
    handle: &BlobHandle,
    bytes: &'static [u8],
    seed: u8,
) -> (BackendLocation, u64) {
    let expected_hash = *blake3::hash(bytes).as_bytes();
    let expected_size = bytes.len() as u64;
    let Event::Blob(BlobEvent::HiddenSpooled {
        location,
        blake3: actual_hash,
        size,
    }) = handle
        .send_blob_effect(BlobEffect::SpoolHidden {
            namespace: Ulid::from_bytes([seed; 16]),
            name: "consortium".to_string(),
            created_by: UserId::nil(RealmId::from_bytes([seed; 32])),
            max_bytes: Some(expected_size),
            blob: BackendStream::new(stream::iter([Ok::<Bytes, std::io::Error>(
                Bytes::from_static(bytes),
            )])),
        })
        .await
    else {
        panic!("fixture spool failed")
    };
    assert_eq!(actual_hash, expected_hash);
    assert_eq!(size, expected_size);
    (location, size)
}

async fn read_fixture(handle: &BlobHandle, bytes: &'static [u8], seed: u8) -> FixtureData {
    let limits = RoCrateLimits::default();
    let (location, size) = spool_archive(handle, bytes, seed).await;
    let inspection = inspect_archive(handle.clone(), location.clone(), size, true, &limits)
        .await
        .unwrap();
    let mut reader = open_archive(handle.clone(), location, size).await.unwrap();
    let metadata = read_metadata(
        &mut reader,
        inspection.metadata_index,
        limits.metadata_bytes,
    )
    .await
    .unwrap();
    let validated = validate_document(&metadata).unwrap();
    let payload = payload_entries(&inspection);
    let mut matched = BTreeSet::new();
    let mut external = Vec::new();
    for file_id in &validated.file_ids {
        let Some(candidates) = file_id_candidates(file_id).unwrap() else {
            external.push(file_id.clone());
            continue;
        };
        let matches = candidates
            .into_iter()
            .filter(|candidate| payload.contains_key(candidate))
            .collect::<Vec<_>>();
        assert_eq!(matches.len(), 1, "{file_id}");
        assert!(matched.insert(matches[0].clone()), "{file_id}");
    }
    let paths = payload.keys().cloned().collect::<BTreeSet<_>>();
    assert!(matched.is_subset(&paths));
    FixtureData {
        value: validated.value,
        file_ids: validated.file_ids,
        paths,
        external,
        signature: signature_entry(&inspection).is_some(),
        wrapper: inspection.wrapper,
    }
}

fn entity<'a>(value: &'a Value, id: &str) -> &'a Value {
    value["@graph"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entity| entity["@id"].as_str() == Some(id))
        .unwrap()
}

#[tokio::test]
async fn validates_corpus() {
    let context = setup_driver_context().await;
    let handle = context.driver_context.blob_handle.as_ref().unwrap();

    let elabftw = read_fixture(handle, ELABFTW, 1).await;
    assert_eq!(
        elabftw.value["@context"].as_str(),
        Some("https://w3id.org/ro/crate/1.2/context")
    );
    assert_eq!(elabftw.wrapper.as_deref(), Some("2025-09-16-103731-export"));
    let image = "./Demo - Gold-master-experiment - 4af4da4e/example.jpg";
    let json = "./Molecular-biology - Facilis-illum-sed-reprehenderit - a7658b02/autesse.json";
    assert_eq!(
        elabftw.file_ids.iter().cloned().collect::<BTreeSet<_>>(),
        BTreeSet::from([image.to_string(), json.to_string()])
    );
    assert_eq!(
        entity(&elabftw.value, image)["alternateName"].as_str(),
        Some(
            "7b/7b82e081f08828fa461979b340d28673a32773169bf19884b61276355c0d873098977ad10b68d6845209108b8470ac4b72a1992b3d81140d0ab0af9e25d886a0.jpg"
        )
    );
    assert_eq!(
        entity(&elabftw.value, json)["alternateName"].as_str(),
        Some(
            "fd/fdedffebcfbbdc8eb9a554d54951783ced67e23ac0c38445f67112bfb81543147d8960561fcd7745e3e3ec098ded2d5f86730ad635520319e502c11c5260ba2c.json"
        )
    );
    assert!(elabftw.paths.contains(image.trim_start_matches("./")));
    assert!(elabftw.paths.contains(json.trim_start_matches("./")));

    let pasta = read_fixture(handle, PASTA, 2).await;
    assert_eq!(
        pasta.value["@context"].as_str(),
        Some("https://w3id.org/ro/crate/1.1/context")
    );
    assert_eq!(pasta.wrapper.as_deref(), Some("test"));
    assert_eq!(
        entity(&pasta.value, "ro-crate-metadata.json")["additionalType"].as_str(),
        Some("https://purl.archive.org/purl/elnconsortium/eln-spec/1.1")
    );
    assert_eq!(
        pasta.external,
        vec![
            "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a4/Misc_pollen.jpg/315px-Misc_pollen.jpg"
                .to_string()
        ]
    );
    assert!(pasta.signature);
    assert!(!pasta.paths.contains("ro-crate-metadata.json.minisig"));
    assert!(pasta.paths.contains("ro-crate.pubkey"));

    let kadi4mat = read_fixture(handle, KADI4MAT, 3).await;
    assert_eq!(
        kadi4mat.value["@context"].as_str(),
        Some("https://w3id.org/ro/crate/1.1/context")
    );
    assert_eq!(kadi4mat.wrapper.as_deref(), Some("records-example"));
    assert_eq!(
        entity(&kadi4mat.value, "./")["license"].as_str(),
        Some(
            "For license information, please refer to the individual dataset nodes, if applicable."
        )
    );
    assert_eq!(
        entity(&kadi4mat.value, "./records-example/")["license"]["@id"].as_str(),
        Some("https://creativecommons.org/licenses/by/4.0/")
    );
    assert_eq!(kadi4mat.file_ids.len(), 4);
    assert_eq!(kadi4mat.paths.len(), 4);
    assert!(kadi4mat.external.is_empty());
    assert!(!kadi4mat.signature);
}
