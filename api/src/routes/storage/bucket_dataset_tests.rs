//! Tests describing a bucket as a dataset in one step.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::structs::storage::dataset_description::describe;
use aruna_core::structs::storage::format::Compression;
use axum::extract::FromRequest;

use super::*;
use crate::routes::metadata::tests::{TestState, drain_metadata_background, setup_network_state};
use crate::routes::storage::bucket_metadata::get_bucket_metadata;

const BUCKET: &str = "lab-db";

async fn add_bucket(test: &TestState) {
    let info = BucketInfo {
        group_id: test.group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: test.auth.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    let write = StorageEffect::Write {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: BUCKET.as_bytes().to_vec().into(),
        value: info.to_bytes().unwrap().into(),
        txn_id: None,
    };
    test.state
        .get_ctx()
        .storage_handle
        .send_storage_effect(write)
        .await;
}

fn parse(body: Value) -> DescribeBucketRequest {
    serde_json::from_value(body).unwrap()
}

async fn post(test: &TestState, body: Value) -> ServerResult<DescribeBucketResponse> {
    let http = axum::http::Request::builder()
        .method("POST")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap();
    let parsed = Json::<DescribeBucketRequest>::from_request(http, &()).await;
    describe_bucket(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Path(BUCKET.to_string()),
        parsed,
    )
    .await
    .map(|(_, Json(response))| response)
}

fn body() -> Value {
    json!({
        "title": "Lab database",
        "description": "Reference genomes of the lab",
        "version": "2.1",
        "license": "CC-BY-4.0",
        "identifiers": [{"name": "DOI", "value": "10.5281/zenodo.7",
                         "url": "https://doi.org/10.5281/zenodo.7"}]
    })
}

// The crate the route writes reads back as the description it was given.
#[test]
fn crate_reads_back() {
    let request = parse(body());
    let identifiers = checked_identifiers(&request).unwrap();
    let part = "arn:aruna:realm:node:s3/lab-db/";
    let jsonld = dataset_crate(&request, &identifiers, (BUCKET, part), "2026-10-30");
    assert_eq!(jsonld["@graph"][1]["hasPart"][0]["@id"], part);
    let described = describe(&jsonld.to_string()).unwrap();
    assert_eq!(described.title.as_deref(), Some("Lab database"));
    assert_eq!(described.version.as_deref(), Some("2.1"));
    assert_eq!(
        described.license.as_deref(),
        Some("https://spdx.org/licenses/CC-BY-4.0.html")
    );
    assert_eq!(described.identifiers, identifiers);

    let empty = parse(json!({"title": " ", "description": "x"}));
    assert!(checked_identifiers(&empty).is_err());
    let unsafe_link = parse(json!({"title": "x", "description": "x",
        "identifiers": [{"name": "x", "value": "x", "url": "javascript:alert(1)"}]}));
    assert!(checked_identifiers(&unsafe_link).is_err());
}

// One request creates the dataset and links it; the bucket then reads its description from
// the dataset, and a repeated request creates nothing more.
#[tokio::test]
async fn describe_then_link() {
    let test = setup_network_state().await;
    add_bucket(&test).await;
    let created = post(&test, body()).await.unwrap();
    assert_eq!(
        created.metadata.dataset.as_deref(),
        Some(created.dataset.as_str())
    );
    assert_eq!(created.description.title.as_deref(), Some("Lab database"));

    drain_metadata_background(test.state.as_ref()).await;
    let Json(read) = get_bucket_metadata(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Path(BUCKET.to_string()),
    )
    .await
    .unwrap();
    let described = read.description.expect("read from the dataset");
    assert_eq!(
        described.description.as_deref(),
        Some("Reference genomes of the lab")
    );
    assert_eq!(described.identifiers.len(), 1);
    assert_eq!(read.description_unavailable, None);

    let again = post(&test, body()).await;
    assert!(matches!(again, Err(ServerError::Conflict(_))), "{again:?}");
    let unknown = post(
        &test,
        json!({"title": "x", "description": "x", "summary": "x"}),
    )
    .await;
    assert!(
        matches!(unknown, Err(ServerError::BadRequestReason(_))),
        "{unknown:?}"
    );
}

// A dataset the route created but could not link is deleted again, so no orphan stays.
#[tokio::test]
async fn unlinked_dataset_removed() {
    let test = setup_network_state().await;
    add_bucket(&test).await;
    let created = post(&test, body()).await.unwrap();
    let dataset = Ulid::from_string(&created.dataset).unwrap();
    drain_metadata_background(test.state.as_ref()).await;
    remove_dataset(&test.state, &test.auth, None, dataset)
        .await
        .unwrap();
    drain_metadata_background(test.state.as_ref()).await;
    let Json(read) = get_bucket_metadata(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Path(BUCKET.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(read.description, None, "the dataset is gone");
    assert!(read.description_unavailable.is_some());
}

// Only a refused link deletes the new dataset; an unknown outcome may have linked it.
#[test]
fn keeps_uncertain_links() {
    for refusal in [
        ServerError::Conflict("changed".to_string()),
        ServerError::BadRequestMessage("invalid".to_string()),
        ServerError::Forbidden,
        ServerError::NotFound,
    ] {
        assert!(refused(&refusal), "{refusal:?}");
    }
    for unknown in [
        ServerError::ServiceUnavailableReason("publisher node is unreachable".to_string()),
        ServerError::InternalError("status".to_string()),
    ] {
        assert!(!refused(&unknown), "{unknown:?}");
    }
}

// The link waits until the new dataset is readable, but only for a bounded time.
#[tokio::test(start_paused = true)]
async fn waits_until_readable() {
    let reads = std::cell::Cell::new(0);
    let later = || {
        reads.set(reads.get() + 1);
        let read = match reads.get() {
            1 | 2 => Err(DescriptionError::Unavailable),
            _ => Ok(DatasetDescription::default()),
        };
        std::future::ready(read)
    };
    assert!(becomes_readable(later).await);
    assert_eq!(reads.get(), 3);

    reads.set(0);
    let never = || {
        reads.set(reads.get() + 1);
        std::future::ready(Err(DescriptionError::Unreadable))
    };
    assert!(!becomes_readable(never).await);
    assert_eq!(reads.get(), READY_DELAYS.len() + 1);
}
