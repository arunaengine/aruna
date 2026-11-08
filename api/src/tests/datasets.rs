//! Builds a bucket and datasets on a metadata test node: a dataset whose root names data
//! links as parts, by default one Dataset-typed link.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::metadata::{CreateMetadataRequest, CreateRoCrateRequest};
use crate::routes::metadata::documents::create_metadata_document;
use crate::routes::metadata::tests::{TestState, drain_metadata_background};
use crate::tests::distributed::{bucket_info, write};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use axum::extract::State;
use axum::{Extension, Json};
use serde_json::json;
use ulid::Ulid;

/// An ordinary bucket of the node's group.
pub(crate) async fn store_bucket(test: &TestState, bucket: &str) {
    let info = bucket_info(test.group_id, None);
    let key = bucket.as_bytes().to_vec();
    write(
        &test.state,
        S3_BUCKET_KEYSPACE,
        key,
        info.to_bytes().unwrap(),
    )
    .await;
}

/// A dataset at `path` whose root names `link` as a part, with the link postings written.
pub(crate) async fn describe(test: &TestState, path: &str, public: bool, link: &str) -> Ulid {
    describe_parts(test, path, public, &[(link, "Dataset")]).await
}

/// A dataset at `path` whose root names each `(id, type)` of `parts` as a part.
pub(crate) async fn describe_parts(
    test: &TestState,
    path: &str,
    public: bool,
    parts: &[(&str, &str)],
) -> Ulid {
    let ids: Vec<_> = parts.iter().map(|(id, _)| json!({ "@id": id })).collect();
    let mut graph = vec![
        json!({
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
            "about": { "@id": "urn:aruna-test:described" }
        }),
        json!({
            "@id": "urn:aruna-test:described",
            "@type": "Dataset",
            "name": "Described data",
            "description": "Names one data link as a part",
            "datePublished": "2026-01-01",
            "license": { "@id": "https://creativecommons.org/licenses/by/4.0/" },
            "hasPart": ids
        }),
    ];
    graph.extend(
        parts
            .iter()
            .map(|(id, kind)| json!({ "@id": id, "@type": kind })),
    );
    let rocrate = json!({ "@context": "https://w3id.org/ro/crate/1.2/context", "@graph": graph });
    store_document(test, path, public, rocrate).await
}

async fn store_document(
    test: &TestState,
    path: &str,
    public: bool,
    rocrate: serde_json::Value,
) -> Ulid {
    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::RoCrate(CreateRoCrateRequest {
            group_id: test.group_id.to_string(),
            path: path.to_string(),
            public,
            rocrate,
            message: None,
            storage_location: None,
        })),
    )
    .await
    .unwrap();
    drain_metadata_background(test.state.as_ref()).await;
    Ulid::from_string(&created.summary.document_id).unwrap()
}
