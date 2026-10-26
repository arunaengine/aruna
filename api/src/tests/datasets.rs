//! Builds a bucket and datasets on a metadata test node: a dataset whose root names a data
//! link as a Dataset-typed part.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::metadata::{CreateMetadataRequest, CreateRoCrateRequest};
use crate::routes::metadata::documents::create_metadata_document;
use crate::routes::metadata::tests::{TestState, drain_metadata_background};
use crate::tests::distributed::{bucket_info, write};
use aruna_core::keyspaces::{AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmNodeKind,
};
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
    let rocrate = json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
                "about": { "@id": "urn:aruna-test:described" }
            },
            {
                "@id": "urn:aruna-test:described",
                "@type": "Dataset",
                "name": "Described data",
                "description": "Names one data link as a part",
                "datePublished": "2026-01-01",
                "license": { "@id": "https://creativecommons.org/licenses/by/4.0/" },
                "hasPart": [{ "@id": link }]
            },
            { "@id": link, "@type": "Dataset" }
        ]
    });
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

/// Makes the node a management node and its user a realm admin, who may curate the database
/// list.
pub(crate) async fn grant_curator(test: &TestState) {
    let realm_id = test.state.get_realm_id();
    let mut document = RealmAuthorizationDocument::default_realm_doc(realm_id);
    for role in document.roles.values_mut() {
        role.assigned_users.insert(test.auth.user_id);
    }
    let actor = Actor {
        node_id: test.state.get_node_id(),
        user_id: test.auth.user_id,
        realm_id,
    };
    let key = realm_id.as_bytes().to_vec();
    write(
        &test.state,
        AUTH_KEYSPACE,
        key.clone(),
        document.to_bytes(&actor).unwrap(),
    )
    .await;
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    config.ensure_node(test.state.get_node_id(), RealmNodeKind::Management);
    config.seed_job_control(test.state.get_node_id(), 0);
    write(
        &test.state,
        REALM_CONFIG_KEYSPACE,
        key,
        config.to_bytes(&actor).unwrap(),
    )
    .await;
}
