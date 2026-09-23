//! Tests the metadata routes for documents, queries, references, RO-Crate and validation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::documents::*;
use super::query::*;
use super::references::*;
use super::rocrate::*;
use super::validation::*;
use crate::auth::ValidatedBearer;
use crate::error::{ServerError, ServerResult};
use crate::metadata::*;
use crate::server::state::ServerState;
use crate::tests::routes::{
    seed_group_docs, seed_realm_auth, test_context, test_state, test_storage,
};
use aruna_core::keys::generate_signing_key;
use aruna_core::metadata::MetadataQueryResults;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::{MetaResourceId, StructuredId};
use aruna_operations::driver::drive;
use aruna_operations::metadata::api::{
    ApiQueryMode, MetadataApiError, MetadataQueryRequest, MetadataSearchRequest, load_realm_nodes,
    query_metadata as run_query_metadata, search_metadata as run_search_metadata,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use ulid::Ulid;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, GROUP_KEYSPACE,
    PATHS_INDEX_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, TASK_TIMER_KEYSPACE,
};
use aruna_core::metadata::{
    GraphLifecycleRecord, MaterializationState, MaterializationStatusRecord, MetadataDeleteRecord,
    MetadataLifecycleRecord,
};
use aruna_core::storage_entries::{materialization_status_entry, registry_delete_entries};
use aruna_core::structs::identity::auth::{NodeCapabilities, TokenClaims};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::structs::placement::record::METADATA_HANDLE;
use aruna_core::structs::storage::blob::{
    BackendRef, BlobHeadKey, BlobVersion, BucketInfo, CurrentVersionPointer, HashIndex, VersionKey,
};
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::task::{PersistedTaskTimer, TaskKey};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::DriverContext;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::metadata::materialization_queue::process_materialization_batch;
use aruna_operations::metadata::projector::{
    drain_projection_queue, replay_event_log, schedule_projection_drain,
};
use aruna_operations::metadata::prune_queue::{process_graph_tombstones, prune_jobs_exist};
use aruna_operations::metadata::repository::{write_document_lifecycle, write_graph_lifecycle};
use aruna_operations::realm::announce_presence::{
    AnnouncePresenceConfig, AnnouncePresenceOperation,
};
use aruna_operations::sync::incoming::initialize_incoming_fixture;
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::json;
use std::collections::HashSet;
use std::time::SystemTime;
use tempfile::TempDir;

async fn state_realm_nodes(state: &ServerState) -> ServerResult<Vec<aruna_core::NodeId>> {
    let ctx = state.get_ctx();
    Ok(load_realm_nodes(ctx.as_ref(), state.get_realm_id(), state.get_node_id()).await)
}

struct TestState {
    _storage_dir: TempDir,
    _metadata_dir: TempDir,
    auth: AuthContext,
    group_id: Ulid,
    state: Arc<ServerState>,
}

fn projected(response: MetadataRoCrateResponse) -> ProjectedRoCrateResponse {
    match response {
        MetadataRoCrateResponse::Projected(response) => response,
        MetadataRoCrateResponse::Raw(_) => panic!("expected projected RO-Crate response"),
    }
}

async fn write_status(state: &ServerState, status: &MaterializationStatusRecord) {
    let (key_space, key, value) = materialization_status_entry(status).unwrap();
    match state
        .get_ctx()
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected status write: {other:?}"),
    }
}

fn installed_metadata_handle(context: &DriverContext) -> &MetadataHandle {
    let DriverContext {
        metadata_handle, ..
    } = context;
    metadata_handle.as_ref().expect("metadata handle installed")
}

#[tokio::test]
async fn public_routes_work() {
    let test = setup_network_state().await;

    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/public-dataset".to_string(),
            name: "Public Dataset".to_string(),
            description: "Visible metadata".to_string(),
            date_published: "2026-01-01".to_string(),
            license: None,
            public: true,
        })),
    )
    .await
    .unwrap();

    let document_id = created.summary.document_id.clone();

    let (_, Json(listed)) = list_metadata_documents(
        State(test.state.clone()),
        Extension(None),
        Path(test.group_id.to_string()),
        Query(ListMetadataQuery::default()),
    )
    .await
    .unwrap();
    assert!(listed.documents.is_empty());

    let fetched = get_metadata_document(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(document_id.clone()),
    )
    .await;
    assert!(matches!(fetched, Err(ServerError::NotFound)));

    drain_metadata_background(test.state.as_ref()).await;
    let (_, Json(listed)) = list_metadata_documents(
        State(test.state.clone()),
        Extension(None),
        Path(test.group_id.to_string()),
        Query(ListMetadataQuery::default()),
    )
    .await
    .unwrap();
    assert_eq!(listed.documents.len(), 1);
    assert_eq!(listed.documents[0].document_id, created.summary.document_id);

    let raw = export_metadata_rocrate(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(document_id.clone()),
        Query(RoCrateExportParams {
            view: Some(MetadataRoCrateView::Raw),
            limit: None,
            offset: None,
            after: None,
        }),
    )
    .await;
    assert!(matches!(raw, Err(ServerError::NotFound)));

    let paged_jsonld = format!(
        r#"{{
  "@context": "https://w3id.org/ro/crate/1.2/context",
  "@graph": [
    {{
      "@id": "ro-crate-metadata.json",
      "@type": "CreativeWork",
      "conformsTo": {{"@id": "https://w3id.org/ro/crate/1.2"}},
      "about": {{"@id": "https://w3id.org/aruna/{document_id}"}}
    }},
    {{
      "@id": "https://w3id.org/aruna/{document_id}",
      "@type": "Dataset",
      "name": "Public Dataset",
      "description": "Visible metadata",
      "datePublished": "2026-01-01",
      "license": {{"@id": "https://creativecommons.org/licenses/by/4.0/"}},
      "hasPart": [
        {{"@id": "./data/file-0.txt"}},
        {{"@id": "./data/file-1.txt"}},
        {{"@id": "./data/file-2.txt"}}
      ]
    }},
    {{
      "@id": "./data/file-0.txt",
      "@type": "File",
      "name": "file-0"
    }},
    {{
      "@id": "./data/file-1.txt",
      "@type": "File",
      "name": "file-1"
    }},
    {{
      "@id": "./data/file-2.txt",
      "@type": "File",
      "name": "file-2"
    }}
  ]
}}"#
    );

    let _ = replace_metadata_rocrate(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Path(document_id.clone()),
        Json(ReplaceRoCrateRequest {
            rocrate: serde_json::from_str(&paged_jsonld).unwrap(),
            public: Some(true),
        }),
    )
    .await
    .unwrap();

    let (_, Json(raw)) = export_metadata_rocrate(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(document_id.clone()),
        Query(RoCrateExportParams {
            view: Some(MetadataRoCrateView::Raw),
            limit: None,
            offset: None,
            after: None,
        }),
    )
    .await
    .unwrap();
    let MetadataRoCrateResponse::Raw(raw) = raw else {
        panic!("expected raw RO-Crate response");
    };
    assert_eq!(raw.projection_state, "pending");
    assert!(raw.raw.to_string().contains("file-2.txt"));
    assert_eq!(raw.context_digest.len(), 64);
    assert_eq!(raw.dataset_digest.as_deref().map(str::len), Some(64));
    assert!(raw.projected_event_id.is_none());
    let projected_jsonld = installed_metadata_handle(test.state.get_ctx().as_ref())
        .export_rocrate_jsonld(created.summary.graph_iri.clone())
        .await
        .unwrap();
    assert!(!projected_jsonld.contains("file-2.txt"));
    let failed_event_id = Ulid::from_string(&raw.winning_event_id).unwrap();
    let mut failed_status = MaterializationStatusRecord {
        document_id: Ulid::from_string(&document_id).unwrap(),
        event_id: failed_event_id,
        graph_iri: created.summary.graph_iri.clone(),
        context_digest: Some(
            hex::decode(&raw.context_digest)
                .unwrap()
                .try_into()
                .unwrap(),
        ),
        dataset_digest: raw
            .dataset_digest
            .as_deref()
            .map(hex::decode)
            .transpose()
            .unwrap()
            .map(|digest| digest.try_into().unwrap()),
        state: MaterializationState::Failed,
        attempts: 1,
        failures: 0,
        last_error: Some("projection failed".to_string()),
        updated_at_ms: 1,
    };
    write_status(test.state.as_ref(), &failed_status).await;
    let (_, Json(failed)) = export_metadata_rocrate(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(document_id.clone()),
        Query(RoCrateExportParams {
            view: Some(MetadataRoCrateView::Raw),
            limit: None,
            offset: None,
            after: None,
        }),
    )
    .await
    .unwrap();
    let MetadataRoCrateResponse::Raw(failed) = failed else {
        panic!("expected failed raw RO-Crate response");
    };
    assert_eq!(failed.projection_state, "failed");
    assert!(failed.projected_event_id.is_none());
    failed_status.state = MaterializationState::Pending;
    failed_status.attempts = 0;
    failed_status.last_error = None;
    write_status(test.state.as_ref(), &failed_status).await;

    drain_metadata_background(test.state.as_ref()).await;

    let (_, Json(bound)) = export_metadata_rocrate(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(document_id.clone()),
        Query(RoCrateExportParams {
            view: Some(MetadataRoCrateView::Raw),
            limit: None,
            offset: None,
            after: None,
        }),
    )
    .await
    .unwrap();
    let MetadataRoCrateResponse::Raw(bound) = bound else {
        panic!("expected materialized raw RO-Crate response");
    };
    assert_eq!(bound.projected_event_id, Some(bound.winning_event_id));

    let (_, Json(response)) = export_metadata_rocrate(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(document_id.clone()),
        Query(RoCrateExportParams::default()),
    )
    .await
    .unwrap();
    let response = projected(response);
    assert!(
        response
            .rocrate
            .to_string()
            .contains(&format!("https://w3id.org/aruna/{document_id}"))
    );

    let (_, Json(summary)) = export_metadata_rocrate(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(document_id.clone()),
        Query(RoCrateExportParams {
            view: Some(MetadataRoCrateView::Summary),
            limit: None,
            offset: None,
            after: None,
        }),
    )
    .await
    .unwrap();
    let summary = projected(summary);
    assert!(summary.rocrate.to_string().contains(&format!(
        "https://w3id.org/aruna/{document_id}?view=summary"
    )));
    assert!(!summary.rocrate.to_string().contains("file-0.txt"));

    let (_, Json(page)) = export_metadata_rocrate(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(document_id.clone()),
        Query(RoCrateExportParams {
            view: Some(MetadataRoCrateView::Page),
            limit: Some(2),
            offset: Some(0),
            after: None,
        }),
    )
    .await
    .unwrap();
    let page = projected(page);
    assert!(page.rocrate.to_string().contains(&format!(
        "https://w3id.org/aruna/{document_id}?view=page&limit=2&offset=0"
    )));
    assert_eq!(page.total_data_entities, Some(3));
    assert_eq!(page.returned_data_entities, Some(2));
    assert_eq!(page.next_offset, Some(2));
    assert!(page.next_cursor.is_some());
    assert!(
        page.rocrate.to_string().contains("file-0.txt")
            || page.rocrate.to_string().contains("file-1.txt")
    );

    let (_, Json(result)) = query_metadata_document(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(document_id.clone()),
        Json(SparqlQueryRequest {
            query: "ASK WHERE { ?s <http://schema.org/name> \"Public Dataset\" }".to_string(),
            mode: None,
            allow_partial: true,
        }),
    )
    .await
    .unwrap();
    assert!(matches!(result.result, MetadataQueryResult::Boolean(true)));
    assert_eq!(result.nodes_queried, 1);
    assert_eq!(result.nodes_failed, 0);

    let ctx = test.state.get_ctx();
    installed_metadata_handle(ctx.as_ref())
        .flush_search_updates()
        .await
        .unwrap();

    let (_, Json(search)) = search_metadata(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Query(MetadataSearchParams {
            q: "Public".to_string(),
            conforms_to: None,
            group_id: None,
            limit: Some(10),
            cursor: None,
            mode: None,
        }),
    )
    .await
    .unwrap();
    assert!(!search.hits.is_empty());
    assert_eq!(search.nodes_queried, 1);
    assert_eq!(search.nodes_failed, 0);
    let dataset_hit = search
        .hits
        .iter()
        .find(|hit| hit.title == "Public Dataset")
        .expect("root dataset hit is enriched with its schema:name title");
    assert!(
        dataset_hit
            .snippet
            .as_deref()
            .is_some_and(|snippet| snippet.to_lowercase().contains("public")),
        "snippet should window the matched query term: {:?}",
        dataset_hit.snippet
    );
    assert_eq!(
        dataset_hit.subject_types,
        vec!["http://schema.org/Dataset".to_string()]
    );

    let (_, Json(parts)) = search_metadata(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Query(MetadataSearchParams {
            q: "file".to_string(),
            conforms_to: None,
            group_id: None,
            limit: Some(10),
            cursor: None,
            mode: None,
        }),
    )
    .await
    .unwrap();
    // A file entity matches as its own subject, so only its rdf:type tells
    // it apart from the dataset it belongs to.
    let file_hit = parts
        .hits
        .iter()
        .find(|hit| hit.subject_iri.ends_with("file-1.txt"))
        .expect("file entity matches as its own subject");
    assert_eq!(
        file_hit.subject_types,
        vec!["http://schema.org/MediaObject".to_string()]
    );
    assert_eq!(file_hit.document_id, document_id);

    let (_, Json(conforming)) = search_metadata(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Query(MetadataSearchParams {
            q: String::new(),
            conforms_to: Some("https://w3id.org/ro/crate/1.2".to_string()),
            group_id: None,
            limit: Some(10),
            cursor: None,
            mode: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(conforming.hits.len(), 1);
    assert_eq!(conforming.hits[0].document_id, document_id);

    let (_, Json(nonconforming)) = search_metadata(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Query(MetadataSearchParams {
            q: "Public".to_string(),
            conforms_to: Some("https://w3id.org/ro/crate/1.1".to_string()),
            group_id: None,
            limit: Some(10),
            cursor: None,
            mode: None,
        }),
    )
    .await
    .unwrap();
    assert!(nonconforming.hits.is_empty());
}

#[tokio::test]
async fn portal_searches_description() {
    let test = setup_state().await;
    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::RoCrate(CreateRoCrateRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/portal-array-context".to_string(),
            public: true,
            rocrate: json!({
                "@context": [
                    "https://w3id.org/ro/crate/1.2/context",
                    {
                        "portalTerm": "https://example.org/portalTerm"
                    }
                ],
                "@graph": [
                    {
                        "@id": "ro-crate-metadata.json",
                        "@type": "CreativeWork",
                        "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
                        "about": { "@id": "urn:dataset:portal-search" }
                    },
                    {
                        "@id": "urn:dataset:portal-search",
                        "@type": "Dataset",
                        "name": "Portal Search Dataset",
                        "description": "A plain multi word constellation dataset description",
                        "datePublished": "2026-07-18",
                        "license": { "@id": "https://creativecommons.org/licenses/by/4.0/" },
                        "portalTerm": "portal-shape"
                    }
                ]
            }),
        })),
    )
    .await
    .unwrap();
    drain_metadata_background(test.state.as_ref()).await;

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let (_, Json(search)) = search_metadata(
                State(test.state.clone()),
                Extension(None),
                Extension(None),
                Query(MetadataSearchParams {
                    q: "constellation".to_string(),
                    conforms_to: None,
                    group_id: None,
                    limit: Some(10),
                    cursor: None,
                    mode: Some(MetadataQueryMode::Local),
                }),
            )
            .await
            .unwrap();
            if search
                .hits
                .iter()
                .any(|hit| hit.document_id == created.summary.document_id)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("woken search worker indexes the portal crate");
}

#[tokio::test]
async fn rocrate_routes_work() {
    let test = setup_state().await;

    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::RoCrate(CreateRoCrateRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/rocrate-dataset".to_string(),
            public: true,
            rocrate: json!({
                "@context": "https://w3id.org/ro/crate/1.2/context",
                "@graph": [
                    {
                        "@id": "ro-crate-metadata.json",
                        "@type": "CreativeWork",
                        "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
                        "about": { "@id": "urn:dataset:rocrate-create" }
                    },
                    {
                        "@id": "urn:dataset:rocrate-create",
                        "@type": "Dataset",
                        "name": "Created From RO-Crate",
                        "description": "Created from inline JSON-LD",
                        "datePublished": "2026-01-01",
                        "license": { "@id": "https://creativecommons.org/licenses/by/4.0/" }
                    }
                ]
            }),
        })),
    )
    .await
    .unwrap();

    let document_id = created.summary.document_id.clone();
    assert_eq!(created.summary.document_path, "datasets/rocrate-dataset");
    assert!(created.summary.created_at.ends_with('Z'));
    assert!(created.summary.updated_at.ends_with('Z'));
    drain_metadata_background(test.state.as_ref()).await;

    let _ = add_contextual_entity(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Path(document_id.clone()),
        Json(json!({
            "@id": "#person-ada",
            "@type": "Person",
            "name": "Ada Lovelace"
        })),
    )
    .await
    .unwrap();

    let _ = add_data_entity(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Path(document_id.clone()),
        Json(json!({
            "@id": "./data/run-42.raw",
            "@type": "File",
            "name": "run-42.raw",
            "creator": { "@id": "#person-ada" }
        })),
    )
    .await
    .unwrap();
    drain_metadata_background(test.state.as_ref()).await;

    let (_, Json(exported)) = export_metadata_rocrate(
        State(test.state),
        Extension(None),
        Extension(None),
        Path(document_id.clone()),
        Query(RoCrateExportParams::default()),
    )
    .await
    .unwrap();

    let exported = projected(exported);
    let json = exported.rocrate.to_string();
    assert!(json.contains(&format!("https://w3id.org/aruna/{document_id}")));
    assert!(json.contains("Created From RO-Crate"));
    assert!(json.contains("Ada Lovelace"));
    assert!(json.contains("run-42.raw"));
}

#[tokio::test]
async fn list_uses_registry() {
    let test = setup_state().await;
    let ctx = test.state.get_ctx();
    installed_metadata_handle(ctx.as_ref()).expire_visibility_caches();

    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/cache-served".to_string(),
            name: "Cache Served Dataset".to_string(),
            description: "Served from the handle registry cache".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public: true,
        })),
    )
    .await
    .unwrap();
    drain_metadata_background(test.state.as_ref()).await;

    installed_metadata_handle(ctx.as_ref()).expire_visibility_caches();

    let (_, Json(listed)) = list_metadata_documents(
        State(test.state.clone()),
        Extension(None),
        Path(test.group_id.to_string()),
        Query(ListMetadataQuery::default()),
    )
    .await
    .unwrap();
    assert_eq!(listed.documents.len(), 1);
    assert_eq!(listed.documents[0].document_id, created.summary.document_id);

    let status = delete_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Path(created.summary.document_id.clone()),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, Json(listed)) = list_metadata_documents(
        State(test.state.clone()),
        Extension(None),
        Path(test.group_id.to_string()),
        Query(ListMetadataQuery::default()),
    )
    .await
    .unwrap();
    assert!(listed.documents.is_empty());
}

#[tokio::test]
async fn tombstone_hides_listing() {
    let test = setup_state().await;
    let ctx = test.state.get_ctx();
    installed_metadata_handle(ctx.as_ref()).expire_visibility_caches();

    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/inbound-tombstone".to_string(),
            name: "Inbound Tombstone Dataset".to_string(),
            description: "Deleted by document lifecycle only".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public: true,
        })),
    )
    .await
    .unwrap();
    drain_metadata_background(test.state.as_ref()).await;

    let document_id = parse_document_id(&created.summary.document_id).unwrap();
    let record = load_document_record(test.state.as_ref(), document_id)
        .await
        .unwrap();
    let (_, Json(listed)) = list_metadata_documents(
        State(test.state.clone()),
        Extension(None),
        Path(test.group_id.to_string()),
        Query(ListMetadataQuery::default()),
    )
    .await
    .unwrap();
    assert_eq!(listed.documents.len(), 1);

    let tombstone = GraphLifecycleRecord::deleted(
        record.graph_iri.clone(),
        record.realm_id,
        record.group_id,
        record.document_id,
        2,
    );
    let lifecycle = MetadataLifecycleRecord::Delete {
        event: MetadataDeleteRecord {
            event_id: Ulid::generate(),
            tombstone: tombstone.clone(),
            deleted_after_id: record.last_event_id,
        },
    };
    for effect in [
        write_graph_lifecycle(&tombstone, None).unwrap(),
        write_document_lifecycle(&lifecycle, None).unwrap(),
    ] {
        match ctx.storage_handle.send_effect(effect).await {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected lifecycle write event: {other:?}"),
        }
    }
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes: registry_delete_entries(&record),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
        other => panic!("unexpected registry delete event: {other:?}"),
    }

    let processed = process_graph_tombstones(ctx.as_ref(), vec![tombstone]).await;

    assert_eq!(processed.enqueued, 1);
    assert!(prune_jobs_exist(&ctx.storage_handle).await.unwrap());
    let (_, Json(listed)) = list_metadata_documents(
        State(test.state.clone()),
        Extension(None),
        Path(test.group_id.to_string()),
        Query(ListMetadataQuery::default()),
    )
    .await
    .unwrap();
    assert!(listed.documents.is_empty());
    let fetched = get_metadata_document(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(created.summary.document_id),
    )
    .await;
    assert!(matches!(fetched, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn drain_timer_persisted() {
    let test = setup_state().await;
    let ctx = test.state.get_ctx();

    schedule_projection_drain(ctx.as_ref(), Duration::ZERO)
        .await
        .expect("projection drain scheduled");

    let timer = read_task_timer(ctx.as_ref(), &TaskKey::DrainProjectionQueue)
        .await
        .expect("projection drain timer persisted");
    assert_eq!(timer.key, TaskKey::DrainProjectionQueue);
}

#[tokio::test]
async fn private_metadata_hidden() {
    let test = setup_state().await;

    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/private-dataset".to_string(),
            name: "Private Dataset".to_string(),
            description: "Private metadata".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public: false,
        })),
    )
    .await
    .unwrap();
    drain_metadata_background(test.state.as_ref()).await;

    let (_, Json(listed)) = list_metadata_documents(
        State(test.state.clone()),
        Extension(None),
        Path(test.group_id.to_string()),
        Query(ListMetadataQuery::default()),
    )
    .await
    .unwrap();
    assert!(listed.documents.is_empty());

    let result = export_metadata_rocrate(
        State(test.state),
        Extension(None),
        Extension(None),
        Path(created.summary.document_id),
        Query(RoCrateExportParams::default()),
    )
    .await;
    assert!(matches!(result, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn hides_document_existence() {
    // Present-but-unreadable and truly-absent both answer NotFound so a
    // caller cannot probe document existence by id.
    let test = setup_state().await;
    let realm_id = test.auth.realm_id;

    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/secret".to_string(),
            name: "Secret".to_string(),
            description: "Secret metadata".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public: false,
        })),
    )
    .await
    .unwrap();
    drain_metadata_background(test.state.as_ref()).await;
    let document_id = created.summary.document_id.clone();

    let owner = get_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Path(document_id.clone()),
    )
    .await;
    assert!(owner.is_ok());

    let stranger = AuthContext {
        user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let stranger_result = get_metadata_document(
        State(test.state.clone()),
        Extension(Some(stranger)),
        Extension(None),
        Path(document_id.clone()),
    )
    .await;
    assert!(matches!(stranger_result, Err(ServerError::NotFound)));

    let anonymous_result = get_metadata_document(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(document_id),
    )
    .await;
    assert!(matches!(anonymous_result, Err(ServerError::NotFound)));

    let absent = get_metadata_document(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(Ulid::generate().to_string()),
    )
    .await;
    assert!(matches!(absent, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn missing_query_notfound() {
    let test = setup_state().await;

    let result = query_metadata_document(
        State(test.state),
        Extension(None),
        Extension(None),
        Path(Ulid::generate().to_string()),
        Json(SparqlQueryRequest {
            query: "ASK WHERE { ?s ?p ?o }".to_string(),
            mode: None,
            allow_partial: true,
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn realm_nodes_fallback() {
    let state = setup_closed_storage().await;

    let nodes = state_realm_nodes(state.as_ref()).await.unwrap();

    assert_eq!(nodes, vec![state.get_node_id()]);
}

#[tokio::test]
async fn presence_tracks_config() {
    let realm_id = test_realm_id(31);
    let coordinator = spawn_metadata_node(realm_id).await;
    let remote = spawn_metadata_node(realm_id).await;

    coordinator
        .net
        .add_peer_addr(remote.net.endpoint_addr())
        .await;
    remote
        .net
        .add_peer_addr(coordinator.net.endpoint_addr())
        .await;
    install_realm_config(&[&coordinator, &remote], realm_id, None).await;

    let initial = state_realm_nodes(coordinator.state.as_ref()).await.unwrap();
    assert_eq!(initial, vec![coordinator.net.node_id()]);

    let remote_ctx = remote.state.get_ctx();
    drive(
        AnnouncePresenceOperation::new(AnnouncePresenceConfig {
            realm_id,
            node_id: remote.net.node_id(),
            schedule_refresh: false,
        }),
        remote_ctx.as_ref(),
    )
    .await
    .unwrap();

    let discovered = state_realm_nodes(coordinator.state.as_ref()).await.unwrap();

    assert!(discovered.contains(&coordinator.net.node_id()));
    assert!(discovered.contains(&remote.net.node_id()));

    coordinator.net.shutdown().await;
    remote.net.shutdown().await;
}

fn draft_crate() -> Value {
    json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
                "about": { "@id": "./" }
            },
            {
                "@id": "./",
                "@type": "Dataset",
                "name": "Draft Dataset",
                "description": "validated before it is saved",
                "datePublished": "2026-01-01"
            }
        ]
    })
}

#[tokio::test]
async fn preview_refuses_stranger() {
    // A group scope resolves that group's non-public Profiles, so a
    // same-realm caller without READ on its metadata must be refused.
    let test = setup_state().await;
    let realm_id = test.state.get_realm_id();
    let stranger = AuthContext {
        user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    };

    let result = preview_profile_validation(
        State(test.state.clone()),
        Extension(Some(stranger)),
        Json(ProfilePreviewRequest {
            rocrate: draft_crate(),
            group_id: Some(test.group_id.to_string()),
            public: false,
        }),
    )
    .await;

    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[tokio::test]
async fn preview_admits_member() {
    let test = setup_state().await;

    let (status, Json(preview)) = preview_profile_validation(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Json(ProfilePreviewRequest {
            rocrate: draft_crate(),
            group_id: Some(test.group_id.to_string()),
            public: false,
        }),
    )
    .await
    .unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(preview.state, "not_profiled");
    assert!(preview.restricted_files.is_empty());
}

const PREVIEW_BUCKET: &str = "preview-reads";
const PREVIEW_KEY: &str = "raw/one.csv";

/// Seeds one stored object and returns the draft crate that references it
/// by content identity, the way the portal writes data entities.
async fn seed_preview_object(test: &TestState) -> Value {
    let hash = [44u8; 32];
    let version_id = Ulid::generate();
    let ctx = test.state.get_ctx();
    let bucket_info = BucketInfo {
        group_id: test.group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: test.auth.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    };
    write_doc(
        &ctx,
        S3_BUCKET_KEYSPACE,
        PREVIEW_BUCKET.as_bytes().into(),
        bucket_info.to_bytes().unwrap().into(),
    )
    .await;
    write_doc(
        &ctx,
        PATHS_INDEX_KEYSPACE,
        HashIndex::new(
            hash,
            version_id,
            test.state.get_realm_id(),
            test.group_id,
            test.state.get_node_id(),
            PREVIEW_BUCKET,
            PREVIEW_KEY,
        )
        .to_bytes()
        .unwrap()
        .into(),
        Vec::<u8>::new().into(),
    )
    .await;
    let mut crate_value = draft_crate();
    crate_value["@graph"].as_array_mut().unwrap().push(json!({
        "@id": format!(
            "{}{}",
            aruna_core::structs::storage::replication::ARUNA_DATA_PREFIX,
            hex::encode(hash)
        ),
        "@type": "File",
        "name": "one.csv",
        "contentUrl": format!("s3://{PREVIEW_BUCKET}/{PREVIEW_KEY}")
    }));
    crate_value
}

async fn grant_anonymous_read(test: &TestState) {
    let realm_id = test.state.get_realm_id();
    let actor = Actor {
        node_id: test.state.get_node_id(),
        user_id: test.auth.user_id,
        realm_id,
    };
    let mut realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
    let role_id = Ulid::generate();
    realm_auth.roles.insert(
        role_id,
        aruna_core::structs::identity::auth::Role {
            role_id,
            name: "everyone".to_string(),
            permissions: std::collections::HashMap::from([(
                format!("/{realm_id}/g/{}/**", test.group_id),
                Permission::READ,
            )]),
            assigned_users: HashSet::from([aruna_core::UserId::nil(realm_id)]),
        },
    );
    write_doc(
        &test.state.get_ctx(),
        AUTH_KEYSPACE,
        (*realm_id.as_bytes()).into(),
        realm_auth.to_bytes(&actor).unwrap().into(),
    )
    .await;
}

#[tokio::test]
async fn preview_lists_restricted() {
    let test = setup_state().await;
    let rocrate = seed_preview_object(&test).await;

    let (_, Json(preview)) = preview_profile_validation(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Json(ProfilePreviewRequest {
            rocrate,
            group_id: Some(test.group_id.to_string()),
            public: true,
        }),
    )
    .await
    .unwrap();

    assert_eq!(preview.restricted_files.len(), 1);
    assert_eq!(preview.restricted_files_complete, Some(true));
    let restricted = &preview.restricted_files[0];
    assert_eq!(restricted.group_id, Some(test.group_id.to_string()));
    assert_eq!(restricted.bucket.as_deref(), Some(PREVIEW_BUCKET));
    assert_eq!(restricted.key.as_deref(), Some(PREVIEW_KEY));
    assert_eq!(
        restricted.permission_path.as_deref(),
        Some(
            aruna_core::structs::storage::blob::object_permission_path(
                test.state.get_realm_id(),
                test.group_id,
                test.state.get_node_id(),
                PREVIEW_BUCKET,
                PREVIEW_KEY,
            )
            .as_str()
        )
    );
}

#[tokio::test]
async fn preview_skips_readable() {
    let test = setup_state().await;
    let rocrate = seed_preview_object(&test).await;
    grant_anonymous_read(&test).await;

    let (_, Json(preview)) = preview_profile_validation(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Json(ProfilePreviewRequest {
            rocrate,
            group_id: Some(test.group_id.to_string()),
            public: true,
        }),
    )
    .await
    .unwrap();

    assert!(preview.restricted_files.is_empty());
    assert_eq!(preview.restricted_files_complete, Some(true));
}

#[tokio::test]
async fn preview_skips_private() {
    let test = setup_state().await;
    let rocrate = seed_preview_object(&test).await;

    let (_, Json(preview)) = preview_profile_validation(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Json(ProfilePreviewRequest {
            rocrate,
            group_id: Some(test.group_id.to_string()),
            public: false,
        }),
    )
    .await
    .unwrap();

    assert!(preview.restricted_files.is_empty());
    assert_eq!(preview.restricted_files_complete, None);
}

#[tokio::test]
async fn pending_export_unavailable() {
    let test = setup_state().await;
    let ctx = test.state.get_ctx();

    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/pending-dataset".to_string(),
            name: "Pending Dataset".to_string(),
            description: "Not yet materialized".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public: true,
        })),
    )
    .await
    .unwrap();

    let result = export_metadata_rocrate(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(created.summary.document_id.clone()),
        Query(RoCrateExportParams::default()),
    )
    .await;
    assert!(matches!(result, Err(ServerError::NotFound)));

    let projected = drain_projection_queue(ctx.as_ref()).await.unwrap();
    assert_eq!(projected.markers_examined, 1);
    assert_eq!(projected.projected, 1);
    let result = export_metadata_rocrate(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(created.summary.document_id.clone()),
        Query(RoCrateExportParams::default()),
    )
    .await;
    assert!(matches!(result, Err(ServerError::ServiceUnavailable)));

    let materialized = process_materialization_batch(ctx.as_ref()).await.unwrap();
    assert_eq!(materialized.processed, 1);
    let result = export_metadata_rocrate(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Path(created.summary.document_id),
        Query(RoCrateExportParams::default()),
    )
    .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn pending_summary_tolerated() {
    let test = setup_state().await;

    let (_, Json(_created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/pending-summary".to_string(),
            name: "Pending Summary".to_string(),
            description: "Summary projection in flight".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public: true,
        })),
    )
    .await
    .unwrap();

    let summary_query = ListMetadataQuery {
        include: Some("summary".to_string()),
        ..ListMetadataQuery::default()
    };

    // No drain_metadata_background: the graph projection is still pending,
    // the list must stay 200 with a null summary for that document.
    let (_, Json(listed)) = list_metadata_documents(
        State(test.state.clone()),
        Extension(None),
        Path(test.group_id.to_string()),
        Query(summary_query.clone()),
    )
    .await
    .unwrap();
    assert_eq!(listed.documents.len(), 1);
    assert!(listed.documents[0].rocrate_summary.is_none());

    drain_metadata_background(test.state.as_ref()).await;
    let (_, Json(listed)) = list_metadata_documents(
        State(test.state.clone()),
        Extension(None),
        Path(test.group_id.to_string()),
        Query(summary_query),
    )
    .await
    .unwrap();
    assert_eq!(listed.documents.len(), 1);
    assert!(listed.documents[0].rocrate_summary.is_some());
}

#[tokio::test]
async fn replacement_summary_withheld() {
    let test = setup_state().await;

    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/replaced-summary".to_string(),
            name: "Original Dataset".to_string(),
            description: "before the replace".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public: true,
        })),
    )
    .await
    .unwrap();
    let document_id = created.summary.document_id.clone();
    drain_metadata_background(test.state.as_ref()).await;

    let summary_query = ListMetadataQuery {
        include: Some("summary".to_string()),
        ..ListMetadataQuery::default()
    };
    // Prime the summary cache with the pre-update revision.
    let (_, Json(listed)) = list_metadata_documents(
        State(test.state.clone()),
        Extension(None),
        Path(test.group_id.to_string()),
        Query(summary_query.clone()),
    )
    .await
    .unwrap();
    let primed = serde_json::to_string(&listed.documents[0].rocrate_summary).unwrap();
    assert!(primed.contains("Original Dataset"));

    let rocrate = format!(
        r#"{{"@context":"https://w3id.org/ro/crate/1.2/context","@graph":[{{"@id":"ro-crate-metadata.json","@type":"CreativeWork","conformsTo":{{"@id":"https://w3id.org/ro/crate/1.2"}},"about":{{"@id":"https://w3id.org/aruna/{document_id}"}}}},{{"@id":"https://w3id.org/aruna/{document_id}","@type":"Dataset","name":"Renamed Dataset","description":"after the replace","datePublished":"2026-01-01","license":{{"@id":"https://creativecommons.org/licenses/by/4.0/"}}}}]}}"#
    );
    let _ = replace_metadata_rocrate(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Path(document_id),
        Json(ReplaceRoCrateRequest {
            rocrate: serde_json::from_str(&rocrate).unwrap(),
            public: Some(true),
        }),
    )
    .await
    .unwrap();

    // Withhold and avoid caching the replaced summary while rematerialization is pending.
    let (_, Json(listed)) = list_metadata_documents(
        State(test.state.clone()),
        Extension(None),
        Path(test.group_id.to_string()),
        Query(summary_query.clone()),
    )
    .await
    .unwrap();
    assert_eq!(listed.documents.len(), 1);
    assert!(
        listed.documents[0].rocrate_summary.is_none(),
        "a pending update must list without a summary, not the replaced content"
    );

    drain_metadata_background(test.state.as_ref()).await;
    let (_, Json(listed)) = list_metadata_documents(
        State(test.state.clone()),
        Extension(None),
        Path(test.group_id.to_string()),
        Query(summary_query),
    )
    .await
    .unwrap();
    let refreshed = serde_json::to_string(&listed.documents[0].rocrate_summary).unwrap();
    assert!(
        refreshed.contains("Renamed Dataset"),
        "the materialized summary must carry the new revision, got: {refreshed}"
    );
}

#[test]
fn openapi_metadata_complete() {
    let openapi = serde_json::to_value(crate::openapi::ApiDoc::openapi()).unwrap();

    assert_eq!(
        openapi["components"]["schemas"]["MetadataRoCrateView"]["type"],
        json!("string")
    );

    let export_params = openapi["paths"]["/metadata/{document_id}/rocrate"]["get"]["parameters"]
        .as_array()
        .unwrap();
    let view_param = export_params
        .iter()
        .find(|param| param["name"] == "view")
        .unwrap();
    assert_eq!(
        view_param["schema"]["$ref"],
        json!("#/components/schemas/MetadataRoCrateView")
    );
    let export_submit = &openapi["paths"]["/metadata/{document_id}/rocrate/exports"]["post"];
    assert!(export_submit["responses"].get("202").is_some());
    assert_eq!(export_submit["security"][0]["bearer_auth"], json!([]));

    let create_examples = openapi["paths"]["/metadata"]["post"]["requestBody"]["content"]
            ["application/json"]["examples"]
            .as_object()
            .unwrap();
    assert!(create_examples.contains_key("ScaffoldCreate"));
    assert!(create_examples.contains_key("RoCrateCreate"));

    let create_response_description =
        openapi["paths"]["/metadata"]["post"]["responses"]["201"]["description"]
            .as_str()
            .unwrap();
    assert!(create_response_description.contains("durable event/projection pipeline"));
    assert!(create_response_description.contains("fully materialized"));
    assert!(create_response_description.contains("replicated yet"));

    for (path, method) in [
        ("/metadata/{document_id}/rocrate", "put"),
        ("/metadata/{document_id}/rocrate/entities/data", "post"),
        (
            "/metadata/{document_id}/rocrate/entities/contextual",
            "post",
        ),
    ] {
        let description = openapi["paths"][path][method]["responses"]["200"]["description"]
            .as_str()
            .unwrap();
        assert!(description.contains("durable event/projection pipeline"));
        assert!(description.contains("fully materialized"));
        assert!(description.contains("replicated yet"));
    }

    let document_query_request =
        &openapi["paths"]["/metadata/{document_id}/sparql/query"]["post"]["requestBody"];
    assert!(
        document_query_request["description"]
            .as_str()
            .unwrap()
            .contains("complete result")
    );

    let query_all_request = &openapi["paths"]["/metadata/sparql/query"]["post"]["requestBody"];
    assert!(
        query_all_request["description"]
            .as_str()
            .unwrap()
            .contains("best-effort")
    );

    let search_params = openapi["paths"]["/metadata/search"]["get"]["parameters"]
        .as_array()
        .unwrap();
    let search_mode_param = search_params
        .iter()
        .find(|param| param["name"] == "mode")
        .unwrap();
    assert!(
        search_mode_param["description"]
            .as_str()
            .unwrap()
            .contains("best-effort")
    );
    let search_cursor_param = search_params
        .iter()
        .find(|param| param["name"] == "cursor")
        .unwrap();
    assert!(
        search_cursor_param["description"]
            .as_str()
            .unwrap()
            .contains("best-effort")
    );

    let data_entity_examples = openapi["paths"]["/metadata/{document_id}/rocrate/entities/data"]
            ["post"]["requestBody"]["content"]["application/json"]["examples"]
            .as_object()
            .unwrap();
    assert!(data_entity_examples.contains_key("DataEntity"));

    let contextual_examples = openapi["paths"]
            ["/metadata/{document_id}/rocrate/entities/contextual"]["post"]["requestBody"]
            ["content"]["application/json"]["examples"]
            .as_object()
            .unwrap();
    assert!(contextual_examples.contains_key("ContextualEntity"));

    let summary_properties =
        openapi["components"]["schemas"]["MetadataDocumentSummary"]["properties"]
            .as_object()
            .unwrap();
    assert!(summary_properties.contains_key("replicas"));
    assert!(summary_properties.contains_key("created_at"));
    assert!(summary_properties.contains_key("updated_at"));
    assert!(!summary_properties.contains_key("holder_count"));
    assert!(!summary_properties.contains_key("created_at_ms"));
    assert!(!summary_properties.contains_key("updated_at_ms"));
}

#[tokio::test]
async fn local_partition_executes() {
    let test = setup_network_state().await;

    let _ = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/local-partition".to_string(),
            name: "Local Partition Dataset".to_string(),
            description: "Coordinator partition".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public: true,
        })),
    )
    .await
    .unwrap();
    drain_metadata_background(test.state.as_ref()).await;

    // Without remote nodes or a net handle, the coordinator partition must run locally.
    let (_, Json(result)) = query_all_metadata(
        State(test.state.clone()),
        Extension(None),
        Extension(None),
        Json(SparqlQueryRequest {
            query: "SELECT DISTINCT ?name WHERE { ?s <http://schema.org/name> ?name } LIMIT 10"
                .to_string(),
            mode: Some(MetadataQueryMode::Distributed),
            allow_partial: true,
        }),
    )
    .await
    .unwrap();

    assert_eq!(result.nodes_queried, 1);
    assert_eq!(result.nodes_failed, 0);
    let MetadataQueryResult::Solutions(rows) = result.result else {
        panic!("expected solutions");
    };
    assert!(rows.iter().any(|row| {
        row.values()
            .any(|value| value.contains("Local Partition Dataset"))
    }));
}

#[tokio::test]
async fn query_applies_visibility() {
    let test = setup_state().await;

    for (path, name, public) in [
        ("datasets/lazy-public", "Lazy Public Dataset", true),
        ("datasets/lazy-private", "Lazy Private Dataset", false),
    ] {
        let _ = create_metadata_document(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Extension(None),
            Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
                group_id: test.group_id.to_string(),
                path: path.to_string(),
                name: name.to_string(),
                description: "Lazy visibility".to_string(),
                date_published: "2026-01-01".to_string(),
                license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                public,
            })),
        )
        .await
        .unwrap();
    }
    drain_metadata_background(test.state.as_ref()).await;

    let query_names = async |auth: Option<AuthContext>| {
        let (_, Json(result)) = query_all_metadata(
            State(test.state.clone()),
            Extension(auth),
            Extension(None),
            Json(SparqlQueryRequest {
                query: "SELECT ?name WHERE { ?s <http://schema.org/name> ?name }".to_string(),
                mode: Some(MetadataQueryMode::Local),
                allow_partial: true,
            }),
        )
        .await
        .unwrap();
        let MetadataQueryResult::Solutions(rows) = result.result else {
            panic!("expected solutions");
        };
        rows.into_iter()
            .flat_map(|row| row.into_values())
            .collect::<Vec<_>>()
    };

    let anonymous = query_names(None).await;
    assert!(anonymous.iter().any(|name| name.contains("Lazy Public")));
    assert!(!anonymous.iter().any(|name| name.contains("Lazy Private")));

    let authorized = query_names(Some(test.auth.clone())).await;
    assert!(authorized.iter().any(|name| name.contains("Lazy Public")));
    assert!(authorized.iter().any(|name| name.contains("Lazy Private")));
}

#[tokio::test]
async fn query_forwards_token() {
    let test = setup_access_state().await;
    let token_auth: AuthContext =
        crate::auth::handle_token(test.coordinator.state.as_ref(), &test.valid_bearer_token)
            .await
            .unwrap()
            .try_into()
            .unwrap();
    assert_eq!(token_auth, test.auth);

    let authorized = query_remote_names(
        &test,
        Some(token_auth.clone()),
        Some(ValidatedBearer::new_for_test(
            test.valid_bearer_token.clone(),
        )),
    )
    .await;
    assert_eq!(authorized.nodes_queried, 1);
    assert_eq!(authorized.nodes_failed, 0);
    assert_contains_name(&authorized.names, "Remote Public Dataset");
    assert_contains_name(&authorized.names, "Remote Private Dataset");

    let anonymous = query_remote_names(&test, None, None).await;
    assert_eq!(anonymous.nodes_failed, 0);
    assert_contains_name(&anonymous.names, "Remote Public Dataset");
    assert_excludes_name(&anonymous.names, "Remote Private Dataset");

    let no_forwardable_token = query_remote_names(&test, Some(token_auth.clone()), None).await;
    assert_eq!(no_forwardable_token.nodes_failed, 0);
    assert_contains_name(&no_forwardable_token.names, "Remote Public Dataset");
    assert_excludes_name(&no_forwardable_token.names, "Remote Private Dataset");

    let oversized_token = query_remote_names(
        &test,
        Some(token_auth.clone()),
        Some(ValidatedBearer::new_for_test("x".repeat(4097))),
    )
    .await;
    assert_eq!(oversized_token.nodes_failed, 0);
    assert_contains_name(&oversized_token.names, "Remote Public Dataset");
    assert_excludes_name(&oversized_token.names, "Remote Private Dataset");

    assert!(
        crate::auth::handle_token(test.coordinator.state.as_ref(), "not-a-jwt")
            .await
            .is_err()
    );
    let invalid_forwarded_token = query_remote_names(
        &test,
        Some(token_auth),
        Some(ValidatedBearer::new_for_test("not-a-jwt")),
    )
    .await;
    assert_eq!(invalid_forwarded_token.nodes_queried, 1);
    assert_eq!(invalid_forwarded_token.nodes_failed, 1);
    assert_eq!(
        invalid_forwarded_token.failed_partitions,
        vec![test.remote.net.node_id()]
    );
    assert_excludes_name(&invalid_forwarded_token.names, "Remote Public Dataset");
    assert_excludes_name(&invalid_forwarded_token.names, "Remote Private Dataset");

    let ctx = test.coordinator.state.get_ctx();
    let strict = run_query_metadata(
        ctx.as_ref(),
        test.coordinator.state.get_realm_id(),
        test.coordinator.state.get_node_id(),
        MetadataQueryRequest {
            auth: None,
            bearer_token: Some("not-a-jwt".to_string()),
            graph_iris: None,
            query: "SELECT DISTINCT ?name WHERE { ?s <http://schema.org/name> ?name }".to_string(),
            mode: Some(ApiQueryMode::Distributed),
            target_nodes: Some(vec![test.remote.net.node_id()]),
            allow_partial: false,
        },
    )
    .await;
    assert!(matches!(strict, Err(MetadataApiError::ServiceUnavailable)));

    test.shutdown().await;
}

#[tokio::test]
async fn private_get_forwards() {
    let test = setup_access_state().await;
    let (_, Json(listed)) = list_metadata_documents(
        State(test.remote.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(test.group_id.to_string()),
        Query(ListMetadataQuery::default()),
    )
    .await
    .unwrap();
    let private = listed
        .documents
        .into_iter()
        .find(|document| document.document_path == "datasets/remote-private")
        .expect("private document exists on its holder");
    let (_, Json(path)) = get_metadata_path(
        State(test.coordinator.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test(
            test.valid_bearer_token.clone(),
        ))),
        Path(test.group_id.to_string()),
        Query(MetadataPathQuery {
            path: "datasets/remote-private".to_string(),
        }),
    )
    .await
    .unwrap();
    assert_eq!(path.winner.document_id, private.document_id);
    assert!(path.conflicts.is_empty());

    let (_, Json(fetched)) = get_metadata_document(
        State(test.coordinator.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test(
            test.valid_bearer_token.clone(),
        ))),
        Path(private.document_id),
    )
    .await
    .unwrap();

    assert_eq!(fetched.document_path, "datasets/remote-private");
    test.shutdown().await;
}

#[tokio::test]
async fn user_writes_forward() {
    let test = setup_access_state().await;
    let denied = create_metadata_document(
        State(test.coordinator.state.clone()),
        Extension(Some(test.denied_auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test(
            test.denied_bearer_token.clone(),
        ))),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/user-denied".to_string(),
            name: "User Denied".to_string(),
            description: "Forwarded write without group permission".to_string(),
            date_published: "2026-01-01".to_string(),
            license: None,
            public: false,
        })),
    )
    .await;
    // A device forwards only for the owner its realm config names, so
    // another user's token is refused before any group check.
    assert!(matches!(denied, Err(ServerError::Forbidden)));

    let bearer = || {
        Extension(Some(ValidatedBearer::new_for_test(
            test.valid_bearer_token.clone(),
        )))
    };

    // The owner binding does not replace the group check: a group the owner
    // holds no role in is still refused at the ingress.
    let foreign_group = Ulid::generate();
    install_group_documents(
        &test.remote,
        test.auth.realm_id,
        test.denied_auth.user_id,
        foreign_group,
    )
    .await;
    let unpermitted = create_metadata_document(
        State(test.coordinator.state.clone()),
        Extension(Some(test.auth.clone())),
        bearer(),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: foreign_group.to_string(),
            path: "datasets/user-unpermitted".to_string(),
            name: "User Unpermitted".to_string(),
            description: "Forwarded write into a foreign group".to_string(),
            date_published: "2026-01-01".to_string(),
            license: None,
            public: false,
        })),
    )
    .await;
    assert!(matches!(unpermitted, Err(ServerError::Forbidden)));
    let missing = MetaResourceId::from_parts(
        1,
        PlacementHandle::new(METADATA_HANDLE).unwrap(),
        BucketId::new(0).unwrap(),
        1,
    )
    .unwrap();
    let missing_result = delete_metadata_document(
        State(test.coordinator.state.clone()),
        Extension(Some(test.auth.clone())),
        bearer(),
        Path(missing.to_string()),
    )
    .await;
    assert!(matches!(missing_result, Err(ServerError::NotFound)));

    let (_, Json(created)) = create_metadata_document(
        State(test.coordinator.state.clone()),
        Extension(Some(test.auth.clone())),
        bearer(),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/user-forward".to_string(),
            name: "User Forward".to_string(),
            description: "Forwarded from a User node".to_string(),
            date_published: "2026-01-01".to_string(),
            license: None,
            public: false,
        })),
    )
    .await
    .unwrap();
    let document_id = created.summary.document_id;
    drain_metadata_background(test.remote.state.as_ref()).await;

    let _ = add_data_entity(
        State(test.coordinator.state.clone()),
        Extension(Some(test.auth.clone())),
        bearer(),
        Path(document_id.clone()),
        Json(json!({
            "@id": "./forwarded.txt",
            "@type": "File",
            "name": "forwarded.txt"
        })),
    )
    .await
    .unwrap();
    assert_eq!(
        delete_metadata_document(
            State(test.coordinator.state.clone()),
            Extension(Some(test.auth.clone())),
            bearer(),
            Path(document_id),
        )
        .await
        .unwrap(),
        StatusCode::NO_CONTENT
    );

    test.shutdown().await;
}

#[tokio::test]
async fn missing_config_fails() {
    let test = setup_access_state().await;
    let context = test.coordinator.state.get_ctx();
    let deleted = context
        .storage_handle
        .send_storage_effect(StorageEffect::Delete {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: (*test.coordinator.state.get_realm_id().as_bytes()).into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        deleted,
        Event::Storage(StorageEvent::DeleteResult { .. })
    ));

    let result = create_metadata_document(
        State(test.coordinator.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test(
            test.valid_bearer_token.clone(),
        ))),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/missing-config".to_string(),
            name: "Missing Config".to_string(),
            description: "Origin must fail closed".to_string(),
            date_published: "2026-01-01".to_string(),
            license: None,
            public: false,
        })),
    )
    .await;
    assert!(matches!(result, Err(ServerError::ServiceUnavailable)));

    test.shutdown().await;
}

#[tokio::test]
async fn search_without_token() {
    let test = setup_access_state().await;
    let ctx = test.remote.state.get_ctx();
    installed_metadata_handle(ctx.as_ref())
        .flush_search_updates()
        .await
        .unwrap();

    let anonymous = search_remote_paths(&test, None, None).await;
    assert_eq!(anonymous.nodes_queried, 1);
    assert_eq!(anonymous.nodes_failed, 0);
    assert_contains_path(&anonymous.paths, "datasets/remote-public");
    assert_excludes_path(&anonymous.paths, "datasets/remote-private");

    test.shutdown().await;
}

async fn run_search_route(
    state: &Arc<ServerState>,
    auth: &AuthContext,
    query: &str,
    limit: usize,
    cursor: Option<String>,
    mode: Option<MetadataQueryMode>,
) -> ServerResult<SearchResultsResponse> {
    search_metadata(
        State(state.clone()),
        Extension(Some(auth.clone())),
        Extension(None),
        Query(MetadataSearchParams {
            q: query.to_string(),
            conforms_to: None,
            group_id: None,
            limit: Some(limit),
            cursor,
            mode,
        }),
    )
    .await
    .map(|(_, Json(response))| response)
}

async fn flush_node_search(node: &DistributedMetadataNode) {
    let ctx = node.state.get_ctx();
    installed_metadata_handle(ctx.as_ref())
        .flush_search_updates()
        .await
        .unwrap();
}

struct SearchPaginationCluster {
    auth: AuthContext,
    group_id: Ulid,
    nodes: Vec<DistributedMetadataNode>,
}

impl SearchPaginationCluster {
    fn coordinator(&self) -> &DistributedMetadataNode {
        &self.nodes[0]
    }

    async fn shutdown(self) {
        for node in self.nodes {
            node.net.shutdown().await;
        }
    }
}

async fn setup_search_cluster(node_count: usize) -> SearchPaginationCluster {
    let realm_signing_key = test_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();

    let mut nodes = Vec::new();
    for _ in 0..node_count {
        nodes.push(spawn_metadata_node(realm_id).await);
    }
    for i in 0..nodes.len() {
        for j in 0..nodes.len() {
            if i != j {
                let addr = nodes[j].net.endpoint_addr();
                nodes[i].net.add_peer_addr(addr).await;
            }
        }
    }
    let node_refs: Vec<&DistributedMetadataNode> = nodes.iter().collect();
    install_realm_config(&node_refs, realm_id, None).await;
    for node in &nodes {
        install_auth_documents(node, realm_id, user_id, group_id).await;
    }

    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    SearchPaginationCluster {
        auth,
        group_id,
        nodes,
    }
}

async fn seed_public_document(
    node: &DistributedMetadataNode,
    auth: &AuthContext,
    group_id: Ulid,
    path: &str,
    name: &str,
) {
    create_test_document(node.state.clone(), auth.clone(), group_id, path, name, true).await;
    drain_metadata_background(node.state.as_ref()).await;
    flush_node_search(node).await;
}

// Drives the coordinator against an explicit node set, matching how the other
// distributed metadata tests fan out (the REST route never sets target_nodes).
async fn search_cluster_page(
    cluster: &SearchPaginationCluster,
    query: &str,
    limit: usize,
    cursor: Option<String>,
) -> aruna_operations::metadata::api::MetadataSearchExecution {
    let coordinator = cluster.coordinator();
    let ctx = coordinator.state.get_ctx();
    let target_nodes = cluster
        .nodes
        .iter()
        .map(|node| node.net.node_id())
        .collect();
    run_search_metadata(
        ctx.as_ref(),
        coordinator.state.get_realm_id(),
        coordinator.state.get_node_id(),
        MetadataSearchRequest {
            auth: Some(cluster.auth.clone()),
            bearer_token: None,
            graph_iris: None,
            query: query.to_string(),
            conforms_to: None,
            group_id: None,
            limit: Some(limit),
            cursor,
            mode: Some(ApiQueryMode::Distributed),
            target_nodes: Some(target_nodes),
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn search_paginates_uniquely() {
    let cluster = setup_search_cluster(3).await;
    for index in 0..9 {
        let node = &cluster.nodes[index % cluster.nodes.len()];
        seed_public_document(
            node,
            &cluster.auth,
            cluster.group_id,
            &format!("datasets/corpus-{index}"),
            &format!("Corpus Document {index}"),
        )
        .await;
    }

    let mut seen_keys: Vec<(String, String)> = Vec::new();
    let mut seen_paths: HashSet<String> = HashSet::new();
    let mut cursor = None;
    let mut last_score = f32::INFINITY;
    let mut pages = 0;
    loop {
        let response = search_cluster_page(&cluster, "Corpus", 3, cursor.clone()).await;
        assert_eq!(response.fanout_stats.nodes_failed, 0);
        assert!(response.hits.len() <= 3);
        for hit in &response.hits {
            assert!(
                hit.score <= last_score,
                "scores must be non-increasing across pages"
            );
            last_score = hit.score;
            seen_keys.push((hit.graph_iri.clone(), hit.subject_iri.clone()));
            seen_paths.insert(hit.document_path.clone());
        }
        pages += 1;
        assert!(pages <= 20, "pagination failed to terminate");
        match response.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    let unique: HashSet<_> = seen_keys.iter().cloned().collect();
    assert_eq!(
        unique.len(),
        seen_keys.len(),
        "pages must not repeat a (graph_iri, subject_iri)"
    );
    for index in 0..9 {
        assert!(
            seen_paths.contains(&format!("datasets/corpus-{index}")),
            "every seeded document must appear across the pages"
        );
    }

    cluster.shutdown().await;
}

#[tokio::test]
async fn pagination_survives_failure() {
    let cluster = setup_search_cluster(3).await;
    for index in 0..9 {
        let node = &cluster.nodes[index % cluster.nodes.len()];
        seed_public_document(
            node,
            &cluster.auth,
            cluster.group_id,
            &format!("datasets/corpus-{index}"),
            &format!("Corpus Document {index}"),
        )
        .await;
    }

    let page1 = search_cluster_page(&cluster, "Corpus", 3, None).await;
    assert_eq!(page1.fanout_stats.nodes_failed, 0);
    let cursor = page1.next_cursor.clone().expect("more pages remain");

    // Drop a non-coordinator holder mid-session; paging must continue.
    cluster.nodes[2].net.shutdown().await;

    let page2 = search_cluster_page(&cluster, "Corpus", 3, Some(cursor)).await;
    assert!(
        page2.fanout_stats.nodes_failed >= 1,
        "the downed node must surface as a failed partition"
    );

    cluster.shutdown().await;
}

async fn search_cluster_group(
    cluster: &SearchPaginationCluster,
    query: &str,
    group_id: Option<Ulid>,
) -> aruna_operations::metadata::api::MetadataSearchExecution {
    let coordinator = cluster.coordinator();
    let ctx = coordinator.state.get_ctx();
    let target_nodes = cluster
        .nodes
        .iter()
        .map(|node| node.net.node_id())
        .collect();
    run_search_metadata(
        ctx.as_ref(),
        coordinator.state.get_realm_id(),
        coordinator.state.get_node_id(),
        MetadataSearchRequest {
            auth: Some(cluster.auth.clone()),
            bearer_token: None,
            graph_iris: None,
            query: query.to_string(),
            conforms_to: None,
            group_id,
            limit: Some(50),
            cursor: None,
            mode: Some(ApiQueryMode::Distributed),
            target_nodes: Some(target_nodes),
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn group_filter_fanout() {
    // Group filter must hold across every fanned-out node.
    let cluster = setup_search_cluster(3).await;
    let other_group = Ulid::generate();
    for node in &cluster.nodes {
        install_auth_documents(
            node,
            node.state.get_realm_id(),
            cluster.auth.user_id,
            other_group,
        )
        .await;
    }
    for index in 0..6 {
        let node = &cluster.nodes[index % cluster.nodes.len()];
        let (group_id, label) = if index % 2 == 0 {
            (cluster.group_id, "primary")
        } else {
            (other_group, "other")
        };
        seed_public_document(
            node,
            &cluster.auth,
            group_id,
            &format!("datasets/fanout-{label}-{index}"),
            &format!("Fanout Document {index}"),
        )
        .await;
    }

    let filtered = search_cluster_group(&cluster, "Fanout", Some(cluster.group_id)).await;
    assert!(!filtered.hits.is_empty());
    assert!(
        filtered
            .hits
            .iter()
            .all(|hit| hit.group_id == cluster.group_id.to_string()),
        "the group filter must hold across every fanned-out node"
    );

    let unfiltered = search_cluster_group(&cluster, "Fanout", None).await;
    let groups: HashSet<String> = unfiltered
        .hits
        .iter()
        .map(|hit| hit.group_id.clone())
        .collect();
    assert!(groups.contains(&cluster.group_id.to_string()));
    assert!(groups.contains(&other_group.to_string()));

    cluster.shutdown().await;
}

#[tokio::test]
async fn discovery_failure_partial() {
    let test = setup_state().await;
    let _ = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/discovery-partial".to_string(),
            name: "Discovery Partial Dataset".to_string(),
            description: "discovery failure fixture".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public: true,
        })),
    )
    .await
    .unwrap();
    drain_metadata_background(test.state.as_ref()).await;
    let ctx = test.state.get_ctx();
    installed_metadata_handle(ctx.as_ref())
        .flush_search_updates()
        .await
        .unwrap();

    let no_net_ctx = DriverContext {
        net_handle: None,
        ..ctx.as_ref().clone()
    };

    let result = run_search_metadata(
        &no_net_ctx,
        test.state.get_realm_id(),
        test.state.get_node_id(),
        MetadataSearchRequest {
            auth: Some(test.auth.clone()),
            bearer_token: None,
            graph_iris: None,
            query: "Discovery".to_string(),
            conforms_to: None,
            group_id: None,
            limit: Some(10),
            cursor: None,
            mode: Some(ApiQueryMode::Distributed),
            target_nodes: None,
        },
    )
    .await
    .unwrap();

    assert_eq!(result.fanout_stats.nodes_queried, 1);
    assert_eq!(result.fanout_stats.nodes_failed, 1);
    assert!(!result.truncated);
}

#[tokio::test]
async fn invalid_cursor_rejected() {
    let test = setup_network_state().await;
    let _ = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/alpha".to_string(),
            name: "Alpha Widget".to_string(),
            description: "cursor fixture".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public: true,
        })),
    )
    .await
    .unwrap();
    drain_metadata_background(test.state.as_ref()).await;
    let ctx = test.state.get_ctx();
    installed_metadata_handle(ctx.as_ref())
        .flush_search_updates()
        .await
        .unwrap();

    let page = run_search_route(
        &test.state,
        &test.auth,
        "Widget",
        1,
        None,
        Some(MetadataQueryMode::Local),
    )
    .await
    .unwrap();
    let cursor = page.next_cursor.clone();

    let malformed = run_search_route(
        &test.state,
        &test.auth,
        "Widget",
        1,
        Some("!!!not-a-cursor!!!".to_string()),
        Some(MetadataQueryMode::Local),
    )
    .await;
    assert!(matches!(malformed, Err(ServerError::BadRequestMessage(_))));

    if let Some(cursor) = cursor {
        let mut tampered = cursor.as_bytes().to_vec();
        let tamper_index = tampered.len() / 2;
        tampered[tamper_index] = if tampered[tamper_index] == b'A' {
            b'B'
        } else {
            b'A'
        };
        let tampered = String::from_utf8(tampered).unwrap();
        let tampered_result = run_search_route(
            &test.state,
            &test.auth,
            "Widget",
            1,
            Some(tampered),
            Some(MetadataQueryMode::Local),
        )
        .await;
        assert!(matches!(
            tampered_result,
            Err(ServerError::BadRequestMessage(_))
        ));

        let mismatched = run_search_route(
            &test.state,
            &test.auth,
            "Different",
            1,
            Some(cursor),
            Some(MetadataQueryMode::Local),
        )
        .await;
        match mismatched {
            Err(ServerError::BadRequestMessage(message)) => {
                assert!(message.contains("does not match query"), "{message}");
            }
            other => panic!("expected a query-mismatch rejection, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn cursor_suppresses_churn() {
    let test = setup_network_state().await;
    for index in 0..5 {
        let _ = create_metadata_document(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Extension(None),
            Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
                group_id: test.group_id.to_string(),
                path: format!("datasets/widget-{index}"),
                name: format!("Widget {index}"),
                description: "churn fixture".to_string(),
                date_published: "2026-01-01".to_string(),
                license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
                public: true,
            })),
        )
        .await
        .unwrap();
    }
    drain_metadata_background(test.state.as_ref()).await;
    let ctx = test.state.get_ctx();
    installed_metadata_handle(ctx.as_ref())
        .flush_search_updates()
        .await
        .unwrap();

    let page1 = run_search_route(
        &test.state,
        &test.auth,
        "Widget",
        2,
        None,
        Some(MetadataQueryMode::Local),
    )
    .await
    .unwrap();
    let page1_keys: HashSet<(String, String)> = page1
        .hits
        .iter()
        .map(|hit| (hit.graph_iri.clone(), hit.subject_iri.clone()))
        .collect();
    let cursor = page1.next_cursor.clone().expect("more pages remain");

    // Introduce a matching document between pages (churn).
    let _ = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/widget-extra".to_string(),
            name: "Widget Extra".to_string(),
            description: "churn fixture".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public: true,
        })),
    )
    .await
    .unwrap();
    drain_metadata_background(test.state.as_ref()).await;
    installed_metadata_handle(ctx.as_ref())
        .flush_search_updates()
        .await
        .unwrap();

    let page2 = run_search_route(
        &test.state,
        &test.auth,
        "Widget",
        2,
        Some(cursor),
        Some(MetadataQueryMode::Local),
    )
    .await
    .unwrap();
    for hit in &page2.hits {
        let key = (hit.graph_iri.clone(), hit.subject_iri.clone());
        assert!(
            !page1_keys.contains(&key),
            "already emitted hits must not repeat under churn"
        );
    }

    let fresh = run_search_route(
        &test.state,
        &test.auth,
        "Widget",
        10,
        None,
        Some(MetadataQueryMode::Local),
    )
    .await
    .unwrap();
    assert!(
        fresh
            .hits
            .iter()
            .any(|hit| hit.document_path == "datasets/widget-extra"),
        "a fresh search must surface the churned document"
    );
}

#[tokio::test]
async fn page_size_clamped() {
    let test = setup_network_state().await;
    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/capacity".to_string(),
            name: "Placeholder Dataset".to_string(),
            description: "placeholder".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public: true,
        })),
    )
    .await
    .unwrap();
    let document_id = created.summary.document_id.clone();
    drain_metadata_background(test.state.as_ref()).await;

    let files: String = (0..105)
            .map(|index| {
                format!(
                    r#"{{"@id":"./data/capacity-{index}.txt","@type":"File","name":"Capacity file {index}"}}"#
                )
            })
            .collect::<Vec<_>>()
            .join(",");
    let parts: String = (0..105)
        .map(|index| format!(r#"{{"@id":"./data/capacity-{index}.txt"}}"#))
        .collect::<Vec<_>>()
        .join(",");
    let rocrate = format!(
        r#"{{"@context":"https://w3id.org/ro/crate/1.2/context","@graph":[{{"@id":"ro-crate-metadata.json","@type":"CreativeWork","conformsTo":{{"@id":"https://w3id.org/ro/crate/1.2"}},"about":{{"@id":"https://w3id.org/aruna/{document_id}"}}}},{{"@id":"https://w3id.org/aruna/{document_id}","@type":"Dataset","name":"Capacity Dataset","description":"capacity","datePublished":"2026-01-01","license":{{"@id":"https://creativecommons.org/licenses/by/4.0/"}},"hasPart":[{parts}]}},{files}]}}"#
    );
    let _ = replace_metadata_rocrate(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Path(document_id.clone()),
        Json(ReplaceRoCrateRequest {
            rocrate: serde_json::from_str(&rocrate).unwrap(),
            public: Some(true),
        }),
    )
    .await
    .unwrap();
    drain_metadata_background(test.state.as_ref()).await;
    let ctx = test.state.get_ctx();
    installed_metadata_handle(ctx.as_ref())
        .flush_search_updates()
        .await
        .unwrap();

    let response = run_search_route(
        &test.state,
        &test.auth,
        "Capacity",
        250,
        None,
        Some(MetadataQueryMode::Local),
    )
    .await
    .unwrap();
    assert!(
        response.hits.len() <= 100,
        "page size must be clamped to the cap, got {}",
        response.hits.len()
    );
    assert!(
        response.next_cursor.is_some(),
        "a corpus larger than the cap must offer a continuation"
    );

    // A request at exactly the cap fills a full page rather than being reduced.
    let boundary = run_search_route(
        &test.state,
        &test.auth,
        "Capacity",
        100,
        None,
        Some(MetadataQueryMode::Local),
    )
    .await
    .unwrap();
    assert_eq!(boundary.hits.len(), 100);
    assert!(boundary.next_cursor.is_some());
}

async fn install_group_auth(test: &TestState, group_id: Ulid) {
    let realm_id = test.state.get_realm_id();
    let actor = Actor {
        node_id: test.state.get_node_id(),
        user_id: test.auth.user_id,
        realm_id,
    };
    let group_auth =
        GroupAuthorizationDocument::default_group_doc(test.auth.user_id, realm_id, group_id);
    let group = Group {
        display_name: "second-metadata-group".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner: test.auth.user_id,
    };
    let ctx = test.state.get_ctx();
    write_doc(
        &ctx,
        AUTH_KEYSPACE,
        group_id.to_bytes().into(),
        group_auth.to_bytes(&actor).unwrap().into(),
    )
    .await;
    write_doc(
        &ctx,
        GROUP_KEYSPACE,
        group_id.to_bytes().into(),
        group.to_bytes(&actor).unwrap().into(),
    )
    .await;
}

async fn search_group_scoped(
    test: &TestState,
    query: &str,
    group_id: Option<Ulid>,
    limit: usize,
    cursor: Option<String>,
) -> ServerResult<SearchResultsResponse> {
    search_metadata(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Query(MetadataSearchParams {
            q: query.to_string(),
            conforms_to: None,
            group_id: group_id.map(|id| id.to_string()),
            limit: Some(limit),
            cursor,
            mode: Some(MetadataQueryMode::Local),
        }),
    )
    .await
    .map(|(_, Json(response))| response)
}

#[tokio::test]
async fn group_filter_scopes() {
    // Filtered search returns only the named group's hits; unfiltered spans groups.
    let test = setup_network_state().await;
    let other_group = Ulid::generate();
    install_group_auth(&test, other_group).await;
    create_test_document(
        test.state.clone(),
        test.auth.clone(),
        test.group_id,
        "datasets/scoped-alpha",
        "Scoped Alpha",
        true,
    )
    .await;
    create_test_document(
        test.state.clone(),
        test.auth.clone(),
        other_group,
        "datasets/scoped-beta",
        "Scoped Beta",
        true,
    )
    .await;
    drain_metadata_background(test.state.as_ref()).await;
    let ctx = test.state.get_ctx();
    installed_metadata_handle(ctx.as_ref())
        .flush_search_updates()
        .await
        .unwrap();

    let unfiltered = search_group_scoped(&test, "Scoped", None, 10, None)
        .await
        .unwrap();
    let paths: HashSet<String> = unfiltered
        .hits
        .iter()
        .map(|hit| hit.document_path.clone())
        .collect();
    assert!(paths.contains("datasets/scoped-alpha"));
    assert!(paths.contains("datasets/scoped-beta"));

    let primary = search_group_scoped(&test, "Scoped", Some(test.group_id), 10, None)
        .await
        .unwrap();
    assert_eq!(primary.hits.len(), 1);
    assert_eq!(primary.hits[0].group_id, test.group_id.to_string());
    assert_eq!(primary.hits[0].document_path, "datasets/scoped-alpha");

    let other = search_group_scoped(&test, "Scoped", Some(other_group), 10, None)
        .await
        .unwrap();
    assert_eq!(other.hits.len(), 1);
    assert_eq!(other.hits[0].group_id, other_group.to_string());
}

#[tokio::test]
async fn cursor_binds_filter() {
    // A cursor is bound to its group filter: adding or dropping the filter on
    // replay must be rejected rather than silently paging a different set.
    let test = setup_network_state().await;
    for index in 0..2 {
        create_test_document(
            test.state.clone(),
            test.auth.clone(),
            test.group_id,
            &format!("datasets/bound-{index}"),
            &format!("Bound Widget {index}"),
            true,
        )
        .await;
    }
    drain_metadata_background(test.state.as_ref()).await;
    let ctx = test.state.get_ctx();
    installed_metadata_handle(ctx.as_ref())
        .flush_search_updates()
        .await
        .unwrap();

    let filtered = search_group_scoped(&test, "Bound", Some(test.group_id), 1, None)
        .await
        .unwrap();
    let filtered_cursor = filtered.next_cursor.expect("filtered page continues");
    let dropped = search_group_scoped(&test, "Bound", None, 1, Some(filtered_cursor.clone())).await;
    assert!(matches!(dropped, Err(ServerError::BadRequestMessage(_))));

    let plain = search_group_scoped(&test, "Bound", None, 1, None)
        .await
        .unwrap();
    let plain_cursor = plain.next_cursor.expect("plain page continues");
    let added =
        search_group_scoped(&test, "Bound", Some(test.group_id), 1, Some(plain_cursor)).await;
    assert!(matches!(added, Err(ServerError::BadRequestMessage(_))));
}

#[tokio::test]
async fn search_tolerates_pending() {
    let test = setup_state().await;
    let _ = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/pending".to_string(),
            name: "Pending Dataset".to_string(),
            description: "pending fixture".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public: true,
        })),
    )
    .await
    .unwrap();

    // Search before draining the projection queue: it must not 5xx, and any
    // hit that does surface must still carry an enriched, non-empty title.
    let response = run_search_route(
        &test.state,
        &test.auth,
        "Pending",
        10,
        None,
        Some(MetadataQueryMode::Local),
    )
    .await
    .unwrap();
    for hit in &response.hits {
        assert!(!hit.title.is_empty(), "title must always be populated");
    }
}

struct DistributedAccessState {
    auth: AuthContext,
    denied_auth: AuthContext,
    group_id: Ulid,
    valid_bearer_token: String,
    denied_bearer_token: String,
    coordinator: DistributedMetadataNode,
    remote: DistributedMetadataNode,
}

impl DistributedAccessState {
    async fn shutdown(self) {
        self.coordinator.net.shutdown().await;
        self.remote.net.shutdown().await;
    }
}

struct DistributedMetadataNode {
    _node_dir: TempDir,
    net: NetHandle,
    state: Arc<ServerState>,
}

struct QueryNamesResult {
    names: Vec<String>,
    nodes_queried: usize,
    nodes_failed: usize,
    failed_partitions: Vec<aruna_core::NodeId>,
}

struct SearchPathsResult {
    paths: Vec<String>,
    nodes_queried: usize,
    nodes_failed: usize,
}

async fn setup_access_state() -> DistributedAccessState {
    let realm_signing_key = test_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
    let denied_user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    let coordinator = spawn_metadata_node(realm_id).await;
    let remote = spawn_metadata_node(realm_id).await;
    let nodes = [&coordinator, &remote];

    coordinator
        .net
        .add_peer_addr(remote.net.endpoint_addr())
        .await;
    remote
        .net
        .add_peer_addr(coordinator.net.endpoint_addr())
        .await;
    // The device forwards for the owner its realm config names, so the
    // fixture binds it to the user whose token the writes carry.
    install_realm_config(&nodes, realm_id, Some((coordinator.net.node_id(), user_id))).await;
    install_auth_documents(&remote, realm_id, user_id, group_id).await;

    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let denied_auth = AuthContext {
        user_id: denied_user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    create_test_document(
        remote.state.clone(),
        auth.clone(),
        group_id,
        "datasets/remote-public",
        "Remote Public Dataset",
        true,
    )
    .await;
    create_test_document(
        remote.state.clone(),
        auth.clone(),
        group_id,
        "datasets/remote-private",
        "Remote Private Dataset",
        false,
    )
    .await;
    drain_metadata_background(remote.state.as_ref()).await;

    let valid_bearer_token =
        sign_test_token(&realm_signing_key, &test_token_claims(realm_id, user_id));
    let denied_bearer_token = sign_test_token(
        &realm_signing_key,
        &test_token_claims(realm_id, denied_user_id),
    );

    DistributedAccessState {
        auth,
        denied_auth,
        group_id,
        valid_bearer_token,
        denied_bearer_token,
        coordinator,
        remote,
    }
}

async fn spawn_metadata_node(realm_id: RealmId) -> DistributedMetadataNode {
    let node_dir = tempfile::tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(node_dir.path().to_str().unwrap()).unwrap();
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage_handle.clone(),
    )
    .await
    .unwrap();
    let metadata_handle = MetadataHandle::new(
        node_dir.path().join("metadata"),
        net.node_id(),
        storage_handle.clone(),
        Some(net.clone()),
        Some(net.document_sync_node()),
        Some(net.document_sync_database()),
    )
    .unwrap();
    let context = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: Some(metadata_handle),
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });
    initialize_incoming_fixture(context.clone());
    let state = Arc::new(
        ServerState::new(
            context,
            realm_id,
            net.node_id(),
            NodeCapabilities::user_node(realm_id).unwrap(),
            false,
            None,
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await,
    );

    DistributedMetadataNode {
        _node_dir: node_dir,
        net,
        state,
    }
}

async fn install_realm_config(
    nodes: &[&DistributedMetadataNode],
    realm_id: RealmId,
    user_node: Option<(aruna_core::NodeId, aruna_core::UserId)>,
) {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    for (band, node) in nodes.iter().enumerate() {
        let kind = match user_node {
            Some((node_id, owner)) if node_id == node.net.node_id() => {
                RealmNodeKind::User { owner }
            }
            _ => RealmNodeKind::Server,
        };
        config.ensure_node(node.net.node_id(), kind);
        config.seed_job_control(node.net.node_id(), band as u32);
    }

    for node in nodes {
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: aruna_core::UserId::nil(realm_id),
            realm_id,
        };
        write_doc(
            &node.state.get_ctx(),
            REALM_CONFIG_KEYSPACE,
            (*realm_id.as_bytes()).into(),
            config.to_bytes(&actor).unwrap().into(),
        )
        .await;
        node.net.refresh_document_peers(&config).await.unwrap();
    }
}

async fn install_auth_documents(
    node: &DistributedMetadataNode,
    realm_id: RealmId,
    user_id: aruna_core::UserId,
    group_id: Ulid,
) {
    let actor = Actor {
        node_id: node.net.node_id(),
        user_id,
        realm_id,
    };
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
    write_doc(
        &node.state.get_ctx(),
        AUTH_KEYSPACE,
        (*realm_id.as_bytes()).into(),
        realm_auth.to_bytes(&actor).unwrap().into(),
    )
    .await;
    install_group_documents(node, realm_id, user_id, group_id).await;
}

/// A group owned by `owner`, with its authorization document, so a caller
/// outside it is refused by the group check rather than by its absence.
async fn install_group_documents(
    node: &DistributedMetadataNode,
    realm_id: RealmId,
    owner: aruna_core::UserId,
    group_id: Ulid,
) {
    let actor = Actor {
        node_id: node.net.node_id(),
        user_id: owner,
        realm_id,
    };
    let group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    let group = Group {
        display_name: "distributed-metadata-group".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    let context = node.state.get_ctx();

    write_doc(
        &context,
        AUTH_KEYSPACE,
        group_id.to_bytes().into(),
        group_auth.to_bytes(&actor).unwrap().into(),
    )
    .await;
    write_doc(
        &context,
        GROUP_KEYSPACE,
        group_id.to_bytes().into(),
        group.to_bytes(&actor).unwrap().into(),
    )
    .await;
}

async fn references_route(
    test: &TestState,
    auth: Option<AuthContext>,
    iri: &str,
    predicate: Option<&str>,
    limit: Option<usize>,
    resolve: bool,
) -> ServerResult<MetadataReferencesResponse> {
    metadata_references(
        State(test.state.clone()),
        Extension(auth),
        Query(MetadataReferencesParams {
            iri: iri.to_string(),
            predicate: predicate.map(str::to_string),
            limit,
            resolve,
        }),
    )
    .await
    .map(|(_, Json(response))| response)
}

async fn preflight_route(
    test: &TestState,
    auth: Option<AuthContext>,
    target: PreflightTargetBody,
    mode: MetadataQueryMode,
    allow_partial: bool,
    limit: Option<usize>,
    cursor: Option<String>,
) -> ServerResult<PreflightResponse> {
    metadata_reference_preflight(
        State(test.state.clone()),
        Extension(auth),
        Extension(None),
        Json(PreflightBody {
            target,
            mode: Some(mode),
            allow_partial,
            limit,
            cursor,
        }),
    )
    .await
    .map(|(_, Json(response))| response)
}

fn preflight_w3id(hash: [u8; 32]) -> String {
    format!("https://w3id.org/aruna/data/{}", hex::encode(hash))
}

async fn create_linking_doc(
    test: &TestState,
    auth: AuthContext,
    group_id: Ulid,
    path: &str,
    name: &str,
    public: bool,
    links: &[(&str, &str)],
) -> String {
    let mut root = serde_json::Map::new();
    root.insert("@id".to_string(), json!("urn:aruna-test:root"));
    root.insert("@type".to_string(), json!("Dataset"));
    root.insert("name".to_string(), json!(name));
    root.insert("description".to_string(), json!("Reference lookup fixture"));
    root.insert("datePublished".to_string(), json!("2026-01-01"));
    root.insert(
        "license".to_string(),
        json!({ "@id": "https://creativecommons.org/licenses/by/4.0/" }),
    );
    for (predicate, object) in links {
        root.insert((*predicate).to_string(), json!({ "@id": object }));
    }
    let rocrate = json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": { "@id": "https://w3id.org/ro/crate/1.2" },
                "about": { "@id": "urn:aruna-test:root" }
            },
            Value::Object(root)
        ]
    });
    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(auth)),
        Extension(None),
        Json(CreateMetadataRequest::RoCrate(CreateRoCrateRequest {
            group_id: group_id.to_string(),
            path: path.to_string(),
            public,
            rocrate,
        })),
    )
    .await
    .unwrap();
    created.summary.document_id
}

async fn seed_owned_group(test: &TestState, group_id: Ulid, owner: aruna_core::UserId) {
    let realm_id = test.state.get_realm_id();
    let actor = Actor {
        node_id: test.state.get_node_id(),
        user_id: owner,
        realm_id,
    };
    let group_auth = GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
    let group = Group {
        display_name: "foreign-metadata-group".to_string(),
        group_id,
        realm_id,
        roles: group_auth.roles.keys().copied().collect(),
        owner,
    };
    let ctx = test.state.get_ctx();
    write_doc(
        &ctx,
        AUTH_KEYSPACE,
        group_id.to_bytes().into(),
        group_auth.to_bytes(&actor).unwrap().into(),
    )
    .await;
    write_doc(
        &ctx,
        GROUP_KEYSPACE,
        group_id.to_bytes().into(),
        group.to_bytes(&actor).unwrap().into(),
    )
    .await;
}

#[tokio::test]
async fn references_lists_docs() {
    let test = setup_state().await;
    let alpha = create_linking_doc(
        &test,
        test.auth.clone(),
        test.group_id,
        "datasets/alpha",
        "Alpha",
        true,
        &[("license", "https://example.test/target-a")],
    )
    .await;
    create_linking_doc(
        &test,
        test.auth.clone(),
        test.group_id,
        "datasets/beta",
        "Beta",
        true,
        &[("license", "https://example.test/target-b")],
    )
    .await;
    drain_metadata_background(test.state.as_ref()).await;

    let response = references_route(
        &test,
        Some(test.auth.clone()),
        "https://example.test/target-a",
        None,
        None,
        false,
    )
    .await
    .unwrap();
    assert_eq!(response.references.len(), 1);
    let entry = &response.references[0];
    assert_eq!(entry.document_id, alpha);
    assert_eq!(entry.document_path, "datasets/alpha");
    assert_eq!(
        entry.predicate.as_deref(),
        Some("http://schema.org/license")
    );
    assert_eq!(entry.title.as_deref(), Some("Alpha"));
    assert!(!entry.subject_iris.is_empty());
}

#[tokio::test]
async fn references_filters_predicate() {
    let test = setup_state().await;
    let doc = create_linking_doc(
        &test,
        test.auth.clone(),
        test.group_id,
        "datasets/linker",
        "Linker",
        true,
        &[
            ("license", "https://example.test/shared"),
            ("creator", "https://example.test/shared"),
        ],
    )
    .await;
    drain_metadata_background(test.state.as_ref()).await;

    let all = references_route(
        &test,
        Some(test.auth.clone()),
        "https://example.test/shared",
        None,
        None,
        false,
    )
    .await
    .unwrap();
    assert_eq!(all.references.len(), 2);
    let predicates: HashSet<String> = all
        .references
        .iter()
        .filter_map(|r| r.predicate.clone())
        .collect();
    assert!(predicates.contains("http://schema.org/license"));
    assert!(predicates.contains("http://schema.org/creator"));

    let filtered = references_route(
        &test,
        Some(test.auth.clone()),
        "https://example.test/shared",
        Some("http://schema.org/license"),
        None,
        false,
    )
    .await
    .unwrap();
    assert_eq!(filtered.references.len(), 1);
    assert_eq!(
        filtered.references[0].predicate.as_deref(),
        Some("http://schema.org/license")
    );
    assert_eq!(filtered.references[0].document_id, doc);
}

#[tokio::test]
async fn references_omits_unauthorized() {
    // A private backlink from a group the caller cannot read is dropped.
    let test = setup_state().await;
    let realm_id = test.state.get_realm_id();
    let foreign_user = aruna_core::UserId::local(Ulid::generate(), realm_id);
    let foreign_group = Ulid::generate();
    seed_owned_group(&test, foreign_group, foreign_user).await;
    let foreign_auth = AuthContext {
        user_id: foreign_user,
        realm_id,
        path_restrictions: None,
        session: None,
    };

    let visible = create_linking_doc(
        &test,
        test.auth.clone(),
        test.group_id,
        "datasets/visible",
        "Visible",
        true,
        &[("license", "https://example.test/shared")],
    )
    .await;
    let hidden = create_linking_doc(
        &test,
        foreign_auth.clone(),
        foreign_group,
        "datasets/hidden",
        "Hidden",
        false,
        &[("license", "https://example.test/shared")],
    )
    .await;
    drain_metadata_background(test.state.as_ref()).await;

    let caller = references_route(
        &test,
        Some(test.auth.clone()),
        "https://example.test/shared",
        None,
        None,
        false,
    )
    .await
    .unwrap();
    assert_eq!(caller.references.len(), 1);
    assert_eq!(caller.references[0].document_id, visible);

    // The owner sees the hidden doc, proving the filter is authorization.
    let owner = references_route(
        &test,
        Some(foreign_auth),
        "https://example.test/shared",
        None,
        None,
        false,
    )
    .await
    .unwrap();
    let ids: HashSet<String> = owner
        .references
        .iter()
        .map(|r| r.document_id.clone())
        .collect();
    assert!(ids.contains(&hidden));
    assert!(ids.contains(&visible));
}

#[tokio::test]
async fn references_resolves_graph() {
    let test = setup_state().await;
    let doc = create_linking_doc(
        &test,
        test.auth.clone(),
        test.group_id,
        "datasets/graphdoc",
        "Graph Doc",
        true,
        &[],
    )
    .await;
    drain_metadata_background(test.state.as_ref()).await;
    let graph_iri = format!("https://w3id.org/aruna/{doc}");

    let response = references_route(&test, Some(test.auth.clone()), &graph_iri, None, None, true)
        .await
        .unwrap();
    assert_eq!(response.references.len(), 1);
    let entry = &response.references[0];
    assert_eq!(entry.document_id, doc);
    assert_eq!(entry.graph_iri, graph_iri);
    assert!(entry.predicate.is_none());
    assert!(entry.subject_iris.is_empty());
    assert_eq!(entry.title.as_deref(), Some("Graph Doc"));
}

#[tokio::test]
async fn references_unknown_empty() {
    let test = setup_state().await;
    create_linking_doc(
        &test,
        test.auth.clone(),
        test.group_id,
        "datasets/any",
        "Any",
        true,
        &[("license", "https://example.test/known")],
    )
    .await;
    drain_metadata_background(test.state.as_ref()).await;

    let response = references_route(
        &test,
        Some(test.auth.clone()),
        "https://example.test/absent",
        None,
        None,
        false,
    )
    .await
    .unwrap();
    assert!(response.references.is_empty());
}

#[tokio::test]
async fn references_requires_auth() {
    let test = setup_state().await;
    let result = references_route(&test, None, "https://example.test/x", None, None, false).await;
    assert!(matches!(result, Err(ServerError::Unauthorized)));
}

#[tokio::test]
async fn references_clamps_limit() {
    // limit 0 clamps up to 1; a small limit caps the returned backlinks.
    let test = setup_state().await;
    create_linking_doc(
        &test,
        test.auth.clone(),
        test.group_id,
        "datasets/one",
        "One",
        true,
        &[("license", "https://example.test/shared")],
    )
    .await;
    create_linking_doc(
        &test,
        test.auth.clone(),
        test.group_id,
        "datasets/two",
        "Two",
        true,
        &[("license", "https://example.test/shared")],
    )
    .await;
    drain_metadata_background(test.state.as_ref()).await;

    let clamped = references_route(
        &test,
        Some(test.auth.clone()),
        "https://example.test/shared",
        None,
        Some(0),
        false,
    )
    .await
    .unwrap();
    assert_eq!(clamped.references.len(), 1);

    let full = references_route(
        &test,
        Some(test.auth.clone()),
        "https://example.test/shared",
        None,
        Some(100),
        false,
    )
    .await
    .unwrap();
    assert_eq!(full.references.len(), 2);
}

#[tokio::test]
async fn preflight_distinguishes_states() {
    let test = setup_state().await;
    let w3id = preflight_w3id([31u8; 32]);
    let target = || PreflightTargetBody::ContentW3ids {
        content_w3ids: vec![w3id.clone()],
        remove_resolvable_locations: false,
    };

    let no_references = preflight_route(
        &test,
        Some(test.auth.clone()),
        target(),
        MetadataQueryMode::Local,
        true,
        None,
        None,
    )
    .await
    .unwrap();
    assert!(no_references.complete);
    assert!(no_references.targets[0].visible_references.is_empty());
    assert!(!no_references.targets[0].hidden_references_exist);
    assert!(!no_references.coverage.realm_coverage_complete);
    let excluded_forms = no_references
        .coverage
        .excluded_forms
        .iter()
        .map(|excluded| excluded.form.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(
        excluded_forms,
        HashSet::from([
            "literal_content_url",
            "imported_relative_identity",
            "imported_external_identity",
        ])
    );

    create_linking_doc(
        &test,
        test.auth.clone(),
        test.group_id,
        "datasets/pending-preflight",
        "Pending preflight",
        true,
        &[("license", &w3id)],
    )
    .await;
    let incomplete = preflight_route(
        &test,
        Some(test.auth.clone()),
        target(),
        MetadataQueryMode::Local,
        true,
        None,
        None,
    )
    .await
    .unwrap();
    assert!(!incomplete.complete);
    assert!(
        incomplete
            .coverage
            .node_freshness
            .iter()
            .any(|freshness| { matches!(freshness.index_state.as_str(), "pending" | "mixed") })
    );
    let strict = preflight_route(
        &test,
        Some(test.auth.clone()),
        target(),
        MetadataQueryMode::Local,
        false,
        None,
        None,
    )
    .await;
    assert!(matches!(strict, Err(ServerError::ServiceUnavailable)));

    let unauthorized = preflight_route(
        &test,
        None,
        target(),
        MetadataQueryMode::Local,
        true,
        None,
        None,
    )
    .await;
    assert!(matches!(unauthorized, Err(ServerError::Unauthorized)));
}

#[tokio::test]
async fn hidden_references_concealed() {
    let test = setup_state().await;
    let realm_id = test.state.get_realm_id();
    let foreign_user = aruna_core::UserId::local(Ulid::generate(), realm_id);
    let foreign_group = Ulid::generate();
    seed_owned_group(&test, foreign_group, foreign_user).await;
    let foreign_auth = AuthContext {
        user_id: foreign_user,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let w3id = preflight_w3id([32u8; 32]);
    let visible_id = create_linking_doc(
        &test,
        test.auth.clone(),
        test.group_id,
        "datasets/visible-preflight",
        "Visible preflight",
        true,
        &[("license", &w3id)],
    )
    .await;
    let hidden_id = create_linking_doc(
        &test,
        foreign_auth,
        foreign_group,
        "datasets/hidden-preflight",
        "Restricted preflight secret",
        false,
        &[("license", &w3id)],
    )
    .await;
    drain_metadata_background(test.state.as_ref()).await;

    let response = preflight_route(
        &test,
        Some(test.auth.clone()),
        PreflightTargetBody::ContentW3ids {
            content_w3ids: vec![w3id],
            remove_resolvable_locations: false,
        },
        MetadataQueryMode::Local,
        true,
        None,
        None,
    )
    .await
    .unwrap();
    let target = &response.targets[0];
    assert!(target.hidden_references_exist);
    assert_eq!(target.visible_references.len(), 1);
    assert_eq!(target.visible_references[0].document_id, visible_id);
    let serialized = serde_json::to_string(&response).unwrap();
    assert!(!serialized.contains(&hidden_id));
    assert!(!serialized.contains("Restricted preflight secret"));
    assert!(!serialized.contains("hidden_references_count"));
}

#[tokio::test]
async fn preflight_finds_ids() {
    let test = setup_state().await;
    test.state
        .register_s3_interface("127.0.0.1:9000".parse().unwrap(), "https://s3.example.test")
        .await;
    let bucket = "preflight-bucket";
    let key = "folder/file.bin";
    let hash = [33u8; 32];
    let version_id = Ulid::generate();
    let ctx = test.state.get_ctx();
    let bucket_info = BucketInfo {
        group_id: test.group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: test.auth.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
    };
    write_doc(
        &ctx,
        S3_BUCKET_KEYSPACE,
        bucket.as_bytes().into(),
        bucket_info.to_bytes().unwrap().into(),
    )
    .await;
    write_doc(
        &ctx,
        BLOB_HEAD_KEYSPACE,
        BlobHeadKey::new(bucket, key).to_bytes().unwrap().into(),
        CurrentVersionPointer::new(version_id)
            .to_bytes()
            .unwrap()
            .into(),
    )
    .await;
    write_doc(
        &ctx,
        BLOB_VERSIONS_KEYSPACE,
        VersionKey::new(bucket, key, version_id)
            .to_bytes()
            .unwrap()
            .into(),
        BlobVersion::materialized(
            hash,
            BackendRef::node_default(),
            SystemTime::UNIX_EPOCH,
            test.auth.user_id,
            None,
        )
        .to_bytes()
        .unwrap()
        .into(),
    )
    .await;
    write_doc(
        &ctx,
        PATHS_INDEX_KEYSPACE,
        HashIndex::new(
            hash,
            version_id,
            test.state.get_realm_id(),
            test.group_id,
            test.state.get_node_id(),
            bucket,
            key,
        )
        .to_bytes()
        .unwrap()
        .into(),
        Vec::<u8>::new().into(),
    )
    .await;
    let document_id = create_linking_doc(
        &test,
        test.auth.clone(),
        test.group_id,
        "datasets/legacy-s3-reference",
        "Legacy S3 reference",
        true,
        &[("license", &format!("s3://{bucket}/{key}"))],
    )
    .await;
    drain_metadata_background(test.state.as_ref()).await;

    let response = preflight_route(
        &test,
        Some(test.auth.clone()),
        PreflightTargetBody::BucketPrefix {
            bucket: bucket.to_string(),
            prefix: Some("folder/".to_string()),
            operation: PreflightStorageBody::AllVersionsPurge,
        },
        MetadataQueryMode::Local,
        true,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(response.targets.len(), 1);
    let target = &response.targets[0];
    assert_eq!(target.content_w3id, preflight_w3id(hash));
    assert_eq!(target.targeted_versions.len(), 1);
    assert_eq!(target.visible_references[0].document_id, document_id);
    assert!(target.would_remove_location);
    assert!(response.coverage.path_style_complete);
}

async fn create_test_document(
    state: Arc<ServerState>,
    auth: AuthContext,
    group_id: Ulid,
    path: &str,
    name: &str,
    public: bool,
) {
    let _ = create_metadata_document(
        State(state),
        Extension(Some(auth)),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: group_id.to_string(),
            path: path.to_string(),
            name: name.to_string(),
            description: "Remote metadata access fixture".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public,
        })),
    )
    .await
    .unwrap();
}

async fn query_remote_names(
    test: &DistributedAccessState,
    auth: Option<AuthContext>,
    bearer_token: Option<ValidatedBearer>,
) -> QueryNamesResult {
    let ctx = test.coordinator.state.get_ctx();
    let result = run_query_metadata(
        ctx.as_ref(),
        test.coordinator.state.get_realm_id(),
        test.coordinator.state.get_node_id(),
        MetadataQueryRequest {
            auth,
            bearer_token: bearer_token_string(bearer_token),
            graph_iris: None,
            query: "SELECT DISTINCT ?name WHERE { ?s <http://schema.org/name> ?name }".to_string(),
            mode: Some(ApiQueryMode::Distributed),
            target_nodes: Some(vec![test.remote.net.node_id()]),
            allow_partial: true,
        },
    )
    .await
    .unwrap();
    let MetadataQueryResults::Solutions(rows) = result.results else {
        panic!("expected SELECT solutions");
    };
    QueryNamesResult {
        names: rows.into_iter().flat_map(|row| row.into_values()).collect(),
        nodes_queried: result.fanout_stats.nodes_queried,
        nodes_failed: result.fanout_stats.nodes_failed,
        failed_partitions: result.fanout_stats.failed_partitions,
    }
}

async fn search_remote_paths(
    test: &DistributedAccessState,
    auth: Option<AuthContext>,
    bearer_token: Option<ValidatedBearer>,
) -> SearchPathsResult {
    let ctx = test.coordinator.state.get_ctx();
    let result = run_search_metadata(
        ctx.as_ref(),
        test.coordinator.state.get_realm_id(),
        test.coordinator.state.get_node_id(),
        MetadataSearchRequest {
            auth,
            bearer_token: bearer_token_string(bearer_token),
            graph_iris: None,
            query: "Remote".to_string(),
            conforms_to: None,
            group_id: None,
            limit: Some(10),
            cursor: None,
            mode: Some(ApiQueryMode::Distributed),
            target_nodes: Some(vec![test.remote.net.node_id()]),
        },
    )
    .await
    .unwrap();
    SearchPathsResult {
        paths: result
            .hits
            .into_iter()
            .map(|hit| hit.document_path)
            .collect(),
        nodes_queried: result.fanout_stats.nodes_queried,
        nodes_failed: result.fanout_stats.nodes_failed,
    }
}

fn assert_contains_name(names: &[String], expected: &str) {
    assert!(
        names.iter().any(|name| name.contains(expected)),
        "expected {names:?} to contain {expected:?}"
    );
}

fn assert_excludes_name(names: &[String], unexpected: &str) {
    assert!(
        !names.iter().any(|name| name.contains(unexpected)),
        "expected {names:?} not to contain {unexpected:?}"
    );
}

fn assert_contains_path(paths: &[String], expected: &str) {
    assert!(
        paths.iter().any(|path| path == expected),
        "expected {paths:?} to contain {expected:?}"
    );
}

fn assert_excludes_path(paths: &[String], unexpected: &str) {
    assert!(
        !paths.iter().any(|path| path == unexpected),
        "expected {paths:?} not to contain {unexpected:?}"
    );
}

fn test_signing_key() -> SigningKey {
    generate_signing_key()
}

fn test_realm_id(seed: u8) -> RealmId {
    RealmId::from_bytes(
        SigningKey::from_bytes(&[seed; 32])
            .verifying_key()
            .to_bytes(),
    )
}

fn test_token_claims(realm_id: RealmId, user_id: aruna_core::UserId) -> TokenClaims {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    TokenClaims {
        sub: user_id.to_string(),
        iss: realm_id.to_string(),
        iat: now,
        exp: now + 600,
        jti: Ulid::generate().to_string(),
        sid: None,
        session_kind: None,
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
    }
}

fn sign_test_token(signing_key: &SigningKey, claims: &TokenClaims) -> String {
    let key_pem = signing_key.to_pkcs8_pem(LineEnding::LF).unwrap();
    encode(
        &Header::new(Algorithm::EdDSA),
        claims,
        &EncodingKey::from_ed_pem(key_pem.as_bytes()).unwrap(),
    )
    .unwrap()
}

async fn setup_state() -> TestState {
    let (storage_dir, storage_handle) = test_storage();
    let metadata_dir = tempfile::tempdir().unwrap();
    let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
    let realm_id = test_realm_id(3);
    let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
    let actor = Actor {
        node_id,
        user_id,
        realm_id,
    };
    let metadata_handle = MetadataHandle::new(
        metadata_dir.path(),
        node_id,
        storage_handle.clone(),
        None,
        None,
        None,
    )
    .unwrap();
    let task_handle = TaskHandle::new();
    let mut context = test_context(storage_handle);
    context.metadata_handle = Some(metadata_handle);
    context.task_handle = Some(task_handle);
    let driver_ctx = Arc::new(context);
    let group_id = Ulid::generate();
    seed_group_docs(
        &driver_ctx,
        realm_id,
        &actor,
        group_id,
        "metadata-group",
        user_id,
    )
    .await;
    seed_realm_auth(&driver_ctx, realm_id, &actor).await;

    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    config.ensure_node(node_id, RealmNodeKind::Server);
    config.seed_job_control(node_id, 0);
    write_doc(
        &driver_ctx,
        REALM_CONFIG_KEYSPACE,
        (*realm_id.as_bytes()).into(),
        config.to_bytes(&actor).unwrap().into(),
    )
    .await;

    let state = Arc::new(
        test_state(
            driver_ctx,
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
        )
        .await,
    );

    TestState {
        _storage_dir: storage_dir,
        _metadata_dir: metadata_dir,
        auth: AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        group_id,
        state,
    }
}

// Net-capable variant for tests that need node discovery or cursor signing.
async fn setup_network_state() -> TestState {
    let (storage_dir, storage_handle) = test_storage();
    let metadata_dir = tempfile::tempdir().unwrap();
    let realm_id = test_realm_id(3);
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            secret_key: Some(iroh::SecretKey::from_bytes(&[11u8; 32])),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage_handle.clone(),
    )
    .await
    .unwrap();
    let node_id = net.node_id();
    let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
    let actor = Actor {
        node_id,
        user_id,
        realm_id,
    };
    let metadata_handle = MetadataHandle::new(
        metadata_dir.path(),
        node_id,
        storage_handle.clone(),
        None,
        None,
        None,
    )
    .unwrap();
    let task_handle = TaskHandle::new();
    let mut context = test_context(storage_handle);
    context.net_handle = Some(net.clone());
    context.metadata_handle = Some(metadata_handle);
    context.task_handle = Some(task_handle);
    let driver_ctx = Arc::new(context);
    // Single-node realm config so the holder proxy serves mutations locally.
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    config.ensure_node(node_id, RealmNodeKind::Server);
    config.seed_job_control(node_id, 0);
    write_doc(
        &driver_ctx,
        REALM_CONFIG_KEYSPACE,
        (*realm_id.as_bytes()).into(),
        config
            .to_bytes(&Actor {
                node_id,
                user_id: aruna_core::UserId::nil(realm_id),
                realm_id,
            })
            .unwrap()
            .into(),
    )
    .await;
    drive(
        AnnouncePresenceOperation::new(AnnouncePresenceConfig {
            realm_id,
            node_id,
            schedule_refresh: false,
        }),
        driver_ctx.as_ref(),
    )
    .await
    .unwrap();
    let group_id = Ulid::generate();
    seed_group_docs(
        &driver_ctx,
        realm_id,
        &actor,
        group_id,
        "metadata-group",
        user_id,
    )
    .await;
    seed_realm_auth(&driver_ctx, realm_id, &actor).await;

    let state = Arc::new(
        test_state(
            driver_ctx,
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
        )
        .await,
    );

    TestState {
        _storage_dir: storage_dir,
        _metadata_dir: metadata_dir,
        auth: AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        group_id,
        state,
    }
}

async fn drain_metadata_background(state: &ServerState) {
    let ctx = state.get_ctx();
    let drained = drain_projection_queue(ctx.as_ref()).await.unwrap();
    if drained.markers_examined == 0 {
        replay_event_log(ctx.as_ref()).await.unwrap();
    }
    process_materialization_batch(ctx.as_ref()).await.unwrap();
}

async fn setup_closed_storage() -> Arc<ServerState> {
    let (storage_handle, receivers) = storage::StorageHandle::new();
    drop(receivers);

    let realm_id = test_realm_id(3);
    let node_id = iroh::SecretKey::from_bytes(&[14u8; 32]).public();
    Arc::new(
        ServerState::new(
            Arc::new(DriverContext {
                storage_handle,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            }),
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
            false,
            None,
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await,
    )
}

async fn write_doc(
    driver_ctx: &Arc<DriverContext>,
    key_space: &str,
    key: byteview::ByteView,
    value: byteview::ByteView,
) {
    let event = driver_ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value,
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn read_task_timer(ctx: &DriverContext, key: &TaskKey) -> Option<PersistedTaskTimer> {
    let event = ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key: postcard::to_allocvec(key).unwrap().into(),
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            value.map(|value| postcard::from_bytes(&value).expect("timer decodes"))
        }
        other => panic!("unexpected task timer read event: {other:?}"),
    }
}

/// Route-family contract tests for D016/D017: every protected metadata route
/// runs in-process for the unauthenticated, foreign-realm, stranger, allowed,
/// and public cases, and a refusal leaves evidence that no write happened.
mod authorization {
    use super::*;

    fn foreign_auth() -> AuthContext {
        let realm_id = RealmId::from_bytes(
            ed25519_dalek::SigningKey::from_bytes(&[99u8; 32])
                .verifying_key()
                .to_bytes(),
        );
        AuthContext {
            user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    fn stranger_auth(state: &ServerState) -> AuthContext {
        let realm_id = state.get_realm_id();
        AuthContext {
            user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    fn scaffold(group_id: Ulid, path: &str, public: bool) -> CreateMetadataRequest {
        CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: group_id.to_string(),
            path: path.to_string(),
            name: "Authorization fixture".to_string(),
            description: "a refused write must leave no trace".to_string(),
            date_published: "2026-01-01".to_string(),
            license: None,
            public,
        })
    }

    async fn create(
        test: &TestState,
        auth: Option<AuthContext>,
        path: &str,
        public: bool,
    ) -> ServerResult<(StatusCode, Json<CreateMetadataResponse>)> {
        create_metadata_document(
            State(test.state.clone()),
            Extension(auth),
            Extension(None),
            Json(scaffold(test.group_id, path, public)),
        )
        .await
    }

    async fn document_count(test: &TestState) -> usize {
        drain_metadata_background(test.state.as_ref()).await;
        run_document_list(
            &test.state,
            Some(test.auth.clone()),
            ListMetadataQuery::default(),
            None,
        )
        .await
        .unwrap()
        .documents
        .len()
    }

    #[tokio::test]
    async fn unauthorized_create_denied() {
        let test = setup_network_state().await;
        let denied = create(&test, None, "datasets/anonymous", false).await;
        assert!(matches!(denied, Err(ServerError::Unauthorized)));
        assert_eq!(document_count(&test).await, 0);
    }

    #[tokio::test]
    async fn foreign_create_denied() {
        let test = setup_network_state().await;
        let denied = create(&test, Some(foreign_auth()), "datasets/foreign", false).await;
        assert!(matches!(denied, Err(ServerError::Forbidden)));
        assert_eq!(document_count(&test).await, 0);
    }

    #[tokio::test]
    async fn stranger_create_denied() {
        let test = setup_network_state().await;
        let denied = create(
            &test,
            Some(stranger_auth(&test.state)),
            "datasets/stranger",
            false,
        )
        .await;
        assert!(matches!(denied, Err(ServerError::Forbidden)));
        assert_eq!(document_count(&test).await, 0);
    }

    #[tokio::test]
    async fn member_create_allowed() {
        let test = setup_network_state().await;
        let (status, Json(created)) =
            create(&test, Some(test.auth.clone()), "datasets/allowed", false)
                .await
                .unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(created.summary.group_id, test.group_id.to_string());
        assert_eq!(document_count(&test).await, 1);
    }

    #[tokio::test]
    async fn stranger_delete_denied() {
        let test = setup_network_state().await;
        let (_, Json(created)) = create(&test, Some(test.auth.clone()), "datasets/guarded", false)
            .await
            .unwrap();
        drain_metadata_background(test.state.as_ref()).await;
        let document_id = created.summary.document_id.clone();

        let denied = delete_metadata_document(
            State(test.state.clone()),
            Extension(Some(stranger_auth(&test.state))),
            Extension(None),
            Path(document_id.clone()),
        )
        .await;
        assert!(matches!(denied, Err(ServerError::Forbidden)));

        let still_readable = get_metadata_document(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Extension(None),
            Path(document_id),
        )
        .await
        .unwrap();
        assert_eq!(still_readable.1.0.document_id, created.summary.document_id);
    }

    #[tokio::test]
    async fn read_visibility() {
        let test = setup_network_state().await;
        let (_, Json(public)) = create(&test, Some(test.auth.clone()), "datasets/public", true)
            .await
            .unwrap();
        let (_, Json(private)) = create(&test, Some(test.auth.clone()), "datasets/private", false)
            .await
            .unwrap();
        drain_metadata_background(test.state.as_ref()).await;

        let anonymous_public = get_metadata_document(
            State(test.state.clone()),
            Extension(None),
            Extension(None),
            Path(public.summary.document_id),
        )
        .await;
        assert!(
            anonymous_public.is_ok(),
            "an intentionally public document is readable anonymously"
        );

        let anonymous_private = get_metadata_document(
            State(test.state.clone()),
            Extension(None),
            Extension(None),
            Path(private.summary.document_id),
        )
        .await;
        assert!(
            matches!(anonymous_private, Err(ServerError::NotFound)),
            "a private document stays hidden from anonymous readers"
        );
    }
}

#[tokio::test]
async fn pid_lookup_hides_private() {
    use crate::routes::pid::{
        LookupQuery, SecondaryKindView, list_persistent_ids, lookup_identifier,
    };
    use aruna_core::structs::secondary_id::{SecondaryIdKind, SecondaryIdentifier};
    let test = setup_network_state().await;
    let (_, Json(created)) = create_metadata_document(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: test.group_id.to_string(),
            path: "datasets/imported".to_string(),
            name: "Imported".to_string(),
            description: "Imported from a repository".to_string(),
            date_published: "2026-01-01".to_string(),
            license: None,
            public: false,
        })),
    )
    .await
    .unwrap();
    drain_metadata_background(test.state.as_ref()).await;
    let document_id = Ulid::from_string(&created.summary.document_id).unwrap();
    let identifiers = vec![
        SecondaryIdentifier::new(SecondaryIdKind::Doi, "10.5281/Zenodo.42", None).unwrap(),
        SecondaryIdentifier::new(
            SecondaryIdKind::InvenioParent,
            "abcde-12345",
            Some("https://zenodo.org/api/"),
        )
        .unwrap(),
    ];
    let (_, changed) = aruna_operations::metadata::persistent_id::forward::add_identifiers_routed(
        &test.state.get_ctx(),
        test.state.get_realm_id(),
        document_id,
        identifiers,
        aruna_core::time::unix_timestamp_millis(),
        None,
    )
    .await
    .unwrap();
    assert!(changed);

    let lookup = |auth: Option<AuthContext>, value: &str| {
        lookup_identifier(
            State(test.state.clone()),
            Extension(auth),
            Query(LookupQuery {
                kind: SecondaryKindView::Doi,
                value: value.to_string(),
                endpoint: None,
            }),
        )
    };
    let Json(found) = lookup(Some(test.auth.clone()), "https://doi.org/10.5281/zenodo.42")
        .await
        .unwrap();
    assert_eq!(found.document_id, document_id.to_string());
    let stranger = AuthContext {
        user_id: aruna_core::UserId::local(Ulid::generate(), test.auth.realm_id),
        ..test.auth.clone()
    };
    for auth in [None, Some(stranger)] {
        let hidden = lookup(auth, "doi:10.5281/zenodo.42").await;
        assert!(matches!(hidden, Err(ServerError::NotFound)));
    }
    let missing = lookup(Some(test.auth.clone()), "10.5281/zenodo.43").await;
    assert!(matches!(missing, Err(ServerError::NotFound)));

    let Json(listed) = list_persistent_ids(
        State(test.state.clone()),
        Extension(Some(test.auth.clone())),
        Path(document_id.to_string()),
    )
    .await
    .unwrap();
    let listed = serde_json::to_value(listed).unwrap();
    assert_eq!(
        listed[0]["secondary_identifiers"],
        serde_json::json!([
            {"kind": "doi", "value": "10.5281/zenodo.42"},
            {"kind": "invenio_parent", "value": "abcde-12345", "endpoint": "https://zenodo.org/api"}
        ])
    );
}
