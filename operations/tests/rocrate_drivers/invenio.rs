//! Exercises native repository transfers against an isolated Invenio HTTP fixture.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::invenio::InvenioDestination;
use axum::body::to_bytes;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Mutex;

#[path = "invenio_live.rs"]
mod live;

#[derive(Default)]
struct Repository {
    calls: Vec<(Method, String)>,
    files: std::collections::BTreeMap<String, Option<Vec<u8>>>,
    committed: HashSet<String>,
    metadata: Option<Value>,
    custom_fields: Value,
    revision: u64,
    lost_metadata: bool,
    author_login: bool,
    published: bool,
    corrupt: bool,
    loop_pages: bool,
    lost_create: bool,
    lost_source: bool,
    lost_commit: bool,
    lost_publish: bool,
    lost_content: bool,
    foreign_page: bool,
    public_files: bool,
    conflict: bool,
    partial_metadata: bool,
    file_name: Option<String>,
    head_started: Option<Arc<tokio::sync::Notify>>,
    head_release: Option<Arc<tokio::sync::Notify>>,
}

struct Server {
    endpoint: String,
    state: Arc<Mutex<Repository>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(state: Repository) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api/", listener.local_addr().unwrap());
    let state = Arc::new(Mutex::new(state));
    let app = Router::new()
        .fallback(mock_request)
        .with_state(state.clone());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Server {
        endpoint,
        state,
        task,
    }
}

fn record(id: &str, published: bool) -> Value {
    json!({
        "id": id, "parent": {"id": "parent", "pids": {"doi": {"identifier": "10.1234/all"}}},
        "revision_id": 1, "files": {"enabled": true}, "is_published": published, "versions": {"index": id.parse::<u64>().unwrap()},
        "pids": {"doi": {"identifier": format!("10.1234/{id}")}},
        "created": "2024-01-01T00:00:00Z", "updated": "2025-01-01T00:00:00Z",
        "custom_fields": {"local:extra": {"preserve": [1, 2, 3]}},
        "metadata": {
            "title": format!("Record {id}"), "publication_date": "2024-01-01", "version": id,
            "resource_type": {"id": "dataset"}, "description": "Repository history",
            "creators": [{"person_or_org": {"type": "personal", "name": "Researcher, A", "given_name": "A",
                "family_name": "Researcher", "identifiers": [{"scheme": "orcid", "identifier": "0000-0002-1825-0097"}]}}],
            "related_identifiers": [{"scheme": "doi", "identifier": "10.1234/related", "relation_type": {"id": "issupplementto"}}]
        }
    })
}

fn file(key: &str, bytes: &[u8], committed: bool) -> Value {
    let md5 = hex::encode(
        aruna_blob::hash::Hasher::new_with_bytes(bytes)
            .finalize()
            .md5,
    );
    json!({"key": key, "size": bytes.len(), "checksum": format!("md5:{md5}"),
        "status": if committed {"completed"} else {"pending"}, "mimetype": "application/octet-stream", "file_id": key})
}

async fn mock_request(State(state): State<Arc<Mutex<Repository>>>, request: Request) -> Response {
    assert_eq!(
        request.headers().get("authorization").unwrap(),
        if state.lock().unwrap().author_login {
            "Bearer author-token"
        } else {
            "Bearer repository-token"
        }
    );
    if (request.method() == Method::GET || request.method() == Method::HEAD)
        && request.uri().path().ends_with("/content")
    {
        assert!(
            request
                .headers()
                .get("accept")
                .is_none_or(|value| value == "*/*")
        );
    } else {
        assert_eq!(
            request.headers().get("accept").unwrap(),
            "application/vnd.inveniordm.v1+json, application/json;q=0.9"
        );
    }
    if request.method() == Method::PUT && request.uri().path().ends_with("/draft") {
        assert_eq!(
            request.headers().get("if-match").unwrap().to_str().unwrap(),
            state.lock().unwrap().revision.max(1).to_string()
        );
    }
    if request.method() == Method::HEAD {
        let wait = {
            let state = state.lock().unwrap();
            state.head_started.clone().zip(state.head_release.clone())
        };
        if let Some((started, release)) = wait {
            started.notify_one();
            release.notified().await;
        }
    }
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or("").to_string();
    let body = to_bytes(request.into_body(), 1024 * 1024).await.unwrap();
    let mut state = state.lock().unwrap();
    state.calls.push((method.clone(), path.clone()));
    let parts = path.trim_start_matches('/').split('/').collect::<Vec<_>>();
    let value = match (method, parts.as_slice()) {
        (Method::GET, ["api", "records"]) => {
            assert!(query.contains("size=25"));
            assert!(query.contains("q=doi%3A"));
            json!({"hits": {"total": 1, "hits": [record("2", true)]}, "links": {"next": null}})
        }
        (Method::GET, ["api", "records", "parent"]) => record("2", true),
        (Method::POST, ["api", "records", "2", "versions"]) => draft_record(&state, false),
        (Method::GET, ["api", "records", "2", "versions"]) => {
            if query.contains("size=100") {
                return StatusCode::BAD_REQUEST.into_response();
            }
            let second = query.contains("page=2");
            if state.foreign_page {
                return axum::Json(json!({"hits": {"total": 2, "hits": [{"id": "1"}]},
                    "links": {"next": "http://127.0.0.1:1/api/records/2/versions"}}))
                .into_response();
            }
            json!({"hits": {"total": 2, "hits": [{"id": if second {"2"} else {"1"}}]},
                "links": {"next": if !second || state.loop_pages {Some("/api/records/2/versions?page=2")} else {None}}})
        }
        (Method::GET, ["api", "records", id @ ("1" | "2")]) => {
            if std::mem::take(&mut state.lost_source) {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            record(id, true)
        }
        (Method::GET, ["api", "records", id @ ("1" | "2"), "files"]) => {
            let mut entry = file(
                state.file_name.as_deref().unwrap_or("data.txt"),
                id.as_bytes(),
                true,
            );
            if state.corrupt {
                entry["checksum"] = json!("md5:00000000000000000000000000000000");
            }
            json!({"entries": [entry]})
        }
        (
            Method::GET | Method::HEAD,
            ["api", "records", id @ ("1" | "2"), "files", key, "content"],
        ) => {
            assert_eq!(*key, state.file_name.as_deref().unwrap_or("data.txt"));
            return (
                [
                    ("content-type", "application/octet-stream"),
                    ("content-length", "1"),
                ],
                id.to_string(),
            )
                .into_response();
        }
        (Method::POST, ["api", "records"]) => {
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["metadata"]["title"], "Exported dataset");
            assert_eq!(body["files"]["enabled"], true);
            assert_eq!(
                body["metadata"]["creators"][0]["person_or_org"]["family_name"],
                "Researcher"
            );
            assert_eq!(
                body["metadata"]["creators"][0]["person_or_org"]["identifiers"][0]["identifier"],
                "0000-0002-1825-0097"
            );
            state.metadata = Some(body["metadata"].clone());
            state.custom_fields = body["custom_fields"].clone();
            assert_eq!(
                body["access"]["files"],
                if state.public_files {
                    "public"
                } else {
                    "restricted"
                }
            );
            if state.lost_create {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            draft_record(&state, false)
        }
        (Method::GET, ["api", "records", "3", "draft"]) => {
            if state.published {
                return StatusCode::NOT_FOUND.into_response();
            }
            draft_record(&state, false)
        }
        (Method::PUT, ["api", "records", "3", "draft"]) => {
            if state.conflict {
                return StatusCode::PRECONDITION_FAILED.into_response();
            }
            let body: Value = serde_json::from_slice(&body).unwrap();
            state.metadata = Some(body["metadata"].clone());
            state.custom_fields = body["custom_fields"].clone();
            if state.partial_metadata {
                state.metadata.as_mut().unwrap()["title"] = Value::Null;
            }
            state.revision = state.revision.max(1) + 1;
            if state.lost_metadata {
                state.lost_metadata = false;
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            draft_record(&state, false)
        }
        (Method::GET, ["api", "records", "3"]) if state.published => draft_record(&state, true),
        (Method::GET, ["api", "records", "3", "draft", "files"])
        | (Method::GET, ["api", "records", "3", "files"]) => {
            json!({"entries": state.files.iter().map(|(key, bytes)| match bytes {
                Some(bytes) => file(key, bytes, state.committed.contains(key)),
                None => json!({"key": key, "status": "pending"}),
            }).collect::<Vec<_>>()})
        }
        (Method::POST, ["api", "records", "3", "draft", "files"]) => {
            let body: Value = serde_json::from_slice(&body).unwrap();
            let key = body[0]["key"].as_str().unwrap();
            assert!(state.files.insert(key.into(), None).is_none());
            json!({"entries": [{"key": key, "status": "pending"}]})
        }
        (Method::PUT, ["api", "records", "3", "draft", "files", key, "content"]) => {
            let key = percent_encoding::percent_decode_str(key)
                .decode_utf8()
                .unwrap()
                .into_owned();
            let entry = state.files.get_mut(&key).unwrap();
            assert!(entry.is_none());
            *entry = Some(body.to_vec());
            if std::mem::take(&mut state.lost_content) {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            json!({})
        }
        (Method::POST, ["api", "records", "3", "draft", "files", key, "commit"]) => {
            let key = percent_encoding::percent_decode_str(key)
                .decode_utf8()
                .unwrap()
                .into_owned();
            assert!(state.files[&key].is_some());
            state.committed.insert(key.clone());
            if std::mem::take(&mut state.lost_commit) {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            file(&key, state.files[&key].as_ref().unwrap(), true)
        }
        (Method::GET, ["api", "records", "3", "files", key, "content"]) if state.published => {
            let key = percent_encoding::percent_decode_str(key)
                .decode_utf8()
                .unwrap();
            return state.files[key.as_ref()].clone().unwrap().into_response();
        }
        (Method::POST, ["api", "records", "3", "draft", "actions", "publish"]) => {
            assert_eq!(state.files.len(), state.committed.len());
            assert_eq!(state.files.len(), 3);
            assert!(!state.published);
            state.published = true;
            if std::mem::take(&mut state.lost_publish) {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            draft_record(&state, true)
        }
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    axum::Json(value).into_response()
}

fn draft_record(state: &Repository, published: bool) -> Value {
    let mut record = record("3", published);
    if let Some(metadata) = &state.metadata {
        record["metadata"] = metadata.clone();
    }
    if let Some(creators) = record["metadata"]["creators"].as_array_mut() {
        for creator in creators {
            if creator["role"]["id"].is_string() {
                creator["role"]["title"] = json!({"en": "Researcher"});
            }
            let person = &mut creator["person_or_org"];
            if let Some(family) = person["family_name"].as_str() {
                person["name"] = json!(format!(
                    "{}, {}",
                    family,
                    person["given_name"].as_str().unwrap_or_default()
                ));
            }
        }
    }
    record["metadata"]["resource_type"]["title"] = json!({"en": "Dataset"});
    record["revision_id"] = json!(state.revision.max(1));
    record["custom_fields"] = if state.custom_fields.is_null() {
        json!({})
    } else {
        state.custom_fields.clone()
    };
    record["parent"]["access"] = json!({"owned_by": {"user": 42}});
    record
}

async fn connector(fixture: &Fixture, server: &Server) -> Ulid {
    drive(
        SourceConnectorOperation::new(SourceConnectorInput {
            group_id: fixture.group_id,
            created_by: fixture.actor.user_id,
            name: "repository".into(),
            kind: SourceConnectorKind::Http,
            public_config: HashMap::from([("endpoint".into(), server.endpoint.clone())]),
            secret_config: HashMap::from([("token".into(), "repository-token".into())]),
        }),
        &fixture.context,
    )
    .await
    .unwrap()
    .connector
    .connector_id
}

#[tokio::test]
async fn invenio_history_imports() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = serve(Repository::default()).await;
    let connector_id = connector(&fixture, &server).await;
    let spec = spec_with_source(
        &fixture,
        ImportRoCrateSource::Invenio {
            group_id: fixture.group_id,
            connector_id,
            record_id: "2".into(),
            options: Default::default(),
        },
        doc_id(1),
    );
    let ctx = claim_context(&fixture, job_id(), JobPayload::ImportRoCrate(spec.clone())).await?;
    let result = run_rocrate_import(&ctx, &spec).await;
    match result {
        JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(result)) => {
            assert_eq!(result.imported, 4);
            assert_eq!(result.failed, 0);
            assert_eq!(result.document_id, Some(doc_id(1)));
        }
        JobRunOutcome::Failed(error) => panic!("{}", error.message),
        _ => panic!("unexpected import outcome"),
    }
    for id in ["1", "2"] {
        let key = format!(
            "imported/{}",
            aruna_core::invenio::file_path(id, "data.txt")?
        );
        assert_eq!(object_versions(&fixture, &key).await?.len(), 1);
        let provenance = format!("imported/versions/{id}/invenio-record.json");
        assert_eq!(object_versions(&fixture, &provenance).await?.len(), 1);
        use aruna_operations::s3::object::get::{GetObjectInput, GetObjectOperation};
        let mut object = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: BUCKET.into(),
                key: provenance,
                version_id: None,
                range: None,
                group_id: fixture.group_id,
                user_identity: fixture.actor.user_id,
                node_id: fixture.actor.node_id,
            }),
            &fixture.context,
        )
        .await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = object.blob.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        let saved: Value = serde_json::from_slice(&bytes)?;
        assert_eq!(saved["record"], record(id, true));
        assert_eq!(
            saved["files"]["entries"][0],
            file("data.txt", id.as_bytes(), true)
        );
    }
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn invenio_import_modes() -> Result<(), Box<dyn std::error::Error>> {
    use aruna_core::invenio::{InvenioMode, InvenioOptions};
    use aruna_operations::s3::object::get::{GetObjectInput, GetObjectOperation};
    for mode in [InvenioMode::Metadata, InvenioMode::Reference] {
        let fixture = build_fixture(false).await?;
        let server = serve(Repository {
            file_name: Some("content".into()),
            ..Default::default()
        })
        .await;
        let connector_id = connector(&fixture, &server).await;
        let spec = spec_with_source(
            &fixture,
            ImportRoCrateSource::Invenio {
                group_id: fixture.group_id,
                connector_id,
                record_id: "parent".into(),
                options: InvenioOptions {
                    mode,
                    all_versions: false,
                },
            },
            doc_id(1),
        );
        let ctx =
            claim_context(&fixture, job_id(), JobPayload::ImportRoCrate(spec.clone())).await?;
        match run_rocrate_import(&ctx, &spec).await {
            JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(result)) => {
                assert_eq!(
                    result.imported,
                    if mode == InvenioMode::Reference { 2 } else { 1 }
                );
            }
            JobRunOutcome::Failed(error) => panic!("{}", error.message),
            _ => panic!("unexpected import result"),
        }
        assert!(
            server
                .state
                .lock()
                .unwrap()
                .calls
                .iter()
                .all(|(method, path)| !path.ends_with("/versions")
                    && !(*method == Method::GET && path.ends_with("/content")))
        );
        let key = format!(
            "imported/{}",
            aruna_core::invenio::file_path("2", "content")?
        );
        if mode == InvenioMode::Reference {
            assert_eq!(object_versions(&fixture, &key).await?.len(), 1);
            let mut object = drive(
                GetObjectOperation::new(GetObjectInput {
                    bucket: BUCKET.into(),
                    key: key.clone(),
                    version_id: None,
                    range: None,
                    group_id: fixture.group_id,
                    user_identity: fixture.actor.user_id,
                    node_id: fixture.actor.node_id,
                }),
                &fixture.context,
            )
            .await?;
            let mut bytes = Vec::new();
            while let Some(chunk) = object.blob.next().await {
                bytes.extend_from_slice(&chunk?);
            }
            assert_eq!(bytes, b"2");
            assert!(matches!(
                run_rocrate_import(&ctx, &spec).await,
                JobRunOutcome::Succeeded(_)
            ));
            assert_eq!(object_versions(&fixture, &key).await?.len(), 1);
        } else {
            assert!(object_versions(&fixture, &key).await?.is_empty());
            assert!(
                server
                    .state
                    .lock()
                    .unwrap()
                    .calls
                    .iter()
                    .all(|(_, path)| !path.ends_with("/files"))
            );
        }
        fixture.stop().await;
    }
    Ok(())
}

#[tokio::test]
async fn invenio_searches_records() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = serve(Repository::default()).await;
    let connector_id = connector(&fixture, &server).await;
    let auth = AuthContext {
        user_id: fixture.actor.user_id,
        realm_id: fixture.actor.realm_id,
        path_restrictions: None,
        session: None,
    };
    let query = aruna_core::invenio::InvenioQuery {
        group_id: fixture.group_id,
        connector_id,
        q: "doi:\"10.1234/2\"".into(),
        page: 1,
        size: 25,
        all_versions: false,
    };
    let page = aruna_operations::jobs::invenio::search_records(
        &fixture.context,
        &auth,
        &query,
        1024 * 1024,
    )
    .await?;
    assert_eq!(page["hits"]["hits"][0]["id"], "2");
    let invalid = aruna_core::invenio::InvenioQuery { size: 100, ..query };
    assert!(
        aruna_operations::jobs::invenio::search_records(&fixture.context, &auth, &invalid, 1024)
            .await
            .is_err()
    );
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn invenio_rejects_corruption() -> Result<(), Box<dyn std::error::Error>> {
    for repository in [
        Repository {
            corrupt: true,
            ..Default::default()
        },
        Repository {
            loop_pages: true,
            ..Default::default()
        },
        Repository {
            foreign_page: true,
            ..Default::default()
        },
    ] {
        let fixture = build_fixture(false).await?;
        let expected = if repository.corrupt {
            "checksum"
        } else if repository.loop_pages {
            "pagination"
        } else {
            "cross-origin"
        };
        let server = serve(repository).await;
        let connector_id = connector(&fixture, &server).await;
        let spec = spec_with_source(
            &fixture,
            ImportRoCrateSource::Invenio {
                group_id: fixture.group_id,
                connector_id,
                record_id: "2".into(),
                options: Default::default(),
            },
            doc_id(1),
        );
        let ctx =
            claim_context(&fixture, job_id(), JobPayload::ImportRoCrate(spec.clone())).await?;
        match run_rocrate_import(&ctx, &spec).await {
            JobRunOutcome::Failed(error) => {
                assert!(error.message.contains(expected), "{}", error.message)
            }
            _ => panic!("invalid repository input was accepted"),
        }
        assert_eq!(hidden_count(&fixture, ctx.job_id.as_ulid()).await?, 0);
        fixture.stop().await;
    }
    Ok(())
}

async fn export_spec(
    fixture: &Fixture,
    server: &Server,
    publish: bool,
) -> Result<ExportRoCrateSpec, Box<dyn std::error::Error>> {
    server.state.lock().unwrap().author_login = true;
    let upload = create_upload(fixture, native_archive().await?).await?;
    let import = import_spec(fixture, upload, doc_id(1));
    let ctx = claim_context(fixture, job_id(), JobPayload::ImportRoCrate(import.clone())).await?;
    assert!(matches!(
        run_rocrate_import(&ctx, &import).await,
        JobRunOutcome::Succeeded(_)
    ));
    replay_event_log(fixture.context.as_ref()).await?;
    process_materialization_batch(fixture.context.as_ref()).await?;
    let mut destination = InvenioDestination {
        group_id: fixture.group_id,
        connector_id: connector(fixture, server).await,
        draft_id: None,
        new_version: None,
        metadata_json: "{}".into(),
        publish,
        public_files: false,
        credential: None,
    };
    destination.credential = Some(
        aruna_operations::jobs::invenio::seal_credential(
            &fixture.context,
            &import.auth_context,
            &destination,
            "author-token",
        )
        .await?,
    );
    Ok(ExportRoCrateSpec {
        auth_context: import.auth_context,
        document_id: doc_id(1),
        limits: RoCrateLimits::default(),
        destination: Some(destination),
    })
}

async fn native_archive() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let document = json!({"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": [
        {"@id": "ro-crate-metadata.json", "@type": "CreativeWork", "about": {"@id": "./"}, "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"}},
        {"@id": "./", "@type": "Dataset", "name": "Exported dataset", "description": "Native files", "datePublished": "2026-09-22",
            "creator": {"@id": "#author"}, "hasPart": [{"@id": "nested/data.txt"}, {"@id": "empty.txt"}],
            "identifier": "https://doi.org/10.1234/source"},
        {"@id": "#author", "@type": "Person", "name": "A Researcher", "familyName": "Researcher", "givenName": "A",
            "identifier": {"@type": "PropertyValue", "propertyID": "orcid", "value": "0000-0002-1825-0097"}},
        {"@id": "nested/data.txt", "@type": "File"}, {"@id": "empty.txt", "@type": "File"}
    ]});
    let mut archive = async_zip::base::write::ZipFileWriter::new(Vec::new());
    archive
        .write_entry_whole(
            ZipEntryBuilder::new("ro-crate-metadata.json".into(), Compression::Stored),
            document.to_string().as_bytes(),
        )
        .await?;
    archive
        .write_entry_whole(
            ZipEntryBuilder::new("nested/data.txt".into(), Compression::Stored),
            PAYLOAD,
        )
        .await?;
    archive
        .write_entry_whole(
            ZipEntryBuilder::new("empty.txt".into(), Compression::Stored),
            &[],
        )
        .await?;
    Ok(archive.close().await?)
}

#[tokio::test]
async fn invenio_export_modes() -> Result<(), Box<dyn std::error::Error>> {
    for (publish, public_files) in [(false, false), (true, false), (true, true)] {
        let fixture = build_fixture(false).await?;
        let server = serve(Repository {
            public_files,
            ..Default::default()
        })
        .await;
        let mut spec = export_spec(&fixture, &server, publish).await?;
        spec.destination.as_mut().unwrap().public_files = public_files;
        let ctx =
            claim_context(&fixture, job_id(), JobPayload::ExportRoCrate(spec.clone())).await?;
        for _ in 0..2 {
            match run_export_job(&ctx, &spec).await {
                JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(result)) => {
                    let record = result.repository.unwrap();
                    assert_eq!(record.published, publish);
                    assert_eq!(record.id, "3");
                }
                JobRunOutcome::Failed(error) => panic!("{}", error.message),
                _ => panic!("unexpected export outcome"),
            }
        }
        {
            let state = server.state.lock().unwrap();
            assert_eq!(state.published, publish);
            assert_eq!(
                state
                    .calls
                    .iter()
                    .filter(|(method, path)| *method == Method::POST && path == "/api/records")
                    .count(),
                1
            );
            assert_eq!(
                state
                    .calls
                    .iter()
                    .filter(|(method, _)| *method == Method::PUT)
                    .count(),
                3
            );
            assert_eq!(
                state.files.keys().map(String::as_str).collect::<Vec<_>>(),
                ["empty.txt", "nested/data.txt", "ro-crate-metadata.json"]
            );
            assert_eq!(state.files["nested/data.txt"].as_deref(), Some(PAYLOAD));
            assert_eq!(state.files["empty.txt"].as_deref(), Some(&b""[..]));
            let metadata: Value =
                serde_json::from_slice(state.files["ro-crate-metadata.json"].as_ref().unwrap())?;
            assert!(metadata["@graph"].is_array());
            assert_eq!(
                state.metadata.as_ref().unwrap()["related_identifiers"][0]["identifier"],
                "10.1234/source"
            );
        }
        if publish {
            let body = reqwest::Client::new()
                .get(format!(
                    "{}records/3/files/nested%2Fdata.txt/content",
                    server.endpoint
                ))
                .bearer_auth("author-token")
                .header("Accept", "*/*")
                .send()
                .await?
                .error_for_status()?
                .bytes()
                .await?;
            assert_eq!(body.as_ref(), PAYLOAD);
            let record: Value = reqwest::Client::new()
                .get(format!("{}records/3", server.endpoint))
                .bearer_auth("author-token")
                .header(
                    "Accept",
                    "application/vnd.inveniordm.v1+json, application/json;q=0.9",
                )
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            assert_eq!(record["parent"]["access"]["owned_by"]["user"], 42);
            assert_eq!(record["metadata"]["title"], "Exported dataset");
        }
        fixture.stop().await;
    }
    Ok(())
}

#[tokio::test]
async fn invenio_export_recovers() -> Result<(), Box<dyn std::error::Error>> {
    for failure in 0..3 {
        let fixture = build_fixture(false).await?;
        let server = serve(Repository {
            lost_commit: failure == 0,
            lost_publish: failure == 1,
            lost_content: failure == 2,
            ..Default::default()
        })
        .await;
        let spec = export_spec(&fixture, &server, true).await?;
        let ctx =
            claim_context(&fixture, job_id(), JobPayload::ExportRoCrate(spec.clone())).await?;
        match run_export_job(&ctx, &spec).await {
            JobRunOutcome::Failed(error) => assert_eq!(
                error.kind,
                aruna_core::structs::execution::job::JobErrorKind::Retryable
            ),
            _ => panic!("lost repository response did not request a retry"),
        }
        match run_export_job(&ctx, &spec).await {
            JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(result)) => {
                assert!(result.repository.unwrap().published);
            }
            JobRunOutcome::Failed(error) => panic!("{}", error.message),
            _ => panic!("unexpected recovery outcome"),
        }
        {
            let state = server.state.lock().unwrap();
            assert_eq!(
                state
                    .calls
                    .iter()
                    .filter(|(method, _)| *method == Method::PUT)
                    .count(),
                3
            );
            assert_eq!(
                state
                    .calls
                    .iter()
                    .filter(|(method, path)| *method == Method::POST && path.ends_with("/publish"))
                    .count(),
                1
            );
        }
        fixture.stop().await;
    }
    Ok(())
}

#[tokio::test]
async fn invenio_continues_versions() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = serve(Repository::default()).await;
    let mut spec = export_spec(&fixture, &server, true).await?;
    spec.destination.as_mut().unwrap().new_version = Some("2".into());
    let ctx = claim_context(&fixture, job_id(), JobPayload::ExportRoCrate(spec.clone())).await?;
    match run_export_job(&ctx, &spec).await {
        JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(result)) => {
            let record = result.repository.unwrap();
            assert_eq!(record.id, "3");
            assert_eq!(record.parent_id, "parent");
            assert_eq!(record.doi.as_deref(), Some("10.1234/3"));
            assert!(record.published);
        }
        JobRunOutcome::Failed(error) => panic!("{}", error.message),
        _ => panic!("unexpected version outcome"),
    }
    {
        let state = server.state.lock().unwrap();
        assert!(
            state
                .calls
                .contains(&(Method::POST, "/api/records/2/versions".into()))
        );
        assert!(!state.calls.contains(&(Method::POST, "/api/records".into())));
        assert!(
            state
                .calls
                .contains(&(Method::PUT, "/api/records/3/draft".into()))
        );
    }
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn invenio_retries_versions() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = serve(Repository {
        lost_source: true,
        ..Default::default()
    })
    .await;
    let mut spec = export_spec(&fixture, &server, true).await?;
    spec.destination.as_mut().unwrap().new_version = Some("2".into());
    let ctx = claim_context(&fixture, job_id(), JobPayload::ExportRoCrate(spec.clone())).await?;
    match run_export_job(&ctx, &spec).await {
        JobRunOutcome::Failed(error) => assert_eq!(
            error.kind,
            aruna_core::structs::execution::job::JobErrorKind::Retryable
        ),
        _ => panic!("unavailable source record did not request a retry"),
    }
    match run_export_job(&ctx, &spec).await {
        JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(result)) => {
            assert!(result.repository.unwrap().published);
        }
        JobRunOutcome::Failed(error) => panic!("{}", error.message),
        _ => panic!("unexpected version retry outcome"),
    }
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn invenio_rejects_updates() -> Result<(), Box<dyn std::error::Error>> {
    for conflict in [true, false] {
        let fixture = build_fixture(false).await?;
        let server = serve(Repository {
            conflict,
            partial_metadata: !conflict,
            ..Default::default()
        })
        .await;
        let mut spec = export_spec(&fixture, &server, true).await?;
        spec.destination.as_mut().unwrap().draft_id = Some("3".into());
        let ctx =
            claim_context(&fixture, job_id(), JobPayload::ExportRoCrate(spec.clone())).await?;
        match run_export_job(&ctx, &spec).await {
            JobRunOutcome::Failed(error) => assert!(
                error
                    .message
                    .contains(if conflict { "412" } else { "metadata differs" }),
                "{}",
                error.message
            ),
            _ => panic!("unsafe draft update accepted"),
        }
        assert!(!server.state.lock().unwrap().published);
        assert!(server.state.lock().unwrap().files.is_empty());
        fixture.stop().await;
    }
    Ok(())
}

#[tokio::test]
async fn invenio_recovers_metadata() -> Result<(), Box<dyn std::error::Error>> {
    for change in 0..4 {
        let fixture = build_fixture(false).await?;
        let server = serve(Repository {
            lost_metadata: true,
            ..Default::default()
        })
        .await;
        let mut spec = export_spec(&fixture, &server, true).await?;
        spec.destination.as_mut().unwrap().new_version = Some("2".into());
        if change == 3 {
            spec.destination.as_mut().unwrap().metadata_json = json!({"creators": [{
                "person_or_org": {"type": "personal", "family_name": "Researcher", "given_name": "A"},
                "role": {"id": "researcher"}
            }]}).to_string();
        }
        let succeeds = matches!(change, 0 | 3);
        let ctx =
            claim_context(&fixture, job_id(), JobPayload::ExportRoCrate(spec.clone())).await?;
        match run_export_job(&ctx, &spec).await {
            JobRunOutcome::Failed(error) => assert_eq!(
                error.kind,
                aruna_core::structs::execution::job::JobErrorKind::Retryable
            ),
            _ => panic!("lost metadata reply did not fail"),
        }
        if change == 1 {
            server.state.lock().unwrap().metadata.as_mut().unwrap()["subjects"] =
                json!([{"subject": "concurrent"}]);
        }
        if change == 2 {
            server.state.lock().unwrap().metadata.as_mut().unwrap()["creators"][0]["role"] =
                json!({"id": "datamanager"});
        }
        match run_export_job(&ctx, &spec).await {
            JobRunOutcome::Succeeded(_) if succeeds => {}
            JobRunOutcome::Failed(error) if !succeeds => {
                assert!(error.message.contains("ambiguous metadata"))
            }
            _ => panic!("incorrect metadata reconciliation"),
        }
        assert_eq!(server.state.lock().unwrap().published, succeeds);
        assert_eq!(
            server
                .state
                .lock()
                .unwrap()
                .calls
                .iter()
                .filter(|(method, path)| *method == Method::PUT && path.ends_with("/draft"))
                .count(),
            1
        );
        fixture.stop().await;
    }
    Ok(())
}

#[tokio::test]
async fn invenio_cancels_reference() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let server = serve(Repository {
        head_started: Some(started.clone()),
        head_release: Some(release.clone()),
        ..Default::default()
    })
    .await;
    let spec = spec_with_source(
        &fixture,
        ImportRoCrateSource::Invenio {
            group_id: fixture.group_id,
            connector_id: connector(&fixture, &server).await,
            record_id: "2".into(),
            options: aruna_core::invenio::InvenioOptions {
                mode: aruna_core::invenio::InvenioMode::Reference,
                all_versions: false,
            },
        },
        doc_id(1),
    );
    let ctx = claim_context(&fixture, job_id(), JobPayload::ImportRoCrate(spec.clone())).await?;
    let cancel = ctx.cancel.clone();
    let task = tokio::spawn(async move { run_rocrate_import(&ctx, &spec).await });
    tokio::time::timeout(std::time::Duration::from_secs(120), started.notified()).await?;
    cancel.cancel();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(120), task).await??,
        JobRunOutcome::Cancelled
    ));
    release.notify_one();
    let key = format!(
        "imported/{}",
        aruna_core::invenio::file_path("2", "data.txt")?
    );
    assert!(object_versions(&fixture, &key).await?.is_empty());
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn invenio_removes_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = serve(Repository::default()).await;
    let mut spec = export_spec(&fixture, &server, false).await?;
    let first = claim_context(&fixture, job_id(), JobPayload::ExportRoCrate(spec.clone())).await?;
    assert!(matches!(
        run_export_job(&first, &spec).await,
        JobRunOutcome::Succeeded(_)
    ));
    server.state.lock().unwrap().metadata.as_mut().unwrap()["subjects"] =
        json!([{"subject": "obsolete"}]);
    spec.destination.as_mut().unwrap().draft_id = Some("3".into());
    let next = claim_context(&fixture, job_id(), JobPayload::ExportRoCrate(spec.clone())).await?;
    match run_export_job(&next, &spec).await {
        JobRunOutcome::Succeeded(_) => {}
        JobRunOutcome::Failed(error) => panic!("{}", error.message),
        _ => panic!("unexpected metadata replacement result"),
    }
    assert!(
        server
            .state
            .lock()
            .unwrap()
            .metadata
            .as_ref()
            .unwrap()
            .get("subjects")
            .is_none()
    );
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn invenio_ambiguous_creation() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = serve(Repository {
        lost_create: true,
        ..Default::default()
    })
    .await;
    let spec = export_spec(&fixture, &server, false).await?;
    let ctx = claim_context(&fixture, job_id(), JobPayload::ExportRoCrate(spec.clone())).await?;
    for _ in 0..2 {
        assert!(matches!(
            run_export_job(&ctx, &spec).await,
            JobRunOutcome::Failed(_)
        ));
    }
    assert_eq!(
        server
            .state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|(method, path)| *method == Method::POST && path == "/api/records")
            .count(),
        1
    );
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn invenio_requires_login() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = serve(Repository::default()).await;
    let mut spec = export_spec(&fixture, &server, true).await?;
    spec.destination.as_mut().unwrap().credential = None;
    let ctx = claim_context(&fixture, job_id(), JobPayload::ExportRoCrate(spec.clone())).await?;
    match run_export_job(&ctx, &spec).await {
        JobRunOutcome::Failed(error) => {
            assert!(error.message.contains("personal repository login"))
        }
        _ => panic!("export accepted a shared connector login"),
    }
    assert!(server.state.lock().unwrap().calls.is_empty());
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn invenio_reuses_draft() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = serve(Repository::default()).await;
    let mut spec = export_spec(&fixture, &server, true).await?;
    spec.destination.as_mut().unwrap().draft_id = Some("3".into());
    let ctx = claim_context(&fixture, job_id(), JobPayload::ExportRoCrate(spec.clone())).await?;
    match run_export_job(&ctx, &spec).await {
        JobRunOutcome::Succeeded(_) => {}
        JobRunOutcome::Failed(error) => panic!("{}", error.message),
        _ => panic!("draft export did not complete"),
    }
    {
        let state = server.state.lock().unwrap();
        assert_eq!(
            state.metadata.as_ref().unwrap()["title"],
            "Exported dataset"
        );
        assert!(
            state
                .calls
                .contains(&(Method::PUT, "/api/records/3/draft".into()))
        );
        assert!(!state.calls.contains(&(Method::POST, "/api/records".into())));
        assert_eq!(state.files.len(), 3);
    }
    fixture.stop().await;
    Ok(())
}
