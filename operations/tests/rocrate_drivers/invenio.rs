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

#[derive(Default)]
struct Repository {
    calls: Vec<(Method, String)>,
    bytes: Vec<u8>,
    key: Option<String>,
    committed: bool,
    published: bool,
    corrupt: bool,
    loop_pages: bool,
    lost_create: bool,
    lost_commit: bool,
    lost_publish: bool,
    foreign_page: bool,
    public_files: bool,
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
        "is_published": published, "versions": {"index": id.parse::<u64>().unwrap()},
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
        "Bearer repository-token"
    );
    let expected = if request.method() == Method::GET && request.uri().path().ends_with("/content")
    {
        "application/octet-stream"
    } else {
        "application/vnd.inveniordm.v1+json, application/json;q=0.9"
    };
    assert_eq!(request.headers().get("accept").unwrap(), expected);
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or("").to_string();
    let body = to_bytes(request.into_body(), 1024 * 1024).await.unwrap();
    let mut state = state.lock().unwrap();
    state.calls.push((method.clone(), path.clone()));
    let parts = path.trim_start_matches('/').split('/').collect::<Vec<_>>();
    let value = match (method, parts.as_slice()) {
        (Method::GET, ["api", "records", "2", "versions"]) => {
            let second = query.contains("page=2");
            if state.foreign_page {
                return axum::Json(json!({"hits": {"total": 2, "hits": [{"id": "1"}]},
                    "links": {"next": "http://127.0.0.1:1/api/records/2/versions"}}))
                .into_response();
            }
            json!({"hits": {"total": 2, "hits": [{"id": if second {"2"} else {"1"}}]},
                "links": {"next": if !second || state.loop_pages {Some("/api/records/2/versions?page=2")} else {None}}})
        }
        (Method::GET, ["api", "records", id @ ("1" | "2")]) => record(id, true),
        (Method::GET, ["api", "records", id @ ("1" | "2"), "files"]) => {
            let mut entry = file("data.txt", id.as_bytes(), true);
            if state.corrupt {
                entry["checksum"] = json!("md5:00000000000000000000000000000000");
            }
            json!({"entries": [entry]})
        }
        (
            Method::GET,
            [
                "api",
                "records",
                id @ ("1" | "2"),
                "files",
                "data.txt",
                "content",
            ],
        ) => {
            return (StatusCode::OK, id.to_string()).into_response();
        }
        (Method::POST, ["api", "records"]) => {
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["metadata"]["title"], "Exported dataset");
            assert_eq!(body["files"]["enabled"], true);
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
            record("3", false)
        }
        (Method::GET, ["api", "records", "3", "draft"]) => {
            if state.published {
                return StatusCode::NOT_FOUND.into_response();
            }
            record("3", false)
        }
        (Method::GET, ["api", "records", "3"]) if state.published => record("3", true),
        (Method::GET, ["api", "records", "3", "draft", "files"])
        | (Method::GET, ["api", "records", "3", "files"]) => {
            json!({"entries": state.key.iter().map(|key| file(key, &state.bytes, state.committed)).collect::<Vec<_>>()})
        }
        (Method::POST, ["api", "records", "3", "draft", "files"]) => {
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert!(state.key.is_none());
            state.key = Some(body[0]["key"].as_str().unwrap().into());
            json!({"entries": [file(state.key.as_ref().unwrap(), &[], false)]})
        }
        (Method::PUT, ["api", "records", "3", "draft", "files", key, "content"]) => {
            assert_eq!(state.key.as_deref(), Some(*key));
            state.bytes = body.to_vec();
            json!({})
        }
        (Method::POST, ["api", "records", "3", "draft", "files", key, "commit"]) => {
            assert_eq!(state.key.as_deref(), Some(*key));
            state.committed = true;
            if std::mem::take(&mut state.lost_commit) {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            file(key, &state.bytes, true)
        }
        (Method::POST, ["api", "records", "3", "draft", "actions", "publish"]) => {
            assert!(state.committed);
            assert!(!state.published);
            state.published = true;
            if std::mem::take(&mut state.lost_publish) {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            record("3", true)
        }
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    axum::Json(value).into_response()
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
        let server = serve(repository).await;
        let connector_id = connector(&fixture, &server).await;
        let spec = spec_with_source(
            &fixture,
            ImportRoCrateSource::Invenio {
                group_id: fixture.group_id,
                connector_id,
                record_id: "2".into(),
            },
            doc_id(1),
        );
        let ctx =
            claim_context(&fixture, job_id(), JobPayload::ImportRoCrate(spec.clone())).await?;
        assert!(matches!(
            run_rocrate_import(&ctx, &spec).await,
            JobRunOutcome::Failed(_)
        ));
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
    let upload = create_upload(fixture, crate_archive().await?).await?;
    let import = import_spec(fixture, upload, doc_id(1));
    let ctx = claim_context(fixture, job_id(), JobPayload::ImportRoCrate(import.clone())).await?;
    assert!(matches!(
        run_rocrate_import(&ctx, &import).await,
        JobRunOutcome::Succeeded(_)
    ));
    replay_event_log(fixture.context.as_ref()).await?;
    process_materialization_batch(fixture.context.as_ref()).await?;
    Ok(ExportRoCrateSpec {
        auth_context: import.auth_context, document_id: doc_id(1), limits: RoCrateLimits::default(),
        destination: Some(InvenioDestination {
            group_id: fixture.group_id, connector_id: connector(fixture, server).await, draft_id: None,
            metadata_json: json!({"title": "Exported dataset", "publication_date": "2026-09-22",
                "resource_type": {"id": "dataset"}, "creators": [{"person_or_org": {"type": "organizational", "name": "Lab"}}]}).to_string(),
            publish,
            public_files: false,
        }),
    })
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
                1
            );
            let mut archive = zip::ZipArchive::new(Cursor::new(&state.bytes))?;
            let mut data = Vec::new();
            archive.by_name("data.txt")?.read_to_end(&mut data)?;
            assert_eq!(data, PAYLOAD);
            assert!(archive.by_name("ro-crate-metadata.json").is_ok());
        }
        fixture.stop().await;
    }
    Ok(())
}

#[tokio::test]
async fn invenio_export_recovers() -> Result<(), Box<dyn std::error::Error>> {
    for lost_commit in [false, true] {
        let fixture = build_fixture(false).await?;
        let server = serve(Repository {
            lost_commit,
            lost_publish: !lost_commit,
            ..Default::default()
        })
        .await;
        let spec = export_spec(&fixture, &server, true).await?;
        let ctx =
            claim_context(&fixture, job_id(), JobPayload::ExportRoCrate(spec.clone())).await?;
        assert!(matches!(
            run_export_job(&ctx, &spec).await,
            JobRunOutcome::Failed(_)
        ));
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
                1
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
