//! Imports native repository records from the isolated Invenio HTTP fixture.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

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
    Box::pin(super::link::run_registration(
        &fixture,
        ctx.job_id,
        spec.auth_context.user_id,
    ))
    .await?;
    let mapping =
        aruna_operations::metadata::persistent_id::read_mapping(&fixture.context, doc_id(1))
            .await?
            .expect("created document has a mapping");
    let identifiers = mapping
        .secondary_identifiers
        .iter()
        .map(|id| (id.kind.as_str(), id.value.as_str(), id.endpoint.clone()))
        .collect::<Vec<_>>();
    let endpoint = Some(server.endpoint.trim_end_matches('/').to_string());
    assert_eq!(
        identifiers,
        vec![
            ("doi", "10.1234/1", None),
            ("doi", "10.1234/2", None),
            ("doi", "10.1234/all", None),
            ("invenio_record", "1", endpoint.clone()),
            ("invenio_record", "2", endpoint.clone()),
            ("invenio_parent", "parent", endpoint),
        ]
    );
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
async fn invenio_follows_redirects() -> Result<(), Box<dyn std::error::Error>> {
    use aruna_core::invenio::{InvenioMode, InvenioOptions};
    let seen = Arc::new(Mutex::new(Vec::new()));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let storage = format!("http://{}/storage/", listener.local_addr()?);
    let recorder = seen.clone();
    let app = Router::new().fallback(move |request: Request| async move {
        recorder
            .lock()
            .unwrap()
            .push(request.headers().contains_key("authorization"));
        let id = request.uri().path().split('/').nth(2).unwrap_or_default();
        stored_content(id)
    });
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for (foreign, mode) in [
        (false, InvenioMode::Copy),
        (true, InvenioMode::Copy),
        (true, InvenioMode::Reference),
    ] {
        let fixture = build_fixture(false).await?;
        let server = serve(Repository::default()).await;
        let target = if foreign {
            storage.clone()
        } else {
            server.endpoint.replace("/api/", "/storage/")
        };
        server.state.lock().unwrap().redirect = Some(target);
        let spec = spec_with_source(
            &fixture,
            ImportRoCrateSource::Invenio {
                group_id: fixture.group_id,
                connector_id: connector(&fixture, &server).await,
                record_id: "2".into(),
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
            JobRunOutcome::Succeeded(_) => {}
            JobRunOutcome::Failed(error) => panic!("{}", error.message),
            _ => panic!("redirected import did not complete"),
        }
        if mode == InvenioMode::Reference {
            use aruna_operations::s3::object::get::{GetObjectInput, GetObjectOperation};
            let mut object = drive(
                GetObjectOperation::new(GetObjectInput {
                    bucket: BUCKET.into(),
                    key: format!(
                        "imported/{}",
                        aruna_core::invenio::file_path("2", "data.txt")?
                    ),
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
        }
        let local = server
            .state
            .lock()
            .unwrap()
            .calls
            .iter()
            .any(|(_, path)| path.starts_with("/storage/"));
        assert_eq!(local, !foreign);
        fixture.stop().await;
    }
    let seen = seen.lock().unwrap();
    assert!(seen.len() >= 2);
    assert!(seen.iter().all(|authorized| !authorized));
    task.abort();
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
