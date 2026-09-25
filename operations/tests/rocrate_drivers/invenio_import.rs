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
            pull: None,
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
    // Registration checks WRITE on the document, so it waits for its registry row.
    replay_event_log(fixture.context.as_ref()).await?;
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
    // Adding identifiers needs WRITE on the document, also on the authority itself.
    let stranger = AuthContext {
        user_id: aruna_core::UserId::local(ulid::Ulid::from_parts(7, 7), fixture.actor.realm_id),
        realm_id: fixture.actor.realm_id,
        path_restrictions: None,
        session: None,
    };
    let doi = aruna_core::structs::secondary_id::SecondaryIdentifier::new(
        aruna_core::structs::secondary_id::SecondaryIdKind::Doi,
        "10.1234/foreign",
        None,
        aruna_core::structs::secondary_id::IdentifierOrigin::Published,
    )?;
    let denied = aruna_operations::metadata::persistent_id::forward::add_identifiers_routed(
        &fixture.context,
        fixture.actor.realm_id,
        doc_id(1),
        vec![doi],
        unix_timestamp_millis(),
        stranger,
    )
    .await;
    assert!(
        matches!(
            denied,
            Err(aruna_operations::metadata::api::MetadataApiError::Forbidden
                | aruna_operations::metadata::api::MetadataApiError::Unauthorized)
        ),
        "{denied:?}"
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
                pull: None,
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
                pull: None,
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
                pull: None,
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
            pull: None,
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
async fn invenio_pull_updates() -> Result<(), Box<dyn std::error::Error>> {
    use aruna_core::invenio::{InvenioOptions, InvenioPull, PullCheck, crate_versions};
    use aruna_operations::jobs::invenio::link_queue::current_event;
    use aruna_operations::jobs::invenio::links::{LinkChange, change_link, list_links};
    use aruna_operations::jobs::invenio::pull::start_pull;
    use aruna_operations::metadata::raw_revision::load_raw_revision;
    let fixture = build_fixture(false).await?;
    let server = serve(Repository::default()).await;
    let connector_id = connector(&fixture, &server).await;
    let spec = spec_with_source(
        &fixture,
        ImportRoCrateSource::Invenio {
            group_id: fixture.group_id,
            connector_id,
            record_id: "1".into(),
            options: InvenioOptions {
                all_versions: false,
                ..InvenioOptions::default()
            },
            pull: Some(InvenioPull::Keep {
                auto_update: false,
                owner_node_url: "https://node.example/api/v1".into(),
            }),
        },
        doc_id(1),
    );
    let ctx = claim_context(&fixture, job_id(), JobPayload::ImportRoCrate(spec.clone())).await?;
    super::link::succeeded(Box::pin(run_rocrate_import(&ctx, &spec)).await);
    replay_event_log(fixture.context.as_ref()).await?;
    process_materialization_batch(fixture.context.as_ref()).await?;
    let storage = &fixture.context.storage_handle;
    let links = list_links(storage, doc_id(1)).await?;
    let [(link, true)] = links.as_slice() else {
        panic!("the import keeps one pull link with its next check queued: {links:?}");
    };
    let first = current_event(&fixture.context, doc_id(1)).await?;
    assert_eq!(link.remote.record_id.as_deref(), Some("1"));
    assert_eq!(link.remote.doi.as_deref(), Some("10.1234/1"));
    assert_eq!(link.pull().and_then(|pull| pull.revision), Some(first));

    // The repository published version 2; the user pulls it.
    let found = PullCheck::Found {
        latest_id: "2".into(),
        revision: 1,
        local: Some(first),
    };
    let link = Box::pin(change_link(
        &fixture.context,
        link,
        LinkChange::Checked(found),
    ))
    .await?
    .ok_or("link missing")?;
    assert_eq!(link.pull_reason(), Some("update_available"));
    let pull = Box::pin(start_pull(&fixture.context, &link)).await?;
    let record = aruna_operations::jobs::store::read_job_record(storage, pull, None)
        .await?
        .ok_or("pull job missing")?;
    let JobPayload::ImportRoCrate(update) = record.payload.clone() else {
        return Err("pull job is not an import".into());
    };
    let ctx = claim_context(&fixture, pull, record.payload).await?;
    super::link::succeeded(Box::pin(run_rocrate_import(&ctx, &update)).await);
    replay_event_log(fixture.context.as_ref()).await?;
    process_materialization_batch(fixture.context.as_ref()).await?;

    let revision = load_raw_revision(&fixture.context, doc_id(1), None)
        .await?
        .ok_or("revision missing")?;
    let document: Value = serde_json::from_str(&revision.jsonld)?;
    assert_eq!(crate_versions(&document), ["1", "2"]);
    let graph = document["@graph"].as_array().ok_or("graph missing")?;
    let root_id = graph
        .iter()
        .find(|entity| entity["@id"] == "ro-crate-metadata.json")
        .map(|descriptor| descriptor["about"]["@id"].clone())
        .ok_or("descriptor missing")?;
    let root = graph
        .iter()
        .find(|entity| entity["@id"] == root_id)
        .ok_or("root missing")?;
    assert_eq!(root["name"], "Record 2");
    for id in ["1", "2"] {
        let key = format!(
            "imported/{}",
            aruna_core::invenio::file_path(id, "data.txt")?
        );
        assert_eq!(object_versions(&fixture, &key).await?.len(), 1, "{key}");
    }
    let (pulled, _) = list_links(storage, doc_id(1)).await?.remove(0);
    assert_eq!(pulled.active_job, None);
    assert_eq!(pulled.remote.record_id.as_deref(), Some("2"));
    assert_eq!(pulled.remote.doi.as_deref(), Some("10.1234/2"));
    assert_eq!(pulled.pull_reason(), None);
    assert_eq!(
        pulled.pull().and_then(|pull| pull.revision),
        Some(revision.winning_event_id)
    );
    Ok(())
}
