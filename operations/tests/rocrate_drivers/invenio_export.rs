//! Exports crates as native repository records to the isolated Invenio HTTP fixture.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

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
