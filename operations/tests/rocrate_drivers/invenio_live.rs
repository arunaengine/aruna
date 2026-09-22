//! Opt-in acceptance against a disposable loopback InvenioRDM instance.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::invenio::{InvenioMode, InvenioOptions, InvenioQuery, InvenioRecord};
use aruna_operations::jobs::invenio::{seal_credential, search_records};
use aruna_operations::s3::object::get::{GetObjectInput, GetObjectOperation};

#[tokio::test]
#[ignore = "requires a disposable loopback Invenio instance and personal token file"]
async fn native_repository() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("ARUNA_INVENIO_ENDPOINT")?;
    let url = reqwest::Url::parse(&endpoint)?;
    assert!(matches!(
        url.host_str(),
        Some("127.0.0.1" | "localhost" | "[::1]")
    ));
    let token = std::fs::read_to_string(std::env::var("ARUNA_INVENIO_TOKEN_FILE")?)?;
    let token = token.trim();
    let user: u64 = std::env::var("ARUNA_INVENIO_USER_ID")?.parse()?;
    let fixture = build_fixture(false).await?;
    let connector = live_connector(&fixture, &endpoint, Some(token)).await?;
    let upload = create_upload(&fixture, native_archive().await?).await?;
    let imported = import_spec(&fixture, upload, doc_id(1));
    let ctx = claim_context(
        &fixture,
        job_id(),
        JobPayload::ImportRoCrate(imported.clone()),
    )
    .await?;
    require_import(run_rocrate_import(&ctx, &imported).await)?;
    replay_event_log(fixture.context.as_ref()).await?;
    process_materialization_batch(fixture.context.as_ref()).await?;
    let title = format!("Aruna acceptance {}", Ulid::generate());
    let mut destination = InvenioDestination {
        group_id: fixture.group_id,
        connector_id: connector,
        draft_id: None,
        new_version: None,
        metadata_json: json!({"title": title}).to_string(),
        publish: false,
        public_files: false,
        credential: None,
    };
    destination.credential = Some(
        seal_credential(
            &fixture.context,
            &imported.auth_context,
            &destination,
            token,
        )
        .await?,
    );
    let mut spec = ExportRoCrateSpec {
        auth_context: imported.auth_context.clone(),
        document_id: doc_id(1),
        limits: RoCrateLimits::default(),
        destination: Some(destination),
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;
    let draft = transfer(&fixture, &spec).await?;
    assert!(!draft.published);
    let value: Value = client
        .get(&draft.url)
        .bearer_auth(token)
        .header("Accept", "application/vnd.inveniordm.v1+json")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(value["metadata"]["title"], title);
    assert_eq!(value["parent"]["access"]["owned_by"]["user"], user);
    assert_eq!(value["access"]["files"], "restricted");
    let target = spec.destination.as_mut().unwrap();
    target.draft_id = Some(draft.id.clone());
    target.publish = true;
    let first = transfer(&fixture, &spec).await?;
    assert!(first.published);
    let target = spec.destination.as_mut().unwrap();
    target.draft_id = None;
    target.new_version = Some(first.id.clone());
    let second = transfer(&fixture, &spec).await?;
    assert!(second.published);
    assert_ne!(first.id, second.id);
    assert_eq!(first.parent_id, second.parent_id);
    let mut file_url = url.clone();
    file_url
        .path_segments_mut()
        .unwrap()
        .pop_if_empty()
        .extend(["records", &second.id, "files", "nested/data.txt", "content"]);
    assert_eq!(
        client
            .get(file_url.clone())
            .bearer_auth(token)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?
            .as_ref(),
        PAYLOAD
    );
    assert_eq!(
        client.get(file_url).send().await?.status(),
        StatusCode::FORBIDDEN
    );
    let query = InvenioQuery {
        group_id: fixture.group_id,
        connector_id: connector,
        q: format!("\"{title}\""),
        page: 1,
        size: 25,
        all_versions: false,
    };
    tokio::time::timeout(std::time::Duration::from_secs(180), async {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let page = search_records(
                &fixture.context,
                &imported.auth_context,
                &query,
                1024 * 1024,
            )
            .await?;
            if page["hits"]["hits"]
                .as_array()
                .is_some_and(|hits| hits.iter().any(|hit| hit["id"] == second.id))
            {
                return Ok::<_, Box<dyn std::error::Error>>(());
            }
        }
    })
    .await??;
    let anonymous = live_connector(&fixture, &endpoint, None).await?;
    for (index, mode) in [
        InvenioMode::Copy,
        InvenioMode::Reference,
        InvenioMode::Metadata,
    ]
    .into_iter()
    .enumerate()
    {
        let mut import = spec_with_source(
            &fixture,
            ImportRoCrateSource::Invenio {
                group_id: fixture.group_id,
                connector_id: if mode == InvenioMode::Metadata {
                    anonymous
                } else {
                    connector
                },
                record_id: second.id.clone(),
                options: InvenioOptions {
                    mode,
                    all_versions: true,
                },
            },
            doc_id(index as u64 + 2),
        );
        import.target.prefix = format!("live-{index}");
        let ctx = claim_context(
            &fixture,
            job_id(),
            JobPayload::ImportRoCrate(import.clone()),
        )
        .await?;
        let count = require_import(run_rocrate_import(&ctx, &import).await)?;
        assert_eq!(count, if mode == InvenioMode::Metadata { 2 } else { 8 });
        if mode != InvenioMode::Metadata {
            let key = format!(
                "live-{index}/{}",
                aruna_core::invenio::file_path(&second.id, "nested/data.txt")?
            );
            let mut object = drive(
                GetObjectOperation::new(GetObjectInput {
                    bucket: BUCKET.into(),
                    key,
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
            assert_eq!(bytes, PAYLOAD);
        }
    }
    fixture.stop().await;
    Ok(())
}

async fn live_connector(
    fixture: &Fixture,
    endpoint: &str,
    token: Option<&str>,
) -> Result<Ulid, Box<dyn std::error::Error>> {
    Ok(drive(
        SourceConnectorOperation::new(SourceConnectorInput {
            group_id: fixture.group_id,
            created_by: fixture.actor.user_id,
            name: format!("live-{}", Ulid::generate()),
            kind: SourceConnectorKind::Http,
            public_config: HashMap::from([("endpoint".into(), endpoint.into())]),
            secret_config: token
                .map(|token| HashMap::from([("token".into(), token.into())]))
                .unwrap_or_default(),
        }),
        &fixture.context,
    )
    .await?
    .connector
    .connector_id)
}

async fn transfer(
    fixture: &Fixture,
    spec: &ExportRoCrateSpec,
) -> Result<InvenioRecord, Box<dyn std::error::Error>> {
    let ctx = claim_context(fixture, job_id(), JobPayload::ExportRoCrate(spec.clone())).await?;
    match run_export_job(&ctx, spec).await {
        JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(result)) => result
            .repository
            .ok_or_else(|| "missing repository result".into()),
        JobRunOutcome::Failed(error) => Err(error.message.into()),
        _ => Err("unexpected export outcome".into()),
    }
}

fn require_import(outcome: JobRunOutcome) -> Result<u64, Box<dyn std::error::Error>> {
    match outcome {
        JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(result)) => Ok(result.imported),
        JobRunOutcome::Failed(error) => Err(error.message.into()),
        _ => Err("unexpected import outcome".into()),
    }
}
