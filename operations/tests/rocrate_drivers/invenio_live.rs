//! Opt-in acceptance against a disposable loopback InvenioRDM instance.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::invenio::{InvenioMode, InvenioOptions, InvenioQuery, InvenioRecord};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_operations::harvest::create_connector::{CreateConnectorInput, CreateConnectorOperation};
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
        metadata_json: json!({"title": title, "publisher": "Aruna acceptance"}).to_string(),
        publish: false,
        public_files: false,
        credential: None,
        link: None,
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
        .header(
            "Accept",
            "application/vnd.inveniordm.v1+json, application/json;q=0.9",
        )
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(value["metadata"]["title"], title);
    let owner = &value["parent"]["access"]["owned_by"]["user"];
    assert_eq!(
        owner.as_u64().or_else(|| owner.as_str()?.parse().ok()),
        Some(user)
    );
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
                pull: None,
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

#[tokio::test]
#[ignore = "requires a disposable loopback Invenio instance and personal token file"]
async fn link_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    use super::link::{change, current, drain, due_now, linked, run_push, succeeded};
    use aruna_core::invenio::{LinkFailure, LinkPatch, LinkStatus};
    use aruna_operations::jobs::invenio::links::{LinkChange, change_link};
    let endpoint = std::env::var("ARUNA_INVENIO_ENDPOINT")?;
    let token = std::fs::read_to_string(std::env::var("ARUNA_INVENIO_TOKEN_FILE")?)?;
    let token = token.trim();
    let fixture = build_fixture(false).await?;
    let link = Box::pin(linked(&fixture, &endpoint, token, false, None)).await?;
    // DOI registration needs a publisher, which the crate does not name.
    let publisher = LinkPatch {
        metadata_json: Some(json!({"publisher": "Aruna acceptance"}).to_string()),
        ..LinkPatch::default()
    };
    Box::pin(change_link(
        fixture.context.as_ref(),
        &link,
        LinkChange::Patch(publisher),
    ))
    .await?;
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    let first = current(&fixture, &link).await.0;
    let draft = first
        .remote
        .draft_id
        .clone()
        .ok_or("first push left no draft")?;
    assert!(!first.remote.published);
    let reserved = first.remote.doi.clone().ok_or("no DOI reserved")?;
    assert!(first.remote.doi_reserved);

    Box::pin(change(&fixture, "Live second revision", true)).await?;
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    let second = current(&fixture, &link).await.0;
    assert_eq!(second.remote.draft_id.as_deref(), Some(draft.as_str()));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;
    let get = async |path: String| -> Result<Value, Box<dyn std::error::Error>> {
        Ok(client
            .get(format!("{endpoint}{path}"))
            .bearer_auth(token)
            .header("Accept", "application/json")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    };
    let record = get(format!("records/{draft}/draft")).await?;
    assert_eq!(record["metadata"]["description"], "Live second revision");
    assert_eq!(
        record["pids"]["doi"]["identifier"].as_str(),
        Some(reserved.as_str())
    );
    assert_eq!(record["access"]["files"], "restricted");
    let files = get(format!("records/{draft}/draft/files")).await?;
    let mut keys = files["entries"]
        .as_array()
        .ok_or("draft files missing")?
        .iter()
        .filter_map(|file| file["key"].as_str().map(str::to_string))
        .collect::<Vec<_>>();
    keys.sort();
    assert_eq!(keys, ["nested/data.txt", "ro-crate-metadata.json"]);

    // An edit in the repository stops the link until the user accepts it.
    let put = client
        .put(format!("{endpoint}records/{draft}/draft"))
        .bearer_auth(token)
        .header("Accept", "application/json")
        .header("If-Match", record["revision_id"].to_string())
        .json(
            &json!({"metadata": record["metadata"], "custom_fields": record["custom_fields"],
            "access": {"record": "public", "files": "restricted"}, "pids": record["pids"],
            "files": {"enabled": true}}),
        )
        .send()
        .await?;
    put.error_for_status()?;
    Box::pin(change(&fixture, "Live revision after remote edit", false)).await?;
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    assert!(matches!(
        run_push(&fixture, &link).await?,
        JobRunOutcome::Failed(_)
    ));
    let failed = current(&fixture, &link).await.0;
    assert_eq!(
        failed.status,
        LinkStatus::Failed {
            reason: LinkFailure::RemoteChanged
        }
    );
    let state = Box::pin(aruna_operations::jobs::invenio::remote_state(
        fixture.context.as_ref(),
        &failed,
    ))
    .await?;
    Box::pin(change_link(
        fixture.context.as_ref(),
        &failed,
        LinkChange::Accept(Box::new(state)),
    ))
    .await?;
    let public = LinkPatch {
        public_files: Some(true),
        ..LinkPatch::default()
    };
    let accepted = current(&fixture, &link).await.0;
    Box::pin(change_link(
        fixture.context.as_ref(),
        &accepted,
        LinkChange::Patch(public),
    ))
    .await?;
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    let continued = current(&fixture, &link).await.0;
    assert_eq!(continued.remote.draft_id.as_deref(), Some(draft.as_str()));
    let record = get(format!("records/{draft}/draft")).await?;
    assert_eq!(record["access"]["files"], "public");
    assert_eq!(
        record["metadata"]["description"],
        "Live revision after remote edit"
    );

    let event = aruna_operations::metadata::raw_revision::load_raw_revision(
        &fixture.context,
        doc_id(1),
        None,
    )
    .await?
    .ok_or("revision missing")?
    .winning_event_id;
    Box::pin(aruna_operations::jobs::invenio::link_queue::start_push(
        &fixture.context,
        &continued,
        event,
        true,
    ))
    .await?;
    succeeded(run_push(&fixture, &link).await?);
    let published = current(&fixture, &link).await.0;
    assert!(published.remote.published && !published.remote.doi_reserved);
    assert_eq!(published.remote.record_id.as_deref(), Some(draft.as_str()));
    assert_eq!(published.remote.doi.as_deref(), Some(reserved.as_str()));
    assert!(published.remote.concept_doi.is_some());
    assert_eq!(published.warning, None);

    Box::pin(change(&fixture, "Live third revision", false)).await?;
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    let version = current(&fixture, &link).await.0;
    let next = version
        .remote
        .draft_id
        .clone()
        .ok_or("no new version draft")?;
    assert_ne!(next, draft);
    assert_eq!(version.remote.parent_id, published.remote.parent_id);
    let record = get(format!("records/{next}/draft")).await?;
    assert_eq!(
        record["parent"]["id"].as_str(),
        published.remote.parent_id.as_deref()
    );
    assert_eq!(record["metadata"]["description"], "Live third revision");
    // Every version reserves its own DOI.
    let doi = version
        .remote
        .doi
        .as_deref()
        .ok_or("new version has no DOI")?;
    assert!(version.remote.doi_reserved && doi != reserved);
    assert_eq!(record["pids"]["doi"]["identifier"].as_str(), Some(doi));
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable loopback Invenio instance and personal token file"]
async fn link_community() -> Result<(), Box<dyn std::error::Error>> {
    use super::link::{attach, current, drain, due_now, import_dataset, run_push, succeeded};
    use aruna_core::invenio::{LinkPatch, LinkReview};
    use aruna_operations::jobs::invenio::links::{LinkChange, change_link};
    let endpoint = std::env::var("ARUNA_INVENIO_ENDPOINT")?;
    let token = std::fs::read_to_string(std::env::var("ARUNA_INVENIO_TOKEN_FILE")?)?;
    let token = token.trim();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;
    let call = async |method: reqwest::Method, path: String, body: Option<Value>| {
        let mut request = client
            .request(method, format!("{endpoint}{path}"))
            .bearer_auth(token)
            .header("Accept", "application/json");
        if let Some(body) = body {
            request = request.json(&body);
        }
        let value: Value = request.send().await?.error_for_status()?.json().await?;
        Ok::<_, Box<dyn std::error::Error>>(value)
    };
    let slug = format!("aruna-{}", Ulid::generate()).to_lowercase();
    call(
        reqwest::Method::POST,
        "communities".into(),
        Some(json!({"slug": slug, "access": {"visibility": "public"},
            "metadata": {"title": "Aruna acceptance"}})),
    )
    .await?;

    let fixture = build_fixture(false).await?;
    Box::pin(import_dataset(&fixture, native_archive().await?)).await?;
    let link = Box::pin(attach(&fixture, &endpoint, token, false, None, Some(&slug))).await?;
    let publisher = LinkPatch {
        metadata_json: Some(json!({"publisher": "Aruna acceptance"}).to_string()),
        ..LinkPatch::default()
    };
    Box::pin(change_link(
        fixture.context.as_ref(),
        &link,
        LinkChange::Patch(publisher),
    ))
    .await?;
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    let pushed = current(&fixture, &link).await.0;
    let draft = pushed.remote.draft_id.clone().ok_or("no draft")?;
    let event = aruna_operations::metadata::raw_revision::load_raw_revision(
        &fixture.context,
        doc_id(1),
        None,
    )
    .await?
    .ok_or("revision missing")?
    .winning_event_id;
    Box::pin(aruna_operations::jobs::invenio::link_queue::start_push(
        &fixture.context,
        &pushed,
        event,
        true,
    ))
    .await?;
    succeeded(run_push(&fixture, &link).await?);
    let submitted = current(&fixture, &link).await.0;
    assert_eq!(submitted.remote.review, LinkReview::Pending);
    assert!(!submitted.remote.published);
    let review = call(
        reqwest::Method::GET,
        format!("records/{draft}/draft/review"),
        None,
    )
    .await?;
    assert_eq!(review["status"], "submitted");

    // The token's owner also owns the community, so it can accept its own submission.
    let request = review["id"].as_str().ok_or("review request id missing")?;
    call(
        reqwest::Method::POST,
        format!("requests/{request}/actions/accept"),
        Some(json!({})),
    )
    .await?;
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    let accepted = current(&fixture, &link).await.0;
    assert_eq!(accepted.remote.review, LinkReview::Accepted);
    assert!(accepted.remote.published);
    assert_eq!(accepted.remote.record_id.as_deref(), Some(draft.as_str()));
    assert!(accepted.remote.doi.is_some() && !accepted.remote.doi_reserved);
    fixture.stop().await;
    Ok(())
}

async fn live_connector(
    fixture: &Fixture,
    endpoint: &str,
    token: Option<&str>,
) -> Result<Ulid, Box<dyn std::error::Error>> {
    Ok(drive(
        CreateConnectorOperation::new(CreateConnectorInput {
            group_id: fixture.group_id,
            created_by: fixture.actor.user_id,
            name: format!("live-{}", Ulid::generate()),
            kind: RepositoryConnectorKind::Invenio,
            endpoint: endpoint.into(),
            public_config: HashMap::new(),
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
    let outcome = Box::pin(run_export_job(&ctx, spec)).await;
    complete(fixture, &ctx, &outcome).await?;
    match outcome {
        JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(result)) => result
            .repository
            .ok_or_else(|| "missing repository result".into()),
        JobRunOutcome::Failed(error) => Err(error.message.into()),
        _ => Err("unexpected export outcome".into()),
    }
}

/// Ends a succeeded job like the runtime, so it no longer counts against the job limit.
async fn complete(
    fixture: &Fixture,
    ctx: &JobContext,
    outcome: &JobRunOutcome,
) -> Result<(), Box<dyn std::error::Error>> {
    let JobRunOutcome::Succeeded(result) = outcome else {
        return Ok(());
    };
    let storage = &fixture.context.storage_handle;
    let record = aruna_operations::jobs::store::read_job_record(storage, ctx.job_id, None)
        .await?
        .ok_or("job missing")?;
    aruna_operations::jobs::store::complete_job(
        storage,
        ctx.job_id,
        ctx.claim_token,
        result.clone(),
        record.progress,
        unix_timestamp_millis(),
    )
    .await?;
    Ok(())
}

fn require_import(outcome: JobRunOutcome) -> Result<u64, Box<dyn std::error::Error>> {
    match outcome {
        JobRunOutcome::Succeeded(JobResultPayload::ImportRoCrate(result)) => Ok(result.imported),
        JobRunOutcome::Failed(error) => Err(error.message.into()),
        _ => Err("unexpected import outcome".into()),
    }
}

/// Imports a public Zenodo record in every mode and checks the files against Zenodo.
#[tokio::test]
#[ignore = "requires network access to zenodo.org"]
async fn zenodo_reference() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint =
        std::env::var("ARUNA_ZENODO_ENDPOINT").unwrap_or("https://zenodo.org/api/".into());
    let record = std::env::var("ARUNA_ZENODO_RECORD").unwrap_or("16623955".into());
    let client = reqwest::Client::builder()
        .user_agent("aruna-acceptance")
        .timeout(std::time::Duration::from_secs(120))
        .build()?;
    let get = async |url: String| -> Result<reqwest::Response, Box<dyn std::error::Error>> {
        Ok(client
            .get(url)
            .header("Accept", "application/json")
            .send()
            .await?
            .error_for_status()?)
    };
    // Zenodo itself is the reference: every version with its files and bytes.
    let versions: Value = get(format!("{endpoint}records/{record}/versions?size=25"))
        .await?
        .json()
        .await?;
    let mut expected = Vec::new();
    for version in versions["hits"]["hits"].as_array().ok_or("no versions")? {
        let id = version["id"].to_string().trim_matches('"').to_string();
        for file in version["files"].as_array().ok_or("no files")? {
            let key = file["key"].as_str().ok_or("file key missing")?.to_string();
            let url = format!("{endpoint}records/{id}/files/{key}/content");
            let bytes = get(url).await?.bytes().await?.to_vec();
            expected.push((id.clone(), key, bytes));
        }
    }
    assert!(!expected.is_empty());
    let current: Value = get(format!("{endpoint}records/{record}"))
        .await?
        .json()
        .await?;
    let doi = current["doi"]
        .as_str()
        .ok_or("record has no DOI")?
        .to_string();

    let fixture = build_fixture(false).await?;
    let connector = live_connector(&fixture, &endpoint, None).await?;
    let auth = import_spec(&fixture, Ulid::generate(), doc_id(1)).auth_context;
    let title = current["metadata"]["title"].as_str().ok_or("no title")?;
    let query = InvenioQuery {
        group_id: fixture.group_id,
        connector_id: connector,
        q: format!("\"{title}\""),
        page: 1,
        size: 10,
        all_versions: false,
    };
    let page = search_records(&fixture.context, &auth, &query, 1024 * 1024).await?;
    let hits = page["hits"]["hits"]
        .as_array()
        .ok_or("search hits missing")?;
    assert!(
        hits.iter()
            .any(|hit| hit["id"].to_string().trim_matches('"') == record)
    );

    for (index, mode) in [
        InvenioMode::Copy,
        InvenioMode::Reference,
        InvenioMode::Metadata,
    ]
    .into_iter()
    .enumerate()
    {
        let document = doc_id(index as u64 + 1);
        let mut import = spec_with_source(
            &fixture,
            ImportRoCrateSource::Invenio {
                group_id: fixture.group_id,
                connector_id: connector,
                record_id: record.clone(),
                options: InvenioOptions {
                    mode,
                    all_versions: true,
                },
                pull: None,
            },
            document,
        );
        import.target.prefix = format!("zenodo-{index}");
        let ctx = claim_context(
            &fixture,
            job_id(),
            JobPayload::ImportRoCrate(import.clone()),
        )
        .await?;
        let count = require_import(run_rocrate_import(&ctx, &import).await)?;
        assert!(count > 0, "{mode:?} imported nothing");
        replay_event_log(fixture.context.as_ref()).await?;
        process_materialization_batch(fixture.context.as_ref()).await?;
        let crate_json = aruna_operations::metadata::raw_revision::load_raw_revision(
            &fixture.context,
            document,
            None,
        )
        .await?
        .ok_or("imported crate missing")?
        .jsonld;
        assert!(crate_json.contains(&doi), "{mode:?} crate lacks {doi}");
        for (version, key, bytes) in &expected {
            let key = format!(
                "zenodo-{index}/{}",
                aruna_core::invenio::file_path(version, key)?
            );
            let object = drive(
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
            .await;
            if mode == InvenioMode::Metadata {
                assert!(object.is_err(), "metadata import stored {key}");
                continue;
            }
            let mut object = object?;
            let mut read = Vec::new();
            while let Some(chunk) = object.blob.next().await {
                read.extend_from_slice(&chunk?);
            }
            assert_eq!(&read, bytes, "{mode:?} bytes differ for {key}");
        }
    }
    fixture.stop().await;
    Ok(())
}

/// Publishes v1, imports it with keep_updated, publishes v2 and lets the daily check pull it.
#[tokio::test]
#[ignore = "requires a disposable loopback Invenio instance and personal token file"]
async fn pull_update() -> Result<(), Box<dyn std::error::Error>> {
    use aruna_core::invenio::{InvenioPull, LinkPatch, crate_versions};
    use aruna_core::structs::secondary_id::SecondaryIdKind;
    use aruna_operations::jobs::invenio::links::{LinkChange, change_link, list_links};
    use aruna_operations::jobs::invenio::pull::drain_pulls;
    let endpoint = std::env::var("ARUNA_INVENIO_ENDPOINT")?;
    let token = std::fs::read_to_string(std::env::var("ARUNA_INVENIO_TOKEN_FILE")?)?;
    let token = token.trim();
    let fixture = build_fixture(false).await?;
    let connector = live_connector(&fixture, &endpoint, Some(token)).await?;
    Box::pin(super::link::import_dataset(
        &fixture,
        native_archive().await?,
    ))
    .await?;
    let title = format!("Aruna pull {}", Ulid::generate());
    let metadata = |version: &str| {
        json!({"title": format!("{title} {version}"), "publisher": "Aruna acceptance"}).to_string()
    };
    let auth = AuthContext {
        user_id: fixture.actor.user_id,
        realm_id: fixture.actor.realm_id,
        path_restrictions: None,
        session: None,
    };
    let mut destination = InvenioDestination {
        group_id: fixture.group_id,
        connector_id: connector,
        draft_id: None,
        new_version: None,
        metadata_json: metadata("v1"),
        publish: true,
        public_files: true,
        credential: None,
        link: None,
    };
    destination.credential =
        Some(seal_credential(&fixture.context, &auth, &destination, token).await?);
    let mut spec = ExportRoCrateSpec {
        auth_context: auth.clone(),
        document_id: doc_id(1),
        limits: RoCrateLimits::default(),
        destination: Some(destination),
    };
    let first = transfer(&fixture, &spec).await?;
    assert!(first.published);

    let import = spec_with_source(
        &fixture,
        ImportRoCrateSource::Invenio {
            group_id: fixture.group_id,
            connector_id: connector,
            record_id: first.id.clone(),
            options: InvenioOptions::default(),
            pull: Some(InvenioPull::Keep {
                auto_update: true,
                owner_node_url: "https://node.example/api/v1".into(),
            }),
        },
        doc_id(2),
    );
    let ctx = claim_context(
        &fixture,
        job_id(),
        JobPayload::ImportRoCrate(import.clone()),
    )
    .await?;
    let outcome = Box::pin(run_rocrate_import(&ctx, &import)).await;
    complete(&fixture, &ctx, &outcome).await?;
    require_import(outcome)?;
    replay_event_log(fixture.context.as_ref()).await?;
    process_materialization_batch(fixture.context.as_ref()).await?;
    let storage = &fixture.context.storage_handle;
    let (link, _) = list_links(storage, doc_id(2))
        .await?
        .pop()
        .ok_or("the import kept no pull link")?;
    assert_eq!(link.remote.record_id.as_deref(), Some(first.id.as_str()));

    let target = spec.destination.as_mut().unwrap();
    target.new_version = Some(first.id.clone());
    target.metadata_json = metadata("v2");
    let second = transfer(&fixture, &spec).await?;
    assert!(second.published && second.parent_id == first.parent_id);

    // Pausing and resuming makes the check due now; the search index may lag a little.
    let job = tokio::time::timeout(std::time::Duration::from_secs(180), async {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let (current, _) = list_links(storage, doc_id(2)).await?.remove(0);
            if let Some(job) = current.active_job {
                return Ok::<_, Box<dyn std::error::Error>>(job);
            }
            for paused in [true, false] {
                let patch = LinkPatch {
                    paused: Some(paused),
                    ..LinkPatch::default()
                };
                let (current, _) = list_links(storage, doc_id(2)).await?.remove(0);
                Box::pin(change_link(
                    &fixture.context,
                    &current,
                    LinkChange::Patch(patch),
                ))
                .await?;
            }
            Box::pin(drain_pulls(&fixture.context)).await?;
        }
    })
    .await??;
    let record = aruna_operations::jobs::store::read_job_record(storage, job, None)
        .await?
        .ok_or("pull job missing")?;
    let JobPayload::ImportRoCrate(update) = record.payload.clone() else {
        return Err("pull job is not an import".into());
    };
    let ctx = claim_context(&fixture, job, record.payload).await?;
    require_import(Box::pin(run_rocrate_import(&ctx, &update)).await)?;
    replay_event_log(fixture.context.as_ref()).await?;
    process_materialization_batch(fixture.context.as_ref()).await?;

    let (pulled, _) = list_links(storage, doc_id(2)).await?.remove(0);
    assert_eq!(pulled.active_job, None);
    assert_eq!(pulled.remote.record_id.as_deref(), Some(second.id.as_str()));
    assert_eq!(pulled.remote.doi, second.doi);
    assert_eq!(pulled.pull_reason(), None);
    let revision = aruna_operations::metadata::raw_revision::load_raw_revision(
        &fixture.context,
        doc_id(2),
        None,
    )
    .await?
    .ok_or("revision missing")?;
    let document: Value = serde_json::from_str(&revision.jsonld)?;
    let mut versions = crate_versions(&document);
    versions.sort();
    let mut expected = vec![first.id.clone(), second.id.clone()];
    expected.sort();
    assert_eq!(versions, expected);
    assert!(revision.jsonld.contains(&format!("{title} v2")));
    Box::pin(super::link::run_registration(
        &fixture,
        job,
        update.auth_context.user_id,
    ))
    .await?;
    let mapping =
        aruna_operations::metadata::persistent_id::read_mapping(&fixture.context, doc_id(2))
            .await?
            .ok_or("pulled dataset has no mapping")?;
    let doi = second.doi.as_deref().ok_or("v2 has no DOI")?;
    assert!(
        mapping
            .secondary_identifiers
            .iter()
            .any(|id| id.kind == SecondaryIdKind::Doi && id.value.eq_ignore_ascii_case(doi))
    );
    fixture.stop().await;
    Ok(())
}
