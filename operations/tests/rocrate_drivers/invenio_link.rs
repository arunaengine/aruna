//! Pushes linked datasets to a stateful Invenio fixture: drafts, versions and failures.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::remote::{RemoteServer, remote};
use super::*;
use aruna_core::invenio::{
    InvenioLink, LinkFailure, LinkPatch, LinkQueueEntry, LinkRemote, LinkStatus, link_key,
};
use aruna_core::keyspaces::{INVENIO_LINK_KEYSPACE, LINK_QUEUE_KEYSPACE};
use aruna_core::structs::secondary_id::SecondaryIdKind;
use aruna_operations::jobs::invenio::link_queue::drain_links;
use aruna_operations::jobs::invenio::links::{ChangeLinkOperation, LinkChange, list_links};
use aruna_operations::jobs::invenio::seal_link_token;
use aruna_operations::jobs::service::submit_export_job;
use aruna_operations::jobs::store::{complete_job, fail_job};
use aruna_operations::jobs::submit::SubmitJobError;
use aruna_operations::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentOperation, CreateDocumentPayload,
};
use aruna_operations::metadata::raw_revision::load_raw_revision;
use aruna_operations::metadata::update_document::{
    UpdateDocumentConfig, UpdateDocumentMutation, UpdateDocumentOperation,
};

const LINK_TOKEN: &str = "link-token";

pub(super) async fn linked(
    fixture: &Fixture,
    endpoint: &str,
    token: &str,
    auto_publish: bool,
    parent_id: Option<&str>,
) -> Result<InvenioLink, Box<dyn std::error::Error>> {
    let upload = create_upload(fixture, native_archive().await?).await?;
    let import = import_spec(fixture, upload, doc_id(1));
    let ctx = claim_context(fixture, job_id(), JobPayload::ImportRoCrate(import.clone())).await?;
    assert!(matches!(
        run_rocrate_import(&ctx, &import).await,
        JobRunOutcome::Succeeded(_)
    ));
    replay_event_log(fixture.context.as_ref()).await?;
    process_materialization_batch(fixture.context.as_ref()).await?;
    Box::pin(attach(fixture, endpoint, token, auto_publish, parent_id)).await
}

/// Links the existing dataset `doc_id(1)` to a new connector of `endpoint`.
async fn attach(
    fixture: &Fixture,
    endpoint: &str,
    token: &str,
    auto_publish: bool,
    parent_id: Option<&str>,
) -> Result<InvenioLink, Box<dyn std::error::Error>> {
    let connector_id = drive(
        CreateConnectorOperation::new(CreateConnectorInput {
            group_id: fixture.group_id,
            created_by: fixture.actor.user_id,
            name: "linked".into(),
            kind: RepositoryConnectorKind::Invenio,
            endpoint: endpoint.into(),
            public_config: HashMap::new(),
            secret_config: HashMap::new(),
        }),
        &fixture.context,
    )
    .await?
    .connector
    .connector_id;
    let link_id = Ulid::generate();
    let secret = seal_link_token(
        &fixture.context,
        fixture.actor.user_id,
        fixture.group_id,
        connector_id,
        link_id,
        token,
    )
    .await?;
    let now = SystemTime::now();
    let link = InvenioLink {
        link_id,
        document_id: doc_id(1),
        group_id: fixture.group_id,
        connector_id,
        endpoint: secret.endpoint.clone(),
        owner_node: fixture.actor.node_id,
        owner_node_url: "http://127.0.0.1/api/v1".into(),
        created_by: fixture.actor.user_id,
        status: LinkStatus::Enabled,
        auto_publish,
        public_files: false,
        metadata_json: "{}".into(),
        remote: LinkRemote {
            parent_id: parent_id.map(str::to_string),
            ..LinkRemote::default()
        },
        last_push: None,
        active_job: None,
        sequence: 0,
        limits: RoCrateLimits::default(),
        created_at: now,
        updated_at: now,
        generation: 0,
    };
    let change = LinkChange::Create {
        link: Box::new(link),
        secret,
    };
    Ok(drive(
        ChangeLinkOperation::new(doc_id(1), link_id, change),
        &fixture.context,
    )
    .await?
    .ok_or("created link missing")?)
}

pub(super) async fn current(fixture: &Fixture, link: &InvenioLink) -> (InvenioLink, bool) {
    list_links(&fixture.context.storage_handle, link.document_id)
        .await
        .unwrap()
        .into_iter()
        .find(|(stored, _)| stored.link_id == link.link_id)
        .unwrap()
}

/// Makes the queued check due now instead of waiting out the debounce.
pub(super) async fn due_now(
    fixture: &Fixture,
    link: &InvenioLink,
) -> Result<(), Box<dyn std::error::Error>> {
    let entry = LinkQueueEntry {
        document_id: link.document_id,
        due_at_ms: 0,
    };
    write_value(
        &fixture.context.storage_handle,
        LINK_QUEUE_KEYSPACE,
        link.link_id.to_bytes().to_vec(),
        postcard::to_allocvec(&entry)?,
    )
    .await
}

/// Runs the push job the link recorded, as the job runtime would.
pub(super) async fn run_push(
    fixture: &Fixture,
    link: &InvenioLink,
) -> Result<JobRunOutcome, Box<dyn std::error::Error>> {
    let job_id = current(fixture, link)
        .await
        .0
        .active_job
        .ok_or("no running push")?;
    let record = aruna_operations::jobs::store::read_job_record(
        &fixture.context.storage_handle,
        job_id,
        None,
    )
    .await?
    .ok_or("push job missing")?;
    let JobPayload::ExportRoCrate(spec) = record.payload.clone() else {
        return Err("push job is not an export".into());
    };
    let ctx = claim_context(fixture, job_id, record.payload).await?;
    let outcome = Box::pin(run_export_job(&ctx, &spec)).await;
    // Ends the job like the runtime, so the per-user active job limit frees up.
    let storage = &fixture.context.storage_handle;
    let now = unix_timestamp_millis();
    match &outcome {
        JobRunOutcome::Succeeded(result) => {
            let progress = record.progress.clone();
            complete_job(
                storage,
                job_id,
                ctx.claim_token,
                result.clone(),
                progress,
                now,
            )
            .await?;
        }
        JobRunOutcome::Failed(error) => {
            fail_job(storage, job_id, ctx.claim_token, error.clone(), now).await?;
        }
        _ => {}
    }
    Ok(outcome)
}

/// Replaces the dataset description and, optionally, drops `empty.txt`.
pub(super) async fn change(
    fixture: &Fixture,
    text: &str,
    drop_empty: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let revision = load_raw_revision(&fixture.context, doc_id(1), None)
        .await?
        .ok_or("revision missing")?;
    let mut document: Value = serde_json::from_str(&revision.jsonld)?;
    let graph = document["@graph"].as_array_mut().ok_or("graph missing")?;
    let empty = |entity: &Value| {
        entity["@id"]
            .as_str()
            .is_some_and(|id| id.contains("/empty.txt@"))
    };
    for entity in graph.iter_mut() {
        if entity["@type"] == "Dataset" {
            entity["description"] = json!(text);
            if drop_empty && let Some(parts) = entity["hasPart"].as_array_mut() {
                parts.retain(|part| !empty(part));
            }
        }
    }
    if drop_empty {
        graph.retain(|entity| !empty(entity));
    }
    replace_crate(fixture, &document).await
}

/// Stores `document` as the dataset's new crate and materializes it.
async fn replace_crate(
    fixture: &Fixture,
    document: &Value,
) -> Result<(), Box<dyn std::error::Error>> {
    drive(
        UpdateDocumentOperation::new(UpdateDocumentConfig {
            actor: fixture.actor.clone(),
            group_id: fixture.group_id,
            document_id: doc_id(1),
            public: false,
            mutation: UpdateDocumentMutation::ReplaceRoCrate {
                jsonld: document.to_string(),
            },
        }),
        &fixture.context,
    )
    .await?;
    replay_event_log(fixture.context.as_ref()).await?;
    process_materialization_batch(fixture.context.as_ref()).await?;
    Ok(())
}

pub(super) async fn drain(fixture: &Fixture) -> Result<(), Box<dyn std::error::Error>> {
    Box::pin(drain_links(&fixture.context)).await?;
    Ok(())
}

pub(super) fn succeeded(outcome: JobRunOutcome) {
    match outcome {
        JobRunOutcome::Succeeded(_) => {}
        JobRunOutcome::Failed(error) => panic!("push failed: {}", error.message),
        _ => panic!("unexpected push outcome"),
    }
}

fn keys(server: &RemoteServer, id: &str) -> Vec<String> {
    server.state.lock().unwrap().records[id]
        .files
        .keys()
        .cloned()
        .collect()
}

#[tokio::test]
async fn link_follows_lineage() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    let link = Box::pin(linked(&fixture, &server.endpoint, LINK_TOKEN, false, None)).await?;
    assert!(current(&fixture, &link).await.1);

    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    let (first, queued) = current(&fixture, &link).await;
    assert!(!queued && first.active_job.is_none());
    assert_eq!(first.remote.draft_id.as_deref(), Some("1"));
    assert_eq!(first.remote.parent_id.as_deref(), Some("p1"));
    assert!(!first.remote.published);
    assert!(
        first
            .remote
            .record_url
            .as_deref()
            .is_some_and(|url| url.ends_with("/records/1"))
    );
    assert_eq!(
        keys(&server, "1"),
        ["empty.txt", "nested/data.txt", "ro-crate-metadata.json"]
    );

    // An unchanged dataset starts no job.
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    assert_eq!(current(&fixture, &link).await, (first.clone(), false));

    Box::pin(change(&fixture, "Second revision", true)).await?;
    assert!(
        current(&fixture, &link).await.1,
        "the change queued a push check"
    );
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    let (second, _) = current(&fixture, &link).await;
    assert_eq!(second.remote.draft_id.as_deref(), Some("1"));
    assert_ne!(second.last_push, first.last_push);
    assert_eq!(
        keys(&server, "1"),
        ["nested/data.txt", "ro-crate-metadata.json"]
    );
    assert_eq!(server.state.lock().unwrap().records.len(), 1);
    assert!(
        server
            .state
            .lock()
            .unwrap()
            .calls
            .iter()
            .any(|(method, path)| *method == Method::DELETE && path.ends_with("empty.txt"))
    );

    let event = load_raw_revision(&fixture.context, doc_id(1), None)
        .await?
        .ok_or("revision missing")?
        .winning_event_id;
    Box::pin(aruna_operations::jobs::invenio::link_queue::start_push(
        &fixture.context,
        &second,
        event,
        true,
    ))
    .await?;
    succeeded(run_push(&fixture, &link).await?);
    let (published, _) = current(&fixture, &link).await;
    assert!(published.remote.published && published.remote.draft_id.is_none());
    assert_eq!(published.remote.record_id.as_deref(), Some("1"));
    assert_eq!(published.remote.doi.as_deref(), Some("10.1234/1"));
    let found = aruna_operations::metadata::secondary_ids::lookup_identifier(
        &fixture.context,
        fixture.actor.realm_id,
        None,
        SecondaryIdKind::Doi,
        "10.1234/1",
        None,
    )
    .await;
    assert!(found.is_ok());

    Box::pin(change(&fixture, "Third revision", false)).await?;
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    let (version, _) = current(&fixture, &link).await;
    assert_eq!(version.remote.draft_id.as_deref(), Some("2"));
    assert_eq!(version.remote.record_id.as_deref(), Some("1"));
    assert_eq!(version.remote.parent_id.as_deref(), Some("p1"));
    assert_eq!(server.state.lock().unwrap().records["2"].parent, "p1");

    // Publishing the draft outside Aruna moves the lineage on without the link.
    server
        .state
        .lock()
        .unwrap()
        .records
        .get_mut("2")
        .unwrap()
        .published = true;
    Box::pin(change(&fixture, "Fourth revision", false)).await?;
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    assert!(matches!(
        run_push(&fixture, &link).await?,
        JobRunOutcome::Failed(_)
    ));
    let (failed, _) = current(&fixture, &link).await;
    assert_eq!(
        failed.status,
        LinkStatus::Failed {
            reason: LinkFailure::RemoteChanged
        }
    );
    assert!(failed.active_job.is_none());
    Box::pin(change(&fixture, "Fifth revision", false)).await?;
    assert!(
        !current(&fixture, &link).await.1,
        "a failed link queues nothing"
    );
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn token_rejection_recovers() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote("other-token").await;
    let link = Box::pin(linked(&fixture, &server.endpoint, LINK_TOKEN, true, None)).await?;
    drain(&fixture).await?;
    assert!(matches!(
        run_push(&fixture, &link).await?,
        JobRunOutcome::Failed(_)
    ));
    let (failed, _) = current(&fixture, &link).await;
    assert_eq!(
        failed.status,
        LinkStatus::Failed {
            reason: LinkFailure::TokenRejected
        }
    );
    assert!(server.state.lock().unwrap().records.is_empty());

    server.state.lock().unwrap().token = "rotated-token".into();
    let secret = seal_link_token(
        &fixture.context,
        link.created_by,
        link.group_id,
        link.connector_id,
        link.link_id,
        "rotated-token",
    )
    .await?;
    drive(
        ChangeLinkOperation::new(link.document_id, link.link_id, LinkChange::Rotate(secret)),
        &fixture.context,
    )
    .await?;
    let (rotated, queued) = current(&fixture, &link).await;
    assert!(queued && rotated.status == LinkStatus::Enabled);
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    let (published, _) = current(&fixture, &link).await;
    assert!(
        published.remote.published,
        "auto publish published the push"
    );
    assert_eq!(published.remote.record_id.as_deref(), Some("1"));
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn queued_push_recovers() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    {
        let mut state = server.state.lock().unwrap();
        let source = state.insert(None, None);
        state.records.get_mut(&source).unwrap().published = true;
    }
    // The link continues the lineage of an existing, already published record.
    let link = Box::pin(linked(
        &fixture,
        &server.endpoint,
        LINK_TOKEN,
        false,
        Some("p1"),
    ))
    .await?;
    // A drain that submitted the job but stopped before recording it leaves the check queued.
    let event = load_raw_revision(&fixture.context, doc_id(1), None)
        .await?
        .ok_or("revision missing")?
        .winning_event_id;
    let spec = ExportRoCrateSpec {
        destination: Some(link.destination(false)),
        auth_context: AuthContext {
            user_id: link.created_by,
            realm_id: link.created_by.realm_id,
            path_restrictions: None,
            session: None,
        },
        document_id: link.document_id,
        limits: link.limits.clone(),
    };
    let orphan = submit_export_job(
        &fixture.context,
        spec,
        fixture.actor.node_id,
        Some(link.push_key(event, false)),
    )
    .await?;
    let stored = fixture
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: INVENIO_LINK_KEYSPACE.into(),
            key: link_key(link.document_id, link.link_id).into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        stored,
        Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
    ));
    assert!(current(&fixture, &link).await.1);
    drain(&fixture).await?;
    let (started, queued) = current(&fixture, &link).await;
    assert_eq!(started.active_job, Some(orphan.job_id));
    assert!(!queued);
    succeeded(run_push(&fixture, &link).await?);
    let (pushed, _) = current(&fixture, &link).await;
    assert_eq!(pushed.remote.draft_id.as_deref(), Some("2"));
    assert_eq!(server.state.lock().unwrap().records["2"].parent, "p1");
    assert_eq!(server.state.lock().unwrap().records.len(), 2);
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn full_job_slots_wait() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    let link = Box::pin(linked(&fixture, &server.endpoint, LINK_TOKEN, false, None)).await?;
    // Other exports of the creator take every active job slot.
    let spec = ExportRoCrateSpec {
        destination: None,
        auth_context: AuthContext {
            user_id: link.created_by,
            realm_id: link.created_by.realm_id,
            path_restrictions: None,
            session: None,
        },
        document_id: link.document_id,
        limits: link.limits.clone(),
    };
    let mut fillers = Vec::new();
    for slot in 0..link.limits.max_active_jobs {
        let key = Some(format!("filler-{slot}"));
        let job = submit_export_job(&fixture.context, spec.clone(), fixture.actor.node_id, key);
        match job.await {
            Ok(job) => fillers.push(job.job_id),
            Err(SubmitJobError::ActiveJobLimit { .. }) => break,
            Err(error) => return Err(error.into()),
        }
    }
    drain(&fixture).await?;
    let (waiting, queued) = current(&fixture, &link).await;
    assert_eq!(
        waiting.status,
        LinkStatus::Enabled,
        "a full job queue is no failure"
    );
    assert!(
        queued && waiting.active_job.is_none(),
        "the push stays pending"
    );

    let filler = fillers[0];
    let payload = JobPayload::ExportRoCrate(spec);
    let ctx = claim_context(&fixture, filler, payload).await?;
    let error = aruna_core::structs::execution::job::JobError::retryable("freed".to_string());
    let now = unix_timestamp_millis();
    fail_job(
        &fixture.context.storage_handle,
        filler,
        ctx.claim_token,
        error,
        now,
    )
    .await?;
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    let (started, queued) = current(&fixture, &link).await;
    assert!(
        started.active_job.is_some() && !queued,
        "the push starts once a slot frees up"
    );
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn lost_holder_fails() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    let link = Box::pin(linked(&fixture, &server.endpoint, LINK_TOKEN, false, None)).await?;
    // The placement moves the dataset to another node; this node keeps only its old link row.
    let other = iroh::SecretKey::from_bytes(&[42; 32]).public();
    let mut config = RealmConfigDocument::new(fixture.actor.realm_id, Vec::new(), 3);
    config.seed_default_placement();
    config.ensure_node(other, RealmNodeKind::Server);
    config.seed_job_control(other, 0);
    write_value(
        &fixture.context.storage_handle,
        REALM_CONFIG_KEYSPACE,
        fixture.actor.realm_id.as_bytes().to_vec(),
        config.to_bytes(&fixture.actor)?,
    )
    .await?;
    assert!(
        !aruna_operations::jobs::invenio::link_queue::owner_holds(&fixture.context, &link).await
    );

    drain(&fixture).await?;
    let (failed, queued) = current(&fixture, &link).await;
    assert_eq!(
        failed.status,
        LinkStatus::Failed {
            reason: LinkFailure::OwnerNotHolder
        }
    );
    assert!(!queued && failed.active_job.is_none());
    assert!(
        server.state.lock().unwrap().calls.is_empty(),
        "nothing was pushed"
    );
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn scaffold_link_pushes() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    drive(
        CreateDocumentOperation::new(CreateDocumentConfig {
            actor: fixture.actor.clone(),
            group_id: fixture.group_id,
            document_id: doc_id(1),
            document_path: "datasets/scaffold".into(),
            public: false,
            payload: CreateDocumentPayload::Scaffold {
                name: "Scaffold".into(),
                description: "Created from fields".into(),
                date_published: "2026-01-01".into(),
                license: Some("https://creativecommons.org/licenses/by/4.0/".into()),
            },
        }),
        &fixture.context,
    )
    .await?;
    replay_event_log(fixture.context.as_ref()).await?;
    process_materialization_batch(fixture.context.as_ref()).await?;
    let raw = load_raw_revision(&fixture.context, doc_id(1), None).await?;
    assert!(raw.is_none(), "a scaffold keeps no raw revision");

    let link = Box::pin(attach(&fixture, &server.endpoint, LINK_TOKEN, false, None)).await?;
    // Scaffold fields name no creator, so the link supplies one as a native override.
    let creators = json!({"creators": [{"person_or_org": {
        "type": "personal", "given_name": "Ada", "family_name": "Lovelace"}}]});
    let patch = LinkPatch {
        metadata_json: Some(creators.to_string()),
        ..LinkPatch::default()
    };
    let change = ChangeLinkOperation::new(link.document_id, link.link_id, LinkChange::Patch(patch));
    drive(change, &fixture.context).await?;
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    let (pushed, _) = current(&fixture, &link).await;
    assert_eq!(pushed.remote.draft_id.as_deref(), Some("1"));
    assert_eq!(keys(&server, "1"), ["ro-crate-metadata.json"]);
    // The pushed revision counts as current, so an unchanged scaffold starts no second push.
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    assert_eq!(current(&fixture, &link).await, (pushed, false));
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn admins_read_pushes() -> Result<(), Box<dyn std::error::Error>> {
    use aruna_operations::jobs::service::{JobReportLookup, read_job_routed, read_report_routed};

    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    let mut link = Box::pin(linked(&fixture, &server.endpoint, LINK_TOKEN, false, None)).await?;
    // Another member created the link, so its pushes run as that member.
    let realm_id = fixture.actor.realm_id;
    link.created_by = UserId::local(Ulid::generate(), realm_id);
    write_value(
        &fixture.context.storage_handle,
        INVENIO_LINK_KEYSPACE,
        link_key(link.document_id, link.link_id),
        link.to_bytes()?,
    )
    .await?;
    drain(&fixture).await?;
    let job_id = current(&fixture, &link)
        .await
        .0
        .active_job
        .ok_or("push missing")?;
    let auth = |user_id| AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };

    // The fixture actor administers the link's group.
    let admin = auth(fixture.actor.user_id);
    let status = read_job_routed(&fixture.context, &admin, job_id, None).await?;
    assert_eq!(status.job.job_id, job_id);
    let report = read_report_routed(&fixture.context, &admin, job_id, None, None, 10, None).await?;
    assert!(matches!(report, JobReportLookup::Pending(_)));

    let stranger = auth(UserId::local(Ulid::generate(), realm_id));
    let hidden = read_job_routed(&fixture.context, &stranger, job_id, None).await;
    assert!(
        hidden.is_err(),
        "a non-admin cannot read another member's push"
    );
    let report = read_report_routed(&fixture.context, &stranger, job_id, None, None, 10, None);
    assert!(matches!(report.await?, JobReportLookup::NotFound));
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn missing_file_fails() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    let link = Box::pin(linked(&fixture, &server.endpoint, LINK_TOKEN, false, None)).await?;
    // The crate names a stored object that does not exist in this realm.
    let revision = load_raw_revision(&fixture.context, doc_id(1), None)
        .await?
        .ok_or("revision missing")?;
    let document: Value = serde_json::from_str(&revision.jsonld)?;
    let entity = document["@graph"]
        .as_array()
        .ok_or("graph missing")?
        .iter()
        .find(|entity| {
            entity["@id"]
                .as_str()
                .is_some_and(|id| id.contains("/empty.txt@"))
        })
        .ok_or("file entity missing")?;
    // Neither the key nor the content hashes resolve, so no candidate holds the bytes.
    let content = entity["contentUrl"].as_str().ok_or("content url missing")?;
    let hash = content.rsplit('/').next().ok_or("hash missing")?;
    let present = entity["@id"].as_str().ok_or("id missing")?;
    let arn_hash = present.split(':').nth(4).ok_or("arn hash missing")?;
    let unknown = hex::encode([0x11; 32]);
    let gone = present
        .replace(arn_hash, &unknown)
        .replace("/empty.txt@", "/gone.txt@");
    let jsonld = revision
        .jsonld
        .replace(present, &gone)
        .replace(hash, &unknown);
    let document: Value = serde_json::from_str(&jsonld)?;
    Box::pin(replace_crate(&fixture, &document)).await?;

    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    let outcome = run_push(&fixture, &link).await?;
    assert!(matches!(outcome, JobRunOutcome::Failed(_)));
    let (failed, _) = current(&fixture, &link).await;
    assert_eq!(
        failed.status,
        LinkStatus::Failed {
            reason: LinkFailure::SourceUnavailable
        }
    );
    let state = server.state.lock().unwrap();
    assert!(state.records.is_empty(), "no partial record was pushed");
    drop(state);
    fixture.stop().await;
    Ok(())
}
