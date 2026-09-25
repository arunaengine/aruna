//! Pushes linked datasets to a stateful Invenio fixture: drafts, versions and failures.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::remote::{RemoteServer, remote};
use super::*;
use aruna_core::keyspaces::{LINK_QUEUE_KEYSPACE, LINK_SECRET_KEYSPACE, REPOSITORY_LINK_KEYSPACE};
use aruna_core::repository::{
    LinkFailure, LinkPatch, LinkQueueEntry, LinkRemote, LinkStatus, RepositoryLink, link_key,
};
use aruna_core::structs::secondary_id::{IdentifierOrigin, SecondaryIdKind};
use aruna_operations::jobs::repository::link_queue::drain_links;
use aruna_operations::jobs::repository::links::{ChangeLinkOperation, LinkChange, list_links};
use aruna_operations::jobs::repository::seal_link_token;
use aruna_operations::jobs::service::submit_export_job;
use aruna_operations::jobs::store::{complete_job, fail_job};
use aruna_operations::jobs::submit::SubmitJobError;
use aruna_operations::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentOperation, CreateDocumentPayload,
};
use aruna_operations::metadata::delete_document::DeleteDocumentOperation;
use aruna_operations::metadata::prune_queue::process_prune_batch;
use aruna_operations::metadata::raw_revision::load_raw_revision;
use aruna_operations::metadata::update_document::{
    UpdateDocumentConfig, UpdateDocumentMutation, UpdateDocumentOperation,
};

pub(super) const LINK_TOKEN: &str = "link-token";

pub(super) async fn linked(
    fixture: &Fixture,
    endpoint: &str,
    token: &str,
    auto_publish: bool,
    parent_id: Option<&str>,
) -> Result<RepositoryLink, Box<dyn std::error::Error>> {
    Box::pin(import_dataset(fixture, native_archive().await?)).await?;
    Box::pin(attach(
        fixture,
        endpoint,
        token,
        auto_publish,
        parent_id,
        None,
    ))
    .await
}

/// Imports `archive` as the dataset `doc_id(1)`.
pub(super) async fn import_dataset(
    fixture: &Fixture,
    archive: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let upload = create_upload(fixture, archive).await?;
    let import = import_spec(fixture, upload, doc_id(1));
    let ctx = claim_context(fixture, job_id(), JobPayload::ImportRoCrate(import.clone())).await?;
    match run_rocrate_import(&ctx, &import).await {
        JobRunOutcome::Succeeded(_) => {}
        JobRunOutcome::Failed(error) => return Err(error.message.into()),
        _ => return Err("import did not finish".into()),
    }
    replay_event_log(fixture.context.as_ref()).await?;
    process_materialization_batch(fixture.context.as_ref()).await?;
    Ok(())
}

/// Links the existing dataset `doc_id(1)` to a new connector of `endpoint`.
pub(super) async fn attach(
    fixture: &Fixture,
    endpoint: &str,
    token: &str,
    auto_publish: bool,
    parent_id: Option<&str>,
    community: Option<&str>,
) -> Result<RepositoryLink, Box<dyn std::error::Error>> {
    let connector_id = drive(
        CreateConnectorOperation::new(CreateConnectorInput {
            group_id: fixture.group_id,
            created_by: fixture.actor.user_id,
            name: "linked".into(),
            kind: RepositoryConnectorKind::Invenio,
            endpoint: endpoint.into(),
            public_config: community
                .map(|community| HashMap::from([("community".into(), community.into())]))
                .unwrap_or_default(),
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
    let link = RepositoryLink {
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
        warning: None,
        direction: aruna_core::repository::LinkDirection::Push,
        kind: aruna_core::structs::execution::harvest::RepositoryConnectorKind::Invenio,
    };
    let change = LinkChange::Create {
        link: Box::new(link),
        secret: Some(secret),
    };
    Ok(drive(
        ChangeLinkOperation::new(doc_id(1), link_id, change, SystemTime::now()),
        &fixture.context,
    )
    .await?
    .ok_or("created link missing")?)
}

pub(super) async fn current(fixture: &Fixture, link: &RepositoryLink) -> (RepositoryLink, bool) {
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
    link: &RepositoryLink,
) -> Result<(), Box<dyn std::error::Error>> {
    let entry = LinkQueueEntry {
        document_id: link.document_id,
        due_at_ms: 0,
        first_at_ms: 0,
    };
    write_value(
        &fixture.context.storage_handle,
        LINK_QUEUE_KEYSPACE,
        link.link_id.to_bytes().to_vec(),
        postcard::to_allocvec(&entry)?,
    )
    .await
}

/// Moves the last push back past the auto_publish quiet time and makes the check due.
pub(super) async fn quiet_draft(
    fixture: &Fixture,
    link: &RepositoryLink,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stored = current(fixture, link).await.0;
    let push = stored.last_push.as_mut().ok_or("nothing pushed yet")?;
    push.pushed_at = SystemTime::UNIX_EPOCH;
    write_value(
        &fixture.context.storage_handle,
        REPOSITORY_LINK_KEYSPACE,
        link_key(link.document_id, link.link_id),
        stored.to_bytes()?,
    )
    .await?;
    due_now(fixture, link).await
}

/// Runs the push job the link recorded, as the job runtime would.
pub(super) async fn run_push(
    fixture: &Fixture,
    link: &RepositoryLink,
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
pub(super) async fn replace_crate(
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
            expected_revision: None,
        }),
        &fixture.context,
    )
    .await?;
    replay_event_log(fixture.context.as_ref()).await?;
    process_materialization_batch(fixture.context.as_ref()).await?;
    Ok(())
}

/// Runs the identifier registration a job queued, as the job runtime would.
/// Returns the caller the registration acts for.
pub(super) async fn run_registration(
    fixture: &Fixture,
    for_job: JobId,
    created_by: UserId,
) -> Result<AuthContext, Box<dyn std::error::Error>> {
    let storage = &fixture.context.storage_handle;
    let key = format!("identifiers/{for_job}");
    let (job_id, _) =
        aruna_operations::jobs::store::find_dedup_plan(storage, created_by, key.as_bytes(), None)
            .await?
            .ok_or("no registration queued")?;
    let record = aruna_operations::jobs::store::read_job_record(storage, job_id, None)
        .await?
        .ok_or("registration job missing")?;
    let JobPayload::RegisterIdentifiers(spec) = record.payload.clone() else {
        return Err("queued job is not a registration".into());
    };
    let ctx = claim_context(fixture, job_id, record.payload).await?;
    succeeded(
        Box::pin(aruna_operations::jobs::persistent_id::run_register_identifiers(&ctx, &spec))
            .await,
    );
    Ok(spec.auth_context)
}

pub(super) async fn drain(fixture: &Fixture) -> Result<(), Box<dyn std::error::Error>> {
    Box::pin(drain_links(&fixture.context)).await?;
    Ok(())
}

pub(super) fn succeeded(outcome: JobRunOutcome) {
    match outcome {
        JobRunOutcome::Succeeded(_) => {}
        JobRunOutcome::Failed(error) => panic!("push failed: {}", error.message),
        JobRunOutcome::Deferred(error) => panic!("job deferred: {}", error.message),
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
    Box::pin(aruna_operations::jobs::repository::link_queue::start_push(
        &fixture.context,
        &second,
        event,
        true,
    ))
    .await?;
    let push_job = current(&fixture, &link)
        .await
        .0
        .active_job
        .ok_or("no push")?;
    let pusher = aruna_operations::jobs::store::read_job_record(
        &fixture.context.storage_handle,
        push_job,
        None,
    )
    .await?
    .ok_or("push job missing")?
    .created_by;
    succeeded(run_push(&fixture, &link).await?);
    let (published, _) = current(&fixture, &link).await;
    assert!(published.remote.published && published.remote.draft_id.is_none());
    assert_eq!(published.remote.record_id.as_deref(), Some("1"));
    assert_eq!(published.remote.identifier.as_deref(), Some("10.1234/1"));
    let auth = Box::pin(run_registration(&fixture, push_job, pusher)).await?;
    let found = Box::pin(aruna_operations::metadata::secondary_ids::lookup_local(
        &fixture.context,
        fixture.actor.realm_id,
        Some(&auth),
        SecondaryIdKind::Doi,
        "10.1234/1",
        None,
    ))
    .await?;
    assert_eq!(
        found
            .iter()
            .map(|found| (found.document_id, found.origin))
            .collect::<Vec<_>>(),
        [(doc_id(1), IdentifierOrigin::Published)],
        "the push queued its DOI as a published identifier"
    );

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
        ChangeLinkOperation::new(
            link.document_id,
            link.link_id,
            LinkChange::Rotate(secret),
            SystemTime::now(),
        ),
        &fixture.context,
    )
    .await?;
    let (rotated, queued) = current(&fixture, &link).await;
    assert!(queued && rotated.status == LinkStatus::Enabled);
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    let (pushed, queued) = current(&fixture, &link).await;
    assert!(
        !pushed.remote.published,
        "auto publish waits for a quiet draft"
    );
    assert!(queued && pushed.active_job.is_none());
    assert_eq!(pushed.remote.identifier.as_deref(), Some("10.1234/1"));
    assert!(pushed.remote.identifier_reserved);
    drain(&fixture).await?;
    assert!(current(&fixture, &link).await.0.active_job.is_none());

    quiet_draft(&fixture, &link).await?;
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    let (published, _) = current(&fixture, &link).await;
    assert!(
        published.remote.published,
        "auto publish published the quiet draft"
    );
    assert_eq!(published.remote.record_id.as_deref(), Some("1"));
    assert_eq!(published.remote.identifier.as_deref(), Some("10.1234/1"));
    assert!(!published.remote.identifier_reserved);
    assert_eq!(
        published.remote.concept_identifier.as_deref(),
        Some("10.1234/p1")
    );
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
            key_space: REPOSITORY_LINK_KEYSPACE.into(),
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
        !aruna_operations::jobs::repository::link_queue::owner_holds(&fixture.context, &link).await
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
async fn scaffold_link_checked() -> Result<(), Box<dyn std::error::Error>> {
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

    let link = Box::pin(attach(
        &fixture,
        &server.endpoint,
        LINK_TOKEN,
        false,
        None,
        None,
    ))
    .await?;
    // Scaffold fields name no creator or publisher, and link overrides do not supply them.
    let creators = json!({"publisher": "Aruna test", "creators": [{"person_or_org": {
        "type": "personal", "given_name": "Ada", "family_name": "Lovelace"}}]});
    let patch = LinkPatch {
        metadata_json: Some(creators.to_string()),
        ..LinkPatch::default()
    };
    let change = ChangeLinkOperation::new(
        link.document_id,
        link.link_id,
        LinkChange::Patch(patch),
        SystemTime::now(),
    );
    drive(change, &fixture.context).await?;
    drain(&fixture).await?;
    assert!(matches!(
        run_push(&fixture, &link).await?,
        JobRunOutcome::Failed(_)
    ));
    // The findings come from the scaffold's rendered crate, which the push read.
    let (failed, _) = current(&fixture, &link).await;
    let LinkStatus::Failed {
        reason: LinkFailure::RequirementsUnmet(findings),
    } = &failed.status
    else {
        panic!("expected unmet requirements, got {:?}", failed.status);
    };
    let paths = findings
        .iter()
        .filter_map(|finding| finding.path.as_deref())
        .collect::<Vec<_>>();
    assert!(paths.contains(&"http://schema.org/publisher"), "{paths:?}");
    assert!(server.state.lock().unwrap().records.is_empty());
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
        REPOSITORY_LINK_KEYSPACE,
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

    // A group admin who cannot read the dataset does not see its push.
    let manager = UserId::local(Ulid::generate(), realm_id);
    let grant = |read_meta: bool| {
        let mut group = GroupAuthorizationDocument::default_group_doc(
            fixture.actor.user_id,
            realm_id,
            fixture.group_id,
        );
        let role_id = Ulid::generate();
        let mut permissions = HashMap::from([(
            format!("/{realm_id}/g/{}/admin", fixture.group_id),
            Permission::WRITE,
        )]);
        if read_meta {
            permissions.insert(
                format!("/{realm_id}/g/{}/meta/**", fixture.group_id),
                Permission::READ,
            );
        }
        let role = aruna_core::structs::identity::auth::Role {
            role_id,
            name: "link-manager".to_string(),
            permissions,
            assigned_users: HashSet::from([manager]),
        };
        group.roles.insert(role_id, role);
        group.to_bytes(&fixture.actor)
    };
    let group_key = fixture.group_id.to_bytes().to_vec();
    let storage = &fixture.context.storage_handle;
    write_value(storage, AUTH_KEYSPACE, group_key.clone(), grant(false)?).await?;
    let blind = read_job_routed(&fixture.context, &auth(manager), job_id, None).await;
    assert!(
        blind.is_err(),
        "an admin without dataset READ must not read the push"
    );
    write_value(storage, AUTH_KEYSPACE, group_key, grant(true)?).await?;
    let status = read_job_routed(&fixture.context, &auth(manager), job_id, None).await?;
    assert_eq!(status.job.job_id, job_id);
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
    let pushed = server.state.lock().unwrap().records.len();
    assert_eq!(pushed, 0, "no partial record was pushed");
    fixture.stop().await;
    Ok(())
}

/// Whether `key_space` still holds a row keyed by the link id.
async fn has_row(fixture: &Fixture, key_space: &str, link: &RepositoryLink) -> bool {
    let event = fixture
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: link.link_id.to_bytes().to_vec().into(),
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
        other => panic!("unexpected read {other:?}"),
    }
}

#[tokio::test]
async fn deleted_dataset_unlinks() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    let link = Box::pin(linked(&fixture, &server.endpoint, LINK_TOKEN, false, None)).await?;
    // Drops the first push check, so the deletion itself must queue the next one.
    fixture
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Delete {
            key_space: LINK_QUEUE_KEYSPACE.to_string(),
            key: link.link_id.to_bytes().to_vec().into(),
            txn_id: None,
        })
        .await;
    drive(
        DeleteDocumentOperation::new(fixture.actor.clone(), fixture.group_id, doc_id(1)),
        &fixture.context,
    )
    .await?;
    replay_event_log(fixture.context.as_ref()).await?;
    process_prune_batch(fixture.context.as_ref()).await?;
    assert!(has_row(&fixture, LINK_QUEUE_KEYSPACE, &link).await);

    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    let storage = &fixture.context.storage_handle;
    assert!(list_links(storage, doc_id(1)).await?.is_empty());
    assert!(!has_row(&fixture, LINK_SECRET_KEYSPACE, &link).await);
    assert!(!has_row(&fixture, LINK_QUEUE_KEYSPACE, &link).await);
    assert!(server.state.lock().unwrap().records.is_empty());
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn registration_awaits_mapping() -> Result<(), Box<dyn std::error::Error>> {
    use aruna_core::structs::secondary_id::{RegisterIdentifiersSpec, SecondaryIdentifier};
    use aruna_operations::jobs::persistent_id::run_register_identifiers;
    let fixture = build_fixture(false).await?;
    let doi = SecondaryIdentifier::new(
        SecondaryIdKind::Doi,
        "10.1234/awaited",
        None,
        IdentifierOrigin::Imported,
    )?;
    let spec = RegisterIdentifiersSpec {
        document_id: doc_id(1),
        identifiers: vec![doi],
        auth_context: AuthContext {
            user_id: fixture.actor.user_id,
            realm_id: fixture.actor.realm_id,
            path_restrictions: None,
            session: None,
        },
    };
    let run = async |job_id| -> Result<JobRunOutcome, Box<dyn std::error::Error>> {
        let payload = JobPayload::RegisterIdentifiers(spec.clone());
        let ctx = claim_context(&fixture, job_id, payload).await?;
        Ok(Box::pin(run_register_identifiers(&ctx, &spec)).await)
    };

    // The authority holds no mapping for the dataset yet, so a new registration waits for it.
    assert!(matches!(run(job_id()).await?, JobRunOutcome::Deferred(_)));
    // Past the wait bound the missing mapping fails the registration.
    let old = JobId::from_parts(
        unix_timestamp_millis() - 60 * 60 * 1000,
        PlacementHandle::new(FIRST_GRANTABLE_HANDLE)?,
        BucketId::new(0)?,
        7,
    )?;
    let JobRunOutcome::Failed(error) = run(old).await? else {
        return Err("a registration past its wait must fail".into());
    };
    assert!(error.message.contains("not found"), "{}", error.message);
    fixture.stop().await;
    Ok(())
}
