//! Pushes and publishes through links: drafts kept after failures, remote edits, reviews,
//! cancellation, file limits and web references.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::link::{
    LINK_TOKEN, attach, change, current, drain, due_now, import_dataset, linked, run_push,
    succeeded,
};
use super::remote::remote;
use super::*;
use aruna_core::invenio::{InvenioLink, LinkFailure, LinkPatch, LinkReview, LinkStatus};
use aruna_operations::jobs::invenio::link_queue::{current_event, start_push};
use aruna_operations::jobs::invenio::links::{LinkChange, change_link};
use aruna_operations::jobs::invenio::remote_state;

async fn push_now(
    fixture: &Fixture,
    link: &InvenioLink,
    publish: bool,
) -> Result<JobRunOutcome, Box<dyn std::error::Error>> {
    let link = current(fixture, link).await.0;
    let event = current_event(&fixture.context, link.document_id).await?;
    Box::pin(start_push(&fixture.context, &link, event, publish)).await?;
    run_push(fixture, &link).await
}

fn failure(link: &InvenioLink) -> Option<&LinkFailure> {
    match &link.status {
        LinkStatus::Failed { reason } => Some(reason),
        _ => None,
    }
}

/// An archive with `files` data files and one web data entity.
async fn archive_with(files: usize) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let names = (0..files)
        .map(|n| format!("data/{n}.txt"))
        .collect::<Vec<_>>();
    let mut parts = names
        .iter()
        .map(|name| json!({"@id": name}))
        .collect::<Vec<_>>();
    parts.push(json!({"@id": "https://example.org/remote.csv"}));
    let mut graph = vec![
        json!({"@id": "ro-crate-metadata.json", "@type": "CreativeWork", "about": {"@id": "./"},
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"}}),
        json!({"@id": "./", "@type": "Dataset", "name": "Exported dataset",
            "description": "Local and web files", "datePublished": "2026-09-22", "publisher": "Aruna test",
            "creator": {"@type": "Person", "familyName": "Researcher"}, "hasPart": parts}),
        json!({"@id": "https://example.org/remote.csv", "@type": "File"}),
    ];
    graph.extend(
        names
            .iter()
            .map(|name| json!({"@id": name, "@type": "File"})),
    );
    let document = json!({"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": graph});
    let mut archive = async_zip::base::write::ZipFileWriter::new(Vec::new());
    archive
        .write_entry_whole(
            ZipEntryBuilder::new("ro-crate-metadata.json".into(), Compression::Stored),
            document.to_string().as_bytes(),
        )
        .await?;
    for name in &names {
        archive
            .write_entry_whole(
                ZipEntryBuilder::new(name.clone().into(), Compression::Stored),
                name.as_bytes(),
            )
            .await?;
    }
    Ok(archive.close().await?)
}

#[tokio::test]
async fn failed_push_continues() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    server.state.lock().unwrap().reject_uploads = true;
    let link = Box::pin(linked(&fixture, &server.endpoint, LINK_TOKEN, false, None)).await?;
    drain(&fixture).await?;
    assert!(matches!(
        run_push(&fixture, &link).await?,
        JobRunOutcome::Failed(_)
    ));
    let (failed, _) = current(&fixture, &link).await;
    assert!(failure(&failed).is_some());
    // The draft was stored before the uploads, with its reserved DOI.
    assert_eq!(failed.remote.draft_id.as_deref(), Some("1"));
    assert_eq!(failed.remote.doi.as_deref(), Some("10.1234/1"));
    assert!(failed.remote.doi_reserved);

    server.state.lock().unwrap().reject_uploads = false;
    succeeded(push_now(&fixture, &link, false).await?);
    let (pushed, _) = current(&fixture, &link).await;
    assert_eq!(pushed.status, LinkStatus::Enabled);
    assert_eq!(pushed.remote.draft_id.as_deref(), Some("1"));
    {
        let remote = server.state.lock().unwrap();
        assert_eq!(
            remote.records.len(),
            1,
            "the retry continued the same draft"
        );
        let calls = remote
            .calls
            .iter()
            .filter(|(method, path)| *method == Method::POST && path == "/api/records")
            .count();
        assert_eq!(calls, 1);
    }
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn busy_reservation_retries() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    server.state.lock().unwrap().busy_reserve = true;
    let link = Box::pin(linked(&fixture, &server.endpoint, LINK_TOKEN, false, None)).await?;
    drain(&fixture).await?;
    let job_id = current(&fixture, &link)
        .await
        .0
        .active_job
        .ok_or("no push")?;
    let storage = &fixture.context.storage_handle;
    let record = aruna_operations::jobs::store::read_job_record(storage, job_id, None)
        .await?
        .ok_or("push job missing")?;
    let JobPayload::ExportRoCrate(spec) = record.payload.clone() else {
        return Err("push job is not an export".into());
    };
    let ctx = claim_context(&fixture, job_id, record.payload).await?;
    assert!(matches!(
        Box::pin(run_export_job(&ctx, &spec)).await,
        JobRunOutcome::Failed(error) if error.kind == aruna_core::structs::execution::job::JobErrorKind::Retryable
    ));
    let (waiting, _) = current(&fixture, &link).await;
    // The draft is on the link before its DOI, so the retry continues it.
    assert_eq!(waiting.remote.draft_id.as_deref(), Some("1"));
    assert!(!waiting.remote.doi_reserved);
    assert!(waiting.status == LinkStatus::Enabled && waiting.active_job.is_some());

    succeeded(Box::pin(run_export_job(&ctx, &spec)).await);
    let (pushed, _) = current(&fixture, &link).await;
    assert_eq!(pushed.remote.doi.as_deref(), Some("10.1234/1"));
    assert!(pushed.remote.doi_reserved);
    assert_eq!(server.state.lock().unwrap().records.len(), 1);
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn cancelled_push_recorded() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    let link = Box::pin(linked(&fixture, &server.endpoint, LINK_TOKEN, false, None)).await?;
    drain(&fixture).await?;
    let started = current(&fixture, &link).await.0;
    let job_id = started.active_job.ok_or("no push")?;
    let storage = &fixture.context.storage_handle;
    let record = aruna_operations::jobs::store::read_job_record(storage, job_id, None)
        .await?
        .ok_or("push job missing")?;
    let JobPayload::ExportRoCrate(spec) = record.payload.clone() else {
        return Err("push job is not an export".into());
    };
    let ctx = claim_context(&fixture, job_id, record.payload).await?;
    succeeded(Box::pin(run_export_job(&ctx, &spec)).await);
    // The push reached the repository, but the link lost the record and the job is cancelled.
    write_value(
        storage,
        aruna_core::keyspaces::INVENIO_LINK_KEYSPACE,
        aruna_core::invenio::link_key(link.document_id, link.link_id),
        started.to_bytes()?,
    )
    .await?;
    ctx.cancel.cancel();
    let outcome = Box::pin(run_export_job(&ctx, &spec)).await;
    assert!(matches!(outcome, JobRunOutcome::Cancelled));
    let (settled, _) = current(&fixture, &link).await;
    assert!(settled.active_job.is_none() && settled.last_push.is_some());
    assert_eq!(settled.remote.draft_id.as_deref(), Some("1"));
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn remote_edits_accepted() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    let link = Box::pin(linked(&fixture, &server.endpoint, LINK_TOKEN, false, None)).await?;
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);

    // Someone edits the draft in the repository.
    server
        .state
        .lock()
        .unwrap()
        .records
        .get_mut("1")
        .unwrap()
        .revision += 1;
    Box::pin(change(&fixture, "Second revision", false)).await?;
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    assert!(matches!(
        run_push(&fixture, &link).await?,
        JobRunOutcome::Failed(_)
    ));
    let (failed, _) = current(&fixture, &link).await;
    assert_eq!(failure(&failed), Some(&LinkFailure::RemoteChanged));

    let state = remote_state(fixture.context.as_ref(), &failed).await?;
    change_link(
        fixture.context.as_ref(),
        &failed,
        LinkChange::Accept(Box::new(state)),
    )
    .await?;
    let (accepted, queued) = current(&fixture, &link).await;
    assert!(queued && accepted.status == LinkStatus::Enabled);
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);

    // A file added in the repository is not the link's to delete.
    server
        .state
        .lock()
        .unwrap()
        .records
        .get_mut("1")
        .unwrap()
        .files
        .insert("added.txt".into(), (Some(b"remote".to_vec()), true));
    Box::pin(change(&fixture, "Third revision", false)).await?;
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    assert!(matches!(
        run_push(&fixture, &link).await?,
        JobRunOutcome::Failed(_)
    ));
    let (failed, _) = current(&fixture, &link).await;
    assert_eq!(failure(&failed), Some(&LinkFailure::RemoteChanged));
    let kept = server.state.lock().unwrap().records["1"]
        .files
        .contains_key("added.txt");
    assert!(kept);
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn community_reviews_first() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    let community = "419f0df8-f1f4-4d56-8950-7749b800c04c";
    server.state.lock().unwrap().community = Some(community.into());
    Box::pin(import_dataset(&fixture, native_archive().await?)).await?;
    let link = Box::pin(attach(
        &fixture,
        &server.endpoint,
        LINK_TOKEN,
        false,
        None,
        Some("aruna"),
    ))
    .await?;
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    succeeded(push_now(&fixture, &link, true).await?);
    let (submitted, queued) = current(&fixture, &link).await;
    assert_eq!(submitted.remote.review, LinkReview::Pending);
    assert!(queued && !submitted.remote.published);
    assert_eq!(
        server.state.lock().unwrap().records["1"].review.as_deref(),
        Some("submitted")
    );

    // A still open review keeps the link waiting.
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    assert_eq!(
        current(&fixture, &link).await.0.remote.review,
        LinkReview::Pending
    );

    // The community accepts, which publishes the draft.
    {
        let mut state = server.state.lock().unwrap();
        let record = state.records.get_mut("1").unwrap();
        record.published = true;
        record.review = None;
    }
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    let (accepted, _) = current(&fixture, &link).await;
    assert_eq!(accepted.remote.review, LinkReview::Accepted);
    assert!(accepted.remote.published);
    assert_eq!(accepted.remote.record_id.as_deref(), Some("1"));

    // Later versions publish directly.
    Box::pin(change(&fixture, "Second version", false)).await?;
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    succeeded(push_now(&fixture, &link, true).await?);
    let (second, _) = current(&fixture, &link).await;
    assert!(second.remote.published);
    assert_eq!(second.remote.record_id.as_deref(), Some("2"));
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn removal_cancels_push() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    let link = Box::pin(linked(&fixture, &server.endpoint, LINK_TOKEN, false, None)).await?;
    drain(&fixture).await?;
    let paused = LinkPatch {
        paused: Some(true),
        ..LinkPatch::default()
    };
    let started = current(&fixture, &link).await.0;
    change_link(
        fixture.context.as_ref(),
        &started,
        LinkChange::Patch(paused),
    )
    .await?;
    assert!(matches!(
        run_push(&fixture, &started).await?,
        JobRunOutcome::Cancelled
    ));
    let (stopped, _) = current(&fixture, &link).await;
    assert_eq!(stopped.status, LinkStatus::Paused);
    assert!(stopped.active_job.is_none());
    assert!(server.state.lock().unwrap().records.is_empty());

    let resume = LinkPatch {
        paused: Some(false),
        ..LinkPatch::default()
    };
    change_link(
        fixture.context.as_ref(),
        &stopped,
        LinkChange::Patch(resume),
    )
    .await?;
    drain(&fixture).await?;
    let running = current(&fixture, &link).await.0;
    let job_id = running.active_job.ok_or("no push")?;
    change_link(fixture.context.as_ref(), &running, LinkChange::Delete).await?;
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
    let ctx = claim_context(&fixture, job_id, record.payload).await?;
    assert!(matches!(
        Box::pin(run_export_job(&ctx, &spec)).await,
        JobRunOutcome::Cancelled
    ));
    assert!(server.state.lock().unwrap().records.is_empty());
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn file_limit_refused() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    Box::pin(import_dataset(&fixture, archive_with(100).await?)).await?;
    let link = Box::pin(attach(
        &fixture,
        &server.endpoint,
        LINK_TOKEN,
        false,
        None,
        None,
    ))
    .await?;
    drain(&fixture).await?;
    assert!(matches!(
        run_push(&fixture, &link).await?,
        JobRunOutcome::Failed(_)
    ));
    let (failed, _) = current(&fixture, &link).await;
    assert_eq!(failure(&failed), Some(&LinkFailure::TooManyFiles));
    assert!(server.state.lock().unwrap().records.is_empty());
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn web_files_referenced() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    Box::pin(import_dataset(&fixture, archive_with(1).await?)).await?;
    let link = Box::pin(attach(
        &fixture,
        &server.endpoint,
        LINK_TOKEN,
        false,
        None,
        None,
    ))
    .await?;
    drain(&fixture).await?;
    succeeded(run_push(&fixture, &link).await?);
    {
        let remote = server.state.lock().unwrap();
        let record = &remote.records["1"];
        assert_eq!(
            record.files.keys().collect::<Vec<_>>(),
            ["data/0.txt", "ro-crate-metadata.json"]
        );
        let related = record.metadata["related_identifiers"].as_array().unwrap();
        assert!(related.contains(&json!({"scheme": "url",
            "identifier": "https://example.org/remote.csv", "relation_type": {"id": "references"}})));
    }
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn stale_push_recorded() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = build_fixture(false).await?;
    let server = remote(LINK_TOKEN).await;
    let link = Box::pin(linked(&fixture, &server.endpoint, LINK_TOKEN, false, None)).await?;
    drain(&fixture).await?;
    let started = current(&fixture, &link).await.0;
    succeeded(run_push(&fixture, &link).await?);
    // The push finished remotely, but recording it on the link failed.
    write_value(
        &fixture.context.storage_handle,
        aruna_core::keyspaces::INVENIO_LINK_KEYSPACE,
        aruna_core::invenio::link_key(link.document_id, link.link_id),
        started.to_bytes()?,
    )
    .await?;
    due_now(&fixture, &link).await?;
    drain(&fixture).await?;
    let (settled, _) = current(&fixture, &link).await;
    assert_eq!(settled.status, LinkStatus::Enabled);
    assert!(settled.active_job.is_none() && settled.last_push.is_some());
    assert_eq!(settled.remote.draft_id.as_deref(), Some("1"));
    fixture.stop().await;
    Ok(())
}
