//! Tests job status, outputs and cancel: a queued copy, its kept summary, outputs on this node
//! only, a cancel before the copy ran, and ids the install does not own.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::OutputObject;

use super::*;
use crate::plugins::install::tests::Fixture;
use crate::plugins::submit::start_copy;
use crate::plugins::submit::tests::{ready, request, scope};
use crate::plugins::work::tests::edit_work;
use crate::plugins::work::{FinalState, WorkState, update_work};

fn output(node: NodeId, key: &str) -> OutputObject {
    OutputObject {
        node_id: node,
        bucket: "out".into(),
        key: key.into(),
        version_id: Ulid::from_parts(4, 4),
        execution_id: Ulid::from_parts(5, 5),
        container_path: format!("/out/{key}"),
        size: 7,
        digest: None,
    }
}

#[tokio::test]
async fn status_from_row() {
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let copy = request(&fixture, &install, "out/a.txt");
    let id = start_copy(&fixture.ctx, &scope(&fixture, &install), copy)
        .await
        .unwrap();
    let local = fixture.actor.node_id;
    let place = (&install, local);
    let queued = job_status(&fixture.ctx, place, &id.to_string())
        .await
        .unwrap();
    let shown = (
        queued.kind.as_str(),
        queued.path.as_str(),
        queued.state.as_str(),
    );
    assert_eq!(shown, ("copy", "out/a.txt", "queued"));

    // Once settled, the kept summary answers, with the outputs on this node.
    let remote = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    let settle = edit_work(&fixture.ctx, (install.install_id, id), |row| {
        let mut summary = summary_of(None).unwrap();
        summary.state = FinalState::Succeeded;
        (summary.message, summary.bytes) = (None, Some(5));
        summary.outputs = vec![output(local, "a.txt"), output(remote, "b.txt")];
        (row.state, row.summary) = (WorkState::Settled, Some(summary));
    });
    settle.await.unwrap();
    let done = job_status(&fixture.ctx, place, &id.to_string())
        .await
        .unwrap();
    assert_eq!((done.state.as_str(), done.bytes), ("succeeded", Some(5)));
    let page = job_outputs(&fixture.ctx, place, &id.to_string(), None).await;
    let page = page.unwrap();
    let paths: Vec<_> = page
        .objects
        .iter()
        .map(|object| object.path.as_str())
        .collect();
    assert_eq!((paths, page.cursor), (vec!["out/a.txt"], None));
    let cursor = job_outputs(&fixture.ctx, place, &id.to_string(), Some("x".into())).await;
    assert_eq!(
        cursor.map(drop).map_err(|e| e.kind),
        Err(ErrorKind::Invalid)
    );

    for unknown in ["nope".to_string(), Ulid::from_parts(8, 8).to_string()] {
        let missing = job_status(&fixture.ctx, place, &unknown).await;
        assert_eq!(
            missing.map(drop).map_err(|e| e.kind),
            Err(ErrorKind::NotFound)
        );
    }
    // A reinstall under the same id has another account and does not see the job.
    let mut other = install.clone();
    other.account = aruna_core::UserId::local(Ulid::from_parts(6, 6), fixture.actor.realm_id);
    let foreign = job_status(&fixture.ctx, (&other, local), &id.to_string()).await;
    assert_eq!(
        foreign.map(drop).map_err(|e| e.kind),
        Err(ErrorKind::NotFound)
    );
}

#[tokio::test]
async fn cancel_stops_job() {
    // A queued copy that never ran ends cancelled; asking again, or after it ended, is no error.
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let copy = request(&fixture, &install, "out/a.txt");
    let id = start_copy(&fixture.ctx, &scope(&fixture, &install), copy).await;
    let id = id.unwrap().to_string();
    let place = (&install, fixture.actor.node_id);
    for _ in 0..2 {
        cancel_job(&fixture.ctx, &install, &id, 5).await.unwrap();
    }
    let status = job_status(&fixture.ctx, place, &id).await.unwrap();
    assert_eq!(status.state, "cancelled");
    let missing = cancel_job(&fixture.ctx, &install, "nope", 5).await;
    assert_eq!(missing.map_err(|e| e.kind), Err(ErrorKind::NotFound));
}
