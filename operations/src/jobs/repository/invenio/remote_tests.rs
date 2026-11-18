//! Checks the calls and results of the link's reads from its repository.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::repository::test_support::{body, child, link, remote, scope, session};
use aruna_core::repository::remote::RemoteFailure;
use serde_json::json;

fn refused(status: u16) -> IoResult {
    remote(Err(RemoteFailure::Invenio(InvenioError::Status(status))))
}

fn connected() -> IoResult {
    child::<_, TransferError>(Ok(session()))
}

fn target() -> LinkTarget {
    LinkTarget {
        link_id: link().link_id,
        published_id: None,
        parent_id: None,
        revision_id: None,
        files: Vec::new(),
    }
}

fn destination(draft_id: Option<&str>) -> RepositoryDestination {
    RepositoryDestination {
        group_id: link().group_id,
        connector_id: link().connector_id,
        draft_id: draft_id.map(str::to_string),
        published_id: None,
        metadata_json: "{}".into(),
        publish: false,
        public_files: false,
        credential: None,
        link: Some(target()),
    }
}

fn lineage(draft_id: Option<&str>, target: LinkTarget) -> CheckLineageOperation {
    CheckLineageOperation::new(
        RepositoryConnectorKind::Invenio,
        scope(),
        creator_auth(&scope(), &link()),
        1024,
        destination(draft_id),
        target,
    )
}

#[test]
fn lineage_needs_links_support() {
    let mut operation = lineage(None, target());
    operation.kind = RepositoryConnectorKind::OaiPmh;
    assert_eq!(operation.first(), Err(not_supported("links")));
}

#[test]
fn no_anchor_passes() {
    let mut operation = lineage(None, target());
    assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.next(connected()), Ok(Next::Done(None)));
}

#[test]
fn edited_draft_is_change() {
    let mut pinned = target();
    pinned.revision_id = Some(3);
    let mut operation = lineage(Some("7"), pinned);
    operation.first().unwrap();
    let url = session().url(&["records", "7", "draft"]).unwrap();
    assert_eq!(
        operation.next(connected()),
        Ok(Next::Call(session().get(url)))
    );
    assert_eq!(operation.next(body(json!({"revision_id": 4}))), Err(changed()));
}

#[test]
fn newer_version_is_change() {
    let mut pinned = target();
    pinned.published_id = Some("7".into());
    let mut operation = lineage(None, pinned);
    operation.first().unwrap();
    operation.next(connected()).unwrap();
    let older = json!({"versions": {"is_latest": false}});
    assert_eq!(operation.next(body(older)), Err(changed()));
}

#[test]
fn parent_gives_latest() {
    let mut pinned = target();
    pinned.parent_id = Some("6".into());
    let mut operation = lineage(None, pinned);
    operation.first().unwrap();
    operation.next(connected()).unwrap();
    let page = json!({"hits": {"hits": [{"id": "9", "parent": {"id": "6"}}]}});
    assert_eq!(
        operation.next(body(page)),
        Ok(Next::Done(Some("9".to_string())))
    );
}

#[test]
fn missing_parent_is_change() {
    let mut pinned = target();
    pinned.parent_id = Some("6".into());
    let mut operation = lineage(None, pinned);
    operation.first().unwrap();
    operation.next(connected()).unwrap();
    assert_eq!(operation.next(refused(404)), Err(changed()));
}

#[test]
fn removed_token_is_permanent() {
    let mut operation = LinkSessionOperation::new(scope(), link());
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::Read(vec![secret_row(link().link_id)])))
    );
    assert_eq!(
        operation.next(IoResult::Values(vec![None])),
        Err(TransferError::Permanent("the link token was removed".into()))
    );
}

fn reviewed() -> RepositoryLink {
    let mut stored = link();
    stored.remote.draft_id = Some("7".into());
    stored
}

fn record(published: bool) -> Value {
    json!({"id": "7", "is_published": published, "parent": {"id": "6"}, "revision_id": 2})
}

#[test]
fn no_draft_has_no_review() {
    let mut operation = ReviewStateOperation::new(scope(), link());
    assert_eq!(operation.first(), Ok(Next::Done(None)));
}

#[test]
fn open_review_answers_nothing() {
    let mut operation = ReviewStateOperation::new(scope(), reviewed());
    operation.first().unwrap();
    operation.next(connected()).unwrap();
    let mut draft = record(false);
    draft["parent"]["review"] = json!({"status": "submitted"});
    assert_eq!(operation.next(body(draft)), Ok(Next::Done(None)));
}

#[test]
fn accepted_review_reads_record() {
    let mut operation = ReviewStateOperation::new(scope(), reviewed());
    operation.first().unwrap();
    operation.next(connected()).unwrap();
    let url = session().url(&["records", "7"]).unwrap();
    assert_eq!(operation.next(refused(404)), Ok(Next::Call(session().get(url))));
    let files = session().url(&["records", "7", "files"]).unwrap();
    assert_eq!(
        operation.next(body(record(true))),
        Ok(Next::Call(session().get(files)))
    );
    let Ok(Next::Done(Some(state))) =
        operation.next(body(json!({"entries": [{"key": "a.txt"}]})))
    else {
        panic!("the review is decided");
    };
    assert_eq!(state.review, LinkReview::Accepted);
    assert_eq!(state.files, ["a.txt"]);
    assert!(state.latest.is_some_and(|latest| latest.published));
}

#[test]
fn vanished_draft_is_change() {
    let mut operation = ReviewStateOperation::new(scope(), reviewed());
    operation.first().unwrap();
    operation.next(connected()).unwrap();
    operation.next(refused(410)).unwrap();
    assert_eq!(operation.next(refused(404)), Err(changed()));
}

#[test]
fn state_without_anchor_is_empty() {
    let mut operation = RemoteStateOperation::new(scope(), link());
    operation.first().unwrap();
    assert_eq!(
        operation.next(connected()),
        Ok(Next::Done(RemoteState {
            draft: None,
            latest: None,
            review: LinkReview::None,
            files: Vec::new(),
        }))
    );
}

#[test]
fn state_reads_draft_files() {
    let mut operation = RemoteStateOperation::new(scope(), reviewed());
    operation.first().unwrap();
    operation.next(connected()).unwrap();
    let versions = operation.next(body(record(false))).unwrap();
    assert!(matches!(versions, Next::Call(IoCall::Effect(_))));
    let files = operation.next(refused(404)).unwrap();
    let url = session().url(&["records", "7", "draft", "files"]).unwrap();
    assert_eq!(files, Next::Call(session().get(url)));
    let Ok(Next::Done(state)) = operation.next(body(json!({"entries": []}))) else {
        panic!("the state is complete");
    };
    assert_eq!(state.draft.map(|draft| draft.id).as_deref(), Some("7"));
    assert_eq!(state.latest, None);
}

fn held() -> RepositoryLink {
    let mut stored = link();
    stored.remote.record_id = Some("7".into());
    stored
}

fn after_connect() -> LatestVersionOperation {
    let mut operation = LatestVersionOperation::new(scope(), held());
    operation.first().unwrap();
    let local = child::<Option<Revision>, LinkError>(Err(LinkError::NoRevision));
    assert!(matches!(
        operation.next(local),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    operation.next(connected()).unwrap();
    operation
}

#[test]
fn latest_needs_record() {
    let mut operation = LatestVersionOperation::new(scope(), link());
    assert!(matches!(operation.first(), Err(TransferError::Permanent(_))));
}

#[test]
fn held_latest_is_found() {
    let mut operation = after_connect();
    let mut latest = record(true);
    latest["versions"] = json!({"is_latest": true});
    assert_eq!(
        operation.next(body(latest)),
        Ok(Next::Done(PullCheck::Found {
            latest_id: "7".into(),
            revision: 2,
            local: None,
        }))
    );
}

#[test]
fn older_record_lists_versions() {
    let mut operation = after_connect();
    let listed = operation.next(body(record(true))).unwrap();
    assert!(matches!(listed, Next::Call(IoCall::Effect(_))));
    let page = json!({"hits": {"hits": [{"id": "8", "revision_id": 5}]}});
    assert_eq!(
        operation.next(body(page)),
        Ok(Next::Done(PullCheck::Found {
            latest_id: "8".into(),
            revision: 5,
            local: None,
        }))
    );
}

#[test]
fn withdrawn_record_is_refused() {
    let mut operation = after_connect();
    assert_eq!(
        operation.next(refused(410)),
        Err(TransferError::Refused(LinkFailure::SourceUnavailable))
    );
}
