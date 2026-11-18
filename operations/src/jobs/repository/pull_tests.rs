//! Checks the calls and results of the pull link checks and of an import's link record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::repository::test_support::{
    NOW_MS, child, job_id, link, link_bytes, local_scope, owner_node,
};
use aruna_core::repository::ImportOptions;
use aruna_core::structs::execution::job::{ImportRoCrateTarget, RoCrateLimits};

fn pull_state(latest: Option<&str>, auto_update: bool) -> LinkPull {
    LinkPull {
        auto_update,
        options: ImportOptions::default(),
        target: target(),
        latest_remote_id: latest.map(str::to_string),
        latest_revision: None,
        last_checked_at: None,
        next_check_ms: 0,
        failures: 0,
        revision: None,
        local_changed: false,
    }
}

fn target() -> ImportRoCrateTarget {
    ImportRoCrateTarget {
        bucket: "data".into(),
        prefix: String::new(),
    }
}

fn pull_link(latest: Option<&str>, auto_update: bool) -> RepositoryLink {
    let mut stored = link();
    stored.direction = LinkDirection::Pull(Box::new(pull_state(latest, auto_update)));
    stored
}

fn due(stored: RepositoryLink) -> CheckDueOperation {
    let mut operation = CheckDueOperation::new(local_scope(), stored);
    operation.stamp(NOW_MS);
    operation
}

#[test]
fn due_checks_lineage() {
    let stored = pull_link(None, false);
    let mut operation = due(stored.clone());
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(CheckNowOperation::new(
            local_scope(),
            stored.clone()
        ))))
    );
    let checked = child::<_, LinkError>(Ok(Some(stored)));
    assert_eq!(operation.next(checked), Ok(Next::Done(None)));
}

#[test]
fn limited_pull_retries() {
    let stored = pull_link(Some("2"), true);
    let mut operation = due(stored.clone());
    operation.first().unwrap();
    let start = operation.next(child::<_, LinkError>(Ok(Some(stored.clone()))));
    assert_eq!(
        start,
        Ok(Next::Call(IoCall::child(StartPullOperation::new(
            local_scope(),
            stored
        ))))
    );
    let limited = child::<JobId, LinkError>(Err(LinkError::JobLimit(1)));
    assert_eq!(
        operation.next(limited),
        Ok(Next::Done(Some(NOW_MS + ACTIVE_RETRY_MS)))
    );
}

#[test]
fn missing_pull_fails() {
    let mut stored = pull_link(None, false);
    stored.active_job = Some(job_id(3));
    let mut operation = due(stored.clone());
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(ReadJobOperation::new(job_id(3)))))
    );
    let change = LinkChange::Finish {
        job_id: job_id(3),
        outcome: Box::new(PushOutcome::Failed(LinkFailure::Other(
            "pull job missing".into(),
        ))),
        requeue: false,
    };
    let missing = child::<Option<JobRecord>, aruna_core::calls::IoError>(Ok(None));
    assert_eq!(
        operation.next(missing),
        Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
            local_scope(),
            stored,
            change
        ))))
    );
    let done = child::<Option<RepositoryLink>, LinkError>(Ok(None));
    assert_eq!(operation.next(done), Ok(Next::Done(None)));
}

#[test]
fn check_maps_answers() {
    for (answer, change) in [
        (
            Err(TransferError::Refused(LinkFailure::SourceUnavailable)),
            LinkChange::Fail(LinkFailure::SourceUnavailable),
        ),
        (
            Err(TransferError::Permanent("gone".into())),
            LinkChange::Fail(LinkFailure::Other("gone".into())),
        ),
        (
            Err(TransferError::Retryable("later".into())),
            LinkChange::Checked(PullCheck::Unavailable),
        ),
        (
            Ok(PullCheck::Unavailable),
            LinkChange::Checked(PullCheck::Unavailable),
        ),
    ] {
        let stored = pull_link(None, false);
        let mut operation = CheckNowOperation::new(local_scope(), stored.clone());
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::child(LatestVersionOperation::new(
                local_scope(),
                stored.clone()
            ))))
        );
        assert_eq!(
            operation.next(child(answer)),
            Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
                local_scope(),
                stored,
                change
            ))))
        );
        let done = child::<Option<RepositoryLink>, LinkError>(Ok(None));
        assert_eq!(operation.next(done), Ok(Next::Done(None)));
    }
}

#[test]
fn pull_needs_revision() {
    let mut operation = StartPullOperation::new(local_scope(), pull_link(None, false));
    assert_eq!(operation.first(), Err(LinkError::NoRevision));
    let mut operation = StartPullOperation::new(local_scope(), link());
    assert!(matches!(operation.first(), Err(LinkError::Unexpected(_))));
}

#[test]
fn pull_needs_document() {
    let stored = pull_link(Some("2"), false);
    let mut operation = StartPullOperation::new(local_scope(), stored.clone());
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(EnsureHolderOperation::new(
            local_scope(),
            stored.clone()
        ))))
    );
    assert_eq!(
        operation.next(child::<(), LinkError>(Ok(()))),
        Ok(Next::Call(
            read_document_registry(stored.document_id, None).into()
        ))
    );
    let missing = crate::jobs::repository::test_support::event(aruna_core::events::Event::Storage(
        aruna_core::events::StorageEvent::ReadResult {
            key: byteview::ByteView::from(Vec::new()),
            value: None,
        },
    ));
    assert_eq!(operation.next(missing), Err(LinkError::NotHolder));
}

fn running(stored: &RepositoryLink) -> IoResult {
    IoResult::Values(vec![Some(link_bytes(stored))])
}

#[test]
fn running_pull_passes() {
    let mut stored = pull_link(None, false);
    stored.active_job = Some(job_id(1));
    let mut operation = PullRunningOperation::new(job_id(1), stored.document_id, stored.link_id);
    operation.first().unwrap();
    assert_eq!(operation.next(running(&stored)), Ok(Next::Done(())));
}

#[test]
fn replaced_pull_stops() {
    let mut stored = pull_link(None, false);
    stored.active_job = Some(job_id(2));
    let mut operation = PullRunningOperation::new(job_id(1), stored.document_id, stored.link_id);
    operation.first().unwrap();
    assert!(matches!(
        operation.next(running(&stored)),
        Err(TransferError::Permanent(_))
    ));
    stored.status = LinkStatus::Paused;
    let mut operation = PullRunningOperation::new(job_id(1), stored.document_id, stored.link_id);
    operation.first().unwrap();
    assert_eq!(
        operation.next(running(&stored)),
        Err(TransferError::Cancelled)
    );
}

fn record() -> RepositoryRecord {
    RepositoryRecord {
        id: "2".into(),
        url: "https://zenodo.org/api/records/2".into(),
        published: true,
        parent_id: "1".into(),
        revision_id: 3,
        identifier: None,
        html_url: None,
        concept_identifier: None,
        in_review: false,
        warning: None,
        identifiers: Vec::new(),
    }
}

fn progress(revision: Option<Ulid>) -> PullProgress {
    PullProgress {
        record: record(),
        endpoint: link().endpoint,
        base: None,
        revision,
        kind: RepositoryConnectorKind::Invenio,
    }
}

fn import_spec(pull: Option<RepositoryPull>) -> ImportRoCrateSpec {
    let link = link();
    ImportRoCrateSpec {
        auth_context: creator_auth(&local_scope(), &link),
        source: ImportRoCrateSource::Repository {
            group_id: link.group_id,
            connector_id: link.connector_id,
            record_id: "2".into(),
            options: ImportOptions::default(),
            pull,
        },
        target: target(),
        metadata: ImportMetadataTarget {
            group_id: link.group_id,
            path: "data".into(),
            public: false,
        },
        limits: RoCrateLimits::default(),
        document_id: link.document_id,
    }
}

fn settle(
    pull: Option<RepositoryPull>,
    progress: Option<PullProgress>,
    failure: Option<&str>,
) -> SettleImportOperation {
    let mut operation = SettleImportOperation::new(
        local_scope(),
        job_id(1),
        owner_node(),
        import_spec(pull),
        progress,
        failure.map(str::to_string),
    );
    operation.stamp(NOW_MS);
    operation
}

#[test]
fn plain_import_skipped() {
    assert_eq!(settle(None, None, None).first(), Ok(Next::Done(())));
}

#[test]
fn kept_import_links() {
    let keep = RepositoryPull::Keep {
        auto_update: true,
        owner_node_url: "https://node.example".into(),
    };
    let mut operation = settle(Some(keep), Some(progress(Some(Ulid::nil()))), None);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let exists = child::<Option<RepositoryLink>, LinkError>(Err(LinkError::Exists));
    assert_eq!(operation.next(exists), Ok(Next::Done(())));
}

#[test]
fn unfinished_keep_skipped() {
    let keep = RepositoryPull::Keep {
        auto_update: true,
        owner_node_url: "https://node.example".into(),
    };
    assert_eq!(settle(Some(keep), None, None).first(), Ok(Next::Done(())));
}

#[test]
fn failed_update_finishes() {
    let mut stored = pull_link(None, false);
    stored.active_job = Some(job_id(1));
    let update = RepositoryPull::Update {
        link_id: stored.link_id,
    };
    let mut operation = settle(Some(update), None, Some("broken"));
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(ReadLinkOperation::new(
            stored.document_id,
            stored.link_id
        ))))
    );
    let change = LinkChange::Finish {
        job_id: job_id(1),
        outcome: Box::new(PushOutcome::Failed(LinkFailure::Other("broken".into()))),
        requeue: false,
    };
    assert_eq!(
        operation.next(child::<_, LinkError>(Ok(Some(stored.clone())))),
        Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
            local_scope(),
            stored,
            change
        ))))
    );
    let gone = child::<Option<RepositoryLink>, LinkError>(Err(LinkError::NotFound));
    assert_eq!(operation.next(gone), Ok(Next::Done(())));
}

#[test]
fn replaced_update_left() {
    let stored = pull_link(None, false);
    let update = RepositoryPull::Update {
        link_id: stored.link_id,
    };
    let mut operation = settle(Some(update), None, None);
    operation.first().unwrap();
    let answer = child::<_, LinkError>(Ok(Some(stored)));
    assert_eq!(operation.next(answer), Ok(Next::Done(())));
}
