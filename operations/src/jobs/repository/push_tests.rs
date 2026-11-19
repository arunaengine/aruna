//! Checks the calls and results of the link steps around a repository push.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::OnceLock;

use super::*;
use crate::jobs::repository::test_support::{
    child, credential_key, job_id, job_scope, link, link_bytes, local_scope, scope,
};
use aruna_core::repository::{LinkFailure, RepositoryCredential};
use aruna_core::structs::execution::job::RoCrateLimits;
use serde::Serialize;

fn target() -> LinkTarget {
    LinkTarget {
        link_id: link().link_id,
        published_id: None,
        parent_id: None,
        revision_id: None,
        files: Vec::new(),
    }
}

fn destination() -> RepositoryDestination {
    RepositoryDestination {
        group_id: link().group_id,
        connector_id: link().connector_id,
        draft_id: None,
        published_id: None,
        metadata_json: "{}".into(),
        publish: false,
        public_files: false,
        credential: None,
        link: Some(target()),
    }
}

fn spec() -> ExportRoCrateSpec {
    ExportRoCrateSpec {
        selection: None,
        destination: Some(destination()),
        auth_context: creator_auth(&scope(), &link()),
        document_id: link().document_id,
        limits: RoCrateLimits::default(),
    }
}

/// One sealed token per test run: sealing picks a new nonce, so two seals differ.
fn token() -> RepositoryCredential {
    static SEALED: OnceLock<RepositoryCredential> = OnceLock::new();
    SEALED
        .get_or_init(|| {
            let link = link();
            RepositoryCredential::seal_link(
                &credential_key(),
                link.created_by,
                link.group_id,
                link.connector_id,
                Some(link.link_id),
                link.endpoint,
                "token",
            )
            .unwrap()
        })
        .clone()
}

fn encoded<T: Serialize>(value: &T) -> aruna_core::types::Value {
    ByteView::from(postcard::to_allocvec(value).unwrap())
}

fn running() -> RepositoryLink {
    let mut running = link();
    running.active_job = Some(job_scope().job_id);
    running
}

fn prepare(checkpoint: ExportCheckpoint) -> PrepareLinkOperation {
    PrepareLinkOperation::new(
        scope(),
        job_scope(),
        spec(),
        destination(),
        target(),
        checkpoint,
    )
}

fn rows(link: &RepositoryLink) -> IoResult {
    IoResult::Values(vec![Some(link_bytes(link)), Some(encoded(&token()))])
}

#[test]
fn prepare_checks_lineage() {
    let mut operation = prepare(ExportCheckpoint::default());
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::Read(vec![
            link_row(link().document_id, link().link_id),
            secret_row(link().link_id),
        ])))
    );
    let mut with_token = destination();
    with_token.credential = Some(token());
    let expected = CheckLineageOperation::new(
        link().kind,
        scope(),
        spec().auth_context,
        spec().limits.metadata_bytes,
        with_token.clone(),
        target(),
    );
    assert_eq!(
        operation.next(rows(&running())),
        Ok(Next::Call(IoCall::child(expected)))
    );
    let done = operation.next(child::<Option<String>, TransferError>(Ok(None)));
    assert_eq!(
        done,
        Ok(Next::Done(Prepared {
            destination: with_token,
            base: None,
        }))
    );
}

#[test]
fn prepare_saves_base() {
    let mut operation = prepare(ExportCheckpoint::default());
    operation.first().unwrap();
    operation.next(rows(&running())).unwrap();
    let base = child::<_, TransferError>(Ok(Some("41".to_string())));
    assert!(matches!(
        operation.next(base),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let done = operation
        .next(child::<(), JobMutationError>(Ok(())))
        .unwrap();
    let Next::Done(prepared) = done else {
        panic!("the preparation goes on");
    };
    assert_eq!(prepared.base.as_deref(), Some("41"));
    assert_eq!(prepared.destination.published_id.as_deref(), Some("41"));
}

#[test]
fn prepare_refuses_replaced() {
    let mut other = link();
    other.active_job = Some(job_id(9));
    let mut operation = prepare(ExportCheckpoint::default());
    operation.first().unwrap();
    assert_eq!(
        operation.next(rows(&other)),
        Err(permanent("a newer push of this link replaced this job"))
    );
    let mut operation = prepare(ExportCheckpoint::default());
    operation.first().unwrap();
    assert!(matches!(
        operation.next(rows(&link())),
        Err(TransferError::Retryable(_))
    ));
}

#[test]
fn prepare_needs_token() {
    let mut operation = prepare(ExportCheckpoint::default());
    operation.first().unwrap();
    let answer = IoResult::Values(vec![Some(link_bytes(&running())), None]);
    assert_eq!(
        operation.next(answer),
        Err(permanent("the link token was removed"))
    );
}

fn settle(end: ExportEnd) -> SettlePushOperation {
    SettlePushOperation::new(
        local_scope(),
        job_scope().job_id,
        link().document_id,
        target(),
        end,
    )
}

fn link_row_answer(link: &RepositoryLink) -> IoResult {
    IoResult::Values(vec![Some(link_bytes(link))])
}

#[test]
fn settle_skips_replaced() {
    let mut operation = settle(ExportEnd::Cancelled);
    operation.first().unwrap();
    assert_eq!(
        operation.next(link_row_answer(&link())),
        Ok(Next::Done(None))
    );
}

#[test]
fn settle_records_failure() {
    let mut operation = settle(ExportEnd::Failed("broken".into()));
    operation.first().unwrap();
    assert_eq!(
        operation.next(link_row_answer(&running())),
        Ok(Next::Call(read_checkpoint(job_scope().job_id)))
    );
    let checkpoint = IoResult::Values(vec![None]);
    let change = LinkChange::Finish {
        job_id: job_scope().job_id,
        outcome: Box::new(PushOutcome::Failed(LinkFailure::Other("broken".into()))),
        requeue: false,
    };
    assert_eq!(
        operation.next(checkpoint),
        Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
            local_scope(),
            running(),
            change
        ))))
    );
    let failed = child::<Option<RepositoryLink>, LinkError>(Err(LinkError::Fenced));
    let Ok(Next::Done(Some(message))) = operation.next(failed) else {
        panic!("a failed record retries the job");
    };
    assert!(message.starts_with("recording the link push failed"));
}

#[test]
fn cancelled_record_ends() {
    let mut operation = settle(ExportEnd::Cancelled);
    operation.first().unwrap();
    operation.next(link_row_answer(&running())).unwrap();
    operation.next(IoResult::Values(vec![None])).unwrap();
    let failed = child::<Option<RepositoryLink>, LinkError>(Err(LinkError::Fenced));
    assert_eq!(operation.next(failed), Ok(Next::Done(None)));
}

#[test]
fn open_end_skipped() {
    let mut operation = settle(ExportEnd::Open);
    operation.first().unwrap();
    operation.next(link_row_answer(&running())).unwrap();
    assert_eq!(
        operation.next(IoResult::Values(vec![None])),
        Ok(Next::Done(None))
    );
}

#[test]
fn unlinked_guard_passes() {
    let mut plain = destination();
    plain.link = None;
    let mut operation = GuardOperation::new(job_scope().job_id, link().document_id, &plain);
    assert_eq!(operation.first(), Ok(Next::Done(())));
}

#[test]
fn guard_stops_paused() {
    let mut paused = running();
    paused.status = LinkStatus::Paused;
    let mut operation = GuardOperation::new(job_scope().job_id, link().document_id, &destination());
    operation.first().unwrap();
    assert_eq!(
        operation.next(link_row_answer(&paused)),
        Err(TransferError::Cancelled)
    );
    let mut operation = GuardOperation::new(job_scope().job_id, link().document_id, &destination());
    operation.first().unwrap();
    assert_eq!(
        operation.next(link_row_answer(&running())),
        Ok(Next::Done(()))
    );
}

fn record() -> RepositoryRecord {
    RepositoryRecord {
        id: "7".into(),
        url: "https://zenodo.org/api/records/7/draft".into(),
        published: false,
        parent_id: "6".into(),
        revision_id: 1,
        identifier: None,
        html_url: None,
        concept_identifier: None,
        in_review: false,
        warning: None,
        identifiers: Vec::new(),
    }
}

#[test]
fn draft_link() {
    let job = job_scope().job_id;
    let stored = link();
    let mut operation = RecordDraftOperation::new(
        local_scope(),
        job,
        stored.document_id,
        stored.link_id,
        record(),
    );
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(ReadLinkOperation::new(
            stored.document_id,
            stored.link_id
        ))))
    );
    let change = LinkChange::Draft {
        job_id: job,
        record: Box::new(record()),
    };
    assert_eq!(
        operation.next(child::<_, LinkError>(Ok(Some(stored.clone())))),
        Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
            local_scope(),
            stored,
            change
        ))))
    );
    let busy = child::<Option<RepositoryLink>, LinkError>(Err(LinkError::NotFound));
    assert_eq!(operation.next(busy), Err(TransferError::Cancelled));
}

#[test]
fn creator_serving_realm() {
    let auth = creator_auth(&scope(), &link());
    assert_eq!(auth.user_id, link().created_by);
    assert_eq!(
        auth.realm_id,
        crate::jobs::repository::test_support::realm()
    );
}
