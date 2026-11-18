//! Checks the calls and results of reading a record's history and acquiring its archive.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::repository::push::creator_auth;
use crate::jobs::repository::test_support::{
    artifact, body, child, job_scope, link, remote, scope, session,
};
use aruna_core::structs::execution::job::{
    ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateTarget,
};

fn options(all_versions: bool, mode: ImportMode) -> ImportOptions {
    ImportOptions { mode, all_versions }
}

fn record(id: &str, index: u64) -> Value {
    json!({
        "id": id,
        "is_published": true,
        "parent": {"id": "6"},
        "revision_id": 1,
        "versions": {"index": index, "is_latest": true},
        "metadata": {"title": "Title", "publication_date": "2026",
            "resource_type": {"id": "dataset"}},
    })
}

fn history(all_versions: bool, mode: ImportMode) -> HistoryOperation {
    HistoryOperation::new(
        session(),
        "7".into(),
        RoCrateLimits::default(),
        options(all_versions, mode),
    )
}

#[test]
fn single_version_files() {
    let mut operation = history(false, ImportMode::Copy);
    let seed = session().url(&["records", "7"]).unwrap();
    assert_eq!(
        operation.first(),
        Ok(Next::Call(session().get(seed.clone())))
    );
    assert_eq!(
        operation.next(body(record("7", 1))),
        Ok(Next::Call(session().get(seed)))
    );
    let files = session().url(&["records", "7", "files"]).unwrap();
    assert_eq!(
        operation.next(body(record("7", 1))),
        Ok(Next::Call(session().get(files)))
    );
    let listing = json!({"entries": []});
    assert_eq!(
        operation.next(body(listing.clone())),
        Ok(Next::Done((
            "7".to_string(),
            vec![(record("7", 1), listing)]
        )))
    );
}

#[test]
fn metadata_skips_files() {
    let mut operation = history(false, ImportMode::Metadata);
    operation.first().unwrap();
    operation.next(body(record("7", 1))).unwrap();
    let Ok(Next::Done((selected, records))) = operation.next(body(record("7", 1))) else {
        panic!("the history is complete");
    };
    assert_eq!(selected, "7");
    assert_eq!(records[0].1["listing_requested"], false);
}

#[test]
fn draft_seed_refused() {
    let mut operation = history(false, ImportMode::Copy);
    operation.first().unwrap();
    let mut seed = record("7", 1);
    seed["is_published"] = json!(false);
    assert_eq!(
        operation.next(body(seed)),
        Err(invalid("record is not published"))
    );
}

#[test]
fn versions_follow_pages() {
    let mut operation = history(true, ImportMode::Metadata);
    operation.first().unwrap();
    let mut listing = session().url(&["records", "7", "versions"]).unwrap();
    listing
        .query_pairs_mut()
        .append_pair("allversions", "true")
        .append_pair("size", "25");
    assert_eq!(
        operation.next(body(record("7", 2))),
        Ok(Next::Call(session().get(listing)))
    );
    let page = json!({"hits": {"total": 2, "hits": [{"id": "6"}, {"id": "7"}]}});
    operation.next(body(page)).unwrap();
    operation.next(body(record("6", 1))).unwrap();
    let Ok(Next::Done((_, records))) = operation.next(body(record("7", 2))) else {
        panic!("the history is complete");
    };
    let order: Vec<_> = records.iter().map(|(record, _)| &record["id"]).collect();
    assert_eq!(order, ["6", "7"]);
}

#[test]
fn repeated_version_refused() {
    let mut operation = history(true, ImportMode::Metadata);
    operation.first().unwrap();
    operation.next(body(record("7", 1))).unwrap();
    let page = json!({"hits": {"total": 2, "hits": [{"id": "7"}, {"id": "7"}]}});
    operation.next(body(page)).unwrap();
    assert_eq!(
        operation.next(body(record("7", 1))),
        Err(invalid("duplicate version or version limit"))
    );
}

fn spec() -> ImportRoCrateSpec {
    ImportRoCrateSpec {
        auth_context: creator_auth(&scope(), &link()),
        source: ImportRoCrateSource::Upload {
            upload_id: Ulid::nil(),
        },
        target: ImportRoCrateTarget {
            bucket: "data".into(),
            prefix: String::new(),
        },
        metadata: ImportMetadataTarget {
            group_id: link().group_id,
            path: "doc".into(),
            public: false,
        },
        limits: RoCrateLimits::default(),
        document_id: link().document_id,
    }
}

fn request(pull: Option<RepositoryPull>) -> AcquireRequest {
    AcquireRequest {
        kind: RepositoryConnectorKind::Invenio,
        scope: scope(),
        job: job_scope(),
        spec: spec(),
        group_id: link().group_id,
        connector_id: link().connector_id,
        record_id: "7".into(),
        options: options(false, ImportMode::Metadata),
        pull,
        current: None,
    }
}

#[test]
fn other_kind_refused() {
    let mut acquire = request(None);
    acquire.kind = RepositoryConnectorKind::OaiPmh;
    assert_eq!(
        AcquireOperation::new(acquire).first(),
        Err(not_supported("import"))
    );
}

#[test]
fn update_checks_running() {
    let pull = RepositoryPull::Update {
        link_id: link().link_id,
    };
    let mut operation = AcquireOperation::new(request(Some(pull)));
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(PullRunningOperation::new(
            job_scope().job_id,
            link().document_id,
            link().link_id
        ))))
    );
    let stopped = child::<(), TransferError>(Err(TransferError::Cancelled));
    assert_eq!(operation.next(stopped), Err(TransferError::Cancelled));
}

#[test]
fn archive_spools_history() {
    let mut operation = AcquireOperation::new(request(None));
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert!(matches!(
        operation.next(child::<_, TransferError>(Ok(session()))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let history = (
        "7".to_string(),
        vec![(record("7", 1), json!({"entries": []}))],
    );
    let spool = operation
        .next(child::<_, TransferError>(Ok(history)))
        .unwrap();
    assert!(matches!(spool, Next::Call(IoCall::Effect(_))));
    let answer = remote(Ok(RemoteAnswer::Artifact(artifact())));
    let Ok(Next::Done((stored, _, progress))) = operation.next(answer) else {
        panic!("the archive is stored");
    };
    assert_eq!(stored, artifact());
    assert_eq!(progress, None);
}

#[test]
fn absent_record_refused() {
    let mut operation = AcquireOperation::new(request(None));
    operation.first().unwrap();
    operation
        .next(child::<_, TransferError>(Ok(session())))
        .unwrap();
    let history = (
        "9".to_string(),
        vec![(record("7", 1), json!({"entries": []}))],
    );
    assert_eq!(
        operation.next(child::<_, TransferError>(Ok(history))),
        Err(invalid("requested record absent from history"))
    );
}

#[test]
fn references_are_recognized() {
    assert!(is_reference("versions/7/files/a.txt"));
    assert!(!is_reference("ro-crate-metadata.json"));
}
