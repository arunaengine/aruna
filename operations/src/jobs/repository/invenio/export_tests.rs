//! Checks the calls and results of every step of a repository export.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::repository::push::creator_auth;
use crate::jobs::repository::test_support::{
    artifact, body, child, credential_key, job_scope, link, remote, scope, session,
};
use crate::jobs::store::mutation_error::JobMutationError;
use aruna_core::repository::remote::{RemoteFailure, StopReason};
use aruna_core::repository::{LinkTarget, RepositoryCredential};
use aruna_core::structs::checksum::Hasher;
use aruna_core::structs::execution::job::RoCrateLimits;

const CRATE: &str = r##"{"@graph": [
    {"@id": "ro-crate-metadata.json", "about": {"@id": "./"}},
    {"@id": "./", "name": "Title", "datePublished": "2026", "creator": {"@id": "#ada"}},
    {"@id": "#ada", "@type": "Person", "name": "Ada"}
]}"##;

fn token() -> RepositoryCredential {
    let link = link();
    RepositoryCredential::seal_link(
        &credential_key(),
        link.created_by,
        link.group_id,
        link.connector_id,
        None,
        link.endpoint,
        "token",
    )
    .unwrap()
}

fn target() -> LinkTarget {
    LinkTarget {
        link_id: link().link_id,
        published_id: None,
        parent_id: None,
        revision_id: None,
        files: vec!["old.txt".into()],
    }
}

fn run(target: Option<LinkTarget>) -> ExportRun {
    let destination = RepositoryDestination {
        group_id: link().group_id,
        connector_id: link().connector_id,
        draft_id: None,
        published_id: None,
        metadata_json: "{}".into(),
        publish: true,
        public_files: false,
        credential: Some(token()),
        link: target,
    };
    ExportRun {
        scope: scope(),
        job: job_scope(),
        spec: ExportRoCrateSpec {
            selection: None,
            destination: Some(destination.clone()),
            auth_context: creator_auth(&scope(), &link()),
            document_id: link().document_id,
            limits: RoCrateLimits::default(),
        },
        destination,
    }
}

fn checkpoint() -> ExportCheckpoint {
    let mut checkpoint = ExportCheckpoint::default();
    checkpoint.raw_jsonld = Some(CRATE.into());
    checkpoint.artifact = Some(artifact());
    checkpoint
}

fn record() -> RepositoryRecord {
    RepositoryRecord {
        id: "7".into(),
        url: session()
            .url(&["records", "7", "draft"])
            .unwrap()
            .to_string(),
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

fn draft() -> Value {
    json!({"id": "7", "parent": {"id": "6"}, "is_published": false, "revision_id": 1,
        "metadata": {"title": "Title"}})
}

fn connected() -> IoResult {
    child::<_, TransferError>(Ok(session()))
}

fn passed() -> IoResult {
    child::<(), TransferError>(Ok(()))
}

fn saved() -> IoResult {
    child::<(), JobMutationError>(Ok(()))
}

fn status(code: u16) -> IoResult {
    remote(Err(RemoteFailure::Invenio(InvenioError::Status(code))))
}

fn inspection(paths: &[&str]) -> ArchiveInspection {
    ArchiveInspection {
        entries: paths.iter().map(|path| entry(path, b"data")).collect(),
        metadata_index: 0,
        wrapper: None,
        eln: false,
    }
}

fn digests() -> FileDigests {
    FileDigests::of(&Hasher::new_with_bytes(b"data"), 4)
}

fn entry(path: &str, data: &[u8]) -> ArchiveEntry {
    ArchiveEntry {
        index: 0,
        archive_path: path.into(),
        path: path.into(),
        compressed_size: data.len() as u64,
        uncompressed_size: data.len() as u64,
        crc32: u32::from_be_bytes(digests().crc32),
        data_offset: 10,
        compression: ArchiveCompression::Stored,
        directory: false,
    }
}

fn remote_file(status: &str) -> Value {
    let md5 = hex::encode(&digests().hashes["md5"]);
    json!({"key": "a.txt", "checksum": format!("md5:{md5}"), "size": 4, "status": status})
}

fn export(run: ExportRun, checkpoint: ExportCheckpoint) -> RepositoryExportOperation {
    let inspection = Ok(inspection(&[]));
    RepositoryExportOperation::new(
        RepositoryConnectorKind::Invenio,
        run,
        checkpoint,
        inspection,
    )
}

#[test]
fn other_kind_ends() {
    let stored = checkpoint();
    let mut operation = export(run(None), stored.clone());
    operation.kind = RepositoryConnectorKind::OaiPmh;
    assert_eq!(
        operation.first(),
        Ok(Next::Done((stored, Err(not_supported("deposit")))))
    );
}

#[test]
fn unknown_creation_stops() {
    let mut stored = checkpoint();
    stored.repository_started = true;
    let mut operation = export(run(None), stored);
    let Ok(Next::Done((_, Err(TransferError::Permanent(message))))) = operation.first() else {
        panic!("an unknown draft creation is final");
    };
    assert!(message.starts_with("draft creation outcome is unknown"));
}

#[test]
fn export_runs_step() {
    let mut operation = export(run(None), checkpoint());
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let created = child::<CreatedDraft, TransferError>(Ok((true, Ok(record()))));
    assert!(matches!(
        operation.next(created),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert!(operation.checkpoint.repository_started);
    assert_eq!(operation.checkpoint.repository, Some(record()));
    // Saved: the DOI reservation follows.
    assert_eq!(
        operation.next(saved()),
        Ok(Next::Call(IoCall::child(ReserveDoiOperation::new(
            run(None),
            record()
        ))))
    );
    assert_eq!(
        operation.next(child::<_, TransferError>(Ok(record()))),
        Ok(Next::Call(IoCall::child(PrepareDraftOperation::new(
            run(None),
            record(),
            CRATE.into(),
            ExportIdentity::default()
        ))))
    );
    let prepared = child::<_, TransferError>(Ok((record(), [5; 32])));
    assert!(matches!(
        operation.next(prepared),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.checkpoint.repository_metadata, Some([5; 32]));
    assert!(matches!(
        operation.next(saved()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let deposited = child::<_, TransferError>(Ok((record(), vec!["a.txt".to_string()])));
    assert!(matches!(
        operation.next(deposited),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let Ok(Next::Done((stored, Ok(())))) = operation.next(saved()) else {
        panic!("the export is complete");
    };
    assert!(stored.repository_complete);
    assert_eq!(stored.repository_files, ["a.txt"]);
}

#[test]
fn failed_create_fenced() {
    let mut operation = export(run(None), checkpoint());
    operation.first().unwrap();
    let lost = Err(TransferError::Retryable("lost".into()));
    let created = child::<CreatedDraft, TransferError>(Ok((true, lost)));
    let Ok(Next::Done((stored, Err(_)))) = operation.next(created) else {
        panic!("the export ends");
    };
    assert!(stored.repository_started);
}

#[test]
fn link_failure_observes() {
    let linked = run(Some(target()));
    let mut stored = checkpoint();
    stored.repository = Some(record());
    stored.repository_metadata = Some([5; 32]);
    let mut operation = export(linked.clone(), stored);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let prepared = Prepared {
        destination: linked.destination.clone(),
        base: None,
    };
    let deposit = operation.next(child::<_, TransferError>(Ok(prepared)));
    assert!(matches!(deposit, Ok(Next::Call(IoCall::Child(_)))));
    let failed = child::<DepositOutput, TransferError>(Err(TransferError::Cancelled));
    assert_eq!(
        operation.next(failed),
        Ok(Next::Call(IoCall::child(ObserveRevisionOperation::new(
            linked,
            record(),
            [5; 32]
        ))))
    );
    let Ok(Next::Done((_, Err(error)))) = operation.next(passed()) else {
        panic!("the export ends with the deposit failure");
    };
    assert_eq!(error, TransferError::Cancelled);
}

#[test]
fn moved_draft_recorded() {
    let linked = run(Some(target()));
    let mut current = draft();
    current["revision_id"] = json!(2);
    let digest = metadata_digest(&current);
    let mut operation = ObserveRevisionOperation::new(linked, record(), digest);
    operation.first().unwrap();
    operation.next(connected()).unwrap();
    assert!(matches!(
        operation.next(body(current)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let refused = child::<(), TransferError>(Err(TransferError::Cancelled));
    assert_eq!(operation.next(refused), Ok(Next::Done(())));
}

#[test]
fn unread_draft_warns() {
    let mut operation = ObserveRevisionOperation::new(run(None), record(), [0; 32]);
    operation.first().unwrap();
    operation.next(connected()).unwrap();
    assert_eq!(operation.next(status(500)), Ok(Next::Done(())));
}

fn create(run: ExportRun, jsonld: &str) -> CreateDraftOperation {
    let mut fenced = checkpoint();
    fenced.repository_started = true;
    CreateDraftOperation::new(run, jsonld.into(), ExportIdentity::default(), fenced)
}

#[test]
fn new_draft_fenced() {
    let mut operation = create(run(None), CRATE);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(
        operation.next(connected()),
        Ok(Next::Call(run(None).guard()))
    );
    assert!(matches!(
        operation.next(passed()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert!(!operation.fenced);
    assert!(matches!(
        operation.next(saved()),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert!(operation.fenced);
    let Ok(Next::Done((true, Ok(created)))) = operation.next(body(draft())) else {
        panic!("the draft exists");
    };
    assert_eq!(created.id, "7");
}

#[test]
fn draft_needs_login() {
    let mut without = run(None);
    without.destination.credential = None;
    let mut operation = create(without, CRATE);
    assert_eq!(
        operation.first(),
        Ok(Next::Done((
            false,
            Err(invalid("a personal repository login is required"))
        )))
    );
}

#[test]
fn incomplete_crate_refused() {
    let mut operation = create(run(None), "{}");
    operation.first().unwrap();
    let Ok(Next::Done((false, Err(TransferError::Refused(reason))))) = operation.next(connected())
    else {
        panic!("the requirements are unmet");
    };
    assert!(matches!(reason, LinkFailure::RequirementsUnmet(_)));
}

#[test]
fn version_checks_parent() {
    let mut versioned = run(None);
    versioned.destination.published_id = Some("5".into());
    let mut operation = create(versioned.clone(), CRATE);
    operation.first().unwrap();
    let source = session().url(&["records", "5"]).unwrap();
    assert_eq!(
        operation.next(connected()),
        Ok(Next::Call(session().get(source)))
    );
    let published = json!({"is_published": true, "parent": {"id": "1"}});
    assert_eq!(
        operation.next(body(published)),
        Ok(Next::Call(versioned.guard()))
    );
    let versions = session().url(&["records", "5", "versions"]).unwrap();
    assert_eq!(
        operation.next(passed()),
        Ok(Next::Call(session().post(versions, None)))
    );
    assert_eq!(
        operation.next(body(draft())),
        Ok(Next::Done((
            false,
            Err(invalid("new version belongs to another record"))
        )))
    );
}

fn reserve() -> ReserveDoiOperation {
    let mut operation = ReserveDoiOperation::new(run(None), record());
    operation.first().unwrap();
    assert_eq!(
        operation.next(connected()),
        Ok(Next::Call(run(None).guard()))
    );
    let url = session()
        .url(&["records", "7", "draft", "pids", "doi"])
        .unwrap();
    assert_eq!(
        operation.next(passed()),
        Ok(Next::Call(session().post(url, None)))
    );
    operation
}

#[test]
fn reserved_doi_stored() {
    let mut reserved = draft();
    reserved["pids"] = json!({"doi": {"identifier": "10.1/7"}});
    let Ok(Next::Done(answer)) = reserve().next(body(reserved)) else {
        panic!("the DOI is reserved");
    };
    assert_eq!(answer.identifier.as_deref(), Some("10.1/7"));
}

#[test]
fn missing_provider_tolerated() {
    assert_eq!(reserve().next(status(400)), Ok(Next::Done(record())));
}

#[test]
fn rejected_reservation_stops() {
    assert_eq!(
        reserve().next(status(403)),
        Err(TransferError::Refused(LinkFailure::TokenRejected))
    );
}

fn prepare(run: ExportRun) -> PrepareDraftOperation {
    let mut operation =
        PrepareDraftOperation::new(run, record(), CRATE.into(), ExportIdentity::default());
    operation.first().unwrap();
    let url = session().url(&["records", "7", "draft"]).unwrap();
    assert_eq!(
        operation.next(connected()),
        Ok(Next::Call(session().get(url)))
    );
    operation
}

/// The draft as the repository keeps it after the mapped fields were stored.
fn stored_draft(operation: &PrepareDraftOperation) -> Value {
    let fields = operation.fields_for(&session(), &draft()).unwrap();
    let mut stored = draft();
    stored["metadata"] = fields["metadata"].clone();
    stored["access"] = fields["access"].clone();
    stored["custom_fields"] = fields["custom_fields"].clone();
    stored
}

#[test]
fn new_draft_unchanged() {
    let mut operation = prepare(run(None));
    let stored = stored_draft(&operation);
    let digest = metadata_digest(&stored);
    assert_eq!(
        operation.next(body(stored)),
        Ok(Next::Done((record(), digest)))
    );
}

#[test]
fn replaced_draft_refused() {
    let mut operation = prepare(run(None));
    let mut other = draft();
    other["parent"]["id"] = json!("9");
    assert_eq!(
        operation.next(body(other)),
        Err(invalid("repository draft identity changed"))
    );
}

#[test]
fn requested_draft_updated() {
    let mut requested = run(None);
    requested.destination.draft_id = Some("7".into());
    let mut operation = prepare(requested.clone());
    let stored = stored_draft(&operation);
    assert_eq!(
        operation.next(body(stored.clone())),
        Ok(Next::Call(requested.guard()))
    );
    assert!(matches!(
        operation.next(passed()),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let digest = metadata_digest(&stored);
    assert_eq!(
        operation.next(body(stored)),
        Ok(Next::Done((record(), digest)))
    );
}

fn deposit(run: ExportRun, uploads: &[&str], metadata: [u8; 32]) -> DepositOperation {
    DepositOperation::new(
        run,
        record(),
        artifact(),
        metadata,
        uploads.iter().map(|path| path.to_string()).collect(),
        Ok(inspection(uploads)),
    )
}

fn draft_listing(operation: &mut DepositOperation) {
    operation.first().unwrap();
    operation.next(connected()).unwrap();
    let files = session().url(&["records", "7", "draft", "files"]).unwrap();
    assert_eq!(
        operation.next(body(draft())),
        Ok(Next::Call(session().get(files)))
    );
}

#[test]
fn moved_endpoint_refused() {
    let mut stale = record();
    stale.url = "https://other.example/api/records/7/draft".into();
    let mut operation = deposit(run(None), &[], metadata_digest(&draft()));
    operation.record = stale;
    operation.first().unwrap();
    assert_eq!(
        operation.next(connected()),
        Err(invalid(
            "repository connector endpoint changed during export"
        ))
    );
}

#[test]
fn empty_deposit_finishes() {
    let digest = metadata_digest(&draft());
    let mut operation = deposit(run(None), &[], digest);
    draft_listing(&mut operation);
    assert!(matches!(
        operation.next(body(json!({"entries": []}))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let finished = child::<_, TransferError>(Ok(record()));
    assert_eq!(
        operation.next(finished),
        Ok(Next::Done((record(), Vec::new())))
    );
}

#[test]
fn upload_counts_progress() {
    let digest = metadata_digest(&draft());
    let scope_run = run(None);
    let progress = scope_run.job.progress.clone();
    let mut operation = deposit(scope_run.clone(), &["a.txt"], digest);
    draft_listing(&mut operation);
    assert_eq!(
        operation.next(body(json!({"entries": []}))),
        Ok(Next::Call(scope_run.guard()))
    );
    assert_eq!(progress.snapshot().total, Some(1));
    assert!(matches!(
        operation.next(passed()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let uploaded = child::<_, TransferError>(Ok(digests()));
    assert!(matches!(
        operation.next(uploaded),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(progress.snapshot().current, 1);
    let finished = child::<_, TransferError>(Ok(record()));
    assert_eq!(
        operation.next(finished),
        Ok(Next::Done((record(), vec!["a.txt".to_string()])))
    );
}

#[test]
fn foreign_file_refused() {
    let digest = metadata_digest(&draft());
    let mut operation = deposit(run(Some(target())), &[], digest);
    draft_listing(&mut operation);
    let listing = json!({"entries": [{"key": "new.txt"}]});
    assert_eq!(
        operation.next(body(listing)),
        Err(TransferError::Refused(LinkFailure::RemoteChanged))
    );
}

#[test]
fn dropped_file_removed() {
    let digest = metadata_digest(&draft());
    let linked = run(Some(target()));
    let mut operation = deposit(linked.clone(), &[], digest);
    draft_listing(&mut operation);
    let listing = json!({"entries": [{"key": "old.txt"}]});
    assert_eq!(
        operation.next(body(listing)),
        Ok(Next::Call(linked.guard()))
    );
    let url = session()
        .url(&["records", "7", "draft", "files", "old.txt"])
        .unwrap();
    assert_eq!(
        operation.next(passed()),
        Ok(Next::Call(session().delete(url)))
    );
    assert!(matches!(
        operation.next(remote(Ok(RemoteAnswer::Done))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
}

#[test]
fn published_failure_warns() {
    let mut operation = deposit(run(None), &[], [9; 32]);
    operation.first().unwrap();
    operation.next(connected()).unwrap();
    let record_url = session().url(&["records", "7"]).unwrap();
    assert_eq!(
        operation.next(status(404)),
        Ok(Next::Call(session().get(record_url)))
    );
    let mut published = draft();
    published["is_published"] = json!(true);
    let files = session().url(&["records", "7", "files"]).unwrap();
    assert_eq!(
        operation.next(body(published)),
        Ok(Next::Call(session().get(files)))
    );
    let Ok(Next::Done((warned, keys))) =
        operation.next(body(json!({"entries": [{"key": "a.txt"}]})))
    else {
        panic!("the published record stays");
    };
    assert_eq!(
        warned.warning.as_deref(),
        Some("repository metadata changed after preparation")
    );
    assert_eq!(keys, ["a.txt"]);
}

#[test]
fn draft_failure_stays() {
    let mut operation = deposit(run(None), &[], [9; 32]);
    operation.first().unwrap();
    operation.next(connected()).unwrap();
    assert_eq!(
        operation.next(body(draft())),
        Err(invalid("repository metadata changed after preparation"))
    );
}

fn upload(existing: Option<Value>, published: bool, replace: bool) -> UploadEntryOperation {
    UploadEntryOperation::new(
        session(),
        "7".into(),
        artifact(),
        entry("a.txt", b"data"),
        existing,
        published,
        replace,
    )
}

fn hashed() -> IoResult {
    remote(Ok(RemoteAnswer::Digests(digests())))
}

fn file_url(tail: &[&str]) -> url::Url {
    let mut segments = vec!["records", "7", "draft", "files", "a.txt"];
    segments.extend_from_slice(tail);
    session().url(&segments).unwrap()
}

#[test]
fn compressed_entry_refused() {
    let mut operation = upload(None, false, false);
    operation.entry.compression = ArchiveCompression::Deflate;
    assert_eq!(
        operation.first(),
        Err(invalid("repository snapshot entry is not stored verbatim"))
    );
}

#[test]
fn changed_bytes_refused() {
    let mut operation = upload(None, false, false);
    operation.entry.crc32 = 1;
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert_eq!(
        operation.next(hashed()),
        Err(invalid("crate file differs from snapshot"))
    );
}

#[test]
fn matching_file_kept() {
    let mut operation = upload(Some(remote_file("completed")), false, false);
    operation.first().unwrap();
    assert_eq!(operation.next(hashed()), Ok(Next::Done(digests())));
}

#[test]
fn pending_file_committed() {
    let mut operation = upload(Some(remote_file("pending")), false, false);
    operation.first().unwrap();
    assert_eq!(
        operation.next(hashed()),
        Ok(Next::Call(session().post(file_url(&["commit"]), None)))
    );
    assert_eq!(
        operation.next(body(remote_file("completed"))),
        Ok(Next::Done(digests()))
    );
}

#[test]
fn missing_file_uploaded() {
    let mut operation = upload(None, false, false);
    operation.first().unwrap();
    let start = session().url(&["records", "7", "draft", "files"]).unwrap();
    assert_eq!(
        operation.next(hashed()),
        Ok(Next::Call(
            session().post(start, Some(json!([{"key": "a.txt"}])))
        ))
    );
    assert_eq!(
        operation.next(body(json!({}))),
        Ok(Next::Call(session().upload(
            file_url(&["content"]),
            4,
            &artifact(),
            10..14
        )))
    );
    assert_eq!(
        operation.next(hashed()),
        Ok(Next::Call(session().post(file_url(&["commit"]), None)))
    );
    assert_eq!(
        operation.next(body(remote_file("completed"))),
        Ok(Next::Done(digests()))
    );
}

#[test]
fn changed_upload_refused() {
    let mut operation = upload(None, false, false);
    operation.first().unwrap();
    operation.next(hashed()).unwrap();
    operation.next(body(json!({}))).unwrap();
    let other = FileDigests::of(&Hasher::new_with_bytes(b"else"), 4);
    assert_eq!(
        operation.next(remote(Ok(RemoteAnswer::Digests(other)))),
        Err(invalid("crate file changed while uploading"))
    );
}

#[test]
fn differing_file_replaced() {
    let mut stale = remote_file("completed");
    stale["size"] = json!(9);
    let mut operation = upload(Some(stale), false, true);
    operation.first().unwrap();
    assert_eq!(
        operation.next(hashed()),
        Ok(Next::Call(session().delete(file_url(&[]))))
    );
    assert!(matches!(
        operation.next(remote(Ok(RemoteAnswer::Done))),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
}

#[test]
fn published_needs_file() {
    let mut operation = upload(None, true, false);
    operation.first().unwrap();
    assert_eq!(
        operation.next(hashed()),
        Err(invalid("published record is missing a complete crate file"))
    );
}

fn finish(run: ExportRun, metadata: [u8; 32]) -> FinishDepositOperation {
    FinishDepositOperation::new(run, session(), record(), false, metadata, BTreeMap::new())
}

fn restricted() -> Value {
    let mut current = draft();
    current["access"] = json!({"files": "restricted"});
    current
}

/// A finish past its file check, about to publish.
fn verified(run: ExportRun) -> FinishDepositOperation {
    let mut operation = finish(run.clone(), metadata_digest(&restricted()));
    operation.first().unwrap();
    operation.next(body(restricted())).unwrap();
    assert_eq!(
        operation.next(body(json!({"entries": []}))),
        Ok(Next::Call(run.guard()))
    );
    operation
}

#[test]
fn changed_metadata_stops() {
    let mut operation = finish(run(None), [0; 32]);
    let current = session().url(&["records", "7", "draft"]).unwrap();
    assert_eq!(operation.first(), Ok(Next::Call(session().get(current))));
    assert_eq!(
        operation.next(body(restricted())),
        Err(invalid("repository metadata changed before publication"))
    );
}

#[test]
fn public_files_stop() {
    let mut operation = finish(run(None), metadata_digest(&draft()));
    operation.first().unwrap();
    assert_eq!(
        operation.next(body(draft())),
        Err(invalid(
            "repository file access is more public than requested"
        ))
    );
}

#[test]
fn draft_export_unpublished() {
    let mut unpublished = run(None);
    unpublished.destination.publish = false;
    let mut operation = finish(unpublished, metadata_digest(&restricted()));
    operation.first().unwrap();
    operation.next(body(restricted())).unwrap();
    let Ok(Next::Done(answer)) = operation.next(body(json!({"entries": []}))) else {
        panic!("a draft export ends here");
    };
    assert!(!answer.published);
}

#[test]
fn publication_is_verified() {
    let mut operation = verified(run(None));
    let publish = session()
        .url(&["records", "7", "draft", "actions", "publish"])
        .unwrap();
    assert_eq!(
        operation.next(passed()),
        Ok(Next::Call(session().post(publish, None)))
    );
    let mut published = restricted();
    published["is_published"] = json!(true);
    let files = session().url(&["records", "7", "files"]).unwrap();
    assert_eq!(
        operation.next(body(published)),
        Ok(Next::Call(session().get(files)))
    );
    let Ok(Next::Done(answer)) = operation.next(body(json!({"entries": [{"key": "x"}]}))) else {
        panic!("a published record stays");
    };
    assert_eq!(
        answer.warning.as_deref(),
        Some("repository file set changed")
    );
}

#[test]
fn verify_stop_stays() {
    let mut operation = verified(run(None));
    operation.next(passed()).unwrap();
    let mut published = restricted();
    published["is_published"] = json!(true);
    operation.next(body(published)).unwrap();
    let stopped = remote(Err(RemoteFailure::Stopped(StopReason::Cancelled)));
    assert_eq!(operation.next(stopped), Err(TransferError::Cancelled));
}

#[test]
fn unconfirmed_publication_fails() {
    let mut operation = verified(run(None));
    operation.next(passed()).unwrap();
    assert_eq!(
        operation.next(body(restricted())),
        Err(invalid("repository did not confirm publication"))
    );
}

#[test]
fn first_version_reviewed() {
    let mut operation = verified(run(None));
    operation.step = FinishStep::ReviewTarget;
    let target = json!({"id": "c1"});
    let review = session().url(&["records", "7", "draft", "review"]).unwrap();
    let request = json!({"receiver": {"community": "c1"}, "type": "community-submission"});
    assert_eq!(
        operation.next(body(target)),
        Ok(Next::Call(session().put(review, request)))
    );
    let submit = session()
        .url(&["records", "7", "draft", "actions", "submit-review"])
        .unwrap();
    assert_eq!(
        operation.next(body(json!({}))),
        Ok(Next::Call(session().post(submit, None)))
    );
    assert!(matches!(
        operation.next(body(json!({}))),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let Ok(Next::Done(answer)) = operation.next(body(draft())) else {
        panic!("the draft waits for its review");
    };
    assert!(answer.in_review);
}

#[test]
fn listing_needs_entries() {
    assert_eq!(
        file_list(&json!({})),
        Err(invalid("missing repository files"))
    );
    let keys = file_list(&json!({"entries": [{"key": "a"}, {}]})).unwrap();
    assert_eq!(keys, ["a"]);
}
