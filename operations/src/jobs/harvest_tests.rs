//! Checks the calls and results of a harvest run and of applying one record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use super::*;
use crate::harvest::oai_pmh::parse::OaiHeader;
use crate::jobs::repository::test_support::{
    NOW_MS, child, event as io_event, job_scope, link, local_scope, realm, registry_record, remote,
    scope,
};
use aruna_core::structs::execution::harvest::{HarvestGranularity, HarvestSelector};

fn source() -> HarvestSource {
    HarvestSource {
        source_id: Ulid::from_bytes([1; 16]),
        group_id: link().group_id,
        connector_id: link().connector_id,
        namespace: "ns".into(),
        target_prefix: "imported".into(),
        selector: HarvestSelector {
            set: None,
            metadata_prefix: None,
        },
        schedule_interval_ms: None,
        granularity: Some(HarvestGranularity::Day),
        cursor: None,
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        created_by: link().created_by,
        last_job: None,
    }
}

fn connector(kind: RepositoryConnectorKind) -> RepositoryConnector {
    RepositoryConnector {
        connector_id: link().connector_id,
        group_id: link().group_id,
        name: "provider".into(),
        kind,
        endpoint: "https://oai.example/oai".into(),
        public_config: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        created_by: link().created_by,
    }
}

fn read_answer<T: serde::Serialize>(value: Option<&T>) -> IoResult {
    io_event(Event::Storage(StorageEvent::ReadResult {
        key: ByteView::from(Vec::new()),
        value: value.map(|value| ByteView::from(postcard::to_allocvec(value).unwrap())),
    }))
}

fn written() -> IoResult {
    io_event(Event::Storage(StorageEvent::WriteResult {
        key: ByteView::from(Vec::new()),
    }))
}

fn gate_open() -> IoResult {
    remote(Ok(RemoteAnswer::Done))
}

fn text(body: String) -> IoResult {
    remote(Ok(RemoteAnswer::Text(body)))
}

fn spec() -> HarvestJobSpec {
    HarvestJobSpec {
        source_id: source().source_id,
        group_id: source().group_id,
    }
}

fn job() -> HarvestJobOperation {
    let mut operation = HarvestJobOperation::new(scope(), job_scope(), spec());
    operation.stamp(NOW_MS);
    operation
}

/// A run past its source and connector reads, about to open the first page.
fn listing(source: HarvestSource) -> HarvestJobOperation {
    let mut operation = job();
    operation.first().unwrap();
    assert!(matches!(
        operation.next(read_answer(Some(&source))),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let answer = read_answer(Some(&connector(RepositoryConnectorKind::OaiPmh)));
    assert_eq!(
        operation.next(answer),
        Ok(Next::Call(operation.checkpoint()))
    );
    operation
}

fn page_xml(records: &[(&str, &str)], token: Option<&str>) -> String {
    let records: String = records
        .iter()
        .map(|(identifier, datestamp)| {
            format!(
                "<record><header><identifier>{identifier}</identifier>\
                 <datestamp>{datestamp}</datestamp></header></record>"
            )
        })
        .collect();
    let token = token.map_or(String::new(), |token| {
        format!("<resumptionToken>{token}</resumptionToken>")
    });
    format!(
        "<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\">\
         <ListRecords>{records}{token}</ListRecords></OAI-PMH>"
    )
}

fn error_xml(code: &str) -> String {
    format!(
        "<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\">\
         <error code=\"{code}\">refused</error></OAI-PMH>"
    )
}

fn saved_cursor(next: Turn<HarvestCounts>) -> HarvestCursor {
    let Ok(Next::Call(IoCall::Effect(effect))) = next else {
        panic!("the run stores its cursor");
    };
    let Effect::Storage(aruna_core::effects::StorageEffect::Write { value, .. }) = *effect else {
        panic!("the cursor is a storage write");
    };
    let stored: HarvestSource = postcard::from_bytes(&value).unwrap();
    stored.cursor.unwrap()
}

#[test]
fn missing_source_permanent() {
    let mut operation = job();
    assert_eq!(
        operation.first(),
        Ok(Next::Call(
            read_source_effect(spec().group_id, spec().source_id, None).into()
        ))
    );
    assert_eq!(
        operation.next(read_answer::<HarvestSource>(None)),
        Err(permanent("harvest source not found"))
    );
}

#[test]
fn wrong_connector_refused() {
    let mut operation = job();
    operation.first().unwrap();
    operation.next(read_answer(Some(&source()))).unwrap();
    let answer = read_answer(Some(&connector(RepositoryConnectorKind::Invenio)));
    assert_eq!(
        operation.next(answer),
        Err(permanent(
            "harvest requires an OAI-PMH repository connector"
        ))
    );
}

#[test]
fn harvest_needs_network() {
    let mut operation = HarvestJobOperation::new(local_scope(), job_scope(), spec());
    operation.first().unwrap();
    operation.next(read_answer(Some(&source()))).unwrap();
    let answer = read_answer(Some(&connector(RepositoryConnectorKind::OaiPmh)));
    assert_eq!(
        operation.next(answer),
        Err(permanent("net plane unavailable for harvest"))
    );
}

#[test]
fn identify_failure_ignored() {
    let mut unknown = source();
    unknown.granularity = None;
    let mut operation = job();
    operation.first().unwrap();
    operation.next(read_answer(Some(&unknown))).unwrap();
    let answer = read_answer(Some(&connector(RepositoryConnectorKind::OaiPmh)));
    assert!(matches!(
        operation.next(answer),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let failed = remote(Err(RemoteFailure::Retryable("down".into())));
    assert_eq!(
        operation.next(failed),
        Ok(Next::Call(operation.checkpoint()))
    );
}

#[test]
fn stop_ends_run() {
    let mut operation = listing(source());
    let stopped = remote(Err(RemoteFailure::Stopped(StopReason::Cancelled)));
    assert_eq!(operation.next(stopped), Err(HarvestFailure::Cancelled));
    let mut operation = listing(source());
    let stopped = remote(Err(RemoteFailure::Stopped(StopReason::Interrupted)));
    assert_eq!(operation.next(stopped), Err(HarvestFailure::Interrupted));
}

#[test]
fn page_advances_window() {
    let mut operation = listing(source());
    assert!(matches!(
        operation.next(gate_open()),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let page = page_xml(&[("oai:1", "2026-01-02T00:00:00Z")], None);
    assert_eq!(
        operation.next(text(page)),
        Ok(Next::Call(operation.checkpoint()))
    );
    assert!(matches!(
        operation.next(gate_open()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let applied = child::<_, HarvestFailure>(Ok(RecordApplied::Minted));
    let cursor = saved_cursor(operation.next(applied));
    assert!(cursor.last_datestamp_ms > 0);
    assert_eq!(cursor.resumption_token, None);
    assert_eq!(operation.job.progress.snapshot().current, 1);
    let Ok(Next::Done(counts)) = operation.next(written()) else {
        panic!("the listing is complete");
    };
    assert_eq!(counts.minted, 1);
}

#[test]
fn token_keeps_window() {
    let mut operation = listing(source());
    operation.next(gate_open()).unwrap();
    let cursor = saved_cursor(operation.next(text(page_xml(&[], Some("T1")))));
    assert_eq!(cursor.last_datestamp_ms, 0);
    assert_eq!(cursor.resumption_token.as_deref(), Some("T1"));
    assert_eq!(
        operation.next(written()),
        Ok(Next::Call(operation.checkpoint()))
    );
    assert_eq!(operation.page_index, 1);
}

#[test]
fn repeated_token_fails() {
    let mut operation = listing(source());
    operation
        .seen_tokens
        .insert(*blake3::hash(b"T1").as_bytes());
    operation.next(gate_open()).unwrap();
    let cursor = saved_cursor(operation.next(text(page_xml(&[], Some("T1")))));
    assert_eq!(cursor.resumption_token, None);
    assert_eq!(
        operation.next(written()),
        Err(retryable("OAI provider returned a resumption token twice"))
    );
}

fn resuming() -> HarvestSource {
    let mut resumed = source();
    resumed.cursor = Some(HarvestCursor {
        last_datestamp_ms: 0,
        resumption_token: Some("T1".into()),
    });
    resumed
}

#[test]
fn bad_token_restarts() {
    let mut operation = listing(resuming());
    operation.next(gate_open()).unwrap();
    let cursor = saved_cursor(operation.next(text(error_xml("badResumptionToken"))));
    assert_eq!(cursor.resumption_token, None);
    assert_eq!(
        operation.next(written()),
        Ok(Next::Call(operation.checkpoint()))
    );
    assert!(operation.restarted);
    assert_eq!(operation.token, None);
}

#[test]
fn repeated_rejection_fails() {
    let mut operation = listing(resuming());
    operation.restarted = true;
    operation.next(gate_open()).unwrap();
    saved_cursor(operation.next(text(error_xml("badResumptionToken"))));
    assert!(matches!(
        operation.next(written()),
        Err(HarvestFailure::Job(_))
    ));
}

#[test]
fn protocol_error_permanent() {
    let mut operation = listing(source());
    operation.next(gate_open()).unwrap();
    let Err(HarvestFailure::Job(error)) = operation.next(text(error_xml("badArgument"))) else {
        panic!("a refused request fails");
    };
    assert!(error.message.starts_with("OAI response"));
    assert_eq!(
        error.kind,
        aruna_core::structs::execution::job::JobErrorKind::Permanent
    );
}

#[test]
fn budget_keeps_position() {
    let mut operation = listing(resuming());
    operation.stamp(NOW_MS + HARVEST_RUN_DEADLINE.as_millis() as u64 + 1);
    let cursor = saved_cursor(operation.next(gate_open()));
    assert_eq!(cursor.resumption_token.as_deref(), Some("T1"));
    assert_eq!(
        operation.next(written()),
        Err(retryable("harvest exhausted its paging or time budget"))
    );
}

#[test]
fn counts_become_outcome() {
    let counts = HarvestCounts {
        minted: 1,
        updated: 2,
        tombstoned: 3,
        skipped: 4,
    };
    assert!(matches!(
        harvest_outcome(Ok(counts)),
        JobRunOutcome::Succeeded(JobResultPayload::Harvest { minted: 1, .. })
    ));
    assert!(matches!(
        harvest_outcome(Err(HarvestFailure::Interrupted)),
        JobRunOutcome::Interrupted
    ));
}

fn actor() -> Actor {
    Actor {
        node_id: link().owner_node,
        user_id: link().created_by,
        realm_id: realm(),
    }
}

fn oai_record(identifier: &str, deleted: bool) -> OaiRecord {
    OaiRecord {
        header: OaiHeader {
            identifier: identifier.into(),
            datestamp: "2026-01-02T00:00:00Z".into(),
            deleted,
            sets: Vec::new(),
        },
        dc: Vec::new(),
    }
}

fn document() -> Ulid {
    link().document_id
}

fn provenance(state: HarvestRecordState, datestamp: u64) -> HarvestProvenance {
    HarvestProvenance {
        group_id: source().group_id,
        namespace: "ns".into(),
        source_record_id: "oai:1".into(),
        meta_resource_id: document(),
        version: 1,
        source_datestamp_ms: datestamp,
        state,
        predecessors: Vec::new(),
    }
}

fn apply(deleted: bool) -> ApplyRecordOperation {
    let mut operation =
        ApplyRecordOperation::new(source(), actor(), oai_record("oai:1", deleted), 100);
    operation.first().unwrap();
    operation
}

fn routed(answer: Result<(), HarvestWriteFailure>) -> IoResult {
    io_event(Event::Net(NetEvent::HarvestWrite(answer)))
}

fn allowed() -> IoResult {
    child::<(), AuthorizeError>(Ok(()))
}

fn stored_row() -> IoResult {
    io_event(Event::Storage(StorageEvent::ReadResult {
        key: ByteView::from(Vec::new()),
        value: Some(ByteView::from(
            postcard::to_allocvec(&registry_record()).unwrap(),
        )),
    }))
}

fn nothing_pending() -> IoResult {
    IoResult::Page {
        rows: Vec::new(),
        next: None,
    }
}

/// An update of a live record, past its access check.
fn updating() -> ApplyRecordOperation {
    let mut operation = apply(false);
    let live = provenance(HarvestRecordState::Live, 50);
    assert!(matches!(
        operation.next(read_answer(Some(&live))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(
        operation.next(allowed()),
        Ok(Next::Call(ApplyRecordOperation::registry_call(document())))
    );
    operation
}

#[test]
fn empty_identifier_skips() {
    let mut operation = ApplyRecordOperation::new(source(), actor(), oai_record("", false), 1);
    assert_eq!(operation.first(), Ok(Next::Done(RecordApplied::Skipped)));
}

#[test]
fn stale_record_skips() {
    let mut operation = apply(false);
    let newer = provenance(HarvestRecordState::Live, 500);
    assert_eq!(
        operation.next(read_answer(Some(&newer))),
        Ok(Next::Done(RecordApplied::Skipped))
    );
}

#[test]
fn new_record_mints() {
    let mut operation = apply(false);
    assert_eq!(
        operation.next(read_answer::<HarvestProvenance>(None)),
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm()))))
    );
    let missing =
        child::<RealmConfigDocument, GetConfigError>(Err(GetConfigError::DocumentNotFound));
    assert!(matches!(
        operation.next(missing),
        Err(HarvestFailure::Job(_))
    ));
}

#[test]
fn pending_row_first() {
    let mut operation = apply(false);
    operation.row = Some(provenance(HarvestRecordState::PendingCreate, 100));
    operation.step = ApplyStep::MintPending(document());
    assert!(matches!(
        operation.next(written()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert!(matches!(
        operation.next(allowed()),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert!(matches!(
        operation.next(routed(Ok(()))),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert_eq!(
        operation.row.as_ref().map(|row| row.state),
        Some(HarvestRecordState::Live)
    );
    assert_eq!(
        operation.next(written()),
        Ok(Next::Done(RecordApplied::Minted))
    );
}

#[test]
fn existing_document_updated() {
    let mut operation = apply(false);
    let pending = provenance(HarvestRecordState::PendingCreate, 50);
    operation.next(read_answer(Some(&pending))).unwrap();
    operation.next(allowed()).unwrap();
    assert_eq!(
        operation.next(routed(Err(HarvestWriteFailure::AlreadyExists))),
        Ok(Next::Call(ApplyRecordOperation::registry_call(document())))
    );
    assert!(matches!(
        operation.next(stored_row()),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert!(matches!(
        operation.next(routed(Ok(()))),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert_eq!(
        operation.next(written()),
        Ok(Next::Done(RecordApplied::Minted))
    );
}

#[test]
fn stored_document_updated() {
    let mut operation = updating();
    assert!(matches!(
        operation.next(stored_row()),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert!(matches!(
        operation.next(routed(Ok(()))),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert_eq!(
        operation.next(written()),
        Ok(Next::Done(RecordApplied::Updated))
    );
}

#[test]
fn stored_absence_fails() {
    let mut operation = updating();
    operation.next(stored_row()).unwrap();
    let Err(HarvestFailure::Job(error)) =
        operation.next(routed(Err(HarvestWriteFailure::NotFound)))
    else {
        panic!("a stored document cannot be missing");
    };
    assert_eq!(
        error.message,
        "harvest apply failed: metadata document not found"
    );
}

#[test]
fn absent_document_remints() {
    let mut operation = updating();
    assert_eq!(
        operation.next(read_answer::<MetadataRegistryRecord>(None)),
        Ok(Next::Call(ApplyRecordOperation::pending_call(document())))
    );
    assert!(matches!(
        operation.next(nothing_pending()),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert_eq!(
        operation.next(routed(Err(HarvestWriteFailure::NotFound))),
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm()))))
    );
    assert_eq!(
        operation.row.as_ref().map(|row| row.predecessors.clone()),
        Some(vec![document()])
    );
}

#[test]
fn pending_projection_waits() {
    let mut operation = updating();
    operation
        .next(read_answer::<MetadataRegistryRecord>(None))
        .unwrap();
    let pending = IoResult::Page {
        rows: vec![(ByteView::from(vec![1]), ByteView::from(vec![1]))],
        next: None,
    };
    let Err(HarvestFailure::Job(error)) = operation.next(pending) else {
        panic!("the harvest waits for the projection");
    };
    assert!(error.message.contains("pending registry projection"));
}

#[test]
fn withdrawn_record_tombstones() {
    let mut operation = apply(true);
    let live = provenance(HarvestRecordState::Live, 50);
    operation.next(read_answer(Some(&live))).unwrap();
    operation.next(allowed()).unwrap();
    assert_eq!(
        operation.next(read_answer::<MetadataRegistryRecord>(None)),
        Ok(Next::Call(ApplyRecordOperation::pending_call(document())))
    );
    assert!(matches!(
        operation.next(nothing_pending()),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert!(matches!(
        operation.next(routed(Err(HarvestWriteFailure::NotFound))),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert_eq!(
        operation.row.as_ref().map(|row| row.state),
        Some(HarvestRecordState::Tombstoned)
    );
    assert_eq!(
        operation.next(written()),
        Ok(Next::Done(RecordApplied::Tombstoned))
    );
}

#[test]
fn rejected_record_permanent() {
    let failure = apply_failure(HarvestWriteFailure::Rejected(Vec::new()));
    assert_eq!(
        failure,
        permanent("harvest record rejected by validation: []")
    );
    assert_eq!(
        apply_failure(HarvestWriteFailure::Retryable("busy".into())),
        retryable("harvest apply: busy")
    );
}

/// An update of a live record, waiting for its access check.
fn checking_access() -> ApplyRecordOperation {
    let mut operation = apply(false);
    let live = provenance(HarvestRecordState::Live, 50);
    operation.next(read_answer(Some(&live))).unwrap();
    operation
}

#[test]
fn access_checks_create() {
    let mut operation = apply(false);
    let live = provenance(HarvestRecordState::Live, 50);
    let path = harvest_document_path("imported", "oai:1").unwrap();
    let expected = AuthorizeCreateOperation::new(
        realm(),
        &operation.owner(),
        PolicyRequestExtras::default(),
        source().group_id,
        &path,
        document(),
        CreateScope::Group,
    );
    assert_eq!(
        operation.next(read_answer(Some(&live))),
        Ok(Next::Call(IoCall::child(expected)))
    );
}

#[test]
fn denial_names_path() {
    let path = harvest_document_path("imported", "oai:1").unwrap();
    let denied = child::<(), AuthorizeError>(Err(AuthorizeError::PermissionDenied));
    assert_eq!(
        checking_access().next(denied),
        Err(permanent(format!(
            "harvest write to [{path}] is not permitted"
        )))
    );
}

#[test]
fn unavailable_access_retries() {
    let failed = child::<(), AuthorizeError>(Err(AuthorizeError::CheckFailed("later".into())));
    assert_eq!(
        checking_access().next(failed),
        Err(retryable(
            "harvest permission check: authorization check failed: later"
        ))
    );
}
