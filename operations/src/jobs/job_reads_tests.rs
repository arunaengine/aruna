//! Call and result tests of the local job reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::errors::StorageError;
use aruna_core::structs::MintPersistentSpec;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;

use super::*;
use crate::driver::{DriverContext, drive};
use crate::jobs::job_cancel::{CancelJobOutcome, CancelOwnedOperation};
use crate::jobs::job_delete::DeleteOwnedOperation;
use crate::jobs::job_reports::{
    JobReportLookup, JobReportOperation, ReportQuery, ReportRouteOperation,
};
use crate::jobs::runtime::JobsRuntime;
use crate::jobs::store::RunDelete;
use aruna_core::time::unix_timestamp_millis;

pub(crate) fn realm() -> RealmId {
    RealmId([1u8; 32])
}

pub(crate) fn owner() -> UserId {
    UserId::new(Ulid::from_bytes([2u8; 16]), realm())
}

pub(crate) fn stranger() -> UserId {
    UserId::new(Ulid::from_bytes([4u8; 16]), realm())
}

pub(crate) fn job_id() -> JobId {
    JobId::from_bytes([3u8; 16])
}

pub(crate) fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

pub(crate) fn auth_of(user_id: UserId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    }
}

pub(crate) fn probe() -> JobPayload {
    JobPayload::Probe {
        steps: 1,
        step_sleep_ms: 0,
        fail_at: None,
        panic_at: None,
        cleanup_marker: None,
    }
}

pub(crate) fn record_of(payload: JobPayload) -> JobRecord {
    JobRecord::new(job_id(), payload, owner(), node(7), 1, 1, None)
}

pub(crate) fn child<T: Send + 'static, E: Send + 'static>(result: Result<T, E>) -> IoResult {
    IoResult::Child(ChildOutcome::new(result))
}

pub(crate) fn stored(record: Option<JobRecord>) -> IoResult {
    child(Ok::<_, JobMutationError>(record))
}

pub(crate) fn is_child<T>(next: &Next<T>) -> bool {
    matches!(next, Next::Call(IoCall::Child(_)))
}

pub(crate) async fn read_owned_job(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
) -> Result<Option<JobRecord>, String> {
    drive(OwnedJobOperation::new(user_id, job_id), context)
        .await
        .map_err(|error| error.0)
}

pub(crate) async fn read_owned_report(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    query: ReportQuery,
) -> Result<JobReportLookup, String> {
    let now_ms = unix_timestamp_millis();
    let read = JobReportOperation::owned(user_id, job_id, query, now_ms);
    drive(read, context).await.map_err(|error| error.0)
}

pub(crate) async fn read_report_routed(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
    query: ReportQuery,
) -> Result<JobReportLookup, JobRouteError> {
    let serving = context.node_and_realm();
    let now_ms = unix_timestamp_millis();
    let route = ReportRouteOperation::new(auth, job_id, query, None, serving, now_ms);
    drive(route, context).await
}

pub(crate) async fn delete_owned_run(
    context: &DriverContext,
    auth: &AuthContext,
    job_id: JobId,
) -> Result<RunDelete, JobMutationError> {
    let serving = context.node_and_realm();
    let delete = DeleteOwnedOperation::new(auth, job_id, serving, unix_timestamp_millis());
    drive(delete, context).await
}

pub(crate) async fn cancel_owned_job(
    context: &DriverContext,
    runtime: &JobsRuntime,
    user_id: UserId,
    job_id: JobId,
) -> Result<CancelJobOutcome, String> {
    let cancel = CancelOwnedOperation::new(user_id, job_id, unix_timestamp_millis());
    let report = drive(cancel, context).await.map_err(|error| error.0)?;
    for running in report.running {
        runtime.request_cancel(running);
    }
    Ok(report.outcome)
}

fn mint_record() -> JobRecord {
    record_of(JobPayload::MintPersistentId(MintPersistentSpec {
        document_id: Ulid::from_bytes([6u8; 16]),
        minted_by: owner(),
    }))
}

fn document() -> MetadataRegistryRecord {
    MetadataRegistryRecord {
        realm_id: realm(),
        group_id: Ulid::from_bytes([5u8; 16]),
        document_id: Ulid::from_bytes([6u8; 16]),
        document_path: "dataset".to_string(),
        graph_iri: String::new(),
        public: false,
        permission_path: "documents/dataset".to_string(),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: Ulid::nil(),
        last_event_id: Ulid::nil(),
    }
}

fn document_result() -> IoResult {
    let bytes = postcard::to_allocvec(&document()).unwrap();
    IoResult::Values(vec![Some(bytes.into())])
}

// The owner reads a job; another user and an internal job read as absent.
#[test]
fn owned_hides_others() {
    let mut read = OwnedJobOperation::new(owner(), job_id());
    assert!(is_child(&read.first().unwrap()));
    let found = read.next(stored(Some(record_of(probe())))).unwrap();
    assert!(matches!(found, Next::Done(Some(_))));

    let mut read = OwnedJobOperation::new(stranger(), job_id());
    let found = read.next(stored(Some(record_of(probe())))).unwrap();
    assert!(matches!(found, Next::Done(None)));

    let internal = JobPayload::WriteRunCrate {
        for_job: JobId::from_bytes([9u8; 16]),
    };
    let mut read = OwnedJobOperation::new(owner(), job_id());
    let found = read.next(stored(Some(record_of(internal)))).unwrap();
    assert!(matches!(found, Next::Done(None)));
}

// A joined mint job is served as the caller's own once WRITE on its document holds.
#[test]
fn joined_mint_authorized() {
    let auth = auth_of(stranger());
    let mut read = ReadableJobOperation::new(&auth, job_id(), None);
    let next = read.next(stored(Some(mint_record()))).unwrap();
    assert!(matches!(next, Next::Call(IoCall::Read(rows)) if rows.len() == 1));
    assert!(is_child(&read.next(document_result()).unwrap()));
    match read.next(child(Ok::<_, AuthorizeError>(()))).unwrap() {
        Next::Done(Some(record)) => assert_eq!(record.created_by, stranger()),
        other => panic!("unexpected step: {other:?}"),
    }
}

// A denied check hides the job, while a failed check is an error.
#[test]
fn readable_check_verdicts() {
    let auth = auth_of(stranger());
    let start = || {
        let mut read = ReadableJobOperation::new(&auth, job_id(), None);
        read.next(stored(Some(mint_record()))).unwrap();
        read.next(document_result()).unwrap();
        read
    };
    let denied = child(Err::<(), _>(AuthorizeError::PermissionDenied));
    assert!(matches!(start().next(denied), Ok(Next::Done(None))));
    let failed = child(Err::<(), _>(AuthorizeError::CheckFailed("x".to_string())));
    assert_eq!(start().next(failed).unwrap_err(), JobReadError("x".into()));
    let lost = child(Err::<(), _>(AuthorizeError::Storage(
        StorageError::TransactionConflict,
    )));
    assert!(start().next(lost).is_err());
}

// A link push asks for the group admin first and the dataset second.
#[test]
fn push_checks_order() {
    let read = ReadableJobOperation::new(&auth_of(stranger()), job_id(), None);
    let group_id = Ulid::from_bytes([5u8; 16]);
    let mut checks = read.checks(document(), ReadGrant::LinkedPush(group_id));
    let admin = checks.pop().unwrap();
    assert_eq!(admin.0, group_admin_path(realm(), group_id));
    assert_eq!(admin.1, Permission::WRITE);
    assert_eq!(
        checks.pop().unwrap(),
        ("documents/dataset".to_string(), Permission::READ)
    );
}

// A job without a reader grant stays hidden and a missing document ends the read.
#[test]
fn foreign_stays_hidden() {
    let auth = auth_of(stranger());
    let mut read = ReadableJobOperation::new(&auth, job_id(), None);
    let found = read.next(stored(Some(record_of(probe())))).unwrap();
    assert!(matches!(found, Next::Done(None)));

    let mut read = ReadableJobOperation::new(&auth, job_id(), None);
    read.next(stored(Some(mint_record()))).unwrap();
    let missing = read.next(IoResult::Values(vec![None])).unwrap();
    assert!(matches!(missing, Next::Done(None)));
}

// An unreadable job is absent, and a key-waiting job also reports its waits.
#[test]
fn local_status_steps() {
    let auth = auth_of(owner());
    let mut status = LocalStatusOperation::new(&auth, job_id(), None);
    assert!(is_child(&status.first().unwrap()));
    let missing = child(Ok::<Option<JobRecord>, JobReadError>(None));
    assert_eq!(status.next(missing).unwrap_err(), JobRouteError::NotFound);

    let mut record = record_of(probe());
    record.state = JobState::AwaitingKey;
    let mut status = LocalStatusOperation::new(&auth, job_id(), None);
    let found = child(Ok::<_, JobReadError>(Some(record)));
    assert!(is_child(&status.next(found).unwrap()));
    let no_crate = child(Ok::<Option<RunCrateStatus>, JobMutationError>(None));
    assert!(is_child(&status.next(no_crate).unwrap()));
    let waits = child(Ok::<Vec<KeyWait>, JobMutationError>(Vec::new()));
    let done = status.next(waits).unwrap();
    assert!(matches!(done, Next::Done(status) if status.run_crate.is_none()));
}

// A job that waits for nothing ends with its run crate read.
#[test]
fn local_status_plain() {
    let mut status = LocalStatusOperation::new(&auth_of(owner()), job_id(), None);
    let found = child(Ok::<_, JobReadError>(Some(record_of(probe()))));
    status.next(found).unwrap();
    let crate_status = child(Ok::<_, JobMutationError>(Some(RunCrateStatus::Pending)));
    let done = status.next(crate_status).unwrap();
    assert!(matches!(done, Next::Done(status) if status.run_crate.is_some()));
}

// Only a staging job reads a checkpoint, and its read failure is reported.
#[test]
fn record_data_steps() {
    let mut read = RecordDataOperation::new(owner(), job_id());
    assert!(is_child(&read.first().unwrap()));
    let found = child(Ok::<_, JobReadError>(Some(record_of(probe()))));
    assert!(matches!(read.next(found), Ok(Next::Done(Some((_, None))))));

    let mut read = RecordDataOperation::new(owner(), job_id());
    read.record = Some(Box::new(record_of(probe())));
    let failed = child(Err::<Option<StagingJobCheckpoint>, _>(StagingStoreError(
        "broken".to_string(),
    )));
    assert_eq!(
        read.next(failed).unwrap_err(),
        JobReadError("broken".into())
    );
}

// The owner read needs a network, and an unreadable config is unavailable.
#[test]
fn owner_needs_config() {
    let mut owner_read = JobOwnerOperation::new(job_id(), None);
    assert!(matches!(
        owner_read.first(),
        Err(JobRouteError::Unavailable(_))
    ));
    let mut owner_read = JobOwnerOperation::new(job_id(), Some((node(1), realm())));
    assert!(is_child(&owner_read.first().unwrap()));
    let missing = child(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    assert_eq!(
        owner_read.next(missing).unwrap_err(),
        unavailable("realm config unavailable")
    );
}
