//! Tests the run-crate job and the crate record by their calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};

use super::pure_tests::execution_record;
use super::*;

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[3; 32]).public()
}

fn realm() -> RealmId {
    RealmId([1; 32])
}

fn job() -> JobId {
    JobId::from_bytes([1; 16])
}

fn serving() -> ServingNode {
    Some((node(), realm()))
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn stored<T>(value: T) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
{
    child(Ok::<_, JobMutationError>(value))
}

fn is_child<T>(next: &Result<Next<T>, JobError>) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

fn done(resource: &str) -> WriteNext {
    Ok(Next::Done(CrateRun::Recorded(resource.to_string())))
}

fn write() -> WriteCrateOperation {
    let mut write = WriteCrateOperation::new(job(), serving());
    assert!(is_child(&write.first()));
    write
}

fn no_status() -> IoResult {
    stored(None::<RunCrateStatus>)
}

fn config(kind: Option<RealmNodeKind>) -> IoResult {
    let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    if let Some(kind) = kind {
        config.ensure_node(node(), kind);
    }
    child(Ok::<_, GetConfigError>(config))
}

/// A crate job that read its parent and waits for the realm configuration.
fn at_config() -> WriteCrateOperation {
    let mut operation = write();
    assert!(is_child(&operation.next(no_status())));
    let (parent, _) = execution_record();
    assert!(is_child(&operation.next(stored(Some(parent)))));
    operation
}

// A recorded outcome is returned without writing anything again.
#[test]
fn recorded_status_returns() {
    let written = RunCrateStatus::Written {
        resource: "doc".to_string(),
    };
    assert_eq!(write().next(stored(Some(written))), done("doc"));
    let denied = RunCrateStatus::Denied {
        message: "no".to_string(),
    };
    assert_eq!(write().next(stored(Some(denied))), done("denied"));
}

// A parent pruned first, or one that is no execution, leaves nothing to write.
#[test]
fn missing_parent_ends() {
    let mut gone = write();
    gone.next(no_status()).unwrap();
    assert_eq!(gone.next(stored(None::<JobRecord>)), done("parent-gone"));

    let (mut parent, _) = execution_record();
    parent.payload = JobPayload::WriteRunCrate { for_job: job() };
    let mut other = write();
    other.next(no_status()).unwrap();
    assert_eq!(other.next(stored(Some(parent))), done("not-execution"));
}

// A node the realm does not name yet decides nothing, and the job comes back.
#[test]
fn unnamed_node_defers() {
    let next = at_config().next(config(None));
    assert!(matches!(next, Ok(Next::Done(CrateRun::NotReady(_)))));
}

// A device keeps the crate of its run locally.
#[test]
fn device_keeps_crate() {
    let owner = execution_record().0.created_by;
    let mut operation = at_config();
    let kind = RealmNodeKind::User { owner };
    assert!(is_child(&operation.next(config(Some(kind)))));
    assert!(is_child(&operation.next(stored(None::<LaunchRecord>))));
    assert_eq!(operation.next(stored(())), done("local"));
}

// A device that already holds a minted id writes nothing new.
#[test]
fn minted_device_ends() {
    let owner = execution_record().0.created_by;
    let (parent, _) = execution_record();
    let mut operation = write();
    let minted = RunCrateStatus::Minted {
        document_id: Ulid::nil(),
    };
    operation.next(stored(Some(minted))).unwrap();
    operation.next(stored(Some(parent))).unwrap();
    let kind = RealmNodeKind::User { owner };
    assert_eq!(operation.next(config(Some(kind))), done("local"));
}

// The job ends with what the record of the crate answers.
#[test]
fn record_outcome_passes() {
    let mut operation = write();
    operation.next(no_status()).unwrap();
    let (parent, _) = execution_record();
    let mut no_net = WriteCrateOperation::new(job(), None);
    no_net.first().unwrap();
    no_net.next(no_status()).unwrap();
    assert!(is_child(&no_net.next(stored(Some(parent)))));
    assert_eq!(
        no_net.next(child(Ok::<_, JobError>("doc".to_string()))),
        done("doc")
    );
    let failed = JobError::retryable("no");
    let reply = child(Err::<String, _>(failed.clone()));
    let mut failing = WriteCrateOperation::new(job(), None);
    failing.first().unwrap();
    failing.next(no_status()).unwrap();
    failing.next(stored(Some(execution_record().0))).unwrap();
    assert_eq!(failing.next(reply), Err(failed));
}

fn record(saved_id: Option<Ulid>) -> RecordCrateOperation {
    let (parent, spec) = execution_record();
    RecordCrateOperation::new(job(), &parent, &spec, saved_id, serving())
}

fn meta_id() -> MetaResourceId {
    let handle = PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap();
    MetaResourceId::from_parts(1, handle, BucketId::new(9).unwrap(), 1).unwrap()
}

fn allowed(allowed: bool) -> IoResult {
    child(Ok::<_, AuthorizationError>(allowed))
}

/// A record that passed its mint and permission check and has no inputs left to ask about.
fn at_create() -> RecordCrateOperation {
    let mut operation = record(Some(Ulid::nil()));
    operation.inputs.clear();
    assert!(is_child(&operation.first()));
    let missing = child(Ok::<_, StorageReadError>(None::<MetadataRegistryRecord>));
    assert!(is_child(&operation.next(missing)));
    assert!(is_child(&operation.next(allowed(true))));
    assert!(is_child(&operation.next(stored(None::<LaunchRecord>))));
    operation
}

// Without a net handle nothing can be created.
#[test]
fn record_needs_net() {
    let (parent, spec) = execution_record();
    let mut operation = RecordCrateOperation::new(job(), &parent, &spec, None, None);
    assert_eq!(
        operation.first(),
        Err(JobError::retryable("run crate needs a net handle"))
    );
}

// A new id is stored before anything is created with it.
#[test]
fn minted_id_kept() {
    let mut operation = record(None);
    assert!(is_child(&operation.first()));
    assert_eq!(operation.step, RecordStep::Mint);
    let id = meta_id();
    let minted = child(Ok::<_, CreateDocumentError>(id));
    assert!(is_child(&operation.next(minted)));
    assert_eq!(operation.step, RecordStep::PutMinted);
    assert_eq!(operation.document_id, meta_id().as_ulid());
    assert!(is_child(&operation.next(stored(()))));
    assert_eq!(operation.step, RecordStep::Allowed);
}

// A refused write is recorded as the outcome and ends the obligation.
#[test]
fn denied_write_recorded() {
    let mut operation = record(Some(Ulid::nil()));
    operation.first().unwrap();
    let missing = child(Ok::<_, StorageReadError>(None::<MetadataRegistryRecord>));
    operation.next(missing).unwrap();
    assert!(is_child(&operation.next(allowed(false))));
    assert!(is_child(&operation.next(stored(None::<RunLink>))));
    assert_eq!(
        operation.next(stored(())),
        Ok(Next::Done("denied".to_string()))
    );
}

// A storage failure of the check retries the crate.
#[test]
fn check_failure_retries() {
    let mut operation = record(Some(Ulid::nil()));
    operation.first().unwrap();
    let missing = child(Ok::<_, StorageReadError>(None::<MetadataRegistryRecord>));
    operation.next(missing).unwrap();
    let failure = AuthorizationError::NotFinished;
    let error = operation.next(child(Err::<bool, _>(failure))).unwrap_err();
    assert!(error.message.starts_with("run crate authorization failed"));
}

// Only the exact version a holder describes names a device.
#[test]
fn device_exact_version() {
    let mut operation = record(None);
    operation.expect_remote = Some(Ulid::from_bytes([9; 16]));
    let denied = child(Err::<LocationSummary, _>(LocationSummaryError::Denied));
    assert_eq!(operation.device_of(denied), Ok(None));
    operation.expect_remote = None;
    let gone = child(Err::<HeadObjectResult, _>(HeadObjectError::NoSuchVersion));
    assert_eq!(operation.device_of(gone), Ok(None));
}

// The create is the step everything before it leads to.
#[test]
fn checks_reach_create() {
    assert_eq!(at_create().step, RecordStep::Create);
}
