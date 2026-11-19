//! Tests of the calls and results of the backend import operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::SystemTime;

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::jobs::{JobEffect, JobEvent, JobReport, JobStops};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobResultPayload;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::cleanup::CleanupStrategy;
use aruna_core::structs::storage::group_backend::GroupBackendKind;
use byteview::ByteView;

use super::job::{BackendImportOperation, ImportRunError};
use super::object::ImportObjectOperation;
use super::prepare::{PrepareRunOperation, RunEnd};
use super::reads::{
    PageReadsOperation, PrefixRowsOperation, StoredIntentsOperation, TakenPathsOperation, exact_of,
    head_of, read_call,
};
use super::*;
use crate::jobs::page_intents::IntentReadError;
use crate::jobs::run_rows::StateReadError;
use crate::jobs::store::mutation_error::JobMutationError;

fn answer<T, E>(outcome: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(outcome))
}

fn job_event(event: JobEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Job(event)))
}

fn job() -> ImportJob {
    let realm_id = RealmId::from_bytes([1; 32]);
    ImportJob {
        spec: Arc::new(BackendImportSpec {
            auth_context: AuthContext {
                user_id: UserId::local(Ulid::from_bytes([2; 16]), realm_id),
                realm_id,
                path_restrictions: None,
                session: None,
            },
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            group_id: Ulid::from_bytes([3; 16]),
            import_id: Ulid::from_bytes([5; 16]),
        }),
        job_id: JobId::from_bytes([6; 16]),
        claim_token: Ulid::from_bytes([7; 16]),
        routing: Default::default(),
        seed: 11,
    }
}

fn rule() -> BackendImport {
    let spec = job().spec;
    BackendImport {
        import_id: spec.import_id,
        group_id: spec.group_id,
        backend_id: Ulid::from_bytes([8; 16]),
        backend_prefix: "raw/".to_string(),
        bucket: "bucket".to_string(),
        key_prefix: "in/".to_string(),
        schedule: None,
        created_by: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        last_job: None,
        last_started_ms: None,
        write_backend_id: None,
        write_prefix: None,
        write_pattern: String::new(),
    }
}

fn run() -> ImportRun {
    let rule = rule();
    let routing = NodeRouting::default().snapshot(rule.group_id);
    ImportRun {
        earlier: Vec::new(),
        backend: GroupStorage {
            backend_id: rule.backend_id,
            group_id: rule.group_id,
            name: "backend".to_string(),
            kind: GroupBackendKind::S3,
            public_config: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: false,
            cleanup: CleanupStrategy::Retain,
        },
        bucket: BucketInfo {
            group_id: rule.group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: None,
        },
        incarnation: 0,
        routing,
        quota: None,
        gate: None,
        intents: Default::default(),
        discovery: Vec::new(),
        shared: Vec::new(),
        page: PageReads::default(),
        rule,
    }
}

fn object(key: &str) -> ExternalObject {
    ExternalObject {
        key: key.to_string(),
        size: 4,
        etag: None,
        modified_ms: None,
    }
}

fn import(run: ImportRun, key: &str) -> ImportObjectOperation {
    ImportObjectOperation::new(job(), Arc::new(run), object(key), 1)
}

// A file outside the rule's folder is not counted at all.
#[test]
fn foreign_path_ignored() {
    let mut import = import(run(), "elsewhere/a");
    assert_eq!(import.first(), Ok(Next::Done(Outcome::Ignored)));
}

// An empty file a write's intent names is that write's reservation.
#[test]
fn reservation_skipped() {
    let mut run = run();
    let intent = WriteIntent {
        key: "in/a".to_string(),
        version_id: Ulid::from_bytes([1; 16]),
        backend: BackendRef::Group(run.rule.backend_id),
        created_by: Default::default(),
        written: None,
        mover: None,
    };
    run.intents
        .insert("raw/a".to_string(), vec![(Key::from(vec![1]), intent)]);
    let mut import = ImportObjectOperation::new(
        job(),
        Arc::new(run),
        ExternalObject {
            size: 0,
            ..object("raw/a")
        },
        1,
    );
    assert_eq!(import.first(), Ok(Next::Done(Outcome::Skipped)));
}

// A listed file without a page decision reads its row first.
#[test]
fn object_reads_row() {
    let run = run();
    let row = object_key(run.rule.import_id, "raw/a");
    let mut import = import(run, "raw/a");
    assert_eq!(
        import.first(),
        Ok(Next::Call(read_call([(IMPORTED_OBJECT_KEYSPACE, row)])))
    );
}

// Rows of one key space are gathered across its pages.
#[test]
fn prefix_rows_gathered() {
    let mut rows = PrefixRowsOperation::new("space", Key::from(vec![1]), 2);
    assert!(matches!(rows.first(), Ok(Next::Call(IoCall::Page { .. }))));
    let row = (Key::from(vec![1, 2]), Value::from(vec![3]));
    let page = IoResult::Page {
        rows: vec![row.clone()],
        next: Some(row.0.clone()),
    };
    assert!(matches!(
        rows.next(page),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let last = IoResult::Page {
        rows: Vec::new(),
        next: None,
    };
    assert_eq!(rows.next(last), Ok(Next::Done(vec![row])));
}

// A tagged intent of the same write is found; one of another write is left alone.
#[test]
fn stored_intents_match() {
    let (import_id, id) = (rule().import_id, Ulid::from_bytes([9; 16]));
    let mut intents = StoredIntentsOperation::new(import_id, "raw/a", id);
    assert!(matches!(intents.first(), Ok(Next::Call(IoCall::Child(_)))));
    let tagged = |version: Ulid| {
        let path = format!("raw/a~{}", Ulid::from_bytes([1; 16]));
        let mut key = object_key(import_id, &path).to_vec();
        key.push(0);
        key.extend_from_slice(&version.to_bytes());
        Key::from(key)
    };
    let (own, other) = (tagged(id), tagged(Ulid::from_bytes([8; 16])));
    let rows = vec![
        (own.clone(), Value::from(Vec::new())),
        (other.clone(), Value::from(Vec::new())),
    ];
    // The file's path and its free path are both read, also when they are the same.
    let turn = intents.next(answer(Ok::<_, ItemError>(rows)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let free = answer(Ok::<_, ItemError>(Vec::<(Key, Value)>::new()));
    let Ok(Next::Done(keys)) = intents.next(free) else {
        panic!("expected the stored intents");
    };
    assert!(keys.contains(&own));
    assert!(!keys.contains(&other));
}

// A version row decides what a file naming it may do.
#[test]
fn exact_states() {
    assert_eq!(exact_of(None), Ok(Exact::Absent));
    assert!(matches!(
        exact_of(Some(Value::from(vec![0xFF]))),
        Err(ItemError::System(_))
    ));
}

// A page with no rows asks for no heads; a missing row stays absent.
#[test]
fn page_reads_empty() {
    let mut reads = PageReadsOperation::new(rule(), vec![object("raw/a")]);
    assert!(matches!(reads.first(), Ok(Next::Call(IoCall::Read(_)))));
    let rows = IoResult::Values(vec![None]);
    assert_eq!(reads.next(rows), Ok(Next::Call(IoCall::Read(Vec::new()))));
    let Ok(Next::Done(page)) = reads.next(IoResult::Values(Vec::new())) else {
        panic!("expected the page");
    };
    assert_eq!(page.rows.get("raw/a"), Some(&None));
}

// Without another rule of the folder no path can be taken.
#[test]
fn taken_without_rules() {
    let mut taken = TakenPathsOperation::new(&[], &[object("raw/a")]);
    assert_eq!(taken.first(), Ok(Next::Done(HashSet::new())));
}

// Each other rule is read in both key spaces, and only listed paths count.
#[test]
fn taken_reads_rules() {
    let other = SharedRule {
        rule: rule(),
        earlier: Vec::new(),
        discovery: Vec::new(),
    };
    let import_id = other.rule.import_id;
    let mut taken = TakenPathsOperation::new(&[other], &[object("raw/a")]);
    assert!(matches!(taken.first(), Ok(Next::Call(IoCall::Effect(_)))));
    let iter = |values| {
        IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        })))
    };
    let row = vec![(object_key(import_id, "raw/a"), Value::from(Vec::new()))];
    assert!(matches!(taken.next(iter(row)), Ok(Next::Call(_))));
    let Ok(Next::Done(paths)) = taken.next(iter(Vec::new())) else {
        panic!("expected the taken paths");
    };
    assert!(paths.contains("raw/a"));
}

// A removed rule ends the job for good; an unreadable row only retries it.
#[test]
fn prepare_ends_run() {
    let mut prepare = PrepareRunOperation::new(job());
    assert!(matches!(prepare.first(), Ok(Next::Call(IoCall::Read(_)))));
    let Err(RunEnd(JobRunOutcome::Failed(error))) = prepare.next(IoResult::Values(vec![None]))
    else {
        panic!("a missing rule ends the run");
    };
    assert_eq!(error.message, "the import rule was removed");
    let mut broken = PrepareRunOperation::new(job());
    broken.first().unwrap();
    let row = IoResult::Values(vec![Some(ByteView::from(vec![0xFF]))]);
    assert!(matches!(broken.next(row), Err(RunEnd(_))));
}

fn started() -> BackendImportOperation {
    let mut import = BackendImportOperation::new(job());
    assert!(matches!(import.first(), Ok(Next::Call(IoCall::Child(_)))));
    let stored = answer(Ok::<_, StateReadError>(None::<BackendImportCheckpoint>));
    assert!(matches!(
        import.next(stored),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    import
}

fn prepared(import: &mut BackendImportOperation) {
    assert!(matches!(
        import.next(answer(Ok::<_, RunEnd>(run()))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
}

fn listed(import: &mut BackendImportOperation, objects: Vec<ExternalObject>, next: Option<&str>) {
    let event = BlobEvent::ExternalListed {
        objects,
        next: next.map(str::to_string),
    };
    assert!(matches!(
        import.next(answer(Ok::<_, BlobError>(event))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
}

fn page_read(import: &mut BackendImportOperation) -> Result<Next<JobRunOutcome>, ImportRunError> {
    let intents = answer(Ok::<_, IntentReadError>(PathIntents::new()));
    import.next(intents).unwrap();
    import
        .next(answer(Ok::<_, ItemError>(PageReads::default())))
        .unwrap();
    import.next(answer(Ok::<_, ItemError>(HashSet::<String>::new())))
}

fn stops(cancelled: bool, shutdown: bool) -> IoResult {
    job_event(JobEvent::Stops(JobStops {
        cancelled,
        shutdown,
    }))
}

fn written() -> IoResult {
    answer(Ok::<(), JobMutationError>(()))
}

fn job_call(effect: JobEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Job(effect)))
}

// An empty last page writes the checkpoint and ends with the counts.
#[test]
fn empty_listing_succeeds() {
    let mut import = started();
    prepared(&mut import);
    listed(&mut import, Vec::new(), None);
    assert!(matches!(
        page_read(&mut import),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let report = JobReport {
        current: Some(0),
        ..JobReport::default()
    };
    assert_eq!(
        import.next(written()),
        Ok(Next::Call(job_call(JobEffect::Report(report))))
    );
    let done = import.next(job_event(JobEvent::Reported));
    assert_eq!(
        done,
        Ok(Next::Done(JobRunOutcome::Succeeded(
            JobResultPayload::BackendImport {
                imported: 0,
                skipped: 0,
                failed: 0,
            }
        )))
    );
}

// A stop is honored before the next object.
#[test]
fn stops_end_run() {
    let at_object = || {
        let mut import = started();
        prepared(&mut import);
        listed(&mut import, vec![object("raw/a")], None);
        assert_eq!(
            page_read(&mut import),
            Ok(Next::Call(job_call(JobEffect::Stops)))
        );
        import
    };
    assert_eq!(
        at_object().next(stops(true, false)),
        Ok(Next::Done(JobRunOutcome::Cancelled))
    );
    assert_eq!(
        at_object().next(stops(false, true)),
        Ok(Next::Done(JobRunOutcome::Interrupted))
    );
}

// A skip goes on without a write; an imported object is persisted at once.
#[test]
fn imported_object_persists() {
    let mut import = started();
    prepared(&mut import);
    listed(&mut import, vec![object("raw/a"), object("raw/b")], None);
    page_read(&mut import).unwrap();
    assert!(matches!(
        import.next(stops(false, false)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(
        import.next(answer(Ok::<_, ItemError>(Outcome::Skipped))),
        Ok(Next::Call(job_call(JobEffect::Stops)))
    );
    import.next(stops(false, false)).unwrap();
    assert!(matches!(
        import.next(answer(Ok::<_, ItemError>(Outcome::Imported))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(import.checkpoint.imported, 1);
    assert_eq!(import.checkpoint.cursor.as_deref(), Some("raw/b"));
}

// A system failure of one object ends the run before its cursor moves.
#[test]
fn system_failure_retries() {
    let mut import = started();
    prepared(&mut import);
    listed(&mut import, vec![object("raw/a")], None);
    page_read(&mut import).unwrap();
    import.next(stops(false, false)).unwrap();
    let failed = answer(Err::<Outcome, _>(ItemError::System("busy".to_string())));
    assert_eq!(import.next(failed), Ok(Next::Done(retryable("busy"))));
    assert_eq!(import.checkpoint.cursor, None);
}

// The head row of a key decodes to its version.
#[test]
fn head_decodes() {
    assert_eq!(head_of(IoResult::Values(vec![None])), Ok(None));
}
