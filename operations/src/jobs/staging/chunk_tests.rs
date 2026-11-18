//! Tests of the calls and results of reading a table chunk and writing its rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::events::Event;

use super::*;
use crate::jobs::staging::pure_tests::{child_answer, test_bucket, test_connector, test_run};
use crate::jobs::staging::{StagingRun, initial_checkpoint};
use crate::jobs::store::JobMutationError;
use crate::s3::object::get::ObjectInfo;
use aruna_core::structs::storage::multipart::MultipartChecksumType;

fn table_run() -> StagingRun {
    let run = test_run();
    let mut spec = (*run.spec).clone();
    let rows = serde_json::json!({ "kind": "file", "source": "{a}", "target": "{a}" });
    let table = serde_json::json!({ "source": { "connector": "list.csv" }, "rows": rows });
    spec.table = Some(serde_json::from_value(table).unwrap());
    StagingRun {
        spec: Arc::new(spec),
        ..run
    }
}

fn observation(length: u64) -> SourceMetadata {
    SourceMetadata {
        content_length: length,
        content_type: None,
        etag: None,
        last_modified: None,
        source_version: None,
        digests: Default::default(),
    }
}

fn reader(read: TableRead) -> ChunkReadOperation {
    let origin = Origin::Connector {
        path: "list.csv".to_string(),
    };
    ChunkReadOperation::new(table_run(), origin, read, None, 16)
}

fn allowed() -> IoResult {
    child_answer(Ok::<bool, AuthorizationError>(true))
}

fn job_event(event: JobEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Job(event)))
}

fn finished<T, E>(outcome: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    let child = Event::Child(aruna_core::calls::ChildOutcome::new(outcome));
    job_event(JobEvent::Finished(Box::new(child)))
}

fn stream() -> ByteStream {
    BackendStream::new(futures_util::stream::empty::<Result<Bytes, StreamError>>())
}

fn read_answer(metadata: SourceMetadata) -> IoResult {
    finished(Ok::<_, ReadSourceError>(ReadSourceResult {
        connector: test_connector(),
        metadata,
        stream: stream(),
    }))
}

// A denied table is recorded once and ends, whatever is left of it.
#[test]
fn denied_table_lasts() {
    let mut chunk = reader(TableRead::default());
    assert!(matches!(chunk.first(), Ok(Next::Call(IoCall::Child(_)))));
    let denied = child_answer(Ok::<bool, AuthorizationError>(false));
    assert_eq!(
        chunk.next(denied),
        Err(ChunkFailure::Lasting("table read denied".to_string()))
    );
}

// The first read is bound to the table's HEAD; its bytes come back with that observation kept.
#[test]
fn first_chunk_observed() {
    let mut chunk = reader(TableRead::default());
    chunk.first().unwrap();
    assert!(matches!(
        chunk.next(allowed()),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let head = finished(Ok::<_, HeadSourceError>(HeadSourceResult {
        connector: test_connector(),
        secret_fingerprint: None,
        metadata: observation(4),
    }));
    assert!(matches!(
        chunk.next(head),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert!(matches!(
        chunk.next(read_answer(observation(4))),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let bytes = job_event(JobEvent::Collected(Ok(b"a,b\n".to_vec())));
    let Ok(Next::Done(read)) = chunk.next(bytes) else {
        panic!("expected the chunk");
    };
    assert!(read.chunk.complete);
    assert_eq!(read.chunk.bytes, b"a,b\n");
    assert_eq!(read.chunk.read.gzip, Some(false));
    assert_eq!(read.chunk.read.observation, Some(observation(4)));
}

// Reading from the end of a known table gives nothing and needs no request.
#[test]
fn end_reads_empty() {
    let read = TableRead {
        offset: 4,
        observation: Some(observation(4)),
        gzip: Some(false),
        ..TableRead::default()
    };
    let mut chunk = reader(read);
    chunk.first().unwrap();
    let Ok(Next::Done(read)) = chunk.next(allowed()) else {
        panic!("expected the end of the table");
    };
    assert!(read.chunk.complete && read.chunk.bytes.is_empty());
}

// A table that changed under a bound read starts over.
#[test]
fn changed_table_restarts() {
    let read = TableRead {
        observation: Some(observation(4)),
        gzip: Some(false),
        ..TableRead::default()
    };
    let mut chunk = reader(read);
    chunk.first().unwrap();
    chunk.next(allowed()).unwrap();
    let failed = finished(Err::<ReadSourceResult, _>(ReadSourceError::Staging(
        StagingSourceError::ConditionFailed,
    )));
    assert_eq!(chunk.next(failed), Err(ChunkFailure::Changed));
}

// A broken stream kept from the last step is opened again; a broken new one retries the chunk.
#[test]
fn broken_gzip_reopens() {
    let read = TableRead {
        observation: Some(observation(4)),
        gzip: Some(true),
        ..TableRead::default()
    };
    let kept = GzipStream {
        version_id: None,
        observation: Some(observation(4)),
        work: GzipWork {
            stream: stream(),
            cursor: GzipCursor::new(0, 16),
        },
    };
    let origin = Origin::Connector {
        path: "list.csv".to_string(),
    };
    let mut chunk = ChunkReadOperation::new(table_run(), origin, read, Some(kept), 16);
    chunk.first().unwrap();
    let Ok(Next::Call(IoCall::Effect(fill))) = chunk.next(allowed()) else {
        panic!("expected the kept stream to fill");
    };
    let Effect::Job(JobEffect::FillGzip(work)) = *fill else {
        panic!("expected the kept stream to fill");
    };
    assert_eq!(chunk.step, ChunkStep::Fill { reused: true });
    let broken = job_event(JobEvent::GzipFilled {
        work,
        result: Err(GzipBroken::Stream("reset".to_string())),
    });
    assert!(matches!(
        chunk.next(broken),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert_eq!(chunk.step, ChunkStep::OpenGzip { reused: false });
    assert_eq!(chunk.kept, None);
    let Ok(Next::Call(IoCall::Effect(fill))) = chunk.next(read_answer(observation(4))) else {
        panic!("expected the new stream to fill");
    };
    let Effect::Job(JobEffect::FillGzip(work)) = *fill else {
        panic!("expected the new stream to fill");
    };
    let broken = job_event(JobEvent::GzipFilled {
        work,
        result: Err(GzipBroken::Stream("reset".to_string())),
    });
    assert_eq!(
        chunk.next(broken),
        Err(ChunkFailure::Retry("reset".to_string()))
    );
}

// Bytes a new stream delivers that are no gzip end the table; they will not get better.
#[test]
fn invalid_gzip_lasts() {
    let mut chunk = reader(TableRead::default());
    chunk.step = ChunkStep::Fill { reused: false };
    let broken = job_event(JobEvent::GzipFilled {
        work: GzipWork {
            stream: stream(),
            cursor: GzipCursor::new(0, 16),
        },
        result: Err(GzipBroken::Gzip("bad header".to_string())),
    });
    assert_eq!(
        chunk.next(broken),
        Err(ChunkFailure::Lasting("bad header".to_string()))
    );
}

fn object_reader() -> ChunkReadOperation {
    let origin = Origin::Object {
        bucket: "tables".to_string(),
        key: "t.csv".to_string(),
        group_id: None,
    };
    let read = TableRead {
        gzip: Some(false),
        ..TableRead::default()
    };
    ChunkReadOperation::new(table_run(), origin, read, None, 16)
}

fn found_object(version: Ulid) -> GetObjectResult {
    GetObjectResult {
        blob: stream(),
        location: None,
        metadata: Default::default(),
        info: ObjectInfo {
            size: 3,
            version_created_at: None,
            etag: None,
            checksum_type: MultipartChecksumType::FullObject,
            hashes: Default::default(),
            composite_hashes: Default::default(),
            part_count: None,
        },
        source_metadata: None,
        source_binding: None,
        last_refresh: None,
        version_id: Some(version),
        resolved_version_id: None,
        resolved_range: None,
        source_policies: Vec::new(),
        served_source: None,
    }
}

// An uploaded table is pinned to the version its first read found.
#[test]
fn object_version_pinned() {
    let run = table_run();
    let mut chunk = object_reader();
    assert!(matches!(chunk.first(), Ok(Next::Call(IoCall::Child(_)))));
    let bucket = child_answer(Ok::<_, GetBucketError>(test_bucket(&run.spec)));
    assert!(matches!(
        chunk.next(bucket),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let expected = chunk.object_call(Some((0, 15))).unwrap();
    assert_eq!(chunk.next(allowed()), Ok(Next::Call(expected)));
    let version = Ulid::from_bytes([3; 16]);
    assert!(matches!(
        chunk.next(child_answer(Ok::<_, GetObjectError>(found_object(version)))),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let bytes = job_event(JobEvent::Collected(Ok(b"abc".to_vec())));
    let Ok(Next::Done(read)) = chunk.next(bytes) else {
        panic!("expected the chunk");
    };
    assert_eq!(read.chunk.read.version_id, Some(version));
    assert!(read.chunk.complete);
}

// A range past the object's end is the table's end; a missing object ends the table.
#[test]
fn object_failures_map() {
    let opened = |failure| {
        let mut chunk = object_reader();
        chunk.step = ChunkStep::OpenRange;
        if let Origin::Object { group_id, .. } = &mut chunk.origin {
            *group_id = Some(Ulid::nil());
        }
        chunk.next(child_answer(Err::<GetObjectResult, _>(failure)))
    };
    let Ok(Next::Done(read)) = opened(GetObjectError::InvalidRange) else {
        panic!("expected the end of the table");
    };
    assert!(read.chunk.complete);
    assert!(matches!(
        opened(GetObjectError::NoSuchKey),
        Err(ChunkFailure::Lasting(_))
    ));
    assert!(matches!(
        opened(GetObjectError::NoTransactionFound),
        Err(ChunkFailure::Retry(_))
    ));
}

fn step(checkpoint: &StagingJobCheckpoint) -> TableChunkOperation {
    let mut step = TableChunkOperation::new(table_run(), checkpoint, None);
    assert!(matches!(step.first(), Ok(Next::Call(IoCall::Child(_)))));
    let source = GetSourceResult {
        connector: test_connector(),
        has_secret_config: false,
    };
    let answer = child_answer(Ok::<_, GetSourceError>(source));
    assert!(matches!(
        step.next(answer),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    step
}

fn table_checkpoint() -> StagingJobCheckpoint {
    initial_checkpoint(&table_run().spec)
}

// A table that cannot be read is recorded and closed in one checkpoint write.
#[test]
fn lasting_failure_recorded() {
    let mut chunk = step(&table_checkpoint());
    let failed = child_answer(Err::<ChunkRead, _>(ChunkFailure::Lasting("gone".into())));
    assert!(matches!(
        chunk.next(failed),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(chunk.next.table, None);
    assert_eq!(chunk.next.items_failed, 1);
    let written = child_answer(Ok::<(), JobMutationError>(()));
    assert!(matches!(chunk.next(written), Ok(Next::Done(_))));
}

// The first chunk only reads the header, kept in the checkpoint for the rows that follow.
#[test]
fn header_read_first() {
    let mut chunk = step(&table_checkpoint());
    let read = ChunkRead {
        chunk: Chunk {
            bytes: b"a\nx\n".to_vec(),
            complete: true,
            read: TableRead::default(),
        },
        kept: None,
    };
    let answer = child_answer(Ok::<_, ChunkFailure>(read));
    assert!(matches!(
        chunk.next(answer),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(chunk.step, TableStep::Commit);
    let table = chunk.next.table.as_ref().expect("the table goes on");
    assert_eq!(table.columns, Some(vec!["a".to_string()]));
    assert_eq!(table.offset, 2);
}

fn table_entry(target: &str) -> TableEntry {
    TableEntry {
        source_path: String::new(),
        target: target.to_string(),
        metadata: BTreeMap::new(),
        size: None,
        digests: Vec::new(),
        modified: None,
        etag: None,
        content_type: None,
        mirrors: Vec::new(),
    }
}

// Rows without a source need write access; a denied one fails alone.
#[test]
fn empty_object_denied() {
    let run = table_run();
    let next = initial_checkpoint(&run.spec);
    let bucket = test_bucket(&run.spec);
    let mut empty = EmptyObjectsOperation::new(run, next, vec![table_entry("k")]);
    empty.first().unwrap();
    empty
        .next(child_answer(Ok::<_, GetBucketError>(bucket)))
        .unwrap();
    assert_eq!(empty.step, EmptyStep::Rules);
    let rules = child_answer(Ok::<_, AuthorizationError>(PermissionRules::default()));
    let Ok(Next::Done(checkpoint)) = empty.next(rules) else {
        panic!("expected the rows' end");
    };
    assert_eq!(checkpoint.items_failed, 1);
}

// An equal folder object stays; another head goes on to the ordinary put.
#[test]
fn equal_folder_stays() {
    let run = table_run();
    let bucket = test_bucket(&run.spec);
    let mut put = PutFolderOperation::new(run, bucket, "f/".to_string(), BTreeMap::new());
    put.first().unwrap();
    let missing = child_answer(Err::<HeadObjectResult, _>(HeadObjectError::NoSuchKey));
    assert!(matches!(
        put.next(missing),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(put.step, PutStep::Routing);
}
