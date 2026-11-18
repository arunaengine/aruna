//! Call and result tests of the job artifact reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::UNIX_EPOCH;

use aruna_core::jobs::WireArtifact;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::execution::job::ExportOmissionCounts;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
use aruna_core::structs::storage::format::StoredFormat;
use futures_util::stream;

use super::*;
use crate::jobs::job_reads::tests::{
    auth_of, child, is_child, job_id, node, owner, probe, realm, record_of, stranger,
};

const BLAKE3: [u8; 32] = [8u8; 32];

fn location() -> BackendLocation {
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/data".to_string(),
        storage_bucket: "storage".to_string(),
        backend_path: "_jobs/artifact".to_string(),
        ulid: Ulid::from_bytes([2u8; 16]),
        format: StoredFormat::default(),
        created_by: owner(),
        created_at: UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: 4,
        hashes: HashMap::from([(HASH_BLAKE3.to_string(), BLAKE3.to_vec())]),
    }
}

fn artifact() -> ArtifactRef {
    ArtifactRef {
        location: location(),
        blake3: BLAKE3,
        size: 4,
        expires_at_ms: 100,
    }
}

fn export_spec(auth: AuthContext) -> ExportRoCrateSpec {
    ExportRoCrateSpec {
        destination: None,
        auth_context: auth,
        document_id: Ulid::from_bytes([6u8; 16]),
        limits: Default::default(),
        selection: None,
    }
}

fn export_record(artifact: Option<ArtifactRef>) -> JobRecord {
    let mut record = record_of(JobPayload::ExportRoCrate(export_spec(auth_of(owner()))));
    record.state = JobState::Succeeded;
    record.result = Some(JobResultPayload::ExportRoCrate(
        aruna_core::structs::execution::job::ExportRoCrateResult {
            repository: None,
            artifact,
            included: 0,
            omitted: ExportOmissionCounts::default(),
            report_digest: [0u8; 32],
        },
    ));
    record
}

fn found(record: Option<JobRecord>) -> IoResult {
    child(Ok::<_, JobReadError>(record))
}

fn read(record: JobRecord, now_ms: u64) -> OwnedNext {
    OwnedArtifactOperation::new(owner(), job_id(), now_ms).next(found(Some(record)))
}

fn body() -> BackendStream<Result<Bytes, StreamError>> {
    BackendStream(Box::pin(stream::empty()))
}

fn wire() -> WireArtifact {
    WireArtifact {
        job_id: job_id(),
        created_by: owner(),
        blake3: BLAKE3,
        size: 4,
        filename: "artifact.zip".to_string(),
    }
}

fn route(range: Option<Range<u64>>, token: Option<AuthToken>) -> ArtifactRouteOperation {
    let serving = Some((node(1), realm()));
    ArtifactRouteOperation::new(owner(), job_id(), 5, range, token, serving)
}

fn token() -> AuthToken {
    AuthToken::internal(auth_of(owner()))
}

fn owned() -> OwnedArtifact {
    OwnedArtifact::from(wire())
}

#[test]
fn artifact_uses_stem() {
    let id = Ulid::from_bytes([1u8; 16]);
    let name = artifact_filename(Some("datasets/experiment.crate"), id);
    assert_eq!(name, "experiment.zip");
    assert_eq!(artifact_filename(None, id), format!("{id}.zip"));
}

// A verified artifact reads its document for the file name.
#[test]
fn ready_artifact_named() {
    let mut read_op = OwnedArtifactOperation::new(owner(), job_id(), 5);
    assert!(is_child(&read_op.first().unwrap()));
    let next = read_op.next(found(Some(export_record(Some(artifact())))));
    assert!(matches!(next, Ok(Next::Call(IoCall::Read(_)))));
    let done = read_op.next(IoResult::Values(vec![None])).unwrap();
    assert!(matches!(
        done,
        Next::Done(ArtifactLookup::Ready(ready))
            if ready.size == 4 && ready.filename.ends_with(".zip") && ready.source().is_some()
    ));
}

// Other jobs have no artifact, and a running export is pending.
#[test]
fn unfinished_artifact_states() {
    assert!(matches!(
        read(record_of(probe()), 5),
        Ok(Next::Done(ArtifactLookup::NotFound))
    ));
    assert!(matches!(
        read(export_record(None), 5),
        Ok(Next::Done(ArtifactLookup::NotFound))
    ));
    let mut running = export_record(None);
    running.state = JobState::Running;
    assert!(matches!(
        read(running, 5),
        Ok(Next::Done(ArtifactLookup::Pending(JobState::Running)))
    ));
}

// An expired artifact is gone, and one whose blob names other bytes is refused.
#[test]
fn artifact_validity_checked() {
    assert!(matches!(
        read(export_record(Some(artifact())), 100),
        Ok(Next::Done(ArtifactLookup::Gone))
    ));
    let mut other = artifact();
    other.blake3 = [9u8; 32];
    assert!(read(export_record(Some(other)), 5).is_err());
    let mut unhashed = artifact();
    unhashed.location.hashes.clear();
    assert!(read(export_record(Some(unhashed)), 5).is_err());
}

// A pruned job stays gone for its owner and unknown for everyone else.
#[test]
fn tombstone_owner_only() {
    let mut read_op = OwnedArtifactOperation::new(owner(), job_id(), 5);
    assert!(is_child(&read_op.next(found(None)).unwrap()));
    let tombstone = child(Ok::<_, JobMutationError>(Some(owner())));
    assert!(matches!(
        read_op.next(tombstone),
        Ok(Next::Done(ArtifactLookup::Gone))
    ));
    let mut read_op = OwnedArtifactOperation::new(owner(), job_id(), 5);
    read_op.next(found(None)).unwrap();
    let foreign = child(Ok::<_, JobMutationError>(Some(stranger())));
    assert!(matches!(
        read_op.next(foreign),
        Ok(Next::Done(ArtifactLookup::NotFound))
    ));
}

// The range read asks the blob store and hands its stream on.
#[test]
fn range_read_opens() {
    let mut range = ArtifactRangeOperation::new(&artifact(), 1..3);
    let Next::Call(IoCall::Effect(effect)) = range.first().unwrap() else {
        panic!("expected the blob effect");
    };
    assert!(matches!(
        *effect,
        Effect::Blob(BlobEffect::ReadHiddenRange { range, .. }) if range == (1..3)
    ));
    let opened = Event::Blob(BlobEvent::HiddenRead {
        blob: body(),
        stream_size: 2,
    });
    let done = range.next(IoResult::Event(Box::new(opened))).unwrap();
    assert!(matches!(done, Next::Done(read) if read.stream_size == 2));
}

#[test]
fn sizes_owners_checked() {
    assert!(artifact_size_matches(Some(&(1..3)), 2));
    assert!(!artifact_size_matches(Some(&(1..3)), 3));
    assert!(artifact_size_matches(None, 0));
    assert!(!artifact_size_matches(None, 1));
    let malformed = Range { start: 5, end: 2 };
    assert!(!artifact_size_matches(Some(&malformed), 0));

    let artifact = owned();
    assert!(artifact_job_matches(&artifact, owner(), job_id()));
    assert!(!artifact_job_matches(&artifact, stranger(), job_id()));
    let renamed = OwnedArtifact {
        filename: "renamed.zip".to_string(),
        ..owned()
    };
    assert!(artifact.same_content(&renamed));
    let changed = OwnedArtifact { size: 5, ..owned() };
    assert!(!artifact.same_content(&changed));
}

// Without a network the artifact is read here; a remote owner needs a token.
#[test]
fn route_chooses_owner() {
    let mut local = ArtifactRouteOperation::new(owner(), job_id(), 5, None, None, None);
    assert!(is_child(&local.first().unwrap()));
    assert_eq!(local.step, RouteStep::Local);

    let mut here = route(None, None);
    assert!(is_child(&here.first().unwrap()));
    let responder = child(Ok::<_, JobRouteError>(Some(node(1))));
    assert!(is_child(&here.next(responder).unwrap()));
    assert_eq!(here.step, RouteStep::Local);

    let mut remote = route(None, None);
    let responder = child(Ok::<_, JobRouteError>(Some(node(2))));
    assert_eq!(
        remote.next(responder).unwrap_err(),
        JobRouteError::Unauthorized
    );
}

// With no responder the owner comes from the job id.
#[test]
fn route_derives_owner() {
    let mut routed = route(None, Some(token()));
    let none = child(Ok::<Option<NodeId>, JobRouteError>(None));
    assert!(is_child(&routed.next(none).unwrap()));
    assert_eq!(routed.step, RouteStep::Owner);
    let owner_node = child(Ok::<_, JobRouteError>(node(2)));
    let next = routed.next(owner_node).unwrap();
    assert!(matches!(next, Next::Call(IoCall::Effect(_))));
    assert_eq!(routed.step, RouteStep::Remote);
}

// A local range is opened and must have the size that was asked for.
#[test]
fn local_range_checked() {
    let lookup = || {
        let ready = OwnedArtifact {
            artifact: Some(artifact()),
            ..owned()
        };
        child(Ok::<_, JobReadError>(ArtifactLookup::Ready(ready)))
    };
    let opened = |stream_size| {
        child(Ok::<_, JobReadError>(ArtifactRead {
            blob: body(),
            stream_size,
        }))
    };
    let mut routed = route(Some(1..3), None);
    routed.step = RouteStep::Local;
    assert!(is_child(&routed.next(lookup()).unwrap()));
    assert!(matches!(
        routed.next(opened(2)),
        Ok(Next::Done((_, Some(_))))
    ));

    let mut routed = route(Some(1..3), None);
    routed.step = RouteStep::Local;
    routed.next(lookup()).unwrap();
    assert!(matches!(
        routed.next(opened(3)),
        Err(JobRouteError::Internal(_))
    ));
}

fn control(event: JobControlEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Net(NetEvent::JobControl(event))))
}

fn ready(stream_size: u64) -> Box<JobResponse> {
    Box::new(JobResponse::ArtifactReady {
        owned: wire(),
        stream_size,
    })
}

fn remote(range: Option<Range<u64>>) -> ArtifactRouteOperation {
    let mut routed = route(range, Some(token()));
    routed.step = RouteStep::Remote;
    routed
}

// A remote range arrives with its bytes, a plain lookup without any.
#[test]
fn remote_body_matched() {
    let streamed = JobControlEvent::Streamed {
        response: ready(2),
        body: body(),
    };
    assert!(matches!(
        remote(Some(1..3)).next(control(streamed)),
        Ok(Next::Done((ArtifactLookup::Ready(_), Some(_))))
    ));
    let plain = JobControlEvent::Response(ready(0));
    assert!(matches!(
        remote(None).next(control(plain)),
        Ok(Next::Done((ArtifactLookup::Ready(_), None)))
    ));
}

// A range answered without bytes or with the wrong size is refused.
#[test]
fn remote_mismatch_refused() {
    let bodiless = JobControlEvent::Response(ready(2));
    assert!(matches!(
        remote(Some(1..3)).next(control(bodiless)),
        Err(JobRouteError::Unavailable(_))
    ));
    let wrong = JobControlEvent::Streamed {
        response: ready(5),
        body: body(),
    };
    assert!(matches!(
        remote(Some(1..3)).next(control(wrong)),
        Err(JobRouteError::Unavailable(_))
    ));
}

// Remote states carry over, and an unreachable owner is unavailable.
#[test]
fn remote_states_mapped() {
    let answer = |response| control(JobControlEvent::Response(Box::new(response)));
    assert!(matches!(
        remote(None).next(answer(JobResponse::ArtifactGone)),
        Ok(Next::Done((ArtifactLookup::Gone, None)))
    ));
    assert!(matches!(
        remote(None).next(answer(JobResponse::NotFound)),
        Ok(Next::Done((ArtifactLookup::NotFound, None)))
    ));
    assert_eq!(
        remote(None)
            .next(answer(JobResponse::Forbidden))
            .unwrap_err(),
        JobRouteError::Forbidden
    );
    let down = control(JobControlEvent::Unavailable("gone".to_string()));
    assert_eq!(
        remote(None).next(down).unwrap_err(),
        JobRouteError::Unavailable("job owner unreachable: gone".to_string())
    );
}
