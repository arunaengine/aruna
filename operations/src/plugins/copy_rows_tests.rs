//! Tests the artifact copy operations as calls and results: what each one reads and writes, the
//! bound on unsettled copies, the checks of a finished copy and the sweep's choices.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::realm::RealmId;

use super::*;
use crate::plugins::rows::{page_result, values_result};

const FIXTURE: &[u8] = include_bytes!("../../../plugins/tests/fixture.wasm");
const DIGEST: &str = "sha256:d";
const NOW: u64 = 1_000_000_000;

/// A progress row of `digest` written at `now_ms`, with no chunk received.
pub(crate) fn stale_progress(digest: &str, total: u64, now_ms: u64) -> Row {
    let value = Progress::new(total, now_ms).encode();
    row(PLUGIN_ARTIFACT_KEYSPACE, progress_key(digest), value)
}

fn config() -> RealmConfigDocument {
    RealmConfigDocument::new(RealmId::from_bytes([1u8; 32]), Vec::new(), 3)
}

fn applied_ok() -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<(), StoreError>(())))
}

fn finished_ok() -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<(), ArtifactError>(())))
}

fn no_state() -> IoResult {
    values_result(vec![None, None])
}

fn empty_page() -> IoResult {
    page_result(Vec::new(), None)
}

fn is_child<T>(next: Next<T>) -> bool {
    matches!(next, Next::Call(IoCall::Child(_)))
}

fn written_keys(next: Next<ChunkStored>) -> Vec<Vec<u8>> {
    let Next::Call(IoCall::Write(rows)) = next else {
        panic!("expected a write");
    };
    rows.into_iter().map(|(_, key, _)| key.to_vec()).collect()
}

#[test]
fn chunk_shapes() {
    assert_eq!(chunk_count(0), 0);
    assert_eq!(chunk_count(CHUNK as u64 + 1), 2);
    assert_eq!(chunk_len(CHUNK as u64 + 1, 1), 1);
    let bytes = vec![1u8; CHUNK + 1];
    let last = chunk_of(DIGEST, &bytes, 1);
    assert!(chunk_fits(&last));
    assert_eq!(last.bytes.0.len(), 1);
    let mut past = last.clone();
    past.index = 2;
    assert!(!chunk_fits(&past));
    let mut empty = last;
    empty.total = 0;
    assert!(!chunk_fits(&empty));
}

#[test]
fn row_codecs() {
    let done = Done::encode(9, 5);
    assert_eq!(
        Done::decode(&done),
        Some(Done {
            total: 9,
            completed_ms: 5
        })
    );
    assert_eq!(Done::decode(&done[1..]), None);
    let mut progress = Progress::new(CHUNK as u64 + 1, 1);
    assert!(!progress.complete());
    progress.mark(0, 2);
    progress.mark(1, 3);
    assert!(progress.complete());
    let decoded = Progress::decode(&progress.encode()).unwrap();
    assert_eq!((decoded.updated_ms, decoded.complete()), (3, true));
    assert_eq!(Progress::decode(&[0; 16]), None, "an empty copy has no row");
}

#[test]
fn reads_total() {
    let mut absent = CopyTotal::new(DIGEST);
    let read = read_key(done_key(DIGEST));
    assert_eq!(absent.first(), Ok(Next::Call(read)));
    let none = absent.next(values_result(vec![None]));
    assert_eq!(none, Ok(Next::Done(None)));
    let mut present = CopyTotal::new(DIGEST);
    let row = values_result(vec![Some(Done::encode(7, 1))]);
    assert_eq!(present.next(row), Ok(Next::Done(Some(7))));
    let mut bad = CopyTotal::new(DIGEST);
    let refused = bad.next(values_result(vec![Some(vec![1])]));
    assert_eq!(refused, Err(storage("a bad done row")));
    assert!(CopyTotal::new(DIGEST).next(IoResult::Done).is_err());
}

#[test]
fn reads_copy_order() {
    let total = CHUNK as u64 + 2;
    let mut read = ReadCopy::new(DIGEST, total);
    let first = read_key(chunk_key(DIGEST, 0));
    assert_eq!(read.first(), Ok(Next::Call(first)));
    let second = read_key(chunk_key(DIGEST, 1));
    let answer = values_result(vec![Some(vec![1; CHUNK])]);
    assert_eq!(read.next(answer), Ok(Next::Call(second)));
    let Next::Done(bytes) = read.next(values_result(vec![Some(vec![2; 2])])).unwrap() else {
        panic!("the last chunk ends the read");
    };
    assert_eq!((bytes.len() as u64, bytes[CHUNK]), (total, 2));
    let mut empty = ReadCopy::new(DIGEST, 0);
    assert_eq!(empty.first(), Ok(Next::Done(Vec::new())));
}

#[test]
fn refuses_bad_chunks() {
    let mut missing = ReadCopy::new(DIGEST, 4);
    let refused = missing.next(values_result(vec![None]));
    assert_eq!(refused, Err(storage("a chunk of a copy is missing")));
    let mut short = ReadCopy::new(DIGEST, 4);
    let refused = short.next(values_result(vec![Some(vec![1])]));
    assert_eq!(
        refused,
        Err(storage("a chunk of a copy has the wrong length"))
    );
}

#[test]
fn reads_one_chunk() {
    let mut absent = ReadChunk::new(DIGEST, 0);
    assert_eq!(absent.first(), Ok(Next::Call(read_key(done_key(DIGEST)))));
    assert_eq!(absent.next(values_result(vec![None])), Ok(Next::Done(None)));
    let done_row = || values_result(vec![Some(Done::encode(4, 1))]);
    let mut past = ReadChunk::new(DIGEST, 1);
    assert_eq!(past.next(done_row()), Ok(Next::Done(None)));
    let mut read = ReadChunk::new(DIGEST, 0);
    let chunk = read_key(chunk_key(DIGEST, 0));
    assert_eq!(read.next(done_row()), Ok(Next::Call(chunk)));
    let answer = read.next(values_result(vec![Some(vec![1; 4])]));
    assert_eq!(answer, Ok(Next::Done(Some((4, vec![1; 4])))));
    let mut gone = ReadChunk::new(DIGEST, 0);
    gone.next(done_row()).unwrap();
    assert_eq!(gone.next(values_result(vec![None])), Ok(Next::Done(None)));
}

#[test]
fn finish_then_sync() {
    let mut finish = FinishCopy::new(DIGEST.to_string(), 4, NOW);
    assert!(is_child(finish.first().unwrap()));
    assert_eq!(finish.next(applied_ok()), Ok(Next::Call(IoCall::Sync)));
    assert_eq!(finish.next(IoResult::Done), Ok(Next::Done(())));
    let mut failed = FinishCopy::new(DIGEST.to_string(), 4, NOW);
    let error = StoreError("down".into());
    let answer = IoResult::Child(ChildOutcome::new(Err::<(), _>(error)));
    assert_eq!(failed.next(answer), Err(storage("down")));
}

#[test]
fn admission_bound() {
    let held = |total: u64| {
        let value = Done::encode(total, 1);
        vec![(done_key("sha256:other"), value)]
    };
    let mut admitted = Admission::start(BTreeSet::new(), DIGEST, 1).unwrap();
    assert_eq!(admitted.take(empty_page()), Ok(false));
    assert_eq!(admitted.take(page_result(held(4), None)), Ok(true));
    let mut full = Admission::start(BTreeSet::new(), DIGEST, 1).unwrap();
    full.take(empty_page()).unwrap();
    let refused = full.take(page_result(held(MAX_UNSETTLED), None));
    assert_eq!(refused, Err(ArtifactError::Busy));
    // A kept copy counts for nothing, and a kept digest needs no check at all.
    let kept = BTreeSet::from(["sha256:other".to_string()]);
    let mut settled = Admission::start(kept.clone(), DIGEST, 1).unwrap();
    settled.take(empty_page()).unwrap();
    let answer = settled.take(page_result(held(MAX_UNSETTLED), None));
    assert_eq!(answer, Ok(true));
    assert_eq!(Admission::start(kept, "sha256:other", 1), None);
}

#[test]
fn stores_whole_copy() {
    let config = config();
    let bytes = vec![3u8; CHUNK + 1];
    let mut store = StoreCopy::new(&config, DIGEST, &bytes, NOW);
    assert_eq!(store.first(), Ok(Next::Call(read_state(DIGEST))));
    let scan_progress = store.next(no_state()).unwrap();
    assert!(matches!(scan_progress, Next::Call(IoCall::Page { .. })));
    store.next(empty_page()).unwrap();
    assert!(is_child(store.next(empty_page()).unwrap()), "first chunk");
    assert_eq!(store.index, 1);
    assert!(is_child(store.next(applied_ok()).unwrap()), "second chunk");
    assert!(is_child(store.next(applied_ok()).unwrap()), "finish");
    assert_eq!(store.phase, StorePhase::Finishing);
    assert_eq!(store.next(finished_ok()), Ok(Next::Done(())));
}

#[test]
fn replaces_known_copy() {
    // A copy this node already knows skips the bound.
    let config = config();
    let mut store = StoreCopy::new(&config, DIGEST, FIXTURE, NOW);
    store.first().unwrap();
    let known = values_result(vec![Some(Done::encode(1, 1)), Some(vec![1])]);
    assert!(is_child(store.next(known).unwrap()));
    assert_eq!(store.phase, StorePhase::Writing);
    let mut bad = StoreCopy::new(&config, DIGEST, FIXTURE, NOW);
    let refused = bad.next(values_result(vec![None, Some(vec![1])]));
    assert_eq!(refused, Err(storage("a bad progress row")));
}

fn small_chunk() -> Chunk {
    chunk_of(DIGEST, &[1u8; 4], 0)
}

#[test]
fn chunk_refusals() {
    let config = config();
    let mut short = small_chunk();
    short.bytes.0.pop();
    let mut misfit = StoreChunk::new(&config, short, NOW);
    assert_eq!(misfit.first(), Err(ArtifactError::Mismatch));
    let mut other_done = StoreChunk::new(&config, small_chunk(), NOW);
    let done_row = values_result(vec![Some(Done::encode(5, 1)), None]);
    assert_eq!(other_done.next(done_row), Err(ArtifactError::Mismatch));
    let mut other_copy = StoreChunk::new(&config, small_chunk(), NOW);
    let progress = Progress::new(5, 1).encode();
    let answer = other_copy.next(values_result(vec![None, Some(progress)]));
    assert_eq!(answer, Err(ArtifactError::Mismatch));
}

#[test]
fn confirms_done_copy() {
    let config = config();
    let mut store = StoreChunk::new(&config, small_chunk(), NOW);
    store.first().unwrap();
    let row = values_result(vec![Some(Done::encode(4, 1)), None]);
    let keys = written_keys(store.next(row).unwrap());
    assert_eq!(keys, [done_key(DIGEST)]);
    assert_eq!(store.next(IoResult::Done), Ok(Next::Call(IoCall::Sync)));
    let confirmed = store.next(IoResult::Done);
    assert_eq!(confirmed, Ok(Next::Done(ChunkStored::Confirmed)));
}

#[test]
fn keeps_new_chunks() {
    let config = config();
    let bytes = vec![1u8; CHUNK + 1];
    let mut held = StoreChunk::new(&config, chunk_of(DIGEST, &bytes, 0), NOW);
    held.first().unwrap();
    held.next(no_state()).unwrap();
    held.next(empty_page()).unwrap();
    let keys = written_keys(held.next(empty_page()).unwrap());
    assert_eq!(keys, [chunk_key(DIGEST, 0), progress_key(DIGEST)]);
    assert_eq!(held.next(IoResult::Done), Ok(Next::Done(ChunkStored::Held)));
    // The last missing chunk of a known copy needs no admission.
    let mut progress = Progress::new(bytes.len() as u64, 1);
    progress.mark(0, 1);
    let mut last = StoreChunk::new(&config, chunk_of(DIGEST, &bytes, 1), NOW);
    let state = values_result(vec![None, Some(progress.encode())]);
    written_keys(last.next(state).unwrap());
    let answer = last.next(IoResult::Done);
    assert_eq!(answer, Ok(Next::Done(ChunkStored::AllHere)));
}

#[test]
fn checks_finished_copy() {
    let digest = ArtifactDigest::of(FIXTURE).to_string();
    let read =
        |bytes: &[u8]| IoResult::Child(ChildOutcome::new(Ok::<_, ArtifactError>(bytes.to_vec())));
    let total = FIXTURE.len() as u64;
    let mut complete = CompleteCopy::new(digest.clone(), total, NOW);
    assert!(is_child(complete.first().unwrap()));
    assert!(is_child(complete.next(read(FIXTURE)).unwrap()));
    assert_eq!(complete.next(finished_ok()), Ok(Next::Done(())));
    let mut changed = CompleteCopy::new(digest, total, NOW);
    assert_eq!(changed.next(read(b"other")), Err(ArtifactError::Mismatch));
    let junk = b"not a plugin";
    let junk_digest = ArtifactDigest::of(junk).to_string();
    let mut invalid = CompleteCopy::new(junk_digest, junk.len() as u64, NOW);
    assert_eq!(invalid.next(read(junk)), Err(ArtifactError::Mismatch));
}

#[test]
fn sweeps_stale_copies() {
    let mut config = config();
    let kept = "sha256:kept";
    config.plugins.push(aruna_core::plugins::PublishedPlugin {
        name: "kept".into(),
        version: "1.0.0".into(),
        digest: kept.into(),
        size: 4,
        manifest: String::new(),
        published_by: aruna_core::UserId::nil(config.realm_id),
        published_at_ms: 1,
    });
    let now = GRACE.as_millis() as u64 + 10;
    let copies = vec![
        (done_key(kept), Done::encode(4, 1)),
        (done_key("sha256:old"), Done::encode(4, 1)),
        (done_key("sha256:new"), Done::encode(4, now)),
    ];
    let unfinished = vec![
        (progress_key("sha256:gone"), Progress::new(4, 1).encode()),
        (progress_key("sha256:new"), Progress::new(4, 1).encode()),
        (progress_key("sha256:live"), Progress::new(4, now).encode()),
    ];
    let mut sweep = SweepCopies::new(&config, now);
    assert!(matches!(sweep.first(), Ok(Next::Call(IoCall::Page { .. }))));
    assert!(matches!(
        sweep.next(page_result(copies, None)),
        Ok(Next::Call(IoCall::Page { .. }))
    ));
    let gone = spaced(vec![
        progress_key("sha256:gone"),
        chunk_key("sha256:gone", 0),
    ]);
    let answer = sweep.next(page_result(unfinished, None));
    assert_eq!(answer, Ok(Next::Call(delete_call(gone))));
    // A stale progress row next to a done row only loses itself.
    let stale = spaced(vec![progress_key("sha256:new")]);
    assert_eq!(
        sweep.next(IoResult::Done),
        Ok(Next::Call(delete_call(stale)))
    );
    let old = spaced(vec![chunk_key("sha256:old", 0), done_key("sha256:old")]);
    assert_eq!(sweep.next(IoResult::Done), Ok(Next::Call(delete_call(old))));
    assert_eq!(sweep.next(IoResult::Done), Ok(Next::Done(2)));
}
