//! Tests the staging checkpoints alpha.159 stopped at a listing offset: a finished one keeps its
//! counts without the cursor, an unfinished one walks again, and a second run changes nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::STAGING_STATE_KEYSPACE;
use aruna_core::structs::execution::source_access::ListCursor;

use super::super::migrate_output;
use super::super::tests::{read, write};
use super::*;

/// A checkpoint as alpha.159 stored it, walking `data` and stopped at `cursor`.
fn stored(phase: StagingJobPhase, cursor: OffsetCursor) -> Vec<u8> {
    let old = OffsetCheckpoint {
        phase,
        flat: Some(false),
        items_done: 3,
        prefixes_done: 1,
        walk: Some(OffsetWalk {
            source_path: "data".to_string(),
            target_prefix: "in/".to_string(),
            cursor: Some(cursor),
            queue_row: Some(2),
            from: None,
        }),
        manifest: None,
        table: None,
        queue_head: 1,
        queue_tail: 3,
        items_current: 5,
        items_succeeded: 4,
        items_failed: 1,
        items_total: Some(9),
        bytes_current: 50,
        bytes_total: None,
        bytes_discovered: 70,
        current_path: Some("data/x".to_string()),
        errors: Vec::new(),
    };
    postcard::to_allocvec(&old).unwrap()
}

// Only an offset cursor marks an alpha.159 row: key cursors read the same in both releases.
#[test]
fn reads_offset_rows() {
    let finished = stored(StagingJobPhase::Completed, OffsetCursor::Offset(40));
    let Some(OffsetRow::Finished(bytes)) = offset_row(&finished) else {
        panic!("a finished checkpoint keeps its counts");
    };
    let kept: StagingJobCheckpoint = postcard::from_bytes(&bytes).unwrap();
    let walk = kept.walk.unwrap();
    assert_eq!((walk.cursor, walk.queue_row), (None, Some(2)));
    assert_eq!((kept.items_succeeded, kept.bytes_discovered), (4, 70));
    let running = stored(StagingJobPhase::Writing, OffsetCursor::Offset(40));
    assert_eq!(offset_row(&running), Some(OffsetRow::Unfinished));
    let keyed = stored(
        StagingJobPhase::Writing,
        OffsetCursor::After("data/b".to_string()),
    );
    assert_eq!(offset_row(&keyed), None);
}

#[test]
fn walks_offset_again() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let finished = stored(StagingJobPhase::Completed, OffsetCursor::Offset(40));
    let unfinished = stored(StagingJobPhase::Writing, OffsetCursor::Offset(12));
    // A checkpoint the current node wrote with a walk cursor stays as it is.
    let mut current: StagingJobCheckpoint = postcard::from_bytes(&stored(
        StagingJobPhase::Writing,
        OffsetCursor::After("data/b".to_string()),
    ))
    .unwrap();
    if let Some(walk) = current.walk.as_mut() {
        walk.cursor = Some(ListCursor::Walk("data/b".to_string()));
    }
    let current = postcard::to_allocvec(&current).unwrap();
    let keys = [[1u8; 16], [2u8; 16], [3u8; 16]];
    let rows = vec![
        (&keys[0][..], finished),
        (&keys[1][..], unfinished),
        (&keys[2][..], current.clone()),
    ];
    write(&path, STAGING_STATE_KEYSPACE, rows);

    let output = migrate_output(path.to_str().unwrap()).unwrap();

    assert_eq!(output.staging_checkpoints_rewritten, 1);
    assert_eq!(
        output.stale_rows_deleted.get(STAGING_STATE_KEYSPACE),
        Some(&1)
    );
    let states = read(&path, STAGING_STATE_KEYSPACE);
    assert_eq!(states.len(), 2);
    let kept: StagingJobCheckpoint = postcard::from_bytes(&states[&keys[0][..]]).unwrap();
    assert_eq!(kept.walk.and_then(|walk| walk.cursor), None);
    assert_eq!(states[&keys[2][..]], current);
    let again = migrate_output(path.to_str().unwrap()).unwrap();
    assert_eq!(again.staging_checkpoints_rewritten, 0);
    assert_eq!(again.stale_rows_deleted.get(STAGING_STATE_KEYSPACE), None);
    assert_eq!(read(&path, STAGING_STATE_KEYSPACE), states);
}
