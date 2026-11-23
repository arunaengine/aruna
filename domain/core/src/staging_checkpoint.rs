//! Reads where a staging job stands, its open prefix lanes counted in, and where a job taken
//! over by a range's new coordinator goes on.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use byteview::ByteView;

use crate::calls::{IoCall, IoError, IoResult, Next};
use crate::keyspaces::STAGING_STATE_KEYSPACE;
use crate::operation::Operation;
use crate::rows::values;
use crate::structs::execution::job::identity::JobId;
use crate::structs::execution::job::input::{
    ManifestRead, StagingJobCheckpoint, StagingJobPhase, StagingJobSpec,
};
use crate::types::{Key, Value};

fn decode<T: serde::de::DeserializeOwned>(value: &[u8]) -> Result<T, String> {
    postcard::from_bytes(value).map_err(|error| error.to_string())
}

/// Most per-entry errors a staging checkpoint keeps; the failed counter counts all of them.
pub const MAX_ERROR_SAMPLE: usize = 100;

/// Lanes one job walks at the same time. Each lane holds at most one listed page ahead, so a
/// job holds a bounded number of pages whatever the source holds.
pub const LANE_LIMIT: usize = 4;

/// Checkpoint rows of one job a reader looks at: its own, and two per lane.
pub const LANE_ROWS: usize = 2 * LANE_LIMIT + 1;

/// Slots per spec prefix: lane `slot` walks part `slot % PARTS` of prefix `slot / PARTS`.
pub const PARTS: u64 = LANE_LIMIT as u64;

/// One open lane: its checkpoint and the exclusive target key it stops before.
pub type Lane = (u64, StagingJobCheckpoint, Option<String>);

/// The checkpoint row of a job, or of its prefix lane `slot`, which extends the job's key.
pub fn checkpoint_key(job_id: JobId, slot: Option<u64>) -> ByteView {
    let mut key = job_id.to_bytes().to_vec();
    if let Some(slot) = slot {
        key.extend_from_slice(&slot.to_be_bytes());
    }
    ByteView::from(key)
}

/// The read of a job's own checkpoint row.
pub fn main_read(job_id: JobId) -> IoCall {
    let row = (
        STAGING_STATE_KEYSPACE.to_string(),
        checkpoint_key(job_id, None),
    );
    IoCall::Read(vec![row])
}

/// The checkpoint a [`main_read`] answered, `None` before the job stored one.
pub fn decode_main(result: IoResult) -> Result<Option<StagingJobCheckpoint>, IoError> {
    let [row] = values::<1>(result)?;
    row.map(|value| postcard::from_bytes(&value))
        .transpose()
        .map_err(|error| IoError::Codec(error.to_string()))
}

/// The page of a job's state rows that holds its open lanes.
pub fn lanes_page(job_id: JobId) -> IoCall {
    IoCall::Page {
        space: STAGING_STATE_KEYSPACE.to_string(),
        prefix: ByteView::from(job_id.to_bytes().to_vec()),
        after: None,
        limit: LANE_ROWS,
    }
}

/// The open lanes among a job's state rows, each with its checkpoint and where it stops.
pub fn parse_lanes(rows: Vec<(Key, Value)>) -> Result<Vec<Lane>, String> {
    let mut lanes: Vec<Lane> = Vec::new();
    for (key, value) in rows {
        let Some(slot) = (key.get(16..24)).and_then(|slot| <[u8; 8]>::try_from(slot).ok()) else {
            continue;
        };
        let slot = u64::from_be_bytes(slot);
        match key.len() {
            24 => lanes.push((slot, decode(&value)?, None)),
            // The bound row sorts right after its checkpoint row.
            _ => match lanes.last_mut() {
                Some(lane) if lane.0 == slot => lane.2 = decode(&value)?,
                _ => {}
            },
        }
    }
    Ok(lanes)
}

/// The job's checkpoint with its open lanes counted in. The first open lane's prefix reads as
/// the next to start, so a job handed over walks the open prefixes again.
pub fn merged(mut main: StagingJobCheckpoint, lanes: Vec<Lane>) -> StagingJobCheckpoint {
    if let Some(first) = lanes.iter().map(|(slot, ..)| *slot).min() {
        main.prefixes_done = first / PARTS;
        main.phase = StagingJobPhase::Registering;
    }
    for (_, lane, _) in lanes {
        if main.current_path.is_none() {
            main.current_path = lane.current_path.clone();
        }
        fold(&mut main, lane);
    }
    main
}

/// Adds a lane's counters and errors to the job's.
pub fn fold(main: &mut StagingJobCheckpoint, lane: StagingJobCheckpoint) {
    main.items_current = main.items_current.saturating_add(lane.items_current);
    main.items_succeeded = main.items_succeeded.saturating_add(lane.items_succeeded);
    main.items_failed = main.items_failed.saturating_add(lane.items_failed);
    main.bytes_current = main.bytes_current.saturating_add(lane.bytes_current);
    main.bytes_discovered = main.bytes_discovered.saturating_add(lane.bytes_discovered);
    let room = MAX_ERROR_SAMPLE.saturating_sub(main.errors.len());
    main.errors.extend(lane.errors.into_iter().take(room));
}

/// Where a job submitted with `spec` starts.
pub fn initial_checkpoint(spec: &StagingJobSpec) -> StagingJobCheckpoint {
    StagingJobCheckpoint {
        phase: StagingJobPhase::Queued,
        flat: None,
        items_done: 0,
        prefixes_done: 0,
        walk: None,
        manifest: None,
        table: spec.table.as_ref().map(|_| Default::default()),
        queue_head: 0,
        queue_tail: 0,
        items_current: 0,
        items_succeeded: 0,
        items_failed: 0,
        items_total: (spec.prefixes.is_empty() && spec.table.is_none())
            .then_some(spec.items.len() as u64),
        bytes_current: 0,
        bytes_total: None,
        bytes_discovered: 0,
        current_path: None,
        errors: Vec::new(),
    }
}

/// Where a job taken over by a range's new coordinator goes on: the former position without
/// counters, errors or directory queue. A flat walk keeps its cursor and rereads its manifest;
/// a manifest read, a table or a directory walk, whose queue stays behind, starts over.
pub fn handed_checkpoint(spec: &StagingJobSpec, at: &StagingJobCheckpoint) -> StagingJobCheckpoint {
    let mut next = initial_checkpoint(spec);
    // The table is read again from its start, pinned to what the former job first read.
    if let (Some(table), Some(read)) = (next.table.as_mut(), at.table.as_ref()) {
        table.version_id = read.version_id;
        table.observation = read.observation.clone();
    }
    next.flat = at.flat;
    next.items_done = at.items_done;
    let queued = at.queue_head < at.queue_tail;
    let open = at.walk.is_some() || at.manifest.is_some() || queued;
    let flat_walk = at.flat == Some(true)
        && at.manifest.is_none()
        && !queued
        && at
            .walk
            .as_ref()
            .is_some_and(|walk| walk.queue_row.is_none());
    match flat_walk {
        true => {
            next.prefixes_done = at.prefixes_done;
            next.walk = at.walk.clone();
            // The manifest's rows stay with the former job, so this one reads it before its walk.
            let manifest = spec.checksum_manifest.as_ref().zip(at.walk.as_ref());
            next.manifest = manifest.map(|(name, walk)| ManifestRead {
                source_path: format!("{}{name}", walk.source_path),
                offset: 0,
                observation: None,
            });
        }
        false => next.prefixes_done = at.prefixes_done.saturating_sub(u64::from(open)),
    }
    next
}

#[derive(Debug, PartialEq)]
enum ReadStep {
    Main,
    Lanes(Box<StagingJobCheckpoint>),
}

/// The job's progress with its open prefix lanes counted in; a lane's prefix reads as not
/// started yet, as a job handed over goes on from there.
#[derive(Debug, PartialEq)]
pub struct ReadCheckpointOperation {
    job_id: JobId,
    step: ReadStep,
}

impl ReadCheckpointOperation {
    pub fn new(job_id: JobId) -> Self {
        Self {
            job_id,
            step: ReadStep::Main,
        }
    }
}

impl Operation for ReadCheckpointOperation {
    type Output = Option<StagingJobCheckpoint>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        Ok(Next::Call(main_read(self.job_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        match std::mem::replace(&mut self.step, ReadStep::Main) {
            ReadStep::Main => {
                let Some(main) = decode_main(result)? else {
                    return Ok(Next::Done(None));
                };
                self.step = ReadStep::Lanes(Box::new(main));
                Ok(Next::Call(lanes_page(self.job_id)))
            }
            ReadStep::Lanes(main) => {
                let (rows, _) = result.page()?;
                let open = parse_lanes(rows).map_err(IoError::Codec)?;
                Ok(Next::Done(Some(merged(*main, open))))
            }
        }
    }
}
