//! Walks the flat spec prefixes of one reference import in parallel lanes, a prefix split into
//! key ranges where its listing allows, each lane from its own checkpoint row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::events::{Event, GroupEvent};
use aruna_core::jobs::{JobEffect, JobEvent, JobReport, JobStops};
use aruna_core::keyspaces::STAGING_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    JobId, StagingJobCheckpoint, StagingJobPhase, StagingJobSpec, StagingWalk,
};
use aruna_core::structs::execution::source_access::ListRequest;
use aruna_core::structs::execution::staging::StagingStrategy;
use aruna_core::types::{Key, Value};
use byteview::ByteView;

use super::walk::{
    Ahead, WalkPageOperation, WalkPlace, WalkedPage, directory_list_path, member, member_outcome,
    target_of,
};
use super::{PageFailure, StagingRun, checkpoint_key, committed};
use crate::jobs::run_rows::SettledRowsOperation;
use crate::jobs::store::JobMutationError;
use crate::staging::list_source::{
    ListStagingError, ListStagingInput, ListStagingOperation, ListStagingResult,
};
use crate::staging::page::rows::MAX_ERROR_SAMPLE;

/// Lanes one job walks at the same time. Each lane holds at most one listed page ahead, so a
/// job holds a bounded number of pages whatever the source holds.
const LANE_LIMIT: usize = 4;
/// Checkpoint rows of one job a reader looks at: its own, and two per lane.
pub(super) const LANE_ROWS: usize = 2 * LANE_LIMIT + 1;
/// Slots per spec prefix: lane `slot` walks part `slot % PARTS` of prefix `slot / PARTS`.
const PARTS: u64 = LANE_LIMIT as u64;
/// One-key listings that split one prefix, whatever its key space looks like.
const MAX_PROBES: usize = 64;

/// One open lane: its checkpoint and the exclusive target key it stops before.
pub(super) type Lane = (u64, StagingJobCheckpoint, Option<String>);

/// Whether the job's remaining prefixes go to lanes: reference imports of flat listings without
/// a checksum manifest, whose prefixes write disjoint key ranges.
pub(super) fn applies(spec: &StagingJobSpec, checkpoint: &StagingJobCheckpoint) -> bool {
    let idle = checkpoint.walk.is_none()
        && checkpoint.manifest.is_none()
        && checkpoint.queue_head == checkpoint.queue_tail;
    spec.strategy == StagingStrategy::Reference
        && spec.checksum_manifest.is_none()
        && checkpoint.flat == Some(true)
        && idle
        && disjoint(spec)
}

/// No target prefix lies within another, so two lanes never write the same key.
fn disjoint(spec: &StagingJobSpec) -> bool {
    // The bucket root, an empty target prefix, holds every key.
    let folders: Vec<String> = (spec.prefixes.iter())
        .map(|prefix| prefix.target_prefix.trim_matches('/'))
        .map(|folder| match folder.is_empty() {
            true => String::new(),
            false => format!("{folder}/"),
        })
        .collect();
    folders.iter().enumerate().all(|(index, folder)| {
        (folders.iter().skip(index + 1))
            .all(|other| !folder.starts_with(other.as_str()) && !other.starts_with(folder.as_str()))
    })
}

/// The row of lane `slot` that holds where it stops, beside its checkpoint row.
fn bound_key(job_id: JobId, slot: u64) -> ByteView {
    let mut key = checkpoint_key(job_id, Some(slot)).to_vec();
    key.push(b'u');
    ByteView::from(key)
}

/// The page of a job's state rows that holds its open lanes.
pub(super) fn lanes_page(job_id: JobId) -> IoCall {
    IoCall::Page {
        space: STAGING_STATE_KEYSPACE.to_string(),
        prefix: ByteView::from(job_id.to_bytes().to_vec()),
        after: None,
        limit: LANE_ROWS,
    }
}

/// The open lanes among a job's state rows, each with its checkpoint and where it stops.
pub(super) fn parse_lanes(rows: Vec<(Key, Value)>) -> Result<Vec<Lane>, String> {
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
pub(super) fn merged(mut main: StagingJobCheckpoint, lanes: Vec<Lane>) -> StagingJobCheckpoint {
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
fn fold(main: &mut StagingJobCheckpoint, lane: StagingJobCheckpoint) {
    main.items_current = main.items_current.saturating_add(lane.items_current);
    main.items_succeeded = main.items_succeeded.saturating_add(lane.items_succeeded);
    main.items_failed = main.items_failed.saturating_add(lane.items_failed);
    main.bytes_current = main.bytes_current.saturating_add(lane.bytes_current);
    main.bytes_discovered = main.bytes_discovered.saturating_add(lane.bytes_discovered);
    let room = MAX_ERROR_SAMPLE.saturating_sub(main.errors.len());
    main.errors.extend(lane.errors.into_iter().take(room));
}

/// What a window member of the lanes run does.
#[derive(Debug, PartialEq)]
enum Member {
    Page(u64),
    Opener,
    Writer,
}

/// A lane between its pages.
#[derive(Debug, PartialEq)]
struct OpenLane {
    checkpoint: StagingJobCheckpoint,
    upper: Option<String>,
    ahead: Option<Ahead>,
}

/// How the lanes run ended: the job's checkpoint and whether every prefix is walked.
#[derive(Debug, PartialEq)]
pub(super) struct LanesEnd {
    pub(super) main: StagingJobCheckpoint,
    pub(super) finished: bool,
}

#[derive(Debug, PartialEq)]
enum LanesStep {
    Read,
    Report,
    Window,
}

/// Walks every prefix not walked yet, at most [`LANE_LIMIT`] at once, and folds each finished
/// lane into the job's checkpoint. Each lane page is one window member, so a failure or a stop
/// lets running pages end and starts no others. One member at a time writes the job's rows.
#[derive(Debug, PartialEq)]
pub(super) struct LanesOperation {
    run: StagingRun,
    main: StagingJobCheckpoint,
    waiting: Vec<(u64, OpenLane)>,
    members: Vec<Member>,
    pages: usize,
    opening: bool,
    writing: bool,
    /// The rows of lanes folded into `main` that the next write removes.
    finished: Vec<u64>,
    stopped: bool,
    exhausted: bool,
    failure: Option<PageFailure>,
    step: LanesStep,
}

impl LanesOperation {
    pub(super) fn new(run: StagingRun, main: StagingJobCheckpoint) -> Self {
        Self {
            run,
            main,
            waiting: Vec::new(),
            members: Vec::new(),
            pages: 0,
            opening: false,
            writing: false,
            finished: Vec::new(),
            stopped: false,
            exhausted: false,
            failure: None,
            step: LanesStep::Read,
        }
    }

    fn blocked(&self) -> bool {
        self.stopped || self.failure.is_some()
    }

    fn add(&mut self, kind: Member, effect: Effect, into: &mut Vec<Effect>) {
        self.members.push(kind);
        into.push(effect);
    }

    /// The members to start now: pages up to the limit, the job's write, then an opener for the
    /// next prefix.
    fn refill(&mut self) -> Result<Vec<Effect>, PageFailure> {
        let mut effects = Vec::new();
        while !self.blocked() && self.pages < LANE_LIMIT {
            let Some((slot, lane)) = self.waiting.pop() else {
                break;
            };
            // A lane without a walk left is finished: it folds into the job.
            if lane.checkpoint.walk.is_none() {
                fold(&mut self.main, lane.checkpoint);
                self.finished.push(slot);
                continue;
            }
            let page = LanePageOperation::new(self.run.clone(), slot, lane);
            self.pages += 1;
            self.add(Member::Page(slot), member(page), &mut effects);
        }
        if !self.writing && !self.opening && !self.finished.is_empty() {
            let call = self.write_member()?;
            self.writing = true;
            self.add(Member::Writer, call, &mut effects);
        }
        let room = LANE_LIMIT.saturating_sub(self.pages);
        let open = !self.blocked() && !self.exhausted && !self.writing && !self.opening;
        if open && room > 0 && self.waiting.is_empty() {
            self.opening = true;
            let opener = OpenPrefixOperation::new(self.run.clone(), self.main.clone(), room);
            self.add(Member::Opener, member(opener), &mut effects);
        }
        Ok(effects)
    }

    fn write_member(&mut self) -> Result<Effect, PageFailure> {
        let job_id = self.run.job_id;
        let mut deletes = Vec::new();
        for slot in self.finished.drain(..) {
            deletes.push((
                STAGING_STATE_KEYSPACE.to_string(),
                checkpoint_key(job_id, Some(slot)),
            ));
            deletes.push((STAGING_STATE_KEYSPACE.to_string(), bound_key(job_id, slot)));
        }
        let (key, value) = state_row(job_id, None, &self.main)?;
        let writes = vec![(STAGING_STATE_KEYSPACE.to_string(), key, value)];
        let token = self.run.claim_token;
        Ok(member(SettledRowsOperation::new(
            job_id, token, writes, deletes,
        )))
    }

    fn fail(&mut self, failure: PageFailure) {
        // The other lanes finish their current page, so no page is left half done.
        self.failure.get_or_insert(failure);
    }

    fn member_done(
        &mut self,
        index: usize,
        result: aruna_core::events::GroupResult,
    ) -> Result<(), IoError> {
        match self.members.get(index) {
            Some(Member::Page(slot)) => {
                let slot = *slot;
                self.pages -= 1;
                match member_outcome::<LanePage, PageFailure>(result)? {
                    // A lane without a walk left is finished, also when the job stops.
                    Ok(LanePage::Walked(lane)) if lane.checkpoint.walk.is_none() => {
                        fold(&mut self.main, lane.checkpoint);
                        self.finished.push(slot);
                    }
                    Ok(LanePage::Walked(lane)) => self.waiting.push((slot, lane)),
                    Ok(LanePage::Stopped) => self.stopped = true,
                    Err(failure) => self.fail(failure),
                }
            }
            Some(Member::Opener) => {
                self.opening = false;
                match member_outcome::<Opened, PageFailure>(result)? {
                    Ok(Opened::Lanes {
                        lanes,
                        prefixes_done,
                    }) => {
                        self.main.prefixes_done = prefixes_done;
                        self.exhausted = lanes.is_empty();
                        // The first lane opened runs first, as lanes are taken from the end.
                        self.waiting.extend(lanes.into_iter().rev().map(open_lane));
                    }
                    Ok(Opened::Stopped) => self.stopped = true,
                    Err(failure) => self.fail(failure),
                }
            }
            Some(Member::Writer) => {
                self.writing = false;
                if let Err(error) = member_outcome::<(), JobMutationError>(result)? {
                    self.fail(PageFailure::Retry(error.to_string()));
                }
            }
            None => {
                return Err(IoError::Unexpected {
                    expected: "a known lane member",
                    got: "an unknown window member",
                });
            }
        }
        Ok(())
    }

    /// The window, its next members, or the end once nothing runs and nothing can start.
    fn proceed(&mut self, starting: bool) -> Result<Next<LanesEnd>, PageFailure> {
        let members = self.refill()?;
        if members.is_empty() {
            if starting {
                return self.end();
            }
            return Ok(Next::Wait);
        }
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members,
            limit: LANE_LIMIT + 2,
            form: GroupForm::Window,
        })))
    }

    fn end(&mut self) -> Result<Next<LanesEnd>, PageFailure> {
        if let Some(failure) = self.failure.take() {
            return Err(failure);
        }
        Ok(Next::Done(LanesEnd {
            main: self.main.clone(),
            finished: !self.stopped && self.waiting.is_empty() && self.exhausted,
        }))
    }
}

fn open_lane((slot, checkpoint, upper): Lane) -> (u64, OpenLane) {
    let lane = OpenLane {
        checkpoint,
        upper,
        ahead: None,
    };
    (slot, lane)
}

impl Operation for LanesOperation {
    type Output = LanesEnd;
    type Error = PageFailure;

    fn first(&mut self) -> Result<Next<LanesEnd>, PageFailure> {
        Ok(Next::Call(lanes_page(self.run.job_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LanesEnd>, PageFailure> {
        match self.step {
            LanesStep::Read => {
                let (rows, _) = result.page()?;
                let open = parse_lanes(rows).map_err(PageFailure::Retry)?;
                let resumed: u64 = open.iter().map(|(_, lane, _)| lane.items_current).sum();
                self.waiting = open.into_iter().map(open_lane).collect();
                self.step = LanesStep::Report;
                let report = JobReport {
                    current: Some(self.main.items_current.saturating_add(resumed)),
                    ..JobReport::default()
                };
                Ok(Next::Call(job_call(JobEffect::Report(report))))
            }
            LanesStep::Report => {
                reported(result)?;
                self.step = LanesStep::Window;
                self.proceed(true)
            }
            LanesStep::Window => match result {
                IoResult::Group(GroupEvent::Member { index, result }) => {
                    self.member_done(index, result)?;
                    self.proceed(false)
                }
                IoResult::Group(GroupEvent::Closed) => self.end(),
                other => Err(IoError::Unexpected {
                    expected: "a lane member",
                    got: other.label(),
                }
                .into()),
            },
        }
    }
}

fn job_call(effect: JobEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Job(effect)))
}

/// Checks the answer to a progress report.
fn reported(result: IoResult) -> Result<(), IoError> {
    let event = result.event()?;
    match event {
        Event::Job(JobEvent::Reported) => Ok(()),
        other => Err(IoError::Unexpected {
            expected: "the recorded progress",
            got: other.label(),
        }),
    }
}

fn stops_of(result: IoResult) -> Result<JobStops, IoError> {
    let event = result.event()?;
    match event {
        Event::Job(JobEvent::Stops(stops)) => Ok(stops),
        other => Err(IoError::Unexpected {
            expected: "the job's stop state",
            got: other.label(),
        }),
    }
}

/// What one page of a lane left.
#[derive(Debug, PartialEq)]
enum LanePage {
    Walked(OpenLane),
    Stopped,
}

#[derive(Debug, PartialEq)]
enum PageStep {
    Stops,
    Walk,
    Report(Option<Box<OpenLane>>),
}

/// Checks that the job goes on, walks one page of a lane and adds the lane's progress.
#[derive(Debug, PartialEq)]
struct LanePageOperation {
    run: StagingRun,
    slot: u64,
    lane: Option<OpenLane>,
    step: PageStep,
}

impl LanePageOperation {
    fn new(run: StagingRun, slot: u64, lane: OpenLane) -> Self {
        Self {
            run,
            slot,
            lane: Some(lane),
            step: PageStep::Stops,
        }
    }
}

impl Operation for LanePageOperation {
    type Output = LanePage;
    type Error = PageFailure;

    fn first(&mut self) -> Result<Next<LanePage>, PageFailure> {
        Ok(Next::Call(job_call(JobEffect::Stops)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LanePage>, PageFailure> {
        match std::mem::replace(&mut self.step, PageStep::Walk) {
            PageStep::Stops => {
                if stops_of(result)?.any() {
                    return Ok(Next::Done(LanePage::Stopped));
                }
                let lane = self.lane.as_mut().ok_or(lost_lane())?;
                let place = WalkPlace {
                    slot: Some(self.slot),
                    upper: lane.upper.clone(),
                    lane: true,
                };
                let walk = WalkPageOperation::new(
                    self.run.clone(),
                    place,
                    lane.checkpoint.clone(),
                    lane.ahead.take(),
                );
                Ok(Next::Call(IoCall::child(walk)))
            }
            PageStep::Walk => {
                let walked = result.child::<WalkedPage, PageFailure>()??;
                let mut lane = self.lane.take().ok_or(lost_lane())?;
                let before = lane.checkpoint.items_current;
                let advance = walked.checkpoint.items_current.saturating_sub(before);
                lane.checkpoint = walked.checkpoint;
                lane.ahead = walked.ahead;
                self.step = PageStep::Report(Some(Box::new(lane)));
                let report = JobReport {
                    advance,
                    checkpoint: true,
                    ..JobReport::default()
                };
                Ok(Next::Call(job_call(JobEffect::Report(report))))
            }
            PageStep::Report(lane) => {
                reported(result)?;
                let lane = lane.ok_or(lost_lane())?;
                Ok(Next::Done(LanePage::Walked(*lane)))
            }
        }
    }
}

fn lost_lane() -> PageFailure {
    PageFailure::Retry("the lane page lost its lane".to_string())
}

fn state_row<T: serde::Serialize>(
    job_id: JobId,
    slot: Option<u64>,
    row: &T,
) -> Result<(Key, Value), PageFailure> {
    Ok((checkpoint_key(job_id, slot), encode(row)?))
}

/// What opening the next prefix found.
#[derive(Debug, PartialEq)]
enum Opened {
    /// The lanes of the prefix, none once every prefix is open.
    Lanes {
        lanes: Vec<Lane>,
        prefixes_done: u64,
    },
    Stopped,
}

/// Finds where the keys below a prefix fork: occupied starts, refined a level per round by
/// one-key listings until there are `parts` or the probes run out.
#[derive(Debug, PartialEq)]
struct SplitPlan {
    parts: usize,
    probes: usize,
    starts: Vec<String>,
    refined: Vec<String>,
    index: usize,
    shared: String,
    lower: String,
    found: Vec<char>,
}

impl SplitPlan {
    fn new(source_path: &str, parts: usize) -> Self {
        Self {
            parts,
            probes: 0,
            starts: vec![source_path.to_string()],
            refined: Vec::new(),
            index: 0,
            shared: String::new(),
            lower: String::new(),
            found: Vec::new(),
        }
    }

    /// Starts the plan; the key to list first, or `None` when no listing is needed.
    fn begin(&mut self) -> Option<String> {
        self.begin_round()
    }

    fn begin_round(&mut self) -> Option<String> {
        if self.parts <= 1 || self.starts.len() >= self.parts || self.probes >= MAX_PROBES {
            return None;
        }
        self.refined = Vec::with_capacity(self.starts.len());
        self.index = 0;
        self.begin_start()
    }

    fn begin_start(&mut self) -> Option<String> {
        match self.starts.get(self.index) {
            Some(start) => {
                self.shared = start.clone();
                self.begin_branch()
            }
            None => self.end_round(),
        }
    }

    fn begin_branch(&mut self) -> Option<String> {
        if self.probes >= MAX_PROBES {
            return self.end_start(Vec::new());
        }
        self.found.clear();
        self.lower = self.shared.clone();
        self.probe()
    }

    fn probe(&mut self) -> Option<String> {
        if self.probes >= MAX_PROBES {
            return self.end_branch();
        }
        self.probes += 1;
        Some(self.lower.clone())
    }

    /// Takes the first key at or after the last listed one; the next key to list, if any.
    fn answer(&mut self, key: Option<String>) -> Option<String> {
        let Some(key) = key else {
            return self.end_branch();
        };
        let Some(rest) = key.strip_prefix(self.shared.as_str()) else {
            return self.end_branch();
        };
        let Some(first) = rest.chars().next() else {
            // A key that ends here sorts first; the probe goes on past it.
            self.lower = format!("{key}\0");
            return self.probe();
        };
        self.found.push(first);
        match char::from_u32(u32::from(first) + 1) {
            Some(after) => {
                self.lower = format!("{}{after}", self.shared);
                self.probe()
            }
            None => self.end_branch(),
        }
    }

    fn end_branch(&mut self) -> Option<String> {
        match self.found.as_slice() {
            [] => self.end_start(Vec::new()),
            [only] => {
                self.shared.push(*only);
                self.begin_branch()
            }
            found => {
                let branches = (found.iter())
                    .map(|first| format!("{}{first}", self.shared))
                    .collect();
                self.end_start(branches)
            }
        }
    }

    fn end_start(&mut self, branches: Vec<String>) -> Option<String> {
        match branches.is_empty() {
            true => self.refined.push(self.starts[self.index].clone()),
            false => self.refined.extend(branches),
        }
        self.index += 1;
        self.begin_start()
    }

    fn end_round(&mut self) -> Option<String> {
        if self.refined.len() == self.starts.len() {
            return None;
        }
        self.starts = std::mem::take(&mut self.refined);
        self.begin_round()
    }

    /// Source keys where the lanes after the first start, at most `parts - 1`.
    fn splits(&self) -> Vec<String> {
        if self.starts.len() < 2 {
            return Vec::new();
        }
        let lanes = self.parts.min(self.starts.len());
        (1..lanes)
            .map(|lane| self.starts[lane * self.starts.len() / lanes].clone())
            .collect()
    }
}

#[derive(Debug, PartialEq)]
enum OpenStep {
    Stops,
    Probe,
    Commit(Vec<Lane>),
}

/// Opens the lanes of the next spec prefix, at most `room`, recorded with the job's checkpoint
/// in one write; none once every prefix is open.
#[derive(Debug, PartialEq)]
struct OpenPrefixOperation {
    run: StagingRun,
    main: StagingJobCheckpoint,
    room: usize,
    walk: Option<StagingWalk>,
    plan: Option<SplitPlan>,
    step: OpenStep,
}

impl OpenPrefixOperation {
    fn new(run: StagingRun, main: StagingJobCheckpoint, room: usize) -> Self {
        Self {
            run,
            main,
            room,
            walk: None,
            plan: None,
            step: OpenStep::Stops,
        }
    }

    fn probe(&mut self, lower: String) -> Result<Next<Opened>, PageFailure> {
        let walk = self.walk.as_ref().ok_or(lost_walk())?;
        self.step = OpenStep::Probe;
        let spec = &self.run.spec;
        let list = ListStagingOperation::new(ListStagingInput {
            group_id: spec.group_id,
            connector_id: spec.connector_id,
            source_path: walk.source_path.clone(),
            request: ListRequest {
                cursor: None,
                lower: Some(lower),
                stop: None,
                limit: 1,
                recursive: true,
                files_only: true,
            },
        });
        Ok(Next::Call(IoCall::child(list)))
    }

    /// Writes the lanes between the plan's split points with the job's checkpoint.
    fn open(&mut self) -> Result<Next<Opened>, PageFailure> {
        let walk = self.walk.clone().ok_or(lost_walk())?;
        let splits = self
            .plan
            .as_ref()
            .map(SplitPlan::splits)
            .unwrap_or_default();
        let bounds: Vec<Option<String>> = std::iter::once(None)
            .chain(splits.iter().map(|split| target_of(&walk, split)))
            .collect();
        let index = self.main.prefixes_done;
        let job_id = self.run.job_id;
        let mut lane = self.main.clone();
        lane.phase = StagingJobPhase::Registering;
        (lane.items_current, lane.items_succeeded, lane.items_failed) = (0, 0, 0);
        (lane.bytes_current, lane.bytes_discovered) = (0, 0);
        lane.current_path = None;
        lane.errors = Vec::new();
        let mut opened = Vec::new();
        let mut rows = Vec::new();
        for (part, from) in bounds.iter().enumerate() {
            let slot = index * PARTS + part as u64;
            let upper = bounds.get(part + 1).cloned().flatten();
            let mut part_lane = lane.clone();
            part_lane.walk = Some(StagingWalk {
                from: from.clone(),
                ..walk.clone()
            });
            let (key, value) = state_row(job_id, Some(slot), &part_lane)?;
            rows.push((STAGING_STATE_KEYSPACE.to_string(), key, value));
            let bound = (bound_key(job_id, slot), encode(&upper)?);
            rows.push((STAGING_STATE_KEYSPACE.to_string(), bound.0, bound.1));
            opened.push((slot, part_lane, upper));
        }
        self.main.prefixes_done += 1;
        self.step = OpenStep::Commit(opened);
        let call = (self.run)
            .commit_call(None, &self.main, rows, Vec::new())
            .map_err(PageFailure::Retry)?;
        Ok(Next::Call(call))
    }
}

fn lost_walk() -> PageFailure {
    PageFailure::Retry("the opened prefix lost its walk".to_string())
}

impl Operation for OpenPrefixOperation {
    type Output = Opened;
    type Error = PageFailure;

    fn first(&mut self) -> Result<Next<Opened>, PageFailure> {
        Ok(Next::Call(job_call(JobEffect::Stops)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Opened>, PageFailure> {
        match std::mem::replace(&mut self.step, OpenStep::Probe) {
            OpenStep::Stops => {
                if stops_of(result)?.any() {
                    return Ok(Next::Done(Opened::Stopped));
                }
                let spec = self.run.spec.clone();
                let prefix = usize::try_from(self.main.prefixes_done)
                    .ok()
                    .and_then(|at| spec.prefixes.get(at));
                let Some(prefix) = prefix else {
                    return Ok(Next::Done(Opened::Lanes {
                        lanes: Vec::new(),
                        prefixes_done: self.main.prefixes_done,
                    }));
                };
                let walk = StagingWalk {
                    source_path: directory_list_path(&prefix.source_prefix),
                    target_prefix: prefix.target_prefix.clone(),
                    cursor: None,
                    queue_row: None,
                    from: None,
                };
                let mut plan = SplitPlan::new(&walk.source_path, self.room);
                let lower = plan.begin();
                self.walk = Some(walk);
                self.plan = Some(plan);
                match lower {
                    Some(lower) => self.probe(lower),
                    None => self.open(),
                }
            }
            OpenStep::Probe => {
                let listed = result.child::<ListStagingResult, ListStagingError>()?;
                let listed = listed.map_err(|error| PageFailure::Retry(error.to_string()))?;
                let key = listed.entries.into_iter().next().map(|entry| entry.path);
                let plan = self.plan.as_mut().ok_or(lost_walk())?;
                match plan.answer(key) {
                    Some(lower) => self.probe(lower),
                    None => self.open(),
                }
            }
            OpenStep::Commit(lanes) => {
                committed(result).map_err(PageFailure::Retry)?;
                Ok(Next::Done(Opened::Lanes {
                    lanes,
                    prefixes_done: self.main.prefixes_done,
                }))
            }
        }
    }
}

fn decode<T: serde::de::DeserializeOwned>(value: &[u8]) -> Result<T, String> {
    postcard::from_bytes(value).map_err(|error| error.to_string())
}

fn encode<T: serde::Serialize>(value: &T) -> Result<ByteView, PageFailure> {
    let bytes = postcard::to_allocvec(value).map_err(|error| PageFailure::Retry(error.to_string()));
    Ok(ByteView::from(bytes?))
}

#[cfg(test)]
#[path = "lanes_tests.rs"]
mod tests;
