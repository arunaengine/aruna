//! Walks one provenance export a step at a time. The caller reads what the export asks for and
//! hands back the answer; the export decides what it adds, what to read next and when to stop.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::PROVENANCE_SPOOL_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::types::{Key, Value as Row};
use serde_json::Value;

use super::workspace::{ENTITY, PART, prefix, row_name};
use super::{
    Awaiting, CrateFile, ExportState, GRAPH_END, Hop, PARTS_END, ProvenanceCrate, Target,
    entity_piece, part_piece,
};
use crate::metadata::provenance::workspace::Workspace;

/// The reason a walk ends when the caller's credential ended.
const REVOKED: &str = "the token expired or was revoked";
/// The reason a walk ends when a read ran out of time or the caller left.
const ENDED: &str = "the export stopped at its time limit or on disconnect";

/// Rows a spool cleanup deletes per call.
const FORGET_PAGE: usize = 256;

/// What a read the export asked for gave the caller.
#[derive(Clone, Debug, PartialEq)]
pub enum Read {
    Answer(Value),
    /// The caller may not read it.
    Hidden,
    /// It did not answer.
    Silent,
}

/// The caller's answer to the export's last ask.
#[derive(Clone, Debug, PartialEq)]
pub enum ExportAnswer {
    /// Starts the walk at the export's start version.
    Begin,
    /// What the read gave, or `None` when it ran out of time or the caller left.
    Read(Option<Read>),
    /// The caller's credential ended before the read.
    Revoked,
}

/// What the export asks of its caller next.
#[derive(Clone, Debug, PartialEq)]
pub enum ExportAsk {
    /// Read the dataset the export starts at.
    Dataset,
    /// Read this hop with the caller's credential.
    Hop(Hop),
    /// Read this run crate document with the credential as it holds now.
    RunCrate(String),
    /// The walk ended; page the root metadata.
    Root,
}

/// The outcome of one step: the state to keep, the files to write in order, and the next ask.
#[derive(Debug, PartialEq)]
pub struct ExportStep {
    pub state: ExportState,
    pub files: Vec<CrateFile>,
    pub ask: ExportAsk,
}

// The answer applied to the crate: the files it adds and whether the walk goes on.
fn apply(built: &mut ProvenanceCrate, answer: ExportAnswer) -> (Vec<CrateFile>, Option<ExportAsk>) {
    let awaiting = built.state().awaiting.take();
    let mut files = Vec::new();
    match (awaiting, answer) {
        (_, ExportAnswer::Begin) => {
            let first = built.version_start();
            built.push([first]);
        }
        (Some(Awaiting::Dataset), answer) => match answer {
            ExportAnswer::Read(Some(Read::Answer(dataset))) => {
                let (hops, file) = built.dataset(&dataset);
                files.push(file);
                built.push(hops);
            }
            ExportAnswer::Read(Some(Read::Hidden)) => built.stop("the dataset is not readable"),
            ExportAnswer::Read(Some(Read::Silent)) => built.stop("the dataset did not answer"),
            ExportAnswer::Read(None) => built.stop("stopped before the dataset was read"),
            ExportAnswer::Revoked | ExportAnswer::Begin => built.stop(REVOKED),
        },
        (Some(Awaiting::RunCrate(hop)), answer) => {
            let read = match answer {
                ExportAnswer::Read(read) => read,
                ExportAnswer::Revoked | ExportAnswer::Begin => {
                    built.run(&hop, None);
                    built.stop(REVOKED);
                    return (files, Some(ExportAsk::Root));
                }
            };
            let (hops, file, go_on) = record_run(built, &hop, read);
            files.extend(file);
            built.push(hops);
            if !go_on {
                return (files, Some(ExportAsk::Root));
            }
        }
        (Some(Awaiting::Hop(hop)), answer) => {
            let read = match answer {
                ExportAnswer::Read(Some(read)) => read,
                ExportAnswer::Read(None) => {
                    built.stop(ENDED);
                    return (files, Some(ExportAsk::Root));
                }
                ExportAnswer::Revoked | ExportAnswer::Begin => {
                    built.stop(REVOKED);
                    return (files, Some(ExportAsk::Root));
                }
            };
            if let Some(ask) = record_hop(built, hop, read, &mut files) {
                return (files, Some(ask));
            }
        }
        (None, _) => return (files, Some(ExportAsk::Root)),
    }
    (files, None)
}

// Records what one hop read gave; a run with a crate document asks for that document next.
fn record_hop(
    built: &mut ProvenanceCrate,
    hop: Hop,
    read: Read,
    files: &mut Vec<CrateFile>,
) -> Option<ExportAsk> {
    match (&hop.target, read) {
        (Target::Version(_), Read::Answer(origin)) => {
            let next = built.version(&hop, &origin);
            built.push(next);
        }
        (Target::Run(_), Read::Answer(run)) => {
            if let Some(document) = run["run_crate"].as_str() {
                built.state().awaiting = Some(Awaiting::RunCrate(hop));
                return Some(ExportAsk::RunCrate(document.to_string()));
            }
            let status = &run["run_crate_status"];
            if let Some(state) = status["status"].as_str()
                && built.unwritten(&hop, state, status["message"].as_str())
            {
                return None;
            }
            let (hops, file, _) = record_run(built, &hop, Some(Read::Hidden));
            files.extend(file);
            built.push(hops);
        }
        (Target::Run(_), Read::Hidden) => {
            built.run(&hop, None);
        }
        (Target::Release(_), Read::Answer(release)) => {
            files.extend(built.release(&hop, Some(&release)));
        }
        (Target::Release(_), read) => {
            built.release(&hop, None);
            if read == Read::Silent {
                built.incomplete("a release crate did not answer");
            }
        }
        (_, Read::Hidden) => built.hidden(&hop),
        (_, Read::Silent) => built.unreachable(&hop),
    }
    None
}

/// Records a run with what its crate read gave. A crate that did not answer in time leaves the
/// export incomplete, never "not visible"; `false` ends the walk at the time limit.
fn record_run(
    built: &mut ProvenanceCrate,
    hop: &Hop,
    read: Option<Read>,
) -> (Vec<Hop>, Option<CrateFile>, bool) {
    match read {
        Some(Read::Answer(run_crate)) => {
            let (hops, file) = built.run(hop, Some(&run_crate));
            (hops, file, true)
        }
        Some(Read::Hidden) => {
            built.run(hop, None);
            (Vec::new(), None, true)
        }
        Some(Read::Silent) => {
            built.run(hop, None);
            built.incomplete("a run record did not answer");
            (Vec::new(), None, true)
        }
        None => {
            built.run(hop, None);
            built.stop(ENDED);
            (Vec::new(), None, false)
        }
    }
}

/// Applies `answer` and takes the next link no earlier read covered.
pub(super) fn step(
    built: &mut ProvenanceCrate,
    answer: ExportAnswer,
) -> (Vec<CrateFile>, ExportAsk) {
    let (files, ask) = apply(built, answer);
    if let Some(ask) = ask {
        return (files, ask);
    }
    while let Some(hop) = built.pop() {
        if built.known(&hop) {
            continue;
        }
        built.state().awaiting = Some(Awaiting::Hop(hop.clone()));
        return (files, ExportAsk::Hop(hop));
    }
    (files, ExportAsk::Root)
}

fn row(bytes: Vec<u8>) -> Row {
    Row::from(bytes)
}

fn spool_key(key: Vec<u8>) -> (String, Key) {
    (PROVENANCE_SPOOL_KEYSPACE.to_string(), Key::from(key))
}

#[derive(Debug, PartialEq)]
enum StepStage {
    Rows(Vec<Vec<u8>>),
    Queue,
    Stored(Vec<Vec<u8>>),
    Deleted,
}

/// One step of an export: applies the caller's answer, reads the rows it needs and stores
/// the rows it changed in one transaction.
#[derive(Debug, PartialEq)]
pub struct ExportStepOperation {
    state: ExportState,
    answer: ExportAnswer,
    rows: Workspace,
    outcome: Option<ExportStep>,
    stage: StepStage,
}

type StepTurn = Result<Next<ExportStep>, IoError>;

impl ExportStepOperation {
    pub fn new(state: ExportState, answer: ExportAnswer) -> Self {
        let rows = Workspace::partial(state.export);
        Self {
            state,
            answer,
            rows,
            outcome: None,
            stage: StepStage::Deleted,
        }
    }

    /// The export that asks for its dataset first.
    pub fn dataset(mut state: ExportState) -> ExportStep {
        state.awaiting = Some(Awaiting::Dataset);
        ExportStep {
            state,
            files: Vec::new(),
            ask: ExportAsk::Dataset,
        }
    }

    // Runs the step on the rows loaded so far; a step that missed rows loads them and reruns.
    fn run(&mut self) -> StepTurn {
        let rows = std::mem::take(&mut self.rows);
        let mut built = ProvenanceCrate::new(self.state.clone(), rows);
        let (files, ask) = step(&mut built, self.answer.clone());
        let (state, rows) = built.into_parts();
        self.rows = rows;
        if !self.rows.misses().is_empty() {
            let keys: Vec<Vec<u8>> = self.rows.misses().iter().cloned().collect();
            let read = keys.iter().cloned().map(spool_key).collect();
            self.rows.reset();
            self.stage = StepStage::Rows(keys);
            return Ok(Next::Call(IoCall::Read(read)));
        }
        if self.rows.queue_missed() {
            let after = self.rows.last_queued().map(Key::from);
            self.rows.reset();
            self.stage = StepStage::Queue;
            return Ok(Next::Call(IoCall::Page {
                space: PROVENANCE_SPOOL_KEYSPACE.to_string(),
                prefix: Key::from(prefix(self.rows.export(), Some(super::workspace::QUEUE))),
                after,
                limit: 1,
            }));
        }
        let (stored, deleted) = self.rows.take_writes();
        self.outcome = Some(ExportStep { state, files, ask });
        let writes: Vec<_> = (stored.into_iter())
            .map(|(key, value)| {
                let (space, key) = spool_key(key);
                (space, key, row(value))
            })
            .collect();
        self.stage = StepStage::Stored(deleted);
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn finish(&mut self) -> StepTurn {
        let outcome = self.outcome.take().ok_or(IoError::Unexpected {
            expected: "the step's outcome",
            got: "none",
        })?;
        Ok(Next::Done(outcome))
    }
}

impl Operation for ExportStepOperation {
    type Output = ExportStep;
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> StepTurn {
        self.run()
    }

    fn next(&mut self, result: IoResult) -> StepTurn {
        match std::mem::replace(&mut self.stage, StepStage::Deleted) {
            StepStage::Rows(keys) => {
                let values = result.values()?;
                let loaded = keys.into_iter().zip(values);
                self.rows
                    .load(loaded.map(|(key, value)| (key, value.map(|value| value.to_vec()))));
                self.run()
            }
            StepStage::Queue => {
                let (rows, _) = result.page()?;
                if rows.is_empty() {
                    self.rows.queue_ended();
                }
                let loaded = rows
                    .into_iter()
                    .map(|(key, value)| (key.to_vec(), Some(value.to_vec())));
                self.rows.load(loaded);
                self.run()
            }
            StepStage::Stored(deleted) => {
                result.done()?;
                if deleted.is_empty() {
                    return self.finish();
                }
                self.stage = StepStage::Deleted;
                let keys = deleted.into_iter().map(spool_key).collect();
                Ok(Next::Call(IoCall::Delete(keys)))
            }
            StepStage::Deleted => {
                result.done()?;
                self.finish()
            }
        }
    }
}

/// Where the root metadata continues: its head, its parts after a key, or its entities after a
/// key.
#[derive(Clone, Debug, PartialEq)]
pub enum RootCursor {
    Head,
    Parts { after: Option<Vec<u8>> },
    Entities { after: Option<Vec<u8>> },
}

/// One piece of the root `ro-crate-metadata.json`: the head, one part or one entity, so no
/// more than one record is held at once. Answers the piece and where to go on, or `None` after
/// the last piece.
#[derive(Debug, PartialEq)]
pub struct RootPageOperation {
    state: ExportState,
    cursor: RootCursor,
}

type RootTurn = Result<Next<(String, Option<RootCursor>)>, IoError>;

impl RootPageOperation {
    pub fn new(state: ExportState, cursor: RootCursor) -> Self {
        Self { state, cursor }
    }

    fn page(&self, kind: u8, after: &Option<Vec<u8>>) -> IoCall {
        IoCall::Page {
            space: PROVENANCE_SPOOL_KEYSPACE.to_string(),
            prefix: Key::from(prefix(self.state.export, Some(kind))),
            after: after.clone().map(Key::from),
            limit: 1,
        }
    }
}

impl Operation for RootPageOperation {
    type Output = (String, Option<RootCursor>);
    type Error = IoError;

    fn first(&mut self) -> RootTurn {
        Ok(Next::Call(match &self.cursor {
            RootCursor::Head => {
                let root = super::workspace::row_key(self.state.export, ENTITY, b"./");
                IoCall::Read(vec![spool_key(root)])
            }
            RootCursor::Parts { after } => self.page(PART, after),
            RootCursor::Entities { after } => self.page(ENTITY, after),
        }))
    }

    fn next(&mut self, result: IoResult) -> RootTurn {
        let piece = match &self.cursor {
            RootCursor::Head => {
                let root = result.value()?.map(|value| value.to_vec());
                let mut rows = Workspace::complete(self.state.export);
                let key = super::workspace::row_key(self.state.export, ENTITY, b"./");
                rows.load([(key, root)]);
                let mut built = ProvenanceCrate::new(self.state.clone(), rows);
                (built.root_head(), Some(RootCursor::Parts { after: None }))
            }
            RootCursor::Parts { after } => match result.page()?.0.into_iter().next() {
                Some((key, _)) => {
                    let piece = part_piece(after.is_none(), row_name(&key));
                    (
                        piece,
                        Some(RootCursor::Parts {
                            after: Some(key.to_vec()),
                        }),
                    )
                }
                None => (
                    PARTS_END.to_string(),
                    Some(RootCursor::Entities { after: None }),
                ),
            },
            RootCursor::Entities { .. } => match result.page()?.0.into_iter().next() {
                Some((key, value)) => {
                    let piece = entity_piece(&value).unwrap_or_default();
                    (
                        piece,
                        Some(RootCursor::Entities {
                            after: Some(key.to_vec()),
                        }),
                    )
                }
                None => (GRAPH_END.to_string(), None),
            },
        };
        Ok(Next::Done(piece))
    }
}

/// Deletes the rows of one export, or of every export when the node starts, a page at a time.
#[derive(Debug, PartialEq)]
pub struct ForgetSpoolOperation {
    prefix: Vec<u8>,
    deleting: bool,
}

impl ForgetSpoolOperation {
    /// The rows of the export `state` names.
    pub fn export(state: &ExportState) -> Self {
        Self {
            prefix: prefix(state.export, None),
            deleting: false,
        }
    }

    /// Every export's rows; a node that starts runs no export yet.
    pub fn all() -> Self {
        Self {
            prefix: Vec::new(),
            deleting: false,
        }
    }

    fn page(&mut self) -> Result<Next<()>, IoError> {
        self.deleting = false;
        Ok(Next::Call(IoCall::Page {
            space: PROVENANCE_SPOOL_KEYSPACE.to_string(),
            prefix: Key::from(self.prefix.clone()),
            after: None,
            limit: FORGET_PAGE,
        }))
    }
}

impl Operation for ForgetSpoolOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        self.page()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.deleting {
            result.done()?;
            return self.page();
        }
        let (rows, _) = result.page()?;
        if rows.is_empty() {
            return Ok(Next::Done(()));
        }
        self.deleting = true;
        let keys = rows
            .into_iter()
            .map(|(key, _)| (PROVENANCE_SPOOL_KEYSPACE.to_string(), key));
        Ok(Next::Call(IoCall::Delete(keys.collect())))
    }
}
