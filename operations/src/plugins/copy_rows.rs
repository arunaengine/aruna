//! The rows of this node's artifact copies and the operations over them: chunk rows, a progress
//! row per unfinished copy and a done row per complete one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};
use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::PLUGIN_ARTIFACT_KEYSPACE;
use aruna_core::operation::{HoldScope, Operation};
use aruna_core::plugins::{ArtifactBytes, Chunk, MAX_ARTIFACT};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_plugins::artifact::{ArtifactDigest, check_artifact};

use super::artifacts::ArtifactError;
use super::copies::{Assembled, MemoryPermit, reserve};
use super::rows::{Row, Rows, Scan, StoreError, delete_call, done, read_call, row, values, writes};

/// Bytes of one artifact chunk; node messages stay far below their 16 MiB limit.
pub const CHUNK: usize = 8 << 20;
/// Bytes of unfinished and unpublished copies one node keeps at most.
pub const MAX_UNSETTLED: u64 = 8 * MAX_ARTIFACT as u64;
/// How long an unfinished or unpublished copy stays untouched before it is reclaimed. It is
/// measured in this node's wall time, so a clock correction shortens or extends it.
pub const GRACE: Duration = Duration::from_secs(3600);

const DONE_PREFIX: &[u8] = b"d/";
const PROGRESS_PREFIX: &[u8] = b"p/";

/// The chunks an artifact of `total` bytes has.
pub fn chunk_count(total: u64) -> u32 {
    total.div_ceil(CHUNK as u64) as u32
}

/// The exact length of chunk `index` of an artifact of `total` bytes.
pub fn chunk_len(total: u64, index: u32) -> u64 {
    total
        .saturating_sub(u64::from(index) * CHUNK as u64)
        .min(CHUNK as u64)
}

/// The `index`-th chunk of `bytes`, ready to send.
pub fn chunk_of(digest: &str, bytes: &[u8], index: u32) -> Chunk {
    Chunk {
        digest: digest.to_string(),
        index,
        total: bytes.len() as u64,
        bytes: ArtifactBytes(chunk_bytes(bytes, index).to_vec()),
    }
}

fn chunk_bytes(bytes: &[u8], index: u32) -> &[u8] {
    let start = (index as usize * CHUNK).min(bytes.len());
    let end = (start + CHUNK).min(bytes.len());
    &bytes[start..end]
}

/// Whether a chunk fits its artifact: a size within the limit, an index and the exact length.
pub fn chunk_fits(chunk: &Chunk) -> bool {
    let sized = (1..=MAX_ARTIFACT as u64).contains(&chunk.total);
    let placed = chunk.index < chunk_count(chunk.total);
    sized && placed && chunk.bytes.0.len() as u64 == chunk_len(chunk.total, chunk.index)
}

/// Digests whose copies stay: published versions and versions an install pins.
fn kept_digests(config: &RealmConfigDocument) -> BTreeSet<String> {
    let pinned = config.installs.iter().map(|install| &install.digest);
    (config.plugins.iter().map(|plugin| &plugin.digest))
        .chain(pinned)
        .cloned()
        .collect()
}

/// A complete copy: its length and when it was last confirmed, in this node's clock.
#[derive(Debug, PartialEq)]
struct Done {
    total: u64,
    completed_ms: u64,
}

impl Done {
    fn encode(total: u64, completed_ms: u64) -> Vec<u8> {
        [total.to_le_bytes(), completed_ms.to_le_bytes()].concat()
    }

    fn decode(row: &[u8]) -> Option<Self> {
        let (total, completed_ms) = two_numbers(row)?;
        Some(Self {
            total,
            completed_ms,
        })
    }
}

/// An unfinished copy: its length, its last change and which chunks arrived.
#[derive(Debug, PartialEq)]
struct Progress {
    total: u64,
    updated_ms: u64,
    received: Vec<u8>,
}

impl Progress {
    fn new(total: u64, now_ms: u64) -> Self {
        let bits = chunk_count(total).div_ceil(8) as usize;
        Self {
            total,
            updated_ms: now_ms,
            received: vec![0; bits],
        }
    }

    fn mark(&mut self, index: u32, now_ms: u64) {
        self.received[index as usize / 8] |= 1 << (index % 8);
        self.updated_ms = now_ms;
    }

    fn complete(&self) -> bool {
        (0..chunk_count(self.total)).all(|i| self.received[i as usize / 8] & (1 << (i % 8)) != 0)
    }

    fn encode(&self) -> Vec<u8> {
        [
            &self.total.to_le_bytes()[..],
            &self.updated_ms.to_le_bytes(),
            &self.received,
        ]
        .concat()
    }

    fn decode(row: &[u8]) -> Option<Self> {
        let (total, updated_ms) = two_numbers(row.get(..16)?)?;
        let received = row[16..].to_vec();
        let fits = (1..=MAX_ARTIFACT as u64).contains(&total)
            && received.len() == chunk_count(total).div_ceil(8) as usize;
        fits.then_some(Self {
            total,
            updated_ms,
            received,
        })
    }
}

fn two_numbers(row: &[u8]) -> Option<(u64, u64)> {
    let first = u64::from_le_bytes(row.get(..8)?.try_into().ok()?);
    let second = u64::from_le_bytes(row.get(8..16)?.try_into().ok()?);
    (row.len() == 16).then_some((first, second))
}

fn chunk_key(digest: &str, index: u32) -> Vec<u8> {
    [b"c/", digest.as_bytes(), b"/", &index.to_be_bytes()].concat()
}

fn done_key(digest: &str) -> Vec<u8> {
    [DONE_PREFIX, digest.as_bytes()].concat()
}

fn progress_key(digest: &str) -> Vec<u8> {
    [PROGRESS_PREFIX, digest.as_bytes()].concat()
}

fn storage(why: &str) -> ArtifactError {
    ArtifactError::Storage(why.to_string())
}

fn stored_done(row: Option<Vec<u8>>) -> Result<Option<Done>, ArtifactError> {
    row.map(|row| Done::decode(&row).ok_or_else(|| storage("a bad done row")))
        .transpose()
}

fn stored_progress(row: Option<Vec<u8>>) -> Result<Option<Progress>, ArtifactError> {
    row.map(|row| Progress::decode(&row).ok_or_else(|| storage("a bad progress row")))
        .transpose()
}

fn artifact_rows(rows: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<Row> {
    let rows = rows.into_iter();
    rows.map(|(key, value)| row(PLUGIN_ARTIFACT_KEYSPACE, key, value))
        .collect()
}

fn spaced(keys: Vec<Vec<u8>>) -> Vec<(&'static str, Vec<u8>)> {
    let keys = keys.into_iter();
    keys.map(|key| (PLUGIN_ARTIFACT_KEYSPACE, key)).collect()
}

fn read_key(key: Vec<u8>) -> IoCall {
    read_call(spaced(vec![key]))
}

/// Reads the done row and the progress row of `digest`, in this order.
fn read_state(digest: &str) -> IoCall {
    read_call(spaced(vec![done_key(digest), progress_key(digest)]))
}

fn one_value(result: IoResult) -> Result<Option<Vec<u8>>, ArtifactError> {
    let [value] = values::<1>(result)?;
    Ok(value)
}

/// Writes `rows` and deletes `keys` in one transaction: all of them or none.
fn apply(rows: Vec<(Vec<u8>, Vec<u8>)>, keys: Vec<Vec<u8>>) -> IoCall {
    IoCall::child(writes(artifact_rows(rows), spaced(keys)))
}

fn applied(result: IoResult) -> Result<(), ArtifactError> {
    Ok(result.child::<(), StoreError>()??)
}

/// The rows of a scan below `prefix`, by digest, that decode; others wait for a later sweep.
fn digest_rows<T>(rows: Rows, prefix: &[u8], decode: fn(&[u8]) -> Option<T>) -> Vec<(String, T)> {
    (rows.into_iter())
        .filter_map(|(key, value)| {
            let digest = std::str::from_utf8(key.get(prefix.len()..)?).ok()?;
            Some((digest.to_string(), decode(&value)?))
        })
        .collect()
}

fn scan(prefix: &[u8]) -> Scan {
    Scan::new(PLUGIN_ARTIFACT_KEYSPACE, prefix.to_vec())
}

/// Sums the unfinished and unpublished bytes and refuses a new copy over the bound.
#[derive(Debug, PartialEq)]
struct Admission {
    kept: BTreeSet<String>,
    total: u64,
    unfinished: Option<u64>,
    scan: Scan,
}

impl Admission {
    /// The check for a new copy; a kept digest needs none.
    fn start(kept: BTreeSet<String>, digest: &str, total: u64) -> Option<Self> {
        (!kept.contains(digest)).then(|| Self {
            kept,
            total,
            unfinished: None,
            scan: scan(PROGRESS_PREFIX),
        })
    }

    fn call(&self) -> IoCall {
        self.scan.ask()
    }

    /// Takes one page; `true` once the copy is admitted.
    fn take(&mut self, result: IoResult) -> Result<bool, ArtifactError> {
        let Some(rows) = self.scan.take(result)? else {
            return Ok(false);
        };
        let Some(unfinished) = self.unfinished else {
            let copies = digest_rows(rows, PROGRESS_PREFIX, Progress::decode);
            self.unfinished = Some(copies.iter().map(|(_, copy)| copy.total).sum());
            self.scan = scan(DONE_PREFIX);
            return Ok(false);
        };
        let copies = digest_rows(rows, DONE_PREFIX, Done::decode).into_iter();
        let unpublished: u64 = copies
            .filter(|(digest, _)| !self.kept.contains(digest))
            .map(|(_, copy)| copy.total)
            .sum();
        let held = unfinished + unpublished;
        match held.saturating_add(self.total) <= MAX_UNSETTLED {
            true => Ok(true),
            false => Err(ArtifactError::Busy),
        }
    }
}

/// The length of this node's complete copy of a digest, if it has one.
#[derive(Debug, PartialEq)]
pub struct CopyTotal(String);

impl CopyTotal {
    pub fn new(digest: &str) -> Self {
        Self(digest.to_string())
    }
}

impl Operation for CopyTotal {
    type Output = Option<u64>;
    type Error = ArtifactError;

    fn first(&mut self) -> Result<Next<Option<u64>>, ArtifactError> {
        Ok(Next::Call(read_key(done_key(&self.0))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<u64>>, ArtifactError> {
        let copy = stored_done(one_value(result)?)?;
        Ok(Next::Done(copy.map(|copy| copy.total)))
    }
}

/// Reads every chunk of a copy in order; a missing chunk or wrong length is a storage fault.
#[derive(Debug, PartialEq)]
pub struct ReadCopy {
    digest: String,
    total: u64,
    index: u32,
    bytes: Vec<u8>,
}

impl ReadCopy {
    pub fn new(digest: &str, total: u64) -> Self {
        Self {
            digest: digest.to_string(),
            total,
            index: 0,
            bytes: Vec::with_capacity(total as usize),
        }
    }

    fn advance(&mut self) -> Next<Vec<u8>> {
        match self.index < chunk_count(self.total) {
            true => Next::Call(read_key(chunk_key(&self.digest, self.index))),
            false => Next::Done(std::mem::take(&mut self.bytes)),
        }
    }
}

impl Operation for ReadCopy {
    type Output = Vec<u8>;
    type Error = ArtifactError;

    fn first(&mut self) -> Result<Next<Vec<u8>>, ArtifactError> {
        Ok(self.advance())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<u8>>, ArtifactError> {
        let chunk = one_value(result)?.ok_or_else(|| storage("a chunk of a copy is missing"))?;
        if chunk.len() as u64 != chunk_len(self.total, self.index) {
            return Err(storage("a chunk of a copy has the wrong length"));
        }
        self.bytes.extend(chunk);
        self.index += 1;
        Ok(self.advance())
    }
}

/// One chunk of this node's complete copy and the copy's length.
#[derive(Debug, PartialEq)]
pub struct ReadChunk {
    digest: String,
    index: u32,
    total: Option<u64>,
}

impl ReadChunk {
    pub fn new(digest: &str, index: u32) -> Self {
        Self {
            digest: digest.to_string(),
            index,
            total: None,
        }
    }
}

impl Operation for ReadChunk {
    type Output = Option<(u64, Vec<u8>)>;
    type Error = ArtifactError;

    fn first(&mut self) -> Result<Next<Self::Output>, ArtifactError> {
        Ok(Next::Call(read_key(done_key(&self.digest))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ArtifactError> {
        let value = one_value(result)?;
        if let Some(total) = self.total {
            return Ok(Next::Done(value.map(|bytes| (total, bytes))));
        }
        let Some(copy) = stored_done(value)? else {
            return Ok(Next::Done(None));
        };
        if self.index >= chunk_count(copy.total) {
            return Ok(Next::Done(None));
        }
        self.total = Some(copy.total);
        Ok(Next::Call(read_key(chunk_key(&self.digest, self.index))))
    }
}

/// Writes the done row and drops the progress row in one transaction, then syncs: only then may
/// a copy count. A crash leaves either the unfinished copy or the complete one.
#[derive(Debug, PartialEq)]
pub struct FinishCopy {
    digest: String,
    total: u64,
    now_ms: u64,
    synced: bool,
}

impl FinishCopy {
    fn new(digest: String, total: u64, now_ms: u64) -> Self {
        Self {
            digest,
            total,
            now_ms,
            synced: false,
        }
    }
}

impl Operation for FinishCopy {
    type Output = ();
    type Error = ArtifactError;

    fn first(&mut self) -> Result<Next<()>, ArtifactError> {
        let row = (
            done_key(&self.digest),
            Done::encode(self.total, self.now_ms),
        );
        let progress = progress_key(&self.digest);
        Ok(Next::Call(apply(vec![row], vec![progress])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ArtifactError> {
        if self.synced {
            done(result)?;
            return Ok(Next::Done(()));
        }
        applied(result)?;
        self.synced = true;
        Ok(Next::Call(IoCall::Sync))
    }
}

fn copy_finished(result: IoResult) -> Result<(), ArtifactError> {
    result.child::<(), ArtifactError>()?
}

#[derive(Debug, PartialEq)]
enum StorePhase {
    Reading,
    Admitting(Admission),
    Writing,
    Finishing,
}

/// Writes this node's whole copy and syncs before anyone is told. Progress is kept with every
/// chunk, so an interrupted write is counted and swept like an unfinished copy from a peer.
#[derive(Debug, PartialEq)]
pub struct StoreCopy<'a> {
    kept: BTreeSet<String>,
    digest: String,
    bytes: &'a [u8],
    now_ms: u64,
    progress: Progress,
    index: u32,
    phase: StorePhase,
}

impl<'a> StoreCopy<'a> {
    pub fn new(config: &RealmConfigDocument, digest: &str, bytes: &'a [u8], now_ms: u64) -> Self {
        Self {
            kept: kept_digests(config),
            digest: digest.to_string(),
            bytes,
            now_ms,
            progress: Progress::new(bytes.len() as u64, now_ms),
            index: 0,
            phase: StorePhase::Reading,
        }
    }

    fn write_next(&mut self) -> Next<()> {
        let total = self.bytes.len() as u64;
        if self.index >= chunk_count(total) {
            self.phase = StorePhase::Finishing;
            let finish = FinishCopy::new(self.digest.clone(), total, self.now_ms);
            return Next::Call(IoCall::child(finish));
        }
        let index = self.index;
        self.index += 1;
        self.progress.mark(index, self.now_ms);
        let rows = vec![
            (
                chunk_key(&self.digest, index),
                chunk_bytes(self.bytes, index).to_vec(),
            ),
            (progress_key(&self.digest), self.progress.encode()),
        ];
        // The first write drops an older done row, so a half-replaced copy is never complete.
        let deletes = match index {
            0 => vec![done_key(&self.digest)],
            _ => Vec::new(),
        };
        self.phase = StorePhase::Writing;
        Next::Call(apply(rows, deletes))
    }
}

impl Operation for StoreCopy<'_> {
    type Output = ();
    type Error = ArtifactError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginCopies)
    }

    fn first(&mut self) -> Result<Next<()>, ArtifactError> {
        Ok(Next::Call(read_state(&self.digest)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ArtifactError> {
        match &mut self.phase {
            StorePhase::Reading => {
                let [copy, progress] = values::<2>(result)?;
                let fresh = stored_done(copy)?.is_none() && stored_progress(progress)?.is_none();
                let kept = std::mem::take(&mut self.kept);
                let total = self.bytes.len() as u64;
                if fresh && let Some(admission) = Admission::start(kept, &self.digest, total) {
                    let call = admission.call();
                    self.phase = StorePhase::Admitting(admission);
                    return Ok(Next::Call(call));
                }
                Ok(self.write_next())
            }
            StorePhase::Admitting(admission) => match admission.take(result)? {
                true => Ok(self.write_next()),
                false => Ok(Next::Call(admission.call())),
            },
            StorePhase::Writing => {
                applied(result)?;
                Ok(self.write_next())
            }
            StorePhase::Finishing => {
                copy_finished(result)?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// What keeping one chunk left behind.
#[derive(Debug, PartialEq, Eq)]
pub enum ChunkStored {
    /// More chunks are missing.
    Held,
    /// Every chunk is here; the whole copy still has to be checked.
    AllHere,
    /// The copy was already complete; its retention restarted and it is synced.
    Confirmed,
}

#[derive(Debug, PartialEq)]
enum ChunkPhase {
    Reading,
    Refreshing,
    Syncing,
    Admitting(Admission),
    Writing { complete: bool },
}

/// Keeps one chunk of a copy another management node sends. A resend to a done copy restarts its
/// retention before it is acknowledged.
#[derive(Debug, PartialEq)]
pub struct StoreChunk {
    kept: BTreeSet<String>,
    chunk: Chunk,
    now_ms: u64,
    phase: ChunkPhase,
}

impl StoreChunk {
    pub fn new(config: &RealmConfigDocument, chunk: Chunk, now_ms: u64) -> Self {
        Self {
            kept: kept_digests(config),
            chunk,
            now_ms,
            phase: ChunkPhase::Reading,
        }
    }

    fn write(&mut self, mut progress: Progress) -> IoCall {
        progress.mark(self.chunk.index, self.now_ms);
        let bytes = std::mem::take(&mut self.chunk.bytes.0);
        let rows = vec![
            (chunk_key(&self.chunk.digest, self.chunk.index), bytes),
            (progress_key(&self.chunk.digest), progress.encode()),
        ];
        self.phase = ChunkPhase::Writing {
            complete: progress.complete(),
        };
        IoCall::Write(artifact_rows(rows))
    }

    fn fresh(&self) -> Progress {
        Progress::new(self.chunk.total, self.now_ms)
    }

    fn read(&mut self, result: IoResult) -> Result<IoCall, ArtifactError> {
        let [copy, progress] = values::<2>(result)?;
        let total = self.chunk.total;
        if let Some(copy) = stored_done(copy)? {
            if copy.total != total {
                return Err(ArtifactError::Mismatch);
            }
            self.phase = ChunkPhase::Refreshing;
            let refreshed = Done::encode(total, self.now_ms);
            let rows = vec![(done_key(&self.chunk.digest), refreshed)];
            return Ok(IoCall::Write(artifact_rows(rows)));
        }
        match stored_progress(progress)? {
            Some(progress) if progress.total != total => Err(ArtifactError::Mismatch),
            Some(progress) => Ok(self.write(progress)),
            None => {
                let kept = std::mem::take(&mut self.kept);
                match Admission::start(kept, &self.chunk.digest, total) {
                    Some(admission) => {
                        let call = admission.call();
                        self.phase = ChunkPhase::Admitting(admission);
                        Ok(call)
                    }
                    None => Ok(self.write(self.fresh())),
                }
            }
        }
    }
}

impl Operation for StoreChunk {
    type Output = ChunkStored;
    type Error = ArtifactError;

    fn first(&mut self) -> Result<Next<ChunkStored>, ArtifactError> {
        if !chunk_fits(&self.chunk) {
            return Err(ArtifactError::Mismatch);
        }
        Ok(Next::Call(read_state(&self.chunk.digest)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ChunkStored>, ArtifactError> {
        match &mut self.phase {
            ChunkPhase::Reading => Ok(Next::Call(self.read(result)?)),
            ChunkPhase::Refreshing => {
                done(result)?;
                self.phase = ChunkPhase::Syncing;
                Ok(Next::Call(IoCall::Sync))
            }
            ChunkPhase::Syncing => {
                done(result)?;
                Ok(Next::Done(ChunkStored::Confirmed))
            }
            ChunkPhase::Admitting(admission) => match admission.take(result)? {
                true => Ok(Next::Call(self.write(self.fresh()))),
                false => Ok(Next::Call(admission.call())),
            },
            ChunkPhase::Writing { complete } => {
                let complete = *complete;
                done(result)?;
                Ok(Next::Done(match complete {
                    true => ChunkStored::AllHere,
                    false => ChunkStored::Held,
                }))
            }
        }
    }
}

/// Checks a copy whose chunks all arrived against its digest and as a plugin, then completes it.
#[derive(Debug, PartialEq)]
pub struct CompleteCopy {
    digest: String,
    total: u64,
    now_ms: u64,
    finishing: bool,
}

impl CompleteCopy {
    pub fn new(digest: String, total: u64, now_ms: u64) -> Self {
        Self {
            digest,
            total,
            now_ms,
            finishing: false,
        }
    }
}

impl Operation for CompleteCopy {
    type Output = ();
    type Error = ArtifactError;

    fn first(&mut self) -> Result<Next<()>, ArtifactError> {
        let read = ReadCopy::new(&self.digest, self.total);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ArtifactError> {
        if self.finishing {
            copy_finished(result)?;
            return Ok(Next::Done(()));
        }
        let bytes = result.child::<Vec<u8>, ArtifactError>()??;
        let digest = ArtifactDigest::of(&bytes);
        if digest.as_str() != self.digest || check_artifact(&bytes).is_err() {
            return Err(ArtifactError::Mismatch);
        }
        self.finishing = true;
        let finish = FinishCopy::new(self.digest.clone(), self.total, self.now_ms);
        Ok(Next::Call(IoCall::child(finish)))
    }
}

#[derive(Debug, PartialEq)]
enum SweepPhase {
    ScanningDone(Scan),
    ScanningProgress(Scan, Vec<(String, Done)>),
    Deleting,
}

/// Deletes unfinished copies and unpublished complete copies untouched for the grace period.
/// Published and pinned copies stay, and a stale progress row next to a done row only loses
/// itself. Answers how many copies it deleted.
#[derive(Debug, PartialEq)]
pub struct SweepCopies {
    kept: BTreeSet<String>,
    now_ms: u64,
    deletes: VecDeque<Vec<Vec<u8>>>,
    deleted: usize,
    phase: SweepPhase,
}

impl SweepCopies {
    pub fn new(config: &RealmConfigDocument, now_ms: u64) -> Self {
        Self {
            kept: kept_digests(config),
            now_ms,
            deletes: VecDeque::new(),
            deleted: 0,
            phase: SweepPhase::ScanningDone(scan(DONE_PREFIX)),
        }
    }

    /// The key batches to delete, unfinished copies first, and how many copies they remove.
    fn plan(&mut self, copies: &[(String, Done)], unfinished: Vec<(String, Progress)>) {
        let grace = GRACE.as_millis() as u64;
        let complete: BTreeSet<&str> = copies.iter().map(|(digest, _)| digest.as_str()).collect();
        for (digest, progress) in unfinished {
            if progress.updated_ms.saturating_add(grace) >= self.now_ms {
                continue;
            }
            let mut keys = vec![progress_key(&digest)];
            if !complete.contains(digest.as_str()) {
                keys.extend((0..chunk_count(progress.total)).map(|i| chunk_key(&digest, i)));
                self.deleted += 1;
            }
            self.deletes.push_back(keys);
        }
        for (digest, copy) in copies {
            if self.kept.contains(digest) || copy.completed_ms.saturating_add(grace) >= self.now_ms
            {
                continue;
            }
            let mut keys: Vec<Vec<u8>> = (0..chunk_count(copy.total))
                .map(|i| chunk_key(digest, i))
                .collect();
            keys.push(done_key(digest));
            self.deletes.push_back(keys);
            self.deleted += 1;
        }
    }

    fn delete_next(&mut self) -> Next<usize> {
        match self.deletes.pop_front() {
            Some(keys) => Next::Call(delete_call(spaced(keys))),
            None => Next::Done(self.deleted),
        }
    }
}

impl Operation for SweepCopies {
    type Output = usize;
    type Error = ArtifactError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginCopies)
    }

    fn first(&mut self) -> Result<Next<usize>, ArtifactError> {
        match &self.phase {
            SweepPhase::ScanningDone(scan) => Ok(Next::Call(scan.ask())),
            _ => Err(storage("the sweep started twice")),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, ArtifactError> {
        match &mut self.phase {
            SweepPhase::ScanningDone(pages) => {
                let Some(rows) = pages.take(result)? else {
                    return Ok(Next::Call(pages.ask()));
                };
                let copies = digest_rows(rows, DONE_PREFIX, Done::decode);
                let pages = scan(PROGRESS_PREFIX);
                let call = pages.ask();
                self.phase = SweepPhase::ScanningProgress(pages, copies);
                Ok(Next::Call(call))
            }
            SweepPhase::ScanningProgress(pages, copies) => {
                let Some(rows) = pages.take(result)? else {
                    return Ok(Next::Call(pages.ask()));
                };
                let copies = std::mem::take(copies);
                let unfinished = digest_rows(rows, PROGRESS_PREFIX, Progress::decode);
                self.phase = SweepPhase::Deleting;
                self.plan(&copies, unfinished);
                Ok(self.delete_next())
            }
            SweepPhase::Deleting => {
                done(result)?;
                Ok(self.delete_next())
            }
        }
    }
}

/// This node's complete copy within the memory budget, or `None` without one.
#[derive(Debug, PartialEq)]
pub struct ReadLocal {
    digest: String,
    memory: Option<HeldMemory>,
}

/// Memory reserved for what an operation assembles; two reservations count as equal.
struct HeldMemory(MemoryPermit);

impl std::fmt::Debug for HeldMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HeldMemory({} MiB)", self.0.num_permits())
    }
}

impl PartialEq for HeldMemory {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

fn held_memory(total: u64) -> Result<HeldMemory, ArtifactError> {
    reserve(total as usize)
        .map(HeldMemory)
        .ok_or(ArtifactError::Busy)
}

impl ReadLocal {
    pub fn new(digest: &str) -> Self {
        Self {
            digest: digest.to_string(),
            memory: None,
        }
    }
}

impl Operation for ReadLocal {
    type Output = Option<Assembled>;
    type Error = ArtifactError;

    fn first(&mut self) -> Result<Next<Self::Output>, ArtifactError> {
        Ok(Next::Call(IoCall::child(CopyTotal::new(&self.digest))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ArtifactError> {
        if let Some(HeldMemory(memory)) = self.memory.take() {
            let bytes = result.child::<Vec<u8>, ArtifactError>()??;
            return Ok(Next::Done(Some(Assembled::new(bytes, memory))));
        }
        let Some(total) = result.child::<Option<u64>, ArtifactError>()?? else {
            return Ok(Next::Done(None));
        };
        self.memory = Some(held_memory(total)?);
        Ok(Next::Call(IoCall::child(ReadCopy::new(
            &self.digest,
            total,
        ))))
    }
}

/// Keeps one chunk of a copy another management node sends. Once every chunk is here and the
/// whole artifact checks out, the copy is done; `true` means it is done and synced now.
#[derive(Debug, PartialEq)]
pub struct KeepChunk {
    store: Option<StoreChunk>,
    digest: String,
    total: u64,
    now_ms: u64,
    /// Held while the whole copy is read and checked.
    memory: Option<HeldMemory>,
}

impl KeepChunk {
    pub fn new(config: &RealmConfigDocument, chunk: Chunk, now_ms: u64) -> Self {
        Self {
            digest: chunk.digest.clone(),
            total: chunk.total,
            now_ms,
            store: Some(StoreChunk::new(config, chunk, now_ms)),
            memory: None,
        }
    }
}

impl Operation for KeepChunk {
    type Output = bool;
    type Error = ArtifactError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::PluginCopies)
    }

    fn first(&mut self) -> Result<Next<bool>, ArtifactError> {
        let store = self.store.take().ok_or_else(|| storage("kept twice"))?;
        Ok(Next::Call(IoCall::child(store)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, ArtifactError> {
        if self.memory.is_some() {
            result.child::<(), ArtifactError>()??;
            return Ok(Next::Done(true));
        }
        match result.child::<ChunkStored, ArtifactError>()?? {
            ChunkStored::Held => Ok(Next::Done(false)),
            ChunkStored::Confirmed => Ok(Next::Done(true)),
            ChunkStored::AllHere => {
                self.memory = Some(held_memory(self.total)?);
                let complete = CompleteCopy::new(self.digest.clone(), self.total, self.now_ms);
                Ok(Next::Call(IoCall::child(complete)))
            }
        }
    }
}

#[cfg(test)]
#[path = "copy_rows_tests.rs"]
pub(crate) mod tests;
