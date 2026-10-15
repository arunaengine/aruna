//! Sends the parts of compressed uploads to the provider as frames and ends them with the seek
//! table. A part below 5 MiB once framed is padded unless it is the last one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::backend::rebuild_backend_path;
use super::frames::{READ_MEMORY, read_range};
use super::io::compose_chunk;
use super::pithos::Share;
use crate::codec::{
    FRAME_SIZE, FrameEncoder, FrameWriter, Guard, PART_MIN, compressor_memory, padding, padding_len,
};
use crate::s3::NativeMultipart;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::storage::blob::{Backend, BackendLocation, ResolvedBackend};
use aruna_core::structs::storage::format::{
    Compression, FrameLayout, PartFrame, PartLayout, StoredFormat, StoredLayout,
};
use aruna_core::structs::storage::multipart::{BackendUpload, MultipartPart};
use bytes::Bytes;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt, stream};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::time::SystemTime;
use tokio::time::timeout;
use ulid::Ulid;

/// Largest part framed in memory to learn its stored size; a larger one waits in a blob.
const BUFFER_MAX: u64 = 64 << 20;
/// The provider part that takes the seek table when the last listed part was streamed.
const TABLE_PART: u16 = 10_000;
/// Largest provider part. Tests use a small one, so a plan that does not fit stays cheap.
#[cfg(not(test))]
const PART_MAX: u64 = aruna_core::structs::storage::multipart::MAX_PART_SIZE;
#[cfg(test)]
const PART_MAX: u64 = 10 << 20;

/// How a compressed completion ended: the provider object with the reservation that must
/// cover its final hash read, or a copy composed instead.
pub(super) enum Finished {
    Framed(FrameLayout, Share),
    Composed(Box<BackendLocation>),
}

/// One staged part of a run: its frames from the first framing and its padding.
struct Staged<'a> {
    part: &'a MultipartPart,
    frames: Vec<PartFrame>,
    padding: u32,
}

/// Staged parts listed one after another. They are sent again as provider parts of at most
/// 5 GiB, cut at frame boundaries and numbered from the run's first part number.
struct Run<'a> {
    parts: Vec<Staged<'a>>,
    first: u16,
    /// Stored length of each frame, padding and table, in order.
    units: Vec<u64>,
    /// Units in each provider part.
    counts: Vec<usize>,
    /// The seek table, when the run holds the last listed part.
    table: Option<Bytes>,
}

impl<'a> Run<'a> {
    fn new(staged: Staged<'a>) -> Self {
        Self {
            first: staged.part.part_number,
            parts: vec![staged],
            units: Vec::new(),
            counts: Vec::new(),
            table: None,
        }
    }
}

/// A completion step in part order: a streamed part, or a run of staged parts.
enum Step<'a> {
    Direct(u16, String),
    Run(Run<'a>),
}

/// Sends stored bytes to the provider parts of a run in turn, each for its number of units.
struct Router {
    pieces: VecDeque<(usize, mpsc::Sender<Result<Bytes, StreamError>>)>,
    /// Set once a provider part stopped taking bytes.
    stopped: bool,
}

impl Router {
    /// Returns `false` once the provider part being written stopped taking bytes.
    async fn send(&mut self, bytes: Bytes) -> bool {
        let Some((left, sink)) = self.pieces.front_mut() else {
            self.stopped = true;
            return false;
        };
        if sink.send(Ok(bytes)).await.is_err() {
            self.stopped = true;
            return false;
        }
        *left -= 1;
        if *left == 0 {
            self.pieces.pop_front();
        }
        true
    }
}

fn stopped() -> BlobError {
    BlobError::WriteError("the part upload stopped".to_string())
}

/// Cuts each run into provider parts. A run may use the part numbers before the next streamed
/// part, listed or not, or up to 10000. Returns `false` when a run needs more numbers.
fn plan_runs(steps: &mut [Step<'_>], table: &Bytes, streamed: &BTreeSet<u16>) -> bool {
    let ends = steps.len();
    for (index, step) in steps.iter_mut().enumerate() {
        let Step::Run(run) = step else {
            continue;
        };
        for staged in &run.parts {
            run.units
                .extend(staged.frames.iter().map(|frame| u64::from(frame.stored)));
            if staged.padding > 0 {
                run.units.push(u64::from(staged.padding));
            }
        }
        if index + 1 == ends {
            run.units.push(table.len() as u64);
            run.table = Some(table.clone());
        }
        run.counts = cut_pieces(&run.units, PART_MAX, PART_MIN);
        let free = run_limit(run.first, streamed) - run.first;
        if run.counts.len() > usize::from(free) + 1 {
            return false;
        }
    }
    true
}

/// The last part number a run from `first` may write: the one before the next streamed part.
fn run_limit(first: u16, streamed: &BTreeSet<u16>) -> u16 {
    streamed
        .range(first + 1..)
        .next()
        .map_or(TABLE_PART, |next| next - 1)
}

/// Cuts units into pieces of at most `max` bytes, filling each in turn. A short last piece takes
/// units from the piece before until it holds `min`. Returns the units in each piece.
fn cut_pieces(units: &[u64], max: u64, min: u64) -> Vec<usize> {
    let mut pieces: Vec<(usize, u64)> = Vec::new();
    for unit in units {
        match pieces.last_mut() {
            Some((count, bytes)) if *bytes + unit <= max => {
                *count += 1;
                *bytes += unit;
            }
            _ => pieces.push((1, *unit)),
        }
    }
    if let [.., before, last] = pieces.as_mut_slice() {
        while last.1 < min && before.0 > 1 {
            let unit = units[units.len() - last.0 - 1];
            if last.1 + unit > max {
                break;
            }
            (before.0, before.1) = (before.0 - 1, before.1 - unit);
            (last.0, last.1) = (last.0 + 1, last.1 + unit);
        }
    }
    pieces.into_iter().map(|(count, _)| count).collect()
}

/// The zstd level of a compressed provider upload; `None` for an upload of raw parts.
pub(super) fn part_level(upload: &BackendUpload) -> Option<u8> {
    match &upload.location.format.layout {
        StoredLayout::FramedPart(layout) => Some(layout.level),
        _ => None,
    }
}

/// Whether a compressed part streams into the provider now. A part below 5 MiB and part 10000
/// can only be last, so they wait in a blob for the seek table, as do parts too large to buffer.
pub(super) fn streams_now(part_number: u16, size: Option<u64>) -> bool {
    part_number < TABLE_PART && size.is_some_and(|size| (PART_MIN..=BUFFER_MAX).contains(&size))
}

/// Memory one seek table entry takes while completing: as a staged frame, a table entry, its
/// table bytes and a cut unit, with room for vector growth.
const ENTRY_MEMORY: u64 = 160;

/// Working set of one compressed completion, held through a fallback: entries and the seek
/// table, framing and composer buffers, both compressors, a frame reader and a writer chunk.
fn completion_charge(sizes: &[u64], level: u8, compression: Compression, backend: &Backend) -> u64 {
    let entries: u64 = sizes.iter().map(|size| size.div_ceil(FRAME_SIZE) + 2).sum();
    let current = match compression {
        Compression::Off => 0,
        Compression::Zstd { level } => compressor_memory(level),
    };
    let total = sizes.iter().sum();
    let chunk = compose_chunk(backend, total, true).map_or(0, |chunk| chunk as u64);
    entries * ENTRY_MEMORY
        + compressor_memory(level)
        + current
        + 16 * FRAME_SIZE
        + READ_MEMORY
        + chunk
}

/// Memory one framed part may hold: its frames or padding, a received chunk of up to the part
/// size, the frame being encoded with its buffers, and the compressor workspace.
fn framed_charge(size: u64, level: u8) -> u64 {
    let stored = (size + size / 1024 + FRAME_SIZE).max(PART_MIN);
    stored + size + 4 * FRAME_SIZE + compressor_memory(level)
}

impl BlobHandler {
    /// Frames one part in memory, pads it to the part minimum and sends it as one provider part.
    pub(super) async fn upload_framed(
        &self,
        upload: &BackendUpload,
        part_number: u16,
        size: u64,
        level: u8,
        mut body: BackendStream<Result<Bytes, StreamError>>,
    ) -> Result<(String, PartLayout), BlobError> {
        // Held until the part is sent; each compression task keeps a clone of its own.
        let share = self.reserve_pithos(framed_charge(size, level)).await?;
        let mut encoder = FrameEncoder::new(level).holding(share.clone());
        let mut pieces = Vec::new();
        let mut received = 0u64;
        while let Some(chunk) = body.0.next().await {
            let bytes = chunk.map_err(|error| BlobError::StreamFailed(error.to_string()))?;
            received += bytes.len() as u64;
            if received > size {
                break;
            }
            pieces.extend(encoder.push(&bytes).await?);
        }
        if received != size {
            return Err(BlobError::StreamFailed(
                "body size did not match Content-Length".to_string(),
            ));
        }
        let (last, frames) = encoder.finish_part().await?;
        pieces.extend(last);
        let stored: u64 = pieces.iter().map(|piece| piece.len() as u64).sum();
        let pad = padding_len(stored);
        if pad > 0 {
            pieces.push(padding(pad));
        }
        let native = self.native_upload(&upload.location)?;
        let path = self.object_path(&upload.location)?;
        let body = BackendStream(Box::pin(stream::iter(pieces.into_iter().map(Ok))));
        let length = stored + u64::from(pad);
        let etag = native
            .upload_part(&path, &upload.upload_id, part_number, length, body)
            .await?;
        let layout = PartLayout {
            level,
            frames,
            padding: pad,
        };
        Ok((etag, layout))
    }

    /// Completes a compressed upload. Staged parts are framed for their entries, then framed
    /// again while they are sent; the seek table goes last.
    pub(super) async fn finish_framed(
        &self,
        upload: &BackendUpload,
        parts: &[MultipartPart],
        level: u8,
        streamed: &[u16],
        compression: Compression,
    ) -> Result<Finished, BlobError> {
        let Some(last) = parts.last() else {
            return Err(BlobError::WriteError(
                "the upload lists no part".to_string(),
            ));
        };
        // Held until the end, also through a fallback; compression tasks keep their own clone.
        let backend = self
            .registry
            .config_for(&upload.location.backend)?
            .backend_type;
        let sizes: Vec<u64> = parts.iter().map(|part| part.location.blob_size).collect();
        let charge = completion_charge(&sizes, level, compression, &backend);
        let share = self.reserve_pithos(charge).await?;
        let mut writer = FrameWriter::new(level);
        let mut steps = Vec::with_capacity(parts.len());
        let mut occupied: BTreeSet<u16> = streamed.iter().copied().collect();
        for part in parts {
            let number = part.part_number;
            match (&part.location.format.layout, part.location.partial) {
                (StoredLayout::FramedPart(layout), true) => {
                    let etag = part.backend_etag.clone().ok_or_else(|| {
                        BlobError::WriteError(format!("part {number} has no provider ETag"))
                    })?;
                    for frame in &layout.frames {
                        writer.entry(frame.clone())?;
                    }
                    if layout.padding > 0 {
                        writer.pad(layout.padding)?;
                    }
                    occupied.insert(number);
                    steps.push(Step::Direct(number, etag));
                }
                (StoredLayout::Raw, false) => {
                    let frames = self.frame_blob(&part.location, level, &share, None);
                    let frames = frames.await?;
                    let stored = frames.iter().map(|frame| u64::from(frame.stored)).sum();
                    let padding = match std::ptr::eq(part, last) {
                        true => 0,
                        false => padding_len(stored),
                    };
                    for frame in &frames {
                        writer.entry(frame.clone())?;
                    }
                    if padding > 0 {
                        writer.pad(padding)?;
                    }
                    let staged = Staged {
                        part,
                        frames,
                        padding,
                    };
                    match steps.last_mut() {
                        Some(Step::Run(run)) => run.parts.push(staged),
                        _ => steps.push(Step::Run(Run::new(staged))),
                    }
                }
                _ => {
                    return Err(BlobError::WriteError(format!(
                        "part {number} holds no frames of this upload"
                    )));
                }
            }
        }
        let (table, layout) = writer.finish()?;
        let table = Bytes::from(table);
        if !plan_runs(&mut steps, &table, &occupied) {
            let fallback = self.compose_fallback(upload, parts, level, compression, &share);
            let composed = Box::pin(fallback).await?;
            return Ok(Finished::Composed(Box::new(composed)));
        }
        let native = self.native_upload(&upload.location)?;
        let path = self.object_path(&upload.location)?;
        let sent = async {
            let mut etags = Vec::with_capacity(parts.len() + 1);
            for step in &steps {
                match step {
                    Step::Direct(number, etag) => etags.push((*number, etag.clone())),
                    Step::Run(run) => {
                        let id = &upload.upload_id;
                        let sent = self.send_run(&native, &path, id, level, &share, run);
                        etags.extend(sent.await?);
                    }
                }
            }
            if last.location.partial {
                let body = BackendStream(Box::pin(stream::iter([Ok(table.clone())])));
                let length = table.len() as u64;
                let etag = native
                    .upload_part(&path, &upload.upload_id, TABLE_PART, length, body)
                    .await?;
                etags.push((TABLE_PART, etag));
            }
            native.complete(&path, &upload.upload_id, &etags).await
        }
        .await;
        // A completion whose answer was lost leaves the object; its seek table names every frame.
        if let Err(error) = sent
            && !self
                .holds_table(&upload.location, &path, &layout, &table)
                .await
        {
            return Err(error);
        }
        Ok(Finished::Framed(layout, share))
    }

    /// Completes the listed streamed parts as an object of their own at the opening `level`, then
    /// composes it with the staged parts into a new copy with the bucket's current `compression`.
    async fn compose_fallback(
        &self,
        upload: &BackendUpload,
        parts: &[MultipartPart],
        level: u8,
        compression: Compression,
        share: &Share,
    ) -> Result<BackendLocation, BlobError> {
        let mut writer = FrameWriter::new(level);
        let mut etags = Vec::new();
        let mut sources = Vec::with_capacity(parts.len());
        let mut assembled = upload.location.clone();
        assembled.blob_size = 0;
        for part in parts {
            let size = part.location.blob_size;
            let StoredLayout::FramedPart(layout) = &part.location.format.layout else {
                sources.push((Some(part.location.clone()), 0..size));
                continue;
            };
            let etag = part.backend_etag.clone().ok_or_else(|| {
                let number = part.part_number;
                BlobError::WriteError(format!("part {number} has no provider ETag"))
            })?;
            for frame in &layout.frames {
                writer.entry(frame.clone())?;
            }
            if layout.padding > 0 {
                writer.pad(layout.padding)?;
            }
            etags.push((part.part_number, etag));
            sources.push((None, assembled.blob_size..assembled.blob_size + size));
            assembled.blob_size += size;
        }
        // Only listed streamed parts and the table part are assembled, so no part is replaced.
        if !etags.is_empty() {
            let (table, layout) = writer.finish()?;
            let table = Bytes::from(table);
            let native = self.native_upload(&upload.location)?;
            let path = self.object_path(&upload.location)?;
            let sent = async {
                let body = BackendStream(Box::pin(stream::iter([Ok(table.clone())])));
                let length = table.len() as u64;
                let etag = native
                    .upload_part(&path, &upload.upload_id, TABLE_PART, length, body)
                    .await?;
                etags.push((TABLE_PART, etag));
                native.complete(&path, &upload.upload_id, &etags).await
            }
            .await;
            if let Err(error) = sent
                && !self
                    .holds_table(&upload.location, &path, &layout, &table)
                    .await
            {
                return Err(error);
            }
            assembled.format.layout = StoredLayout::Frames(Box::new(layout));
        }
        let sources = sources
            .into_iter()
            .map(|(part, range)| (part.unwrap_or_else(|| assembled.clone()), range))
            .collect();
        let ulid = Ulid::generate();
        let backend_path = rebuild_backend_path(&upload.location.backend_path, ulid)
            .map_err(BlobError::ConversionError)?;
        let template = BackendLocation {
            storage_bucket: String::new(),
            backend_path,
            ulid,
            format: StoredFormat::default(),
            created_at: SystemTime::now(),
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
            ..upload.location.clone()
        };
        let backend = upload.location.backend.clone();
        let resolved = ResolvedBackend::new(backend, upload.location.storage_class.clone())
            .with_compression(compression);
        let guard: Guard = share.clone();
        match Box::pin(self.compose_into(template, resolved, sources, Some(guard))).await {
            BlobEvent::WriteFinished { location } => Ok(location),
            BlobEvent::Error(error) => Err(error),
            other => Err(BlobError::WriteError(format!(
                "unexpected compose result: {other:?}"
            ))),
        }
    }

    /// Frames the staged parts of a run again and sends them as its provider parts, checking
    /// that they framed as before. Returns the number and ETag of each provider part.
    async fn send_run(
        &self,
        native: &NativeMultipart,
        path: &str,
        upload_id: &str,
        level: u8,
        share: &Share,
        run: &Run<'_>,
    ) -> Result<Vec<(u16, String)>, BlobError> {
        let mut router = Router {
            pieces: VecDeque::new(),
            stopped: false,
        };
        let mut receivers = Vec::with_capacity(run.counts.len());
        for count in &run.counts {
            let (sink, receiver) = mpsc::channel(4);
            router.pieces.push_back((*count, sink));
            receivers.push(receiver);
        }
        let produce = async {
            let produced = async {
                for staged in &run.parts {
                    let location = &staged.part.location;
                    let sink = Some(&mut router);
                    let frames = self.frame_blob(location, level, share, sink).await?;
                    if frames != staged.frames {
                        return Err(BlobError::IntegrityCheckFailed(
                            "a staged part framed differently".to_string(),
                        ));
                    }
                    if staged.padding > 0 && !router.send(padding(staged.padding)).await {
                        return Err(stopped());
                    }
                }
                match &run.table {
                    Some(table) if !router.send(table.clone()).await => Err(stopped()),
                    _ => Ok(()),
                }
            }
            .await;
            // Ends every body still open, so no provider part waits for bytes that never come.
            router.pieces.clear();
            produced
        };
        let consume = async {
            let mut etags = Vec::with_capacity(receivers.len());
            let mut units = run.units.iter();
            for ((index, receiver), count) in receivers.into_iter().enumerate().zip(&run.counts) {
                let number = run.first + index as u16;
                let length = units.by_ref().take(*count).sum();
                let body = BackendStream(Box::pin(receiver));
                let etag = native
                    .upload_part(path, upload_id, number, length, body)
                    .await?;
                etags.push((number, etag));
            }
            Ok::<_, BlobError>(etags)
        };
        let (produced, sent) = tokio::join!(produce, consume);
        match (produced, sent) {
            (Ok(()), sent) => sent,
            (Err(error), _) if !router.stopped => Err(error),
            (_, Err(error)) => Err(error),
            (Err(error), Ok(_)) => Err(error),
        }
    }

    /// Frames the raw bytes of a staged part and sends each stored frame to `sink` when given.
    async fn frame_blob(
        &self,
        location: &BackendLocation,
        level: u8,
        share: &Share,
        mut sink: Option<&mut Router>,
    ) -> Result<Vec<PartFrame>, BlobError> {
        let read = |error: opendal::Error| BlobError::ReadError(error.to_string());
        let late = || BlobError::ReadError("timed out reading a staged part".to_string());
        let operator = self.operator_from_location(location)?;
        let path = self.object_path(location)?;
        let reader = timeout(self.io_timeout(), operator.reader(&path))
            .await
            .map_err(|_| late())?
            .map_err(read)?;
        let mut stream = timeout(self.io_timeout(), reader.into_bytes_stream(..))
            .await
            .map_err(|_| late())?
            .map_err(read)?;
        let mut encoder = FrameEncoder::new(level).holding(share.clone());
        while let Some(chunk) = timeout(self.transfer_idle_timeout(), stream.next())
            .await
            .map_err(|_| late())?
        {
            let chunk = chunk.map_err(|error| BlobError::ReadError(error.to_string()))?;
            for piece in encoder.push(&chunk).await? {
                if let Some(sink) = sink.as_mut()
                    && !sink.send(piece).await
                {
                    return Err(stopped());
                }
            }
        }
        let (last, frames) = encoder.finish_part().await?;
        if let (Some(sink), Some(last)) = (sink.as_mut(), last)
            && !sink.send(last).await
        {
            return Err(stopped());
        }
        let original: u64 = frames.iter().map(|frame| u64::from(frame.original)).sum();
        if original != location.blob_size {
            return Err(BlobError::IntegrityCheckFailed(
                "a staged part has the wrong size".to_string(),
            ));
        }
        Ok(frames)
    }

    /// Whether the object at `path` is this completion: its size and seek table match.
    async fn holds_table(
        &self,
        location: &BackendLocation,
        path: &str,
        layout: &FrameLayout,
        table: &Bytes,
    ) -> bool {
        let Ok(operator) = self.operator_from_location(location) else {
            return false;
        };
        match timeout(self.io_timeout(), operator.stat(path)).await {
            Ok(Ok(metadata)) if metadata.content_length() == layout.stored_size => {}
            _ => return false,
        }
        let range = layout.stored_size - table.len() as u64..layout.stored_size;
        let idle = self.transfer_idle_timeout();
        read_range(&operator, path, range, idle)
            .await
            .is_ok_and(|tail| tail == *table)
    }
}

#[cfg(test)]
mod tests {
    use super::{completion_charge, cut_pieces, run_limit};
    use crate::blob::pithos::WORKING_SET;
    use crate::codec::compressor_memory;
    use aruna_core::structs::storage::blob::Backend;
    use aruna_core::structs::storage::format::Compression;
    use std::collections::BTreeSet;

    #[test]
    fn cuts_fill_pieces() {
        // Parts of incompressible frames: each grows a little, so a run needs one more piece.
        let units = [40, 40, 40, 40, 40];
        assert_eq!(cut_pieces(&units, 100, 10), vec![2, 2, 1]);
        assert_eq!(cut_pieces(&units, 200, 10), vec![5]);
        // A short last piece takes units from the one before until it holds the minimum.
        assert_eq!(cut_pieces(&[30, 30, 30, 5], 90, 20), vec![2, 2]);
        assert_eq!(cut_pieces(&[50, 20, 20, 9, 2], 100, 25), vec![2, 3]);
        // A last piece may stay short when the piece before has a single unit.
        assert_eq!(cut_pieces(&[95, 6], 100, 10), vec![1, 1]);
        assert_eq!(cut_pieces(&[], 100, 10), Vec::<usize>::new());
    }

    #[test]
    fn charge_fits_budget() {
        // The largest upload, 1,024 parts of 5 GiB at the highest level, fits the working set.
        let largest = vec![5 << 30; 1_024];
        let zstd = Compression::Zstd { level: 22 };
        let charge = completion_charge(&largest, 22, zstd, &Backend::S3);
        assert!(charge <= WORKING_SET, "{charge}");
        // Both compressors count: the opening level and the bucket's current one.
        let off = completion_charge(&largest, 22, Compression::Off, &Backend::S3);
        assert_eq!(charge - off, compressor_memory(22));
        let small = completion_charge(&[6 << 20], 3, zstd, &Backend::S3);
        assert!(small > compressor_memory(3) + compressor_memory(22));
    }

    #[test]
    fn streamed_ends_runs() {
        // Streamed parts keep their numbers, listed or not; a run ends before the next one.
        let streamed = BTreeSet::from([2, 7]);
        assert_eq!(run_limit(1, &streamed), 1);
        assert_eq!(run_limit(3, &streamed), 6);
        assert_eq!(run_limit(8, &streamed), 10_000);
        assert_eq!(run_limit(10_000, &BTreeSet::new()), 10_000);
    }
}
