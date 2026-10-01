//! Stores blobs as 1 MiB frames, each zstd compressed or raw, with a BLAKE3 digest per frame.
//! Frames form groups; each group ends with its frame entries, and the object ends with a tail
//! that lists every group. The location record keeps the tail hash.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::BlobError;
use aruna_core::structs::storage::format::FrameLayout;
use bytes::{Bytes, BytesMut};
use std::ops::Range;
use zstd::zstd_safe::DParameter;

pub(crate) const FRAME_SIZE: u64 = 1 << 20;
/// Frames per group; one group's entries are read and checked together.
#[cfg(not(test))]
const GROUP_FRAMES: u64 = 1024;
/// Tests use small groups so several groups fit in a few MiB.
#[cfg(test)]
const GROUP_FRAMES: u64 = 2;
const ENTRY_LEN: u64 = 37;
const SUMMARY_LEN: u64 = 40;
const TAG_RAW: u8 = 0;
const TAG_ZSTD: u8 = 1;
const MIN_SAVING: usize = 1024;
/// A zstd window of 1 MiB covers any frame; larger windows are refused.
const WINDOW_LOG: u32 = 20;

fn frame_count(size: u64) -> u64 {
    size.div_ceil(FRAME_SIZE)
}

fn group_count(size: u64) -> u64 {
    frame_count(size).div_ceil(GROUP_FRAMES)
}

/// Original length of one frame of an object of `size` bytes.
pub(crate) fn frame_len(size: u64, frame: u64) -> u64 {
    FRAME_SIZE.min(size - frame * FRAME_SIZE)
}

fn group_frames(size: u64, group: u64) -> u64 {
    GROUP_FRAMES.min(frame_count(size) - group * GROUP_FRAMES)
}

fn tail_len(size: u64) -> u64 {
    group_count(size) * SUMMARY_LEN
}

fn integrity(message: &str) -> BlobError {
    BlobError::IntegrityCheckFailed(message.to_string())
}

/// One stored frame: its codec tag, stored length and the digest of its stored bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FrameEntry {
    tag: u8,
    len: u32,
    digest: [u8; 32],
}

impl FrameEntry {
    fn encode(&self, out: &mut Vec<u8>) {
        out.push(self.tag);
        out.extend_from_slice(&self.len.to_le_bytes());
        out.extend_from_slice(&self.digest);
    }

    fn decode(bytes: &[u8]) -> Self {
        let mut len = [0; 4];
        len.copy_from_slice(&bytes[1..5]);
        let mut digest = [0; 32];
        digest.copy_from_slice(&bytes[5..37]);
        Self {
            tag: bytes[0],
            len: u32::from_le_bytes(len),
            digest,
        }
    }

    pub(crate) fn stored_len(&self) -> u64 {
        u64::from(self.len)
    }
}

/// Compresses one frame and keeps the result only when it saves at least
/// max(1024 bytes, 5 percent); otherwise the frame stays raw.
pub(crate) fn encode_frame(raw: Bytes, level: u8) -> Result<(u8, Bytes), BlobError> {
    let packed = zstd::bulk::compress(&raw, i32::from(level))
        .map_err(|error| BlobError::WriteError(format!("zstd compression failed: {error}")))?;
    let saved = raw.len().saturating_sub(packed.len());
    if saved >= MIN_SAVING && saved * 20 >= raw.len() {
        Ok((TAG_ZSTD, Bytes::from(packed)))
    } else {
        Ok((TAG_RAW, raw))
    }
}

/// Collects frame entries while frames are written and builds the trailing index.
pub(crate) struct FrameWriter {
    level: u8,
    entries: Vec<u8>,
    group_len: u64,
    group_frames: u64,
    tail: Vec<u8>,
    stored: u64,
}

impl FrameWriter {
    pub(crate) fn new(level: u8) -> Self {
        Self {
            level,
            entries: Vec::new(),
            group_len: 0,
            group_frames: 0,
            tail: Vec::new(),
            stored: 0,
        }
    }

    /// Records one written frame. Returns the entries to write next when its group is full.
    pub(crate) fn push(&mut self, tag: u8, stored: &[u8]) -> Result<Option<Vec<u8>>, BlobError> {
        let len = u32::try_from(stored.len())
            .map_err(|_| BlobError::WriteError("frame is too large".to_string()))?;
        FrameEntry {
            tag,
            len,
            digest: *blake3::hash(stored).as_bytes(),
        }
        .encode(&mut self.entries);
        self.group_len += u64::from(len);
        self.stored += u64::from(len);
        self.group_frames += 1;
        if self.group_frames == GROUP_FRAMES {
            return Ok(Some(self.close_group()));
        }
        Ok(None)
    }

    fn close_group(&mut self) -> Vec<u8> {
        let entries = std::mem::take(&mut self.entries);
        let len = self.group_len + entries.len() as u64;
        self.tail.extend_from_slice(&len.to_le_bytes());
        self.tail
            .extend_from_slice(blake3::hash(&entries).as_bytes());
        self.stored += entries.len() as u64;
        self.group_len = 0;
        self.group_frames = 0;
        entries
    }

    /// Returns the bytes that end the object and the layout record that names them.
    pub(crate) fn finish(mut self) -> (Vec<u8>, FrameLayout) {
        let mut rest = Vec::new();
        if self.group_frames > 0 {
            rest = self.close_group();
        }
        let tail = std::mem::take(&mut self.tail);
        let layout = FrameLayout {
            level: self.level,
            stored_size: self.stored + tail.len() as u64,
            index_hash: *blake3::hash(&tail).as_bytes(),
        };
        rest.extend_from_slice(&tail);
        (rest, layout)
    }
}

/// Cuts a byte stream into frames and returns the stored bytes to write, in order.
pub(crate) struct FrameEncoder {
    level: u8,
    pending: BytesMut,
    writer: FrameWriter,
}

impl FrameEncoder {
    pub(crate) fn new(level: u8) -> Self {
        Self {
            level,
            pending: BytesMut::new(),
            writer: FrameWriter::new(level),
        }
    }

    pub(crate) async fn push(&mut self, bytes: &[u8]) -> Result<Vec<Bytes>, BlobError> {
        self.pending.extend_from_slice(bytes);
        let mut out = Vec::new();
        while self.pending.len() as u64 >= FRAME_SIZE {
            let frame = self.pending.split_to(FRAME_SIZE as usize).freeze();
            self.encode(frame, &mut out).await?;
        }
        Ok(out)
    }

    async fn encode(&mut self, frame: Bytes, out: &mut Vec<Bytes>) -> Result<(), BlobError> {
        let level = self.level;
        let (tag, stored) = tokio::task::spawn_blocking(move || encode_frame(frame, level))
            .await
            .map_err(|error| BlobError::WriteError(error.to_string()))??;
        let entries = self.writer.push(tag, &stored)?;
        out.push(stored);
        if let Some(entries) = entries {
            out.push(Bytes::from(entries));
        }
        Ok(())
    }

    /// Encodes the last short frame and returns the closing bytes with the layout record.
    pub(crate) async fn finish(mut self) -> Result<(Vec<Bytes>, FrameLayout), BlobError> {
        let mut out = Vec::new();
        if !self.pending.is_empty() {
            let frame = std::mem::take(&mut self.pending).freeze();
            self.encode(frame, &mut out).await?;
        }
        let (rest, layout) = self.writer.finish();
        out.push(Bytes::from(rest));
        Ok((out, layout))
    }
}

/// Byte range of the tail inside the stored object, checked against the record first.
pub(crate) fn tail_range(size: u64, layout: &FrameLayout) -> Result<Range<u64>, BlobError> {
    let len = tail_len(size);
    // Every frame takes at least one byte plus its entry, so smaller objects are invalid.
    let minimum = frame_count(size)
        .checked_mul(ENTRY_LEN + 1)
        .and_then(|frames| frames.checked_add(len))
        .ok_or_else(|| integrity("frame index size overflows"))?;
    if layout.stored_size < minimum {
        return Err(integrity("stored size is too small for the frame index"));
    }
    Ok(layout.stored_size - len..layout.stored_size)
}

/// The checked tail: where each group starts and how long it is.
#[derive(Debug)]
pub(crate) struct FrameIndex {
    size: u64,
    starts: Vec<u64>,
    lens: Vec<u64>,
    digests: Vec<[u8; 32]>,
}

impl FrameIndex {
    /// Verifies the tail hash before using any value, then checks every group bound.
    pub(crate) fn parse(size: u64, layout: &FrameLayout, tail: &[u8]) -> Result<Self, BlobError> {
        if tail.len() as u64 != tail_len(size) {
            return Err(integrity("frame index tail has the wrong length"));
        }
        if blake3::hash(tail).as_bytes() != &layout.index_hash {
            return Err(integrity("frame index hash mismatch"));
        }
        let groups = group_count(size) as usize;
        let mut index = Self {
            size,
            starts: Vec::with_capacity(groups),
            lens: Vec::with_capacity(groups),
            digests: Vec::with_capacity(groups),
        };
        let mut start = 0u64;
        for (group, summary) in tail.chunks_exact(SUMMARY_LEN as usize).enumerate() {
            let mut len = [0; 8];
            len.copy_from_slice(&summary[..8]);
            let len = u64::from_le_bytes(len);
            let mut digest = [0; 32];
            digest.copy_from_slice(&summary[8..]);
            let frames = group_frames(size, group as u64);
            let original = (group as u64 * GROUP_FRAMES..group as u64 * GROUP_FRAMES + frames)
                .map(|frame| frame_len(size, frame))
                .sum::<u64>();
            if len < frames * (ENTRY_LEN + 1) || len > original + frames * ENTRY_LEN {
                return Err(integrity("frame group has an invalid length"));
            }
            index.starts.push(start);
            index.lens.push(len);
            index.digests.push(digest);
            start += len;
        }
        if start + tail.len() as u64 != layout.stored_size {
            return Err(integrity("frame groups do not fill the stored object"));
        }
        Ok(index)
    }

    /// Byte range of one group's entries.
    pub(crate) fn entries_range(&self, group: u64) -> Range<u64> {
        let end = self.starts[group as usize] + self.lens[group as usize];
        end - group_frames(self.size, group) * ENTRY_LEN..end
    }

    /// Verifies one group's entries against the tail, then checks each entry.
    pub(crate) fn entries(&self, group: u64, bytes: &[u8]) -> Result<Vec<FrameEntry>, BlobError> {
        let range = self.entries_range(group);
        if bytes.len() as u64 != range.end - range.start {
            return Err(integrity("frame entries have the wrong length"));
        }
        if blake3::hash(bytes).as_bytes() != &self.digests[group as usize] {
            return Err(integrity("frame entries hash mismatch"));
        }
        let first = group * GROUP_FRAMES;
        let mut total = 0u64;
        let mut entries = Vec::with_capacity(bytes.len() / ENTRY_LEN as usize);
        for (offset, bytes) in bytes.chunks_exact(ENTRY_LEN as usize).enumerate() {
            let entry = FrameEntry::decode(bytes);
            let expected = frame_len(self.size, first + offset as u64);
            let valid = match entry.tag {
                TAG_RAW => entry.stored_len() == expected,
                TAG_ZSTD => entry.len > 0 && entry.stored_len() < expected,
                _ => false,
            };
            if !valid {
                return Err(integrity("frame entry is invalid"));
            }
            total += entry.stored_len();
            entries.push(entry);
        }
        if total + (range.end - range.start) != self.lens[group as usize] {
            return Err(integrity("frame entries do not fill their group"));
        }
        Ok(entries)
    }

    /// Stored offset of the first frame of a group.
    pub(crate) fn group_start(&self, group: u64) -> u64 {
        self.starts[group as usize]
    }
}

/// Group of a frame and the frame's place inside it.
pub(crate) fn group_of(frame: u64) -> (u64, usize) {
    (frame / GROUP_FRAMES, (frame % GROUP_FRAMES) as usize)
}

/// Checks a stored frame against its entry, then decodes it into exactly `expected` bytes.
pub(crate) fn decode_frame(
    entry: &FrameEntry,
    expected: u64,
    stored: Bytes,
) -> Result<Bytes, BlobError> {
    if stored.len() as u64 != entry.stored_len() {
        return Err(integrity("stored frame has the wrong length"));
    }
    if blake3::hash(&stored).as_bytes() != &entry.digest {
        return Err(integrity("stored frame hash mismatch"));
    }
    if entry.tag == TAG_RAW {
        return Ok(stored);
    }
    match zstd::zstd_safe::get_frame_content_size(&stored) {
        Ok(Some(size)) if size == expected => {}
        _ => return Err(integrity("zstd frame declares the wrong size")),
    }
    let mut decoder =
        zstd::bulk::Decompressor::new().map_err(|error| BlobError::ReadError(error.to_string()))?;
    decoder
        .set_parameter(DParameter::WindowLogMax(WINDOW_LOG))
        .map_err(|error| BlobError::ReadError(error.to_string()))?;
    let mut out = Vec::with_capacity(expected as usize);
    decoder
        .decompress_to_buffer(&stored, &mut out)
        .map_err(|error| integrity(&format!("zstd frame does not decode: {error}")))?;
    if out.len() as u64 != expected {
        return Err(integrity("zstd frame decoded to the wrong size"));
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Seeded xorshift bytes, so random samples are the same on every run.
    fn random(len: usize, mut seed: u64) -> Vec<u8> {
        (0..len)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed as u8
            })
            .collect()
    }

    fn text(len: usize) -> Vec<u8> {
        b"aruna stores research data in frames. "
            .iter()
            .copied()
            .cycle()
            .take(len)
            .collect()
    }

    fn encode(data: &[u8], level: u8) -> (Vec<u8>, FrameLayout) {
        let mut writer = FrameWriter::new(level);
        let mut out = Vec::new();
        for chunk in data.chunks(FRAME_SIZE as usize) {
            let (tag, stored) = encode_frame(Bytes::copy_from_slice(chunk), level).unwrap();
            out.extend_from_slice(&stored);
            if let Some(entries) = writer.push(tag, &stored).unwrap() {
                out.extend(entries);
            }
        }
        let (rest, layout) = writer.finish();
        out.extend(rest);
        (out, layout)
    }

    fn decode(
        stored: &[u8],
        layout: &FrameLayout,
        size: u64,
        range: Range<u64>,
    ) -> Result<Vec<u8>, BlobError> {
        let tail = tail_range(size, layout)?;
        let index = FrameIndex::parse(size, layout, &stored[tail.start as usize..])?;
        let mut out = Vec::new();
        let mut position = range.start;
        while position < range.end {
            let frame = position / FRAME_SIZE;
            let (group, slot) = group_of(frame);
            let bounds = index.entries_range(group);
            let entries =
                index.entries(group, &stored[bounds.start as usize..bounds.end as usize])?;
            let skipped: u64 = entries[..slot].iter().map(FrameEntry::stored_len).sum();
            let start = (index.group_start(group) + skipped) as usize;
            let entry = &entries[slot];
            let bytes = &stored[start..start + entry.stored_len() as usize];
            let length = frame_len(size, frame);
            let decoded = decode_frame(entry, length, Bytes::copy_from_slice(bytes))?;
            let frame_start = frame * FRAME_SIZE;
            let to = (range.end - frame_start).min(length);
            out.extend_from_slice(&decoded[(position - frame_start) as usize..to as usize]);
            position = frame_start + to;
        }
        Ok(out)
    }

    fn samples() -> Vec<Vec<u8>> {
        let mut mixed = text(3 * FRAME_SIZE as usize / 2);
        mixed.extend(random(2 * FRAME_SIZE as usize, 7));
        vec![
            text(5 * FRAME_SIZE as usize + 17),
            random(3 * FRAME_SIZE as usize + 5, 3),
            mixed,
            b"tiny".to_vec(),
            Vec::new(),
        ]
    }

    #[test]
    fn round_trips_samples() {
        for data in samples() {
            let size = data.len() as u64;
            let (stored, layout) = encode(&data, 3);

            assert_eq!(layout.stored_size, stored.len() as u64);
            assert_eq!(decode(&stored, &layout, size, 0..size).unwrap(), data);
        }
        let compressible = text(4 * FRAME_SIZE as usize);
        let (stored, _) = encode(&compressible, 3);
        assert!(stored.len() < compressible.len() / 10);
    }

    #[test]
    fn ranges_cross_frames() {
        let data = samples().remove(2);
        let size = data.len() as u64;
        let (stored, layout) = encode(&data, 9);
        let ranges = [
            FRAME_SIZE - 10..FRAME_SIZE + 10,
            0..1,
            size - 1..size,
            FRAME_SIZE + 3..3 * FRAME_SIZE + 1,
            5..5,
        ];
        for range in ranges {
            let expected = &data[range.start as usize..range.end as usize];
            assert_eq!(decode(&stored, &layout, size, range).unwrap(), expected);
        }
    }

    #[test]
    fn small_savings_raw() {
        let (tag, _) = encode_frame(Bytes::from(vec![0u8; 900]), 3).unwrap();
        assert_eq!(tag, TAG_RAW);
        let (tag, _) = encode_frame(Bytes::from(random(FRAME_SIZE as usize, 5)), 3).unwrap();
        assert_eq!(tag, TAG_RAW);
        let (tag, _) = encode_frame(Bytes::from(vec![0u8; 4096]), 3).unwrap();
        assert_eq!(tag, TAG_ZSTD);
    }

    #[test]
    fn tampered_bytes_fail() {
        let data = samples().remove(0);
        let size = data.len() as u64;
        let (stored, layout) = encode(&data, 3);
        let integrity = |result: Result<Vec<u8>, BlobError>| {
            matches!(result, Err(BlobError::IntegrityCheckFailed(_)))
        };
        let mut frame = stored.clone();
        frame[10] ^= 1;
        assert!(integrity(decode(&frame, &layout, size, 0..size)));
        let mut tail = stored.clone();
        let last = tail.len() - 1;
        tail[last] ^= 1;
        assert!(integrity(decode(&tail, &layout, size, 0..size)));
        let index = tail_range(size, &layout).unwrap();
        let tail = FrameIndex::parse(size, &layout, &stored[index.start as usize..]).unwrap();
        let mut entries = stored.clone();
        entries[tail.entries_range(0).start as usize + 1] ^= 1;
        assert!(integrity(decode(&entries, &layout, size, 0..size)));
        let mut record = layout.clone();
        record.stored_size += 1;
        assert!(integrity(decode(&stored, &record, size, 0..size)));
    }

    #[test]
    fn bombs_stay_bounded() {
        let entry = |stored: &[u8]| FrameEntry {
            tag: TAG_ZSTD,
            len: stored.len() as u32,
            digest: *blake3::hash(stored).as_bytes(),
        };
        // A frame that claims more output than the frame may hold is refused unread.
        let large = zstd::bulk::compress(&vec![0u8; 64 * FRAME_SIZE as usize], 3).unwrap();
        let result = decode_frame(&entry(&large), FRAME_SIZE, Bytes::from(large.clone()));
        assert!(matches!(result, Err(BlobError::IntegrityCheckFailed(_))));
        // A frame without a declared size is refused too.
        let open = zstd::stream::encode_all(&vec![0u8; 1024][..], 3).unwrap();
        let result = decode_frame(&entry(&open), 1024, Bytes::from(open.clone()));
        assert!(matches!(result, Err(BlobError::IntegrityCheckFailed(_))));
    }

    #[tokio::test]
    async fn encoder_matches_frames() {
        let data = samples().remove(2);
        let mut encoder = FrameEncoder::new(3);
        let mut stored = Vec::new();
        for chunk in data.chunks(300_001) {
            for piece in encoder.push(chunk).await.unwrap() {
                stored.extend_from_slice(&piece);
            }
        }
        let (rest, layout) = encoder.finish().await.unwrap();
        for piece in rest {
            stored.extend_from_slice(&piece);
        }

        assert_eq!((stored.clone(), layout.clone()), encode(&data, 3));
        let size = data.len() as u64;
        assert_eq!(decode(&stored, &layout, size, 0..size).unwrap(), data);
    }
}
