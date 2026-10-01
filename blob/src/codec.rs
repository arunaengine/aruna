//! Stores blobs in the zstd seekable format: 1 MiB zstd frames, then a seek table in a skippable
//! frame that plain zstd decoders skip. The location record keeps the table's BLAKE3 hash.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::BlobError;
use aruna_core::structs::storage::format::FrameLayout;
use bytes::{Bytes, BytesMut};
use std::ops::Range;
use zstd::zstd_safe::{CParameter, DParameter};

pub(crate) const FRAME_SIZE: u64 = 1 << 20;
const SKIPPABLE_MAGIC: u32 = 0x184D_2A5E;
const SEEKABLE_MAGIC: u32 = 0x8F92_EAB1;
/// Stored size and original size of one frame.
const ENTRY_LEN: u64 = 8;
/// Skippable frame header (magic, size) and table footer (frame count, descriptor, magic).
const HEADER_LEN: u64 = 8;
const FOOTER_LEN: u64 = 9;
/// A zstd window of 1 MiB covers any frame; larger windows are refused.
const WINDOW_LOG: u32 = 20;

fn frame_count(size: u64) -> u64 {
    size.div_ceil(FRAME_SIZE)
}

/// Original length of one frame of an object of `size` bytes.
pub(crate) fn frame_len(size: u64, frame: u64) -> u64 {
    FRAME_SIZE.min(size - frame * FRAME_SIZE)
}

fn table_len(size: u64) -> u64 {
    HEADER_LEN + frame_count(size) * ENTRY_LEN + FOOTER_LEN
}

fn integrity(message: &str) -> BlobError {
    BlobError::IntegrityCheckFailed(message.to_string())
}

fn read_u32(bytes: &[u8], at: usize) -> u32 {
    let mut value = [0; 4];
    value.copy_from_slice(&bytes[at..at + 4]);
    u32::from_le_bytes(value)
}

/// Compresses one frame with a zstd checksum. Data that does not compress becomes raw blocks.
pub(crate) fn encode_frame(raw: Bytes, level: u8) -> Result<Bytes, BlobError> {
    let failed =
        |error: std::io::Error| BlobError::WriteError(format!("zstd compression failed: {error}"));
    let mut compressor = zstd::bulk::Compressor::new(i32::from(level)).map_err(failed)?;
    compressor
        .set_parameter(CParameter::ChecksumFlag(true))
        .map_err(failed)?;
    compressor.compress(&raw).map(Bytes::from).map_err(failed)
}

/// Collects the seek table while frames are written.
pub(crate) struct FrameWriter {
    level: u8,
    entries: Vec<u8>,
    frames: u32,
    stored: u64,
}

impl FrameWriter {
    pub(crate) fn new(level: u8) -> Self {
        Self {
            level,
            entries: Vec::new(),
            frames: 0,
            stored: 0,
        }
    }

    pub(crate) fn push(&mut self, original: usize, stored: &[u8]) -> Result<(), BlobError> {
        let too_large = || BlobError::WriteError("frame is too large".to_string());
        let len = u32::try_from(stored.len()).map_err(|_| too_large())?;
        let original = u32::try_from(original).map_err(|_| too_large())?;
        self.frames = self
            .frames
            .checked_add(1)
            .ok_or_else(|| BlobError::WriteError("object has too many frames".to_string()))?;
        self.entries.extend_from_slice(&len.to_le_bytes());
        self.entries.extend_from_slice(&original.to_le_bytes());
        self.stored += u64::from(len);
        Ok(())
    }

    /// Returns the seek table that ends the object and the layout record that names it.
    pub(crate) fn finish(self) -> Result<(Vec<u8>, FrameLayout), BlobError> {
        let frame_size = u32::try_from(self.entries.len() as u64 + FOOTER_LEN)
            .map_err(|_| BlobError::WriteError("seek table is too large".to_string()))?;
        let mut table = Vec::with_capacity((HEADER_LEN + u64::from(frame_size)) as usize);
        table.extend_from_slice(&SKIPPABLE_MAGIC.to_le_bytes());
        table.extend_from_slice(&frame_size.to_le_bytes());
        table.extend_from_slice(&self.entries);
        table.extend_from_slice(&self.frames.to_le_bytes());
        table.push(0);
        table.extend_from_slice(&SEEKABLE_MAGIC.to_le_bytes());
        let layout = FrameLayout {
            level: self.level,
            stored_size: self.stored + table.len() as u64,
            index_hash: *blake3::hash(&table).as_bytes(),
        };
        Ok((table, layout))
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
            out.push(self.encode(frame).await?);
        }
        Ok(out)
    }

    async fn encode(&mut self, frame: Bytes) -> Result<Bytes, BlobError> {
        let (level, original) = (self.level, frame.len());
        let stored = tokio::task::spawn_blocking(move || encode_frame(frame, level))
            .await
            .map_err(|error| BlobError::WriteError(error.to_string()))??;
        self.writer.push(original, &stored)?;
        Ok(stored)
    }

    /// Encodes the last short frame and returns the closing bytes with the layout record.
    pub(crate) async fn finish(mut self) -> Result<(Vec<Bytes>, FrameLayout), BlobError> {
        let mut out = Vec::new();
        if !self.pending.is_empty() {
            let frame = std::mem::take(&mut self.pending).freeze();
            out.push(self.encode(frame).await?);
        }
        let (table, layout) = self.writer.finish()?;
        out.push(Bytes::from(table));
        Ok((out, layout))
    }
}

/// Byte range of the seek table inside the stored object, checked against the record first.
pub(crate) fn table_range(size: u64, layout: &FrameLayout) -> Result<Range<u64>, BlobError> {
    let len = table_len(size);
    if layout.stored_size < len {
        return Err(integrity("stored size is too small for the seek table"));
    }
    Ok(layout.stored_size - len..layout.stored_size)
}

/// The checked seek table: where each frame starts in the stored object.
#[derive(Debug)]
pub(crate) struct FrameIndex {
    /// One start per frame, then the end of the last frame.
    offsets: Vec<u64>,
}

impl FrameIndex {
    /// Verifies the table hash before using any value, then checks every frame bound.
    pub(crate) fn parse(size: u64, layout: &FrameLayout, table: &[u8]) -> Result<Self, BlobError> {
        let frames = frame_count(size);
        if table.len() as u64 != table_len(size) {
            return Err(integrity("seek table has the wrong length"));
        }
        if blake3::hash(table).as_bytes() != &layout.index_hash {
            return Err(integrity("seek table hash mismatch"));
        }
        let footer = table.len() - FOOTER_LEN as usize;
        if read_u32(table, 0) != SKIPPABLE_MAGIC
            || u64::from(read_u32(table, 4)) != table.len() as u64 - HEADER_LEN
            || u64::from(read_u32(table, footer)) != frames
            || table[footer + 4] != 0
            || read_u32(table, footer + 5) != SEEKABLE_MAGIC
        {
            return Err(integrity("seek table header or footer is invalid"));
        }
        let bound = zstd::zstd_safe::compress_bound(FRAME_SIZE as usize) as u64;
        let entries = &table[HEADER_LEN as usize..footer];
        let mut offsets = Vec::with_capacity(frames as usize + 1);
        let mut start = 0u64;
        for (frame, entry) in entries
            .as_chunks::<{ ENTRY_LEN as usize }>()
            .0
            .iter()
            .enumerate()
        {
            let stored = u64::from(read_u32(entry, 0));
            let original = u64::from(read_u32(entry, 4));
            if stored == 0 || stored > bound || original != frame_len(size, frame as u64) {
                return Err(integrity("seek table entry is invalid"));
            }
            offsets.push(start);
            start += stored;
        }
        offsets.push(start);
        if start + table.len() as u64 != layout.stored_size {
            return Err(integrity("frames do not fill the stored object"));
        }
        Ok(Self { offsets })
    }

    /// Stored byte range of one frame.
    pub(crate) fn frame_range(&self, frame: u64) -> Range<u64> {
        self.offsets[frame as usize]..self.offsets[frame as usize + 1]
    }
}

/// Decodes one stored frame into exactly `expected` bytes; zstd checks the frame checksum.
pub(crate) fn decode_frame(expected: u64, stored: Bytes) -> Result<Bytes, BlobError> {
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
            let stored = encode_frame(Bytes::copy_from_slice(chunk), level).unwrap();
            writer.push(chunk.len(), &stored).unwrap();
            out.extend_from_slice(&stored);
        }
        let (table, layout) = writer.finish().unwrap();
        out.extend(table);
        (out, layout)
    }

    fn index_of(stored: &[u8], layout: &FrameLayout, size: u64) -> FrameIndex {
        let table = table_range(size, layout).unwrap();
        FrameIndex::parse(size, layout, &stored[table.start as usize..]).unwrap()
    }

    fn decode(
        stored: &[u8],
        layout: &FrameLayout,
        size: u64,
        range: Range<u64>,
    ) -> Result<Vec<u8>, BlobError> {
        let table = table_range(size, layout)?;
        let index = FrameIndex::parse(size, layout, &stored[table.start as usize..])?;
        let mut out = Vec::new();
        let mut position = range.start;
        while position < range.end {
            let frame = position / FRAME_SIZE;
            let bytes = index.frame_range(frame);
            let bytes = &stored[bytes.start as usize..bytes.end as usize];
            let length = frame_len(size, frame);
            let decoded = decode_frame(length, Bytes::copy_from_slice(bytes))?;
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
        // Data that does not compress grows only by frame headers and the table.
        let random = random(3 * FRAME_SIZE as usize, 11);
        let (stored, _) = encode(&random, 3);
        assert!(stored.len() < random.len() + 1024);
    }

    #[test]
    fn plain_zstd_decodes() {
        // The seek table is a skippable frame, so any zstd decoder returns the original.
        for data in samples() {
            let (stored, _) = encode(&data, 3);
            assert_eq!(zstd::stream::decode_all(&stored[..]).unwrap(), data);
        }
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
    fn tampered_bytes_fail() {
        let data = samples().remove(0);
        let size = data.len() as u64;
        let (stored, layout) = encode(&data, 3);
        let integrity = |result: Result<Vec<u8>, BlobError>| {
            matches!(result, Err(BlobError::IntegrityCheckFailed(_)))
        };
        let table = table_range(size, &layout).unwrap();
        let frame = index_of(&stored, &layout, size).frame_range(1);
        for at in [
            frame.start + 10,
            frame.end - 1,
            table.start,
            table.start + HEADER_LEN,
            table.end - 1,
        ] {
            let mut tampered = stored.clone();
            tampered[at as usize] ^= 1;
            assert!(integrity(decode(&tampered, &layout, size, 0..size)));
        }
        let mut record = layout.clone();
        record.stored_size += 1;
        assert!(integrity(decode(&stored, &record, size, 0..size)));
        let mut record = layout.clone();
        record.index_hash[0] ^= 1;
        assert!(integrity(decode(&stored, &record, size, 0..size)));
    }

    #[test]
    fn bombs_stay_bounded() {
        // A frame that claims more output than the frame may hold is refused unread.
        let large = zstd::bulk::compress(&vec![0u8; 64 * FRAME_SIZE as usize], 3).unwrap();
        let result = decode_frame(FRAME_SIZE, Bytes::from(large));
        assert!(matches!(result, Err(BlobError::IntegrityCheckFailed(_))));
        // A frame without a declared size is refused too.
        let open = zstd::stream::encode_all(&vec![0u8; 1024][..], 3).unwrap();
        let result = decode_frame(1024, Bytes::from(open));
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
