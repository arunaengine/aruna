//! Stores blobs as 1 MiB frames, each zstd compressed or raw, with a BLAKE3 digest per frame.
//! Frames form groups; each group ends with its frame entries, and the object ends with a tail
//! that lists every group. The location record keeps the tail hash.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::errors::BlobError;
use aruna_core::structs::storage::format::FrameLayout;
use bytes::{Bytes, BytesMut};

pub(crate) const FRAME_SIZE: u64 = 1 << 20;
/// Frames per group; one group's entries are read and checked together.
#[cfg(not(test))]
const GROUP_FRAMES: u64 = 1024;
/// Tests use small groups so several groups fit in a few MiB.
#[cfg(test)]
const GROUP_FRAMES: u64 = 2;
const TAG_RAW: u8 = 0;
const TAG_ZSTD: u8 = 1;
const MIN_SAVING: usize = 1024;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_saving_stays_raw() {
        let (tag, _) = encode_frame(Bytes::from(vec![0u8; 900]), 3).unwrap();
        assert_eq!(tag, TAG_RAW);
        let (tag, _) = encode_frame(Bytes::from(vec![0u8; 4096]), 3).unwrap();
        assert_eq!(tag, TAG_ZSTD);
    }

    #[tokio::test]
    async fn encoder_writes_index() {
        // Five frames in groups of two: three entry blocks and three tail summaries.
        let data: Vec<u8> = (0..5 * FRAME_SIZE as usize)
            .map(|i| (i % 7) as u8)
            .collect();
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

        assert_eq!(layout.stored_size, stored.len() as u64);
        let tail = &stored[stored.len() - 3 * 40..];
        assert_eq!(blake3::hash(tail).as_bytes(), &layout.index_hash);
        assert!(stored.len() < data.len() / 10);
    }
}
