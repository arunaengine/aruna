//! Bounded XDR (RFC 4506) reading and writing: every variable item has a limit checked before
//! any allocation, and padding must be zero.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum XdrError {
    #[error("the message ends inside an item")]
    Truncated,
    #[error("an item padding byte is not zero")]
    Padding,
    #[error("an item of {length} exceeds its limit of {limit}")]
    TooLong { length: usize, limit: usize },
    #[error("a string is not valid UTF-8")]
    Utf8,
    #[error("a boolean or union value is out of range")]
    Discriminant,
}

impl From<XdrError> for crate::types::NfsStat4 {
    fn from(_: XdrError) -> Self {
        Self::BadXdr
    }
}

/// The zero bytes that round `length` up to a multiple of four.
fn padding(length: usize) -> usize {
    (4 - length % 4) % 4
}

pub struct XdrReader {
    buffer: Bytes,
}

impl XdrReader {
    pub fn new(buffer: Bytes) -> Self {
        Self { buffer }
    }

    pub fn remaining(&self) -> usize {
        self.buffer.remaining()
    }

    pub fn u32(&mut self) -> Result<u32, XdrError> {
        if self.buffer.remaining() < 4 {
            return Err(XdrError::Truncated);
        }
        Ok(self.buffer.get_u32())
    }

    pub fn u64(&mut self) -> Result<u64, XdrError> {
        if self.buffer.remaining() < 8 {
            return Err(XdrError::Truncated);
        }
        Ok(self.buffer.get_u64())
    }

    pub fn i64(&mut self) -> Result<i64, XdrError> {
        if self.buffer.remaining() < 8 {
            return Err(XdrError::Truncated);
        }
        Ok(self.buffer.get_i64())
    }

    pub fn bool(&mut self) -> Result<bool, XdrError> {
        match self.u32()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(XdrError::Discriminant),
        }
    }

    /// Fixed-length opaque data of `length` bytes and its zero padding.
    pub fn fixed(&mut self, length: usize) -> Result<Bytes, XdrError> {
        let pad = padding(length);
        let total = length.checked_add(pad).ok_or(XdrError::Truncated)?;
        if self.buffer.remaining() < total {
            return Err(XdrError::Truncated);
        }
        let data = self.buffer.split_to(length);
        let fill = self.buffer.split_to(pad);
        if fill.iter().any(|byte| *byte != 0) {
            return Err(XdrError::Padding);
        }
        Ok(data)
    }

    pub fn array<const N: usize>(&mut self) -> Result<[u8; N], XdrError> {
        let data = self.fixed(N)?;
        let mut out = [0u8; N];
        out.copy_from_slice(&data);
        Ok(out)
    }

    /// Variable-length opaque data of at most `limit` bytes.
    pub fn opaque(&mut self, limit: usize) -> Result<Bytes, XdrError> {
        let length = self.u32()? as usize;
        if length > limit {
            return Err(XdrError::TooLong { length, limit });
        }
        self.fixed(length)
    }

    pub fn string(&mut self, limit: usize) -> Result<String, XdrError> {
        let data = self.opaque(limit)?;
        String::from_utf8(data.to_vec()).map_err(|_| XdrError::Utf8)
    }

    /// An array length of at most `limit` elements. Every XDR element takes at least four
    /// bytes, so a count the remaining input cannot hold is refused before any allocation.
    pub fn count(&mut self, limit: usize) -> Result<usize, XdrError> {
        let length = self.u32()? as usize;
        if length > limit {
            return Err(XdrError::TooLong { length, limit });
        }
        if length > self.buffer.remaining() / 4 {
            return Err(XdrError::Truncated);
        }
        Ok(length)
    }

    pub fn into_rest(self) -> Bytes {
        self.buffer
    }
}

#[derive(Default)]
pub struct XdrWriter {
    buffer: BytesMut,
}

impl XdrWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn put_u32(&mut self, value: u32) {
        self.buffer.put_u32(value);
    }

    pub fn put_u64(&mut self, value: u64) {
        self.buffer.put_u64(value);
    }

    pub fn put_i64(&mut self, value: i64) {
        self.buffer.put_i64(value);
    }

    pub fn put_bool(&mut self, value: bool) {
        self.buffer.put_u32(u32::from(value));
    }

    /// Fixed-length opaque data with its zero padding.
    pub fn put_fixed(&mut self, data: &[u8]) {
        self.buffer.put_slice(data);
        self.buffer.put_bytes(0, padding(data.len()));
    }

    /// Variable-length opaque data: its length, the bytes and the padding.
    pub fn put_opaque(&mut self, data: &[u8]) {
        self.buffer.put_u32(data.len() as u32);
        self.put_fixed(data);
    }

    pub fn put_string(&mut self, value: &str) {
        self.put_opaque(value.as_bytes());
    }

    /// Bytes already in XDR form, such as a nested encoded result.
    pub fn put_raw(&mut self, data: &[u8]) {
        self.buffer.put_slice(data);
    }

    /// Overwrites the four bytes at `offset`, which an earlier `put_u32` wrote.
    pub fn patch_u32(&mut self, offset: usize, value: u32) {
        self.buffer[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
    }

    pub fn truncate(&mut self, length: usize) {
        self.buffer.truncate(length);
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buffer
    }

    pub fn into_bytes(self) -> Bytes {
        self.buffer.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_roundtrip_pads() {
        let mut writer = XdrWriter::new();
        writer.put_opaque(b"abcde");
        writer.put_u64(7);
        assert_eq!(writer.len(), 4 + 8 + 8);
        let mut reader = XdrReader::new(writer.into_bytes());
        assert_eq!(&reader.opaque(16).expect("opaque")[..], b"abcde");
        assert_eq!(reader.u64().expect("u64"), 7);
        assert_eq!(reader.remaining(), 0);
    }

    // A declared length beyond the limit is refused before its bytes are read.
    #[test]
    fn rejects_oversized_opaque() {
        let mut writer = XdrWriter::new();
        writer.put_u32(5000);
        let mut reader = XdrReader::new(writer.into_bytes());
        assert_eq!(
            reader.opaque(1024),
            Err(XdrError::TooLong {
                length: 5000,
                limit: 1024
            })
        );
    }

    #[test]
    fn rejects_nonzero_padding() {
        let raw = Bytes::from_static(&[0, 0, 0, 1, b'x', 1, 0, 0]);
        assert_eq!(XdrReader::new(raw).opaque(8), Err(XdrError::Padding));
    }

    // An array count larger than the remaining input could hold never allocates.
    #[test]
    fn rejects_impossible_count() {
        let raw = Bytes::from_static(&[0, 0, 0, 100, 0, 0, 0, 1]);
        assert_eq!(XdrReader::new(raw).count(1000), Err(XdrError::Truncated));
        let raw = Bytes::from_static(&[0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2]);
        assert_eq!(XdrReader::new(raw).count(1000), Ok(2));
    }

    #[test]
    fn rejects_bad_boolean() {
        let raw = Bytes::from_static(&[0, 0, 0, 2]);
        assert_eq!(XdrReader::new(raw).bool(), Err(XdrError::Discriminant));
        assert_eq!(
            XdrReader::new(Bytes::from_static(&[0, 0])).u32(),
            Err(XdrError::Truncated)
        );
    }
}
