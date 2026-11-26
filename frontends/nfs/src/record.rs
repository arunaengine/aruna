//! RFC 5531 record marking over a byte stream, with the record size limit checked before any
//! fragment is buffered.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const LAST_FRAGMENT: u32 = 0x8000_0000;
/// Most fragments one record may have, so empty fragments cannot spin the reader.
const FRAGMENT_LIMIT: usize = 1024;

#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    #[error("a record exceeds {limit} bytes")]
    TooLarge { limit: usize },
    #[error("a record has too many fragments")]
    Fragments,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Reads one record of at most `limit` bytes; `None` on a clean end of stream between records.
pub async fn read_record<R>(reader: &mut R, limit: usize) -> Result<Option<Bytes>, RecordError>
where
    R: AsyncRead + Unpin,
{
    let mut record = BytesMut::new();
    for index in 0..FRAGMENT_LIMIT {
        let mut header = [0u8; 4];
        if index == 0 {
            if reader.read(&mut header[..1]).await? == 0 {
                return Ok(None);
            }
            reader.read_exact(&mut header[1..]).await?;
        } else {
            reader.read_exact(&mut header).await?;
        }
        let word = u32::from_be_bytes(header);
        let length = (word & !LAST_FRAGMENT) as usize;
        let start = record.len();
        let total = start
            .checked_add(length)
            .filter(|total| *total <= limit)
            .ok_or(RecordError::TooLarge { limit })?;
        record.resize(total, 0);
        reader.read_exact(&mut record[start..]).await?;
        if word & LAST_FRAGMENT != 0 {
            return Ok(Some(record.freeze()));
        }
    }
    Err(RecordError::Fragments)
}

/// Writes `payload` as one final fragment in a single write.
pub async fn write_record<W>(writer: &mut W, payload: &[u8]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let length = u32::try_from(payload.len())
        .ok()
        .filter(|length| *length < LAST_FRAGMENT)
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let mut framed = BytesMut::with_capacity(payload.len() + 4);
    framed.put_u32(LAST_FRAGMENT | length);
    framed.put_slice(payload);
    writer.write_all(&framed).await?;
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn joins_fragments() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&3u32.to_be_bytes());
        raw.extend_from_slice(b"abc");
        raw.extend_from_slice(&(LAST_FRAGMENT | 2).to_be_bytes());
        raw.extend_from_slice(b"de");
        let mut input = &raw[..];
        let record = read_record(&mut input, 16).await.expect("record");
        assert_eq!(record.as_deref(), Some(&b"abcde"[..]));
        assert!(read_record(&mut input, 16).await.expect("end").is_none());
    }

    // The declared fragment length is refused before its bytes are read or buffered.
    #[tokio::test]
    async fn rejects_oversized_fragment() {
        let raw = (LAST_FRAGMENT | 0x7fff_0000).to_be_bytes();
        let mut input = &raw[..];
        let result = read_record(&mut input, 1024).await;
        assert!(matches!(result, Err(RecordError::TooLarge { limit: 1024 })));
    }

    #[tokio::test]
    async fn rejects_endless_fragments() {
        let raw: Vec<u8> = std::iter::repeat_n(0u8, 4 * (FRAGMENT_LIMIT + 1)).collect();
        let mut input = &raw[..];
        let result = read_record(&mut input, 1024).await;
        assert!(matches!(result, Err(RecordError::Fragments)));
    }

    #[tokio::test]
    async fn writes_last_fragment() {
        let mut output = Vec::new();
        write_record(&mut output, b"xyz").await.expect("write");
        assert_eq!(&output[..4], &(LAST_FRAGMENT | 3).to_be_bytes());
        assert_eq!(&output[4..], b"xyz");
    }
}
