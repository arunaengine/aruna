//! Length-prefixed JSON frames: a four-byte big-endian payload length, then the payload, bounded
//! by `MAX_FRAME_BYTES`. Blocking readers and writers here; async callers reuse the codec.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::io::{ErrorKind, Read, Write};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::MAX_FRAME_BYTES;

/// Why a frame could not be written or read.
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("a frame must carry at least one byte")]
    Empty,
    #[error("a frame of {size} bytes exceeds the limit of {MAX_FRAME_BYTES} bytes")]
    TooLarge { size: usize },
    /// The stream ended inside a frame.
    #[error("the stream ended inside a frame")]
    Truncated,
    #[error("the frame is no valid message: {0}")]
    Json(#[from] serde_json::Error),
    #[error("the stream failed: {0}")]
    Io(#[from] std::io::Error),
}

/// The payload length that a prefix announces, refused when it is empty or too large.
pub fn decode_frame_len(prefix: [u8; 4]) -> Result<usize, FrameError> {
    let size = u32::from_be_bytes(prefix) as usize;
    check_size(size)?;
    Ok(size)
}

/// The prefix and the payload as one buffer, ready to be written.
pub fn encode_frame(payload: &[u8]) -> Result<Vec<u8>, FrameError> {
    check_size(payload.len())?;
    let size = u32::try_from(payload.len()).map_err(|_| FrameError::TooLarge {
        size: payload.len(),
    })?;
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&size.to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// The JSON of a message as a whole frame.
pub fn encode_message<T: Serialize>(message: &T) -> Result<Vec<u8>, FrameError> {
    encode_frame(&serde_json::to_vec(message)?)
}

/// A message from the payload of a frame.
pub fn decode_message<T: DeserializeOwned>(payload: &[u8]) -> Result<T, FrameError> {
    check_size(payload.len())?;
    Ok(serde_json::from_slice(payload)?)
}

/// Writes one message as a frame and flushes it.
pub fn write_frame<W: Write, T: Serialize>(writer: &mut W, message: &T) -> Result<(), FrameError> {
    writer.write_all(&encode_message(message)?)?;
    writer.flush()?;
    Ok(())
}

/// Reads one message; `None` when the stream ended cleanly between frames.
pub fn read_frame<R: Read, T: DeserializeOwned>(reader: &mut R) -> Result<Option<T>, FrameError> {
    let mut prefix = [0u8; 4];
    let mut filled = 0;
    while filled < prefix.len() {
        match reader.read(&mut prefix[filled..]) {
            Ok(0) if filled == 0 => return Ok(None),
            Ok(0) => return Err(FrameError::Truncated),
            Ok(read) => filled += read,
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    let size = decode_frame_len(prefix)?;
    let mut payload = vec![0u8; size];
    reader
        .read_exact(&mut payload)
        .map_err(|error| match error.kind() {
            ErrorKind::UnexpectedEof => FrameError::Truncated,
            _ => FrameError::Io(error),
        })?;
    decode_message(&payload).map(Some)
}

fn check_size(size: usize) -> Result<(), FrameError> {
    match size {
        0 => Err(FrameError::Empty),
        size if size > MAX_FRAME_BYTES => Err(FrameError::TooLarge { size }),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::message::HostToCoordinator;

    #[test]
    fn round_trips_frames() {
        let mut buffer = Vec::new();
        write_frame(&mut buffer, &HostToCoordinator::Shutdown).expect("writes");
        write_frame(&mut buffer, &serde_json::json!({"a": 1})).expect("writes");
        let mut reader = Cursor::new(buffer);
        let first: Option<HostToCoordinator> = read_frame(&mut reader).expect("reads");
        assert_eq!(first, Some(HostToCoordinator::Shutdown));
        let second: Option<serde_json::Value> = read_frame(&mut reader).expect("reads");
        assert_eq!(second, Some(serde_json::json!({"a": 1})));
        let end: Option<serde_json::Value> = read_frame(&mut reader).expect("a clean end");
        assert_eq!(end, None);
    }

    #[test]
    fn rejects_empty_frames() {
        assert!(matches!(encode_frame(b""), Err(FrameError::Empty)));
        assert!(matches!(decode_frame_len([0; 4]), Err(FrameError::Empty)));
        let mut reader = Cursor::new(vec![0u8, 0, 0, 0]);
        let read: Result<Option<serde_json::Value>, _> = read_frame(&mut reader);
        assert!(matches!(read, Err(FrameError::Empty)));
    }

    #[test]
    fn bounds_frame_size() {
        let limit = u32::try_from(MAX_FRAME_BYTES).expect("fits");
        assert_eq!(
            decode_frame_len(limit.to_be_bytes()).ok(),
            Some(MAX_FRAME_BYTES)
        );
        let over = decode_frame_len((limit + 1).to_be_bytes());
        assert!(matches!(over, Err(FrameError::TooLarge { size }) if size == MAX_FRAME_BYTES + 1));
        let payload = vec![b' '; MAX_FRAME_BYTES + 1];
        assert!(matches!(
            encode_frame(&payload),
            Err(FrameError::TooLarge { .. })
        ));
        // A reader refuses the announced size before it allocates the payload.
        let mut reader = Cursor::new(u32::MAX.to_be_bytes().to_vec());
        let read: Result<Option<serde_json::Value>, _> = read_frame(&mut reader);
        assert!(matches!(read, Err(FrameError::TooLarge { .. })));
    }

    #[test]
    fn reports_truncated_streams() {
        let mut reader = Cursor::new(vec![0u8, 0]);
        let read: Result<Option<serde_json::Value>, _> = read_frame(&mut reader);
        assert!(matches!(read, Err(FrameError::Truncated)));
        let mut frame = encode_message(&serde_json::json!({"key": "value"})).expect("encodes");
        frame.truncate(frame.len() - 3);
        let read: Result<Option<serde_json::Value>, _> = read_frame(&mut Cursor::new(frame));
        assert!(matches!(read, Err(FrameError::Truncated)));
    }

    #[test]
    fn rejects_invalid_json() {
        let frame = encode_frame(b"{not json").expect("encodes");
        let read: Result<Option<serde_json::Value>, _> = read_frame(&mut Cursor::new(frame));
        assert!(matches!(read, Err(FrameError::Json(_))));
    }
}
