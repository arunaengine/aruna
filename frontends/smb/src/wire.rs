//! SMB2 message framing: the direct TCP length prefix, the 64-byte header, the 52-byte
//! transform header, compound splitting and bounded little-endian readers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL_ID: [u8; 4] = *b"\xFESMB";
pub const TRANSFORM_ID: [u8; 4] = *b"\xFDSMB";
pub const LEGACY_ID: [u8; 4] = *b"\xFFSMB";
pub const HEADER_SIZE: usize = 64;
pub const TRANSFORM_SIZE: usize = 52;
/// The direct TCP transport carries a 24-bit length; nothing larger can be framed.
pub const TRANSPORT_LIMIT: usize = 0x00FF_FFFF;

pub const FLAG_RESPONSE: u32 = 0x0000_0001;
pub const FLAG_ASYNC: u32 = 0x0000_0002;
pub const FLAG_RELATED: u32 = 0x0000_0004;
pub const FLAG_SIGNED: u32 = 0x0000_0008;

/// The FileId value that names the open created earlier in a related compound chain.
pub const RELATED_FILE: FileId = FileId {
    persistent: u64::MAX,
    volatile: u64::MAX,
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireError {
    #[error("message truncated")]
    Truncated,
    #[error("malformed message: {0}")]
    Malformed(&'static str),
    #[error("frame of {0} bytes exceeds the limit")]
    TooLarge(usize),
    #[error("transport error: {0}")]
    Transport(String),
}

/// SMB2 command codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Command {
    Negotiate,
    SessionSetup,
    Logoff,
    TreeConnect,
    TreeDisconnect,
    Create,
    Close,
    Flush,
    Read,
    Write,
    Lock,
    Ioctl,
    Cancel,
    Echo,
    QueryDirectory,
    ChangeNotify,
    QueryInfo,
    SetInfo,
    OplockBreak,
}

impl Command {
    pub fn from_code(code: u16) -> Option<Self> {
        Some(match code {
            0x00 => Self::Negotiate,
            0x01 => Self::SessionSetup,
            0x02 => Self::Logoff,
            0x03 => Self::TreeConnect,
            0x04 => Self::TreeDisconnect,
            0x05 => Self::Create,
            0x06 => Self::Close,
            0x07 => Self::Flush,
            0x08 => Self::Read,
            0x09 => Self::Write,
            0x0A => Self::Lock,
            0x0B => Self::Ioctl,
            0x0C => Self::Cancel,
            0x0D => Self::Echo,
            0x0E => Self::QueryDirectory,
            0x0F => Self::ChangeNotify,
            0x10 => Self::QueryInfo,
            0x11 => Self::SetInfo,
            0x12 => Self::OplockBreak,
            _ => return None,
        })
    }

    pub fn code(self) -> u16 {
        match self {
            Self::Negotiate => 0x00,
            Self::SessionSetup => 0x01,
            Self::Logoff => 0x02,
            Self::TreeConnect => 0x03,
            Self::TreeDisconnect => 0x04,
            Self::Create => 0x05,
            Self::Close => 0x06,
            Self::Flush => 0x07,
            Self::Read => 0x08,
            Self::Write => 0x09,
            Self::Lock => 0x0A,
            Self::Ioctl => 0x0B,
            Self::Cancel => 0x0C,
            Self::Echo => 0x0D,
            Self::QueryDirectory => 0x0E,
            Self::ChangeNotify => 0x0F,
            Self::QueryInfo => 0x10,
            Self::SetInfo => 0x11,
            Self::OplockBreak => 0x12,
        }
    }
}

/// The 16-byte SMB2 file identifier of one open.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct FileId {
    pub persistent: u64,
    pub volatile: u64,
}

impl FileId {
    pub fn read(reader: &mut Reader<'_>) -> Result<Self, WireError> {
        Ok(Self {
            persistent: reader.u64()?,
            volatile: reader.u64()?,
        })
    }

    pub fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.persistent.to_le_bytes());
        out.extend_from_slice(&self.volatile.to_le_bytes());
    }
}

/// The 64-byte SMB2 packet header in its sync or async form.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Header {
    pub credit_charge: u16,
    /// NTSTATUS in responses; ChannelSequence and Reserved in requests.
    pub status: u32,
    pub command: u16,
    /// CreditRequest in requests, CreditResponse in responses.
    pub credits: u16,
    pub flags: u32,
    pub next_command: u32,
    pub message_id: u64,
    pub async_id: u64,
    pub tree_id: u32,
    pub session_id: u64,
    pub signature: [u8; 16],
}

impl Header {
    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() < HEADER_SIZE {
            return Err(WireError::Truncated);
        }
        if bytes[0..4] != PROTOCOL_ID {
            return Err(WireError::Malformed("protocol identifier"));
        }
        let mut reader = Reader::new(&bytes[4..HEADER_SIZE]);
        if reader.u16()? != 64 {
            return Err(WireError::Malformed("header structure size"));
        }
        let credit_charge = reader.u16()?;
        let status = reader.u32()?;
        let command = reader.u16()?;
        let credits = reader.u16()?;
        let flags = reader.u32()?;
        let next_command = reader.u32()?;
        let message_id = reader.u64()?;
        let (async_id, tree_id) = if flags & FLAG_ASYNC != 0 {
            (reader.u64()?, 0)
        } else {
            reader.skip(4)?;
            (0, reader.u32()?)
        };
        let session_id = reader.u64()?;
        let mut signature = [0u8; 16];
        signature.copy_from_slice(reader.take(16)?);
        Ok(Self {
            credit_charge,
            status,
            command,
            credits,
            flags,
            next_command,
            message_id,
            async_id,
            tree_id,
            session_id,
            signature,
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&PROTOCOL_ID);
        out.extend_from_slice(&64u16.to_le_bytes());
        out.extend_from_slice(&self.credit_charge.to_le_bytes());
        out.extend_from_slice(&self.status.to_le_bytes());
        out.extend_from_slice(&self.command.to_le_bytes());
        out.extend_from_slice(&self.credits.to_le_bytes());
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&self.next_command.to_le_bytes());
        out.extend_from_slice(&self.message_id.to_le_bytes());
        if self.flags & FLAG_ASYNC != 0 {
            out.extend_from_slice(&self.async_id.to_le_bytes());
        } else {
            out.extend_from_slice(&0u32.to_le_bytes());
            out.extend_from_slice(&self.tree_id.to_le_bytes());
        }
        out.extend_from_slice(&self.session_id.to_le_bytes());
        out.extend_from_slice(&self.signature);
    }

    pub fn is_related(&self) -> bool {
        self.flags & FLAG_RELATED != 0
    }
}

/// The 52-byte transform header that wraps an encrypted message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransformHeader {
    pub signature: [u8; 16],
    pub nonce: [u8; 16],
    pub message_size: u32,
    pub flags: u16,
    pub session_id: u64,
}

impl TransformHeader {
    pub const ENCRYPTED: u16 = 0x0001;

    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() < TRANSFORM_SIZE {
            return Err(WireError::Truncated);
        }
        if bytes[0..4] != TRANSFORM_ID {
            return Err(WireError::Malformed("transform identifier"));
        }
        let mut reader = Reader::new(&bytes[4..TRANSFORM_SIZE]);
        let mut signature = [0u8; 16];
        signature.copy_from_slice(reader.take(16)?);
        let mut nonce = [0u8; 16];
        nonce.copy_from_slice(reader.take(16)?);
        let message_size = reader.u32()?;
        reader.skip(2)?;
        let flags = reader.u16()?;
        let session_id = reader.u64()?;
        Ok(Self {
            signature,
            nonce,
            message_size,
            flags,
            session_id,
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&TRANSFORM_ID);
        out.extend_from_slice(&self.signature);
        out.extend_from_slice(&self.nonce);
        out.extend_from_slice(&self.message_size.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&self.session_id.to_le_bytes());
    }

    /// The authenticated bytes: everything after the signature field.
    pub fn associated(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(TRANSFORM_SIZE);
        self.encode(&mut out);
        out.split_off(20)
    }
}

/// A bounds-checked little-endian reader over one message slice.
pub struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    pub fn position(&self) -> usize {
        self.position
    }

    pub fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    pub fn take(&mut self, length: usize) -> Result<&'a [u8], WireError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(WireError::Truncated)?;
        let slice = self
            .bytes
            .get(self.position..end)
            .ok_or(WireError::Truncated)?;
        self.position = end;
        Ok(slice)
    }

    pub fn skip(&mut self, length: usize) -> Result<(), WireError> {
        self.take(length).map(|_| ())
    }

    pub fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }

    pub fn u16(&mut self) -> Result<u16, WireError> {
        let bytes = self.take(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub fn u32(&mut self) -> Result<u32, WireError> {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    pub fn u64(&mut self) -> Result<u64, WireError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(bytes))
    }
}

/// The slice of `message` at a header-relative offset, refusing anything out of bounds.
pub fn buffer_at(message: &[u8], offset: usize, length: usize) -> Result<&[u8], WireError> {
    if length == 0 {
        return Ok(&[]);
    }
    if offset < HEADER_SIZE {
        return Err(WireError::Malformed("buffer offset inside the header"));
    }
    let end = offset.checked_add(length).ok_or(WireError::Truncated)?;
    message.get(offset..end).ok_or(WireError::Truncated)
}

/// Appends zero bytes until `out` has a length that is a multiple of `align`.
pub fn pad_to(out: &mut Vec<u8>, align: usize) {
    while !out.len().is_multiple_of(align) {
        out.push(0);
    }
}

/// Decodes UTF-16LE text; an odd length or an unpaired surrogate fails.
pub fn utf16_text(bytes: &[u8]) -> Option<String> {
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    let units: Vec<u16> = bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u16::from_le_bytes(*pair))
        .collect();
    String::from_utf16(&units).ok()
}

pub fn utf16_bytes(text: &str) -> Vec<u8> {
    text.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

/// Splits a compound request into its elements at each NextCommand offset.
pub fn split_compound(message: &[u8]) -> Result<Vec<&[u8]>, WireError> {
    let mut elements = Vec::new();
    let mut start = 0usize;
    loop {
        let rest = &message[start..];
        let header = Header::decode(rest)?;
        let next = header.next_command as usize;
        if next == 0 {
            elements.push(rest);
            return Ok(elements);
        }
        if next < HEADER_SIZE || !next.is_multiple_of(8) || next >= rest.len() {
            return Err(WireError::Malformed("compound offset"));
        }
        elements.push(&rest[..next]);
        start += next;
    }
}

/// Joins response elements into one compound, padding all but the last to eight bytes.
pub fn join_compound(mut elements: Vec<Vec<u8>>) -> Vec<u8> {
    let count = elements.len();
    let mut out = Vec::new();
    for (index, element) in elements.iter_mut().enumerate() {
        if index + 1 < count {
            pad_to(element, 8);
            let next = element.len() as u32;
            element[20..24].copy_from_slice(&next.to_le_bytes());
        }
        out.extend_from_slice(element);
    }
    out
}

/// Reads one direct TCP frame, refusing a length above `limit` before allocating.
/// Returns `None` on a clean end of stream before a frame starts.
pub async fn read_frame<R>(reader: &mut R, limit: usize) -> Result<Option<Vec<u8>>, WireError>
where
    R: AsyncRead + Unpin,
{
    let mut prefix = [0u8; 4];
    match reader.read_exact(&mut prefix[..1]).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(WireError::Transport(error.to_string())),
    }
    reader
        .read_exact(&mut prefix[1..])
        .await
        .map_err(|error| WireError::Transport(error.to_string()))?;
    if prefix[0] != 0 {
        return Err(WireError::Malformed("transport message type"));
    }
    let length = u32::from_be_bytes([0, prefix[1], prefix[2], prefix[3]]) as usize;
    if length > limit.min(TRANSPORT_LIMIT) {
        return Err(WireError::TooLarge(length));
    }
    if length < 4 {
        return Err(WireError::Malformed(
            "frame shorter than a protocol identifier",
        ));
    }
    let mut message = vec![0u8; length];
    reader
        .read_exact(&mut message)
        .await
        .map_err(|error| WireError::Transport(error.to_string()))?;
    Ok(Some(message))
}

/// Prefixes one message with its direct TCP length.
pub fn frame(message: &[u8]) -> Result<Vec<u8>, WireError> {
    if message.len() > TRANSPORT_LIMIT {
        return Err(WireError::TooLarge(message.len()));
    }
    let length = (message.len() as u32).to_be_bytes();
    let mut out = Vec::with_capacity(message.len() + 4);
    out.push(0);
    out.extend_from_slice(&length[1..]);
    out.extend_from_slice(message);
    Ok(out)
}

pub async fn write_frame<W>(writer: &mut W, framed: &[u8]) -> Result<(), WireError>
where
    W: AsyncWrite + Unpin,
{
    writer
        .write_all(framed)
        .await
        .map_err(|error| WireError::Transport(error.to_string()))?;
    writer
        .flush()
        .await
        .map_err(|error| WireError::Transport(error.to_string()))
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;
