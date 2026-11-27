//! Request decoders and response encoders for every SMB2 command body the server handles.
//! Offsets inside requests are relative to the start of the SMB2 header.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::wire::{FileId, HEADER_SIZE, Reader, WireError, buffer_at};

/// A reader positioned on the command body after checking its StructureSize.
fn body(message: &[u8], size: u16) -> Result<Reader<'_>, WireError> {
    let rest = message.get(HEADER_SIZE..).ok_or(WireError::Truncated)?;
    let mut reader = Reader::new(rest);
    if reader.u16()? != size {
        return Err(WireError::Malformed("structure size"));
    }
    Ok(reader)
}

/// The SMB2 ERROR response body with optional error data.
pub fn error_body(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + data.len().max(1));
    out.extend_from_slice(&9u16.to_le_bytes());
    out.push(0);
    out.push(0);
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    if data.is_empty() {
        out.push(0);
    } else {
        out.extend_from_slice(data);
    }
    out
}

/// A four-byte body of StructureSize 4 used by ECHO, LOGOFF and TREE_DISCONNECT.
pub fn empty_body() -> Vec<u8> {
    let mut out = 4u16.to_le_bytes().to_vec();
    out.extend_from_slice(&0u16.to_le_bytes());
    out
}

pub fn parse_empty(message: &[u8]) -> Result<(), WireError> {
    body(message, 4).map(|_| ())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegotiateContext {
    pub kind: u16,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct NegotiateRequest {
    pub dialects: Vec<u16>,
    pub security_mode: u16,
    pub capabilities: u32,
    pub client_guid: [u8; 16],
    pub contexts: Vec<NegotiateContext>,
}

pub fn parse_negotiate(message: &[u8]) -> Result<NegotiateRequest, WireError> {
    let mut reader = body(message, 36)?;
    let dialect_count = reader.u16()?;
    let security_mode = reader.u16()?;
    reader.skip(2)?;
    let capabilities = reader.u32()?;
    let mut client_guid = [0u8; 16];
    client_guid.copy_from_slice(reader.take(16)?);
    let context_offset = reader.u32()? as usize;
    let context_count = reader.u16()?;
    reader.skip(2)?;
    if dialect_count == 0 {
        return Err(WireError::Malformed("no dialects"));
    }
    let mut dialects = Vec::with_capacity(dialect_count as usize);
    for _ in 0..dialect_count {
        dialects.push(reader.u16()?);
    }
    let mut contexts = Vec::new();
    if context_count > 0 {
        if !context_offset.is_multiple_of(8) {
            return Err(WireError::Malformed("negotiate context alignment"));
        }
        let mut position = context_offset;
        for index in 0..context_count {
            if index > 0 {
                position = position.next_multiple_of(8);
            }
            let fixed = buffer_at(message, position, 8)?;
            let mut header = Reader::new(fixed);
            let kind = header.u16()?;
            let length = header.u16()? as usize;
            let data = buffer_at(message, position + 8, length)?.to_vec();
            contexts.push(NegotiateContext { kind, data });
            position += 8 + length;
        }
    }
    Ok(NegotiateRequest {
        dialects,
        security_mode,
        capabilities,
        client_guid,
        contexts,
    })
}

/// The fixed fields of a NEGOTIATE response before its security buffer and contexts.
pub struct NegotiateReply<'a> {
    pub security_mode: u16,
    pub dialect: u16,
    pub server_guid: [u8; 16],
    pub capabilities: u32,
    pub max_transact: u32,
    pub max_read: u32,
    pub max_write: u32,
    pub system_time: u64,
    pub security: &'a [u8],
    pub contexts: &'a [NegotiateContext],
}

pub fn negotiate_body(reply: &NegotiateReply<'_>) -> Vec<u8> {
    let security_offset = HEADER_SIZE + 64;
    let mut out = Vec::new();
    out.extend_from_slice(&65u16.to_le_bytes());
    out.extend_from_slice(&reply.security_mode.to_le_bytes());
    out.extend_from_slice(&reply.dialect.to_le_bytes());
    out.extend_from_slice(&(reply.contexts.len() as u16).to_le_bytes());
    out.extend_from_slice(&reply.server_guid);
    out.extend_from_slice(&reply.capabilities.to_le_bytes());
    out.extend_from_slice(&reply.max_transact.to_le_bytes());
    out.extend_from_slice(&reply.max_read.to_le_bytes());
    out.extend_from_slice(&reply.max_write.to_le_bytes());
    out.extend_from_slice(&reply.system_time.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(&(security_offset as u16).to_le_bytes());
    out.extend_from_slice(&(reply.security.len() as u16).to_le_bytes());
    let context_offset = (security_offset + reply.security.len()).next_multiple_of(8);
    let offset_field = if reply.contexts.is_empty() {
        0
    } else {
        context_offset as u32
    };
    out.extend_from_slice(&offset_field.to_le_bytes());
    out.extend_from_slice(reply.security);
    for context in reply.contexts {
        while !(HEADER_SIZE + out.len()).is_multiple_of(8) {
            out.push(0);
        }
        out.extend_from_slice(&context.kind.to_le_bytes());
        out.extend_from_slice(&(context.data.len() as u16).to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&context.data);
    }
    if out.len() == 64 {
        out.push(0);
    }
    out
}

#[derive(Clone, Debug)]
pub struct SetupRequest {
    pub flags: u8,
    pub security: Vec<u8>,
    pub previous: u64,
}

pub fn parse_setup(message: &[u8]) -> Result<SetupRequest, WireError> {
    let mut reader = body(message, 25)?;
    let flags = reader.u8()?;
    reader.skip(1 + 4 + 4)?;
    let offset = reader.u16()? as usize;
    let length = reader.u16()? as usize;
    let previous = reader.u64()?;
    let security = buffer_at(message, offset, length)?.to_vec();
    Ok(SetupRequest {
        flags,
        security,
        previous,
    })
}

pub fn setup_body(session_flags: u16, security: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + security.len().max(1));
    out.extend_from_slice(&9u16.to_le_bytes());
    out.extend_from_slice(&session_flags.to_le_bytes());
    let offset = if security.is_empty() {
        0
    } else {
        (HEADER_SIZE + 8) as u16
    };
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&(security.len() as u16).to_le_bytes());
    if security.is_empty() {
        out.push(0);
    } else {
        out.extend_from_slice(security);
    }
    out
}

#[derive(Clone, Debug)]
pub struct TreeRequest {
    pub flags: u16,
    pub path: Vec<u8>,
}

pub fn parse_tree(message: &[u8]) -> Result<TreeRequest, WireError> {
    let mut reader = body(message, 9)?;
    let flags = reader.u16()?;
    let offset = reader.u16()? as usize;
    let length = reader.u16()? as usize;
    let path = buffer_at(message, offset, length)?.to_vec();
    Ok(TreeRequest { flags, path })
}

pub fn tree_body(share_flags: u32, maximal_access: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&16u16.to_le_bytes());
    out.push(0x01);
    out.push(0);
    out.extend_from_slice(&share_flags.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&maximal_access.to_le_bytes());
    out
}

#[derive(Clone, Debug)]
pub struct CreateContext {
    pub name: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct CreateRequest {
    pub oplock: u8,
    pub impersonation: u32,
    pub desired_access: u32,
    pub attributes: u32,
    pub share_access: u32,
    pub disposition: u32,
    pub options: u32,
    pub name: Vec<u8>,
    pub contexts: Vec<CreateContext>,
}

pub fn parse_create(message: &[u8]) -> Result<CreateRequest, WireError> {
    let mut reader = body(message, 57)?;
    reader.skip(1)?;
    let oplock = reader.u8()?;
    let impersonation = reader.u32()?;
    reader.skip(16)?;
    let desired_access = reader.u32()?;
    let attributes = reader.u32()?;
    let share_access = reader.u32()?;
    let disposition = reader.u32()?;
    let options = reader.u32()?;
    let name_offset = reader.u16()? as usize;
    let name_length = reader.u16()? as usize;
    let context_offset = reader.u32()? as usize;
    let context_length = reader.u32()? as usize;
    let name = buffer_at(message, name_offset, name_length)?.to_vec();
    let contexts = parse_contexts(buffer_at(message, context_offset, context_length)?)?;
    Ok(CreateRequest {
        oplock,
        impersonation,
        desired_access,
        attributes,
        share_access,
        disposition,
        options,
        name,
        contexts,
    })
}

fn parse_contexts(mut bytes: &[u8]) -> Result<Vec<CreateContext>, WireError> {
    let mut contexts = Vec::new();
    while !bytes.is_empty() {
        let mut reader = Reader::new(bytes);
        let next = reader.u32()? as usize;
        let name_offset = reader.u16()? as usize;
        let name_length = reader.u16()? as usize;
        reader.skip(2)?;
        let data_offset = reader.u16()? as usize;
        let data_length = reader.u32()? as usize;
        let name = slice_at(bytes, name_offset, name_length)?.to_vec();
        let data = slice_at(bytes, data_offset, data_length)?.to_vec();
        contexts.push(CreateContext { name, data });
        if next == 0 {
            break;
        }
        if !next.is_multiple_of(8) || next >= bytes.len() {
            return Err(WireError::Malformed("create context chain"));
        }
        bytes = &bytes[next..];
    }
    Ok(contexts)
}

fn slice_at(bytes: &[u8], offset: usize, length: usize) -> Result<&[u8], WireError> {
    if length == 0 {
        return Ok(&[]);
    }
    let end = offset.checked_add(length).ok_or(WireError::Truncated)?;
    bytes.get(offset..end).ok_or(WireError::Truncated)
}

/// Encodes create response contexts as one chain, each eight-byte aligned.
pub fn context_chain(contexts: &[CreateContext]) -> Vec<u8> {
    let mut out = Vec::new();
    for (index, context) in contexts.iter().enumerate() {
        let start = out.len();
        let name_offset = 16u16;
        let data_offset = (16 + context.name.len()).next_multiple_of(8) as u16;
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&name_offset.to_le_bytes());
        out.extend_from_slice(&(context.name.len() as u16).to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&data_offset.to_le_bytes());
        out.extend_from_slice(&(context.data.len() as u32).to_le_bytes());
        out.extend_from_slice(&context.name);
        while out.len() - start < data_offset as usize {
            out.push(0);
        }
        out.extend_from_slice(&context.data);
        if index + 1 < contexts.len() {
            while !(out.len() - start).is_multiple_of(8) {
                out.push(0);
            }
            let next = (out.len() - start) as u32;
            out[start..start + 4].copy_from_slice(&next.to_le_bytes());
        }
    }
    out
}

/// Timestamps, sizes and attributes reported by CREATE and CLOSE.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FileTimes {
    pub created: u64,
    pub accessed: u64,
    pub written: u64,
    pub changed: u64,
    pub allocation: u64,
    pub size: u64,
    pub attributes: u32,
}

impl FileTimes {
    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.created.to_le_bytes());
        out.extend_from_slice(&self.accessed.to_le_bytes());
        out.extend_from_slice(&self.written.to_le_bytes());
        out.extend_from_slice(&self.changed.to_le_bytes());
        out.extend_from_slice(&self.allocation.to_le_bytes());
        out.extend_from_slice(&self.size.to_le_bytes());
        out.extend_from_slice(&self.attributes.to_le_bytes());
    }
}

pub fn create_body(action: u32, times: &FileTimes, file_id: FileId, contexts: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(96 + contexts.len());
    out.extend_from_slice(&89u16.to_le_bytes());
    out.push(0);
    out.push(0);
    out.extend_from_slice(&action.to_le_bytes());
    times.write(&mut out);
    out.extend_from_slice(&0u32.to_le_bytes());
    file_id.write(&mut out);
    if contexts.is_empty() {
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.push(0);
    } else {
        let offset = (HEADER_SIZE + 88) as u32;
        out.extend_from_slice(&offset.to_le_bytes());
        out.extend_from_slice(&(contexts.len() as u32).to_le_bytes());
        out.extend_from_slice(contexts);
    }
    out
}

#[derive(Clone, Copy, Debug)]
pub struct CloseRequest {
    pub flags: u16,
    pub file_id: FileId,
}

pub const CLOSE_POSTQUERY: u16 = 0x0001;

pub fn parse_close(message: &[u8]) -> Result<CloseRequest, WireError> {
    let mut reader = body(message, 24)?;
    let flags = reader.u16()?;
    reader.skip(4)?;
    let file_id = FileId::read(&mut reader)?;
    Ok(CloseRequest { flags, file_id })
}

pub fn close_body(times: Option<&FileTimes>) -> Vec<u8> {
    let mut out = Vec::with_capacity(60);
    out.extend_from_slice(&60u16.to_le_bytes());
    let flags = if times.is_some() { CLOSE_POSTQUERY } else { 0 };
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    times.copied().unwrap_or_default().write(&mut out);
    out
}

pub fn parse_flush(message: &[u8]) -> Result<FileId, WireError> {
    let mut reader = body(message, 24)?;
    reader.skip(6)?;
    FileId::read(&mut reader)
}

#[derive(Clone, Copy, Debug)]
pub struct ReadRequest {
    pub length: u32,
    pub offset: u64,
    pub file_id: FileId,
    pub minimum: u32,
    pub channel: u32,
}

pub fn parse_read(message: &[u8]) -> Result<ReadRequest, WireError> {
    let mut reader = body(message, 49)?;
    reader.skip(2)?;
    let length = reader.u32()?;
    let offset = reader.u64()?;
    let file_id = FileId::read(&mut reader)?;
    let minimum = reader.u32()?;
    let channel = reader.u32()?;
    Ok(ReadRequest {
        length,
        offset,
        file_id,
        minimum,
        channel,
    })
}

pub fn read_body(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + data.len());
    out.extend_from_slice(&17u16.to_le_bytes());
    out.push((HEADER_SIZE + 16) as u8);
    out.push(0);
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    if data.is_empty() {
        out.push(0);
    } else {
        out.extend_from_slice(data);
    }
    out
}

pub const WRITE_THROUGH: u32 = 0x0000_0001;

#[derive(Clone, Debug)]
pub struct WriteRequest {
    pub offset: u64,
    pub file_id: FileId,
    pub channel: u32,
    pub flags: u32,
    pub data: Vec<u8>,
}

pub fn parse_write(message: &[u8]) -> Result<WriteRequest, WireError> {
    let mut reader = body(message, 49)?;
    let data_offset = reader.u16()? as usize;
    let length = reader.u32()? as usize;
    let offset = reader.u64()?;
    let file_id = FileId::read(&mut reader)?;
    let channel = reader.u32()?;
    reader.skip(4 + 2 + 2)?;
    let flags = reader.u32()?;
    let data = buffer_at(message, data_offset, length)?.to_vec();
    Ok(WriteRequest {
        offset,
        file_id,
        channel,
        flags,
        data,
    })
}

pub fn write_body(count: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(17);
    out.extend_from_slice(&17u16.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.push(0);
    out
}

pub const LOCK_SHARED: u32 = 0x0000_0001;
pub const LOCK_EXCLUSIVE: u32 = 0x0000_0002;
pub const LOCK_UNLOCK: u32 = 0x0000_0004;
pub const LOCK_IMMEDIATE: u32 = 0x0000_0010;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LockElement {
    pub offset: u64,
    pub length: u64,
    pub flags: u32,
}

#[derive(Clone, Debug)]
pub struct LockRequest {
    pub file_id: FileId,
    pub locks: Vec<LockElement>,
}

pub fn parse_lock(message: &[u8]) -> Result<LockRequest, WireError> {
    let mut reader = body(message, 48)?;
    let count = reader.u16()?;
    reader.skip(4)?;
    let file_id = FileId::read(&mut reader)?;
    if count == 0 {
        return Err(WireError::Malformed("no lock elements"));
    }
    let mut locks = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let offset = reader.u64()?;
        let length = reader.u64()?;
        let flags = reader.u32()?;
        reader.skip(4)?;
        locks.push(LockElement {
            offset,
            length,
            flags,
        });
    }
    Ok(LockRequest { file_id, locks })
}

pub fn lock_body() -> Vec<u8> {
    let mut out = 4u16.to_le_bytes().to_vec();
    out.extend_from_slice(&0u16.to_le_bytes());
    out
}

pub const IOCTL_FSCTL: u32 = 0x0000_0001;

#[derive(Clone, Debug)]
pub struct IoctlRequest {
    pub control: u32,
    pub file_id: FileId,
    pub input: Vec<u8>,
    pub output_limit: u32,
    pub flags: u32,
}

pub fn parse_ioctl(message: &[u8]) -> Result<IoctlRequest, WireError> {
    let mut reader = body(message, 57)?;
    reader.skip(2)?;
    let control = reader.u32()?;
    let file_id = FileId::read(&mut reader)?;
    let input_offset = reader.u32()? as usize;
    let input_count = reader.u32()? as usize;
    reader.skip(4 + 4 + 4)?;
    let output_limit = reader.u32()?;
    let flags = reader.u32()?;
    let input = buffer_at(message, input_offset, input_count)?.to_vec();
    Ok(IoctlRequest {
        control,
        file_id,
        input,
        output_limit,
        flags,
    })
}

pub fn ioctl_body(control: u32, file_id: FileId, output: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(48 + output.len());
    out.extend_from_slice(&49u16.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&control.to_le_bytes());
    file_id.write(&mut out);
    let offset = (HEADER_SIZE + 48) as u32;
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&(output.len() as u32).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(output);
    if output.is_empty() {
        out.push(0);
    }
    out
}

pub const RESTART_SCANS: u8 = 0x01;
pub const SINGLE_ENTRY: u8 = 0x02;
pub const INDEX_SPECIFIED: u8 = 0x04;
pub const REOPEN: u8 = 0x10;

#[derive(Clone, Debug)]
pub struct DirectoryRequest {
    pub class: u8,
    pub flags: u8,
    pub file_id: FileId,
    pub pattern: Vec<u8>,
    pub output_limit: u32,
}

pub fn parse_directory(message: &[u8]) -> Result<DirectoryRequest, WireError> {
    let mut reader = body(message, 33)?;
    let class = reader.u8()?;
    let flags = reader.u8()?;
    reader.skip(4)?;
    let file_id = FileId::read(&mut reader)?;
    let offset = reader.u16()? as usize;
    let length = reader.u16()? as usize;
    let output_limit = reader.u32()?;
    let pattern = buffer_at(message, offset, length)?.to_vec();
    Ok(DirectoryRequest {
        class,
        flags,
        file_id,
        pattern,
        output_limit,
    })
}

/// The QUERY_DIRECTORY and QUERY_INFO response body: an offset, a length and the data.
pub fn output_body(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + data.len().max(1));
    out.extend_from_slice(&9u16.to_le_bytes());
    let offset = if data.is_empty() {
        0
    } else {
        (HEADER_SIZE + 8) as u16
    };
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    if data.is_empty() {
        out.push(0);
    } else {
        out.extend_from_slice(data);
    }
    out
}

#[derive(Clone, Debug)]
pub struct QueryRequest {
    pub info_type: u8,
    pub class: u8,
    pub output_limit: u32,
    pub additional: u32,
    pub flags: u32,
    pub file_id: FileId,
}

pub fn parse_query(message: &[u8]) -> Result<QueryRequest, WireError> {
    let mut reader = body(message, 41)?;
    let info_type = reader.u8()?;
    let class = reader.u8()?;
    let output_limit = reader.u32()?;
    reader.skip(2 + 2 + 4)?;
    let additional = reader.u32()?;
    let flags = reader.u32()?;
    let file_id = FileId::read(&mut reader)?;
    Ok(QueryRequest {
        info_type,
        class,
        output_limit,
        additional,
        flags,
        file_id,
    })
}

#[derive(Clone, Debug)]
pub struct SetRequest {
    pub info_type: u8,
    pub class: u8,
    pub additional: u32,
    pub file_id: FileId,
    pub buffer: Vec<u8>,
}

pub fn parse_set(message: &[u8]) -> Result<SetRequest, WireError> {
    let mut reader = body(message, 33)?;
    let info_type = reader.u8()?;
    let class = reader.u8()?;
    let length = reader.u32()? as usize;
    let offset = reader.u16()? as usize;
    reader.skip(2)?;
    let additional = reader.u32()?;
    let file_id = FileId::read(&mut reader)?;
    let buffer = buffer_at(message, offset, length)?.to_vec();
    Ok(SetRequest {
        info_type,
        class,
        additional,
        file_id,
        buffer,
    })
}

pub fn set_body() -> Vec<u8> {
    2u16.to_le_bytes().to_vec()
}
