//! Access masks: generic right expansion, the grant computed at CREATE from live Aruna
//! rights, and the per-command and per-information-class checks against that grant.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::info::{class, security, volume};
use crate::status;

pub const FILE_READ_DATA: u32 = 0x0000_0001;
pub const FILE_LIST_DIRECTORY: u32 = 0x0000_0001;
pub const FILE_WRITE_DATA: u32 = 0x0000_0002;
pub const FILE_ADD_FILE: u32 = 0x0000_0002;
pub const FILE_APPEND_DATA: u32 = 0x0000_0004;
pub const FILE_ADD_SUBDIRECTORY: u32 = 0x0000_0004;
pub const FILE_READ_EA: u32 = 0x0000_0008;
pub const FILE_WRITE_EA: u32 = 0x0000_0010;
pub const FILE_EXECUTE: u32 = 0x0000_0020;
pub const FILE_DELETE_CHILD: u32 = 0x0000_0040;
pub const FILE_READ_ATTRIBUTES: u32 = 0x0000_0080;
pub const FILE_WRITE_ATTRIBUTES: u32 = 0x0000_0100;
pub const DELETE: u32 = 0x0001_0000;
pub const READ_CONTROL: u32 = 0x0002_0000;
pub const WRITE_DAC: u32 = 0x0004_0000;
pub const WRITE_OWNER: u32 = 0x0008_0000;
pub const SYNCHRONIZE: u32 = 0x0010_0000;
pub const ACCESS_SYSTEM_SECURITY: u32 = 0x0100_0000;
pub const MAXIMUM_ALLOWED: u32 = 0x0200_0000;
pub const GENERIC_ALL: u32 = 0x1000_0000;
pub const GENERIC_EXECUTE: u32 = 0x2000_0000;
pub const GENERIC_WRITE: u32 = 0x4000_0000;
pub const GENERIC_READ: u32 = 0x8000_0000;
/// FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_GENERIC_EXECUTE and FILE_ALL_ACCESS.
pub const READ_MAPPING: u32 = 0x0012_0089;
pub const WRITE_MAPPING: u32 = 0x0012_0116;
pub const EXECUTE_MAPPING: u32 = 0x0012_00A0;
pub const ALL_ACCESS: u32 = 0x001F_01FF;
const VALID_MASK: u32 = 0xF31F_01FF;
const GENERIC_BITS: u32 = GENERIC_ALL | GENERIC_EXECUTE | GENERIC_WRITE | GENERIC_READ;

/// Live Aruna rights on one resolved object, evaluated by the backend for this request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rights {
    pub read: bool,
    pub write: bool,
    pub delete: bool,
}

/// How a WRITE may change a file: anywhere, or only at or beyond the shared end of file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    Overwrite,
    Append,
}

/// Replaces the GENERIC_* bits with the file rights they map to.
pub fn expand_generic(mask: u32) -> u32 {
    let mut out = mask & !GENERIC_BITS;
    if mask & GENERIC_READ != 0 {
        out |= READ_MAPPING;
    }
    if mask & GENERIC_WRITE != 0 {
        out |= WRITE_MAPPING;
    }
    if mask & GENERIC_EXECUTE != 0 {
        out |= EXECUTE_MAPPING;
    }
    if mask & GENERIC_ALL != 0 {
        out |= ALL_ACCESS;
    }
    out
}

/// The most access live rights allow; a read-only share never allows a change.
pub fn allowed_mask(rights: Rights, read_only: bool) -> u32 {
    let mut mask = SYNCHRONIZE;
    if rights.read {
        mask |= FILE_READ_DATA | FILE_READ_ATTRIBUTES | FILE_READ_EA | FILE_EXECUTE | READ_CONTROL;
    }
    if rights.write && !read_only {
        mask |= FILE_WRITE_DATA | FILE_APPEND_DATA | FILE_WRITE_ATTRIBUTES | FILE_WRITE_EA;
    }
    if rights.delete && !read_only {
        mask |= DELETE | FILE_DELETE_CHILD;
    }
    mask
}

/// The access granted for a desired mask: explicit rights must all be allowed, and
/// MAXIMUM_ALLOWED adds every allowed right. Errors carry the NTSTATUS to return.
pub fn grant(desired: u32, allowed: u32) -> Result<u32, u32> {
    if desired & !VALID_MASK != 0 {
        return Err(status::ACCESS_DENIED);
    }
    let expanded = expand_generic(desired);
    let maximum = expanded & MAXIMUM_ALLOWED != 0;
    let explicit = expanded & !MAXIMUM_ALLOWED;
    if explicit & ACCESS_SYSTEM_SECURITY != 0 {
        return Err(status::PRIVILEGE_NOT_HELD);
    }
    if explicit & !allowed != 0 {
        return Err(status::ACCESS_DENIED);
    }
    Ok(if maximum {
        allowed | explicit
    } else {
        explicit
    })
}

pub fn require_all(granted: u32, needed: u32) -> Result<(), u32> {
    if granted & needed == needed {
        Ok(())
    } else {
        Err(status::ACCESS_DENIED)
    }
}

pub fn require_any(granted: u32, needed: u32) -> Result<(), u32> {
    if granted & needed != 0 {
        Ok(())
    } else {
        Err(status::ACCESS_DENIED)
    }
}

pub fn check_read(granted: u32) -> Result<(), u32> {
    require_all(granted, FILE_READ_DATA)
}

pub fn check_list(granted: u32) -> Result<(), u32> {
    require_all(granted, FILE_LIST_DIRECTORY)
}

pub fn check_flush(granted: u32, directory: bool) -> Result<(), u32> {
    if directory {
        require_any(granted, FILE_ADD_FILE | FILE_ADD_SUBDIRECTORY)
    } else {
        require_any(granted, FILE_WRITE_DATA | FILE_APPEND_DATA)
    }
}

/// The offset that MS-FSA reads as "write at the current end of file".
pub const END_OFFSET: u64 = u64::MAX;

/// Classifies a WRITE before any byte moves: rights, append-only mode and range overflow.
/// Returns the mode and the offset, `None` meaning the shared end of file.
pub fn check_write(
    granted: u32,
    offset: u64,
    length: usize,
) -> Result<(WriteMode, Option<u64>), u32> {
    let write = granted & FILE_WRITE_DATA != 0;
    let append = granted & FILE_APPEND_DATA != 0;
    if !write && !append {
        return Err(status::ACCESS_DENIED);
    }
    let mode = if write {
        WriteMode::Overwrite
    } else {
        WriteMode::Append
    };
    if offset == END_OFFSET {
        return Ok((mode, None));
    }
    let end = offset
        .checked_add(length as u64)
        .ok_or(status::INVALID_PARAMETER)?;
    if end > i64::MAX as u64 {
        return Err(status::INVALID_PARAMETER);
    }
    Ok((mode, Some(offset)))
}

/// Whether an append-only open may write at `offset` given the shared end of file. The
/// backend calls this under the same serialization as the write itself.
pub fn append_allowed(offset: u64, end_of_file: u64) -> bool {
    offset >= end_of_file
}

/// The rights a security information query needs for `additional`.
pub fn security_need(additional: u32) -> u32 {
    let mut need = 0;
    if additional
        & (security::OWNER
            | security::GROUP
            | security::DACL
            | security::LABEL
            | security::ATTRIBUTE
            | security::SCOPE)
        != 0
    {
        need |= READ_CONTROL;
    }
    if additional & security::SACL != 0 {
        need |= ACCESS_SYSTEM_SECURITY;
    }
    if additional & security::BACKUP != 0 {
        need |= READ_CONTROL | ACCESS_SYSTEM_SECURITY;
    }
    need
}

pub const INFO_FILE: u8 = 0x01;
pub const INFO_FILESYSTEM: u8 = 0x02;
pub const INFO_SECURITY: u8 = 0x03;
pub const INFO_QUOTA: u8 = 0x04;

/// The rights a QUERY_INFO request needs, or the status for a class the server rejects.
pub fn query_need(info_type: u8, file_class: u8, additional: u32) -> Result<u32, u32> {
    match info_type {
        INFO_FILE => match file_class {
            class::BASIC | class::ALL | class::NETWORK_OPEN | class::ATTRIBUTE_TAG => {
                Ok(FILE_READ_ATTRIBUTES)
            }
            class::FULL_EA => Ok(FILE_READ_EA),
            class::STANDARD
            | class::INTERNAL
            | class::EA
            | class::ACCESS
            | class::POSITION
            | class::MODE
            | class::ALIGNMENT
            | class::STREAM => Ok(0),
            class::NAME
            | class::COMPRESSION
            | class::NORMALIZED_NAME
            | class::ID_INFORMATION
            | class::REPARSE_TAG => Err(status::NOT_SUPPORTED),
            _ => Err(status::INVALID_INFO_CLASS),
        },
        INFO_FILESYSTEM => match file_class {
            volume::VOLUME
            | volume::SIZE
            | volume::DEVICE
            | volume::ATTRIBUTE
            | volume::FULL_SIZE
            | volume::SECTOR_SIZE => Ok(0),
            volume::CONTROL | volume::OBJECT_ID => Err(status::NOT_SUPPORTED),
            _ => Err(status::INVALID_INFO_CLASS),
        },
        INFO_SECURITY => Ok(security_need(additional)),
        INFO_QUOTA => Err(status::NOT_SUPPORTED),
        _ => Err(status::INVALID_PARAMETER),
    }
}

/// The rights a SET_INFO request needs, or the status for a class the server rejects.
pub fn set_need(info_type: u8, file_class: u8, additional: u32) -> Result<u32, u32> {
    match info_type {
        INFO_FILE => match file_class {
            class::BASIC => Ok(FILE_WRITE_ATTRIBUTES),
            class::ALLOCATION | class::END_OF_FILE => Ok(FILE_WRITE_DATA),
            class::RENAME | class::DISPOSITION | class::DISPOSITION_EX => Ok(DELETE),
            class::LINK
            | class::POSITION
            | class::MODE
            | class::FULL_EA
            | class::SHORT_NAME
            | class::VALID_LENGTH => Err(status::NOT_SUPPORTED),
            _ => Err(status::INVALID_INFO_CLASS),
        },
        INFO_SECURITY => {
            let mut need = 0;
            if additional & (security::OWNER | security::GROUP) != 0 {
                need |= WRITE_OWNER;
            }
            if additional & security::DACL != 0 {
                need |= WRITE_DAC;
            }
            if additional & security::SACL != 0 {
                need |= ACCESS_SYSTEM_SECURITY;
            }
            Ok(need)
        }
        INFO_FILESYSTEM | INFO_QUOTA => Err(status::NOT_SUPPORTED),
        _ => Err(status::INVALID_PARAMETER),
    }
}

#[cfg(test)]
#[path = "access_tests.rs"]
mod tests;
