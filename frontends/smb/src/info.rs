//! MS-FSCC information encoders: file, directory, file system and security classes, file
//! attributes, FILETIME conversion and directory pattern matching.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::backend::{FileInfo, ObjectKind};
use crate::wire::utf16_bytes;

/// File and directory information classes, named without the `File` prefix.
pub mod class {
    pub const DIRECTORY: u8 = 0x01;
    pub const FULL_DIRECTORY: u8 = 0x02;
    pub const BOTH_DIRECTORY: u8 = 0x03;
    pub const BASIC: u8 = 0x04;
    pub const STANDARD: u8 = 0x05;
    pub const INTERNAL: u8 = 0x06;
    pub const EA: u8 = 0x07;
    pub const ACCESS: u8 = 0x08;
    pub const NAME: u8 = 0x09;
    pub const RENAME: u8 = 0x0A;
    pub const LINK: u8 = 0x0B;
    pub const NAMES: u8 = 0x0C;
    pub const DISPOSITION: u8 = 0x0D;
    pub const POSITION: u8 = 0x0E;
    pub const FULL_EA: u8 = 0x0F;
    pub const MODE: u8 = 0x10;
    pub const ALIGNMENT: u8 = 0x11;
    pub const ALL: u8 = 0x12;
    pub const ALLOCATION: u8 = 0x13;
    pub const END_OF_FILE: u8 = 0x14;
    pub const STREAM: u8 = 0x16;
    pub const COMPRESSION: u8 = 0x1C;
    pub const REPARSE_TAG: u8 = 0x21;
    pub const NETWORK_OPEN: u8 = 0x22;
    pub const ATTRIBUTE_TAG: u8 = 0x23;
    pub const ID_BOTH_DIRECTORY: u8 = 0x25;
    pub const ID_FULL_DIRECTORY: u8 = 0x26;
    pub const VALID_LENGTH: u8 = 0x27;
    pub const SHORT_NAME: u8 = 0x28;
    pub const NORMALIZED_NAME: u8 = 0x30;
    pub const ID_INFORMATION: u8 = 0x3B;
    pub const DISPOSITION_EX: u8 = 0x40;
}

/// File system information classes, named without the `FileFs` prefix.
pub mod volume {
    pub const VOLUME: u8 = 0x01;
    pub const SIZE: u8 = 0x03;
    pub const DEVICE: u8 = 0x04;
    pub const ATTRIBUTE: u8 = 0x05;
    pub const CONTROL: u8 = 0x06;
    pub const FULL_SIZE: u8 = 0x07;
    pub const OBJECT_ID: u8 = 0x08;
    pub const SECTOR_SIZE: u8 = 0x0B;
}

/// Security information bits of AdditionalInformation.
pub mod security {
    pub const OWNER: u32 = 0x0000_0001;
    pub const GROUP: u32 = 0x0000_0002;
    pub const DACL: u32 = 0x0000_0004;
    pub const SACL: u32 = 0x0000_0008;
    pub const LABEL: u32 = 0x0000_0010;
    pub const ATTRIBUTE: u32 = 0x0000_0020;
    pub const SCOPE: u32 = 0x0000_0040;
    pub const BACKUP: u32 = 0x0001_0000;
}

pub const ATTRIBUTE_READONLY: u32 = 0x0000_0001;
pub const ATTRIBUTE_DIRECTORY: u32 = 0x0000_0010;
pub const ATTRIBUTE_ARCHIVE: u32 = 0x0000_0020;
const SECTOR_BYTES: u32 = 512;
const UNIT_SECTORS: u32 = 8;
const UNIT_BYTES: u64 = (SECTOR_BYTES * UNIT_SECTORS) as u64;
/// 100-nanosecond intervals between 1601-01-01 and the Unix epoch.
const EPOCH_OFFSET: u64 = 116_444_736_000_000_000;
const CASE_PRESERVED: u32 = 0x0000_0002;
const UNICODE_DISK: u32 = 0x0000_0004;
const READONLY_VOLUME: u32 = 0x0008_0000;
const DEVICE_DISK: u32 = 0x0000_0007;
const WORLD_SID: [u8; 12] = [1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0];

/// FILETIME for a Unix time in milliseconds.
pub fn filetime(unix_millis: u64) -> u64 {
    unix_millis
        .saturating_mul(10_000)
        .saturating_add(EPOCH_OFFSET)
}

/// Attributes reported for an object; read-only shares mark files read-only.
pub fn attributes(info: &FileInfo, read_only: bool) -> u32 {
    match info.kind {
        ObjectKind::Directory => ATTRIBUTE_DIRECTORY,
        ObjectKind::File if read_only => ATTRIBUTE_ARCHIVE | ATTRIBUTE_READONLY,
        ObjectKind::File => ATTRIBUTE_ARCHIVE,
    }
}

fn put_times(out: &mut Vec<u8>, info: &FileInfo) {
    out.extend_from_slice(&info.created.to_le_bytes());
    out.extend_from_slice(&info.accessed.to_le_bytes());
    out.extend_from_slice(&info.written.to_le_bytes());
    out.extend_from_slice(&info.changed.to_le_bytes());
}

pub fn basic(info: &FileInfo, attributes: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(40);
    put_times(&mut out, info);
    out.extend_from_slice(&attributes.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out
}

pub fn standard(info: &FileInfo) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&info.allocation.to_le_bytes());
    out.extend_from_slice(&info.size.to_le_bytes());
    out.extend_from_slice(&1u32.to_le_bytes());
    out.push(u8::from(info.delete_pending));
    out.push(u8::from(info.kind == ObjectKind::Directory));
    out.extend_from_slice(&0u16.to_le_bytes());
    out
}

/// FileInternalInformation: V1 has no filesystem-wide file ID, so IndexNumber is zero.
pub fn internal() -> Vec<u8> {
    0u64.to_le_bytes().to_vec()
}

/// FileEaInformation: no extended attributes, size zero.
pub fn extended() -> Vec<u8> {
    0u32.to_le_bytes().to_vec()
}

pub fn access(granted: u32) -> Vec<u8> {
    granted.to_le_bytes().to_vec()
}

pub fn all(info: &FileInfo, attributes: u32, granted: u32, name: &str) -> Vec<u8> {
    let name = utf16_bytes(name);
    let mut out = Vec::with_capacity(104 + name.len());
    out.extend_from_slice(&basic(info, attributes));
    out.extend_from_slice(&standard(info));
    out.extend_from_slice(&internal());
    out.extend_from_slice(&extended());
    out.extend_from_slice(&access(granted));
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&(name.len() as u32).to_le_bytes());
    out.extend_from_slice(&name);
    out
}

pub fn network_open(info: &FileInfo, attributes: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(56);
    put_times(&mut out, info);
    out.extend_from_slice(&info.allocation.to_le_bytes());
    out.extend_from_slice(&info.size.to_le_bytes());
    out.extend_from_slice(&attributes.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out
}

pub fn attribute_tag(attributes: u32) -> Vec<u8> {
    let mut out = attributes.to_le_bytes().to_vec();
    out.extend_from_slice(&0u32.to_le_bytes());
    out
}

/// FileStreamInformation: files have only the unnamed data stream, directories none.
pub fn stream(info: &FileInfo) -> Vec<u8> {
    if info.kind == ObjectKind::Directory {
        return Vec::new();
    }
    let name = utf16_bytes("::$DATA");
    let mut out = Vec::with_capacity(24 + name.len());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&(name.len() as u32).to_le_bytes());
    out.extend_from_slice(&info.size.to_le_bytes());
    out.extend_from_slice(&info.allocation.to_le_bytes());
    out.extend_from_slice(&name);
    out
}

/// Whether a QUERY_INFO class has a fixed size, so a short buffer is a length mismatch
/// instead of a truncated overflow.
pub fn fixed_size(file_class: u8) -> bool {
    !matches!(file_class, class::ALL | class::STREAM)
}

/// One directory entry in `file_class`, with FileIndex and FileId zero, or `None` for a
/// class the server does not encode. NextEntryOffset is filled in when packing.
pub fn directory_entry(
    file_class: u8,
    name: &str,
    info: &FileInfo,
    attributes: u32,
) -> Option<Vec<u8>> {
    let name = utf16_bytes(name);
    let mut out = Vec::with_capacity(104 + name.len());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    if file_class == class::NAMES {
        out.extend_from_slice(&(name.len() as u32).to_le_bytes());
        out.extend_from_slice(&name);
        return Some(out);
    }
    if !matches!(
        file_class,
        class::DIRECTORY
            | class::FULL_DIRECTORY
            | class::BOTH_DIRECTORY
            | class::ID_BOTH_DIRECTORY
            | class::ID_FULL_DIRECTORY
    ) {
        return None;
    }
    put_times(&mut out, info);
    out.extend_from_slice(&info.size.to_le_bytes());
    out.extend_from_slice(&info.allocation.to_le_bytes());
    out.extend_from_slice(&attributes.to_le_bytes());
    out.extend_from_slice(&(name.len() as u32).to_le_bytes());
    if file_class != class::DIRECTORY {
        out.extend_from_slice(&0u32.to_le_bytes());
    }
    match file_class {
        class::BOTH_DIRECTORY => {
            out.push(0);
            out.push(0);
            out.extend_from_slice(&[0u8; 24]);
        }
        class::ID_BOTH_DIRECTORY => {
            out.push(0);
            out.push(0);
            out.extend_from_slice(&[0u8; 24]);
            out.extend_from_slice(&0u16.to_le_bytes());
            out.extend_from_slice(&0u64.to_le_bytes());
        }
        class::ID_FULL_DIRECTORY => {
            out.extend_from_slice(&0u32.to_le_bytes());
            out.extend_from_slice(&0u64.to_le_bytes());
        }
        _ => {}
    }
    out.extend_from_slice(&name);
    Some(out)
}

/// Appends one directory entry to `buffer`, linking the previous entry at `last`, unless
/// the result would exceed `limit`. Returns the new entry's offset when it fits.
pub fn append_entry(
    buffer: &mut Vec<u8>,
    last: Option<usize>,
    entry: &[u8],
    limit: usize,
) -> Option<usize> {
    let start = if last.is_some() {
        buffer.len().next_multiple_of(8)
    } else {
        buffer.len()
    };
    if start + entry.len() > limit {
        return None;
    }
    buffer.resize(start, 0);
    if let Some(previous) = last {
        let next = (start - previous) as u32;
        buffer[previous..previous + 4].copy_from_slice(&next.to_le_bytes());
    }
    buffer.extend_from_slice(entry);
    Some(start)
}

/// Volume details reported by file system queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VolumeFacts {
    pub capacity: u64,
    pub available: u64,
    pub read_only: bool,
    pub serial: u32,
}

/// One file system information class, or `None` when unsupported.
pub fn filesystem(file_class: u8, facts: &VolumeFacts) -> Option<Vec<u8>> {
    let total = facts.capacity / UNIT_BYTES;
    let free = facts.available.min(facts.capacity) / UNIT_BYTES;
    let mut out = Vec::new();
    match file_class {
        volume::VOLUME => {
            let label = utf16_bytes("aruna");
            out.extend_from_slice(&0u64.to_le_bytes());
            out.extend_from_slice(&facts.serial.to_le_bytes());
            out.extend_from_slice(&(label.len() as u32).to_le_bytes());
            out.push(0);
            out.push(0);
            out.extend_from_slice(&label);
        }
        volume::SIZE => {
            out.extend_from_slice(&total.to_le_bytes());
            out.extend_from_slice(&free.to_le_bytes());
            out.extend_from_slice(&UNIT_SECTORS.to_le_bytes());
            out.extend_from_slice(&SECTOR_BYTES.to_le_bytes());
        }
        volume::DEVICE => {
            out.extend_from_slice(&DEVICE_DISK.to_le_bytes());
            out.extend_from_slice(&0u32.to_le_bytes());
        }
        volume::ATTRIBUTE => {
            let name = utf16_bytes("NTFS");
            let mut flags = CASE_PRESERVED | UNICODE_DISK;
            if facts.read_only {
                flags |= READONLY_VOLUME;
            }
            out.extend_from_slice(&flags.to_le_bytes());
            out.extend_from_slice(&255u32.to_le_bytes());
            out.extend_from_slice(&(name.len() as u32).to_le_bytes());
            out.extend_from_slice(&name);
        }
        volume::FULL_SIZE => {
            out.extend_from_slice(&total.to_le_bytes());
            out.extend_from_slice(&free.to_le_bytes());
            out.extend_from_slice(&free.to_le_bytes());
            out.extend_from_slice(&UNIT_SECTORS.to_le_bytes());
            out.extend_from_slice(&SECTOR_BYTES.to_le_bytes());
        }
        volume::SECTOR_SIZE => {
            for _ in 0..4 {
                out.extend_from_slice(&SECTOR_BYTES.to_le_bytes());
            }
            out.extend_from_slice(&0u32.to_le_bytes());
            out.extend_from_slice(&0u32.to_le_bytes());
            out.extend_from_slice(&0u32.to_le_bytes());
        }
        _ => return None,
    }
    Some(out)
}

/// A self-relative security descriptor with the requested parts: owner and group are
/// Everyone and the DACL grants Everyone `allowed`. SACLs are never returned.
pub fn security_descriptor(additional: u32, allowed: u32) -> Vec<u8> {
    let owner = additional & (security::OWNER | security::BACKUP) != 0;
    let group = additional & (security::GROUP | security::BACKUP) != 0;
    let dacl = additional & (security::DACL | security::BACKUP) != 0;
    let mut control: u16 = 0x8000;
    let mut tail = Vec::new();
    let mut offsets = [0u32; 4];
    if owner {
        offsets[0] = 20 + tail.len() as u32;
        tail.extend_from_slice(&WORLD_SID);
    }
    if group {
        offsets[1] = 20 + tail.len() as u32;
        tail.extend_from_slice(&WORLD_SID);
    }
    if dacl {
        control |= 0x0004;
        offsets[3] = 20 + tail.len() as u32;
        let ace_size = (8 + WORLD_SID.len()) as u16;
        let acl_size = 8 + ace_size;
        tail.push(2);
        tail.push(0);
        tail.extend_from_slice(&acl_size.to_le_bytes());
        tail.extend_from_slice(&1u16.to_le_bytes());
        tail.extend_from_slice(&0u16.to_le_bytes());
        tail.push(0);
        tail.push(0);
        tail.extend_from_slice(&ace_size.to_le_bytes());
        tail.extend_from_slice(&allowed.to_le_bytes());
        tail.extend_from_slice(&WORLD_SID);
    }
    let mut out = Vec::with_capacity(20 + tail.len());
    out.push(1);
    out.push(0);
    out.extend_from_slice(&control.to_le_bytes());
    for offset in offsets {
        out.extend_from_slice(&offset.to_le_bytes());
    }
    out.extend_from_slice(&tail);
    out
}

/// Case-insensitive match of a directory name against a pattern with `*`, `?` and the DOS
/// wildcards `<`, `>` and `"`, which match like `*`, `?` and `.` here.
pub fn pattern_matches(pattern: &str, name: &str) -> bool {
    let pattern: Vec<char> = pattern
        .chars()
        .map(|character| match character {
            '<' => '*',
            '>' => '?',
            '"' => '.',
            other => other,
        })
        .flat_map(char::to_uppercase)
        .collect();
    let name: Vec<char> = name.chars().flat_map(char::to_uppercase).collect();
    let (mut position, mut index) = (0usize, 0usize);
    let mut backtrack: Option<(usize, usize)> = None;
    while index < name.len() {
        if position < pattern.len()
            && (pattern[position] == '?' || pattern[position] == name[index])
        {
            position += 1;
            index += 1;
        } else if position < pattern.len() && pattern[position] == '*' {
            backtrack = Some((position, index));
            position += 1;
        } else if let Some((star, matched)) = backtrack {
            position = star + 1;
            index = matched + 1;
            backtrack = Some((star, matched + 1));
        } else {
            return false;
        }
    }
    pattern[position..]
        .iter()
        .all(|character| *character == '*')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(kind: ObjectKind) -> FileInfo {
        FileInfo {
            kind,
            size: 10,
            allocation: 4096,
            created: 1,
            accessed: 2,
            written: 3,
            changed: 4,
            delete_pending: false,
        }
    }

    #[test]
    fn directory_ids_zero() {
        let info = sample(ObjectKind::File);
        let sizes = [
            (class::DIRECTORY, 64),
            (class::FULL_DIRECTORY, 68),
            (class::BOTH_DIRECTORY, 94),
            (class::ID_BOTH_DIRECTORY, 104),
            (class::ID_FULL_DIRECTORY, 80),
            (class::NAMES, 12),
        ];
        for (file_class, fixed) in sizes {
            let entry = directory_entry(file_class, "ab", &info, ATTRIBUTE_ARCHIVE)
                .expect("supported class");
            assert_eq!(entry.len(), fixed + 4, "class {file_class:#x}");
            assert_eq!(&entry[4..8], &[0u8; 4], "FileIndex is zero");
        }
        let both = directory_entry(class::ID_BOTH_DIRECTORY, "ab", &info, 0).expect("supported");
        assert_eq!(&both[96..104], &[0u8; 8], "FileId is zero");
        let full = directory_entry(class::ID_FULL_DIRECTORY, "ab", &info, 0).expect("supported");
        assert_eq!(&full[72..80], &[0u8; 8], "FileId is zero");
        assert_eq!(
            &full[40..48],
            &10u64.to_le_bytes(),
            "EndOfFile precedes AllocationSize"
        );
        assert!(directory_entry(0x3C, "ab", &info, 0).is_none());
    }

    #[test]
    fn internal_index_zero() {
        assert_eq!(internal(), vec![0u8; 8]);
        let info = sample(ObjectKind::File);
        let encoded = all(&info, ATTRIBUTE_ARCHIVE, 0x1234, "x");
        assert_eq!(&encoded[64..72], &[0u8; 8]);
        assert_eq!(&encoded[72..76], &[0u8; 4], "EaSize is zero");
        assert_eq!(&encoded[76..80], &0x1234u32.to_le_bytes());
        assert_eq!(encoded.len(), 100 + 2);
    }

    #[test]
    fn packs_entries_aligned() {
        let mut buffer = Vec::new();
        let first = append_entry(&mut buffer, None, &[1u8; 13], 64).expect("fits");
        let second = append_entry(&mut buffer, Some(first), &[2u8; 13], 64).expect("fits");
        assert_eq!(second, 16);
        assert_eq!(&buffer[0..4], &16u32.to_le_bytes());
        assert!(append_entry(&mut buffer, Some(second), &[3u8; 40], 64).is_none());
        assert_eq!(buffer.len(), 29);
    }

    #[test]
    fn matches_wildcards() {
        assert!(pattern_matches("*", "anything"));
        assert!(pattern_matches("*.TXT", "notes.txt"));
        assert!(pattern_matches("n?tes.*", "Notes.md"));
        assert!(pattern_matches("<.txt", "a.b.txt"));
        assert!(pattern_matches("exact", "EXACT"));
        assert!(!pattern_matches("*.txt", "notes.md"));
        assert!(!pattern_matches("a?", "a"));
        assert!(pattern_matches("a*b*c", "aXXbYYc"));
    }

    #[test]
    fn descriptor_requested_parts() {
        let owner_only = security_descriptor(security::OWNER, 0);
        assert_eq!(owner_only.len(), 32);
        assert_eq!(&owner_only[4..8], &20u32.to_le_bytes());
        let with_dacl = security_descriptor(security::DACL, 0x0012_0089);
        assert_eq!(u16::from_le_bytes([with_dacl[2], with_dacl[3]]), 0x8004);
        assert_eq!(&with_dacl[16..20], &20u32.to_le_bytes());
        assert_eq!(with_dacl.len(), 20 + 8 + 20);
    }
}
