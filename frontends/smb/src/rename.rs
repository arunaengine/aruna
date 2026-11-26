//! Normative SMB2 FileRenameInformation decoding: a 24-byte minimum buffer, the 20-byte
//! fixed prefix and a counted UTF-16LE target confined to the share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::path::parse_path;
use crate::status;
use crate::wire::utf16_text;

/// The smallest FILE_RENAME_INFORMATION_TYPE_2 buffer MS-SMB2 accepts.
pub const MINIMUM_SIZE: usize = 24;
const PREFIX_SIZE: usize = 20;

/// A validated rename target relative to the share root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameTarget {
    pub replace: bool,
    pub components: Vec<String>,
}

/// Decodes `ReplaceIfExists`, seven reserved bytes, `RootDirectory`, `FileNameLength` and
/// the name. Refuses a remote root handle, a bad count, an empty, absolute or UNC name, a
/// traversal and invalid separators.
pub fn parse_rename(buffer: &[u8]) -> Result<RenameTarget, u32> {
    if buffer.len() < MINIMUM_SIZE {
        return Err(status::INFO_LENGTH_MISMATCH);
    }
    let replace = buffer[0] != 0;
    let mut root = [0u8; 8];
    root.copy_from_slice(&buffer[8..16]);
    if u64::from_le_bytes(root) != 0 {
        return Err(status::INVALID_PARAMETER);
    }
    let length = u32::from_le_bytes([buffer[16], buffer[17], buffer[18], buffer[19]]) as usize;
    if length == 0 || !length.is_multiple_of(2) {
        return Err(status::INVALID_PARAMETER);
    }
    let end = PREFIX_SIZE
        .checked_add(length)
        .ok_or(status::INVALID_PARAMETER)?;
    let name = buffer
        .get(PREFIX_SIZE..end)
        .ok_or(status::INVALID_PARAMETER)?;
    let text = utf16_text(name).ok_or(status::NAME_INVALID)?;
    if text.starts_with('\\') || text.starts_with('/') {
        return Err(status::NAME_INVALID);
    }
    let components = parse_path(&text)?;
    if components.is_empty() {
        return Err(status::INVALID_PARAMETER);
    }
    Ok(RenameTarget {
        replace,
        components,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::utf16_bytes;

    fn buffer(replace: u8, root: u64, name: &str) -> Vec<u8> {
        let encoded = utf16_bytes(name);
        let mut out = vec![replace, 0, 0, 0, 0, 0, 0, 0];
        out.extend_from_slice(&root.to_le_bytes());
        out.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
        out.extend_from_slice(&encoded);
        while out.len() < MINIMUM_SIZE {
            out.push(0);
        }
        out
    }

    #[test]
    fn decodes_nested_target() {
        let target = parse_rename(&buffer(1, 0, "dir\\new.txt")).expect("valid rename");
        assert!(target.replace);
        assert_eq!(target.components, vec!["dir", "new.txt"]);
        let target = parse_rename(&buffer(0, 0, "a")).expect("short name pads to 24");
        assert!(!target.replace);
    }

    #[test]
    fn nonzero_means_replace() {
        assert!(parse_rename(&buffer(0x80, 0, "x")).expect("valid").replace);
    }

    #[test]
    fn rejects_short_buffer() {
        let full = buffer(0, 0, "a");
        assert_eq!(parse_rename(&full[..23]), Err(status::INFO_LENGTH_MISMATCH));
        assert_eq!(parse_rename(&[]), Err(status::INFO_LENGTH_MISMATCH));
    }

    #[test]
    fn rejects_root_directory() {
        assert_eq!(
            parse_rename(&buffer(0, 7, "a")),
            Err(status::INVALID_PARAMETER)
        );
    }

    #[test]
    fn rejects_bad_counts() {
        let mut odd = buffer(0, 0, "abc");
        odd[16..20].copy_from_slice(&5u32.to_le_bytes());
        assert_eq!(parse_rename(&odd), Err(status::INVALID_PARAMETER));
        let mut past = buffer(0, 0, "abc");
        past[16..20].copy_from_slice(&40u32.to_le_bytes());
        assert_eq!(parse_rename(&past), Err(status::INVALID_PARAMETER));
        let mut huge = buffer(0, 0, "abc");
        huge[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(parse_rename(&huge), Err(status::INVALID_PARAMETER));
        let mut empty = buffer(0, 0, "ab");
        empty[16..20].copy_from_slice(&0u32.to_le_bytes());
        assert_eq!(parse_rename(&empty), Err(status::INVALID_PARAMETER));
    }

    #[test]
    fn rejects_escaping_names() {
        for name in [
            "\\abs.txt",
            "\\\\server\\share\\x",
            "C:\\x",
            "..\\up",
            "a\\..\\..\\b",
            "a\\\\b",
            "a/b",
            "dir\\",
        ] {
            assert_eq!(
                parse_rename(&buffer(0, 0, name)),
                Err(status::NAME_INVALID),
                "{name:?}"
            );
        }
    }

    #[test]
    fn rejects_unpaired_surrogate() {
        let mut bad = buffer(0, 0, "ab");
        bad[20..22].copy_from_slice(&0xD800u16.to_le_bytes());
        assert_eq!(parse_rename(&bad), Err(status::NAME_INVALID));
    }
}
