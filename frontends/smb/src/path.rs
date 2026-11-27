//! SMB path validation shared by CREATE, QUERY_DIRECTORY patterns and rename targets:
//! backslash separators, UTF-16 component limits, prohibited characters and device names.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::status;

/// Longest component in UTF-16 code units, as NTFS and MS-FSCC allow.
pub const COMPONENT_LIMIT: usize = 255;
const PROHIBITED: &[char] = &['"', '*', '/', ':', '<', '>', '?', '|'];
const DEVICES: &[&str] = &["CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$", "CLOCK$"];

/// Splits a share-relative path into validated components; an empty path is the share root.
/// The caller rejects a leading separator with the status its command defines.
pub fn parse_path(name: &str) -> Result<Vec<String>, u32> {
    if name.is_empty() {
        return Ok(Vec::new());
    }
    name.split('\\')
        .map(|component| check_component(component).map(|()| component.to_owned()))
        .collect()
}

/// Joins validated components into the backend's slash-separated key path.
pub fn join_path(components: &[String]) -> String {
    components.join("/")
}

/// One path component: not empty, not `.` or `..`, no prohibited or control characters,
/// no trailing dot or space, no reserved device name and at most 255 UTF-16 units.
pub fn check_component(component: &str) -> Result<(), u32> {
    if component.is_empty() || component == "." || component == ".." {
        return Err(status::NAME_INVALID);
    }
    if component.encode_utf16().count() > COMPONENT_LIMIT {
        return Err(status::NAME_INVALID);
    }
    if component
        .chars()
        .any(|character| character < ' ' || PROHIBITED.contains(&character))
    {
        return Err(status::NAME_INVALID);
    }
    if component.ends_with('.') || component.ends_with(' ') {
        return Err(status::NAME_INVALID);
    }
    if is_device(component) {
        return Err(status::NAME_INVALID);
    }
    Ok(())
}

/// Whether the part before the first dot names a DOS device such as `CON` or `COM1`.
pub fn is_device(component: &str) -> bool {
    let stem = component
        .split('.')
        .next()
        .unwrap_or(component)
        .trim_end_matches(' ')
        .to_ascii_uppercase();
    if DEVICES.contains(&stem.as_str()) {
        return true;
    }
    let bytes = stem.as_bytes();
    bytes.len() == 4
        && (stem.starts_with("COM") || stem.starts_with("LPT"))
        && (b'1'..=b'9').contains(&bytes[3])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_nested_paths() {
        let parts = parse_path("data\\raw\\file.txt").expect("valid path");
        assert_eq!(parts, vec!["data", "raw", "file.txt"]);
        assert_eq!(join_path(&parts), "data/raw/file.txt");
        assert_eq!(parse_path(""), Ok(Vec::new()));
    }

    #[test]
    fn rejects_bad_components() {
        for name in [
            "a\\\\b",
            "a\\",
            "..",
            "a\\..\\b",
            ".",
            "a/b",
            "a:b",
            "x*",
            "name.",
            "name ",
            "CON",
            "con.txt",
            "COM1",
            "lpt9.log",
            "tab\tname",
        ] {
            assert_eq!(parse_path(name), Err(status::NAME_INVALID), "{name:?}");
        }
    }

    #[test]
    fn accepts_device_lookalikes() {
        for name in ["CONSOLE", "COM10", "com", "LPT0", "aux_file", "nul2"] {
            assert!(parse_path(name).is_ok(), "{name:?}");
        }
    }

    #[test]
    fn limits_component_length() {
        let longest = "a".repeat(COMPONENT_LIMIT);
        assert!(parse_path(&longest).is_ok());
        let longer = "a".repeat(COMPONENT_LIMIT + 1);
        assert_eq!(parse_path(&longer), Err(status::NAME_INVALID));
    }
}
