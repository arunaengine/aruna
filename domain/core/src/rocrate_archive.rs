//! Describes the entries of an RO-Crate ZIP and decides which of its paths are acceptable.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::path::{Component, Path};

use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;
use url::Url;

const METADATA_PATH: &str = "ro-crate-metadata.json";
const SIGNATURE_PATH: &str = "ro-crate-metadata.json.minisig";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ArchiveCompression {
    Stored,
    Deflate,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArchiveEntry {
    pub index: usize,
    pub archive_path: String,
    pub path: String,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub crc32: u32,
    pub data_offset: u64,
    pub compression: ArchiveCompression,
    pub directory: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArchiveInspection {
    pub entries: Vec<ArchiveEntry>,
    pub metadata_index: usize,
    pub wrapper: Option<String>,
    pub eln: bool,
}

pub fn file_id_candidates(value: &str) -> Result<Option<Vec<String>>, String> {
    if Url::parse(value).is_ok() {
        return Ok(None);
    }
    let lower = value.to_ascii_lowercase();
    if lower.contains("%2f") || lower.contains("%5c") {
        return Err(format!(
            "File identifier contains an encoded path separator `{value}`"
        ));
    }
    let raw = normalize_relative_id(value)?;
    let mut candidates = vec![raw.clone()];
    if let Ok(decoded) = percent_decode_str(value).decode_utf8() {
        let decoded = normalize_relative_id(&decoded)?;
        if decoded != raw {
            candidates.push(decoded);
        }
    }
    Ok(Some(candidates))
}

pub fn payload_entries(inspection: &ArchiveInspection) -> BTreeMap<String, &ArchiveEntry> {
    inspection
        .entries
        .iter()
        .filter(|entry| {
            !entry.directory && entry.path != METADATA_PATH && entry.path != SIGNATURE_PATH
        })
        .map(|entry| (entry.path.clone(), entry))
        .collect()
}

pub fn signature_entry(inspection: &ArchiveInspection) -> Option<&ArchiveEntry> {
    inspection
        .entries
        .iter()
        .find(|entry| entry.path == SIGNATURE_PATH)
}

pub fn inspect_layout(
    mut entries: Vec<ArchiveEntry>,
    eln: bool,
) -> Result<ArchiveInspection, String> {
    let root_metadata = entries
        .iter()
        .filter(|entry| entry.path == METADATA_PATH)
        .map(|entry| entry.index)
        .collect::<Vec<_>>();
    let wrapper_metadata = entries
        .iter()
        .filter_map(|entry| {
            entry
                .path
                .split_once('/')
                .filter(|(_, suffix)| *suffix == METADATA_PATH)
                .map(|(wrapper, _)| (wrapper.to_string(), entry.index))
        })
        .collect::<Vec<_>>();
    let (wrapper, metadata_index) = match (root_metadata.as_slice(), wrapper_metadata.as_slice()) {
        ([metadata_index], []) if !eln => (None, *metadata_index),
        ([_], []) => {
            return Err("ELN archive requires a single wrapper directory".to_string());
        }
        ([], [(wrapper, metadata_index)]) => {
            let prefix = format!("{wrapper}/");
            if entries.iter().any(|entry| {
                (entry.path == *wrapper && !entry.directory)
                    || (entry.path != *wrapper && !entry.path.starts_with(&prefix))
            }) {
                return Err("wrapper archive contains another top-level entry".to_string());
            }
            (Some(wrapper.clone()), *metadata_index)
        }
        ([], []) => return Err("archive does not contain ro-crate-metadata.json".to_string()),
        _ => {
            return Err("archive has ambiguous root and wrapper metadata descriptors".to_string());
        }
    };

    if let Some(wrapper) = wrapper.as_deref() {
        let prefix = format!("{wrapper}/");
        entries.retain(|entry| entry.path != wrapper);
        for entry in &mut entries {
            entry.path = entry
                .path
                .strip_prefix(&prefix)
                .ok_or_else(|| "archive entry is outside its wrapper".to_string())?
                .to_string();
        }
    }
    let metadata_count = entries
        .iter()
        .filter(|entry| entry.path == METADATA_PATH)
        .count();
    if metadata_count != 1 {
        return Err("archive must contain exactly one metadata descriptor".to_string());
    }
    Ok(ArchiveInspection {
        entries,
        metadata_index,
        wrapper,
        eln,
    })
}

pub fn normalize_entry_path(value: &str) -> Result<String, String> {
    if value.contains('\\') || value.contains('\0') {
        return Err(format!("unsafe archive path `{value}`"));
    }
    let mut value = value.trim_end_matches('/');
    while let Some(stripped) = value.strip_prefix("./") {
        value = stripped;
    }
    if value.is_empty() {
        return Err("archive contains an empty path".to_string());
    }
    if value.starts_with('/') {
        return Err(format!("unsafe relative path `{value}`"));
    }
    let value = value
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    validate_relative_path(&value)?;
    Ok(value.nfc().collect())
}

fn normalize_relative_id(value: &str) -> Result<String, String> {
    let mut value = value;
    while let Some(stripped) = value.strip_prefix("./") {
        value = stripped;
    }
    if value.is_empty() {
        return Err("File identifier is empty".to_string());
    }
    validate_relative_path(value)?;
    Ok(value.nfc().collect())
}

fn validate_relative_path(value: &str) -> Result<(), String> {
    if value.starts_with('/')
        || value
            .as_bytes()
            .get(1)
            .is_some_and(|byte| *byte == b':' && value.as_bytes()[0].is_ascii_alphabetic())
        || value.split('/').any(|part| part.is_empty() || part == "..")
        || Path::new(value)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!("unsafe relative path `{value}`"));
    }
    if value.chars().any(char::is_control) {
        return Err(format!("path contains a control character `{value}`"));
    }
    Ok(())
}

pub fn reject_special_entry(
    permissions: Option<u16>,
    directory: bool,
    path: &str,
) -> Result<(), String> {
    let Some(mode) = permissions else {
        return Ok(());
    };
    let file_type = mode & 0o170000;
    if file_type == 0 || file_type == 0o100000 || (directory && file_type == 0o040000) {
        return Ok(());
    }
    Err(format!("archive entry `{path}` is not a regular file"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: usize, path: &str) -> ArchiveEntry {
        ArchiveEntry {
            index,
            archive_path: path.to_string(),
            path: path.to_string(),
            compressed_size: 1,
            uncompressed_size: 1,
            crc32: 0,
            data_offset: 0,
            compression: ArchiveCompression::Stored,
            directory: false,
        }
    }

    #[test]
    fn layout_accepts_root() {
        let inspected = inspect_layout(
            vec![entry(0, METADATA_PATH), entry(1, "data/file.txt")],
            false,
        )
        .unwrap();
        assert_eq!(inspected.wrapper, None);
        assert_eq!(inspected.entries[1].path, "data/file.txt");
    }

    #[test]
    fn layout_strips_wrapper() {
        let inspected = inspect_layout(
            vec![
                entry(0, "experiment/ro-crate-metadata.json"),
                entry(1, "experiment/data/file.txt"),
            ],
            true,
        )
        .unwrap();
        assert_eq!(inspected.wrapper.as_deref(), Some("experiment"));
        assert_eq!(inspected.entries[1].path, "data/file.txt");
    }

    #[test]
    fn layout_rejects_siblings() {
        assert!(
            inspect_layout(
                vec![
                    entry(0, "experiment/ro-crate-metadata.json"),
                    entry(1, "outside.txt"),
                ],
                true,
            )
            .is_err()
        );
    }

    #[test]
    fn layout_rejects_file() {
        assert!(
            inspect_layout(
                vec![
                    entry(0, "experiment/ro-crate-metadata.json"),
                    entry(1, "experiment"),
                ],
                true,
            )
            .is_err()
        );
    }

    #[test]
    fn path_rejects_traversal() {
        for path in ["/root", "../escape", "a/../b", "C:/drive", "a\\b"] {
            assert!(normalize_entry_path(path).is_err(), "{path}");
        }
        assert!(file_id_candidates("data%2Fsecret").is_err());
    }

    #[test]
    fn path_collapses_slashes() {
        assert_eq!(
            normalize_entry_path("wrapper/data//file.txt").unwrap(),
            "wrapper/data/file.txt"
        );
        assert!(normalize_entry_path("//server/file.txt").is_err());
        assert!(normalize_entry_path("data//../file.txt").is_err());
    }

    #[test]
    fn ids_decode_spaces() {
        assert_eq!(
            file_id_candidates("./data/a%20b.txt").unwrap().unwrap(),
            vec!["data/a%20b.txt".to_string(), "data/a b.txt".to_string()]
        );
        assert!(
            file_id_candidates("https://example.org/file")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn ids_ignore_absolute() {
        for id in [
            "urn:uuid:12345678-1234-1234-1234-123456789abc",
            "ftp://example.org/file",
            "doi:10.1234/example",
        ] {
            assert!(file_id_candidates(id).unwrap().is_none(), "{id}");
        }
    }
}
