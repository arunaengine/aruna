use std::collections::{BTreeMap, HashSet};
use std::path::{Component, Path};

use aruna_blob::blob::BlobHandle;
use aruna_core::structs::{BackendLocation, RoCrateLimits};
use async_zip::Compression;
use async_zip::base::read::seek::ZipFileReader as BaseZipReader;
use futures_util::io::AsyncReadExt;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use tokio::io::BufReader;
use unicode_normalization::UnicodeNormalization;
use url::Url;

use super::reader::HiddenRangeReader;

pub type ArchiveReader = async_zip::tokio::read::seek::ZipFileReader<BufReader<HiddenRangeReader>>;

const METADATA_PATH: &str = "ro-crate-metadata.json";
const SIGNATURE_PATH: &str = "ro-crate-metadata.json.minisig";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArchiveEntry {
    pub index: usize,
    pub archive_path: String,
    pub path: String,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub crc32: u32,
    pub directory: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArchiveInspection {
    pub entries: Vec<ArchiveEntry>,
    pub metadata_index: usize,
    pub wrapper: Option<String>,
    pub eln: bool,
}

pub async fn inspect_archive(
    handle: BlobHandle,
    location: BackendLocation,
    size: u64,
    eln: bool,
    limits: &RoCrateLimits,
) -> Result<ArchiveInspection, String> {
    let reader = open_archive(handle, location, size).await?;
    let stored = reader.file().entries();
    if stored.len() as u64 > limits.max_entries {
        return Err(format!(
            "archive entry count exceeds limit {}",
            limits.max_entries
        ));
    }
    let mut entries = Vec::with_capacity(stored.len());
    let mut expanded = 0u64;
    let mut normalized = HashSet::with_capacity(stored.len());
    for (index, entry) in stored.iter().enumerate() {
        if entry
            .filename()
            .alternative()
            .is_some_and(|filename| !filename.is_ascii())
        {
            return Err("archive path lacks the UTF-8 flag".to_string());
        }
        let filename = entry
            .filename()
            .as_str()
            .map_err(|_| "archive path is not UTF-8".to_string())?;
        let directory = entry
            .dir()
            .map_err(|error| format!("invalid archive entry: {error}"))?;
        let path = normalize_entry_path(filename)?;
        if !normalized.insert(path.clone()) {
            return Err(format!("duplicate normalized archive path `{path}`"));
        }
        reject_special_entry(entry.unix_permissions(), directory, &path)?;
        match entry.compression() {
            Compression::Stored | Compression::Deflate => {}
            _ => return Err(format!("unsupported compression for `{path}`")),
        }
        expanded = expanded
            .checked_add(entry.uncompressed_size())
            .ok_or_else(|| "declared expanded archive size overflow".to_string())?;
        if expanded > limits.expanded_import_bytes {
            return Err(format!(
                "declared expanded archive size exceeds limit {}",
                limits.expanded_import_bytes
            ));
        }
        entries.push(ArchiveEntry {
            index,
            archive_path: filename.to_string(),
            path,
            compressed_size: entry.compressed_size(),
            uncompressed_size: entry.uncompressed_size(),
            crc32: entry.crc32(),
            directory,
        });
    }
    inspect_layout(entries, eln)
}

pub async fn open_archive(
    handle: BlobHandle,
    location: BackendLocation,
    size: u64,
) -> Result<ArchiveReader, String> {
    BaseZipReader::with_tokio(BufReader::new(HiddenRangeReader::new(
        handle, location, size,
    )))
    .await
    .map_err(|error| format!("invalid ZIP archive: {error}"))
}

pub async fn read_metadata(
    reader: &mut ArchiveReader,
    index: usize,
    limit: u64,
) -> Result<String, String> {
    let entry = reader
        .file()
        .entries()
        .get(index)
        .ok_or_else(|| "metadata ZIP entry is missing".to_string())?;
    if entry.uncompressed_size() > limit {
        return Err(format!("RO-Crate metadata exceeds limit {limit}"));
    }
    let capacity = usize::try_from(entry.uncompressed_size())
        .map_err(|_| "RO-Crate metadata is too large".to_string())?;
    let mut bytes = Vec::with_capacity(capacity);
    let mut entry = reader
        .reader_with_entry(index)
        .await
        .map_err(|error| format!("cannot open RO-Crate metadata: {error}"))?;
    let expected_size = entry.entry().uncompressed_size();
    let expected_crc = entry.entry().crc32();
    (&mut entry)
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| format!("cannot read RO-Crate metadata: {error}"))?;
    if bytes.len() as u64 > limit {
        return Err(format!("RO-Crate metadata exceeds limit {limit}"));
    }
    if bytes.len() as u64 != expected_size {
        return Err("RO-Crate metadata size does not match its ZIP entry".to_string());
    }
    if entry.compute_hash() != expected_crc {
        return Err("RO-Crate metadata CRC32 does not match".to_string());
    }
    String::from_utf8(bytes).map_err(|_| "RO-Crate metadata is not UTF-8".to_string())
}

pub fn file_id_candidates(value: &str) -> Result<Option<Vec<String>>, String> {
    if let Ok(url) = Url::parse(value) {
        return match url.scheme() {
            "http" | "https" => Ok(None),
            _ => Err(format!("unsupported absolute File identifier `{value}`")),
        };
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

fn inspect_layout(mut entries: Vec<ArchiveEntry>, eln: bool) -> Result<ArchiveInspection, String> {
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
                entry.path != *wrapper && !entry.path.starts_with(&prefix) && !entry.path.is_empty()
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

fn normalize_entry_path(value: &str) -> Result<String, String> {
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
    validate_relative_path(value)?;
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

fn reject_special_entry(
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
    fn path_rejects_traversal() {
        for path in ["/root", "../escape", "a/../b", "C:/drive", "a\\b"] {
            assert!(normalize_entry_path(path).is_err(), "{path}");
        }
        assert!(file_id_candidates("data%2Fsecret").is_err());
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
}
