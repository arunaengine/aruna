use std::collections::{BTreeMap, HashSet};
use std::path::{Component, Path};

use aruna_blob::blob::BlobHandle;
use aruna_core::structs::{BackendLocation, RoCrateLimits};
use async_zip::Compression;
use async_zip::base::read::seek::ZipFileReader as BaseZipReader;
use futures_util::io::AsyncReadExt as FuturesReadExt;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt as TokioReadExt, AsyncSeekExt as TokioSeekExt, BufReader, SeekFrom};
use unicode_normalization::UnicodeNormalization;
use url::Url;

use super::reader::{HiddenRangeReader, read_hidden_range};

pub type ArchiveReader = async_zip::tokio::read::seek::ZipFileReader<BufReader<HiddenRangeReader>>;

const METADATA_PATH: &str = "ro-crate-metadata.json";
const SIGNATURE_PATH: &str = "ro-crate-metadata.json.minisig";
const EOCD_BYTES: u64 = 20 + 22 + u16::MAX as u64;
const EOCD_SIGNATURE: [u8; 4] = 0x0605_4b50u32.to_le_bytes();
const LOCAL_SIGNATURE: [u8; 4] = 0x0403_4b50u32.to_le_bytes();
const ZIP64_EOCD_SIGNATURE: [u8; 4] = 0x0606_4b50u32.to_le_bytes();
const ZIP64_LOCATOR_SIGNATURE: [u8; 4] = 0x0706_4b50u32.to_le_bytes();

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

#[cfg(test)]
pub async fn inspect_archive(
    handle: BlobHandle,
    location: BackendLocation,
    size: u64,
    eln: bool,
    limits: &RoCrateLimits,
) -> Result<ArchiveInspection, String> {
    inspect_reader(handle, location, size, eln, limits)
        .await
        .map(|(inspection, _)| inspection)
}

pub async fn inspect_reader(
    handle: BlobHandle,
    location: BackendLocation,
    size: u64,
    eln: bool,
    limits: &RoCrateLimits,
) -> Result<(ArchiveInspection, ArchiveReader), String> {
    let directory = precheck_archive(handle.clone(), location.clone(), size, limits).await?;
    let reader = open_archive(handle.clone(), location.clone(), size).await?;
    let stored_len = reader.file().entries().len();
    if stored_len as u64 > limits.max_entries {
        return Err(format!(
            "archive entry count exceeds limit {}",
            limits.max_entries
        ));
    }
    let (headers, reader) =
        inspect_headers(handle, location, size, directory.offset, reader).await?;
    let mut entries = Vec::with_capacity(stored_len);
    let mut expanded = 0u64;
    let mut normalized = HashSet::with_capacity(stored_len);
    for (index, &(data_offset, compression)) in headers.iter().enumerate().take(stored_len) {
        let entry = &reader.file().entries()[index];
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
        if path.len() as u64 > limits.key_bytes {
            return Err(format!("archive path exceeds limit {}", limits.key_bytes));
        }
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
            data_offset,
            compression,
            directory,
        });
    }
    inspect_layout(entries, eln).map(|inspection| (inspection, reader))
}

async fn precheck_archive(
    handle: BlobHandle,
    location: BackendLocation,
    size: u64,
    limits: &RoCrateLimits,
) -> Result<DirectoryRecord, String> {
    if size < 22 {
        return Err("ZIP end record is missing".to_string());
    }
    let start = size.saturating_sub(EOCD_BYTES);
    let tail = read_hidden_range(handle.clone(), location.clone(), start..size)
        .await
        .map_err(|error| format!("cannot read ZIP end record: {error}"))?;
    let record = end_record(&tail)?;
    let directory = if let Some(offset) = record.zip64_offset {
        let end = offset
            .checked_add(56)
            .filter(|end| *end <= size)
            .ok_or_else(|| "ZIP64 end record is outside the archive".to_string())?;
        let bytes = read_hidden_range(handle, location, offset..end)
            .await
            .map_err(|error| format!("cannot read ZIP64 end record: {error}"))?;
        zip64_record(&bytes)?
    } else {
        DirectoryRecord {
            entries: u64::from(record.entries),
            size: u64::from(record.directory_size),
            offset: u64::from(record.directory_offset),
        }
    };
    if directory.entries > limits.max_entries {
        return Err(format!(
            "archive entry count exceeds limit {}",
            limits.max_entries
        ));
    }
    let directory_limit = limits
        .max_entries
        .saturating_mul(46u64.saturating_add(limits.key_bytes.saturating_mul(2)));
    if directory.size > directory_limit {
        return Err(format!(
            "ZIP central directory exceeds limit {directory_limit}"
        ));
    }
    directory
        .offset
        .checked_add(directory.size)
        .filter(|end| *end <= size)
        .ok_or_else(|| "ZIP central directory is outside the archive".to_string())?;
    Ok(directory)
}

struct EndRecord {
    entries: u16,
    directory_size: u32,
    directory_offset: u32,
    zip64_offset: Option<u64>,
}

#[derive(Clone, Copy)]
struct DirectoryRecord {
    entries: u64,
    size: u64,
    offset: u64,
}

fn end_record(tail: &[u8]) -> Result<EndRecord, String> {
    let offset = tail
        .windows(4)
        .rposition(|signature| signature == EOCD_SIGNATURE)
        .ok_or_else(|| "ZIP end record is missing".to_string())?;
    if offset.saturating_add(22) > tail.len() {
        return Err("ZIP end record is truncated".to_string());
    }
    let entries = u16::from_le_bytes([tail[offset + 10], tail[offset + 11]]);
    let directory_size = u32::from_le_bytes([
        tail[offset + 12],
        tail[offset + 13],
        tail[offset + 14],
        tail[offset + 15],
    ]);
    let directory_offset = u32::from_le_bytes([
        tail[offset + 16],
        tail[offset + 17],
        tail[offset + 18],
        tail[offset + 19],
    ]);
    let zip64_locator = offset
        .checked_sub(20)
        .filter(|locator| tail[*locator..*locator + 4] == ZIP64_LOCATOR_SIGNATURE);
    let zip64_offset = zip64_locator
        .map(|locator| zip_u64_at(tail, locator + 8))
        .transpose()?;
    Ok(EndRecord {
        entries,
        directory_size,
        directory_offset,
        zip64_offset,
    })
}

fn zip64_record(bytes: &[u8]) -> Result<DirectoryRecord, String> {
    if bytes.len() != 56 || bytes[..4] != ZIP64_EOCD_SIGNATURE {
        return Err("ZIP64 end record is invalid".to_string());
    }
    let record_size = zip_u64_at(bytes, 4)?;
    if record_size < 44 {
        return Err("ZIP64 end record is too short".to_string());
    }
    Ok(DirectoryRecord {
        entries: zip_u64_at(bytes, 32)?,
        size: zip_u64_at(bytes, 40)?,
        offset: zip_u64_at(bytes, 48)?,
    })
}

fn zip_u64_at(bytes: &[u8], offset: usize) -> Result<u64, String> {
    let value = bytes
        .get(offset..offset.saturating_add(8))
        .ok_or_else(|| "ZIP64 field is truncated".to_string())?
        .try_into()
        .map_err(|_| "ZIP64 field has invalid length".to_string())?;
    Ok(u64::from_le_bytes(value))
}

async fn inspect_header(
    reader: &mut BufReader<HiddenRangeReader>,
    expected: &HeaderExpectation<'_>,
) -> Result<(u64, ArchiveCompression), String> {
    let end = expected
        .offset
        .checked_add(30)
        .filter(|end| *end <= expected.boundary)
        .ok_or_else(|| {
            format!(
                "local ZIP header is outside its entry range for `{}`",
                expected.path
            )
        })?;
    reader
        .seek(SeekFrom::Start(expected.offset))
        .await
        .map_err(|error| {
            format!(
                "cannot seek to local ZIP header for `{}`: {error}",
                expected.path
            )
        })?;
    let mut header = [0u8; 30];
    reader.read_exact(&mut header).await.map_err(|error| {
        format!(
            "cannot read local ZIP header for `{}`: {error}",
            expected.path
        )
    })?;
    if header[..4] != LOCAL_SIGNATURE {
        return Err(format!("invalid local ZIP header for `{}`", expected.path));
    }
    let version = u16::from_le_bytes([header[4], header[5]]) & 0x00ff;
    if version > 63 {
        return Err(format!("unsupported ZIP version for `{}`", expected.path));
    }
    let flags = u16::from_le_bytes([header[6], header[7]]);
    if flags & 0x0041 != 0 {
        return Err(format!(
            "encrypted ZIP entry `{}` is unsupported",
            expected.path
        ));
    }
    if flags & 0x0020 != 0 {
        return Err(format!(
            "patched ZIP entry `{}` is unsupported",
            expected.path
        ));
    }
    if flags & 0x0008 != u16::from(expected.data_descriptor) << 3 {
        return Err(format!(
            "local file header data-descriptor flag did not match the central directory flag for `{}`",
            expected.path
        ));
    }
    let method = u16::from_le_bytes([header[8], header[9]]);
    let compression = match Compression::try_from(method) {
        Ok(Compression::Stored) => ArchiveCompression::Stored,
        Ok(Compression::Deflate) => ArchiveCompression::Deflate,
        _ => {
            return Err(format!("unsupported compression for `{}`", expected.path));
        }
    };
    if Compression::try_from(method).ok() != Some(expected.compression) {
        return Err(format!(
            "ZIP compression fields disagree for `{}`",
            expected.path
        ));
    }
    let local_compressed = u32::from_le_bytes([header[18], header[19], header[20], header[21]]);
    let local_uncompressed = u32::from_le_bytes([header[22], header[23], header[24], header[25]]);
    let name_len = usize::from(u16::from_le_bytes([header[26], header[27]]));
    let extra_len = usize::from(u16::from_le_bytes([header[28], header[29]]));
    let data_offset = end
        .checked_add(name_len as u64)
        .and_then(|offset| offset.checked_add(extra_len as u64))
        .filter(|offset| {
            offset
                .checked_add(expected.compressed_size)
                .is_some_and(|end| end <= expected.boundary)
        })
        .ok_or_else(|| {
            format!(
                "ZIP entry data overlaps another record for `{}`",
                expected.path
            )
        })?;
    let mut filename = vec![0; name_len];
    reader.read_exact(&mut filename).await.map_err(|error| {
        format!(
            "cannot read local ZIP filename for `{}`: {error}",
            expected.path
        )
    })?;
    if filename != expected.filename {
        return Err(format!(
            "local file header name did not match the central directory name for `{}`",
            expected.path
        ));
    }
    let mut extra = vec![0; extra_len];
    reader.read_exact(&mut extra).await.map_err(|error| {
        format!(
            "cannot read local ZIP extra fields for `{}`: {error}",
            expected.path
        )
    })?;
    let (combined_compressed, combined_uncompressed) =
        local_sizes(&extra, local_compressed, local_uncompressed, expected.path)?;
    if !expected.data_descriptor
        && (combined_compressed != expected.compressed_size
            || combined_uncompressed != expected.uncompressed_size)
    {
        return Err(format!("local ZIP sizes disagree for `{}`", expected.path));
    }
    Ok((data_offset, compression))
}

struct HeaderExpectation<'a> {
    offset: u64,
    boundary: u64,
    compressed_size: u64,
    uncompressed_size: u64,
    compression: Compression,
    data_descriptor: bool,
    filename: &'a [u8],
    path: &'a str,
}

async fn inspect_headers(
    handle: BlobHandle,
    location: BackendLocation,
    size: u64,
    directory_offset: u64,
    archive: ArchiveReader,
) -> Result<(Vec<(u64, ArchiveCompression)>, ArchiveReader), String> {
    let entry_count = archive.file().entries().len();
    let mut order = (0..entry_count).collect::<Vec<_>>();
    order.sort_unstable_by_key(|index| archive.file().entries()[*index].header_offset());
    let mut inspected = vec![None; entry_count];
    let mut reader = BufReader::new(HiddenRangeReader::new(handle, location, size));
    for (position, index) in order.iter().copied().enumerate() {
        let entry = archive.file().entries()[index].clone();
        let boundary = order
            .get(position.saturating_add(1))
            .map(|next| archive.file().entries()[*next].header_offset())
            .unwrap_or(directory_offset)
            .min(directory_offset);
        let filename = entry
            .filename()
            .alternative()
            .unwrap_or_else(|| entry.filename().as_bytes());
        let path = entry.filename().as_str().unwrap_or("<non-UTF-8>");
        inspected[index] = Some(
            inspect_header(
                &mut reader,
                &HeaderExpectation {
                    offset: entry.header_offset(),
                    boundary,
                    compressed_size: entry.compressed_size(),
                    uncompressed_size: entry.uncompressed_size(),
                    compression: entry.compression(),
                    data_descriptor: entry.data_descriptor(),
                    filename,
                    path,
                },
            )
            .await?,
        );
    }
    let inspected = inspected
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| "local ZIP header inspection is incomplete".to_string())?;
    Ok((inspected, archive))
}

fn local_sizes(
    extra: &[u8],
    compressed_size: u32,
    uncompressed_size: u32,
    path: &str,
) -> Result<(u64, u64), String> {
    let mut cursor = 0usize;
    let mut seen = HashSet::new();
    let mut zip64 = None;
    while cursor < extra.len() {
        let header = extra
            .get(cursor..cursor.saturating_add(4))
            .ok_or_else(|| format!("truncated local ZIP extra field for `{path}`"))?;
        let identifier = u16::from_le_bytes([header[0], header[1]]);
        let size = usize::from(u16::from_le_bytes([header[2], header[3]]));
        let start = cursor.saturating_add(4);
        let end = start
            .checked_add(size)
            .filter(|end| *end <= extra.len())
            .ok_or_else(|| format!("invalid local ZIP extra field for `{path}`"))?;
        if !seen.insert(identifier) {
            return Err(format!("duplicate local ZIP extra field for `{path}`"));
        }
        let content = &extra[start..end];
        if identifier == 0x0001 {
            zip64 = Some(content);
        } else if matches!(identifier, 0x6375 | 0x7075)
            && (content.is_empty() || (content[0] == 1 && content.len() < 5))
        {
            return Err(format!("invalid local ZIP Unicode field for `{path}`"));
        }
        cursor = end;
    }

    let mut combined_compressed = u64::from(compressed_size);
    let mut combined_uncompressed = u64::from(uncompressed_size);
    let needs_compressed = compressed_size == u32::MAX;
    let needs_uncompressed = uncompressed_size == u32::MAX;
    match (zip64, needs_uncompressed, needs_compressed) {
        (None, true, _) | (None, _, true) => {
            return Err(format!("local ZIP64 sizes are missing for `{path}`"));
        }
        (Some(content), true, needs_compressed) => {
            combined_uncompressed =
                zip_u64_at(content, 0).map_err(|error| format!("{error} for `{path}`"))?;
            let consumed = 8;
            if needs_compressed {
                combined_compressed = zip_u64_at(content, consumed)
                    .map_err(|error| format!("{error} for `{path}`"))?;
            }
            let expected = consumed + usize::from(needs_compressed) * 8;
            if content.len() != expected {
                return Err(format!("invalid local ZIP64 sizes for `{path}`"));
            }
        }
        (Some(content), false, true) => {
            combined_compressed =
                zip_u64_at(content, 0).map_err(|error| format!("{error} for `{path}`"))?;
            if content.len() != 8 {
                return Err(format!("invalid local ZIP64 sizes for `{path}`"));
            }
        }
        (Some([]), false, false) => {}
        (Some(content), false, false) if content.len() == 16 => {
            let extra_uncompressed =
                zip_u64_at(content, 0).map_err(|error| format!("{error} for `{path}`"))?;
            let extra_compressed =
                zip_u64_at(content, 8).map_err(|error| format!("{error} for `{path}`"))?;
            if extra_uncompressed != combined_uncompressed
                || extra_compressed != combined_compressed
            {
                return Err(format!("invalid local ZIP64 sizes for `{path}`"));
            }
        }
        (Some(_), false, false) => {
            return Err(format!("invalid local ZIP64 sizes for `{path}`"));
        }
        (None, false, false) => {}
    }
    Ok((combined_compressed, combined_uncompressed))
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

    #[test]
    fn counts_legacy_entries() {
        let mut bytes = vec![0; 22];
        bytes[..4].copy_from_slice(&EOCD_SIGNATURE);
        bytes[8..10].copy_from_slice(&7u16.to_le_bytes());
        bytes[10..12].copy_from_slice(&7u16.to_le_bytes());
        bytes[12..16].copy_from_slice(&123u32.to_le_bytes());
        bytes[16..20].copy_from_slice(&456u32.to_le_bytes());

        let record = end_record(&bytes).unwrap();

        assert_eq!(record.entries, 7);
        assert_eq!(record.directory_size, 123);
        assert_eq!(record.directory_offset, 456);
        assert_eq!(record.zip64_offset, None);
    }

    #[test]
    fn counts_zip64_entries() {
        let mut tail = vec![0; 42];
        tail[..4].copy_from_slice(&ZIP64_LOCATOR_SIGNATURE);
        tail[8..16].copy_from_slice(&123u64.to_le_bytes());
        tail[20..24].copy_from_slice(&EOCD_SIGNATURE);
        tail[28..30].copy_from_slice(&u16::MAX.to_le_bytes());
        tail[30..32].copy_from_slice(&u16::MAX.to_le_bytes());
        let record = end_record(&tail).unwrap();
        assert_eq!(record.zip64_offset, Some(123));

        let mut bytes = vec![0; 56];
        bytes[..4].copy_from_slice(&ZIP64_EOCD_SIGNATURE);
        bytes[4..12].copy_from_slice(&44u64.to_le_bytes());
        bytes[24..32].copy_from_slice(&100_001u64.to_le_bytes());
        bytes[32..40].copy_from_slice(&100_001u64.to_le_bytes());
        bytes[40..48].copy_from_slice(&123u64.to_le_bytes());
        bytes[48..56].copy_from_slice(&456u64.to_le_bytes());
        let record = zip64_record(&bytes).unwrap();
        assert_eq!(record.entries, 100_001);
        assert_eq!(record.size, 123);
        assert_eq!(record.offset, 456);
    }

    #[test]
    fn finds_max_locator() {
        let mut tail = vec![0; EOCD_BYTES as usize];
        tail[..4].copy_from_slice(&ZIP64_LOCATOR_SIGNATURE);
        tail[8..16].copy_from_slice(&123u64.to_le_bytes());
        tail[20..24].copy_from_slice(&EOCD_SIGNATURE);
        tail[30..32].copy_from_slice(&u16::MAX.to_le_bytes());
        tail[40..42].copy_from_slice(&u16::MAX.to_le_bytes());

        let record = end_record(&tail).unwrap();

        assert_eq!(record.zip64_offset, Some(123));
    }

    #[test]
    fn selects_last_record() {
        let mut tail = vec![0; 80];
        tail[..4].copy_from_slice(&EOCD_SIGNATURE);
        tail[20..22].copy_from_slice(&58u16.to_le_bytes());
        tail[24..28].copy_from_slice(&ZIP64_LOCATOR_SIGNATURE);
        tail[32..40].copy_from_slice(&123u64.to_le_bytes());
        tail[44..48].copy_from_slice(&EOCD_SIGNATURE);
        tail[54..56].copy_from_slice(&u16::MAX.to_le_bytes());

        let record = end_record(&tail).unwrap();

        assert_eq!(record.entries, u16::MAX);
        assert_eq!(record.zip64_offset, Some(123));
    }

    #[test]
    fn parses_zip64_sizes() {
        let mut extra = Vec::new();
        extra.extend_from_slice(&1u16.to_le_bytes());
        extra.extend_from_slice(&16u16.to_le_bytes());
        extra.extend_from_slice(&123u64.to_le_bytes());
        extra.extend_from_slice(&45u64.to_le_bytes());

        assert_eq!(
            local_sizes(&extra, u32::MAX, u32::MAX, "data").unwrap(),
            (45, 123)
        );
    }

    #[test]
    fn rejects_bad_zip64() {
        let mut extra = Vec::new();
        extra.extend_from_slice(&1u16.to_le_bytes());
        extra.extend_from_slice(&8u16.to_le_bytes());
        extra.extend_from_slice(&123u64.to_le_bytes());

        assert!(local_sizes(&extra, u32::MAX, u32::MAX, "data").is_err());
        assert!(local_sizes(&[0], 1, 1, "data").is_err());
    }
}
