//! Reads and writes RO-Crate ZIP archives over hidden blobs: inspects an archive, streams one
//! entry and builds a new archive entry by entry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use aruna_core::effects::BlobEffect;
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::jobs::{ByteStream, ZipFailure, ZipWork};
use aruna_core::rocrate_archive::{
    ArchiveCompression, ArchiveEntry, ArchiveInspection, inspect_layout, normalize_entry_path,
    reject_special_entry,
};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::storage::blob::BackendLocation;
use async_zip::base::read::seek::ZipFileReader as BaseZipReader;
use async_zip::base::write::ZipFileWriter;
use async_zip::{Compression, ZipDateTime, ZipDateTimeBuilder, ZipEntryBuilder};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Datelike, Timelike, Utc};
use futures::StreamExt;
use futures::io::{AsyncReadExt as FuturesReadExt, AsyncWriteExt as FuturesWriteExt};
use tokio::io::{
    AsyncRead, AsyncReadExt as TokioReadExt, AsyncSeek, AsyncSeekExt as TokioSeekExt, BufReader,
    DuplexStream, ReadBuf, SeekFrom,
};
use tokio::sync::mpsc;
use tokio_util::compat::Compat;

use crate::blob::BlobHandle;

pub type ArchiveReader = async_zip::tokio::read::seek::ZipFileReader<BufReader<HiddenRangeReader>>;

/// The archive being written to a duplex stream.
type ZipSink = ZipFileWriter<Compat<DuplexStream>>;

const EOCD_BYTES: u64 = 20 + 22 + u16::MAX as u64;
const EOCD_SIGNATURE: [u8; 4] = 0x0605_4b50u32.to_le_bytes();
const LOCAL_SIGNATURE: [u8; 4] = 0x0403_4b50u32.to_le_bytes();
const ZIP64_EOCD_SIGNATURE: [u8; 4] = 0x0606_4b50u32.to_le_bytes();
const ZIP64_LOCATOR_SIGNATURE: [u8; 4] = 0x0706_4b50u32.to_le_bytes();
const RANGE_CHUNK_BYTES: u64 = 1024 * 1024;
const PAYLOAD_CHUNK_BYTES: usize = 64 * 1024;
/// Bytes buffered between the archive writer and the spool that stores it.
const ZIP_PIPE_BYTES: usize = 128 * 1024;

/// Inspects the archive and, when `metadata` holds the limit, reads its metadata file.
pub async fn inspect_full(
    handle: BlobHandle,
    location: BackendLocation,
    size: u64,
    eln: bool,
    limits: &RoCrateLimits,
    metadata: Option<u64>,
) -> Result<(ArchiveInspection, Option<String>), String> {
    let (inspection, mut reader) = inspect_reader(handle, location, size, eln, limits).await?;
    let jsonld = match metadata {
        Some(limit) => Some(read_metadata(&mut reader, inspection.metadata_index, limit).await?),
        None => None,
    };
    Ok((inspection, jsonld))
}

/// The entries of an archive, each checked against its local header and the limits.
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

async fn inspect_reader(
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

type RangeFuture = Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send + 'static>>;

/// Presents a hidden blob, read in ranges, as a seekable stream.
pub struct HiddenRangeReader {
    handle: BlobHandle,
    location: BackendLocation,
    length: u64,
    position: u64,
    buffer_start: u64,
    buffer: Bytes,
    pending: Option<RangeFuture>,
}

impl HiddenRangeReader {
    pub fn new(handle: BlobHandle, location: BackendLocation, length: u64) -> Self {
        Self {
            handle,
            location,
            length,
            position: 0,
            buffer_start: 0,
            buffer: Bytes::new(),
            pending: None,
        }
    }

    fn buffered(&self) -> Option<&[u8]> {
        let offset = self.position.checked_sub(self.buffer_start)?;
        let offset = usize::try_from(offset).ok()?;
        self.buffer.get(offset..)
    }

    fn begin_fetch(&mut self) {
        let start = self.position;
        let end = start.saturating_add(RANGE_CHUNK_BYTES).min(self.length);
        self.pending = Some(fetch_range(
            self.handle.clone(),
            self.location.clone(),
            start..end,
        ));
    }
}

impl Clone for HiddenRangeReader {
    fn clone(&self) -> Self {
        Self::new(self.handle.clone(), self.location.clone(), self.length)
    }
}

impl AsyncRead for HiddenRangeReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        destination: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if self.position >= self.length || destination.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
            if let Some(buffered) = self.buffered().filter(|buffered| !buffered.is_empty()) {
                let copied = buffered.len().min(destination.remaining());
                destination.put_slice(&buffered[..copied]);
                self.position = self.position.saturating_add(copied as u64);
                return Poll::Ready(Ok(()));
            }
            if self.pending.is_none() {
                self.begin_fetch();
            }
            let Some(pending) = self.pending.as_mut() else {
                return Poll::Ready(Err(io::Error::other("range fetch was not initialized")));
            };
            match pending.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.pending = None;
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Ok(bytes)) => {
                    self.pending = None;
                    self.buffer_start = self.position;
                    self.buffer = bytes;
                    if self.buffer.is_empty() {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "hidden blob range returned no bytes",
                        )));
                    }
                }
            }
        }
    }
}

impl AsyncSeek for HiddenRangeReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        self.position = seek_offset(self.position, self.length, position)?;
        self.pending = None;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

fn seek_offset(current: u64, length: u64, position: SeekFrom) -> io::Result<u64> {
    let target = match position {
        SeekFrom::Start(target) => i128::from(target),
        SeekFrom::End(delta) => i128::from(length) + i128::from(delta),
        SeekFrom::Current(delta) => i128::from(current) + i128::from(delta),
    };
    if !(0..=i128::from(length)).contains(&target) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ZIP seek is outside the hidden blob",
        ));
    }
    u64::try_from(target).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "ZIP seek offset is not representable",
        )
    })
}

fn fetch_range(handle: BlobHandle, location: BackendLocation, range: Range<u64>) -> RangeFuture {
    Box::pin(async move {
        let expected = range.end.saturating_sub(range.start);
        let event = handle
            .send_blob_effect(BlobEffect::ReadHiddenRange { location, range })
            .await;
        let Event::Blob(BlobEvent::HiddenRead { mut blob, .. }) = event else {
            return match event {
                Event::Blob(BlobEvent::Error(error)) => Err(io::Error::other(error.to_string())),
                other => Err(io::Error::other(format!(
                    "unexpected hidden range event: {}",
                    other.label()
                ))),
            };
        };
        let capacity = usize::try_from(expected).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "hidden range is too large")
        })?;
        let mut bytes = BytesMut::with_capacity(capacity);
        while let Some(chunk) = blob.next().await {
            let chunk = chunk.map_err(|error| io::Error::other(error.to_string()))?;
            bytes.extend_from_slice(&chunk);
            if bytes.len() > capacity {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "hidden range exceeded requested length",
                ));
            }
        }
        if bytes.len() != capacity {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "hidden range was truncated",
            ));
        }
        Ok(bytes.freeze())
    })
}

async fn read_hidden_range(
    handle: BlobHandle,
    location: BackendLocation,
    range: Range<u64>,
) -> io::Result<Bytes> {
    fetch_range(handle, location, range).await
}

/// Where one entry lies in an archive and how it is stored.
#[derive(Clone, Copy, Debug)]
pub struct EntrySpan {
    pub data_offset: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub compression: ArchiveCompression,
}

/// The expanded bytes of one archive entry; a size other than the declared one fails the stream.
pub async fn entry_stream(
    handle: BlobHandle,
    location: BackendLocation,
    size: u64,
    span: EntrySpan,
) -> io::Result<ByteStream> {
    let mut source = HiddenRangeReader::new(handle, location, size);
    source.seek(SeekFrom::Start(span.data_offset)).await?;
    let source = BufReader::new(source.take(span.compressed_size));
    let mut reader: Pin<Box<dyn AsyncRead + Send>> = match span.compression {
        ArchiveCompression::Stored => Box::pin(source),
        ArchiveCompression::Deflate => Box::pin(
            async_compression::tokio::bufread::DeflateDecoder::new(source),
        ),
    };
    let expected = span.uncompressed_size;
    let (sender, mut receiver) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    tokio::spawn(async move {
        let mut buffer = vec![0; PAYLOAD_CHUNK_BYTES];
        let mut expanded = 0u64;
        loop {
            let failure = match reader.read(&mut buffer).await {
                Ok(0) if expanded == expected => break,
                Ok(0) => "ZIP entry is shorter than its declared expanded size",
                Ok(read) => {
                    expanded = expanded.saturating_add(read as u64);
                    if expanded <= expected {
                        let chunk = Bytes::copy_from_slice(&buffer[..read]);
                        if sender.send(Ok(chunk)).await.is_err() {
                            break;
                        }
                        continue;
                    }
                    "ZIP entry exceeds its declared expanded size"
                }
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    break;
                }
            };
            let error = io::Error::new(io::ErrorKind::InvalidData, failure);
            let _ = sender.send(Err(error)).await;
            break;
        }
    });
    Ok(BackendStream::new(futures::stream::poll_fn(move |cx| {
        receiver.poll_recv(cx)
    })))
}

/// A new archive and the stream of its bytes.
pub fn open_zip() -> (ZipWork, ByteStream) {
    let (writer, reader) = tokio::io::duplex(ZIP_PIPE_BYTES);
    let sink: ZipSink = ZipFileWriter::with_tokio(writer);
    let stream = BackendStream::new(tokio_util::io::ReaderStream::new(reader));
    (ZipWork::new(Box::new(sink)), stream)
}

fn sink_of(zip: ZipWork) -> Result<Box<ZipSink>, String> {
    (zip.into_inner().downcast::<ZipSink>())
        .map_err(|_| "the archive writer has another type".to_string())
}

/// Writes one whole entry.
pub async fn zip_whole(
    zip: ZipWork,
    path: &str,
    modified_ms: u64,
    bytes: &[u8],
) -> (Option<ZipWork>, Result<(), String>) {
    let mut sink = match sink_of(zip) {
        Ok(sink) => sink,
        Err(error) => return (None, Err(error)),
    };
    let written = sink
        .write_entry_whole(zip_entry(path, modified_ms), bytes)
        .await;
    let result = written.map_err(|error| error.to_string());
    (Some(ZipWork::new(sink)), result)
}

/// Writes one entry from `stream` until it ends, `stop` completes or a side fails, and answers
/// the BLAKE3 hash of its bytes.
pub async fn zip_stream(
    zip: ZipWork,
    path: &str,
    modified_ms: u64,
    mut stream: ByteStream,
    stop: impl Future<Output = ()>,
) -> (Option<ZipWork>, Result<[u8; 32], ZipFailure>) {
    let mut sink = match sink_of(zip) {
        Ok(sink) => sink,
        Err(error) => return (None, Err(ZipFailure::Sink(error))),
    };
    let result = async {
        let entry = zip_entry(path, modified_ms);
        let sink_error = |error: async_zip::error::ZipError| ZipFailure::Sink(error.to_string());
        let mut writer = sink.write_entry_stream(entry).await.map_err(sink_error)?;
        let mut hasher = blake3::Hasher::new();
        tokio::pin!(stop);
        loop {
            let next = tokio::select! {
                biased;
                _ = &mut stop => return Err(ZipFailure::Stopped),
                next = stream.next() => next,
            };
            match next {
                Some(Ok(bytes)) => {
                    hasher.update(&bytes);
                    (writer.write_all(&bytes).await)
                        .map_err(|error| ZipFailure::Sink(error.to_string()))?;
                }
                Some(Err(error)) => {
                    return Err(ZipFailure::Source {
                        corrupt: corrupt_stream(&error),
                        message: error.to_string(),
                    });
                }
                None => break,
            }
        }
        writer.close().await.map_err(sink_error)?;
        Ok(*hasher.finalize().as_bytes())
    }
    .await;
    (Some(ZipWork::new(sink)), result)
}

/// Writes one entry from `stream` to its end and answers how many bytes it holds.
pub async fn zip_counted(
    zip: ZipWork,
    path: &str,
    modified_ms: u64,
    stream: ByteStream,
) -> (Option<ZipWork>, Result<u64, ZipFailure>) {
    let size = Arc::new(AtomicU64::new(0));
    let seen = size.clone();
    let counted = stream.0.inspect(move |item| {
        if let Ok(bytes) = item {
            seen.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
    });
    let counted = BackendStream(Box::pin(counted));
    let (zip, result) = zip_stream(zip, path, modified_ms, counted, std::future::pending()).await;
    (zip, result.map(|_| size.load(Ordering::Relaxed)))
}

/// Ends the archive and its stream.
pub async fn zip_close(zip: ZipWork) -> Result<(), String> {
    let sink = sink_of(zip)?;
    sink.close()
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn corrupt_stream(error: &StreamError) -> bool {
    matches!(
        error.0.downcast_ref::<BlobError>(),
        Some(BlobError::IntegrityCheckFailed(_))
    )
}

/// An archive entry stored as it is.
pub fn zip_entry(path: &str, modified_ms: u64) -> ZipEntryBuilder {
    // A regular file readable by everyone, so an unpacked archive opens without a chmod.
    ZipEntryBuilder::new(path.to_string().into(), Compression::Stored)
        .last_modification_date(zip_date(modified_ms))
        .unix_permissions(0o100_644)
}

/// MS-DOS ZIP timestamps only cover 1980 through 2107, so a moment outside
/// that window keeps its month and day but clamps to the nearest year.
pub fn zip_date(modified_ms: u64) -> ZipDateTime {
    let Some(moment) = i64::try_from(modified_ms)
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
    else {
        return ZipDateTime::default();
    };
    ZipDateTimeBuilder::new()
        .year(moment.year().clamp(1980, 2107))
        .month(moment.month())
        .day(moment.day())
        .hour(moment.hour())
        .minute(moment.minute())
        .second(moment.second())
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seek_checks_bounds() {
        assert_eq!(seek_offset(4, 10, SeekFrom::Current(3)).unwrap(), 7);
        assert_eq!(seek_offset(4, 10, SeekFrom::End(-2)).unwrap(), 8);
        assert!(seek_offset(0, 10, SeekFrom::Current(-1)).is_err());
        assert!(seek_offset(0, 10, SeekFrom::Start(11)).is_err());
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
