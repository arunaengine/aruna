//! Builds the input tar stream and reads output files back out of a container.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use aruna_core::compute::{
    BackendError, InputStream, MAX_OUTPUT_MATCHES, MAX_TRANSFER_BYTES, OutputMatcher, TaskInput,
    TaskSpec,
};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio_util::io::{StreamReader, SyncIoBridge};

use super::super::staging::StageLayout;

pub(super) fn container_path(path: &str) -> Result<PathBuf, BackendError> {
    let Some(relative) = path.strip_prefix('/') else {
        return Err(BackendError::InvalidSpec(format!(
            "container path `{path}` is not absolute"
        )));
    };
    if relative.is_empty()
        || relative
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | "..") || part.contains('\0'))
    {
        return Err(BackendError::InvalidSpec(format!(
            "container path `{path}` is not a safe file path"
        )));
    }
    Ok(PathBuf::from(relative))
}

fn add_parents(path: &Path, mode: u32, directories: &mut BTreeMap<PathBuf, u32>) {
    let mut parent = path.parent();
    while let Some(path) = parent {
        if path.as_os_str().is_empty() {
            break;
        }
        directories
            .entry(path.to_path_buf())
            .and_modify(|current| *current = (*current).max(mode))
            .or_insert(mode);
        parent = path.parent();
    }
}

pub(super) struct ArchivePlan<'a> {
    pub(super) inputs: BTreeMap<PathBuf, &'a TaskInput>,
    pub(super) outputs: BTreeSet<PathBuf>,
    pub(super) directories: BTreeMap<PathBuf, u32>,
}

impl<'a> ArchivePlan<'a> {
    pub(super) fn new(spec: &'a TaskSpec) -> Result<Self, BackendError> {
        StageLayout::from_spec(spec)?;
        let mut inputs = BTreeMap::new();
        let mut outputs = BTreeSet::new();
        let mut files = BTreeSet::new();
        let mut input_bytes = 0u64;
        for input in &spec.inputs {
            let path = container_path(&input.path)?;
            if inputs.insert(path.clone(), input).is_some() || !files.insert(path) {
                return Err(BackendError::InvalidSpec(format!(
                    "duplicate input path `{}`",
                    input.path
                )));
            }
            input_bytes = input_bytes
                .checked_add(input.size())
                .filter(|total| *total <= MAX_TRANSFER_BYTES)
                .ok_or_else(transfer_error)?;
        }
        for output in &spec.output_paths {
            let path = container_path(output)?;
            if !outputs.insert(path.clone()) || !files.insert(path) {
                return Err(BackendError::InvalidSpec(format!(
                    "duplicate or conflicting output path `{output}`"
                )));
            }
        }
        for path in &files {
            for parent in path.ancestors().skip(1) {
                if parent.as_os_str().is_empty() {
                    break;
                }
                if files.contains(parent) {
                    return Err(BackendError::InvalidSpec(format!(
                        "container file path `{}` is nested below another file",
                        path.display()
                    )));
                }
            }
        }

        let mut directories = BTreeMap::new();
        for path in inputs.keys() {
            add_parents(path, 0o755, &mut directories);
        }
        for path in &outputs {
            add_parents(path, 0o777, &mut directories);
        }
        // The non-root run identity writes caches and scratch relative to its
        // working directory, so that one ships writable like output parents.
        if let Some(workdir) = &spec.workdir {
            let path = container_path(workdir)?;
            if !files.contains(&path) {
                add_parents(&path, 0o755, &mut directories);
                directories
                    .entry(path)
                    .and_modify(|mode| *mode = (*mode).max(0o777))
                    .or_insert(0o777);
            }
        }

        Ok(Self {
            inputs,
            outputs,
            directories,
        })
    }
}

/// Marker smuggled through `io::Error` when a streaming transfer passes the
/// aggregate byte guard.
#[derive(Debug)]
struct TransferLimitError;

impl std::fmt::Display for TransferLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "task file transfer exceeds the {MAX_TRANSFER_BYTES}-byte limit"
        )
    }
}

impl std::error::Error for TransferLimitError {}

fn transfer_error() -> BackendError {
    BackendError::InvalidSpec(TransferLimitError.to_string())
}

/// Byte-counting tar sink feeding the upload body channel.
struct ChannelWriter {
    tx: mpsc::Sender<io::Result<Bytes>>,
    sent: u64,
    limit: u64,
}

impl Write for ChannelWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.sent = self
            .sent
            .checked_add(bytes.len() as u64)
            .filter(|total| *total <= self.limit)
            .ok_or_else(|| io::Error::other(TransferLimitError))?;
        self.tx
            .blocking_send(Ok(Bytes::copy_from_slice(bytes)))
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Guards a tar entry body: the stream must yield exactly the declared size or
/// the archive would be silently corrupt.
struct ExactReader<R> {
    inner: R,
    remaining: u64,
}

impl<R: Read> Read for ExactReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            let mut probe = [0u8; 1];
            if self.inner.read(&mut probe)? != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "input is larger than its declared size",
                ));
            }
            return Ok(0);
        }
        let cap = buffer
            .len()
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        let read = self.inner.read(&mut buffer[..cap])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "input ended before its declared size",
            ));
        }
        self.remaining -= read as u64;
        Ok(read)
    }
}

pub(super) enum BuildError {
    /// The body receiver went away; the upload error is authoritative.
    Aborted,
    Failed(BackendError),
}

fn input_error(error: io::Error) -> BuildError {
    if error.kind() == io::ErrorKind::BrokenPipe {
        return BuildError::Aborted;
    }
    if error
        .get_ref()
        .is_some_and(|inner| inner.is::<TransferLimitError>())
    {
        return BuildError::Failed(transfer_error());
    }
    BuildError::Failed(BackendError::Unavailable(format!(
        "task input transfer failed: {error}"
    )))
}

pub(super) fn build_archive(
    tx: mpsc::Sender<io::Result<Bytes>>,
    handle: Handle,
    directories: &BTreeMap<PathBuf, u32>,
    files: Vec<(PathBuf, u64, InputStream)>,
    limit: u64,
) -> Result<(), BuildError> {
    let writer = ChannelWriter {
        tx: tx.clone(),
        sent: 0,
        limit,
    };
    let built = write_entries(writer, handle, directories, files);
    // An explicit error chunk aborts the request; a clean close of a truncated
    // body could otherwise be accepted by the daemon as a complete archive.
    if let Err(BuildError::Failed(_)) = &built {
        let _ = tx.blocking_send(Err(io::Error::other("task archive build failed")));
    }
    built
}

fn write_entries(
    writer: ChannelWriter,
    handle: Handle,
    directories: &BTreeMap<PathBuf, u32>,
    files: Vec<(PathBuf, u64, InputStream)>,
) -> Result<(), BuildError> {
    let mut builder = tar::Builder::new(writer);
    for (path, mode) in directories {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Directory);
        header.set_mode(*mode);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_size(0);
        builder
            .append_data(&mut header, path, io::empty())
            .map_err(input_error)?;
    }
    for (path, size, stream) in files {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o444);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_size(size);
        let reader = ExactReader {
            inner: SyncIoBridge::new_with_handle(StreamReader::new(stream), handle.clone()),
            remaining: size,
        };
        builder
            .append_data(&mut header, path, reader)
            .map_err(input_error)?;
    }
    builder.finish().map_err(input_error)
}

fn archive_api(error: impl std::fmt::Display) -> BackendError {
    BackendError::InvalidSpec(format!("invalid Docker output archive: {error}"))
}

pub(super) fn output_spec(message: impl Into<String>) -> BackendError {
    BackendError::InvalidSpec(format!("invalid Docker output: {}", message.into()))
}

/// Recover the classification smuggled through the archive reader: transport
/// faults keep their classified form, everything else is a malformed archive.
fn archive_error(error: io::Error) -> BackendError {
    if error
        .get_ref()
        .is_some_and(|inner| inner.is::<TransferLimitError>())
    {
        return transfer_error();
    }
    match error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<BackendError>())
    {
        Some(backend) => backend.clone(),
        None => archive_api(error),
    }
}

/// Enforce the aggregate byte guard on raw archive bytes as they stream.
pub(super) fn count_limited<S>(stream: S, limit: u64) -> impl Stream<Item = io::Result<Bytes>>
where
    S: Stream<Item = io::Result<Bytes>>,
{
    let mut total = 0u64;
    stream.map(move |chunk| {
        let chunk = chunk?;
        total = total
            .checked_add(chunk.len() as u64)
            .filter(|total| *total <= limit)
            .ok_or_else(|| io::Error::other(TransferLimitError))?;
        Ok(chunk)
    })
}

fn first_entry<'a, R: Read>(
    entries: &mut tar::Entries<'a, R>,
    expected: &Path,
) -> Result<tar::Entry<'a, R>, BackendError> {
    let entry = entries
        .next()
        .ok_or_else(|| output_spec("archive is empty"))?
        .map_err(archive_error)?;
    {
        let path = entry.path().map_err(archive_api)?;
        if path.is_absolute()
            || path
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
        {
            return Err(output_spec("archive entry path is unsafe"));
        }
        if path != expected {
            return Err(output_spec(format!(
                "archive contains unexpected entry `{}`",
                path.display()
            )));
        }
    }
    if !entry.header().entry_type().is_file() || entry.link_name().map_err(archive_api)?.is_some() {
        return Err(output_spec("declared output is not a regular file"));
    }
    Ok(entry)
}

/// Blocking tar parse: validate the single expected entry, hand its size to the
/// header channel, then stream its bytes chunkwise.
pub(super) fn stream_output(
    archive: impl Read,
    expected: &Path,
    header: oneshot::Sender<Result<u64, BackendError>>,
    tx: mpsc::Sender<Result<Bytes, BackendError>>,
) {
    let mut archive = tar::Archive::new(archive);
    let mut entries = match archive.entries().map_err(archive_error) {
        Ok(entries) => entries,
        Err(error) => {
            let _ = header.send(Err(error));
            return;
        }
    };
    let mut entry = match first_entry(&mut entries, expected) {
        Ok(entry) => entry,
        Err(error) => {
            let _ = header.send(Err(error));
            return;
        }
    };
    let size = entry.size();
    if header.send(Ok(size)).is_err() {
        return;
    }
    let mut streamed = 0u64;
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        match entry.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                streamed += read as u64;
                if tx
                    .blocking_send(Ok(Bytes::copy_from_slice(&buffer[..read])))
                    .is_err()
                {
                    return;
                }
            }
            Err(error) => {
                let _ = tx.blocking_send(Err(archive_error(error)));
                return;
            }
        }
    }
    if streamed != size {
        let _ = tx.blocking_send(Err(output_spec("archive entry is truncated")));
        return;
    }
    drop(entry);
    match entries.next() {
        None => {}
        Some(Ok(_)) => {
            let _ = tx.blocking_send(Err(output_spec("archive contains multiple entries")));
        }
        Some(Err(error)) => {
            let _ = tx.blocking_send(Err(archive_error(error)));
        }
    }
}

/// Collect the regular files of a directory archive that the pattern selects.
/// Entry payloads are skipped; only the headers are inspected.
pub(super) fn list_archive(
    archive: impl Read,
    base: &Path,
    glob: &OutputMatcher,
) -> Result<Vec<String>, BackendError> {
    let mut archive = tar::Archive::new(archive);
    let mut matched = Vec::new();
    for entry in archive.entries().map_err(archive_error)? {
        let entry = entry.map_err(archive_error)?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path = entry.path().map_err(archive_api)?;
        if path
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
        {
            return Err(output_spec("archive entry path is unsafe"));
        }
        // A lossy name would address a file that does not exist, so skip it.
        let joined = base.join(path);
        let Some(absolute) = joined.to_str().map(str::to_string) else {
            tracing::warn!(
                path = %joined.display(),
                "skipping output path that is not UTF-8"
            );
            continue;
        };
        if glob.is_match(&absolute) {
            if matched.len() >= MAX_OUTPUT_MATCHES {
                return Err(output_spec("pattern matches too many files"));
            }
            matched.push(absolute);
        }
    }
    matched.sort();
    Ok(matched)
}
