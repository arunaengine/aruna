//! Spools a byte stream to an unnamed local temporary file, so its hashes are known before any
//! byte is served. The file, its byte permit and file slot live until its read stream ends.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The read stream may also drop; a crash leaves nothing, since the file has no name.

use super::BlobHandler;
use super::runtime::SPOOL_BUDGET_MIB;
use crate::hash::Hasher;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::{BackendStream, StreamError};
use bytes::Bytes;
use futures::{StreamExt, stream};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::Path;
use std::time::Instant as StdInstant;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::{Instant, timeout, timeout_at};

/// Spool files open at once, each until its read stream ends.
pub(super) const SPOOL_FILES: usize = 256;
const MIB: u64 = 1024 * 1024;
/// Bytes read from a spool file per chunk of its stream.
const READ_CHUNK: u64 = 256 * 1024;

/// One spool file with the capacity it holds. Fields drop in order, so the file is closed
/// before its permits return; every file call moves the whole value to a blocking thread
/// and back, so a dropped stream never releases capacity while that thread still writes.
struct SpoolFile {
    file: File,
    _bytes: OwnedSemaphorePermit,
    _slot: OwnedSemaphorePermit,
}

/// Creates `dir` when missing and checks that it takes unnamed files.
pub(super) fn prepare_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    unnamed_file(dir).map(drop)
}

/// An unnamed file in `dir`; it is refused unless the system confirms it has no name left.
fn unnamed_file(dir: &Path) -> std::io::Result<File> {
    let file = tempfile::tempfile_in(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if file.metadata()?.nlink() != 0 {
            return Err(std::io::Error::other("the spool file kept a name"));
        }
    }
    Ok(file)
}

/// Runs `work` on the spool file on a blocking thread and hands the file back.
async fn on_file<T, F>(spool: SpoolFile, work: F) -> Result<(SpoolFile, T), BlobError>
where
    T: Send + 'static,
    F: FnOnce(&mut File) -> std::io::Result<T> + Send + 'static,
{
    let joined = tokio::task::spawn_blocking(move || {
        let mut spool = spool;
        work(&mut spool.file).map(|value| (spool, value))
    })
    .await;
    match joined {
        Ok(Ok(done)) => Ok(done),
        Ok(Err(error)) => Err(BlobError::WriteError(format!("spool file failed: {error}"))),
        Err(error) => Err(BlobError::WriteError(format!(
            "spool file task failed: {error}"
        ))),
    }
}

/// Streams `range` of the spool file; the file and its capacity go when the stream ends.
fn read_stream(spool: SpoolFile, range: Range<u64>) -> BackendStream<Result<Bytes, StreamError>> {
    let chunks = stream::try_unfold(
        (Some(spool), range.start, range.end),
        |(spool, position, end)| async move {
            let Some(spool) = spool.filter(|_| position < end) else {
                return Ok(None);
            };
            let length = (end - position).min(READ_CHUNK);
            let (spool, bytes) = on_file(spool, move |file| {
                file.seek(SeekFrom::Start(position))?;
                let mut bytes = vec![0; length as usize];
                file.read_exact(&mut bytes)?;
                Ok(bytes)
            })
            .await
            .map_err(|error| BlobError::ReadError(error.reason()))?;
            let next = position + length;
            let spool = (next < end).then_some(spool);
            Ok::<_, BlobError>(Some((Bytes::from(bytes), (spool, next, end))))
        },
    );
    BackendStream::new(chunks)
}

impl BlobHandler {
    /// Spools exactly `size` bytes of `blob`, then answers their hashes and a stream of
    /// `range`. Capacity is taken before the first write and refused, never awaited.
    pub(super) async fn spool_local(
        &self,
        size: u64,
        deadline: Option<StdInstant>,
        range: Option<Range<u64>>,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let limit = SPOOL_BUDGET_MIB as u64 * MIB;
        if size > limit {
            return BlobEvent::Error(BlobError::SpoolTooLarge { limit });
        }
        let range = range.unwrap_or(0..size);
        if range.start > range.end || range.end > size {
            return BlobEvent::Error(BlobError::ReadError(
                "the spool range is outside the spooled bytes".to_string(),
            ));
        }
        let Some(dir) = self.spool_dir.clone() else {
            return BlobEvent::Error(BlobError::WriteError(
                "this node has no spool directory".to_string(),
            ));
        };
        let units = u32::try_from(size.div_ceil(MIB)).unwrap_or(u32::MAX);
        let Ok(bytes) = self.spool_budget.clone().try_acquire_many_owned(units) else {
            return BlobEvent::Error(BlobError::SpoolBudgetExhausted { limit });
        };
        let Ok(slot) = self.spool_files.clone().try_acquire_owned() else {
            return BlobEvent::Error(BlobError::SpoolBudgetExhausted { limit });
        };
        // The permits travel with the opening, so a cancelled read releases them only after it.
        let opened = tokio::task::spawn_blocking(move || {
            unnamed_file(&dir).map(|file| SpoolFile {
                file,
                _bytes: bytes,
                _slot: slot,
            })
        })
        .await;
        let spool = match opened {
            Ok(Ok(spool)) => spool,
            Ok(Err(error)) => {
                return BlobEvent::Error(BlobError::WriteError(format!(
                    "spool file was not created: {error}"
                )));
            }
            Err(error) => return BlobEvent::Error(BlobError::WriteError(error.to_string())),
        };
        let fill = self.fill_spool(spool, size, blob);
        let filled = match deadline {
            Some(deadline) => timeout_at(Instant::from_std(deadline), fill)
                .await
                .unwrap_or_else(|_| {
                    Err(BlobError::WriteError("spool deadline expired".to_string()))
                }),
            None => fill.await,
        };
        match filled {
            Ok((spool, hasher)) => BlobEvent::LocalSpooled {
                hashes: hasher.to_map(),
                size,
                blob: read_stream(spool, range),
            },
            Err(error) => BlobEvent::Error(error),
        }
    }

    /// Writes and hashes the whole stream, checking the length before every write.
    async fn fill_spool(
        &self,
        mut spool: SpoolFile,
        size: u64,
        mut blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> Result<(SpoolFile, Hasher), BlobError> {
        let idle = self.transfer_idle_timeout();
        let mut hasher = Hasher::new();
        let mut written = 0u64;
        loop {
            let chunk = timeout(idle, blob.next())
                .await
                .map_err(|_| BlobError::ReadError("spool input idle timeout".to_string()))?;
            let chunk = match chunk {
                Some(Ok(chunk)) => chunk,
                Some(Err(error)) => return Err(BlobError::ReadError(error.to_string())),
                None => break,
            };
            written = written
                .checked_add(chunk.len() as u64)
                .filter(|written| *written <= size)
                .ok_or_else(|| {
                    BlobError::IntegrityCheckFailed("spool input is longer than expected".into())
                })?;
            (spool, hasher) = on_file(spool, move |file| {
                file.write_all(&chunk)?;
                let mut hasher = hasher;
                hasher.update(&chunk);
                Ok(hasher)
            })
            .await?;
        }
        if written != size {
            return Err(BlobError::IntegrityCheckFailed(format!(
                "spool input ended after {written} of {size} bytes"
            )));
        }
        let (spool, ()) = on_file(spool, |file| file.flush()).await?;
        Ok((spool, hasher))
    }
}

#[cfg(test)]
mod tests {
    use super::{prepare_dir, unnamed_file};

    #[test]
    fn spool_files_unnamed() {
        let dir = tempfile::tempdir().unwrap();
        let spool = dir.path().join("spool");
        prepare_dir(&spool).unwrap();
        let _file = unnamed_file(&spool).unwrap();
        assert_eq!(std::fs::read_dir(&spool).unwrap().count(), 0);
    }

    #[test]
    fn refuses_missing_parent() {
        let dir = tempfile::tempdir().unwrap();
        let blocked = dir.path().join("file");
        std::fs::write(&blocked, b"x").unwrap();
        assert!(prepare_dir(&blocked.join("spool")).is_err());
    }
}
