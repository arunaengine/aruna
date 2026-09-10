use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use aruna_core::compute::FenceContext;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Filesystem, decode or fencing fault a stored control record reports back.
pub(crate) enum ControlError {
    Io(std::io::Error),
    Decode(serde_json::Error),
    Encode(serde_json::Error),
    NoParent,
    Missing,
    EpochMismatch,
    Fenced,
}

/// Control record fields that generation acceptance validates and advances.
pub(crate) trait ControlState {
    fn attempt_epoch(&self) -> u64;
    fn highest_generation(&self) -> u64;
    fn set_highest_generation(&mut self, generation: u64);
}

pub(crate) struct ControlGuard<T> {
    _lock: File,
    path: PathBuf,
    record: Option<T>,
}

impl<T: ControlState + Serialize + DeserializeOwned> ControlGuard<T> {
    /// Locks the attempt's control directory and admits `context`, creating the
    /// record through `fresh` when none is stored yet.
    pub(crate) fn open(
        directory: &Path,
        context: &FenceContext,
        fresh: impl FnOnce(u64, u64) -> T,
    ) -> Result<Self, ControlError> {
        std::fs::create_dir_all(directory).map_err(ControlError::Io)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("control.lock"))
            .map_err(ControlError::Io)?;
        lock.lock().map_err(ControlError::Io)?;
        let path = directory.join("control.json");
        let record = read_optional(&path)?;
        let mut guard = Self {
            _lock: lock,
            path,
            record,
        };
        guard.accept(context, fresh)?;
        Ok(guard)
    }

    fn accept(
        &mut self,
        context: &FenceContext,
        fresh: impl FnOnce(u64, u64) -> T,
    ) -> Result<(), ControlError> {
        match self.record.as_mut() {
            Some(record) if record.attempt_epoch() != context.attempt_epoch => {
                return Err(ControlError::EpochMismatch);
            }
            Some(record) if context.controller_generation < record.highest_generation() => {
                return Err(ControlError::Fenced);
            }
            Some(record) if context.controller_generation > record.highest_generation() => {
                record.set_highest_generation(context.controller_generation);
                self.persist()?;
            }
            Some(_) => {}
            None => {
                self.record = Some(fresh(context.attempt_epoch, context.controller_generation));
                self.persist()?;
            }
        }
        Ok(())
    }

    pub(crate) fn record(&self) -> Option<&T> {
        self.record.as_ref()
    }

    pub(crate) fn record_mut(&mut self) -> Option<&mut T> {
        self.record.as_mut()
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn persist(&self) -> Result<(), ControlError> {
        let record = self.record.as_ref().ok_or(ControlError::Missing)?;
        write_json(&self.path, record)
    }
}

pub(crate) fn read_required<T: DeserializeOwned>(path: &Path) -> Result<T, ControlError> {
    let mut file = File::open(path).map_err(ControlError::Io)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(ControlError::Io)?;
    serde_json::from_slice(&bytes).map_err(ControlError::Decode)
}

pub(crate) fn read_optional<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, ControlError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ControlError::Io(error)),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(ControlError::Io)?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(ControlError::Decode)
}

pub(crate) fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), ControlError> {
    let bytes = serde_json::to_vec(value).map_err(ControlError::Encode)?;
    let temp = path.with_extension("json.tmp");
    let mut file = File::create(&temp).map_err(ControlError::Io)?;
    file.write_all(&bytes).map_err(ControlError::Io)?;
    file.sync_all().map_err(ControlError::Io)?;
    std::fs::rename(&temp, path).map_err(ControlError::Io)?;
    let parent = path.parent().ok_or(ControlError::NoParent)?;
    sync_dir(parent)
}

pub(crate) fn sync_dir(path: &Path) -> Result<(), ControlError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(ControlError::Io)
}
