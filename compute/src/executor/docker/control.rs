use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use aruna_core::compute::{BackendError, FenceContext, TombstoneEvidence};
use serde::{Deserialize, Serialize};

pub struct DaemonLock {
    _file: File,
    root: PathBuf,
}

impl DaemonLock {
    pub fn acquire(state_root: &Path) -> Result<Self, BackendError> {
        let root = state_root.join("docker");
        std::fs::create_dir_all(root.join("attempts")).map_err(api_error)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("daemon.lock"))
            .map_err(api_error)?;
        file.try_lock().map_err(|error| {
            BackendError::Conflict(format!(
                "Docker daemon/state root is already owned: {error}"
            ))
        })?;
        sync_dir(&root)?;
        Ok(Self { _file: file, root })
    }

    pub fn control(&self, context: &FenceContext) -> Result<ControlGuard, BackendError> {
        let directory = self
            .root
            .join("attempts")
            .join(context.attempt.external_name());
        std::fs::create_dir_all(&directory).map_err(api_error)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("control.lock"))
            .map_err(api_error)?;
        lock.lock().map_err(api_error)?;
        let path = directory.join("control.json");
        let record = read_record(&path)?;
        let mut guard = ControlGuard {
            _lock: lock,
            path,
            record,
        };
        guard.accept(context)?;
        Ok(guard)
    }

    pub fn read(&self, context: &FenceContext) -> Result<Option<ControlRecord>, BackendError> {
        let path = self
            .root
            .join("attempts")
            .join(context.attempt.external_name())
            .join("control.json");
        read_record(&path)
    }

    pub fn verify(&self) -> Result<(), BackendError> {
        let path = self.root.join("health.tmp");
        let mut file = File::create(&path).map_err(api_error)?;
        file.write_all(b"ok").map_err(api_error)?;
        file.sync_all().map_err(api_error)?;
        std::fs::remove_file(path).map_err(api_error)?;
        sync_dir(&self.root)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ControlRecord {
    pub attempt_epoch: u64,
    pub highest_generation: u64,
    pub tombstone: bool,
    pub tombstone_ref: Option<String>,
}

pub struct ControlGuard {
    _lock: File,
    path: PathBuf,
    record: Option<ControlRecord>,
}

impl ControlGuard {
    pub fn accept(&mut self, context: &FenceContext) -> Result<(), BackendError> {
        match self.record.as_mut() {
            Some(record) if record.attempt_epoch != context.attempt_epoch => {
                return Err(BackendError::Conflict("attempt epoch mismatch".to_string()));
            }
            Some(record) if context.controller_generation < record.highest_generation => {
                return Err(BackendError::Fenced);
            }
            Some(record) if context.controller_generation > record.highest_generation => {
                record.highest_generation = context.controller_generation;
                self.persist()?;
            }
            Some(_) => {}
            None => {
                self.record = Some(ControlRecord {
                    attempt_epoch: context.attempt_epoch,
                    highest_generation: context.controller_generation,
                    tombstone: false,
                    tombstone_ref: None,
                });
                self.persist()?;
            }
        }
        Ok(())
    }

    pub fn tombstone(&self) -> Option<TombstoneEvidence> {
        let record = self.record.as_ref()?;
        record.tombstone.then(|| TombstoneEvidence {
            backend_ref: record
                .tombstone_ref
                .clone()
                .unwrap_or_else(|| self.path.display().to_string()),
            attempt_epoch: record.attempt_epoch,
        })
    }

    pub fn seal(&mut self, backend_ref: String) -> Result<TombstoneEvidence, BackendError> {
        let record = self
            .record
            .as_mut()
            .ok_or_else(|| BackendError::Api("missing Docker control record".to_string()))?;
        record.tombstone = true;
        record.tombstone_ref = Some(backend_ref.clone());
        let attempt_epoch = record.attempt_epoch;
        self.persist()?;
        Ok(TombstoneEvidence {
            backend_ref,
            attempt_epoch,
        })
    }

    fn persist(&self) -> Result<(), BackendError> {
        let record = self
            .record
            .as_ref()
            .ok_or_else(|| BackendError::Api("missing Docker control record".to_string()))?;
        let bytes = serde_json::to_vec(record)
            .map_err(|error| BackendError::Api(format!("serialize Docker control: {error}")))?;
        let temp = self.path.with_extension("json.tmp");
        let mut file = File::create(&temp).map_err(api_error)?;
        file.write_all(&bytes).map_err(api_error)?;
        file.sync_all().map_err(api_error)?;
        std::fs::rename(&temp, &self.path).map_err(api_error)?;
        sync_dir(
            self.path
                .parent()
                .ok_or_else(|| BackendError::Api("control path has no parent".to_string()))?,
        )
    }
}

fn read_record(path: &Path) -> Result<Option<ControlRecord>, BackendError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(api_error(error)),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(api_error)?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| BackendError::Api(format!("decode Docker control: {error}")))
}

fn sync_dir(path: &Path) -> Result<(), BackendError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(api_error)
}

fn api_error(error: std::io::Error) -> BackendError {
    BackendError::Api(format!("Docker control file: {error}"))
}

#[cfg(test)]
mod tests {
    use aruna_core::compute::AttemptRef;
    use tempfile::tempdir;

    use super::*;

    fn fence(generation: u64) -> FenceContext {
        FenceContext {
            attempt: AttemptRef::new("job", 0),
            attempt_epoch: 7,
            controller_generation: generation,
        }
    }

    #[test]
    fn rejects_stale_generation() {
        let directory = tempdir().unwrap();
        let daemon = DaemonLock::acquire(directory.path()).unwrap();
        drop(daemon.control(&fence(2)).unwrap());
        assert!(matches!(
            daemon.control(&fence(1)),
            Err(BackendError::Fenced)
        ));
    }

    #[test]
    fn retains_tombstone() {
        let directory = tempdir().unwrap();
        let daemon = DaemonLock::acquire(directory.path()).unwrap();
        let mut control = daemon.control(&fence(1)).unwrap();
        control.seal("control.json".to_string()).unwrap();
        drop(control);
        assert!(daemon.control(&fence(2)).unwrap().tombstone().is_some());
    }
}
