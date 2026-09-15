use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use aruna_core::compute::{BackendError, FenceContext, TombstoneEvidence};
use serde::{Deserialize, Serialize};

use crate::executor::control_store::{
    self, ControlError, ControlGuard as StoredGuard, ControlState,
};

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
        let guard = StoredGuard::open(&directory, context, |attempt_epoch, highest_generation| {
            ControlRecord {
                attempt_epoch,
                highest_generation,
                started: false,
                tombstone: false,
                tombstone_ref: None,
            }
        })
        .map_err(control_error)?;
        Ok(ControlGuard(guard))
    }

    pub fn read(&self, context: &FenceContext) -> Result<Option<ControlRecord>, BackendError> {
        let path = self
            .root
            .join("attempts")
            .join(context.attempt.external_name())
            .join("control.json");
        control_store::read_optional(&path).map_err(control_error)
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
    pub started: bool,
    pub tombstone: bool,
    pub tombstone_ref: Option<String>,
}

impl ControlState for ControlRecord {
    fn attempt_epoch(&self) -> u64 {
        self.attempt_epoch
    }

    fn highest_generation(&self) -> u64 {
        self.highest_generation
    }

    fn set_highest_generation(&mut self, generation: u64) {
        self.highest_generation = generation;
    }
}

pub struct ControlGuard(StoredGuard<ControlRecord>);

impl ControlGuard {
    pub fn started(&self) -> bool {
        self.0.record().is_some_and(|record| record.started)
    }

    /// Fences the start request before the daemon is asked to start: from here
    /// on an absent container is lost evidence, never a licence to create one.
    pub fn mark_start(&mut self) -> Result<(), BackendError> {
        let record = self.0.record_mut().ok_or_else(missing_record)?;
        if record.started {
            return Ok(());
        }
        record.started = true;
        self.0.persist().map_err(control_error)
    }

    pub fn tombstone(&self) -> Option<TombstoneEvidence> {
        let record = self.0.record()?;
        record.tombstone.then(|| TombstoneEvidence {
            backend_ref: record
                .tombstone_ref
                .clone()
                .unwrap_or_else(|| self.0.path().display().to_string()),
            attempt_epoch: record.attempt_epoch,
        })
    }

    pub fn store(&mut self, backend_ref: String) -> Result<TombstoneEvidence, BackendError> {
        let record = self.0.record_mut().ok_or_else(missing_record)?;
        record.tombstone = true;
        record.tombstone_ref = Some(backend_ref.clone());
        let attempt_epoch = record.attempt_epoch;
        self.0.persist().map_err(control_error)?;
        Ok(TombstoneEvidence {
            backend_ref,
            attempt_epoch,
        })
    }
}

fn missing_record() -> BackendError {
    BackendError::Api("missing Docker control record".to_string())
}

fn control_error(error: ControlError) -> BackendError {
    match error {
        ControlError::Io(error) => api_error(error),
        ControlError::Decode(error) => BackendError::Api(format!("decode Docker control: {error}")),
        ControlError::Encode(error) => {
            BackendError::Api(format!("serialize Docker control: {error}"))
        }
        ControlError::NoParent => BackendError::Api("control path has no parent".to_string()),
        ControlError::Missing => missing_record(),
        ControlError::EpochMismatch => BackendError::Conflict("attempt epoch mismatch".to_string()),
        ControlError::Fenced => BackendError::Fenced,
    }
}

fn sync_dir(path: &Path) -> Result<(), BackendError> {
    control_store::sync_dir(path).map_err(control_error)
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
    fn records_start_durably() {
        // A recorded start must survive the guard so a resubmit cannot re-create.
        let directory = tempdir().unwrap();
        let daemon = DaemonLock::acquire(directory.path()).unwrap();
        let mut control = daemon.control(&fence(1)).unwrap();
        assert!(!control.started());
        control.mark_start().unwrap();
        drop(control);
        assert!(daemon.control(&fence(2)).unwrap().started());
    }

    #[test]
    fn retains_tombstone() {
        let directory = tempdir().unwrap();
        let daemon = DaemonLock::acquire(directory.path()).unwrap();
        let mut control = daemon.control(&fence(1)).unwrap();
        control.store("control.json".to_string()).unwrap();
        drop(control);
        assert!(daemon.control(&fence(2)).unwrap().tombstone().is_some());
    }
}
