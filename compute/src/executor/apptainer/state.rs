use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use aruna_core::compute::{AttemptPhase, BackendError, FenceContext, TombstoneEvidence};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ControlRecord {
    pub attempt_epoch: u64,
    pub highest_generation: u64,
    pub cancel: bool,
    pub tombstone_ref: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttemptRecord {
    pub attempt_epoch: u64,
    pub pinned_image: String,
    pub layout_digest: String,
    pub output_paths: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OciMetadata {
    pub entrypoint: Vec<String>,
    pub command: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BindMount {
    pub host: PathBuf,
    pub container: PathBuf,
    pub writable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LaunchRecord {
    pub sif: PathBuf,
    pub argv: Vec<String>,
    pub binds: Vec<BindMount>,
    pub workdir: Option<String>,
    pub cgroup: PathBuf,
    pub control: PathBuf,
    pub stop_grace_ms: u64,
    pub walltime_ms: Option<u64>,
    pub pids_limit: u64,
    pub memory_bytes: Option<u64>,
    pub cpu_cores: Option<u32>,
    pub isolated_network: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcessRecord {
    pub pid: u32,
    pub start_ticks: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PayloadRecord {
    pub process: ProcessRecord,
    pub cgroup: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusRecord {
    pub phase: AttemptPhase,
    pub started_at_ms: Option<u64>,
    pub finished_at_ms: u64,
}

pub struct StateRoot {
    root: PathBuf,
}

impl StateRoot {
    pub fn open(path: &Path) -> Result<Self, BackendError> {
        std::fs::create_dir_all(path.join("controls")).map_err(state_error)?;
        std::fs::create_dir_all(path.join("attempts")).map_err(state_error)?;
        sync_dir(path)?;
        Ok(Self {
            root: path.to_path_buf(),
        })
    }

    pub fn control(&self, context: &FenceContext) -> Result<ControlGuard, BackendError> {
        let directory = self.control_dir(context);
        std::fs::create_dir_all(&directory).map_err(state_error)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("control.lock"))
            .map_err(state_error)?;
        lock.lock().map_err(state_error)?;
        let path = directory.join("control.json");
        let record = read_optional(&path)?;
        let mut guard = ControlGuard {
            _lock: lock,
            path,
            record,
        };
        guard.accept(context)?;
        Ok(guard)
    }

    pub fn read(&self, context: &FenceContext) -> Result<Option<ControlRecord>, BackendError> {
        read_optional(&self.control_dir(context).join("control.json"))
    }

    pub fn attempt_dir(&self, context: &FenceContext) -> PathBuf {
        self.root
            .join("attempts")
            .join(context.attempt.external_name())
    }

    pub fn control_dir(&self, context: &FenceContext) -> PathBuf {
        self.root
            .join("controls")
            .join(context.attempt.external_name())
    }

    pub fn verify(&self) -> Result<(), BackendError> {
        let path = self.root.join("health.tmp");
        let mut file = File::create(&path).map_err(state_error)?;
        file.write_all(b"ok").map_err(state_error)?;
        file.sync_all().map_err(state_error)?;
        std::fs::remove_file(path).map_err(state_error)?;
        sync_dir(&self.root)
    }
}

pub struct ControlGuard {
    _lock: File,
    path: PathBuf,
    record: Option<ControlRecord>,
}

impl ControlGuard {
    fn accept(&mut self, context: &FenceContext) -> Result<(), BackendError> {
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
                    cancel: false,
                    tombstone_ref: None,
                });
                self.persist()?;
            }
        }
        Ok(())
    }

    pub fn tombstone(&self) -> Option<TombstoneEvidence> {
        let record = self.record.as_ref()?;
        record
            .tombstone_ref
            .as_ref()
            .map(|reference| TombstoneEvidence {
                backend_ref: reference.clone(),
                attempt_epoch: record.attempt_epoch,
            })
    }

    pub fn mark_cancel(&mut self) -> Result<(), BackendError> {
        let record = self
            .record
            .as_mut()
            .ok_or_else(|| BackendError::Api("missing Apptainer control record".to_string()))?;
        record.cancel = true;
        self.persist()
    }

    pub fn seal(&mut self, reference: String) -> Result<TombstoneEvidence, BackendError> {
        let record = self
            .record
            .as_mut()
            .ok_or_else(|| BackendError::Api("missing Apptainer control record".to_string()))?;
        record.tombstone_ref = Some(reference.clone());
        let attempt_epoch = record.attempt_epoch;
        self.persist()?;
        Ok(TombstoneEvidence {
            backend_ref: reference,
            attempt_epoch,
        })
    }
}

impl ControlGuard {
    fn persist(&self) -> Result<(), BackendError> {
        write_json(
            &self.path,
            self.record
                .as_ref()
                .ok_or_else(|| BackendError::Api("missing control record".to_string()))?,
        )
    }
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, BackendError> {
    let mut file = File::open(path).map_err(state_error)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(state_error)?;
    serde_json::from_slice(&bytes)
        .map_err(|error| BackendError::Api(format!("decode Apptainer state: {error}")))
}

pub fn read_optional<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, BackendError> {
    match File::open(path) {
        Ok(mut file) => {
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).map_err(state_error)?;
            serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| BackendError::Api(format!("decode Apptainer state: {error}")))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(state_error(error)),
    }
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), BackendError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| BackendError::Api(format!("serialize Apptainer state: {error}")))?;
    let temp = path.with_extension("json.tmp");
    let mut file = File::create(&temp).map_err(state_error)?;
    file.write_all(&bytes).map_err(state_error)?;
    file.sync_all().map_err(state_error)?;
    std::fs::rename(&temp, path).map_err(state_error)?;
    sync_dir(
        path.parent()
            .ok_or_else(|| BackendError::Api("state path has no parent".to_string()))?,
    )
}

pub fn sync_dir(path: &Path) -> Result<(), BackendError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(state_error)
}

fn state_error(error: std::io::Error) -> BackendError {
    BackendError::Api(format!("Apptainer state: {error}"))
}
