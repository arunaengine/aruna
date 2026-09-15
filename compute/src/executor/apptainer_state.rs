use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use aruna_core::compute::{AttemptPhase, BackendError, FenceContext, TombstoneEvidence};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::executor::control_store::{
    self, ControlError, ControlGuard as StoredGuard, ControlState,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ControlRecord {
    pub attempt_epoch: u64,
    pub highest_generation: u64,
    pub cancel: bool,
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
    /// Always bounded: an attempt with no ceiling is refused before launch.
    pub memory_bytes: u64,
    pub cpu_cores: u32,
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
    #[serde(default)]
    pub started_at_ms: Option<u64>,
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
        let guard = StoredGuard::open(&directory, context, |attempt_epoch, highest_generation| {
            ControlRecord {
                attempt_epoch,
                highest_generation,
                cancel: false,
                tombstone_ref: None,
            }
        })
        .map_err(control_error)?;
        Ok(ControlGuard(guard))
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

pub struct ControlGuard(StoredGuard<ControlRecord>);

impl ControlGuard {
    pub fn tombstone(&self) -> Option<TombstoneEvidence> {
        let record = self.0.record()?;
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
            .0
            .record_mut()
            .ok_or_else(|| BackendError::Api("missing Apptainer control record".to_string()))?;
        record.cancel = true;
        self.0.persist().map_err(control_error)
    }

    pub fn store(&mut self, reference: String) -> Result<TombstoneEvidence, BackendError> {
        let record = self
            .0
            .record_mut()
            .ok_or_else(|| BackendError::Api("missing Apptainer control record".to_string()))?;
        record.tombstone_ref = Some(reference.clone());
        let attempt_epoch = record.attempt_epoch;
        self.0.persist().map_err(control_error)?;
        Ok(TombstoneEvidence {
            backend_ref: reference,
            attempt_epoch,
        })
    }
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, BackendError> {
    control_store::read_required(path).map_err(control_error)
}

pub fn read_optional<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, BackendError> {
    control_store::read_optional(path).map_err(control_error)
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), BackendError> {
    control_store::write_json(path, value).map_err(control_error)
}

pub fn sync_dir(path: &Path) -> Result<(), BackendError> {
    control_store::sync_dir(path).map_err(control_error)
}

fn control_error(error: ControlError) -> BackendError {
    match error {
        ControlError::Io(error) => state_error(error),
        ControlError::Decode(error) => {
            BackendError::Api(format!("decode Apptainer state: {error}"))
        }
        ControlError::Encode(error) => {
            BackendError::Api(format!("serialize Apptainer state: {error}"))
        }
        ControlError::NoParent => BackendError::Api("state path has no parent".to_string()),
        ControlError::Missing => BackendError::Api("missing control record".to_string()),
        ControlError::EpochMismatch => BackendError::Conflict("attempt epoch mismatch".to_string()),
        ControlError::Fenced => BackendError::Fenced,
    }
}

fn state_error(error: std::io::Error) -> BackendError {
    BackendError::Api(format!("Apptainer state: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_start_optional() {
        // A payload written before the start time existed must still decode.
        let record = PayloadRecord {
            process: ProcessRecord {
                pid: 7,
                start_ticks: 9,
            },
            cgroup: PathBuf::from("/sys/fs/cgroup/attempt"),
            started_at_ms: Some(1_700_000_000_000),
        };
        let encoded = serde_json::to_vec(&record).expect("encode payload");

        let decoded: PayloadRecord = serde_json::from_slice(&encoded).expect("decode payload");
        assert_eq!(decoded.started_at_ms, Some(1_700_000_000_000));

        let legacy = br#"{"process":{"pid":7,"start_ticks":9},"cgroup":"/c"}"#;
        let decoded: PayloadRecord = serde_json::from_slice(legacy).expect("decode legacy payload");
        assert_eq!(decoded.started_at_ms, None);
    }
}
