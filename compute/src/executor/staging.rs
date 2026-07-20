use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use aruna_core::compute::{
    BackendError, InputStream, StagingMode, TaskSpec, normalize_container_path, paths_overlap,
};
use aruna_core::structs::ensure_confined_relative_path;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageLayout {
    pub files: Vec<StagedPath>,
    pub mounts: Vec<MountedPath>,
    pub output_parents: BTreeSet<PathBuf>,
    pub mode: StagingMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedPath {
    pub path: PathBuf,
    pub size: u64,
    pub workspace_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MountedPath {
    pub path: PathBuf,
    pub bucket: String,
    pub key: String,
}

pub struct StagePlan {
    pub layout: StageLayout,
    pub entries: Vec<StageEntry>,
}

pub struct StageEntry {
    pub path: PathBuf,
    pub size: u64,
    pub workspace_key: String,
    pub stream: InputStream,
}

impl StageLayout {
    pub fn from_spec(spec: &TaskSpec) -> Result<Self, BackendError> {
        match spec.staging_mode {
            StagingMode::Files if spec.workspace.is_some() || !spec.secret_env.is_empty() => {
                return Err(BackendError::InvalidSpec(
                    "Files staging must not include task credentials".to_string(),
                ));
            }
            StagingMode::DirectS3 if !spec.inputs.is_empty() => {
                return Err(BackendError::InvalidSpec(
                    "DirectS3 staging must not include container file inputs".to_string(),
                ));
            }
            StagingMode::S3Mount if !spec.inputs.is_empty() || spec.workspace.is_some() => {
                return Err(BackendError::InvalidSpec(
                    "S3Mount staging must not include copied inputs or a workspace binding"
                        .to_string(),
                ));
            }
            _ => {}
        }
        if spec.staging_mode != StagingMode::S3Mount && !spec.s3_mounts.is_empty() {
            return Err(BackendError::InvalidSpec(
                "S3 mounts require S3Mount staging".to_string(),
            ));
        }
        if spec.staging_mode == StagingMode::S3Mount
            && !spec.s3_mounts.is_empty()
            && spec.secret_env.is_empty()
        {
            return Err(BackendError::InvalidSpec(
                "S3 mounts require credentials".to_string(),
            ));
        }
        if spec.security.read_only_rootfs && !spec.output_paths.is_empty() {
            return Err(BackendError::InvalidSpec(
                "file outputs require a writable root filesystem".to_string(),
            ));
        }
        let mut files = Vec::with_capacity(spec.inputs.len());
        let mut input_paths = BTreeSet::new();
        for input in &spec.inputs {
            let path = normalize_container_path(&input.path).map_err(BackendError::InvalidSpec)?;
            if !input_paths.insert(path.clone()) {
                return Err(BackendError::InvalidSpec(format!(
                    "duplicate input path `{}`",
                    input.path
                )));
            }
            files.push(StagedPath {
                path,
                size: input.size(),
                workspace_key: input.workspace_key.clone(),
            });
        }
        let mut mounts = Vec::with_capacity(spec.s3_mounts.len());
        for mount in &spec.s3_mounts {
            let path = normalize_container_path(&mount.path).map_err(BackendError::InvalidSpec)?;
            ensure_confined_relative_path(Path::new(&mount.key))
                .map_err(|error| BackendError::InvalidSpec(error.to_string()))?;
            if mount.bucket.is_empty() || !input_paths.insert(path.clone()) {
                return Err(BackendError::InvalidSpec(format!(
                    "duplicate or invalid S3 mount path `{}`",
                    mount.path
                )));
            }
            mounts.push(MountedPath {
                path,
                bucket: mount.bucket.clone(),
                key: mount.key.clone(),
            });
        }
        for path in &input_paths {
            if input_paths
                .iter()
                .any(|other| other != path && path.starts_with(other))
            {
                return Err(BackendError::InvalidSpec(format!(
                    "input path `{}` is nested below another input",
                    path.display()
                )));
            }
        }

        let mut outputs = BTreeSet::new();
        let mut output_parents = BTreeSet::new();
        for output in &spec.output_paths {
            let path = normalize_container_path(output).map_err(BackendError::InvalidSpec)?;
            if !outputs.insert(path.clone()) {
                return Err(BackendError::InvalidSpec(format!(
                    "duplicate output path `{output}`"
                )));
            }
            let parent = path.parent().ok_or_else(|| {
                BackendError::InvalidSpec(format!("output path `{output}` has no parent"))
            })?;
            if parent == std::path::Path::new("/") {
                return Err(BackendError::InvalidSpec(
                    "root output parent is forbidden".to_string(),
                ));
            }
            output_parents.insert(parent.to_path_buf());
        }
        for input in files
            .iter()
            .map(|input| &input.path)
            .chain(mounts.iter().map(|mount| &mount.path))
        {
            if outputs.contains(input) {
                return Err(BackendError::InvalidSpec(
                    "input and output paths overlap".to_string(),
                ));
            }
            for parent in &output_parents {
                if paths_overlap(
                    input.to_str().ok_or_else(|| {
                        BackendError::InvalidSpec("input path is not UTF-8".to_string())
                    })?,
                    parent.to_str().ok_or_else(|| {
                        BackendError::InvalidSpec("output path is not UTF-8".to_string())
                    })?,
                )
                .map_err(BackendError::InvalidSpec)?
                {
                    return Err(BackendError::InvalidSpec(
                        "input and output paths overlap".to_string(),
                    ));
                }
            }
        }
        Ok(Self {
            files,
            mounts,
            output_parents,
            mode: spec.staging_mode,
        })
    }

    pub fn digest(&self) -> String {
        let mut rows = self
            .files
            .iter()
            .map(|file| {
                format!(
                    "i\0{}\0{}\0{}",
                    file.path.display(),
                    file.size,
                    file.workspace_key
                )
            })
            .collect::<Vec<_>>();
        rows.extend(self.mounts.iter().map(|mount| {
            format!(
                "m\0{}\0{}\0{}",
                mount.path.display(),
                mount.bucket,
                mount.key
            )
        }));
        rows.extend(
            self.output_parents
                .iter()
                .map(|path| format!("o\0{}", path.display())),
        );
        rows.sort();
        let mut hasher = blake3::Hasher::new();
        hasher.update(match self.mode {
            StagingMode::Files => b"files",
            StagingMode::DirectS3 => b"direct-s3",
            StagingMode::S3Mount => b"s3-mount",
        });
        for row in rows {
            hasher.update(row.as_bytes());
            hasher.update(&[0]);
        }
        hasher.finalize().to_hex().to_string()
    }
}

impl StagePlan {
    pub fn from_spec(spec: &TaskSpec) -> Result<Self, BackendError> {
        let layout = StageLayout::from_spec(spec)?;
        let entries = spec
            .inputs
            .iter()
            .map(|input| {
                let stream = input.take_stream().ok_or_else(|| {
                    BackendError::Unavailable(format!(
                        "input `{}` stream is already consumed",
                        input.path
                    ))
                })?;
                Ok(StageEntry {
                    path: normalize_container_path(&input.path)
                        .map_err(BackendError::InvalidSpec)?,
                    size: input.size(),
                    workspace_key: input.workspace_key.clone(),
                    stream,
                })
            })
            .collect::<Result<Vec<_>, BackendError>>()?;
        Ok(Self { layout, entries })
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::compute::{AttemptRef, S3Mount, Secret, TaskInput, TaskSpec};

    use super::*;

    #[test]
    fn rejects_path_overlap() {
        let mut spec = TaskSpec::new(AttemptRef::new("job", 0), "image");
        spec.inputs = vec![TaskInput::from_bytes("/output/result", "data")];
        spec.output_paths = vec!["/output/result".to_string()];
        assert!(StageLayout::from_spec(&spec).is_err());

        let mut spec = TaskSpec::new(AttemptRef::new("job", 0), "image");
        spec.inputs = vec![TaskInput::from_bytes("/data", "data")];
        spec.output_paths = vec!["/data/output/result".to_string()];
        assert!(StageLayout::from_spec(&spec).is_err());
    }

    #[test]
    fn allows_shared_workdir() {
        let mut spec = TaskSpec::new(AttemptRef::new("job", 0), "image");
        spec.inputs = vec![TaskInput::from_bytes("/work/.command.sh", "data")];
        spec.output_paths = vec!["/work/out.txt".to_string()];
        assert!(StageLayout::from_spec(&spec).is_ok());
    }

    #[test]
    fn rejects_mount_escape() {
        let mut spec = TaskSpec::new(AttemptRef::new("job", 0), "image");
        spec.staging_mode = StagingMode::S3Mount;
        spec.s3_mounts.push(S3Mount {
            bucket: "bucket".to_string(),
            key: "../escape".to_string(),
            path: "/input".to_string(),
        });
        spec.secret_env
            .insert("access_key_id".to_string(), Secret::new("key"));

        assert!(StageLayout::from_spec(&spec).is_err());
    }
}
