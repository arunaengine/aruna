use std::collections::BTreeSet;
use std::path::PathBuf;

use aruna_core::compute::{
    BackendError, InputStream, StagingMode, TaskSpec, normalize_container_path, paths_overlap,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageLayout {
    pub files: Vec<StagedPath>,
    pub output_parents: BTreeSet<PathBuf>,
    pub mode: StagingMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedPath {
    pub path: PathBuf,
    pub size: u64,
    pub workspace_key: String,
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
            _ => {}
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
        for input in &files {
            for parent in &output_parents {
                if paths_overlap(
                    input.path.to_str().ok_or_else(|| {
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
            output_parents,
            mode: spec.staging_mode,
        })
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
    use aruna_core::compute::{AttemptRef, TaskInput, TaskSpec};

    use super::*;

    #[test]
    fn rejects_path_overlap() {
        let mut spec = TaskSpec::new(AttemptRef::new("job", 0), "image");
        spec.inputs = vec![TaskInput::from_bytes("/output/input", "data")];
        spec.output_paths = vec!["/output/result".to_string()];
        assert!(StageLayout::from_spec(&spec).is_err());

        let mut spec = TaskSpec::new(AttemptRef::new("job", 0), "image");
        spec.inputs = vec![TaskInput::from_bytes("/data", "data")];
        spec.output_paths = vec!["/data/output/result".to_string()];
        assert!(StageLayout::from_spec(&spec).is_err());
    }
}
