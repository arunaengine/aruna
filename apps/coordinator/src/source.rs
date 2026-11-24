//! Writes the files of a workflow into a private folder and prepares the uwe settings that read
//! them there, with no config file of the process user and no reach outside the folder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use aruna_workflow_contract::ContractError;
use aruna_workflow_contract::preflight::{WorkflowLanguage, WorkflowSource, check_path};
use aruna_workflow_contract::settings::ArunaStepSettings;
use uwe::hosted::Isolation;
use uwe::run::{Language, SecretValues, Settings};

use crate::composed;

/// The folder of one request or run: the workflow files and uwe's own folders beside them.
pub struct Workspace {
    folder: tempfile::TempDir,
    /// The entry file of the workflow inside `workflow`.
    pub entry: PathBuf,
    /// The `aruna` settings that a composed entry file held, by step id.
    pub step_settings: BTreeMap<String, ArunaStepSettings>,
}

impl Workspace {
    /// Writes the files below a new folder. A composed entry file loses its `aruna` keys,
    /// which uwe does not read; their settings stay in `step_settings`.
    pub fn create(source: &WorkflowSource, entrypoint: &str) -> Result<Workspace, ContractError> {
        source.validate()?;
        check_path(entrypoint)?;
        if source.file(entrypoint).is_none() {
            return Err(ContractError::invalid(format!(
                "the workflow has no file {entrypoint}"
            )));
        }
        let folder = tempfile::Builder::new()
            .prefix("aruna-coordinator-")
            .tempdir()
            .map_err(io_error)?;
        let base = folder.path().join("workflow");
        let mut step_settings = BTreeMap::new();
        for file in &source.files {
            let path = base.join(&file.path);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(io_error)?;
            }
            let mut content = file.content.clone();
            if file.path == entrypoint && uwe::tools::composed::is_composed(&path) {
                let text = String::from_utf8(content).map_err(|_| {
                    ContractError::workflow("the composed workflow is no UTF-8 text")
                })?;
                let stripped = composed::strip_settings(&text);
                if let Some(error) = stripped.errors().next() {
                    return Err(ContractError::workflow(error.message.clone()));
                }
                step_settings = stripped.settings;
                content = stripped.document.into_bytes();
            }
            std::fs::write(&path, content).map_err(io_error)?;
            let mode = if file.executable { 0o755 } else { 0o644 };
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))
                .map_err(io_error)?;
        }
        for name in ["cache", "store", "outputs", "launch", "journals", "scratch"] {
            std::fs::create_dir_all(folder.path().join(name)).map_err(io_error)?;
        }
        Ok(Workspace {
            entry: base.join(entrypoint),
            folder,
            step_settings,
        })
    }

    pub fn path(&self) -> &Path {
        self.folder.path()
    }

    /// The folder for attempts and other files of the run, like downloaded outputs.
    pub fn scratch(&self) -> PathBuf {
        self.folder.path().join("scratch")
    }

    /// Settings that read the workflow here, keep every uwe folder here and read no config
    /// file. The caller adds the executor, the stores and the values.
    pub fn settings(
        &self,
        language: Option<WorkflowLanguage>,
        secrets: &SecretValues,
    ) -> Result<Settings, ContractError> {
        let root = self.path();
        let mut settings = Settings::new(&self.entry);
        settings.language = language.map(uwe_language);
        settings.setup = Some(
            uwe::setup::Setup::from_files(&[], secrets)
                .map_err(|failure| ContractError::internal(failure.to_string()))?,
        );
        settings.secrets = secrets.clone();
        settings.cache_folder = Some(root.join("cache"));
        settings.journal_folder = Some(root.join("journals"));
        settings.output_dir = Some(root.join("outputs"));
        settings.store = Some(root.join("store").to_string_lossy().into_owned());
        settings.isolation = Some(Isolation {
            roots: vec![root.to_owned()],
            launch: Some(root.join("launch")),
            variables: ["PATH".to_owned()].into(),
            ..Isolation::default()
        });
        Ok(settings)
    }
}

pub fn uwe_language(language: WorkflowLanguage) -> Language {
    match language {
        WorkflowLanguage::Nextflow => Language::Nextflow,
        WorkflowLanguage::Snakemake => Language::Snakemake,
        WorkflowLanguage::Cwl => Language::Cwl,
        WorkflowLanguage::Argo => Language::Argo,
        WorkflowLanguage::Wdl => Language::Wdl,
        WorkflowLanguage::Wir => Language::Wir,
        WorkflowLanguage::Galaxy => Language::Galaxy,
    }
}

pub fn io_error(error: std::io::Error) -> ContractError {
    ContractError::internal(format!("the coordinator folder failed: {error}"))
}
