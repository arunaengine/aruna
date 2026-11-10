//! Builds a plugin project with its own toolchain and packages the result as one checked artifact.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use thiserror::Error;
use wasmparser::Parser;

use crate::artifact::{
    SectionError, attach_manifest, check_artifact, component_imports, read_handlers,
};
use crate::limits::RuntimeConfig;
use crate::manifest::{BuildStep, Manifest, ManifestError};
use crate::runtime::Runtime;

/// The target Rust projects without a build step compile to; the host adds no WASI there.
pub const RUST_TARGET: &str = "wasm32-unknown-unknown";

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("plugin.toml: {0}")]
    Manifest(String),
    #[error("{0}")]
    Toolchain(String),
    #[error("the build output is neither a module nor a component: {0}")]
    Output(String),
    #[error("the host cannot run the artifact: {0}")]
    Host(String),
    #[error(transparent)]
    Section(#[from] SectionError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<ManifestError> for BuildError {
    fn from(error: ManifestError) -> Self {
        Self::Manifest(error.to_string())
    }
}

/// A packaged plugin and where it was written.
#[derive(Debug)]
pub struct Built {
    pub manifest: Manifest,
    pub artifact: Vec<u8>,
    pub path: PathBuf,
}

/// Reads and checks a project's `plugin.toml`.
pub fn load_manifest(project: &Path) -> Result<Manifest, BuildError> {
    let text = std::fs::read_to_string(project.join("plugin.toml"))?;
    let manifest: Manifest =
        toml::from_str(&text).map_err(|err| BuildError::Manifest(err.to_string()))?;
    manifest.check()?;
    Ok(manifest)
}

/// Runs the project's build step, or cargo for a Rust project without one, then packages
/// the output and writes `<name>-<version>.wasm` beside it.
pub fn build_project(project: &Path) -> Result<Built, BuildError> {
    let mut manifest = load_manifest(project)?;
    let output = match &manifest.build {
        Some(step) => run_step(project, step)?,
        None if project.join("Cargo.toml").is_file() => cargo_module(&project.join("Cargo.toml"))?,
        None => {
            let hint = "add a [build] section with the command and output of your toolchain";
            return Err(BuildError::Toolchain(hint.into()));
        }
    };
    let bytes = std::fs::read(&output)?;
    let artifact = package(&mut manifest, &bytes)?;
    let path = output.with_file_name(format!("{}-{}.wasm", manifest.name, manifest.version));
    std::fs::write(&path, &artifact)?;
    Ok(Built {
        manifest,
        artifact,
        path,
    })
}

/// Turns a core module with embedded WIT, or a finished component from any toolchain, into
/// an artifact: handlers wired, imports checked, manifest embedded, whole artifact checked.
pub fn package(manifest: &mut Manifest, bytes: &[u8]) -> Result<Vec<u8>, BuildError> {
    let component = if Parser::is_core_wasm(bytes) {
        wit_component::ComponentEncoder::default()
            .module(bytes)
            .and_then(|encoder| encoder.validate(true).encode())
            .map_err(|err| BuildError::Output(format!("{err:#}")))?
    } else if Parser::is_component(bytes) {
        bytes.to_vec()
    } else {
        return Err(BuildError::Output("unknown file format".into()));
    };
    manifest.wire(read_handlers(&component)?)?;
    let imports = component_imports(&component)?;
    manifest.check_imports(imports.iter().map(String::as_str))?;
    let artifact = attach_manifest(&component, manifest)?;
    check_artifact(&artifact)?;
    // Compiling against the host linker type-checks every import and the `handle` export.
    let config = RuntimeConfig::default();
    let runtime = Runtime::new(config).map_err(|err| BuildError::Host(err.to_string()))?;
    (runtime.compile(&artifact)).map_err(|err| BuildError::Host(err.to_string()))?;
    Ok(artifact)
}

fn run_step(project: &Path, step: &BuildStep) -> Result<PathBuf, BuildError> {
    let (program, args) = step
        .command
        .split_first()
        .ok_or_else(|| BuildError::Manifest("build.command is empty".into()))?;
    let status = Command::new(program)
        .args(args)
        .current_dir(project)
        .status()
        .map_err(|err| BuildError::Toolchain(format!("cannot run `{program}`: {err}")))?;
    if !status.success() {
        return Err(BuildError::Toolchain(format!(
            "`{program}` failed with {status}"
        )));
    }
    Ok(project.join(&step.output))
}

/// Runs `cargo build` for [`RUST_TARGET`] and returns the project's module.
fn cargo_module(cargo_toml: &Path) -> Result<PathBuf, BuildError> {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args(["build", "--release", "--target", RUST_TARGET])
        .arg("--message-format=json-render-diagnostics")
        .arg("--manifest-path")
        .arg(cargo_toml)
        .stderr(Stdio::inherit())
        .output()?;
    if !output.status.success() {
        let hint = format!("cargo build failed; is the {RUST_TARGET} target installed?");
        return Err(BuildError::Toolchain(hint));
    }
    let wanted = std::fs::canonicalize(cargo_toml)?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let module = (stdout.lines())
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|message| message["reason"] == "compiler-artifact")
        .filter(|message| {
            message["manifest_path"].as_str().map(PathBuf::from) == Some(wanted.clone())
        })
        .flat_map(|message| message["filenames"].as_array().cloned().unwrap_or_default())
        .filter_map(|file| file.as_str().map(PathBuf::from))
        .find(|file| file.extension().is_some_and(|ext| ext == "wasm"));
    module.ok_or_else(|| {
        BuildError::Toolchain("cargo built no .wasm module; set crate-type = [\"cdylib\"]".into())
    })
}
