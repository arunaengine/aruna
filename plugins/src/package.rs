//! Builds a plugin project with its own toolchain and packages the result as one checked artifact.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use thiserror::Error;
use wasmparser::Parser;

use crate::artifact::{SectionError, attach_manifest, check_artifact, component_imports};
use crate::limits::RuntimeConfig;
use crate::manifest::{Manifest, ManifestError};
use crate::runtime::{Compiled, Runtime};

/// The target Rust projects compile to; the host adds no WASI there.
pub const RUST_TARGET: &str = "wasm32-unknown-unknown";

/// The host contract Python and JavaScript builds read from the build folder.
const CONTRACT: &str = include_str!("../wit/plugin.wit");

/// The folder a Python or JavaScript build writes into the project, again on every build.
const BUILD_DIR: &str = ".aruna";

/// The pinned toolchains of Python and JavaScript projects.
const COMPONENTIZE_PY: &str = "componentize-py==0.25.1";
const JCO: &str = "@bytecodealliance/jco@1.17.0";
const ESBUILD: &str = "esbuild@0.25.12";

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
    let manifest = crate::authored::parse(&text)?;
    manifest.check()?;
    Ok(manifest)
}

/// Builds a Rust project with cargo, a Python (`plugin.py`) or JavaScript (`plugin.js`) one with
/// its pinned toolchain, then packages the output as `<name>-<version>.wasm`: beside cargo's
/// module, or in the project folder.
pub fn build_project(project: &Path) -> Result<Built, BuildError> {
    let mut manifest = load_manifest(project)?;
    let file = format!("{}-{}.wasm", manifest.name, manifest.version);
    let (output, path) = match project.join("Cargo.toml") {
        cargo if cargo.is_file() => {
            let module = cargo_module(&cargo)?;
            let path = module.with_file_name(file);
            (module, path)
        }
        _ => (component(project, &manifest)?, project.join(file)),
    };
    let bytes = std::fs::read(&output)?;
    let artifact = package(&mut manifest, &bytes)?;
    std::fs::write(&path, &artifact)?;
    Ok(Built {
        manifest,
        artifact,
        path,
    })
}

/// Turns a core module with embedded WIT, or a finished component from any toolchain, into
/// an artifact: manifest checked and embedded, imports checked, whole artifact checked, and the
/// triggers the code registered compared with the manifest's.
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
    manifest.wire()?;
    let imports = component_imports(&component)?;
    manifest.check_imports(imports.iter().map(String::as_str))?;
    let artifact = attach_manifest(&component, manifest)?;
    check_artifact(&artifact)?;
    // Compiling against the host linker type-checks every import and both exports.
    let config = RuntimeConfig::default();
    let runtime = Runtime::new(config).map_err(|err| BuildError::Host(err.to_string()))?;
    let compiled = (runtime.compile(&artifact)).map_err(|err| BuildError::Host(err.to_string()))?;
    manifest.check_triggers(&registered(&runtime, &compiled)?)?;
    Ok(artifact)
}

/// Asks the compiled code for its triggers on a thread of its own, since a build may already run
/// inside an async runtime.
fn registered(runtime: &Runtime, compiled: &Compiled) -> Result<BTreeSet<String>, BuildError> {
    let host = |err: String| BuildError::Host(format!("listing the code's triggers: {err}"));
    std::thread::scope(|scope| {
        let asked = scope.spawn(|| {
            let threads = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| host(err.to_string()))?;
            (threads.block_on(runtime.triggers(compiled))).map_err(|err| host(err.to_string()))
        });
        asked
            .join()
            .map_err(|_| host("the thread ended early".into()))?
    })
}

/// Writes the contract to `.aruna/wit/` and builds the project's component into `.aruna/`.
fn component(project: &Path, manifest: &Manifest) -> Result<PathBuf, BuildError> {
    let (wit, output) = (".aruna/wit", ".aruna/plugin.wasm");
    let commands = if project.join("plugin.py").is_file() {
        // The library's entry imports the project's `plugin` module, which registers handlers.
        crate::bundle::python(project, manifest)?;
        let world = [
            "-d",
            wit,
            "-w",
            "plugin",
            "componentize",
            "-p",
            ".aruna/python",
            "-p",
            ".",
            "aruna._component",
            "-o",
            output,
        ];
        let uv = [
            "uv",
            "run",
            "--no-project",
            "--with",
            COMPONENTIZE_PY,
            "componentize-py",
        ];
        vec![[&uv[..], &world[..]].concat()]
    } else if project.join("plugin.js").is_file() {
        // The entry binds the host and imports `plugin.js`; `aruna` names the written library.
        crate::bundle::javascript(project, manifest)?;
        let bundle = [
            "npx",
            "--yes",
            ESBUILD,
            ".aruna/javascript/entry.js",
            "--bundle",
            "--format=esm",
            "--platform=browser",
            "--alias:aruna=./.aruna/javascript/aruna.js",
            "--external:aruna:plugin/*",
            "--log-level=warning",
            "--outfile=.aruna/javascript/bundle.js",
        ];
        let world = [
            "--wit",
            wit,
            "--world-name",
            "plugin",
            "--disable",
            "all",
            "-o",
            output,
        ];
        let componentize = [
            "npx",
            "--yes",
            JCO,
            "componentize",
            ".aruna/javascript/bundle.js",
        ];
        vec![bundle.to_vec(), [&componentize[..], &world[..]].concat()]
    } else {
        let hint = "found no Cargo.toml, plugin.py or plugin.js to build";
        return Err(BuildError::Toolchain(hint.into()));
    };
    let folder = project.join(BUILD_DIR).join("wit");
    std::fs::create_dir_all(&folder)?;
    std::fs::write(folder.join("plugin.wit"), CONTRACT)?;
    for command in commands {
        let program = command[0];
        let status = Command::new(program)
            .args(&command[1..])
            .current_dir(project)
            .status()
            .map_err(|err| BuildError::Toolchain(format!("cannot run `{program}`: {err}")))?;
        if !status.success() {
            return Err(BuildError::Toolchain(format!(
                "`{program}` failed with {status}"
            )));
        }
    }
    Ok(project.join(output))
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
