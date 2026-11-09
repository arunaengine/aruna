//! Starts plugin projects and builds them into one checked WASM component with its manifest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use aruna_plugins::artifact::{ArtifactDigest, attach_manifest, component_imports, read_handlers};
use aruna_plugins::manifest::{Manifest, valid_plugin};
use serde::Serialize;
use serde_json::Value;

use super::Context;
use crate::args::PluginCommand;
use crate::error::CliError;

const TARGET: &str = "wasm32-unknown-unknown";
const SDK_SOURCE: &str = r#"{ git = "https://github.com/arunaengine/aruna", branch = "main" }"#;

pub async fn plugin(context: &mut Context<'_>, command: PluginCommand) -> Result<(), CliError> {
    match command {
        PluginCommand::New { name, path } => {
            let path = path.unwrap_or_else(|| PathBuf::from(&name));
            create(&name, &path)?;
            context.out.line(format!(
                "created {}; next: aruna-cli plugin build {}",
                path.display(),
                path.display()
            ))
        }
        PluginCommand::Build { path } => {
            let built = build(&path)?;
            match context.out.json {
                true => context.out.json(&built),
                false => context
                    .out
                    .line(format!("{} {}", built.artifact.display(), built.digest)),
            }
        }
    }
}

/// Writes a starter project with one manual handler into an empty or new folder.
fn create(name: &str, path: &Path) -> Result<(), CliError> {
    if !valid_plugin(name) {
        return Err(CliError::usage(
            "a plugin name has 1 to 64 of a-z, 0-9 and -, starts with a letter and does not end with -",
        ));
    }
    if path.exists() && path.read_dir()?.next().is_some() {
        return Err(CliError::usage(format!(
            "{} exists and is not empty",
            path.display()
        )));
    }
    std::fs::create_dir_all(path.join("src"))?;
    let manifest = format!(
        "name = \"{name}\"\nversion = \"0.1.0\"\n\n[triggers]\nmanual = {{ handler = \"run\" }}\n\n[permissions]\nrequired = []\n"
    );
    let cargo = format!(
        "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2024\"\npublish = false\n\n[lib]\ncrate-type = [\"cdylib\"]\n\n[dependencies]\naruna-plugin = {SDK_SOURCE}\nserde_json = \"1\"\n"
    );
    let code = "use aruna_plugin::{Context, Event, Result};\n\nasync fn run(ctx: Context, _event: Event) -> Result<String> {\n    Ok(format!(\"hello from {}\", ctx.install()))\n}\n\naruna_plugin::export!(run);\n";
    std::fs::write(path.join("plugin.toml"), manifest)?;
    std::fs::write(path.join("Cargo.toml"), cargo)?;
    std::fs::write(path.join("src").join("lib.rs"), code)?;
    Ok(())
}

#[derive(Debug, Serialize)]
struct Built {
    artifact: PathBuf,
    digest: String,
    handlers: Vec<String>,
}

/// Compiles the project for WASM, wires its triggers to the exported handlers, checks the
/// imports against the requested permissions and writes `<name>-<version>.wasm`.
fn build(path: &Path) -> Result<Built, CliError> {
    let text = std::fs::read_to_string(path.join("plugin.toml"))?;
    let mut manifest: Manifest =
        toml::from_str(&text).map_err(|err| CliError::usage(format!("plugin.toml: {err}")))?;
    manifest.check().map_err(manifest_error)?;
    let module_path = compile(&path.join("Cargo.toml"))?;
    let module = std::fs::read(&module_path)?;
    let handlers = read_handlers(&module).map_err(|err| CliError::usage(err.to_string()))?;
    manifest.wire(handlers).map_err(manifest_error)?;
    let component = wit_component::ComponentEncoder::default()
        .module(&module)
        .and_then(|encoder| encoder.validate(true).encode())
        .map_err(|err| CliError::usage(format!("component encoding failed: {err:#}")))?;
    let imports = component_imports(&component).map_err(|err| CliError::usage(err.to_string()))?;
    manifest
        .check_imports(imports.iter().map(String::as_str))
        .map_err(manifest_error)?;
    let artifact =
        attach_manifest(&component, &manifest).map_err(|err| CliError::usage(err.to_string()))?;
    let name = format!("{}-{}.wasm", manifest.name, manifest.version);
    let out = module_path.with_file_name(name);
    std::fs::write(&out, &artifact)?;
    Ok(Built {
        artifact: out,
        digest: ArtifactDigest::of(&artifact).to_string(),
        handlers: manifest.handlers.into_iter().collect(),
    })
}

/// Runs `cargo build` for the WASM target and returns the project's module.
fn compile(cargo_toml: &Path) -> Result<PathBuf, CliError> {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args([
            "build",
            "--release",
            "--target",
            TARGET,
            "--message-format=json-render-diagnostics",
        ])
        .arg("--manifest-path")
        .arg(cargo_toml)
        .stderr(Stdio::inherit())
        .output()?;
    if !output.status.success() {
        return Err(CliError::usage(format!(
            "cargo build failed; is the {TARGET} target installed?"
        )));
    }
    let wanted = std::fs::canonicalize(cargo_toml)?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let module = stdout
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|message| message["reason"] == "compiler-artifact")
        .filter(|message| {
            message["manifest_path"].as_str().map(PathBuf::from) == Some(wanted.clone())
        })
        .flat_map(|message| message["filenames"].as_array().cloned().unwrap_or_default())
        .filter_map(|file| file.as_str().map(PathBuf::from))
        .find(|file| file.extension().is_some_and(|ext| ext == "wasm"));
    module.ok_or_else(|| {
        CliError::usage("cargo built no .wasm module; set crate-type = [\"cdylib\"]")
    })
}

fn manifest_error(error: aruna_plugins::manifest::ManifestError) -> CliError {
    CliError::usage(format!("plugin.toml: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starter_project() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lab-sync");
        create("lab-sync", &path).unwrap();
        let text = std::fs::read_to_string(path.join("plugin.toml")).unwrap();
        let mut manifest: Manifest = toml::from_str(&text).unwrap();
        assert_eq!(manifest.check(), Ok(()));
        assert_eq!(manifest.wire(["run".to_string()].into()), Ok(()));
        let code = std::fs::read_to_string(path.join("src").join("lib.rs")).unwrap();
        assert!(code.contains("aruna_plugin::export!(run);"));
        assert!(
            create("lab-sync", &path).is_err(),
            "a folder with files is refused"
        );
        assert!(create("Lab", &dir.path().join("other")).is_err());
    }
}
