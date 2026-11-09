//! Reads and writes the sections of a plugin artifact: the manifest, handler names and imports.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use wasm_encoder::{ComponentSection, CustomSection};
use wasmparser::{Parser, Payload};

use crate::manifest::{Manifest, ManifestError};

/// Top-level component section that carries the manifest JSON.
pub const MANIFEST_SECTION: &str = "aruna-plugin";
/// Core module section the SDK's `export!` writes: handler names separated by newlines.
pub const HANDLERS_SECTION: &str = "aruna-handlers";
const DIGEST_PREFIX: &str = "sha256:";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("artifact digests are `sha256:` followed by 64 lowercase hex characters")]
pub struct DigestError;

/// SHA-256 of the exact artifact bytes, written `sha256:<hex>`; it pins code and manifest.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ArtifactDigest(String);

impl ArtifactDigest {
    pub fn of(bytes: &[u8]) -> Self {
        Self(format!(
            "{DIGEST_PREFIX}{}",
            hex::encode(Sha256::digest(bytes))
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ArtifactDigest {
    type Err = DigestError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let hex = value.strip_prefix(DIGEST_PREFIX).ok_or(DigestError)?;
        let lower = |byte: u8| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte);
        if hex.len() != 64 || !hex.bytes().all(lower) {
            return Err(DigestError);
        }
        Ok(Self(value.to_string()))
    }
}

impl TryFrom<String> for ArtifactDigest {
    type Error = DigestError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ArtifactDigest> for String {
    fn from(value: ArtifactDigest) -> Self {
        value.0
    }
}

impl fmt::Display for ArtifactDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Error)]
pub enum SectionError {
    #[error("not a WebAssembly {0}")]
    WrongKind(&'static str),
    #[error("the artifact needs exactly one top-level {MANIFEST_SECTION} section")]
    Manifest,
    #[error(
        "the module needs exactly one {HANDLERS_SECTION} section; call `aruna_plugin::export!`"
    )]
    Handlers,
    #[error("the module lists a handler name that is not valid UTF-8 or is empty")]
    HandlerName,
    #[error("the binary does not parse: {0}")]
    Parse(String),
    #[error("artifact manifest is invalid: {0}")]
    Invalid(#[from] ManifestError),
}

/// The checked manifest of a component, without compiling it. Sections of nested
/// modules or components are ignored, so they cannot supply a manifest.
pub fn read_manifest(component: &[u8]) -> Result<Manifest, SectionError> {
    let data = top_section(component, MANIFEST_SECTION)?.ok_or(SectionError::Manifest)?;
    Ok(Manifest::parse(data)?)
}

/// Appends `manifest` as the manifest section of a component that has none.
pub fn attach_manifest(component: &[u8], manifest: &Manifest) -> Result<Vec<u8>, SectionError> {
    if top_section(component, MANIFEST_SECTION)?.is_some() {
        return Err(SectionError::Manifest);
    }
    let json = serde_json::to_vec(manifest).map_err(|err| SectionError::Parse(err.to_string()))?;
    let mut out = component.to_vec();
    CustomSection {
        name: MANIFEST_SECTION.into(),
        data: json.into(),
    }
    .append_to_component(&mut out);
    read_manifest(&out)?;
    Ok(out)
}

/// The handler names the SDK recorded in a core module, before it becomes a component.
pub fn read_handlers(module: &[u8]) -> Result<BTreeSet<String>, SectionError> {
    if !Parser::is_core_wasm(module) {
        return Err(SectionError::WrongKind("module"));
    }
    let mut found = None;
    for payload in Parser::new(0).parse_all(module) {
        let payload = payload.map_err(|err| SectionError::Parse(err.to_string()))?;
        if let Payload::CustomSection(section) = payload
            && section.name() == HANDLERS_SECTION
            && found.replace(section.data()).is_some()
        {
            return Err(SectionError::Handlers);
        }
    }
    let text = std::str::from_utf8(found.ok_or(SectionError::Handlers)?)
        .map_err(|_| SectionError::HandlerName)?;
    let names: BTreeSet<String> = text.split('\n').map(str::to_string).collect();
    if names.iter().any(String::is_empty) {
        return Err(SectionError::HandlerName);
    }
    Ok(names)
}

/// Names of the top-level imports of a component, such as `aruna:plugin/log@0.2.0`.
pub fn component_imports(component: &[u8]) -> Result<Vec<String>, SectionError> {
    let mut depth = 0usize;
    let mut names = Vec::new();
    for payload in components(component)? {
        match payload.map_err(|err| SectionError::Parse(err.to_string()))? {
            Payload::ModuleSection { .. } | Payload::ComponentSection { .. } => depth += 1,
            Payload::End(_) => depth = depth.saturating_sub(1),
            Payload::ComponentImportSection(reader) if depth == 0 => {
                for import in reader {
                    let import = import.map_err(|err| SectionError::Parse(err.to_string()))?;
                    names.push(import.name.name.to_string());
                }
            }
            _ => {}
        }
    }
    Ok(names)
}

fn components(
    bytes: &[u8],
) -> Result<impl Iterator<Item = wasmparser::Result<Payload<'_>>>, SectionError> {
    if !Parser::is_component(bytes) {
        return Err(SectionError::WrongKind("component"));
    }
    Ok(Parser::new(0).parse_all(bytes))
}

fn top_section<'a>(component: &'a [u8], name: &str) -> Result<Option<&'a [u8]>, SectionError> {
    let mut depth = 0usize;
    let mut found = None;
    for payload in components(component)? {
        match payload.map_err(|err| SectionError::Parse(err.to_string()))? {
            Payload::ModuleSection { .. } | Payload::ComponentSection { .. } => depth += 1,
            Payload::End(_) => depth = depth.saturating_sub(1),
            Payload::CustomSection(section) if depth == 0 && section.name() == name => {
                if found.replace(section.data()).is_some() {
                    return Err(SectionError::Manifest);
                }
            }
            _ => {}
        }
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use wasm_encoder::{Component, Module, ModuleSection};

    use super::*;
    use crate::manifest::PLUGIN_ABI;

    fn manifest() -> Manifest {
        let json = serde_json::json!({
            "abi": PLUGIN_ABI, "name": "probe", "version": "1.0.0",
            "triggers": { "manual": { "handler": "run" } }, "handlers": ["run"]
        });
        serde_json::from_value(json).unwrap()
    }

    fn module_with(sections: &[(&str, &[u8])]) -> Module {
        let mut module = Module::new();
        for (name, data) in sections {
            module.section(&CustomSection {
                name: (*name).into(),
                data: (*data).into(),
            });
        }
        module
    }

    #[test]
    fn nested_manifest_ignored() {
        let json = serde_json::to_vec(&manifest()).unwrap();
        let module = module_with(&[(MANIFEST_SECTION, &json)]);
        let mut component = Component::new();
        component.section(&ModuleSection(&module));
        let nested = component.finish();
        assert!(matches!(
            read_manifest(&nested),
            Err(SectionError::Manifest)
        ));
        let attached = attach_manifest(&nested, &manifest()).unwrap();
        assert_eq!(read_manifest(&attached).unwrap(), manifest());
        assert!(matches!(
            attach_manifest(&attached, &manifest()),
            Err(SectionError::Manifest)
        ));
        assert_eq!(component_imports(&attached).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn handler_names() {
        let module = module_with(&[(HANDLERS_SECTION, b"dataset_created\nresync")]).finish();
        let names = read_handlers(&module).unwrap();
        assert_eq!(
            names.into_iter().collect::<Vec<_>>(),
            ["dataset_created", "resync"]
        );
        let empty = module_with(&[(HANDLERS_SECTION, b"a\n\nb")]).finish();
        assert!(matches!(
            read_handlers(&empty),
            Err(SectionError::HandlerName)
        ));
        let twice = module_with(&[(HANDLERS_SECTION, b"a"), (HANDLERS_SECTION, b"b")]).finish();
        assert!(matches!(read_handlers(&twice), Err(SectionError::Handlers)));
        assert!(matches!(
            read_handlers(&module_with(&[]).finish()),
            Err(SectionError::Handlers)
        ));
    }

    #[test]
    fn digest_spelling() {
        let digest = ArtifactDigest::of(b"artifact");
        assert_eq!(
            digest.as_str().parse::<ArtifactDigest>(),
            Ok(digest.clone())
        );
        let upper = digest.as_str().to_uppercase().replace("SHA256", "sha256");
        for value in [
            "",
            "sha256:",
            "md5:00",
            upper.as_str(),
            &digest.as_str()[1..],
        ] {
            assert_eq!(value.parse::<ArtifactDigest>(), Err(DigestError), "{value}");
        }
    }
}
