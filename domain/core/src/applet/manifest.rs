//! The manifest of one applet version: its pinned files, Git commit and locked packages.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::errors::ConversionError;
use crate::git::valid_path;
use crate::id::NodeId;

/// Bytes the files of one version may hold together.
pub const MAX_SITE_BYTES: u64 = 256 << 20;
/// The lockfile the curated builder writes.
pub const LOCKFILE: &str = "package-lock.json";

/// One served file, pinned to an exact object version in the held deployment area.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestFile {
    pub path: String,
    pub node: NodeId,
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub blake3: String,
    pub content_type: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Package {
    pub name: String,
    pub version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionManifest {
    pub applet_id: Ulid,
    pub version: u32,
    pub commit: Option<String>,
    pub lockfile_blake3: Option<String>,
    pub packages: Vec<Package>,
    /// Sorted by path, so equal versions encode to equal bytes.
    pub files: Vec<ManifestFile>,
    /// The builder's `aruna-source-map.json`, kept out of `files` so only preview sessions of
    /// the creator and approvers ever read it.
    pub source_map: Option<crate::applet::record::PinnedObject>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ManifestError {
    #[error("`{0}` is not a valid site path")]
    Path(String),
    #[error("`{0}` appears twice")]
    Duplicate(String),
    #[error("a version holds no index.html at its root")]
    NoIndex,
    #[error("the files of a version may hold at most {MAX_SITE_BYTES} bytes")]
    TooLarge,
    #[error("the lockfile is not valid: {0}")]
    Lockfile(String),
}

impl VersionManifest {
    /// Sorts the files and checks their paths, their total size and the root index.
    pub fn checked(mut self) -> Result<Self, ManifestError> {
        self.files.sort_by(|left, right| left.path.cmp(&right.path));
        let mut total = 0u64;
        for pair in self.files.windows(2) {
            if pair[0].path == pair[1].path {
                return Err(ManifestError::Duplicate(pair[0].path.clone()));
            }
        }
        for file in &self.files {
            if !valid_path(&file.path) {
                return Err(ManifestError::Path(file.path.clone()));
            }
            total = total.saturating_add(file.size_bytes);
        }
        if total > MAX_SITE_BYTES {
            return Err(ManifestError::TooLarge);
        }
        if self.file("index.html").is_none() {
            return Err(ManifestError::NoIndex);
        }
        Ok(self)
    }

    pub fn file(&self, path: &str) -> Option<&ManifestFile> {
        (self.files)
            .binary_search_by(|file| file.path.as_str().cmp(path))
            .ok()
            .map(|index| &self.files[index])
    }

    /// The file a request path serves: a folder serves its `index.html`.
    pub fn resolve(&self, request_path: &str) -> Option<&ManifestFile> {
        let path = request_path.trim_start_matches('/');
        if path.is_empty() || path.ends_with('/') {
            return self.file(&format!("{path}index.html"));
        }
        (self.file(path)).or_else(|| self.file(&format!("{path}/index.html")))
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(serde_json::to_vec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// The packages an npm lockfile (version 2 or 3) installs, sorted by name.
pub fn lockfile_packages(bytes: &[u8]) -> Result<Vec<Package>, ManifestError> {
    #[derive(Deserialize)]
    struct Entry {
        version: Option<String>,
    }
    #[derive(Deserialize)]
    struct Lockfile {
        #[serde(default)]
        packages: BTreeMap<String, Entry>,
    }
    let lockfile: Lockfile = serde_json::from_slice(bytes)
        .map_err(|error| ManifestError::Lockfile(error.to_string()))?;
    let mut packages: Vec<Package> = (lockfile.packages.into_iter())
        .filter_map(|(path, entry)| {
            let name = path.rsplit_once("node_modules/")?.1.to_string();
            Some(Package {
                name,
                version: entry.version?,
            })
        })
        .collect();
    packages.sort();
    packages.dedup();
    Ok(packages)
}

/// The content type a file is served with, from its extension; unknown types are bytes.
pub fn content_type(path: &str) -> &'static str {
    let extension = path.rsplit_once('.').map(|(_, extension)| extension);
    match extension.map(str::to_ascii_lowercase).as_deref() {
        Some("html" | "htm") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("json" | "map") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        Some("wasm") => "application/wasm",
        Some("txt" | "md") => "text/plain; charset=utf-8",
        Some("csv") => "text/csv; charset=utf-8",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, size_bytes: u64) -> ManifestFile {
        ManifestFile {
            path: path.into(),
            node: iroh::SecretKey::from_bytes(&[1; 32]).public(),
            bucket: "applets".into(),
            key: format!("v1/{path}"),
            version_id: "v".into(),
            blake3: "00".into(),
            content_type: content_type(path).into(),
            size_bytes,
        }
    }

    fn manifest(files: Vec<ManifestFile>) -> VersionManifest {
        VersionManifest {
            applet_id: Ulid::nil(),
            version: 1,
            commit: None,
            lockfile_blake3: None,
            packages: Vec::new(),
            files,
            source_map: None,
        }
    }

    #[test]
    fn checks_and_resolves() {
        let checked = manifest(vec![file("docs/index.html", 2), file("index.html", 1)])
            .checked()
            .unwrap();
        assert_eq!(checked.files[0].path, "docs/index.html");
        assert_eq!(checked.resolve("/").unwrap().path, "index.html");
        assert_eq!(checked.resolve("/docs").unwrap().path, "docs/index.html");
        assert!(checked.resolve("/missing.js").is_none());
        let refused = manifest(vec![file("../index.html", 1)]).checked();
        assert_eq!(refused, Err(ManifestError::Path("../index.html".into())));
        let empty = manifest(vec![file("app.js", 1)]).checked();
        assert_eq!(empty, Err(ManifestError::NoIndex));
        let large = manifest(vec![file("index.html", MAX_SITE_BYTES + 1)]).checked();
        assert_eq!(large, Err(ManifestError::TooLarge));
    }

    #[test]
    fn reads_lockfile_packages() {
        let lockfile = br#"{"lockfileVersion":3,"packages":{
            "":{"name":"site"},
            "node_modules/d3":{"version":"7.9.0"},
            "node_modules/@scope/plot":{"version":"1.2.3"},
            "node_modules/d3/node_modules/d3-array":{"version":"3.2.4"}}}"#;
        let packages = lockfile_packages(lockfile).unwrap();
        let names: Vec<&str> = packages
            .iter()
            .map(|package| package.name.as_str())
            .collect();
        assert_eq!(names, vec!["@scope/plot", "d3", "d3-array"]);
        assert!(lockfile_packages(b"not json").is_err());
    }
}
