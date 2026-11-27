//! The approval card's payload: what a pending change does compared with the live version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};

use super::manifest::{Package, VersionManifest};
use super::record::{Access, Applet, ApprovalRequest, Grants};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileChanges {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageChanges {
    pub added: Vec<Package>,
    pub removed: Vec<Package>,
    /// The package with its new version.
    pub changed: Vec<Package>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDiff {
    pub version: u32,
    pub live: Option<u32>,
    pub commits: (Option<String>, Option<String>),
    pub access: (Access, Access),
    pub files: FileChanges,
    pub packages: PackageChanges,
    pub grants: (Grants, Grants),
    pub origins_added: Vec<String>,
    pub domains: Vec<String>,
    /// Custom domains that share a registered domain with the portal, API, S3 or sign-in hosts.
    #[serde(default)]
    pub shared_domains: Vec<String>,
    /// The work the applet starts may run on a backend without a kernel sandbox.
    pub unsandboxed: bool,
}

impl ApprovalDiff {
    /// Marks custom domains that share a registered domain with any protected host.
    pub fn with_protected(mut self, protected: &[String]) -> Self {
        self.shared_domains = (self.domains.iter())
            .filter(|domain| {
                protected
                    .iter()
                    .any(|host| super::hosts::same_site(domain, host))
            })
            .cloned()
            .collect();
        self
    }
}

/// Compares `pending` and its manifest `next` with the live state and its manifest `live`.
pub fn approval_diff(
    applet: &Applet,
    pending: &ApprovalRequest,
    live: Option<&VersionManifest>,
    next: &VersionManifest,
    unsandboxed: bool,
) -> ApprovalDiff {
    let mut files = FileChanges::default();
    for file in &next.files {
        match live.and_then(|live| live.file(&file.path)) {
            None => files.added.push(file.path.clone()),
            Some(old) if old.blake3 != file.blake3 => files.changed.push(file.path.clone()),
            Some(_) => {}
        }
    }
    if let Some(live) = live {
        (files.removed).extend(
            (live.files.iter())
                .filter(|file| next.file(&file.path).is_none())
                .map(|file| file.path.clone()),
        );
    }
    let before: &[Package] = live.map_or(&[], |live| &live.packages);
    let mut packages = PackageChanges::default();
    for package in &next.packages {
        match before.iter().find(|old| old.name == package.name) {
            None => packages.added.push(package.clone()),
            Some(old) if old.version != package.version => packages.changed.push(package.clone()),
            Some(_) => {}
        }
    }
    (packages.removed).extend(
        (before.iter())
            .filter(|old| !next.packages.iter().any(|package| package.name == old.name))
            .cloned(),
    );
    ApprovalDiff {
        version: pending.version,
        live: applet.live,
        commits: (
            live.and_then(|live| live.commit.clone()),
            next.commit.clone(),
        ),
        access: (applet.access, pending.access),
        files,
        packages,
        grants: (applet.grants.clone(), pending.grants.clone()),
        origins_added: (pending.origins.iter())
            .filter(|origin| !applet.embedding.origins.contains(origin))
            .cloned()
            .collect(),
        domains: applet
            .domains
            .iter()
            .map(|domain| domain.host.clone())
            .collect(),
        shared_domains: Vec::new(),
        unsandboxed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::applet::manifest::ManifestFile;
    use crate::applet::record::tests::{applet, request};

    fn manifest(files: &[(&str, &str)], packages: &[(&str, &str)]) -> VersionManifest {
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        VersionManifest {
            applet_id: ulid::Ulid::nil(),
            version: 1,
            commit: None,
            lockfile_blake3: None,
            packages: (packages.iter())
                .map(|(name, version)| Package {
                    name: name.to_string(),
                    version: version.to_string(),
                })
                .collect(),
            files: (files.iter())
                .map(|(path, hash)| ManifestFile {
                    path: path.to_string(),
                    node,
                    bucket: "b".into(),
                    key: path.to_string(),
                    version_id: "v".into(),
                    blake3: hash.to_string(),
                    content_type: "text/plain".into(),
                    size_bytes: 1,
                })
                .collect(),
            source_map: None,
        }
    }

    #[test]
    fn lists_every_change() {
        let mut applet = applet();
        applet.embedding.origins = vec!["https://kept.org".into()];
        let mut pending = request(2);
        pending.access = Access::Public;
        pending.origins = vec!["https://kept.org".into(), "https://new.org".into()];
        let live = manifest(
            &[("index.html", "a"), ("old.js", "a")],
            &[("d3", "7.8.0"), ("gone", "1")],
        );
        let next = manifest(
            &[("index.html", "b"), ("new.js", "a")],
            &[("d3", "7.9.0"), ("plot", "1")],
        );
        let diff = approval_diff(&applet, &pending, Some(&live), &next, true);
        assert_eq!(diff.files.added, vec!["new.js"]);
        assert_eq!(diff.files.changed, vec!["index.html"]);
        assert_eq!(diff.files.removed, vec!["old.js"]);
        let names = |list: &[Package]| list.iter().map(|p| p.name.clone()).collect::<Vec<_>>();
        assert_eq!(names(&diff.packages.added), vec!["plot"]);
        assert_eq!(names(&diff.packages.changed), vec!["d3"]);
        assert_eq!(names(&diff.packages.removed), vec!["gone"]);
        assert_eq!(diff.access, (Access::Private, Access::Public));
        assert_eq!(diff.origins_added, vec!["https://new.org"]);
        assert!(diff.unsandboxed);
        let first = approval_diff(&applet, &pending, None, &next, false);
        assert_eq!(first.files.added.len(), 2);
    }

    #[test]
    fn flags_shared_domains() {
        let mut applet = applet();
        let entry = |host: &str| crate::applet::record::CustomDomain {
            host: host.into(),
            nonce: "n".into(),
            state: crate::applet::record::DomainState::Active,
            failures: 0,
            checked_at_ms: 0,
        };
        applet.domains = vec![entry("bakta.lab.example.org"), entry("bakta.other.net")];
        let pending = request(1);
        let next = manifest(&[("index.html", "a")], &[]);
        let protected = vec!["portal.example.org".to_string()];
        let diff = approval_diff(&applet, &pending, None, &next, false).with_protected(&protected);
        assert_eq!(diff.shared_domains, vec!["bakta.lab.example.org"]);
    }
}
