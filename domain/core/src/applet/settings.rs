//! A node's applet settings: hosting through the edge and building in builder jobs, both off
//! unless the operator configures them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};

/// Embedding origins an applet may list when the operator sets no other bound.
pub const DEFAULT_ORIGINS: usize = 8;

/// Hosting: the edge serves applets of this node below `applet_domain`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostingSettings {
    pub applet_domain: String,
    /// SHA-256 of the key the edge sends, so the node never stores the key itself.
    pub edge_key_sha256: [u8; 32],
    pub custom_domains: bool,
    pub guests: bool,
    pub embedding: bool,
    pub max_origins: usize,
    /// The exact portal origin allowed to frame private previews; never caller supplied.
    pub portal_origin: Option<String>,
}

/// Building: the curated builder image runs as a batch job on `backend`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildingSettings {
    pub backend: String,
    /// The builder image, pinned by digest.
    pub image: String,
    /// An npm registry mirror the builder may reach; none means offline installs only.
    pub mirror: Option<String>,
    /// Build previews an applet keeps at once, and how long one lives.
    pub max_drafts: usize,
    pub draft_ttl_hours: u64,
    /// Libraries agents should reach for first. Defaults, never an allow-list.
    pub catalogue: Vec<CataloguePackage>,
}

/// One library of the builder's starter, at the version its lockfile pins.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CataloguePackage {
    pub name: String,
    pub version: String,
    pub role: String,
    pub license: String,
    /// Where the license was checked, such as `npm:vue@3.5.42` or an installed LICENSE file.
    pub license_source: Option<String>,
}

/// Build previews an applet keeps when the operator sets no other number.
pub const DEFAULT_DRAFTS: usize = 6;
/// Hours a build preview lives when the operator sets no other number.
pub const DEFAULT_DRAFT_HOURS: u64 = 24;

/// The starter's libraries, each license checked on the pinned version. `reka-ui` backs the
/// starter's copied shadcn-vue component source, which is no npm package of its own.
pub fn default_catalogue() -> Vec<CataloguePackage> {
    [
        ("vue", "3.5.42", "framework", "MIT"),
        ("vite", "8.2.2", "bundler", "MIT"),
        ("typescript", "6.0.3", "language", "Apache-2.0"),
        ("tailwindcss", "4.3.3", "styling", "MIT"),
        ("reka-ui", "2.11.0", "components", "MIT"),
        ("@lucide/vue", "1.38.0", "icons", "ISC"),
        ("echarts", "6.1.0", "charts", "Apache-2.0"),
        ("three", "0.187.0", "3d", "MIT"),
        ("@tanstack/vue-table", "9.3.1", "tables", "MIT"),
        ("maplibre-gl", "6.19.0", "maps", "BSD-3-Clause"),
        ("papaparse", "5.7.0", "data", "MIT"),
    ]
    .into_iter()
    .map(|(name, version, role, license)| CataloguePackage {
        name: name.to_string(),
        version: version.to_string(),
        role: role.to_string(),
        license: license.to_string(),
        // TODO: point reka-ui, echarts, three, vue-table and maplibre-gl at their installed
        // LICENSE files once the starter lockfile is generated.
        license_source: Some(format!("npm:{name}@{version}")),
    })
    .collect()
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppletSettings {
    pub hosting: Option<HostingSettings>,
    pub building: Option<BuildingSettings>,
}

impl HostingSettings {
    /// Whether `key` is the configured edge key, compared in constant time.
    pub fn edge_key_valid(&self, key: &str) -> bool {
        use sha2::Digest;
        let digest: [u8; 32] = sha2::Sha256::digest(key.as_bytes()).into();
        let mut difference = 0u8;
        for (left, right) in digest.iter().zip(self.edge_key_sha256.iter()) {
            difference |= left ^ right;
        }
        !key.is_empty() && difference == 0
    }
}

/// The exact restriction a visitor token of `applet_id` carries: it names no data path, so the
/// token reaches only the node's applet decisions for that applet.
pub fn visitor_marker(realm_id: impl std::fmt::Display, applet_id: ulid::Ulid) -> String {
    format!("/{realm_id}/applets/{applet_id}")
}

/// A builder image is accepted only when pinned by a SHA-256 digest.
pub fn image_pinned(image: &str) -> bool {
    image.rsplit_once("@sha256:").is_some_and(|(name, digest)| {
        !name.is_empty() && digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;

    #[test]
    fn edge_key_matches() {
        let hosting = HostingSettings {
            applet_domain: "apps.example.org".into(),
            edge_key_sha256: sha2::Sha256::digest(b"edge-secret").into(),
            custom_domains: false,
            guests: false,
            embedding: false,
            max_origins: DEFAULT_ORIGINS,
            portal_origin: None,
        };
        assert!(hosting.edge_key_valid("edge-secret"));
        assert!(!hosting.edge_key_valid("edge-secreT"));
        assert!(!hosting.edge_key_valid(""));
    }

    #[test]
    fn images_need_digest() {
        let digest = "a".repeat(64);
        assert!(image_pinned(&format!(
            "harbor.example/aruna/builder@sha256:{digest}"
        )));
        assert!(!image_pinned("harbor.example/aruna/builder:latest"));
        assert!(!image_pinned("@sha256:abc"));
    }
}
