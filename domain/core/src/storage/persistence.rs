//! Defines the storage persist policy and the default block cache size of the node database.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

/// Block cache of the node database; Fjall's 32 MiB default rereads blocks on large scans.
pub const CACHE_BYTES: u64 = 256 << 20;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FjallPersistPolicy {
    #[default]
    Buffer,
    SyncAll,
}

impl FjallPersistPolicy {
    pub fn label(self) -> &'static str {
        match self {
            Self::Buffer => "buffer",
            Self::SyncAll => "sync_all",
        }
    }
}

impl std::str::FromStr for FjallPersistPolicy {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "buffer" => Ok(Self::Buffer),
            "sync_all" => Ok(Self::SyncAll),
            other => Err(format!("unsupported fjall persist policy `{other}`")),
        }
    }
}
