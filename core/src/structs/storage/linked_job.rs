//! Defines the background jobs on a linked bucket: key re-mapping and duplicate file cleanup.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::structs::identity::auth::AuthContext;
use crate::structs::storage::backend_import::TokenGrant;
use crate::structs::storage::glob::{check_glob, glob_match};
use crate::types::GroupId;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// What a remap selects and where each selected key goes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemapPlan {
    /// Folder whose keys are selected: empty for the bucket root, else ending with `/`.
    pub from: String,
    /// File name pattern of `glob.rs`, matched against the last part of each key.
    pub pattern: String,
    /// New key: `{name}` is the file name, `{dir}` the folders between `from` and the name.
    pub template: String,
    /// Also selects keys in folders below `from`.
    pub subfolders: bool,
}

impl RemapPlan {
    /// Says why the plan cannot run, if it cannot.
    pub fn check(&self) -> Result<(), String> {
        check_glob(&self.pattern)?;
        if !(self.from.is_empty() || self.from.ends_with('/')) || self.from.starts_with('/') {
            return Err("`from` must be empty or a folder ending with `/`".to_string());
        }
        if !self.template.contains("{name}") {
            return Err("the template needs `{name}`".to_string());
        }
        let rest = self.template.replace("{name}", "").replace("{dir}", "");
        if rest.contains(['{', '}']) {
            return Err("the template knows only `{name}` and `{dir}`".to_string());
        }
        if self.template.contains("{dir}") && !self.subfolders {
            return Err("`{dir}` needs `subfolders`".to_string());
        }
        Ok(())
    }

    /// The folders below `from` and the file name of a selected key; `None` when not selected.
    fn split<'a>(&self, key: &'a str) -> Option<(&'a str, &'a str)> {
        let rest = key.strip_prefix(&self.from)?;
        let (dir, name) = match rest.rfind('/') {
            Some(at) => rest.split_at(at + 1),
            None => ("", rest),
        };
        let selected = !name.is_empty()
            && (self.subfolders || dir.is_empty())
            && glob_match(&self.pattern, name);
        selected.then_some((dir, name))
    }

    /// Whether the plan selects `key`.
    pub fn selects(&self, key: &str) -> bool {
        self.split(key).is_some()
    }

    /// The new key of a selected `key`.
    pub fn target(&self, key: &str) -> Option<String> {
        let (dir, name) = self.split(key)?;
        Some(self.template.replace("{dir}", dir).replace("{name}", name))
    }
}

/// The work of one linked job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkedTask {
    /// Gives the selected keys new names; the stored files stay.
    Remap(RemapPlan),
    /// Lets the versions of one key with the same content read one linked file, for the keys
    /// under `prefix`, and releases the other files.
    Unify { prefix: String },
}

impl LinkedTask {
    /// The folder whose keys the task walks through.
    pub fn prefix(&self) -> &str {
        match self {
            Self::Remap(plan) => &plan.from,
            Self::Unify { prefix } => prefix,
        }
    }
}

/// Where a linked job stands. Only `Running` jobs make progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkedState {
    Running,
    Paused,
    /// Stopped for good by its caller; what it finished stays.
    Aborted,
    Finished,
}

/// The one job of a bucket, keyed by bucket name; a finished or aborted job stays until the
/// next one replaces it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkedJob {
    pub id: Ulid,
    pub task: LinkedTask,
    pub group_id: GroupId,
    /// The caller the job runs as; each key is checked against it when the job reaches it.
    pub auth: AuthContext,
    pub node_id: NodeId,
    pub grant: TokenGrant,
    pub state: LinkedState,
    /// Last bucket key handled; the next page starts after it.
    pub cursor: Option<String>,
    /// Keys the task selected when it started, or a lower bound when `counted` is false.
    pub total: u64,
    pub counted: bool,
    /// Keys changed, keys left as they are with a reason, and keys that failed.
    pub done: u64,
    pub skipped: u64,
    pub failed: u64,
    /// Why the job waits before its next page, such as a running import.
    pub waiting: Option<String>,
    pub started_at_ms: u64,
    pub updated_at_ms: u64,
    pub finished_at_ms: Option<u64>,
}

impl LinkedJob {
    /// Whether the job still holds its bucket: running or paused.
    pub fn is_active(&self) -> bool {
        matches!(self.state, LinkedState::Running | LinkedState::Paused)
    }

    /// Keys the job has not reached yet.
    pub fn remaining(&self) -> u64 {
        let reached = self.done + self.skipped + self.failed;
        self.total.saturating_sub(reached)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(from: &str, template: &str, subfolders: bool) -> RemapPlan {
        RemapPlan {
            from: from.to_string(),
            pattern: "*.faa.gz".to_string(),
            template: template.to_string(),
            subfolders,
        }
    }

    #[test]
    fn maps_root_files() {
        let plan = plan("", "proteins/{name}", false);
        assert_eq!(plan.check(), Ok(()));
        assert_eq!(
            plan.target("a.faa.gz").as_deref(),
            Some("proteins/a.faa.gz")
        );
        assert_eq!(plan.target("x/a.faa.gz"), None);
        assert_eq!(plan.target("a.fna.gz"), None);
    }

    #[test]
    fn keeps_subfolders() {
        let plan = plan("raw/", "proteins/{dir}{name}", true);
        assert_eq!(plan.check(), Ok(()));
        assert_eq!(
            plan.target("raw/x/y/a.faa.gz").as_deref(),
            Some("proteins/x/y/a.faa.gz")
        );
        assert_eq!(plan.target("other/a.faa.gz"), None);
    }

    #[test]
    fn rejects_bad_plans() {
        assert!(plan("", "proteins/", false).check().is_err());
        assert!(plan("", "{dir}{name}", false).check().is_err());
        assert!(plan("raw", "{name}", false).check().is_err());
        assert!(plan("", "{name}{id}", false).check().is_err());
        let mut broken = plan("", "{name}", false);
        broken.pattern = "[a".to_string();
        assert!(broken.check().is_err());
    }
}
