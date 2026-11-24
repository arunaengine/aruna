//! Records of the workflow registry and the tool catalog: workflows, their versions with files,
//! image pins and stored preflight results, and tool definitions, with their storage keys.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_workflow_contract::preflight::{SourceFile, WorkflowLanguage, WorkflowSource};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::errors::ConversionError;
use crate::types::{GroupId, Key};
use crate::user::id::UserId;

/// Who may read a workflow: the members of its group, or every user of the realm.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowVisibility {
    #[default]
    Group,
    Public,
}

/// The language a workflow version is stored in; `Uwe` is a composed `.uwe.yaml` workflow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredWorkflowLanguage {
    Uwe,
    Nextflow,
    Snakemake,
    Cwl,
    Wdl,
    Galaxy,
    Argo,
    Wir,
}

/// How far the catalog trusts a tool definition, from most to least.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCuration {
    Curated,
    Imported,
    #[default]
    Unchecked,
}

/// One file of a stored workflow version or tool.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredFile {
    pub path: String,
    pub content: Vec<u8>,
    pub executable: bool,
}

/// A workflow of a group; its versions hold the files.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRecord {
    pub workflow_id: Ulid,
    pub group_id: GroupId,
    pub name: String,
    pub description: Option<String>,
    pub language: StoredWorkflowLanguage,
    pub visibility: WorkflowVisibility,
    /// Versions are numbered from 1; the latest is this count.
    pub version_count: u32,
    pub edam_formats: Vec<String>,
    pub created_by: UserId,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

/// The preflight results of a version, as JSON text, stored at import or on the first read.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowInspection {
    /// The diagnostics of the check, as the portal reads them.
    pub check: Option<String>,
    /// The JSON Schema of the inputs.
    pub schema: Option<String>,
    pub graph: Option<String>,
    /// The tool definitions found in the workflow.
    pub tools: Option<String>,
}

/// One immutable version of a workflow.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVersionRecord {
    pub workflow_id: Ulid,
    pub version: u32,
    pub language: StoredWorkflowLanguage,
    pub entrypoint: String,
    /// The paths of its files, in path order.
    pub files: Vec<String>,
    /// The commit a Git import read.
    pub commit: Option<String>,
    /// The digest of its source, as `source_digest` computes it.
    pub digest: String,
    pub created_by: UserId,
    pub created_at_ms: u64,
    pub message: Option<String>,
    /// The `.uwe.yaml` text of a composed workflow.
    pub uwe_yaml: Option<String>,
    /// Images as the workflow names them, pinned to the digests the check found.
    pub images: BTreeMap<String, String>,
    pub inspection: WorkflowInspection,
}

/// The files of one workflow version.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowFiles {
    pub files: Vec<StoredFile>,
}

/// One version of a tool definition in a group's catalog, with the files its source names.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRecord {
    pub tool_id: String,
    pub version: String,
    pub group_id: GroupId,
    pub title: String,
    pub curation: ToolCuration,
    /// The definition as uwe's tool-definition crate writes it, as JSON text.
    pub definition: String,
    pub files: Vec<StoredFile>,
    pub created_by: UserId,
    pub created_at_ms: u64,
}

macro_rules! postcard_codec {
    ($($record:ty),*) => {$(
        impl $record {
            pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
                Ok(postcard::to_allocvec(self)?)
            }

            pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
                Ok(postcard::from_bytes(bytes)?)
            }
        }
    )*};
}

postcard_codec!(
    WorkflowRecord,
    WorkflowVersionRecord,
    WorkflowFiles,
    ToolRecord
);

impl StoredWorkflowLanguage {
    pub const ALL: [StoredWorkflowLanguage; 8] = [
        Self::Uwe,
        Self::Nextflow,
        Self::Snakemake,
        Self::Cwl,
        Self::Wdl,
        Self::Galaxy,
        Self::Argo,
        Self::Wir,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Uwe => "uwe",
            Self::Nextflow => "nextflow",
            Self::Snakemake => "snakemake",
            Self::Cwl => "cwl",
            Self::Wdl => "wdl",
            Self::Galaxy => "galaxy",
            Self::Argo => "argo",
            Self::Wir => "wir",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|language| language.as_str() == text)
    }

    /// The language the coordinator is told; a composed workflow runs as wir, so it is detected.
    pub const fn contract(self) -> Option<WorkflowLanguage> {
        match self {
            Self::Uwe => None,
            Self::Nextflow => Some(WorkflowLanguage::Nextflow),
            Self::Snakemake => Some(WorkflowLanguage::Snakemake),
            Self::Cwl => Some(WorkflowLanguage::Cwl),
            Self::Wdl => Some(WorkflowLanguage::Wdl),
            Self::Galaxy => Some(WorkflowLanguage::Galaxy),
            Self::Argo => Some(WorkflowLanguage::Argo),
            Self::Wir => Some(WorkflowLanguage::Wir),
        }
    }
}

impl WorkflowVisibility {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Group => "group",
            Self::Public => "public",
        }
    }
}

impl ToolCuration {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Curated => "curated",
            Self::Imported => "imported",
            Self::Unchecked => "unchecked",
        }
    }
}

impl WorkflowFiles {
    /// The files as the coordinator takes them.
    pub fn source(&self) -> WorkflowSource {
        WorkflowSource {
            files: self.files.iter().map(StoredFile::contract).collect(),
        }
    }
}

impl StoredFile {
    pub fn contract(&self) -> SourceFile {
        SourceFile {
            path: self.path.clone(),
            content: self.content.clone(),
            executable: self.executable,
        }
    }
}

impl From<SourceFile> for StoredFile {
    fn from(file: SourceFile) -> Self {
        Self {
            path: file.path,
            content: file.content,
            executable: file.executable,
        }
    }
}

/// The row of a workflow: its id.
pub fn workflow_key(workflow_id: Ulid) -> Key {
    Key::from(workflow_id.to_bytes().to_vec())
}

/// The row of a version and of its files: the workflow id, then the version big-endian, so a
/// workflow's versions list in number order.
pub fn version_key(workflow_id: Ulid, version: u32) -> Key {
    let mut key = workflow_id.to_bytes().to_vec();
    key.extend_from_slice(&version.to_be_bytes());
    Key::from(key)
}

/// The rows of every version of a workflow.
pub fn version_prefix(workflow_id: Ulid) -> Key {
    workflow_key(workflow_id)
}

/// The row of a tool version in one group: the tool id and the version, each with its length
/// first, then the group, so every group's copy of one tool version lists together.
pub fn tool_key(tool_id: &str, version: &str, group_id: GroupId) -> Key {
    let mut key = tool_version_prefix(tool_id, version).to_vec();
    key.extend_from_slice(&group_id.to_bytes());
    Key::from(key)
}

/// The rows of one tool version across groups.
pub fn tool_version_prefix(tool_id: &str, version: &str) -> Key {
    let mut key = Vec::with_capacity(4 + tool_id.len() + version.len());
    for part in [tool_id, version] {
        key.extend_from_slice(&(part.len() as u16).to_be_bytes());
        key.extend_from_slice(part.as_bytes());
    }
    Key::from(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_version_keys() {
        let workflow = Ulid::from_bytes([1; 16]);
        let second = version_key(workflow, 2);
        let tenth = version_key(workflow, 10);
        assert!(second < tenth);
        assert!(tenth.starts_with(version_prefix(workflow)));
    }

    #[test]
    fn separates_tool_keys() {
        let group = Ulid::from_bytes([2; 16]);
        let joined = tool_key("ab", "c", group);
        assert_ne!(joined, tool_key("a", "bc", group));
        assert!(joined.starts_with(tool_version_prefix("ab", "c")));
        assert!(!tool_key("ab", "cd", group).starts_with(tool_version_prefix("ab", "c")));
    }

    #[test]
    fn round_trips_records() {
        let record = ToolRecord {
            tool_id: "fastqc".into(),
            version: "0.12.1".into(),
            group_id: Ulid::from_bytes([3; 16]),
            title: "FastQC".into(),
            curation: ToolCuration::Imported,
            definition: "{\"id\":\"fastqc\"}".into(),
            files: vec![StoredFile {
                path: "main.nf".into(),
                content: b"process X {}".to_vec(),
                executable: false,
            }],
            created_by: UserId::default(),
            created_at_ms: 7,
        };
        let bytes = record.to_bytes().unwrap();
        assert_eq!(ToolRecord::from_bytes(&bytes).unwrap(), record);
        for language in StoredWorkflowLanguage::ALL {
            assert_eq!(
                StoredWorkflowLanguage::parse(language.as_str()),
                Some(language)
            );
        }
    }
}
