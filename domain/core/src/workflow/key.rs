//! The digest that names a workflow source, so stored preflight results stay tied to the exact
//! files they were computed from.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_workflow_contract::preflight::WorkflowSource;
use sha2::{Digest, Sha256};

/// `sha256:<hex>` over every file in path order: path, executable flag and content, each with
/// its length first, so no two sources share a digest by moving bytes between fields.
pub fn source_digest(source: &WorkflowSource) -> String {
    let mut files: Vec<_> = source.files.iter().collect();
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let mut hasher = Sha256::new();
    hasher.update(b"aruna-workflow-source-v1\0");
    for file in files {
        hasher.update((file.path.len() as u64).to_be_bytes());
        hasher.update(file.path.as_bytes());
        hasher.update([u8::from(file.executable)]);
        hasher.update((file.content.len() as u64).to_be_bytes());
        hasher.update(&file.content);
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use aruna_workflow_contract::preflight::SourceFile;

    use super::*;

    fn file(path: &str, content: &str) -> SourceFile {
        SourceFile {
            path: path.into(),
            content: content.as_bytes().to_vec(),
            executable: false,
        }
    }

    #[test]
    fn digests_ignore_order() {
        let one = WorkflowSource {
            files: vec![file("a", "1"), file("b", "2")],
        };
        let two = WorkflowSource {
            files: vec![file("b", "2"), file("a", "1")],
        };
        assert_eq!(source_digest(&one), source_digest(&two));
        assert!(source_digest(&one).starts_with("sha256:"));
        let moved = WorkflowSource {
            files: vec![file("a1", ""), file("b", "2")],
        };
        assert_ne!(source_digest(&one), source_digest(&moved));
    }
}
