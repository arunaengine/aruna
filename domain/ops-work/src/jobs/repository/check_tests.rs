//! Checks the requirement report and the calls of the content check.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::repository::test_support::{artifact, remote, stop};
use aruna_core::repository::remote::RemoteFailure;
use aruna_core::repository::rules::Select;

fn entry(path: &str, compression: ArchiveCompression) -> ArchiveEntry {
    ArchiveEntry {
        index: 0,
        archive_path: path.into(),
        path: path.into(),
        compressed_size: 10,
        uncompressed_size: 10,
        crc32: 0,
        data_offset: 20,
        compression,
        directory: false,
    }
}

fn target(format: bool) -> Target {
    Target {
        name: "file".into(),
        select: Select::default(),
        crate_files: false,
        group: None,
        relations: Vec::new(),
        min: 0,
        content: Content {
            max_files: Some(0),
            format: format.then(|| "fastq".to_string()),
            ..Content::default()
        },
        fields: Vec::new(),
    }
}

fn operation(format: bool, compression: ArchiveCompression) -> CheckContentOperation {
    CheckContentOperation {
        artifact: artifact(),
        entries: HashMap::from([("a.txt".to_string(), entry("a.txt", compression))]),
        targets: VecDeque::from([TargetCheck {
            target: target(format),
            paths: vec!["a.txt".to_string()],
            prefixes: Vec::new(),
        }]),
        findings: Vec::new(),
        stop: stop(),
    }
}

#[test]
fn sizes_skip_reads() {
    let Ok(Next::Done(findings)) = operation(false, ArchiveCompression::Stored).first() else {
        panic!("a size rule reads no bytes");
    };
    assert_eq!(findings.len(), 1);
}

#[test]
fn format_reads_prefix() {
    let mut check = operation(true, ArchiveCompression::Stored);
    assert_eq!(
        check.first(),
        Ok(Next::Call(remote_call(
            RemoteCall::ReadRange {
                source: artifact().location,
                range: 20..30,
            },
            Some(stop()),
        )))
    );
    let answer = remote(Ok(RemoteAnswer::Bytes(b"hello".to_vec())));
    assert!(matches!(check.next(answer), Ok(Next::Done(_))));
}

#[test]
fn compressed_entry_refused() {
    let mut check = operation(true, ArchiveCompression::Deflate);
    assert!(matches!(check.first(), Err(TransferError::Permanent(_))));
}

#[test]
fn failed_read_retries() {
    let mut check = operation(true, ArchiveCompression::Stored);
    check.first().unwrap();
    let failed = remote(Err(RemoteFailure::Retryable("later".into())));
    assert_eq!(
        check.next(failed),
        Err(TransferError::Retryable("later".into()))
    );
}

#[test]
fn invalid_crate_permanent() {
    let check = CrateCheck::new(
        RepositoryConnectorKind::Invenio,
        "https://zenodo.org/api/",
        "not json",
    );
    assert!(matches!(check, Err(TransferError::Permanent(_))));
}

#[test]
fn profile_errors_sort() {
    assert_eq!(
        profile_failure(MetadataError::InvalidInput("bad".into())),
        TransferError::Permanent("bad".into())
    );
    assert!(matches!(
        profile_failure(MetadataError::HandleMissing),
        TransferError::Retryable(_)
    ));
}
