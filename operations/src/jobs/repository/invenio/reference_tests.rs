//! Checks the calls and results of writing a repository file reference.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::repository::push::creator_auth;
use crate::jobs::repository::test_support::{child, job_scope, link, remote, scope, session};
use aruna_core::repository::remote::{RemoteAnswer, RemoteFailure, StopReason};
use aruna_core::repository::{ImportOptions, RepositoryPull};
use aruna_core::structs::execution::job::{
    ImportMetadataTarget, ImportRoCrateTarget, RoCrateLimits,
};
use serde_json::json;
use std::time::SystemTime;

fn spec(pull: Option<RepositoryPull>) -> ImportRoCrateSpec {
    ImportRoCrateSpec {
        auth_context: creator_auth(&scope(), &link()),
        source: ImportRoCrateSource::Repository {
            group_id: link().group_id,
            connector_id: link().connector_id,
            record_id: "7".into(),
            options: ImportOptions::default(),
            pull,
        },
        target: ImportRoCrateTarget {
            bucket: "data".into(),
            prefix: String::new(),
        },
        metadata: ImportMetadataTarget {
            group_id: link().group_id,
            path: "doc".into(),
            public: false,
        },
        limits: RoCrateLimits::default(),
        document_id: link().document_id,
    }
}

fn bucket(group_id: Ulid) -> BucketInfo {
    BucketInfo {
        group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: link().created_by,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Default::default(),
        distribution: None,
    }
}

fn operation(kind: RepositoryConnectorKind, group_id: Ulid) -> WriteRepositoryReference {
    WriteRepositoryReference::new(
        kind,
        scope(),
        job_scope(),
        spec(None),
        bucket(group_id),
        "key".into(),
        Ulid::from_bytes([7; 16]),
        json!({"record_id": "7", "file": {"key": "a.txt", "size": 4}}),
        Ulid::from_bytes([8; 16]),
    )
}

fn metadata(size: u64) -> SourceMetadata {
    SourceMetadata {
        content_length: size,
        content_type: None,
        etag: None,
        last_modified: None,
        source_version: None,
        digests: Default::default(),
    }
}

#[test]
fn other_kind_refused() {
    let mut write = operation(RepositoryConnectorKind::OaiPmh, link().group_id);
    assert_eq!(write.first(), Err(not_supported("references")));
}

#[test]
fn foreign_group_refused() {
    let mut write = operation(RepositoryConnectorKind::Invenio, Ulid::nil());
    assert_eq!(
        write.first(),
        Err(invalid("reference target and connector must share a group"))
    );
}

fn at_head() -> WriteRepositoryReference {
    let mut write = operation(RepositoryConnectorKind::Invenio, link().group_id);
    assert!(matches!(write.first(), Ok(Next::Call(IoCall::Child(_)))));
    let url = session()
        .url(&["records", "7", "files", "a.txt", "content"])
        .unwrap();
    assert_eq!(
        write.next(child::<_, TransferError>(Ok(session()))),
        Ok(Next::Call(session().head(url)))
    );
    write
}

#[test]
fn changed_size_refused() {
    let mut write = at_head();
    let answer = remote(Ok(RemoteAnswer::Metadata(metadata(9))));
    assert_eq!(
        write.next(answer),
        Err(invalid("repository reference size changed"))
    );
}

#[test]
fn stop_precedes_write() {
    let mut write = at_head();
    let checkpoint = write
        .next(remote(Ok(RemoteAnswer::Metadata(metadata(4)))))
        .unwrap();
    assert_eq!(
        checkpoint,
        Next::Call(remote_call(RemoteCall::Checkpoint, Some(job_scope().stop)))
    );
    let stopped = remote(Err(RemoteFailure::Stopped(StopReason::Interrupted)));
    assert_eq!(write.next(stopped), Err(TransferError::Interrupted));
}

#[test]
fn written_reference_answers() {
    let mut write = at_head();
    write
        .next(remote(Ok(RemoteAnswer::Metadata(metadata(4)))))
        .unwrap();
    let started = write.next(remote(Ok(RemoteAnswer::Done))).unwrap();
    assert!(matches!(started, Next::Call(IoCall::Child(_))));
    assert_eq!(
        write.next(child::<(), String>(Ok(()))),
        Ok(Next::Done(metadata(4)))
    );
}

#[test]
fn failed_write_retries() {
    let mut write = at_head();
    write
        .next(remote(Ok(RemoteAnswer::Metadata(metadata(4)))))
        .unwrap();
    write.next(remote(Ok(RemoteAnswer::Done))).unwrap();
    assert_eq!(
        write.next(child::<(), String>(Err("busy".into()))),
        Err(TransferError::Retryable("busy".into()))
    );
}
