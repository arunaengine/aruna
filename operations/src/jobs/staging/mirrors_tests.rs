//! Tests of the calls and results of adding further sources.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::staging::initial_checkpoint;
use crate::jobs::staging::pure_tests::{child_answer, test_connector, test_run};
use aruna_core::errors::AuthorizationError;
use aruna_core::structs::execution::source_access::SourceMetadata;

fn entry() -> PageEntry {
    PageEntry {
        source_path: "src/a".to_string(),
        target_key: "copy/a".to_string(),
        observation: SourceMetadata {
            content_length: 1,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        },
        metadata: Default::default(),
        mirrors: Vec::new(),
    }
}

fn operation(rendered: Vec<(Ulid, String)>) -> MirrorsOperation {
    let run = test_run();
    let next = initial_checkpoint(&run.spec);
    MirrorsOperation::new(
        run,
        PermissionRules::default(),
        next,
        vec![(entry(), rendered)],
    )
}

// An entry without further sources is kept without reading any connector.
#[test]
fn plain_entry_kept() {
    let mut mirrors = operation(Vec::new());
    let Ok(Next::Done(page)) = mirrors.first() else {
        panic!("expected the page");
    };
    assert_eq!(page.entries, [entry()]);
    assert!(page.connectors.is_empty());
}

// A further source the submitter may not read drops its entry and records why.
#[test]
fn denied_mirror_drops() {
    let connector = test_connector();
    let mut mirrors = operation(vec![(connector.connector_id, "other/a".to_string())]);
    assert!(matches!(mirrors.first(), Ok(Next::Call(IoCall::Child(_)))));
    let found = GetSourceResult {
        connector,
        has_secret_config: false,
    };
    let Ok(Next::Done(page)) = mirrors.next(child_answer(Ok::<_, GetSourceError>(found))) else {
        panic!("expected the page");
    };
    assert!(page.entries.is_empty());
    assert_eq!(page.next.items_failed, 1);
    assert_eq!(page.connectors.len(), 1);
}

// A connector that is gone ends the job; a read that failed retries the page.
#[test]
fn connector_failures_map() {
    let id = test_connector().connector_id;
    let mut gone = operation(vec![(id, "x".to_string())]);
    gone.first().unwrap();
    let missing = child_answer(Err::<GetSourceResult, _>(GetSourceError::NotFound));
    assert!(matches!(gone.next(missing), Err(PageFailure::Permanent(_))));
}

// One listed file asks for the submitter's rules first; without mappings it needs nothing more.
#[test]
fn item_mirrors_empty() {
    let mut mirrors = ItemMirrorsOperation::new(test_run(), "src/a".to_string());
    assert!(matches!(mirrors.first(), Ok(Next::Call(IoCall::Child(_)))));
    let rules = child_answer(Ok::<_, AuthorizationError>(PermissionRules::default()));
    assert_eq!(mirrors.next(rules), Ok(Next::Done(Vec::new())));
}
