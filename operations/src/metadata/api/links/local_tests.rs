//! Drives the links read and the set preview through their pages and checks without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn set_id() -> Ulid {
    Ulid::from_bytes([5; 16])
}

fn scope() -> LinkScope {
    LinkScope::Set {
        realm_id: realm(),
        set_id: set_id(),
    }
}

fn page(rows: Rows, more: bool) -> IoResult {
    let next_start_after = more
        .then(|| rows.last().map(|(key, _)| key.clone()))
        .flatten();
    IoResult::Event(Box::new(Event::Storage(StorageEvent::IterResult {
        values: rows,
        next_start_after,
    })))
}

/// One posting row of `length` in the scope, as the prefix walk reads it.
fn length_row(length: u16) -> (Key, Value) {
    let mut key = scope().length_start(length);
    key.extend_from_slice(&[0; 4]);
    (Key::from(key), Value::from(Vec::new()))
}

fn request(queries: Vec<LinkQuery>) -> LinkNodeRequest {
    LinkNodeRequest {
        queries,
        parts_only: false,
        after: None,
        limit: 10,
        confirm: None,
    }
}

fn prefixes(key: &str) -> LinksLocalOperation {
    let query = LinkQuery::KeyPrefixes {
        scope: scope(),
        key: key.to_string(),
    };
    LinksLocalOperation::new(realm(), None, request(vec![query]))
}

#[test]
fn probes_used_lengths() {
    let mut links = prefixes("raw/2026/a.fastq");
    assert!(matches!(links.first(), Ok(Next::Call(IoCall::Effect(_)))));
    for length in [0, 4, 6, 9] {
        let turn = links.next(page(vec![length_row(length)], false));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Effect(_)))));
    }
    // The final miss ends the walk; the found prefixes are read next, one seek each.
    let turn = links.next(page(Vec::new(), false));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Effect(_)))));
    let read: Vec<_> = links.links.iter().map(DataLink::to_id).collect();
    let expected: Vec<_> = ["raw/", "raw/20", "raw/2026/"]
        .into_iter()
        .map(|prefix| scope().link(prefix).to_id())
        .collect();
    assert_eq!(read, expected);
    assert_eq!(links.budget.seeks, NODE_SEEKS - 6);
    assert!(!links.page.cut);
}

#[test]
fn budget_marks_incomplete() {
    let mut links = prefixes("abcd");
    links.budget = LinkBudget { seeks: 2, rows: 10 };
    links.first().unwrap();
    links.next(page(vec![length_row(1)], false)).unwrap();
    links.next(page(vec![length_row(2)], false)).unwrap();
    assert!(links.page.cut);
}

#[test]
fn postings_find_documents() {
    let link = scope().link("raw/");
    let mut links =
        LinksLocalOperation::new(realm(), None, request(vec![LinkQuery::Exact(link.clone())]));
    assert!(matches!(links.first(), Ok(Next::Call(IoCall::Effect(_)))));
    let document = Ulid::from_bytes([9; 16]);
    let posting = LinkPosting::default();
    let row = (
        Key::from(posting_key(&link.key(), document)),
        Value::from(posting.to_bytes().unwrap()),
    );
    let Ok(Next::Call(IoCall::Group(describe))) = links.next(page(vec![row], false)) else {
        panic!("expected the document checks");
    };
    assert_eq!(describe.members.len(), 1);
    let shown = LinkDocument {
        document_id: document,
        group_id: Ulid::from_bytes([2; 16]).to_string(),
        document_path: "datasets/a".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document),
        title: None,
        links: Vec::new(),
    };
    let member = GroupResult::Done(Box::new(Event::Child(ChildOutcome::new(Ok::<
        _,
        MetadataApiError,
    >(
        Described::Shown(shown.clone()),
    )))));
    let finished = IoResult::Group(GroupEvent::Finished {
        results: vec![member],
    });
    let Ok(Next::Call(IoCall::Group(status))) = links.next(finished) else {
        panic!("expected the status counts");
    };
    assert_eq!(status.members.len(), 3);
    let counts = |rows: usize| {
        let values = (0..rows).map(|index| (Key::from(vec![index as u8]), Value::from(Vec::new())));
        GroupResult::Done(Box::new(Event::Storage(StorageEvent::IterResult {
            values: values.collect(),
            next_start_after: None,
        })))
    };
    let finished = IoResult::Group(GroupEvent::Finished {
        results: vec![counts(1), counts(0), counts(2)],
    });
    let Ok(Next::Done(page)) = links.next(finished) else {
        panic!("expected the page");
    };
    assert_eq!(page.documents, vec![shown]);
    assert_eq!(
        (
            page.pending_materializations,
            page.failed_materializations,
            page.truncated_documents
        ),
        (1, 0, 2)
    );
}

#[test]
fn hidden_match_confirmed() {
    let mut page = LinkNodePage::default();
    let document = Ulid::from_bytes([9; 16]);
    let member = GroupResult::Done(Box::new(Event::Child(ChildOutcome::new(Ok::<
        _,
        MetadataApiError,
    >(
        Described::Hidden,
    )))));
    let finished = IoResult::Group(GroupEvent::Finished {
        results: vec![member],
    });
    described(vec![document], finished, Some(document), &mut page).unwrap();
    assert!(page.hidden && page.confirmed);
}

#[test]
fn preview_pages_both_ranges() {
    let mut preview = SetPreviewOperation::new((realm(), None), set_id(), None, 1);
    let Ok(Next::Call(IoCall::Effect(_))) = preview.first() else {
        panic!("expected the prefix range");
    };
    let document = Ulid::from_bytes([9; 16]);
    let link = scope().link("a/");
    let posting = Value::from(LinkPosting::default().to_bytes().unwrap());
    let row = (Key::from(posting_key(&link.key(), document)), posting);
    // A full page names its last key, so the next page starts after it.
    let turn = preview.next(page(vec![row.clone()], true));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Group(_)))));
    assert_eq!(preview.preview.next, Some(row.0.to_vec()));
}

#[test]
fn version_link_named() {
    let preview = SetPreviewOperation::new((realm(), None), set_id(), None, 1);
    let version_id = Ulid::from_bytes([8; 16]);
    let document = Ulid::from_bytes([9; 16]);
    let verified = DataLink::SetVersion {
        realm_id: realm(),
        set_id: set_id(),
        version_id,
    };
    let key = posting_key(&verified.key(), document);
    assert_eq!(preview.link_of(&key), verified);
}

#[test]
fn deleted_document_skipped() {
    let mut describe = DescribeOperation {
        realm_id: realm(),
        auth: None,
        document_id: Ulid::from_bytes([9; 16]),
        postings: Vec::new(),
        stage: DescribeStage::Record,
    };
    assert!(matches!(describe.first(), Ok(Next::Call(IoCall::Child(_)))));
    let gone = IoResult::Child(ChildOutcome::new(Ok::<
        Option<MetadataRegistryRecord>,
        LiveRecordsError,
    >(None)));
    assert_eq!(describe.next(gone), Ok(Next::Done(Described::Skipped)));
}
