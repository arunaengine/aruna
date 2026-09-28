//! Tests metadata adapter parsing, error mapping, and envelope shapes without any storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::auth::ValidatedBearer;
use aruna_core::metadata::MetadataQueryResults;
use aruna_operations::metadata::AuthToken;
use aruna_operations::metadata::api::{
    MetadataQueryForm as QueryForm, aggregate_query_results, deduplicate_fanout_nodes,
    query_select_limit,
};
use serde_json::json;
use std::collections::BTreeMap;

#[test]
fn maps_conflict_409() {
    use axum::response::IntoResponse;
    let mapped = map_create_error(CreateDocumentError::StorageError(
        StorageError::TransactionConflict,
    ));
    assert!(matches!(mapped, ServerError::Conflict(_)));
    assert_eq!(
        mapped.into_response().status(),
        axum::http::StatusCode::CONFLICT
    );
}

#[test]
fn rejects_unknown_order() {
    use axum::response::IntoResponse;
    assert!(matches!(
        parse_metadata_order(None),
        Ok(MetadataListOrder::Created)
    ));
    assert!(matches!(
        parse_metadata_order(Some("created")),
        Ok(MetadataListOrder::Created)
    ));
    assert!(matches!(
        parse_metadata_order(Some("recent")),
        Ok(MetadataListOrder::Recent)
    ));
    let rejected = parse_metadata_order(Some("updated")).expect_err("unknown order rejected");
    assert_eq!(
        rejected.into_response().status(),
        axum::http::StatusCode::BAD_REQUEST
    );
}

#[test]
fn fanout_order_preserved() {
    let first = iroh::SecretKey::from_bytes(&[31u8; 32]).public();
    let second = iroh::SecretKey::from_bytes(&[32u8; 32]).public();
    let third = iroh::SecretKey::from_bytes(&[33u8; 32]).public();

    assert_eq!(
        deduplicate_fanout_nodes(vec![first, second, first, third, second]),
        vec![first, second, third]
    );
}

#[test]
fn bearer_limits() {
    let limit = ValidatedBearer::new_for_test("x".repeat(4096));
    let oversized = ValidatedBearer::new_for_test("x".repeat(4097));

    assert!(matches!(
        forwarded_auth_token(Some(limit)),
        Ok(Some(AuthToken::Bearer(_)))
    ));
    assert!(matches!(
        forwarded_auth_token(Some(oversized)),
        Err(ServerError::BadRequest)
    ));
    assert!(matches!(forwarded_auth_token(None), Ok(None)));
}

#[test]
fn select_rows_deduplicated() {
    let results = aggregate_query_results(
        vec![
            MetadataQueryResults::Solutions(vec![
                BTreeMap::from([(String::from("s"), String::from("<urn:a>"))]),
                BTreeMap::from([(String::from("s"), String::from("<urn:b>"))]),
            ]),
            MetadataQueryResults::Solutions(vec![BTreeMap::from([(
                String::from("s"),
                String::from("<urn:a>"),
            )])]),
        ],
        QueryForm::Select,
        None,
    )
    .unwrap();

    let MetadataQueryResults::Solutions(rows) = results else {
        panic!("expected solutions");
    };
    assert_eq!(rows.len(), 2);
}

#[test]
fn select_limit_reapplied() {
    let results = aggregate_query_results(
        vec![
            MetadataQueryResults::Solutions(vec![
                BTreeMap::from([(String::from("s"), String::from("<urn:a>"))]),
                BTreeMap::from([(String::from("s"), String::from("<urn:b>"))]),
            ]),
            MetadataQueryResults::Solutions(vec![
                BTreeMap::from([(String::from("s"), String::from("<urn:c>"))]),
                BTreeMap::from([(String::from("s"), String::from("<urn:d>"))]),
            ]),
        ],
        QueryForm::Select,
        Some(3),
    )
    .unwrap();

    let MetadataQueryResults::Solutions(rows) = results else {
        panic!("expected solutions");
    };
    assert_eq!(rows.len(), 3);
}

#[test]
fn outer_limit_used() {
    assert_eq!(
        query_select_limit("SELECT ?s WHERE { ?s ?p ?o } LIMIT 5"),
        Some(5)
    );
    assert_eq!(
        query_select_limit("SELECT ?s WHERE { ?s ?p ?o } LIMIT 7 OFFSET 3"),
        Some(7)
    );
    assert_eq!(query_select_limit("SELECT ?s WHERE { ?s ?p ?o }"), None);
    assert_eq!(
        query_select_limit("SELECT ?s WHERE { { SELECT ?s WHERE { ?s ?p ?o } LIMIT 5 } ?s ?p ?o }"),
        None
    );
    assert_eq!(query_select_limit("ASK WHERE { ?s ?p ?o }"), None);
    assert_eq!(query_select_limit("not sparql"), None);
}

#[test]
fn query_envelope_serializes() {
    let response = MetadataQueryResponse {
        result: MetadataQueryResult::Boolean(true),
        nodes_queried: 3,
        nodes_failed: 1,
        complete: false,
        failed_partitions: vec!["partition-a".to_string()],
    };
    let value = serde_json::to_value(&response).unwrap();
    assert_eq!(value["kind"], json!("Boolean"));
    assert_eq!(value["value"], json!(true));
    assert_eq!(value["nodes_queried"], json!(3));
    assert_eq!(value["nodes_failed"], json!(1));
    assert_eq!(value["complete"], json!(false));
    assert_eq!(value["failed_partitions"], json!(["partition-a"]));

    let roundtrip: MetadataQueryResponse = serde_json::from_value(value).unwrap();
    assert!(matches!(
        roundtrip.result,
        MetadataQueryResult::Boolean(true)
    ));
    assert_eq!(roundtrip.nodes_queried, 3);
    assert_eq!(roundtrip.nodes_failed, 1);
    assert!(!roundtrip.complete);
    assert_eq!(roundtrip.failed_partitions, vec!["partition-a"]);
}

#[test]
fn export_refuses_destination() {
    let destination = json!({"destination": {"group_id": "g", "connector_id": "c"}});
    assert!(serde_json::from_value::<SubmitExportRequest>(destination).is_err());
    let plain = json!({"idempotency_key": "run-1"});
    let request = serde_json::from_value::<SubmitExportRequest>(plain).unwrap();
    assert!(request.destination.is_none());
}

#[test]
fn validates_commit_message() {
    assert_eq!(commit_message(None).unwrap(), None);
    assert_eq!(commit_message(Some(" \n\t ".into())).unwrap(), None);
    assert_eq!(
        commit_message(Some("  Add run 42\n\nNew LC-MS data \n".into())).unwrap(),
        Some("Add run 42\n\nNew LC-MS data".into())
    );
    let longest = "a".repeat(MAX_COMMIT_MESSAGE);
    assert_eq!(
        commit_message(Some(longest.clone())).unwrap(),
        Some(longest)
    );
    for refused in [
        "a".repeat(MAX_COMMIT_MESSAGE + 1),
        "Add\0run".into(),
        "Add\u{1b}[31m".into(),
    ] {
        assert!(matches!(
            commit_message(Some(refused)),
            Err(ServerError::BadRequestMessage(_))
        ));
    }
}

#[test]
fn create_accepts_message() {
    let request: CreateMetadataRequest = serde_json::from_value(json!({
        "group_id": "01JABCDEF0123456789ABCDEFG",
        "path": "datasets/run-42",
        "name": "Run 42",
        "description": "LC-MS run",
        "date_published": "2026-04-09",
        "message": "Add run 42"
    }))
    .unwrap();
    let CreateMetadataRequest::Scaffold(request) = request else {
        panic!("scaffold fields select the scaffold form");
    };
    assert_eq!(request.message.as_deref(), Some("Add run 42"));
}
