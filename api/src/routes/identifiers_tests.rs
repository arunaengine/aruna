//! Tests trying identifier rules: links found on a sample, and refused rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::routes::metadata::tests::setup_state;

#[tokio::test]
async fn rules_tried_unstored() {
    let test = setup_state().await;
    let parser = |pattern: &str| IdentifierParserBody {
        rules: vec![IdentifierRuleBody {
            name: "BioSample".to_string(),
            pattern: pattern.to_string(),
            target: IdentifierTargetBody::Key,
            url_template: "https://www.ebi.ac.uk/biosamples/samples/{id}".to_string(),
        }],
    };
    let tried = |parser| {
        test_rules(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Json(ParserTestRequest {
                parser,
                key: "SAMN02138552/contigs.fna.gz".to_string(),
                source_url: None,
            }),
        )
    };
    let Json(found) = tried(parser(r"(?P<id>SAMN\d+)")).await.unwrap();
    assert_eq!(found.identifiers.len(), 1);
    assert_eq!(found.identifiers[0].value, "SAMN02138552");
    assert!(matches!(
        tried(parser("(?P<id>[")).await,
        Err(ServerError::BadRequestReason(_))
    ));
    assert!(matches!(
        tried(parser("(?P<x>a)")).await,
        Err(ServerError::BadRequestReason(_))
    ));
}
